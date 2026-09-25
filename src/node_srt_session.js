//
//  SRT publish session.
//  MPEG-TS over SRT is demuxed into FLV tags and fed through the
//  NodeRtmpSession publish path, so GOP cache, players, relay and
//  trans (HLS/DASH) work unchanged.
//

const QueryString = require('querystring');
const AV = require('./node_core_av');
const AMF = require('./node_core_amf');
const Bitop = require('./node_core_bitop');
const Logger = require('./node_core_logger');
const context = require('./node_core_ctx');
const NodeRtmpSession = require('./node_rtmp_session');
const NodeSrtTsDemuxer = require('./node_srt_ts_demuxer');

const FLV_TAG_AUDIO = 8;
const FLV_TAG_VIDEO = 9;
const FLV_TAG_DATA = 18;

const FLV_CODEC_H264 = 7;
const FLV_CODEC_H265 = 12;

const AAC_SAMPLE_RATE = [96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350];

const H264_NAL_IDR = 5;
const H264_NAL_SPS = 7;
const H264_NAL_PPS = 8;
const H264_NAL_AUD = 9;

const H265_NAL_VPS = 32;
const H265_NAL_SPS = 33;
const H265_NAL_PPS = 34;
const H265_NAL_AUD = 35;

const SRT_STATS_INTERVAL = 1000;

// Stands in for net.Socket: players never write to a publisher, and
// destroy() hands the close to the libsrt worker thread.
class NodeSrtSocket {
  constructor(srt, sock, ip, port) {
    this.srt = srt;
    this.sock = sock;
    this.remoteAddress = ip;
    this.remotePort = port;
    this.bytesRead = 0;
    this.bytesWritten = 0;
    this.destroyed = false;
  }

  write() { return true; }
  cork() { }
  uncork() { }
  setTimeout() { }

  destroy() {
    if (this.destroyed) return;
    this.destroyed = true;
    this.srt.closeSocket(this.sock);
  }
}

/**
 * Parse an SRT streamid into { app, name, mode, args }.
 *   #!::r=live/name,m=publish,u=user,...   (SRT access control syntax)
 *   live/name?sign=...
 */
function parseStreamId(streamid, defaultApp) {
  let resource = '';
  let mode = 'publish';
  let args = {};

  if (streamid.startsWith('#!::')) {
    mode = '';
    for (let item of streamid.slice(4).split(',')) {
      let eq = item.indexOf('=');
      if (eq <= 0) continue;
      let key = item.slice(0, eq).trim();
      let value = item.slice(eq + 1).trim();
      if (key === 'r') {
        resource = value;
      } else if (key === 'm') {
        mode = value;
      } else {
        args[key] = value;
      }
    }
    let q = resource.indexOf('?');
    if (q >= 0) {
      Object.assign(args, QueryString.parse(resource.slice(q + 1)));
      resource = resource.slice(0, q);
    }
  } else {
    let q = streamid.indexOf('?');
    resource = q >= 0 ? streamid.slice(0, q) : streamid;
    if (q >= 0) args = QueryString.parse(streamid.slice(q + 1));
  }

  resource = resource.replace(/^\/+/, '');
  let slash = resource.indexOf('/');
  let app = slash >= 0 ? resource.slice(0, slash) : '';
  let name = slash >= 0 ? resource.slice(slash + 1) : resource;
  return { app: app || defaultApp, name, mode, args };
}

const START_CODE = Buffer.from([0, 0, 1]);

// Split an Annex-B byte stream into NAL units (start codes removed).
function splitAnnexB(data) {
  let nals = [];
  let start = -1;
  let i = data.indexOf(START_CODE);
  while (i >= 0) {
    if (start >= 0) pushNal(nals, data, start, i);
    start = i + 3;
    i = data.indexOf(START_CODE, start);
  }
  if (start >= 0) pushNal(nals, data, start, data.length);
  return nals;
}

function pushNal(nals, data, start, end) {
  while (end > start && data[end - 1] === 0) end--; // next 4-byte start code / trailing zeros
  if (end > start) nals.push(data.subarray(start, end));
}

function removeEmulationPrevention(nal) {
  let out = Buffer.alloc(nal.length);
  let n = 0;
  for (let i = 0; i < nal.length; i++) {
    if (i + 2 < nal.length && nal[i] === 0 && nal[i + 1] === 0 && nal[i + 2] === 3) {
      out[n++] = 0;
      out[n++] = 0;
      i += 2;
    } else {
      out[n++] = nal[i];
    }
  }
  return out.subarray(0, n);
}

function sameNals(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (!a[i].equals(b[i])) return false;
  }
  return true;
}

function nalArray(nals) {
  let parts = [];
  for (let nal of nals) {
    let len = Buffer.alloc(2);
    len.writeUInt16BE(nal.length);
    parts.push(len, nal);
  }
  return parts;
}

function buildAvcC(spsList, ppsList) {
  let sps = spsList[0];
  let count = Buffer.alloc(1);
  count[0] = ppsList.length;
  return Buffer.concat([
    Buffer.from([0x01, sps[1], sps[2], sps[3], 0xff, 0xe0 | spsList.length]),
    ...nalArray(spsList),
    count,
    ...nalArray(ppsList)
  ]);
}

function skipBits(bitop, n) {
  while (n > 0) {
    let k = Math.min(n, 16);
    bitop.read(k);
    n -= k;
  }
}

// Fields of an HEVC SPS that hvcC needs besides the general PTL bytes.
function parseHevcSps(rbsp) {
  let bitop = new Bitop(rbsp.subarray(2));
  bitop.read(4); // sps_video_parameter_set_id
  let maxSubLayersMinus1 = bitop.read(3);
  let temporalIdNested = bitop.read(1);
  skipBits(bitop, 96); // general profile_tier_level
  let profilePresent = [];
  let levelPresent = [];
  for (let i = 0; i < maxSubLayersMinus1; i++) {
    profilePresent[i] = bitop.read(1);
    levelPresent[i] = bitop.read(1);
  }
  if (maxSubLayersMinus1 > 0) {
    for (let i = maxSubLayersMinus1; i < 8; i++) bitop.read(2);
  }
  for (let i = 0; i < maxSubLayersMinus1; i++) {
    if (profilePresent[i]) skipBits(bitop, 88);
    if (levelPresent[i]) skipBits(bitop, 8);
  }
  bitop.read_golomb(); // sps_seq_parameter_set_id
  let chromaFormatIdc = bitop.read_golomb();
  if (chromaFormatIdc === 3) bitop.read(1);
  bitop.read_golomb(); // pic_width_in_luma_samples
  bitop.read_golomb(); // pic_height_in_luma_samples
  if (bitop.read(1)) { // conformance_window_flag
    for (let i = 0; i < 4; i++) bitop.read_golomb();
  }
  let bitDepthLumaMinus8 = bitop.read_golomb();
  let bitDepthChromaMinus8 = bitop.read_golomb();
  return { maxSubLayersMinus1, temporalIdNested, chromaFormatIdc, bitDepthLumaMinus8, bitDepthChromaMinus8 };
}

function buildHvcC(vpsList, spsList, ppsList) {
  let rbsp = removeEmulationPrevention(spsList[0]);
  if (rbsp.length < 15) return null;
  let ptl = rbsp.subarray(3, 15); // general_profile_space .. general_level_idc
  let sps = parseHevcSps(rbsp);

  let header = Buffer.alloc(23);
  header[0] = 0x01; // configurationVersion
  ptl.copy(header, 1, 0, 12);
  header[13] = 0xf0; // min_spatial_segmentation_idc = 0
  header[14] = 0x00;
  header[15] = 0xfc; // parallelismType = 0
  header[16] = 0xfc | (sps.chromaFormatIdc & 0x03);
  header[17] = 0xf8 | (sps.bitDepthLumaMinus8 & 0x07);
  header[18] = 0xf8 | (sps.bitDepthChromaMinus8 & 0x07);
  header[19] = 0x00; // avgFrameRate
  header[20] = 0x00;
  header[21] = (((sps.maxSubLayersMinus1 + 1) & 0x07) << 3) | (sps.temporalIdNested << 2) | 0x03;
  header[22] = 3; // numOfArrays

  let arrays = [];
  for (let [type, list] of [[H265_NAL_VPS, vpsList], [H265_NAL_SPS, spsList], [H265_NAL_PPS, ppsList]]) {
    // No array_completeness bit: readHEVCSpecificConfig compares this byte
    // against the NAL type unmasked and would miss the SPS (0x0 resolution).
    let h = Buffer.alloc(3);
    h[0] = type;
    h.writeUInt16BE(list.length, 1);
    arrays.push(h, ...nalArray(list));
  }
  return Buffer.concat([header, ...arrays]);
}

class NodeSrtSession extends NodeRtmpSession {
  constructor(config, srt, info) {
    let socket = new NodeSrtSocket(srt, info.sock, info.ip, info.port);
    // NodeRtmpSession reads chunk/ping/gop settings from config.rtmp.
    super(config.rtmp ? config : Object.assign({}, config, { rtmp: {} }), socket);
    this.TAG = 'srt';
    this.srt = srt;
    this.sock = info.sock;
    this.streamid = info.streamid || '';
    this.req = { socket, connection: socket };

    this.parserPacket = {
      header: { fmt: 0, cid: 0, timestamp: 0, length: 0, type: 0, stream_id: 1 },
      clock: 0,
      payload: null,
      capacity: 0,
      bytes: 0
    };

    this.demuxer = new NodeSrtTsDemuxer();
    this.tsBase = null;
    this.gotKeyframe = false;
    this.metaDataSent = false;
    this.videoParams = null;
    this.videoConfigDirty = false;
    this.aacConfig = null;
    this.aacInfo = null;
    this.srtStats = null;
    this.srtStatsInterval = null;
  }

  run() {
    let { app, name, mode, args } = parseStreamId(this.streamid, this.config.srt.app || 'live');
    this.isStarting = true;

    if (mode !== 'publish') {
      Logger.log(`[srt connect] Rejected: mode '${mode || '(none)'}' is not supported, only m=publish. id=${this.id} ip=${this.ip} streamid=${this.streamid}`);
      return this.reject();
    }
    if (!name) {
      Logger.log(`[srt connect] Rejected: no stream name in streamid. id=${this.id} ip=${this.ip} streamid=${this.streamid}`);
      return this.reject();
    }

    this.connectCmdObj = {
      app,
      flashVer: 'SRT',
      tcUrl: `srt://${this.ip}/${app}`,
      streamid: this.streamid
    };
    context.nodeEvent.emit('preConnect', this.id, this.connectCmdObj);
    if (!this.isStarting) return;

    this.appname = app;
    this.connectTime = new Date();
    this.startTimestamp = Date.now();
    this.bitrateCache = {
      intervalMs: 1000,
      last_update: this.startTimestamp,
      bytes: 0,
    };
    Logger.log(`[srt connect] id=${this.id} ip=${this.ip} port=${this.socket.remotePort} app=${app} streamid=${this.streamid}`);
    context.nodeEvent.emit('postConnect', this.id, this.connectCmdObj);
    if (!this.isStarting) return;

    let query = QueryString.stringify(args);
    this.onPublish({ streamName: query ? `${name}?${query}` : name });
    if (!this.isStarting) return; // rejected inside onPublish (e.g. duplicate path)
    if (!this.isPublishing) {
      return this.reject(); // unauthorized; onPublish logged the reason
    }

    this.demuxer.on('pes', this.onPes.bind(this));
    this.demuxer.on('unsupported', ({ pid, streamType }) => {
      Logger.log(`[srt publish] Ignoring unsupported stream_type=0x${streamType.toString(16)} pid=${pid} id=${this.id} streamPath=${this.publishStreamPath}`);
    });
    this.srtStatsInterval = setInterval(() => {
      let stats = this.srt.stats(this.sock, true);
      if (stats) this.srtStats = stats;
    }, SRT_STATS_INTERVAL);
  }

  stop() {
    if (!this.isStarting) return;
    if (this.srtStatsInterval) {
      clearInterval(this.srtStatsInterval);
      this.srtStatsInterval = null;
    }
    if (this.isPublishing) {
      let stats = (!this.socket.destroyed && this.srt.stats(this.sock, false)) || this.srtStats;
      if (stats) {
        this.srtStats = stats;
        Logger.log(`[srt stats] id=${this.id} streamPath=${this.publishStreamPath} bytes=${stats.byteRecvTotal} rtt=${stats.msRTT.toFixed(1)}ms loss=${stats.pktRcvLossTotal} drop=${stats.pktRcvDropTotal} retrans=${stats.pktRcvRetransTotal}`);
      }
    }
    this.demuxer.removeAllListeners();
    super.stop();
  }

  reject() {
    // Every reject path already logged its reason.
    this.stop();
  }

  onSrtData(data) {
    if (!this.isStarting) return;
    this.socket.bytesRead += data.length;
    this.demuxer.push(data);

    this.bitrateCache.bytes += data.length;
    let now = Date.now();
    let diff = now - this.bitrateCache.last_update;
    if (diff >= this.bitrateCache.intervalMs) {
      this.bitrate = Math.round(this.bitrateCache.bytes * 8 / diff);
      this.bitrateCache.bytes = 0;
      this.bitrateCache.last_update = now;
    }
  }

  onSrtClose() {
    this.socket.destroyed = true;
    this.onSocketClose();
  }

  // 90kHz -> ms relative to the first DTS, clamped at 0.
  timestamp(t90) {
    return Math.max(0, Math.round((t90 - this.tsBase) / 90));
  }

  onPes(pes) {
    if (!this.isStarting || pes.data.length === 0) return;
    if (this.tsBase === null) this.tsBase = pes.dts;
    switch (pes.codec) {
      case 'h264':
      case 'h265':
        return this.onVideoPes(pes);
      case 'aac':
        return this.onAacPes(pes);
    }
  }

  sendTag(type, payload, timestamp) {
    let packet = this.parserPacket;
    packet.header.type = type;
    packet.header.length = payload.length;
    packet.header.timestamp = timestamp;
    packet.payload = payload;
    packet.clock = timestamp;
    switch (type) {
      case FLV_TAG_AUDIO:
        return this.rtmpAudioHandler();
      case FLV_TAG_VIDEO:
        return this.rtmpVideoHandler();
      case FLV_TAG_DATA:
        return this.rtmpDataHandler();
    }
  }

  sendMetaData(videoInfo) {
    if (this.metaDataSent) return;
    this.metaDataSent = true;
    let dataObj = { encoder: 'Node-Media-Server SRT ingest' };
    if (videoInfo) {
      dataObj.width = videoInfo.width;
      dataObj.height = videoInfo.height;
      dataObj.videocodecid = videoInfo.codecId;
    }
    if (this.aacInfo) {
      dataObj.audiocodecid = 10;
      dataObj.audiosamplerate = this.aacInfo.sampleRate;
      dataObj.audiochannels = this.aacInfo.channels;
      dataObj.stereo = this.aacInfo.channels > 1;
    }

    let prev = { sr: this.audioSamplerate, ch: this.audioChannels, w: this.videoWidth, h: this.videoHeight };
    this.sendTag(FLV_TAG_DATA, AMF.encodeAmf0Data({ cmd: '@setDataFrame', method: 'onMetaData', dataObj }), 0);
    // rtmpDataHandler copies these from the metadata; keep what is unknown so far.
    if (!this.aacInfo) {
      this.audioSamplerate = prev.sr;
      this.audioChannels = prev.ch;
    }
    if (!videoInfo) {
      this.videoWidth = prev.w;
      this.videoHeight = prev.h;
    }
    this.videoFps = 0; // let NodeRtmpSession measure it
  }

  onVideoPes(pes) {
    let hevc = pes.codec === 'h265';
    let nals = splitAnnexB(pes.data);
    let vps = [];
    let sps = [];
    let pps = [];
    let frame = [];
    let isKey = false;

    for (let nal of nals) {
      if (hevc) {
        let type = (nal[0] >> 1) & 0x3f;
        if (type === H265_NAL_AUD) continue;
        if (type === H265_NAL_VPS) { vps.push(nal); continue; }
        if (type === H265_NAL_SPS) { sps.push(nal); continue; }
        if (type === H265_NAL_PPS) { pps.push(nal); continue; }
        if (type >= 16 && type <= 21) isKey = true; // IRAP
      } else {
        let type = nal[0] & 0x1f;
        if (type === H264_NAL_AUD) continue;
        if (type === H264_NAL_SPS) { sps.push(nal); continue; }
        if (type === H264_NAL_PPS) { pps.push(nal); continue; }
        if (type === H264_NAL_IDR) isKey = true;
      }
      frame.push(nal);
    }

    this.updateVideoParams(hevc, vps, sps, pps);
    if (!this.videoParams || frame.length === 0) return;
    if (!this.gotKeyframe) {
      if (!isKey) return; // cannot decode anything before the first IDR
      this.gotKeyframe = true;
    }

    let timestamp = this.timestamp(pes.dts);
    let codecId = hevc ? FLV_CODEC_H265 : FLV_CODEC_H264;

    if (this.videoConfigDirty) {
      this.videoConfigDirty = false;
      let seqHeader = Buffer.concat([Buffer.from([0x10 | codecId, 0, 0, 0, 0]), this.videoParams.config]);
      if (!this.metaDataSent) {
        let info = AV.readAVCSpecificConfig(seqHeader);
        this.sendMetaData({ width: info.width, height: info.height, codecId });
      }
      this.sendTag(FLV_TAG_VIDEO, seqHeader, timestamp);
    }

    let cts = Math.round((pes.pts - pes.dts) / 90);
    let size = 5;
    for (let nal of frame) size += 4 + nal.length;
    let payload = Buffer.alloc(size);
    payload[0] = ((isKey ? 1 : 2) << 4) | codecId;
    payload[1] = 1;
    payload.writeIntBE(cts, 2, 3);
    let p = 5;
    for (let nal of frame) {
      payload.writeUInt32BE(nal.length, p);
      nal.copy(payload, p + 4);
      p += 4 + nal.length;
    }
    this.sendTag(FLV_TAG_VIDEO, payload, timestamp);
  }

  updateVideoParams(hevc, vps, sps, pps) {
    let cur = this.videoParams;
    if (sps.length === 0 && pps.length === 0 && vps.length === 0) return;
    let next = {
      hevc,
      vps: vps.length ? vps : cur && cur.vps,
      sps: sps.length ? sps : cur && cur.sps,
      pps: pps.length ? pps : cur && cur.pps
    };
    if (!next.sps || !next.pps || (hevc && !next.vps)) return;
    if (cur && cur.hevc === hevc && sameNals(cur.sps, next.sps) && sameNals(cur.pps, next.pps) &&
      (!hevc || sameNals(cur.vps, next.vps))) {
      return;
    }
    next.sps = next.sps.map(b => Buffer.from(b));
    next.pps = next.pps.map(b => Buffer.from(b));
    next.vps = next.vps ? next.vps.map(b => Buffer.from(b)) : null;
    next.config = hevc ? buildHvcC(next.vps, next.sps, next.pps) : buildAvcC(next.sps, next.pps);
    if (!next.config) return;
    this.videoParams = next;
    this.videoConfigDirty = true;
  }

  onAacPes(pes) {
    let data = pes.data;
    let frames = [];
    let p = 0;
    while (p + 7 <= data.length) {
      if (data[p] !== 0xff || (data[p + 1] & 0xf0) !== 0xf0) break;
      let protectionAbsent = data[p + 1] & 0x01;
      let profile = (data[p + 2] >> 6) & 0x03;
      let samplingIndex = (data[p + 2] >> 2) & 0x0f;
      let channelConfig = ((data[p + 2] & 0x01) << 2) | (data[p + 3] >> 6);
      let frameLength = ((data[p + 3] & 0x03) << 11) | (data[p + 4] << 3) | (data[p + 5] >> 5);
      let headerLength = protectionAbsent ? 7 : 9;
      if (frameLength < headerLength || p + frameLength > data.length) break;
      frames.push({
        objectType: profile + 1,
        samplingIndex,
        channelConfig,
        raw: data.subarray(p + headerLength, p + frameLength)
      });
      p += frameLength;
    }
    if (frames.length === 0) return;

    let f0 = frames[0];
    let sampleRate = AAC_SAMPLE_RATE[f0.samplingIndex];
    if (!sampleRate) return;
    let asc = Buffer.from([
      (f0.objectType << 3) | (f0.samplingIndex >> 1),
      ((f0.samplingIndex & 0x01) << 7) | (f0.channelConfig << 3)
    ]);
    this.aacInfo = { sampleRate, channels: f0.channelConfig === 7 ? 8 : f0.channelConfig };

    if (this.demuxer.hasVideo) {
      if (!this.gotKeyframe) return; // start A/V together at the first video keyframe
    } else if (!this.metaDataSent) {
      this.sendMetaData(null);
    }

    let timestamp = this.timestamp(pes.pts);
    if (!this.aacConfig || !this.aacConfig.equals(asc)) {
      this.aacConfig = asc;
      this.sendTag(FLV_TAG_AUDIO, Buffer.concat([Buffer.from([0xaf, 0x00]), asc]), timestamp);
    }

    for (let i = 0; i < frames.length; i++) {
      let t90 = pes.pts + i * 1024 * 90000 / sampleRate;
      let payload = Buffer.concat([Buffer.from([0xaf, 0x01]), frames[i].raw]);
      this.sendTag(FLV_TAG_AUDIO, payload, this.timestamp(t90));
    }
  }
}

NodeSrtSession.parseStreamId = parseStreamId;

module.exports = NodeSrtSession;
