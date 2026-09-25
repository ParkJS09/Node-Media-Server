//
//  MPEG-TS demuxer for SRT ingest.
//  PAT -> PMT -> PES, emits 'pes' { codec, pid, pts, dts, data } with 33-bit
//  timestamps unwrapped into a monotonic 90kHz clock per stream.
//

const EventEmitter = require('events');

const TS_PACKET_SIZE = 188;
const TS_SYNC_BYTE = 0x47;
const PID_PAT = 0x0000;

const PTS_WRAP = 0x200000000; // 2^33
const PTS_HALF = 0x100000000; // 2^32

const STREAM_TYPES = {
  0x1b: 'h264',
  0x24: 'h265',
  0x0f: 'aac'
};

class NodeSrtTsDemuxer extends EventEmitter {
  constructor() {
    super();
    this.remainder = null;
    this.pmtPid = -1;
    this.pmtVersion = -1;
    this.streams = new Map();
    this.unsupported = new Set();
  }

  get hasVideo() {
    for (let s of this.streams.values()) {
      if (s.codec !== 'aac') return true;
    }
    return false;
  }

  push(data) {
    if (this.remainder) {
      data = Buffer.concat([this.remainder, data]);
      this.remainder = null;
    }
    let p = 0;
    while (p + TS_PACKET_SIZE <= data.length) {
      if (data[p] !== TS_SYNC_BYTE) {
        let next = data.indexOf(TS_SYNC_BYTE, p + 1);
        if (next < 0) {
          p = data.length;
          break;
        }
        p = next;
        continue;
      }
      this.parsePacket(data.subarray(p, p + TS_PACKET_SIZE));
      p += TS_PACKET_SIZE;
    }
    if (p < data.length) {
      this.remainder = Buffer.from(data.subarray(p));
    }
  }

  flush() {
    for (let stream of this.streams.values()) {
      this.emitPes(stream);
    }
  }

  parsePacket(pkt) {
    let pusi = (pkt[1] & 0x40) !== 0;
    let pid = ((pkt[1] & 0x1f) << 8) | pkt[2];
    let afc = (pkt[3] >> 4) & 0x03;
    let p = 4;
    if (afc === 0 || afc === 2) return; // reserved / adaptation field only
    if (afc === 3) {
      p += 1 + pkt[4];
      if (p >= TS_PACKET_SIZE) return;
    }
    let payload = pkt.subarray(p);

    if (pid === PID_PAT) {
      if (pusi) this.parsePat(payload);
    } else if (pid === this.pmtPid) {
      if (pusi) this.parsePmt(payload);
    } else {
      let stream = this.streams.get(pid);
      if (stream) this.onPesPayload(stream, pusi, payload);
    }
  }

  // Returns the section body (after pointer_field), or null.
  section(payload) {
    let p = 1 + payload[0];
    if (p + 3 > payload.length) return null;
    let len = ((payload[p + 1] & 0x0f) << 8) | payload[p + 2];
    if (p + 3 + len > payload.length) return null;
    return payload.subarray(p, p + 3 + len);
  }

  parsePat(payload) {
    let sec = this.section(payload);
    if (!sec || sec[0] !== 0x00) return;
    let end = sec.length - 4; // CRC32
    for (let p = 8; p + 4 <= end; p += 4) {
      let program = (sec[p] << 8) | sec[p + 1];
      let pid = ((sec[p + 2] & 0x1f) << 8) | sec[p + 3];
      if (program !== 0) {
        if (pid !== this.pmtPid) {
          this.pmtPid = pid;
          this.pmtVersion = -1;
        }
        return;
      }
    }
  }

  parsePmt(payload) {
    let sec = this.section(payload);
    if (!sec || sec[0] !== 0x02) return;
    let version = (sec[5] >> 1) & 0x1f;
    if (version === this.pmtVersion) return;
    this.pmtVersion = version;

    let end = sec.length - 4;
    let programInfoLength = ((sec[10] & 0x0f) << 8) | sec[11];
    let p = 12 + programInfoLength;
    while (p + 5 <= end) {
      let streamType = sec[p];
      let pid = ((sec[p + 1] & 0x1f) << 8) | sec[p + 2];
      let esInfoLength = ((sec[p + 3] & 0x0f) << 8) | sec[p + 4];
      p += 5 + esInfoLength;

      let codec = STREAM_TYPES[streamType];
      if (!codec) {
        if (!this.unsupported.has(pid)) {
          this.unsupported.add(pid);
          this.emit('unsupported', { pid, streamType });
        }
        continue;
      }
      let old = this.streams.get(pid);
      if (old && old.codec === codec) continue;
      this.streams.set(pid, {
        pid,
        codec,
        chunks: [],
        size: 0,
        expected: 0,
        pts: 0,
        dts: 0,
        ref: null
      });
    }
  }

  onPesPayload(stream, pusi, payload) {
    if (pusi) {
      this.emitPes(stream);
      if (payload.length < 6 || payload[0] !== 0 || payload[1] !== 0 || payload[2] !== 1) return;
      let pesLength = (payload[4] << 8) | payload[5];
      stream.expected = pesLength > 0 ? pesLength + 6 : 0;
    } else if (stream.size === 0) {
      return; // no PES start seen yet
    }
    stream.chunks.push(Buffer.from(payload));
    stream.size += payload.length;

    // Bounded PES (mostly audio): emit as soon as it is complete instead of
    // waiting for the next PUSI, which saves one frame of latency.
    if (stream.expected > 0 && stream.size >= stream.expected) {
      this.emitPes(stream);
    }
  }

  emitPes(stream) {
    if (stream.size === 0) return;
    let pes = stream.chunks.length === 1 ? stream.chunks[0] : Buffer.concat(stream.chunks, stream.size);
    if (stream.expected > 0 && pes.length > stream.expected) {
      pes = pes.subarray(0, stream.expected);
    }
    stream.chunks = [];
    stream.size = 0;
    stream.expected = 0;

    if (pes.length < 9) return;
    let flags = pes[7];
    let headerLength = pes[8];
    let p = 9 + headerLength;
    if (p > pes.length) return;

    let ptsDtsFlags = flags >> 6;
    if (ptsDtsFlags & 0x02) {
      let rawPts = readTimestamp(pes, 9);
      let rawDts = ptsDtsFlags === 0x03 && headerLength >= 10 ? readTimestamp(pes, 14) : rawPts;
      let dts = this.unwrap(rawDts, stream.ref);
      let pts = this.unwrap(rawPts, dts);
      stream.pts = pts;
      stream.dts = dts;
      stream.ref = dts;
    }

    this.emit('pes', {
      codec: stream.codec,
      pid: stream.pid,
      pts: stream.pts,
      dts: stream.dts,
      data: pes.subarray(p)
    });
  }

  // Map a raw 33-bit timestamp onto the stream's continuous timeline so that
  // wrapping back to 0 keeps increasing.
  unwrap(raw, ref) {
    if (ref === null) return raw;
    let v = raw + Math.floor(ref / PTS_WRAP) * PTS_WRAP;
    if (v - ref > PTS_HALF) {
      v -= PTS_WRAP;
    } else if (ref - v > PTS_HALF) {
      v += PTS_WRAP;
    }
    return v;
  }
}

function readTimestamp(b, p) {
  return (
    (b[p] & 0x0e) * 0x20000000 + // bits 32..30, already shifted left by 1
    ((b[p + 1] << 22) | ((b[p + 2] & 0xfe) << 14) | (b[p + 3] << 7) | (b[p + 4] >> 1))
  );
}

module.exports = NodeSrtTsDemuxer;
