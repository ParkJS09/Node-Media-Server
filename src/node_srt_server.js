//
//  SRT ingest server backed by the libsrt N-API addon in native/srt.
//
const Logger = require('./node_core_logger');
const NodeSrtSession = require('./node_srt_session');
const context = require('./node_core_ctx');

const SRT_PORT = 9000;

class NodeSrtServer {
  constructor(config) {
    this.config = config;
    config.srt.port = this.port = config.srt.port ? config.srt.port : SRT_PORT;
    config.srt.app = config.srt.app || 'live';
    this.sessions = new Map();
    this.srt = null;
    this.listenerId = 0;
  }

  run() {
    try {
      this.srt = require('../native/srt');
    } catch (e) {
      Logger.error(`Node Media Srt Server startup failed. The libsrt addon could not be loaded: ${e.message.split('\n')[0]}`);
      Logger.error('Build it with `npm run build:srt` (needs cmake, a C++17 compiler and the libsrt source in $SRT_SRC, default ~/GithubProject/srt).');
      return;
    }

    let srtConfig = this.config.srt;
    try {
      this.listenerId = this.srt.listen({
        host: srtConfig.host || '0.0.0.0',
        port: this.port,
        latency: srtConfig.latency,
        passphrase: srtConfig.passphrase,
        pbkeylen: srtConfig.pbkeylen,
        rcvbuf: srtConfig.rcvbuf
      }, this.onEvent.bind(this));
    } catch (e) {
      Logger.error(`Node Media Srt Server ${e.message}`);
      return;
    }
    Logger.log(`Node Media Srt Server started on port: ${this.port} (libsrt ${this.srt.srtVersion}${srtConfig.passphrase ? ', encrypted' : ''})`);
  }

  onEvent(ev) {
    switch (ev.type) {
      case 'accept': {
        context.stat.accepted++;
        let session = new NodeSrtSession(this.config, this.srt, ev);
        this.sessions.set(ev.sock, session);
        session.run();
        break;
      }
      case 'data': {
        let session = this.sessions.get(ev.sock);
        if (session) session.onSrtData(ev.data);
        break;
      }
      case 'close': {
        let session = this.sessions.get(ev.sock);
        if (session) {
          this.sessions.delete(ev.sock);
          context.stat.inbytes += session.socket.bytesRead;
          session.onSrtClose();
        }
        break;
      }
      case 'error':
        Logger.error(`Node Media Srt Server ${ev.message}${ev.sock !== undefined ? ` sock=${ev.sock}` : ''}`);
        break;
    }
  }

  stop() {
    for (let session of this.sessions.values()) {
      session.stop();
    }
    this.sessions.clear();
    if (this.srt && this.listenerId) {
      this.srt.close(this.listenerId);
      this.listenerId = 0;
      Logger.log('Node Media Srt Server Close.');
    }
  }
}

module.exports = NodeSrtServer;
