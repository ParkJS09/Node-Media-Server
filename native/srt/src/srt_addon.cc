//
//  libsrt listener addon for Node-Media-Server SRT ingest.
//
//  One worker thread per listener runs a srt_epoll_uwait loop and forwards
//  accept / data / close / error events to JS through a threadsafe function.
//

#include <node_api.h>
#include <srt/srt.h>
#include <srt/version.h>

#include <atomic>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <syslog.h>

namespace {

constexpr int kEpollTimeoutMs = 50;
constexpr size_t kDataChunk = 64 * 1024;
constexpr int kMaxEvents = 64;
constexpr int kRecvBufSize = 1500;

#define NAPI_CALL(env, call)                                        \
  do {                                                              \
    if ((call) != napi_ok) {                                        \
      const napi_extended_error_info* info = nullptr;               \
      napi_get_last_error_info((env), &info);                       \
      bool pending = false;                                         \
      napi_is_exception_pending((env), &pending);                   \
      if (!pending) {                                               \
        napi_throw_error((env), nullptr,                            \
                         info && info->error_message                \
                             ? info->error_message                  \
                             : "N-API call failed");                \
      }                                                             \
      return nullptr;                                               \
    }                                                               \
  } while (0)

enum class EventType { Accept, Data, Close, Error };

struct Event {
  explicit Event(EventType t) : type(t) {}
  EventType type;
  SRTSOCKET sock = SRT_INVALID_SOCK;
  std::string streamid;
  std::string ip;
  int port = 0;
  std::vector<char> data;
  std::string message;
};

struct ListenerOptions {
  std::string host = "0.0.0.0";
  int port = 0;
  int latency = -1;
  std::string passphrase;
  int pbkeylen = 0;
  int rcvbuf = 0;
};

struct Listener {
  uint32_t id = 0;
  ListenerOptions opts;
  SRTSOCKET lsock = SRT_INVALID_SOCK;
  napi_threadsafe_function tsfn = nullptr;
  std::thread worker;
  std::atomic<bool> stopping{false};
  std::mutex closeMutex;
  std::deque<SRTSOCKET> closeQueue;
};

std::mutex gMutex;
std::map<uint32_t, std::shared_ptr<Listener>> gListeners;
std::map<SRTSOCKET, uint32_t> gSockOwner;
// libsrt has no pktRcvRetransTotal; keep the count flushed by clear=true calls.
std::map<SRTSOCKET, int64_t> gRetransCleared;
uint32_t gNextId = 1;
bool gStarted = false;

void Emit(Listener* l, Event* ev) {
  if (napi_call_threadsafe_function(l->tsfn, ev, napi_tsfn_blocking) != napi_ok) {
    delete ev;
  }
}

void EmitError(Listener* l, const std::string& msg, SRTSOCKET sock = SRT_INVALID_SOCK) {
  auto* ev = new Event(EventType::Error);
  ev->sock = sock;
  ev->message = msg;
  Emit(l, ev);
}

std::string LastError() {
  return srt_getlasterror_str();
}

void AddrToString(const sockaddr_storage& ss, std::string* ip, int* port) {
  char buf[INET6_ADDRSTRLEN] = {0};
  if (ss.ss_family == AF_INET) {
    auto* a = reinterpret_cast<const sockaddr_in*>(&ss);
    inet_ntop(AF_INET, &a->sin_addr, buf, sizeof(buf));
    *port = ntohs(a->sin_port);
  } else if (ss.ss_family == AF_INET6) {
    auto* a = reinterpret_cast<const sockaddr_in6*>(&ss);
    inet_ntop(AF_INET6, &a->sin6_addr, buf, sizeof(buf));
    *port = ntohs(a->sin6_port);
  }
  *ip = buf;
}

void CloseClient(Listener* l, int eid, std::set<SRTSOCKET>* clients, SRTSOCKET sock) {
  if (clients->erase(sock) == 0) return;
  srt_epoll_remove_usock(eid, sock);
  srt_close(sock);
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gSockOwner.erase(sock);
    gRetransCleared.erase(sock);
  }
  auto* ev = new Event(EventType::Close);
  ev->sock = sock;
  Emit(l, ev);
}

// Runs on the JS thread so bind/listen failures surface as exceptions.
bool SetupListenSocket(const ListenerOptions& o, SRTSOCKET* out, std::string* err) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_PASSIVE | AI_NUMERICSERV;
  addrinfo* res = nullptr;
  std::string port = std::to_string(o.port);
  int gai = getaddrinfo(o.host.empty() ? nullptr : o.host.c_str(), port.c_str(), &hints, &res);
  if (gai != 0 || res == nullptr) {
    *err = std::string("resolve ") + o.host + " failed: " + gai_strerror(gai);
    return false;
  }

  SRTSOCKET s = srt_create_socket();
  if (s == SRT_INVALID_SOCK) {
    freeaddrinfo(res);
    *err = "srt_create_socket: " + LastError();
    return false;
  }

  bool no = false;
  srt_setsockflag(s, SRTO_RCVSYN, &no, sizeof(no));
  if (o.latency >= 0) {
    srt_setsockflag(s, SRTO_LATENCY, &o.latency, sizeof(o.latency));
  }
  if (o.rcvbuf > 0) {
    srt_setsockflag(s, SRTO_RCVBUF, &o.rcvbuf, sizeof(o.rcvbuf));
  }
  if (!o.passphrase.empty()) {
    if (srt_setsockflag(s, SRTO_PASSPHRASE, o.passphrase.c_str(), (int)o.passphrase.size()) == SRT_ERROR) {
      freeaddrinfo(res);
      *err = "invalid passphrase (must be 10..79 chars): " + LastError();
      srt_close(s);
      return false;
    }
    if (o.pbkeylen > 0 && srt_setsockflag(s, SRTO_PBKEYLEN, &o.pbkeylen, sizeof(o.pbkeylen)) == SRT_ERROR) {
      freeaddrinfo(res);
      *err = "invalid pbkeylen (16, 24 or 32): " + LastError();
      srt_close(s);
      return false;
    }
  }

  int rc = srt_bind(s, res->ai_addr, (int)res->ai_addrlen);
  freeaddrinfo(res);
  if (rc == SRT_ERROR) {
    *err = "srt_bind " + o.host + ":" + port + ": " + LastError();
    srt_close(s);
    return false;
  }
  if (srt_listen(s, 16) == SRT_ERROR) {
    *err = "srt_listen: " + LastError();
    srt_close(s);
    return false;
  }
  *out = s;
  return true;
}

void AcceptOne(Listener* l, int eid, SRTSOCKET lsock, std::set<SRTSOCKET>* clients) {
  // Exactly one srt_accept per epoll event: looping until failure makes
  // libsrt log an EASYNCRCV error for every drained listener.
  sockaddr_storage ss{};
  int len = sizeof(ss);
  SRTSOCKET s = srt_accept(lsock, reinterpret_cast<sockaddr*>(&ss), &len);
  if (s == SRT_INVALID_SOCK) {
    if (srt_getlasterror(nullptr) != SRT_EASYNCRCV) {
      EmitError(l, "srt_accept: " + LastError());
    }
    return;
  }

  bool no = false;
  srt_setsockflag(s, SRTO_RCVSYN, &no, sizeof(no));

  char sid[513] = {0};
  int sidLen = sizeof(sid) - 1;
  if (srt_getsockflag(s, SRTO_STREAMID, sid, &sidLen) == SRT_ERROR) sidLen = 0;

  int events = SRT_EPOLL_IN | SRT_EPOLL_ERR;
  if (srt_epoll_add_usock(eid, s, &events) == SRT_ERROR) {
    EmitError(l, "srt_epoll_add_usock: " + LastError());
    srt_close(s);
    return;
  }
  clients->insert(s);
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gSockOwner[s] = l->id;
    gRetransCleared[s] = 0;
  }

  auto* ev = new Event(EventType::Accept);
  ev->sock = s;
  ev->streamid.assign(sid, sidLen);
  AddrToString(ss, &ev->ip, &ev->port);
  Emit(l, ev);
}

void FlushData(Listener* l, SRTSOCKET sock, std::vector<char>* pending) {
  if (pending->empty()) return;
  auto* ev = new Event(EventType::Data);
  ev->sock = sock;
  ev->data.swap(*pending);
  pending->reserve(kDataChunk);
  Emit(l, ev);
}

// Returns false when the connection is gone.
bool ReadAll(Listener* l, SRTSOCKET sock) {
  char buf[kRecvBufSize];
  std::vector<char> pending;
  pending.reserve(kDataChunk);
  for (;;) {
    int n = srt_recvmsg(sock, buf, sizeof(buf));
    if (n == SRT_ERROR) {
      bool again = srt_getlasterror(nullptr) == SRT_EASYNCRCV;
      FlushData(l, sock, &pending);
      return again;
    }
    if (n == 0) continue;
    pending.insert(pending.end(), buf, buf + n);
    if (pending.size() >= kDataChunk) FlushData(l, sock, &pending);
  }
}

void WorkerMain(Listener* l) {
  SRTSOCKET lsock = l->lsock;
  int eid = srt_epoll_create();
  int lev = SRT_EPOLL_IN | SRT_EPOLL_ERR;
  srt_epoll_add_usock(eid, lsock, &lev);

  std::set<SRTSOCKET> clients;
  SRT_EPOLL_EVENT events[kMaxEvents];

  while (!l->stopping.load()) {
    std::deque<SRTSOCKET> toClose;
    {
      std::lock_guard<std::mutex> lock(l->closeMutex);
      toClose.swap(l->closeQueue);
    }
    for (SRTSOCKET s : toClose) CloseClient(l, eid, &clients, s);

    int n = srt_epoll_uwait(eid, events, kMaxEvents, kEpollTimeoutMs);
    for (int i = 0; i < n; i++) {
      SRTSOCKET s = events[i].fd;
      int e = events[i].events;
      if (s == lsock) {
        if (e & SRT_EPOLL_ERR) {
          EmitError(l, "listener socket error: " + LastError());
          l->stopping.store(true);
          break;
        }
        AcceptOne(l, eid, lsock, &clients);
        continue;
      }
      if (clients.count(s) == 0) continue;
      bool alive = true;
      if (e & SRT_EPOLL_IN) alive = ReadAll(l, s);
      if (!alive || (e & SRT_EPOLL_ERR) || srt_getsockstate(s) > SRTS_CONNECTED) {
        CloseClient(l, eid, &clients, s);
      }
    }
  }

  std::set<SRTSOCKET> remaining = clients;
  for (SRTSOCKET s : remaining) CloseClient(l, eid, &clients, s);
  srt_epoll_release(eid);
  srt_close(lsock);
  napi_release_threadsafe_function(l->tsfn, napi_tsfn_release);
}

void SetNamed(napi_env env, napi_value obj, const char* key, napi_value v) {
  napi_set_named_property(env, obj, key, v);
}

void SetString(napi_env env, napi_value obj, const char* key, const std::string& s) {
  napi_value v;
  napi_create_string_utf8(env, s.data(), s.size(), &v);
  SetNamed(env, obj, key, v);
}

void SetDouble(napi_env env, napi_value obj, const char* key, double d) {
  napi_value v;
  napi_create_double(env, d, &v);
  SetNamed(env, obj, key, v);
}

void CallJs(napi_env env, napi_value cb, void* /*context*/, void* data) {
  std::unique_ptr<Event> ev(static_cast<Event*>(data));
  if (env == nullptr || cb == nullptr) return;

  static const char* kNames[] = {"accept", "data", "close", "error"};
  napi_value obj;
  napi_create_object(env, &obj);
  SetString(env, obj, "type", kNames[static_cast<int>(ev->type)]);
  if (ev->sock != SRT_INVALID_SOCK) SetDouble(env, obj, "sock", ev->sock);

  switch (ev->type) {
    case EventType::Accept:
      SetString(env, obj, "streamid", ev->streamid);
      SetString(env, obj, "ip", ev->ip);
      SetDouble(env, obj, "port", ev->port);
      break;
    case EventType::Data: {
      napi_value buf;
      void* out = nullptr;
      napi_create_buffer_copy(env, ev->data.size(), ev->data.data(), &out, &buf);
      SetNamed(env, obj, "data", buf);
      break;
    }
    case EventType::Error:
      SetString(env, obj, "message", ev->message);
      break;
    case EventType::Close:
      break;
  }

  napi_value global;
  napi_get_global(env, &global);
  napi_call_function(env, global, cb, 1, &obj, nullptr);
}

void StopListener(const std::shared_ptr<Listener>& l) {
  l->stopping.store(true);
  if (l->worker.joinable()) l->worker.join();
}

void Cleanup(void* /*arg*/) {
  std::map<uint32_t, std::shared_ptr<Listener>> all;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    all.swap(gListeners);
  }
  for (auto& it : all) StopListener(it.second);
  if (gStarted) {
    srt_cleanup();
    gStarted = false;
  }
}

bool GetProp(napi_env env, napi_value obj, const char* key, napi_value* out) {
  bool has = false;
  if (napi_has_named_property(env, obj, key, &has) != napi_ok || !has) return false;
  napi_get_named_property(env, obj, key, out);
  napi_valuetype t;
  napi_typeof(env, *out, &t);
  return t != napi_undefined && t != napi_null;
}

bool GetInt(napi_env env, napi_value obj, const char* key, int* out) {
  napi_value v;
  if (!GetProp(env, obj, key, &v)) return false;
  return napi_get_value_int32(env, v, out) == napi_ok;
}

bool GetStr(napi_env env, napi_value obj, const char* key, std::string* out) {
  napi_value v;
  if (!GetProp(env, obj, key, &v)) return false;
  size_t len = 0;
  if (napi_get_value_string_utf8(env, v, nullptr, 0, &len) != napi_ok) return false;
  out->resize(len);
  napi_get_value_string_utf8(env, v, &(*out)[0], len + 1, &len);
  return true;
}

bool GetSock(napi_env env, napi_value v, SRTSOCKET* out) {
  int32_t s;
  if (napi_get_value_int32(env, v, &s) != napi_ok) return false;
  *out = s;
  return true;
}

// listen(options, callback) -> listener id
napi_value Listen(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr));
  napi_valuetype t0, t1;
  if (argc < 2 || napi_typeof(env, argv[0], &t0) != napi_ok || napi_typeof(env, argv[1], &t1) != napi_ok ||
      t0 != napi_object || t1 != napi_function) {
    napi_throw_type_error(env, nullptr, "listen(options, callback) expected");
    return nullptr;
  }

  auto l = std::make_shared<Listener>();
  GetStr(env, argv[0], "host", &l->opts.host);
  if (!GetInt(env, argv[0], "port", &l->opts.port) || l->opts.port <= 0 || l->opts.port > 65535) {
    napi_throw_range_error(env, nullptr, "options.port must be 1..65535");
    return nullptr;
  }
  GetInt(env, argv[0], "latency", &l->opts.latency);
  GetStr(env, argv[0], "passphrase", &l->opts.passphrase);
  GetInt(env, argv[0], "pbkeylen", &l->opts.pbkeylen);
  GetInt(env, argv[0], "rcvbuf", &l->opts.rcvbuf);

  {
    std::lock_guard<std::mutex> lock(gMutex);
    if (!gStarted) {
      srt_startup();
      srt_setloglevel(LOG_ERR);
      gStarted = true;
      napi_add_env_cleanup_hook(env, Cleanup, nullptr);
    }
    l->id = gNextId++;
  }

  std::string err;
  if (!SetupListenSocket(l->opts, &l->lsock, &err)) {
    napi_throw_error(env, nullptr, err.c_str());
    return nullptr;
  }

  napi_value name;
  NAPI_CALL(env, napi_create_string_utf8(env, "srt-listener", NAPI_AUTO_LENGTH, &name));
  NAPI_CALL(env, napi_create_threadsafe_function(env, argv[1], nullptr, name, 0, 1, nullptr, nullptr,
                                                 nullptr, CallJs, &l->tsfn));
  {
    std::lock_guard<std::mutex> lock(gMutex);
    gListeners[l->id] = l;
  }
  l->worker = std::thread(WorkerMain, l.get());

  napi_value id;
  NAPI_CALL(env, napi_create_uint32(env, l->id, &id));
  return id;
}

// close(listenerId): stops the worker; every open client gets a close event.
napi_value Close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr));
  uint32_t id = 0;
  if (argc < 1 || napi_get_value_uint32(env, argv[0], &id) != napi_ok) {
    napi_throw_type_error(env, nullptr, "close(listenerId) expected");
    return nullptr;
  }
  std::shared_ptr<Listener> l;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    auto it = gListeners.find(id);
    if (it != gListeners.end()) {
      l = it->second;
      gListeners.erase(it);
    }
  }
  if (l) StopListener(l);
  napi_value r;
  napi_get_boolean(env, l != nullptr, &r);
  return r;
}

// closeSocket(sock): queued and closed on the owning worker thread.
napi_value CloseSocket(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr));
  SRTSOCKET sock;
  if (argc < 1 || !GetSock(env, argv[0], &sock)) {
    napi_throw_type_error(env, nullptr, "closeSocket(sock) expected");
    return nullptr;
  }
  std::shared_ptr<Listener> l;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    auto owner = gSockOwner.find(sock);
    if (owner != gSockOwner.end()) {
      auto it = gListeners.find(owner->second);
      if (it != gListeners.end()) l = it->second;
    }
  }
  if (l) {
    std::lock_guard<std::mutex> lock(l->closeMutex);
    l->closeQueue.push_back(sock);
  }
  napi_value r;
  napi_get_boolean(env, l != nullptr, &r);
  return r;
}

// stats(sock, clear) -> object | null
napi_value Stats(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr));
  SRTSOCKET sock;
  if (argc < 1 || !GetSock(env, argv[0], &sock)) {
    napi_throw_type_error(env, nullptr, "stats(sock, clear) expected");
    return nullptr;
  }
  bool clear = false;
  if (argc > 1) napi_get_value_bool(env, argv[1], &clear);

  napi_value result;
  SRT_TRACEBSTATS s;
  std::memset(&s, 0, sizeof(s));
  if (srt_bistats(sock, &s, clear ? 1 : 0, 1) == SRT_ERROR) {
    napi_get_null(env, &result);
    return result;
  }

  int64_t retransTotal = s.pktRcvRetrans;
  {
    std::lock_guard<std::mutex> lock(gMutex);
    auto it = gRetransCleared.find(sock);
    if (it != gRetransCleared.end()) {
      retransTotal += it->second;
      if (clear) it->second += s.pktRcvRetrans;
    }
  }

  NAPI_CALL(env, napi_create_object(env, &result));
  SetDouble(env, result, "msRTT", s.msRTT);
  SetDouble(env, result, "mbpsRecvRate", s.mbpsRecvRate);
  SetDouble(env, result, "mbpsBandwidth", s.mbpsBandwidth);
  SetDouble(env, result, "msRcvBuf", s.msRcvBuf);
  SetDouble(env, result, "pktRcvLoss", s.pktRcvLoss);
  SetDouble(env, result, "pktRcvLossTotal", s.pktRcvLossTotal);
  SetDouble(env, result, "pktRcvDrop", s.pktRcvDrop);
  SetDouble(env, result, "pktRcvDropTotal", s.pktRcvDropTotal);
  SetDouble(env, result, "pktRcvRetrans", s.pktRcvRetrans);
  SetDouble(env, result, "pktRcvRetransTotal", (double)retransTotal);
  SetDouble(env, result, "byteRecvTotal", (double)s.byteRecvTotal);
  return result;
}

napi_value Init(napi_env env, napi_value exports) {
  napi_property_descriptor props[] = {
      {"listen", nullptr, Listen, nullptr, nullptr, nullptr, napi_default_jsproperty, nullptr},
      {"close", nullptr, Close, nullptr, nullptr, nullptr, napi_default_jsproperty, nullptr},
      {"closeSocket", nullptr, CloseSocket, nullptr, nullptr, nullptr, napi_default_jsproperty, nullptr},
      {"stats", nullptr, Stats, nullptr, nullptr, nullptr, napi_default_jsproperty, nullptr},
  };
  NAPI_CALL(env, napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props));
  napi_value ver;
  NAPI_CALL(env, napi_create_string_utf8(env, SRT_VERSION_STRING, NAPI_AUTO_LENGTH, &ver));
  NAPI_CALL(env, napi_set_named_property(env, exports, "srtVersion", ver));
  return exports;
}

}  // namespace

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
