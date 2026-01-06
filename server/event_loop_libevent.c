#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <event2/event.h>
#include <event2/util.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include "log.h"
#include "util.h"
#include "event_loop.h"

enum {
  MELIAN_BEV_OPTS = (BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS),
};

typedef struct LoopSignal {
  struct event* ev;
  event_loop_signal_cb cb;
  void* ctx;
  struct LoopSignal* next;
} LoopSignal;

typedef struct LoopTimer {
  struct event* ev;
  event_loop_timer_cb cb;
  void* ctx;
} LoopTimer;

struct EventLoop {
  struct event_base* base;
  struct evconnlistener* listener;
  event_loop_accept_cb accept_cb;
  void* accept_ctx;
  LoopSignal* signals;
};

struct EventConn {
  struct bufferevent* bev;
  event_loop_read_cb read_cb;
  event_loop_event_cb event_cb;
  void* ctx;
};

static void on_accept(struct evconnlistener* lev, evutil_socket_t fd,
                      struct sockaddr* addr, int socklen, void* arg) {
  UNUSED(lev);
  UNUSED(addr);
  UNUSED(socklen);
  EventLoop* loop = arg;
  if (loop->accept_cb) loop->accept_cb(fd, loop->accept_ctx);
}

static void on_read(struct bufferevent* bev, void* ctx) {
  EventConn* conn = ctx;
  UNUSED(bev);
  if (conn->read_cb) conn->read_cb(conn, conn->ctx);
}

static void on_event(struct bufferevent* bev, short events, void* ctx) {
  EventConn* conn = ctx;
  UNUSED(bev);
  unsigned out = 0;
  if (events & BEV_EVENT_EOF) out |= EVENT_LOOP_EVENT_EOF;
  if (events & BEV_EVENT_ERROR) out |= EVENT_LOOP_EVENT_ERROR;
  if (out && conn->event_cb) conn->event_cb(conn, conn->ctx, out);
}

static void on_signal(evutil_socket_t fd, short what, void* arg) {
  LoopSignal* sig = arg;
  UNUSED(fd);
  UNUSED(what);
  if (sig->cb) sig->cb(event_get_signal(sig->ev), sig->ctx);
}

static void on_timer(evutil_socket_t fd, short what, void* arg) {
  LoopTimer* timer = arg;
  UNUSED(fd);
  UNUSED(what);
  if (timer->cb) timer->cb(timer->ctx);
  if (timer->ev) event_free(timer->ev);
  free(timer);
}

EventLoop* event_loop_build(void) {
  EventLoop* loop = calloc(1, sizeof(EventLoop));
  if (!loop) return NULL;
  loop->base = event_base_new();
  if (!loop->base) {
    free(loop);
    return NULL;
  }
  return loop;
}

void event_loop_destroy(EventLoop* loop) {
  if (!loop) return;
  if (loop->listener) evconnlistener_free(loop->listener);
  LoopSignal* sig = loop->signals;
  while (sig) {
    LoopSignal* next = sig->next;
    if (sig->ev) event_free(sig->ev);
    free(sig);
    sig = next;
  }
  if (loop->base) event_base_free(loop->base);
  free(loop);
}

int event_loop_listen_unix(EventLoop* loop, const char* path,
                           event_loop_accept_cb cb, void* ctx) {
  if (!loop || !loop->base || !path || !path[0]) return 0;
  loop->accept_cb = cb;
  loop->accept_ctx = ctx;

  struct sockaddr_un sun;
  memset(&sun, 0, sizeof(sun));
  sun.sun_family = AF_UNIX;
  int wrote = snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", path);
  if (wrote < 0 || (size_t)wrote >= sizeof(sun.sun_path)) {
    errno = ENOMEM;
    LOG_FATAL("UNIX socket path '%s' exceeds %zu bytes", path, sizeof(sun.sun_path) - 1);
  }

  loop->listener = evconnlistener_new_bind(loop->base, on_accept, loop,
                                           LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
                                           -1, (struct sockaddr*)&sun, sizeof(sun));
  return loop->listener != NULL;
}

int event_loop_listen_tcp(EventLoop* loop, const char* host, unsigned port,
                          event_loop_accept_cb cb, void* ctx) {
  if (!loop || !loop->base || !host || !host[0] || !port) return 0;
  loop->accept_cb = cb;
  loop->accept_ctx = ctx;

  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons((uint16_t)port);
  inet_pton(AF_INET, host, &sin.sin_addr);
  unsigned flags = LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE | LEV_OPT_REUSEABLE_PORT;
  loop->listener = evconnlistener_new_bind(loop->base, on_accept, loop, flags,
                                           -1, (struct sockaddr*)&sin, sizeof(sin));
  return loop->listener != NULL;
}

int event_loop_run(EventLoop* loop) {
  if (!loop || !loop->base) return 0;
  return event_base_dispatch(loop->base);
}

void event_loop_stop(EventLoop* loop) {
  if (!loop || !loop->base) return;
  event_base_loopexit(loop->base, NULL);
}

EventConn* event_loop_conn_build(EventLoop* loop, int fd) {
  if (!loop || !loop->base || fd < 0) return NULL;
  EventConn* conn = calloc(1, sizeof(EventConn));
  if (!conn) return NULL;
  conn->bev = bufferevent_socket_new(loop->base, fd, MELIAN_BEV_OPTS);
  if (!conn->bev) {
    free(conn);
    return NULL;
  }
  bufferevent_setcb(conn->bev, on_read, NULL, on_event, conn);
  return conn;
}

void event_loop_conn_free(EventConn* conn) {
  if (!conn) return;
  if (conn->bev) bufferevent_free(conn->bev);
  free(conn);
}

void event_loop_conn_set_cb(EventConn* conn, event_loop_read_cb rcb,
                            event_loop_event_cb ecb, void* ctx) {
  if (!conn) return;
  conn->read_cb = rcb;
  conn->event_cb = ecb;
  conn->ctx = ctx;
}

void event_loop_conn_enable(EventConn* conn) {
  if (!conn || !conn->bev) return;
  bufferevent_enable(conn->bev, EV_READ | EV_WRITE);
}

size_t event_loop_conn_in_len(EventConn* conn) {
  if (!conn || !conn->bev) return 0;
  struct evbuffer* in = bufferevent_get_input(conn->bev);
  return evbuffer_get_length(in);
}

const uint8_t* event_loop_conn_in_peek(EventConn* conn, size_t len) {
  if (!conn || !conn->bev) return NULL;
  struct evbuffer* in = bufferevent_get_input(conn->bev);
  return (const uint8_t*)evbuffer_pullup(in, len);
}

void event_loop_conn_in_drain(EventConn* conn, size_t len) {
  if (!conn || !conn->bev) return;
  struct evbuffer* in = bufferevent_get_input(conn->bev);
  evbuffer_drain(in, len);
}

int event_loop_conn_out_add(EventConn* conn, const void* data, size_t len) {
  if (!conn || !conn->bev) return 0;
  struct evbuffer* out = bufferevent_get_output(conn->bev);
  return evbuffer_add(out, data, len) == 0;
}

int event_loop_conn_out_add_ref(EventConn* conn, const void* data, size_t len) {
  if (!conn || !conn->bev) return 0;
  struct evbuffer* out = bufferevent_get_output(conn->bev);
  return evbuffer_add_reference(out, data, len, NULL, NULL) == 0;
}

int event_loop_add_signal(EventLoop* loop, int signum,
                          event_loop_signal_cb cb, void* ctx) {
  if (!loop || !loop->base) return 0;
  LoopSignal* sig = calloc(1, sizeof(LoopSignal));
  if (!sig) return 0;
  sig->cb = cb;
  sig->ctx = ctx;
  sig->ev = evsignal_new(loop->base, signum, on_signal, sig);
  if (!sig->ev) {
    free(sig);
    return 0;
  }
  if (event_add(sig->ev, NULL) != 0) {
    event_free(sig->ev);
    free(sig);
    return 0;
  }
  sig->next = loop->signals;
  loop->signals = sig;
  return 1;
}

int event_loop_add_timer(EventLoop* loop, unsigned ms,
                         event_loop_timer_cb cb, void* ctx) {
  if (!loop || !loop->base) return 0;
  LoopTimer* timer = calloc(1, sizeof(LoopTimer));
  if (!timer) return 0;
  timer->cb = cb;
  timer->ctx = ctx;
  timer->ev = evtimer_new(loop->base, on_timer, timer);
  if (!timer->ev) {
    free(timer);
    return 0;
  }
  struct timeval tv = { (time_t)(ms / 1000), (suseconds_t)((ms % 1000) * 1000) };
  if (evtimer_add(timer->ev, &tv) != 0) {
    event_free(timer->ev);
    free(timer);
    return 0;
  }
  return 1;
}

const char* event_loop_backend_name(EventLoop* loop) {
  if (!loop || !loop->base) return "unknown";
  return event_base_get_method(loop->base);
}

const char* event_loop_backend_version(EventLoop* loop) {
  UNUSED(loop);
  return event_get_version();
}
