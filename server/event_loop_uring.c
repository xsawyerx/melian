#if defined(HAVE_URING)

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <liburing.h>
#include "util.h"
#include "log.h"
#include "event_loop.h"

enum {
  URING_QUEUE_DEPTH = 256,
  READ_CHUNK = 8192,
  MAX_IOV = 16,
};

enum OpType {
  OP_ACCEPT = 1,
  OP_READ,
  OP_WRITE,
  OP_SIGNAL,
  OP_TIMER,
  OP_WAKE,
};

typedef struct OpBase {
  int type;
} OpBase;

typedef struct Buffer {
  uint8_t* data;
  size_t len;
  size_t cap;
} Buffer;

typedef struct OutSeg {
  const uint8_t* data;
  uint8_t* owned;
  size_t len;
  size_t off;
  int owns;
  struct OutSeg* next;
} OutSeg;

struct EventLoop {
  struct io_uring ring;
  int running;
  int listen_fd;
  event_loop_accept_cb accept_cb;
  void* accept_ctx;
  int signal_fd;
  event_loop_signal_cb signal_cb;
  void* signal_ctx;
  int wake_fd;
};

struct EventConn {
  EventLoop* loop;
  int fd;
  int closing;
  unsigned refcount;
  int read_inflight;
  int write_inflight;
  int notified;
  Buffer in;
  OutSeg* out_head;
  OutSeg* out_tail;
  event_loop_read_cb read_cb;
  event_loop_event_cb event_cb;
  void* ctx;
  uint8_t read_buf[READ_CHUNK];
};

typedef struct AcceptOp {
  OpBase base;
  EventLoop* loop;
  struct sockaddr_storage addr;
  socklen_t addrlen;
} AcceptOp;

typedef struct ReadOp {
  OpBase base;
  EventConn* conn;
} ReadOp;

typedef struct WriteOp {
  OpBase base;
  EventConn* conn;
  struct iovec* iov;
  unsigned iovcnt;
} WriteOp;

typedef struct SignalOp {
  OpBase base;
  EventLoop* loop;
  int fd;
  struct signalfd_siginfo info;
} SignalOp;

typedef struct TimerOp {
  OpBase base;
  EventLoop* loop;
  int fd;
  uint64_t ticks;
  event_loop_timer_cb cb;
  void* ctx;
} TimerOp;

typedef struct WakeOp {
  OpBase base;
  EventLoop* loop;
  int fd;
  uint64_t value;
} WakeOp;

static int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return 0;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

static int buffer_reserve(Buffer* buf, size_t cap) {
  if (cap <= buf->cap) return 1;
  size_t newcap = buf->cap ? buf->cap : 256;
  while (newcap < cap) newcap *= 2;
  uint8_t* tmp = realloc(buf->data, newcap);
  if (!tmp) return 0;
  buf->data = tmp;
  buf->cap = newcap;
  return 1;
}

static int buffer_append(Buffer* buf, const void* data, size_t len) {
  if (!len) return 1;
  if (!buffer_reserve(buf, buf->len + len)) return 0;
  memcpy(buf->data + buf->len, data, len);
  buf->len += len;
  return 1;
}

static void buffer_drain(Buffer* buf, size_t len) {
  if (len >= buf->len) {
    buf->len = 0;
    return;
  }
  memmove(buf->data, buf->data + len, buf->len - len);
  buf->len -= len;
}

static void outseg_free(OutSeg* seg) {
  if (!seg) return;
  if (seg->owns && seg->owned) free(seg->owned);
  free(seg);
}

static void conn_ref(EventConn* conn) {
  if (conn) conn->refcount++;
}

static void conn_unref(EventConn* conn) {
  if (!conn) return;
  if (conn->refcount) conn->refcount--;
  if (conn->closing && conn->refcount == 0) {
    while (conn->out_head) {
      OutSeg* next = conn->out_head->next;
      outseg_free(conn->out_head);
      conn->out_head = next;
    }
    free(conn->in.data);
    free(conn);
  }
}

static void submit_accept(EventLoop* loop) {
  if (!loop || loop->listen_fd < 0) return;
  AcceptOp* op = calloc(1, sizeof(AcceptOp));
  if (!op) return;
  op->base.type = OP_ACCEPT;
  op->loop = loop;
  op->addrlen = sizeof(op->addr);
  struct io_uring_sqe* sqe = io_uring_get_sqe(&loop->ring);
  if (!sqe) {
    free(op);
    return;
  }
  io_uring_prep_accept(sqe, loop->listen_fd,
                       (struct sockaddr*)&op->addr, &op->addrlen, 0);
  io_uring_sqe_set_data(sqe, op);
}

static void submit_read(EventConn* conn) {
  if (!conn || conn->closing || conn->read_inflight) return;
  ReadOp* op = calloc(1, sizeof(ReadOp));
  if (!op) return;
  op->base.type = OP_READ;
  op->conn = conn;
  struct io_uring_sqe* sqe = io_uring_get_sqe(&conn->loop->ring);
  if (!sqe) {
    free(op);
    return;
  }
  io_uring_prep_recv(sqe, conn->fd, conn->read_buf, sizeof(conn->read_buf), 0);
  io_uring_sqe_set_data(sqe, op);
  conn->read_inflight = 1;
  conn_ref(conn);
}

static unsigned build_iov(EventConn* conn, struct iovec* iov, unsigned max_iov) {
  unsigned count = 0;
  for (OutSeg* seg = conn->out_head; seg && count < max_iov; seg = seg->next) {
    if (seg->off >= seg->len) continue;
    iov[count].iov_base = (void*)(seg->data + seg->off);
    iov[count].iov_len = seg->len - seg->off;
    count++;
  }
  return count;
}

static void submit_write(EventConn* conn) {
  if (!conn || conn->closing || conn->write_inflight) return;
  if (!conn->out_head) return;
  WriteOp* op = calloc(1, sizeof(WriteOp));
  if (!op) return;
  op->base.type = OP_WRITE;
  op->conn = conn;
  op->iov = calloc(MAX_IOV, sizeof(struct iovec));
  if (!op->iov) {
    free(op);
    return;
  }
  op->iovcnt = build_iov(conn, op->iov, MAX_IOV);
  if (!op->iovcnt) {
    free(op->iov);
    free(op);
    return;
  }
  struct io_uring_sqe* sqe = io_uring_get_sqe(&conn->loop->ring);
  if (!sqe) {
    free(op->iov);
    free(op);
    return;
  }
  io_uring_prep_writev(sqe, conn->fd, op->iov, op->iovcnt, 0);
  io_uring_sqe_set_data(sqe, op);
  conn->write_inflight = 1;
  conn_ref(conn);
}

static void submit_signal(EventLoop* loop) {
  if (!loop || loop->signal_fd < 0) return;
  SignalOp* op = calloc(1, sizeof(SignalOp));
  if (!op) return;
  op->base.type = OP_SIGNAL;
  op->loop = loop;
  op->fd = loop->signal_fd;
  struct io_uring_sqe* sqe = io_uring_get_sqe(&loop->ring);
  if (!sqe) {
    free(op);
    return;
  }
  io_uring_prep_read(sqe, op->fd, &op->info, sizeof(op->info), 0);
  io_uring_sqe_set_data(sqe, op);
}

static void submit_timer(EventLoop* loop, int fd, TimerOp* op) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&loop->ring);
  if (!sqe) return;
  io_uring_prep_read(sqe, fd, &op->ticks, sizeof(op->ticks), 0);
  io_uring_sqe_set_data(sqe, op);
}

static void submit_wake(EventLoop* loop) {
  if (!loop || loop->wake_fd < 0) return;
  WakeOp* op = calloc(1, sizeof(WakeOp));
  if (!op) return;
  op->base.type = OP_WAKE;
  op->loop = loop;
  op->fd = loop->wake_fd;
  struct io_uring_sqe* sqe = io_uring_get_sqe(&loop->ring);
  if (!sqe) {
    free(op);
    return;
  }
  io_uring_prep_read(sqe, op->fd, &op->value, sizeof(op->value), 0);
  io_uring_sqe_set_data(sqe, op);
}

EventLoop* event_loop_build(void) {
  EventLoop* loop = calloc(1, sizeof(EventLoop));
  if (!loop) return NULL;
  loop->listen_fd = -1;
  loop->signal_fd = -1;
  loop->wake_fd = -1;
  if (io_uring_queue_init(URING_QUEUE_DEPTH, &loop->ring, 0) < 0) {
    free(loop);
    return NULL;
  }
  loop->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (loop->wake_fd < 0) {
    io_uring_queue_exit(&loop->ring);
    free(loop);
    return NULL;
  }
  submit_wake(loop);
  return loop;
}

void event_loop_destroy(EventLoop* loop) {
  if (!loop) return;
  if (loop->listen_fd >= 0) close(loop->listen_fd);
  if (loop->signal_fd >= 0) close(loop->signal_fd);
  if (loop->wake_fd >= 0) close(loop->wake_fd);
  io_uring_queue_exit(&loop->ring);
  free(loop);
}

int event_loop_listen_unix(EventLoop* loop, const char* path,
                           event_loop_accept_cb cb, void* ctx) {
  if (!loop || !path || !path[0]) return 0;
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return 0;
  struct sockaddr_un sun;
  memset(&sun, 0, sizeof(sun));
  sun.sun_family = AF_UNIX;
  snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", path);
  if (bind(fd, (struct sockaddr*)&sun, sizeof(sun)) < 0) {
    close(fd);
    return 0;
  }
  if (listen(fd, SOMAXCONN) < 0) {
    close(fd);
    return 0;
  }
  set_nonblocking(fd);
  loop->listen_fd = fd;
  loop->accept_cb = cb;
  loop->accept_ctx = ctx;
  submit_accept(loop);
  return 1;
}

int event_loop_listen_tcp(EventLoop* loop, const char* host, unsigned port,
                          event_loop_accept_cb cb, void* ctx) {
  if (!loop || !host || !host[0] || !port) return 0;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;
  int yes = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons((uint16_t)port);
  inet_pton(AF_INET, host, &sin.sin_addr);
  if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
    close(fd);
    return 0;
  }
  if (listen(fd, SOMAXCONN) < 0) {
    close(fd);
    return 0;
  }
  set_nonblocking(fd);
  loop->listen_fd = fd;
  loop->accept_cb = cb;
  loop->accept_ctx = ctx;
  submit_accept(loop);
  return 1;
}

int event_loop_run(EventLoop* loop) {
  if (!loop) return 0;
  loop->running = 1;
  while (loop->running) {
    int ret = io_uring_submit(&loop->ring);
    if (ret < 0) {
      LOG_ERROR("io_uring_submit failed: %s", strerror(-ret));
    }
    struct io_uring_cqe* cqe = NULL;
    ret = io_uring_wait_cqe(&loop->ring, &cqe);
    if (ret < 0) {
      if (ret == -EINTR) continue;
      LOG_ERROR("io_uring_wait_cqe failed: %s", strerror(-ret));
      continue;
    }
    OpBase* base = io_uring_cqe_get_data(cqe);
    int res = cqe->res;
    if (!base) {
      io_uring_cqe_seen(&loop->ring, cqe);
      continue;
    }
    switch (base->type) {
      case OP_ACCEPT: {
        AcceptOp* op = (AcceptOp*)base;
        if (res >= 0) {
          if (loop->accept_cb) {
            loop->accept_cb(res, loop->accept_ctx);
          } else {
            close(res);
          }
        }
        free(op);
        if (loop->running) submit_accept(loop);
        break;
      }
      case OP_READ: {
        ReadOp* op = (ReadOp*)base;
        EventConn* conn = op->conn;
        conn->read_inflight = 0;
        if (res <= 0) {
          unsigned ev = (res == 0) ? EVENT_LOOP_EVENT_EOF : EVENT_LOOP_EVENT_ERROR;
          if (!conn->notified && conn->event_cb) {
            conn->event_cb(conn, conn->ctx, ev);
            conn->notified = 1;
          }
          conn->closing = 1;
        } else {
          if (!buffer_append(&conn->in, conn->read_buf, (size_t)res)) {
            if (conn->event_cb) conn->event_cb(conn, conn->ctx, EVENT_LOOP_EVENT_ERROR);
            conn->closing = 1;
          } else {
            if (conn->read_cb) conn->read_cb(conn, conn->ctx);
            submit_read(conn);
          }
        }
        free(op);
        conn_unref(conn);
        break;
      }
      case OP_WRITE: {
        WriteOp* op = (WriteOp*)base;
        EventConn* conn = op->conn;
        conn->write_inflight = 0;
        if (res < 0) {
          if (!conn->notified && conn->event_cb) {
            conn->event_cb(conn, conn->ctx, EVENT_LOOP_EVENT_ERROR);
            conn->notified = 1;
          }
          conn->closing = 1;
        } else {
          size_t remaining = (size_t)res;
          while (remaining && conn->out_head) {
            OutSeg* seg = conn->out_head;
            size_t avail = seg->len - seg->off;
            if (remaining >= avail) {
              remaining -= avail;
              conn->out_head = seg->next;
              if (!conn->out_head) conn->out_tail = NULL;
              outseg_free(seg);
            } else {
              seg->off += remaining;
              remaining = 0;
            }
          }
          submit_write(conn);
        }
        free(op->iov);
        free(op);
        conn_unref(conn);
        break;
      }
      case OP_SIGNAL: {
        SignalOp* op = (SignalOp*)base;
        if (res > 0 && loop->signal_cb) {
          loop->signal_cb((int)op->info.ssi_signo, loop->signal_ctx);
        }
        free(op);
        if (loop->running) submit_signal(loop);
        break;
      }
      case OP_TIMER: {
        TimerOp* op = (TimerOp*)base;
        if (op->cb) op->cb(op->ctx);
        close(op->fd);
        free(op);
        break;
      }
      case OP_WAKE: {
        WakeOp* op = (WakeOp*)base;
        loop->running = 0;
        free(op);
        break;
      }
      default:
        free(base);
        break;
    }
    io_uring_cqe_seen(&loop->ring, cqe);
  }
  return 0;
}

void event_loop_stop(EventLoop* loop) {
  if (!loop) return;
  loop->running = 0;
  if (loop->wake_fd >= 0) {
    uint64_t one = 1;
    write(loop->wake_fd, &one, sizeof(one));
  }
}

EventConn* event_loop_conn_build(EventLoop* loop, int fd) {
  if (!loop || fd < 0) return NULL;
  EventConn* conn = calloc(1, sizeof(EventConn));
  if (!conn) return NULL;
  conn->loop = loop;
  conn->fd = fd;
  set_nonblocking(fd);
  return conn;
}

void event_loop_conn_free(EventConn* conn) {
  if (!conn) return;
  conn->closing = 1;
  conn->notified = 1;
  conn->read_cb = NULL;
  conn->event_cb = NULL;
  if (conn->fd >= 0) {
    close(conn->fd);
    conn->fd = -1;
  }
  conn_unref(conn);
}

void event_loop_conn_set_cb(EventConn* conn, event_loop_read_cb rcb,
                            event_loop_event_cb ecb, void* ctx) {
  if (!conn) return;
  conn->read_cb = rcb;
  conn->event_cb = ecb;
  conn->ctx = ctx;
}

void event_loop_conn_enable(EventConn* conn) {
  if (!conn) return;
  submit_read(conn);
  submit_write(conn);
}

size_t event_loop_conn_in_len(EventConn* conn) {
  if (!conn) return 0;
  return conn->in.len;
}

const uint8_t* event_loop_conn_in_peek(EventConn* conn, size_t len) {
  if (!conn || conn->in.len < len) return NULL;
  return conn->in.data;
}

void event_loop_conn_in_drain(EventConn* conn, size_t len) {
  if (!conn) return;
  buffer_drain(&conn->in, len);
}

int event_loop_conn_out_add(EventConn* conn, const void* data, size_t len) {
  if (!conn || conn->closing || !data || !len) return 0;
  OutSeg* seg = calloc(1, sizeof(OutSeg));
  if (!seg) return 0;
  seg->owned = malloc(len);
  if (!seg->owned) {
    free(seg);
    return 0;
  }
  memcpy(seg->owned, data, len);
  seg->data = seg->owned;
  seg->len = len;
  seg->owns = 1;
  if (conn->out_tail) {
    conn->out_tail->next = seg;
  } else {
    conn->out_head = seg;
  }
  conn->out_tail = seg;
  submit_write(conn);
  return 1;
}

int event_loop_conn_out_add_ref(EventConn* conn, const void* data, size_t len) {
  if (!conn || conn->closing || !data || !len) return 0;
  OutSeg* seg = calloc(1, sizeof(OutSeg));
  if (!seg) return 0;
  seg->data = data;
  seg->len = len;
  seg->owns = 0;
  if (conn->out_tail) {
    conn->out_tail->next = seg;
  } else {
    conn->out_head = seg;
  }
  conn->out_tail = seg;
  submit_write(conn);
  return 1;
}

int event_loop_add_signal(EventLoop* loop, int signum,
                          event_loop_signal_cb cb, void* ctx) {
  if (!loop) return 0;
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, signum);
  sigprocmask(SIG_BLOCK, &mask, NULL);
  loop->signal_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
  if (loop->signal_fd < 0) return 0;
  loop->signal_cb = cb;
  loop->signal_ctx = ctx;
  submit_signal(loop);
  return 1;
}

int event_loop_add_timer(EventLoop* loop, unsigned ms,
                         event_loop_timer_cb cb, void* ctx) {
  if (!loop) return 0;
  int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (fd < 0) return 0;
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  its.it_value.tv_sec = (time_t)(ms / 1000);
  its.it_value.tv_nsec = (long)((ms % 1000) * 1000000);
  if (timerfd_settime(fd, 0, &its, NULL) != 0) {
    close(fd);
    return 0;
  }
  TimerOp* op = calloc(1, sizeof(TimerOp));
  if (!op) {
    close(fd);
    return 0;
  }
  op->base.type = OP_TIMER;
  op->loop = loop;
  op->fd = fd;
  op->cb = cb;
  op->ctx = ctx;
  submit_timer(loop, fd, op);
  return 1;
}

const char* event_loop_backend_name(EventLoop* loop) {
  UNUSED(loop);
  return "io_uring";
}

const char* event_loop_backend_version(EventLoop* loop) {
  UNUSED(loop);
  return "kernel";
}

#endif
