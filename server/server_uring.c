// io_uring I/O backend implementation for the Melian server.
// This file is compiled on all platforms but only active when HAVE_IOURING is defined.

#include "server_io.h"

#include <string.h>
#include <strings.h>

// Implementation of backend selection functions (always compiled)
IoBackend io_backend_detect(void) {
#ifdef HAVE_IOURING
  // Runtime check: try to create a minimal ring
  // This validates the kernel supports io_uring
  extern int uring_backend_probe(void);
  if (uring_backend_probe()) {
    return IO_BACKEND_IOURING;
  }
#endif
  return IO_BACKEND_LIBEVENT;
}

const char* io_backend_name(IoBackend backend) {
  switch (backend) {
    case IO_BACKEND_IOURING:  return "iouring";
    case IO_BACKEND_LIBEVENT: return "libevent";
    default:                  return "unknown";
  }
}

IoBackend io_backend_parse(const char* value) {
  if (!value || !value[0] || strcasecmp(value, "auto") == 0) {
    return io_backend_detect();
  }
  if (strcasecmp(value, "iouring") == 0 ) {
    return IO_BACKEND_IOURING;
  }
  if (strcasecmp(value, "libevent") == 0) {
    return IO_BACKEND_LIBEVENT;
  }
  // Unknown value, default to auto-detect
  return io_backend_detect();
}

#ifdef HAVE_IOURING

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <liburing.h>

#include "util.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "status.h"
#include "data.h"
#include "db.h"
#include "cron.h"
#include "protocol.h"
#include "server.h"

// Ring configuration
enum {
  URING_ENTRIES = 256,         // SQ entries (CQ is 2x by default)
  URING_MAX_CONNS = 1024,      // Maximum concurrent connections
  URING_RBUF_SIZE = 4096,      // Read buffer per connection
  URING_WBUF_SIZE = 65536,     // Write buffer per connection
  URING_MAX_KEY_LEN = 256,     // Maximum key length
};

// Operation types encoded in user_data
enum UringOp {
  URING_OP_ACCEPT = 1,
  URING_OP_RECV = 2,
  URING_OP_SEND = 3,
  URING_OP_TIMER = 4,
  URING_OP_SIGNAL = 5,
};

// Connection state for io_uring backend
typedef struct uring_conn_t {
  struct uring_ctx_t* ctx;
  int fd;

  // Read buffer
  uint8_t rbuf[URING_RBUF_SIZE];
  unsigned rbuf_len;
  unsigned rbuf_pos;

  // Write buffer
  uint8_t wbuf[URING_WBUF_SIZE];
  unsigned wbuf_len;
  unsigned wbuf_pos;

  // Zero-copy pending response
  const uint8_t* pending_ref;
  unsigned pending_ref_len;
  unsigned pending_ref_pos;

  // Parse state
  MelianRequestHeader hdr;
  uint32_t hdr_have;
  uint8_t action;
  uint8_t table_id;
  uint8_t index_id;
  uint32_t key_len;
  uint32_t key_have;
  unsigned discarding;

  // State flags
  unsigned recv_pending : 1;
  unsigned send_pending : 1;
  unsigned closing : 1;

  // Free list linkage
  struct uring_conn_t* next;
} uring_conn_t;

// io_uring backend context
typedef struct uring_ctx_t {
  Server* server;
  struct io_uring ring;
  int listen_fd;
  int timer_fd;
  int signal_fd;

  // Feature flags from runtime probing
  unsigned has_fast_poll : 1;
  unsigned has_sqpoll_nonfixed : 1;
  unsigned has_multishot_accept : 1;

  // Connection management
  uring_conn_t* conn_free;
  unsigned conn_count;

  // Accept state
  struct sockaddr_storage accept_addr;
  socklen_t accept_addrlen;

  // Running state
  unsigned running;
  unsigned quit_requested;
} uring_ctx_t;

// Forward declarations
static void uring_submit_accept(uring_ctx_t* ctx);
static void uring_submit_recv(uring_conn_t* conn);
static void uring_submit_send(uring_conn_t* conn);
static void uring_submit_timer(uring_ctx_t* ctx);
static void uring_handle_accept(uring_ctx_t* ctx, struct io_uring_cqe* cqe);
static void uring_handle_recv(uring_conn_t* conn, struct io_uring_cqe* cqe);
static void uring_handle_send(uring_conn_t* conn, struct io_uring_cqe* cqe);
static void uring_handle_timer(uring_ctx_t* ctx, struct io_uring_cqe* cqe);
static void uring_conn_close(uring_conn_t* conn);
static uring_conn_t* uring_conn_alloc(uring_ctx_t* ctx, int fd);
static void uring_process_requests(uring_conn_t* conn);

// Inline fetch for hot path (duplicated from server.c for independence)
static inline const Bucket* uring_data_fetch(Data* data, unsigned table_id,
                                              unsigned index_id,
                                              const void *key, unsigned len) {
  if (unlikely(table_id >= ALEN(data->lookup))) return NULL;
  Table* table = data->lookup[table_id];
  if (unlikely(!table)) return NULL;
  if (unlikely(index_id >= table->index_count)) return NULL;

  unsigned current_slot = table->current_slot;
  struct TableSlot* slot = &table->slots[current_slot];
  Hash* hash = slot->indexes[index_id];
  if (unlikely(!hash)) return NULL;

  return hash_get(hash, key, len);
}

// Encode operation + connection pointer into user_data
static inline uint64_t encode_user_data(enum UringOp op, void* ptr) {
  return ((uint64_t)op << 56) | ((uint64_t)(uintptr_t)ptr & 0x00FFFFFFFFFFFFFF);
}

static inline enum UringOp decode_op(uint64_t user_data) {
  return (enum UringOp)(user_data >> 56);
}

static inline void* decode_ptr(uint64_t user_data) {
  return (void*)(uintptr_t)(user_data & 0x00FFFFFFFFFFFFFF);
}

// Runtime probe for io_uring availability
int uring_backend_probe(void) {
  struct io_uring ring;
  int ret = io_uring_queue_init(4, &ring, 0);
  if (ret < 0) {
    LOG_DEBUG("io_uring probe failed: %s", strerror(-ret));
    return 0;
  }
  io_uring_queue_exit(&ring);
  return 1;
}

// Initialize the io_uring backend
void* uring_backend_init(Server* server) {
  uring_ctx_t* ctx = calloc(1, sizeof(uring_ctx_t));
  if (!ctx) {
    LOG_WARN("Could not allocate io_uring context");
    return NULL;
  }
  ctx->server = server;
  ctx->listen_fd = -1;
  ctx->timer_fd = -1;
  ctx->signal_fd = -1;

  // Initialize the ring with optimization flags
  struct io_uring_params params = {0};

  // Request optimizations (kernel will ignore unsupported ones)
#ifdef IORING_SETUP_COOP_TASKRUN
  params.flags |= IORING_SETUP_COOP_TASKRUN;  // Reduce kernel task switches
#endif
#ifdef IORING_SETUP_SINGLE_ISSUER
  params.flags |= IORING_SETUP_SINGLE_ISSUER; // Single-threaded submission
#endif
#ifdef IORING_SETUP_DEFER_TASKRUN
  params.flags |= IORING_SETUP_DEFER_TASKRUN; // Defer work to submit time
#endif

  int ret = io_uring_queue_init_params(URING_ENTRIES, &ctx->ring, &params);
  if (ret < 0) {
    // Retry without optimization flags on older kernels
    params.flags = 0;
    ret = io_uring_queue_init_params(URING_ENTRIES, &ctx->ring, &params);
    if (ret < 0) {
      LOG_WARN("io_uring_queue_init failed: %s", strerror(-ret));
      free(ctx);
      return NULL;
    }
  }

  // Check for feature flags
  if (params.features & IORING_FEAT_FAST_POLL) {
    ctx->has_fast_poll = 1;
    LOG_DEBUG("io_uring: FAST_POLL supported");
  }
  if (params.features & IORING_FEAT_SQPOLL_NONFIXED) {
    ctx->has_sqpoll_nonfixed = 1;
    LOG_DEBUG("io_uring: SQPOLL_NONFIXED supported");
  }

  // Probe for multishot accept
  struct io_uring_probe* probe = io_uring_get_probe_ring(&ctx->ring);
  if (probe) {
    if (io_uring_opcode_supported(probe, IORING_OP_ACCEPT)) {
      // Note: multishot accept requires kernel 5.19+
      // For now we use regular accept
      LOG_DEBUG("io_uring: ACCEPT opcode supported");
    }
    io_uring_free_probe(probe);
  }

  LOG_INFO("io_uring backend initialized (entries=%u, flags=0x%x, features=0x%x)",
           URING_ENTRIES, params.flags, params.features);
  return ctx;
}

// Destroy the io_uring backend
void uring_backend_destroy(void* io_ctx) {
  if (!io_ctx) return;
  uring_ctx_t* ctx = io_ctx;

  // Close all active connections
  // (Connection cleanup happens in uring_conn_close)

  // Free connection pool
  uring_conn_t* conn = ctx->conn_free;
  while (conn) {
    uring_conn_t* next = conn->next;
    free(conn);
    conn = next;
  }

  if (ctx->listen_fd >= 0) close(ctx->listen_fd);
  if (ctx->timer_fd >= 0) close(ctx->timer_fd);
  if (ctx->signal_fd >= 0) close(ctx->signal_fd);

  io_uring_queue_exit(&ctx->ring);
  free(ctx);
}

// Set up listening socket for io_uring backend
int uring_backend_listen(void* io_ctx, const char* path, const char* host, unsigned port) {
  uring_ctx_t* ctx = io_ctx;
  int fd = -1;

  if (path && path[0]) {
    // Unix socket
    unlink(path);
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
      LOG_WARN("socket(AF_UNIX) failed: %s", strerror(errno));
      return -1;
    }

    struct sockaddr_un sun;
    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    int wrote = snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", path);
    if (wrote < 0 || (size_t)wrote >= sizeof(sun.sun_path)) {
      LOG_WARN("UNIX socket path too long: %s", path);
      close(fd);
      return -1;
    }

    if (bind(fd, (struct sockaddr*)&sun, sizeof(sun)) < 0) {
      LOG_WARN("bind(UNIX) failed: %s", strerror(errno));
      close(fd);
      return -1;
    }

    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
    chmod(path, mode);
    LOG_INFO("io_uring: Listening on UNIX socket [%s]", path);

  } else if (host && host[0] && port) {
    // TCP socket
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      LOG_WARN("socket(AF_INET) failed: %s", strerror(errno));
      return -1;
    }

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    inet_pton(AF_INET, host, &sin.sin_addr);

    if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
      LOG_WARN("bind(TCP) failed: %s", strerror(errno));
      close(fd);
      return -1;
    }
    LOG_INFO("io_uring: Listening on TCP socket [%s:%u]", host, port);

  } else {
    LOG_WARN("No socket configuration provided");
    return -1;
  }

  if (listen(fd, 128) < 0) {
    LOG_WARN("listen() failed: %s", strerror(errno));
    close(fd);
    return -1;
  }

  // Make non-blocking for io_uring
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);

  ctx->listen_fd = fd;
  return 0;
}

// Set up timer for cron
static int uring_setup_timer(uring_ctx_t* ctx, int period_sec) {
  ctx->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (ctx->timer_fd < 0) {
    LOG_WARN("timerfd_create failed: %s", strerror(errno));
    return -1;
  }

  struct itimerspec ts;
  ts.it_value.tv_sec = period_sec;
  ts.it_value.tv_nsec = 0;
  ts.it_interval.tv_sec = period_sec;
  ts.it_interval.tv_nsec = 0;

  if (timerfd_settime(ctx->timer_fd, 0, &ts, NULL) < 0) {
    LOG_WARN("timerfd_settime failed: %s", strerror(errno));
    close(ctx->timer_fd);
    ctx->timer_fd = -1;
    return -1;
  }

  LOG_DEBUG("io_uring: Timer set up with period %d seconds", period_sec);
  return 0;
}

// Run the io_uring event loop
int uring_backend_run(void* io_ctx) {
  uring_ctx_t* ctx = io_ctx;

  if (ctx->listen_fd < 0) {
    LOG_WARN("io_uring: No listening socket configured");
    return -1;
  }

  ctx->running = 1;
  ctx->quit_requested = 0;

  // Set up the timer for cron (5 second period matches libevent backend)
  uring_setup_timer(ctx, 5);

  // Submit initial accept
  uring_submit_accept(ctx);

  // Submit timer read if configured
  if (ctx->timer_fd >= 0) {
    uring_submit_timer(ctx);
  }

  // Initial submit to kick off operations
  io_uring_submit(&ctx->ring);

  LOG_INFO("io_uring: Running event loop");

  while (ctx->running && !ctx->quit_requested) {
    // Submit and wait in one syscall - this is the key optimization
    int ret = io_uring_submit_and_wait(&ctx->ring, 1);
    if (ret < 0) {
      if (ret == -EINTR) continue;
      LOG_WARN("io_uring_submit_and_wait failed: %s", strerror(-ret));
      break;
    }

    // Process all available completions
    struct io_uring_cqe* cqe;
    unsigned head;
    unsigned completed = 0;
    io_uring_for_each_cqe(&ctx->ring, head, cqe) {
      uint64_t user_data = cqe->user_data;
      enum UringOp op = decode_op(user_data);
      void* ptr = decode_ptr(user_data);

      switch (op) {
        case URING_OP_ACCEPT:
          uring_handle_accept(ctx, cqe);
          break;
        case URING_OP_RECV:
          uring_handle_recv((uring_conn_t*)ptr, cqe);
          break;
        case URING_OP_SEND:
          uring_handle_send((uring_conn_t*)ptr, cqe);
          break;
        case URING_OP_TIMER:
          uring_handle_timer(ctx, cqe);
          break;
        case URING_OP_SIGNAL:
          LOG_INFO("io_uring: Received signal, stopping");
          ctx->quit_requested = 1;
          break;
        default:
          LOG_WARN("io_uring: Unknown operation %d", op);
          break;
      }
      completed++;
    }
    io_uring_cq_advance(&ctx->ring, completed);
  }

  ctx->running = 0;
  LOG_INFO("io_uring: Event loop stopped");
  return 0;
}

// Stop the io_uring backend
int uring_backend_stop(void* io_ctx) {
  if (!io_ctx) return 0;
  uring_ctx_t* ctx = io_ctx;
  ctx->quit_requested = 1;
  ctx->running = 0;
  return 0;
}

// Submit an accept operation
static void uring_submit_accept(uring_ctx_t* ctx) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
  if (!sqe) {
    LOG_WARN("io_uring: No SQE available for accept");
    return;
  }

  ctx->accept_addrlen = sizeof(ctx->accept_addr);
  io_uring_prep_accept(sqe, ctx->listen_fd,
                       (struct sockaddr*)&ctx->accept_addr,
                       &ctx->accept_addrlen, 0);
  io_uring_sqe_set_data64(sqe, encode_user_data(URING_OP_ACCEPT, ctx));
}

// Submit a recv operation
static void uring_submit_recv(uring_conn_t* conn) {
  if (conn->recv_pending || conn->closing) return;

  struct io_uring_sqe* sqe = io_uring_get_sqe(&conn->ctx->ring);
  if (!sqe) {
    LOG_WARN("io_uring: No SQE available for recv");
    return;
  }

  unsigned space = URING_RBUF_SIZE - conn->rbuf_len;
  io_uring_prep_recv(sqe, conn->fd, conn->rbuf + conn->rbuf_len, space, 0);
  io_uring_sqe_set_data64(sqe, encode_user_data(URING_OP_RECV, conn));
  conn->recv_pending = 1;
}

// Submit a send operation
static void uring_submit_send(uring_conn_t* conn) {
  if (conn->send_pending || conn->closing) return;

  // Determine what to send
  const uint8_t* data = NULL;
  unsigned len = 0;

  if (conn->wbuf_pos < conn->wbuf_len) {
    data = conn->wbuf + conn->wbuf_pos;
    len = conn->wbuf_len - conn->wbuf_pos;
  } else if (conn->pending_ref && conn->pending_ref_pos < conn->pending_ref_len) {
    data = conn->pending_ref + conn->pending_ref_pos;
    len = conn->pending_ref_len - conn->pending_ref_pos;
  }

  if (!data || !len) return;

  struct io_uring_sqe* sqe = io_uring_get_sqe(&conn->ctx->ring);
  if (!sqe) {
    LOG_WARN("io_uring: No SQE available for send");
    return;
  }

  io_uring_prep_send(sqe, conn->fd, data, len, MSG_NOSIGNAL);
  io_uring_sqe_set_data64(sqe, encode_user_data(URING_OP_SEND, conn));
  conn->send_pending = 1;
}

// Submit timer read
static void uring_submit_timer(uring_ctx_t* ctx) {
  if (ctx->timer_fd < 0) return;

  struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
  if (!sqe) {
    LOG_WARN("io_uring: No SQE available for timer");
    return;
  }

  // We'll read the timer expiration count but don't really need the value
  static uint64_t timer_buf;
  io_uring_prep_read(sqe, ctx->timer_fd, &timer_buf, sizeof(timer_buf), 0);
  io_uring_sqe_set_data64(sqe, encode_user_data(URING_OP_TIMER, ctx));
}

// Handle accept completion
static void uring_handle_accept(uring_ctx_t* ctx, struct io_uring_cqe* cqe) {
  if (cqe->res < 0) {
    if (cqe->res != -EAGAIN && cqe->res != -ECANCELED) {
      LOG_WARN("io_uring: accept failed: %s", strerror(-cqe->res));
    }
  } else {
    int client_fd = cqe->res;

    // Make non-blocking
    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

    // Set TCP_NODELAY for TCP connections (not UNIX sockets)
    if (ctx->accept_addr.ss_family == AF_INET || ctx->accept_addr.ss_family == AF_INET6) {
      int one = 1;
      setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }

    // Allocate connection state
    uring_conn_t* conn = uring_conn_alloc(ctx, client_fd);
    if (conn) {
      LOG_DEBUG("io_uring: Accepted connection fd=%d", client_fd);
      uring_submit_recv(conn);
    } else {
      LOG_WARN("io_uring: Could not allocate connection state, closing fd=%d", client_fd);
      close(client_fd);
    }
  }

  // Re-arm accept (unless stopping)
  if (ctx->running && !ctx->quit_requested) {
    uring_submit_accept(ctx);
  }
}

// Handle recv completion
static void uring_handle_recv(uring_conn_t* conn, struct io_uring_cqe* cqe) {
  conn->recv_pending = 0;

  if (cqe->res <= 0) {
    if (cqe->res < 0 && cqe->res != -ECONNRESET && cqe->res != -ECANCELED) {
      LOG_DEBUG("io_uring: recv error fd=%d: %s", conn->fd, strerror(-cqe->res));
    }
    uring_conn_close(conn);
    return;
  }

  conn->rbuf_len += cqe->res;

  // Process any complete requests
  uring_process_requests(conn);

  // Re-arm recv if connection still active and buffer has space
  if (!conn->closing && conn->rbuf_len < URING_RBUF_SIZE) {
    uring_submit_recv(conn);
  }
}

// Handle send completion
static void uring_handle_send(uring_conn_t* conn, struct io_uring_cqe* cqe) {
  conn->send_pending = 0;

  if (cqe->res < 0) {
    if (cqe->res != -ECONNRESET && cqe->res != -EPIPE && cqe->res != -ECANCELED) {
      LOG_DEBUG("io_uring: send error fd=%d: %s", conn->fd, strerror(-cqe->res));
    }
    uring_conn_close(conn);
    return;
  }

  unsigned sent = (unsigned)cqe->res;

  // Update send position
  if (conn->wbuf_pos < conn->wbuf_len) {
    unsigned wbuf_remain = conn->wbuf_len - conn->wbuf_pos;
    if (sent >= wbuf_remain) {
      sent -= wbuf_remain;
      conn->wbuf_pos = conn->wbuf_len = 0;
    } else {
      conn->wbuf_pos += sent;
      sent = 0;
    }
  }

  if (sent > 0 && conn->pending_ref) {
    conn->pending_ref_pos += sent;
    if (conn->pending_ref_pos >= conn->pending_ref_len) {
      conn->pending_ref = NULL;
      conn->pending_ref_len = 0;
      conn->pending_ref_pos = 0;
    }
  }

  // Continue sending if more data
  if ((conn->wbuf_pos < conn->wbuf_len) ||
      (conn->pending_ref && conn->pending_ref_pos < conn->pending_ref_len)) {
    uring_submit_send(conn);
  }
}

// Handle timer completion
static void uring_handle_timer(uring_ctx_t* ctx, struct io_uring_cqe* cqe) {
  UNUSED(cqe);

  // Poke the cron thread
  Server* server = ctx->server;
  if (server->cron) {
    LOG_DEBUG("io_uring: Timer tick, poking cron");
    // Use the same mechanism as libevent backend
    extern void cron_poke(struct Cron* cron);
    cron_poke(server->cron);
  }

  // Re-arm timer
  if (ctx->running && !ctx->quit_requested) {
    uring_submit_timer(ctx);
  }
}

// Allocate or reuse a connection state
static uring_conn_t* uring_conn_alloc(uring_ctx_t* ctx, int fd) {
  uring_conn_t* conn = NULL;

  if (ctx->conn_free) {
    conn = ctx->conn_free;
    ctx->conn_free = conn->next;
    LOG_DEBUG("io_uring: REUSED conn state");
  } else {
    conn = calloc(1, sizeof(uring_conn_t));
    if (!conn) return NULL;
    LOG_DEBUG("io_uring: CREATED conn state");
  }

  // Initialize/reset state
  conn->ctx = ctx;
  conn->fd = fd;
  conn->rbuf_len = 0;
  conn->rbuf_pos = 0;
  conn->wbuf_len = 0;
  conn->wbuf_pos = 0;
  conn->pending_ref = NULL;
  conn->pending_ref_len = 0;
  conn->pending_ref_pos = 0;
  conn->hdr_have = 0;
  conn->key_have = 0;
  conn->key_len = 0;
  conn->action = 0;
  conn->table_id = 0;
  conn->index_id = 0;
  conn->discarding = 0;
  conn->recv_pending = 0;
  conn->send_pending = 0;
  conn->closing = 0;
  conn->next = NULL;

  ctx->conn_count++;
  return conn;
}

// Close connection and return to free list
static void uring_conn_close(uring_conn_t* conn) {
  if (conn->closing) return;
  conn->closing = 1;

  LOG_DEBUG("io_uring: Closing connection fd=%d", conn->fd);

  if (conn->fd >= 0) {
    close(conn->fd);
    conn->fd = -1;
  }

  // Reset state
  conn->rbuf_len = 0;
  conn->rbuf_pos = 0;
  conn->wbuf_len = 0;
  conn->wbuf_pos = 0;
  conn->pending_ref = NULL;
  conn->pending_ref_len = 0;
  conn->pending_ref_pos = 0;
  conn->hdr_have = 0;
  conn->key_have = 0;
  conn->key_len = 0;
  conn->action = 0;
  conn->table_id = 0;
  conn->index_id = 0;
  conn->discarding = 0;
  conn->recv_pending = 0;
  conn->send_pending = 0;
  conn->closing = 0;

  // Return to free list
  uring_ctx_t* ctx = conn->ctx;
  conn->next = ctx->conn_free;
  ctx->conn_free = conn;
  ctx->conn_count--;
}

// Queue response for writing
// For preframed data (no header), use zero-copy reference to arena
// For non-preframed (with header), copy both to wbuf for single send
static inline void uring_queue_response(uring_conn_t* conn,
                                         const uint8_t* hdr, unsigned hdr_len,
                                         const uint8_t* data, unsigned data_len) {
  if (!hdr || !hdr_len) {
    // Preframed data path (hot path for FETCH) - zero-copy from arena
    if (data && data_len) {
      conn->pending_ref = data;
      conn->pending_ref_len = data_len;
      conn->pending_ref_pos = 0;
    }
  } else {
    // Non-preframed path (DESCRIBE_SCHEMA, etc.) - combine header+data in wbuf
    unsigned total = hdr_len + data_len;
    if (conn->wbuf_len + total <= URING_WBUF_SIZE) {
      memcpy(conn->wbuf + conn->wbuf_len, hdr, hdr_len);
      conn->wbuf_len += hdr_len;
      if (data && data_len) {
        memcpy(conn->wbuf + conn->wbuf_len, data, data_len);
        conn->wbuf_len += data_len;
      }
    } else {
      // Fallback: buffer header, reference data
      memcpy(conn->wbuf + conn->wbuf_len, hdr, hdr_len);
      conn->wbuf_len += hdr_len;
      if (data && data_len) {
        conn->pending_ref = data;
        conn->pending_ref_len = data_len;
        conn->pending_ref_pos = 0;
      }
    }
  }

  // Kick off send
  uring_submit_send(conn);
}

// Process complete requests (mirrors server.c on_read logic)
static void uring_process_requests(uring_conn_t* conn) {
  uring_ctx_t* ctx = conn->ctx;
  Server* server = ctx->server;
  static const uint8_t zero_hdr[4] = {0};

  while (1) {
    unsigned avail = conn->rbuf_len - conn->rbuf_pos;

    // Step 1: Parse header
    if (conn->hdr_have < sizeof(MelianRequestHeader)) {
      if (avail < sizeof(MelianRequestHeader)) break;

      const MelianRequestHeader* H = (const MelianRequestHeader*)(conn->rbuf + conn->rbuf_pos);
      if (unlikely(H->data.version != MELIAN_HEADER_VERSION)) {
        LOG_WARN("io_uring: Invalid protocol version 0x%02x", H->data.version);
        uring_conn_close(conn);
        return;
      }
      conn->action = H->data.action;
      conn->table_id = H->data.table_id;
      conn->index_id = H->data.index_id;
      conn->key_len = ntohl(H->data.length);
      conn->rbuf_pos += sizeof(MelianRequestHeader);
      conn->hdr_have = sizeof(MelianRequestHeader);
      conn->discarding = unlikely(conn->key_len > URING_MAX_KEY_LEN);
      conn->key_have = 0;
      avail = conn->rbuf_len - conn->rbuf_pos;
    }

    // Step 2: Handle key payload
    if (conn->discarding) {
      unsigned to_consume = conn->key_len - conn->key_have;
      if (to_consume > avail) to_consume = avail;
      conn->rbuf_pos += to_consume;
      conn->key_have += to_consume;
      if (conn->key_have < conn->key_len) break;
    } else {
      if (avail < conn->key_len) break;
    }

    const uint8_t* key_ptr = conn->discarding ? NULL : (conn->rbuf + conn->rbuf_pos);

    // Step 3: Lookup & prepare response
    const uint8_t* rptr = NULL;
    unsigned rlen = 0;
    unsigned rfmt = 0;
    uint8_t len_hdr[4];

    if (unlikely(conn->discarding)) {
      // Discarding oversized key
    } else if (likely(conn->action == MELIAN_ACTION_FETCH)) {
      const Bucket* bucket = uring_data_fetch(server->data, conn->table_id,
                                               conn->index_id, key_ptr, conn->key_len);
      if (likely(bucket)) {
        rptr = bucket->frame_ptr;
        rlen = bucket->frame_len;
        rfmt = 1;
      }
    } else {
      switch (conn->action) {
        case MELIAN_ACTION_DESCRIBE_SCHEMA: {
          unsigned schema_len = 0;
          const char* schema = data_schema_json(server->data, &schema_len);
          if (schema && schema_len) {
            rptr = (const uint8_t*)schema;
            rlen = schema_len;
          }
          break;
        }

        case MELIAN_ACTION_GET_STATISTICS: {
          status_json(server->status, server->config, server->data);
          rptr = (uint8_t*)server->status->json.jbuf;
          rlen = server->status->json.jlen;
          break;
        }

        case MELIAN_ACTION_QUIT: {
          const char* bye = "{\"BYE\":true}";
          rptr = (uint8_t*)bye;
          rlen = strlen(bye);
          ctx->quit_requested = 1;
          break;
        }

        default:
          break;
      }
    }

    // Step 4: Send response
    if (likely(rptr && rlen)) {
      LOG_DEBUG("io_uring: Writing response with %u bytes", rlen);
      if (unlikely(!rfmt)) {
        uint32_t l = htonl(rlen);
        memcpy(len_hdr, &l, 4);
        uring_queue_response(conn, len_hdr, 4, rptr, rlen);
      } else {
        uring_queue_response(conn, NULL, 0, rptr, rlen);
      }
    } else {
      LOG_DEBUG("io_uring: Writing ZERO response");
      uring_queue_response(conn, zero_hdr, 4, NULL, 0);
    }

    // Consume key bytes
    if (!conn->discarding) {
      conn->rbuf_pos += conn->key_len;
    }

    // Reset parse state
    conn->hdr_have = 0;
    conn->key_have = 0;
    conn->discarding = 0;
  }

  // Compact read buffer
  if (conn->rbuf_pos > 0) {
    unsigned remaining = conn->rbuf_len - conn->rbuf_pos;
    if (remaining > 0) {
      memmove(conn->rbuf, conn->rbuf + conn->rbuf_pos, remaining);
    }
    conn->rbuf_len = remaining;
    conn->rbuf_pos = 0;
  }
}

#else  // !HAVE_IOURING

#include "util.h"

struct Server;

// Stubs when io_uring is not available
int uring_backend_probe(void) { return 0; }
void* uring_backend_init(struct Server* server) { UNUSED(server); return NULL; }
void uring_backend_destroy(void* io_ctx) { UNUSED(io_ctx); }
int uring_backend_listen(void* io_ctx, const char* path, const char* host, unsigned port) {
  UNUSED(io_ctx); UNUSED(path); UNUSED(host); UNUSED(port);
  return -1;
}
int uring_backend_run(void* io_ctx) { UNUSED(io_ctx); return -1; }
int uring_backend_stop(void* io_ctx) { UNUSED(io_ctx); return -1; }

#endif  // HAVE_IOURING
