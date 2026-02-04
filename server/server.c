#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <event2/event.h>
#include <event2/listener.h>
#include "util.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "config.h"
#include "status.h"
#include "data.h"
#include "db.h"
#include "cron.h"
#include "protocol.h"
#include "server_io.h"
#include "server.h"

// io_uring backend functions (defined in server_uring.c)
extern void* uring_backend_init(Server* server);
extern void uring_backend_destroy(void* io_ctx);
extern int uring_backend_listen(void* io_ctx, const char* path, const char* host, unsigned port);
extern int uring_backend_run(void* io_ctx);
extern int uring_backend_stop(void* io_ctx);

enum {
  MELIAN_MAX_KEY_LEN = 256,   // max key length in bytes
  MELIAN_RBUF_SIZE = 4096,    // read buffer size
  MELIAN_WBUF_SIZE = 65536,   // write buffer size
};

// State for each client connection using direct I/O
struct conn_state_t {
  Server* server;
  int fd;
  struct event *rev;           // read event
  struct event *wev;           // write event (lazy-created)

  // Read buffer
  uint8_t rbuf[MELIAN_RBUF_SIZE];
  unsigned rbuf_len;           // bytes in buffer
  unsigned rbuf_pos;           // current parse position

  // Write buffer for small responses
  uint8_t wbuf[MELIAN_WBUF_SIZE];
  unsigned wbuf_len;           // bytes to write
  unsigned wbuf_pos;           // bytes written

  // Zero-copy pending response (arena data)
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

  struct conn_state_t* next;
};

static HOT_FUNC void on_read(evutil_socket_t fd, short events, void *ctx);
static void on_write(evutil_socket_t fd, short events, void *ctx);
static void on_accept(struct evconnlistener *lev, evutil_socket_t fd,
                      struct sockaddr *addr, int socklen, void *ctx);
static void on_quit(evutil_socket_t fd, short what, void *ctx);
static void on_signal(int signal, short events, void *ctx);
static void conn_close(struct conn_state_t *state);

// Inline fetch combining data_fetch + table_fetch + hash_get for hot path
static inline const Bucket* data_fetch_inline(Data* data, unsigned table_id,
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

Server* server_build(void) {
  Server* server = 0;
  unsigned bad = 0;
  do {
    server = calloc(1, sizeof(Server));
    if (!server) {
      LOG_WARN("Could not allocate a Server object");
      break;
    }

    server->config = config_build();
    if (!server->config) {
      ++bad;
      break;
    }

    // Select I/O backend
    server->io_backend = server->config->server.io_backend;
    LOG_INFO("I/O backend selected: %s", io_backend_name(server->io_backend));

    if (server->io_backend == IO_BACKEND_IOURING) {
#ifdef HAVE_IOURING
      server->io_ctx = uring_backend_init(server);
      if (!server->io_ctx) {
        LOG_WARN("Failed to initialize io_uring backend, falling back to libevent");
        server->io_backend = IO_BACKEND_LIBEVENT;
      }
#else
      LOG_WARN("io_uring backend requested but not available in this build, using libevent");
      server->io_backend = IO_BACKEND_LIBEVENT;
#endif
    }

    // Initialize libevent if using that backend (or as fallback)
    if (server->io_backend == IO_BACKEND_LIBEVENT) {
      server->base = event_base_new();
      if (!server->base) {
        LOG_WARN("Could not allocate a Server event_base object");
        ++bad;
        break;
      }
    }

    server->db = db_build(server->config);
    if (!server->db) {
      ++bad;
      break;
    }
    server->data = data_build(server->config);
    if (!server->data) {
      ++bad;
      break;
    }
    server->cron = cron_build(server);
    if (!server->cron) {
      ++bad;
      break;
    }

    // status_build needs event_base for libevent info, pass NULL for io_uring
    server->status = status_build(server->base, server->db);
    if (!server->status) {
      ++bad;
      break;
    }
    status_log(server->status);

    // Signal handler only needed for libevent backend
    if (server->io_backend == IO_BACKEND_LIBEVENT && server->base) {
      server->sev = evsignal_new(server->base, SIGINT, on_signal, server);
      event_add(server->sev, NULL);
    }
  } while (0);
  if (bad) {
    server_destroy(server);
    server = 0;
  }
  return server;
}

void server_destroy(Server* server) {
  if (!server) return;
  server_stop(server);

  // Clean up io_uring backend if used
  if (server->io_ctx) {
    uring_backend_destroy(server->io_ctx);
    server->io_ctx = NULL;
  }

  // Clean up libevent connection pool (only used with libevent backend)
  if (server->io_backend == IO_BACKEND_LIBEVENT) {
    unsigned size = 0;
    for (struct conn_state_t* p = server->conn_free; p; ) {
      ++size;
      struct conn_state_t* q = p;
      p = p->next;
      if (q->rev) event_free(q->rev);
      if (q->wev) event_free(q->wev);
      if (q->fd >= 0) close(q->fd);
      free(q);
    }
    if (size) {
      LOG_INFO("Cleared conn free list with %u elements", size);
    }
  }

  if (server->listener) evconnlistener_free(server->listener);
  if (server->cron) cron_destroy(server->cron);
  if (server->data) data_destroy(server->data);
  if (server->db) db_destroy(server->db);
  if (server->status) status_destroy(server->status);
  if (server->config) config_destroy(server->config);
  if (server->sev) event_free(server->sev);
  if (server->tev) event_free(server->tev);
  if (server->base) event_base_free(server->base);
  free(server);
}

unsigned server_initial_load(Server* server) {
  unsigned total_rows = data_load_all_tables_from_db(server->data, server->db);
  return total_rows > 0;
}

unsigned server_listen(Server* server) {
  const char* path = server->config->socket.path;
  const char* host = server->config->socket.host;
  unsigned port = server->config->socket.port;

  // Use io_uring backend if selected
  if (server->io_backend == IO_BACKEND_IOURING && server->io_ctx) {
    if (uring_backend_listen(server->io_ctx, path, host, port) == 0) {
      return 1;
    }
    LOG_WARN("io_uring listen failed");
    return 0;
  }

  // libevent backend
  if (path && path[0]) {
    unlink(path);
    struct sockaddr_un sun;
    memset(&sun, 0, sizeof(sun));
    sun.sun_family = AF_UNIX;
    int wrote = snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", path);
    if (wrote < 0 || (size_t)wrote >= sizeof(sun.sun_path)) {
      errno = ENOMEM;
      LOG_FATAL("UNIX socket path '%s' exceeds %zu bytes", path, sizeof(sun.sun_path) - 1);
    }
    server->listener = evconnlistener_new_bind(server->base, on_accept, server,
                                               LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
                                               (struct sockaddr*)&sun, sizeof(sun));
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP; // 0660
    chmod(path, mode);
    LOG_INFO("Listening on UNIX socket [%s]", path);
    return 1;
  }

  if (host && host[0] && port) {
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    inet_pton(AF_INET, host, &sin.sin_addr);
    unsigned flags = LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE | LEV_OPT_REUSEABLE_PORT;
    server->listener = evconnlistener_new_bind(server->base, on_accept, server, flags, -1,
                                               (struct sockaddr*)&sin, sizeof(sin));
    LOG_INFO("Listening on TCP socket [%s:%u]", host, port);
    return 1;
  }

  return 0;
}

unsigned server_run(Server* server) {
  do {
    if (server->running) break;
    server->running = 1;

    cron_run(server->cron);

    if (server->io_backend == IO_BACKEND_IOURING && server->io_ctx) {
      LOG_INFO("Running io_uring event loop");
      uring_backend_run(server->io_ctx);
    } else {
      LOG_INFO("Running libevent event loop");
      event_base_dispatch(server->base);
    }
  } while (0);
  return 0;
}

unsigned server_stop(Server* server) {
  do {
    if (!server->running) break;
    server->running = 0;

    cron_stop(server->cron);
    LOG_INFO("Stopping event loop");

    if (server->io_backend == IO_BACKEND_IOURING && server->io_ctx) {
      uring_backend_stop(server->io_ctx);
    } else if (server->base) {
      event_base_loopexit(server->base, 0);
    }
  } while (0);
  return 0;
}

// Close connection and return state to free list
static void conn_close(struct conn_state_t *state) {
  LOG_DEBUG("Closing connection fd=%d", state->fd);
  if (state->rev && event_get_base(state->rev)) event_del(state->rev);
  if (state->wev && event_get_base(state->wev)) event_del(state->wev);
  if (state->fd >= 0) {
    close(state->fd);
    state->fd = -1;
  }
  // Reset state
  state->rbuf_len = 0;
  state->rbuf_pos = 0;
  state->wbuf_len = 0;
  state->wbuf_pos = 0;
  state->pending_ref = NULL;
  state->pending_ref_len = 0;
  state->pending_ref_pos = 0;
  state->hdr_have = 0;
  state->key_have = 0;
  state->key_len = 0;
  state->action = 0;
  state->table_id = 0;
  state->index_id = 0;
  state->discarding = 0;
  // Return to free list
  Server* server = state->server;
  state->next = server->conn_free;
  server->conn_free = state;
}

// Write callback: flush pending data
static void on_write(evutil_socket_t fd, short events, void *ctx) {
  UNUSED(events);
  struct conn_state_t *state = ctx;

  // First flush write buffer
  while (state->wbuf_pos < state->wbuf_len) {
    ssize_t n = write(fd, state->wbuf + state->wbuf_pos,
                      state->wbuf_len - state->wbuf_pos);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return; // wait for next write event
      }
      conn_close(state);
      return;
    }
    state->wbuf_pos += n;
  }

  // Then flush pending reference (zero-copy arena data)
  while (state->pending_ref && state->pending_ref_pos < state->pending_ref_len) {
    ssize_t n = write(fd, state->pending_ref + state->pending_ref_pos,
                      state->pending_ref_len - state->pending_ref_pos);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return; // wait for next write event
      }
      conn_close(state);
      return;
    }
    state->pending_ref_pos += n;
  }

  // All done - disable write event, clear buffers
  event_del(state->wev);
  state->wbuf_len = 0;
  state->wbuf_pos = 0;
  state->pending_ref = NULL;
  state->pending_ref_len = 0;
  state->pending_ref_pos = 0;
}

// Queue response for writing, using writev for zero-copy when possible
static inline void queue_response(struct conn_state_t *state,
                                   const uint8_t *hdr, unsigned hdr_len,
                                   const uint8_t *data, unsigned data_len) {
  // Try immediate write with writev for zero-copy
  struct iovec iov[2];
  int iovcnt = 0;
  if (hdr && hdr_len) {
    iov[iovcnt].iov_base = (void*)hdr;
    iov[iovcnt].iov_len = hdr_len;
    iovcnt++;
  }
  if (data && data_len) {
    iov[iovcnt].iov_base = (void*)data;
    iov[iovcnt].iov_len = data_len;
    iovcnt++;
  }
  if (iovcnt == 0) return;

  ssize_t total = (hdr_len + data_len);
  ssize_t n = writev(state->fd, iov, iovcnt);

  if (n == total) {
    // Complete write - done
    return;
  }

  if (n < 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      conn_close(state);
      return;
    }
    n = 0; // treat as zero bytes written
  }

  // Partial write - buffer the rest
  unsigned written = (unsigned)n;

  // Buffer header remainder
  if (hdr && hdr_len) {
    if (written < hdr_len) {
      unsigned hdr_remain = hdr_len - written;
      memcpy(state->wbuf + state->wbuf_len, hdr + written, hdr_remain);
      state->wbuf_len += hdr_remain;
      written = 0;
    } else {
      written -= hdr_len;
    }
  }

  // Set up pending reference for remaining data
  if (data && data_len && written < data_len) {
    state->pending_ref = data + written;
    state->pending_ref_len = data_len - written;
    state->pending_ref_pos = 0;
  }

  // Enable write event
  if (state->wev) {
    event_add(state->wev, NULL);
  }
}

// Read callback: parse requests, send replies using direct I/O
static HOT_FUNC void on_read(evutil_socket_t fd, short events, void *ctx) {
  UNUSED(events);
  struct conn_state_t *state = ctx;
  Server* server = state->server;

  // Read into buffer
  ssize_t space = MELIAN_RBUF_SIZE - state->rbuf_len;
  if (space > 0) {
    ssize_t n = read(fd, state->rbuf + state->rbuf_len, space);
    if (n <= 0) {
      if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
        conn_close(state);
        return;
      }
      // EAGAIN - no data available yet
      if (state->rbuf_len == 0) return;
    } else {
      state->rbuf_len += n;
    }
  }

  // Process all complete requests in tight loop
  static const uint8_t zero_hdr[4] = {0};

  while (1) {
    unsigned avail = state->rbuf_len - state->rbuf_pos;

    // Step 1: Parse header
    if (state->hdr_have < sizeof(MelianRequestHeader)) {
      if (avail < sizeof(MelianRequestHeader)) break; // need more bytes

      const MelianRequestHeader *H = (const MelianRequestHeader *)(state->rbuf + state->rbuf_pos);
      if (unlikely(H->data.version != MELIAN_HEADER_VERSION)) {
        LOG_WARN("Invalid protocol version 0x%02x (expected 0x%02x), closing connection",
                 H->data.version, MELIAN_HEADER_VERSION);
        conn_close(state);
        return;
      }
      state->action = H->data.action;
      state->table_id = H->data.table_id;
      state->index_id = H->data.index_id;
      state->key_len = ntohl(H->data.length);
      state->rbuf_pos += sizeof(MelianRequestHeader);
      state->hdr_have = sizeof(MelianRequestHeader);
      state->discarding = unlikely(state->key_len > MELIAN_MAX_KEY_LEN);
      state->key_have = 0;
      avail = state->rbuf_len - state->rbuf_pos;
    }

    // Step 2: Handle key payload
    // For discarded (oversized) keys, consume bytes incrementally
    if (state->discarding) {
      unsigned to_consume = state->key_len - state->key_have;
      if (to_consume > avail) to_consume = avail;
      state->rbuf_pos += to_consume;
      state->key_have += to_consume;
      if (state->key_have < state->key_len) break; // need more bytes
    } else {
      // Normal case: wait for complete key
      if (avail < state->key_len) break;
    }

    const uint8_t *key_ptr = state->discarding ? NULL : (state->rbuf + state->rbuf_pos);

    // Step 3: Lookup & prepare response
    const uint8_t* rptr = NULL;
    unsigned rlen = 0;
    unsigned rfmt = 0;  // 1 = preframed (arena data)
    uint8_t len_hdr[4];

    if (unlikely(state->discarding)) {
      // Discarding oversized key - skip to response
    } else if (likely(state->action == MELIAN_ACTION_FETCH)) {
      // Hot path: FETCH action - use inline lookup
      const Bucket* bucket = data_fetch_inline(server->data, state->table_id,
                                                state->index_id, key_ptr, state->key_len);
      if (likely(bucket)) {
        rptr = bucket->frame_ptr;
        rlen = bucket->frame_len;
        rfmt = 1;
      }
    } else {
      // Cold path: non-FETCH actions
      switch (state->action) {
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

          const struct timeval one_sec = { 1, 0 };
          server->tev = evtimer_new(server->base, on_quit, server);
          evtimer_add(server->tev, &one_sec);
          break;
        }

        default:
          break;
      }
    }

    // Step 4: Send response
    if (likely(rptr && rlen)) {
      LOG_DEBUG("Writing response with %u bytes", rlen);
      if (unlikely(!rfmt)) {
        // Non-arena reply - need length header
        uint32_t l = htonl(rlen);
        memcpy(len_hdr, &l, 4);
        queue_response(state, len_hdr, 4, rptr, rlen);
      } else {
        // Arena data is preframed
        queue_response(state, NULL, 0, rptr, rlen);
      }
    } else {
      if (unlikely(state->action == MELIAN_ACTION_DESCRIBE_SCHEMA)) {
        LOG_WARN("Describe schema returned empty data");
      }
      LOG_DEBUG("Writing ZERO response");
      queue_response(state, zero_hdr, 4, NULL, 0);
    }

    // Consume key bytes (only for non-discarded keys; discarded already consumed above)
    if (!state->discarding) {
      state->rbuf_pos += state->key_len;
    }

    // Reset parse state for next request
    state->hdr_have = 0;
    state->key_have = 0;
    state->discarding = 0;
  }

  // Compact read buffer if needed
  if (state->rbuf_pos > 0) {
    unsigned remaining = state->rbuf_len - state->rbuf_pos;
    if (remaining > 0) {
      memmove(state->rbuf, state->rbuf + state->rbuf_pos, remaining);
    }
    state->rbuf_len = remaining;
    state->rbuf_pos = 0;
  }
}

// Accept callback: set up direct I/O for new client
static void on_accept(struct evconnlistener *lev, evutil_socket_t fd,
                      struct sockaddr *addr, int socklen, void *ctx) {
  UNUSED(lev);
  UNUSED(addr);
  UNUSED(socklen);

  // Set non-blocking
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);

#if __APPLE__
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

  Server* server = ctx;
  struct event_base *base = server->base;
  struct conn_state_t *state = NULL;

  if (server->conn_free) {
    state = server->conn_free;
    server->conn_free = server->conn_free->next;
    state->next = NULL;
    state->fd = fd;
    // Reset buffers
    state->rbuf_len = 0;
    state->rbuf_pos = 0;
    state->wbuf_len = 0;
    state->wbuf_pos = 0;
    state->pending_ref = NULL;
    state->pending_ref_len = 0;
    state->pending_ref_pos = 0;
    state->hdr_have = 0;
    state->key_have = 0;
    state->key_len = 0;
    state->action = 0;
    state->table_id = 0;
    state->index_id = 0;
    state->discarding = 0;
    LOG_DEBUG("REUSED conn state");
  } else {
    state = calloc(1, sizeof(struct conn_state_t));
    state->server = server;
    state->fd = fd;
    state->rev = event_new(base, fd, EV_READ | EV_PERSIST, on_read, state);
    state->wev = event_new(base, fd, EV_WRITE | EV_PERSIST, on_write, state);
    LOG_DEBUG("CREATED conn state");
  }

  // Update events for new fd (in case reused)
  if (state->rev) {
    event_del(state->rev);
    event_assign(state->rev, base, fd, EV_READ | EV_PERSIST, on_read, state);
  }
  if (state->wev) {
    event_del(state->wev);
    event_assign(state->wev, base, fd, EV_WRITE | EV_PERSIST, on_write, state);
  }

  event_add(state->rev, NULL);
}

static void on_quit(evutil_socket_t fd, short what, void *ctx) {
  UNUSED(fd);
  UNUSED(what);
  Server *server = ctx;
  server_stop(server);
}

static void on_signal(int signal, short events, void *ctx) {
  UNUSED(events);
  Server* server = ctx;
  LOG_INFO("Received signal %d, quitting", signal);
  server_stop(server);
}
