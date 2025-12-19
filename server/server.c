#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "event_loop.h"
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
#include "server.h"

enum {
  MELIAN_MAX_KEY_LEN = 65536,
  MELIAN_INLINE_KEY = 1024,
};

static const uint8_t ZERO_HDR[4] = {0};

// Buffers for each connection. Keep fixed fields together for cache locality.
struct conn_state_t {
  Server* server;
  int fd;
  MelianRequestHeader hdr;
  uint32_t hdr_have;
  uint8_t action;
  uint8_t table_id;
  uint8_t index_id;
  uint32_t key_len;
  uint32_t key_have;
  uint8_t* keybuf;
  uint32_t key_cap;
  uint8_t inline_key[MELIAN_INLINE_KEY];
  unsigned discarding;
  struct conn_state_t* next;

  // Write-side bookkeeping
  struct iovec outv[2];
  size_t out_count;
  size_t out_idx;
  size_t out_off;
  uint8_t hdr_buf[sizeof(MelianResponseHeader)];
};

static Server* g_server;

static void on_listener_ready(int fd, uint32_t events, void *arg);
static void on_conn_ready(int fd, uint32_t events, void *arg);
static void on_control_ready(int fd, uint32_t events, void *arg);
static void install_signal_handlers(Server* server);
static void signal_trampoline(int sig);

static int set_nonblock(int fd);
static int tune_socket(int fd);
static int create_tcp_listener(Server* server);
static int create_unix_listener(Server* server);
static int accept_one(Server* server, int lfd);
static void recycle_conn(Server* server, struct conn_state_t* state);
static int conn_read(struct conn_state_t* state);
static int conn_flush(struct conn_state_t* state);
static int conn_handle_request(struct conn_state_t* state, const uint8_t* key_ptr);

Server* server_build(void) {
  Server* server = 0;
  unsigned bad = 0;
  do {
    server = calloc(1, sizeof(Server));
    if (!server) {
      LOG_WARN("Could not allocate a Server object");
      break;
    }
    server->tcp_fd = server->unix_fd = -1;
    server->ctrl_pipe[0] = server->ctrl_pipe[1] = -1;

    server->loop = calloc(1, sizeof(struct MelLoop));
    if (!server->loop) {
      LOG_WARN("Could not allocate event loop object");
      ++bad;
      break;
    }
    MelLoopConfig cfg = {
      .fd_hint = 2048,
      .force_epoll = 0,
      .prefer_uring = 1,
    };
    if (mel_loop_init(server->loop, &cfg) < 0) {
      LOG_WARN("Could not initialize event loop");
      ++bad;
      break;
    }
    snprintf(server->loop_backend, sizeof(server->loop_backend), "%s",
             mel_loop_backend_name(server->loop));

    if (pipe(server->ctrl_pipe) < 0) {
      LOG_WARN("Failed to create control pipe: %s", strerror(errno));
      ++bad;
      break;
    }
    set_nonblock(server->ctrl_pipe[0]);
    set_nonblock(server->ctrl_pipe[1]);
    if (mel_loop_add(server->loop, server->ctrl_pipe[0], MEL_LOOP_READ,
                     on_control_ready, server) < 0) {
      LOG_WARN("Failed to watch control pipe");
      ++bad;
      break;
    }

    server->config = config_build();
    if (!server->config) {
      ++bad;
      break;
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

    server->status = status_build(server->loop_backend, server->db);
    if (!server->status) {
      ++bad;
      break;
    }
    status_log(server->status);

    install_signal_handlers(server);
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

  if (server->tcp_fd >= 0) close(server->tcp_fd);
  if (server->unix_fd >= 0) close(server->unix_fd);
  if (server->ctrl_pipe[0] >= 0) close(server->ctrl_pipe[0]);
  if (server->ctrl_pipe[1] >= 0) close(server->ctrl_pipe[1]);

  unsigned size = 0;
  for (struct conn_state_t* p = server->conn_free; p; ) {
    ++size;
    struct conn_state_t* q = p;
    p = p->next;
    if (q->fd >= 0) close(q->fd);
    free(q);
  }
  if (size) {
    LOG_INFO("Cleared conn free list with %u elements", size);
  }

  if (server->cron) cron_destroy(server->cron);
  if (server->data) data_destroy(server->data);
  if (server->db) db_destroy(server->db);
  if (server->status) status_destroy(server->status);
  if (server->config) config_destroy(server->config);
  mel_loop_close(server->loop);
  free(server->loop);
  free(server);
}

unsigned server_initial_load(Server* server) {
  unsigned total_rows = data_load_all_tables_from_db(server->data, server->db);
  return total_rows > 0;
}

unsigned server_listen(Server* server) {
  unsigned ok = 0;
  const char* path = server->config->socket.path;
  const char* host = server->config->socket.host;
  unsigned port = server->config->socket.port;

  if (path && path[0]) {
    if (create_unix_listener(server)) ok = 1;
  }
  if (host && host[0] && port) {
    if (create_tcp_listener(server)) ok = 1;
  }
  if (!ok) {
    LOG_WARN("No listener configured");
  }
  return ok;
}

unsigned server_run(Server* server) {
  do {
    if (server->running) break;
    server->running = 1;

    cron_run(server->cron);
    LOG_INFO("Running event loop (%s)", server->loop_backend);
    mel_loop_run(server->loop);
  } while (0);
  return 0;
}

unsigned server_stop(Server* server) {
  do {
    if (!server->running) break;
    server->running = 0;
    cron_stop(server->cron);
    mel_loop_stop(server->loop);
    LOG_INFO("Stopping event loop");
  } while (0);
  return 0;
}

static void on_control_ready(int fd, uint32_t events, void *arg) {
  UNUSED(events);
  Server* server = arg;
  uint8_t buf[16];
  while (read(fd, buf, sizeof(buf)) > 0) {}
  server_stop(server);
}

static void install_signal_handlers(Server* server) {
  g_server = server;
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_trampoline;
  sigfillset(&sa.sa_mask);
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
}

static void signal_trampoline(int sig) {
  UNUSED(sig);
  if (!g_server) return;
  uint8_t b = 1;
  write(g_server->ctrl_pipe[1], &b, 1);
}

static int create_tcp_listener(Server* server) {
  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons(server->config->socket.port);
  if (inet_pton(AF_INET, server->config->socket.host, &sin.sin_addr) != 1) {
    LOG_WARN("Invalid host %s", server->config->socket.host);
    return 0;
  }
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_WARN("Failed to create TCP socket: %s", strerror(errno));
    return 0;
  }
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
  if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
    LOG_WARN("Failed to bind TCP socket: %s", strerror(errno));
    close(fd);
    return 0;
  }
  if (listen(fd, 1024) < 0) {
    LOG_WARN("listen() failed: %s", strerror(errno));
    close(fd);
    return 0;
  }
  set_nonblock(fd);
  server->tcp_fd = fd;
  mel_loop_add(server->loop, fd, MEL_LOOP_READ, on_listener_ready, server);
  LOG_INFO("Listening on TCP socket [%s:%u]", server->config->socket.host,
           server->config->socket.port);
  return 1;
}

static int create_unix_listener(Server* server) {
  const char* path = server->config->socket.path;
  unlink(path);
  struct sockaddr_un sun;
  memset(&sun, 0, sizeof(sun));
  sun.sun_family = AF_UNIX;
  snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", path);
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_WARN("Failed to create UNIX socket: %s", strerror(errno));
    return 0;
  }
  if (bind(fd, (struct sockaddr*)&sun, sizeof(sun)) < 0) {
    LOG_WARN("Failed to bind UNIX socket: %s", strerror(errno));
    close(fd);
    return 0;
  }
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
  chmod(path, mode);
  if (listen(fd, 1024) < 0) {
    LOG_WARN("listen() failed: %s", strerror(errno));
    close(fd);
    return 0;
  }
  set_nonblock(fd);
  server->unix_fd = fd;
  mel_loop_add(server->loop, fd, MEL_LOOP_READ, on_listener_ready, server);
  LOG_INFO("Listening on UNIX socket [%s]", path);
  return 1;
}

static void on_listener_ready(int fd, uint32_t events, void *arg) {
  UNUSED(events);
  Server* server = arg;
  while (accept_one(server, fd)) {}
}

static int accept_one(Server* server, int lfd) {
  struct sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int fd = accept(lfd, (struct sockaddr*)&ss, &slen);
  if (fd < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
    if (errno == EINTR) return 1;
    LOG_WARN("accept() failed: %s", strerror(errno));
    return 0;
  }
  if (set_nonblock(fd) < 0) {
    close(fd);
    return 1;
  }
  tune_socket(fd);
#if __APPLE__
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
  struct conn_state_t* state = 0;
  if (server->conn_free) {
    state = server->conn_free;
    server->conn_free = server->conn_free->next;
    memset(state, 0, sizeof(*state));
  } else {
    state = calloc(1, sizeof(struct conn_state_t));
  }
  state->server = server;
  state->fd = fd;
  state->keybuf = state->inline_key;
  state->key_cap = MELIAN_INLINE_KEY;
  if (mel_loop_add(server->loop, fd, MEL_LOOP_READ, on_conn_ready, state) < 0) {
    LOG_WARN("Failed to add connection to loop");
    close(fd);
    free(state);
  }
  return 1;
}

static void on_conn_ready(int fd, uint32_t events, void *arg) {
  UNUSED(fd);
  struct conn_state_t* state = arg;
  if (events & (MEL_LOOP_HUP | MEL_LOOP_ERR)) {
    recycle_conn(state->server, state);
    return;
  }
  if (events & MEL_LOOP_WRITE) {
    if (!conn_flush(state)) {
      recycle_conn(state->server, state);
      return;
    }
  }
  if (events & MEL_LOOP_READ) {
    if (!conn_read(state)) {
      recycle_conn(state->server, state);
      return;
    }
  }
}

static void recycle_conn(Server* server, struct conn_state_t* state) {
  mel_loop_del(server->loop, state->fd);
  close(state->fd);
  state->fd = -1;
  if (state->keybuf && state->keybuf != state->inline_key) {
    free(state->keybuf);
  }
  state->hdr_have = state->key_have = 0;
  state->key_len = 0;
  state->discarding = 0;
  state->out_count = state->out_idx = state->out_off = 0;
  state->keybuf = state->inline_key;
  state->key_cap = MELIAN_INLINE_KEY;
  state->next = server->conn_free;
  server->conn_free = state;
}

static int ensure_keybuf(struct conn_state_t* state, uint32_t need) {
  if (need <= state->key_cap) return 1;
  if (need > MELIAN_MAX_KEY_LEN) return 0;
  uint8_t* nb = malloc(need);
  if (!nb) return 0;
  state->keybuf = nb;
  state->key_cap = need;
  return 1;
}

static int conn_read(struct conn_state_t* state) {
  uint8_t buf[4096];
  while (1) {
    ssize_t n = recv(state->fd, buf, sizeof(buf), 0);
    if (n == 0) return 0;
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return 1;
      if (errno == EINTR) continue;
      return 0;
    }
    size_t off = 0;
    while (off < (size_t)n) {
      // Step 1: read header
      if (state->hdr_have < sizeof(MelianRequestHeader)) {
        size_t need = sizeof(MelianRequestHeader) - state->hdr_have;
        size_t take = need < (size_t)n - off ? need : (size_t)n - off;
        memcpy(state->hdr.bytes + state->hdr_have, buf + off, take);
        state->hdr_have += take;
        off += take;
        if (state->hdr_have < sizeof(MelianRequestHeader)) continue;
        assert(state->hdr.data.version == MELIAN_HEADER_VERSION);
        state->action = state->hdr.data.action;
        state->table_id = state->hdr.data.table_id;
        state->index_id = state->hdr.data.index_id;
        state->key_len = ntohl(state->hdr.data.length);
        state->discarding = (state->key_len > MELIAN_MAX_KEY_LEN);
        state->key_have = 0;
        if (!state->discarding && state->key_len > 0) {
          if (!ensure_keybuf(state, state->key_len)) {
            state->discarding = 1;
          }
        }
      }

      // Step 2: key payload
      uint32_t remaining = state->key_len - state->key_have;
      size_t chunk = remaining;
      size_t avail = (size_t)n - off;
      if (chunk > avail) chunk = avail;
      if (!state->discarding && chunk) {
        memcpy(state->keybuf + state->key_have, buf + off, chunk);
      }
      state->key_have += chunk;
      off += chunk;
      if (state->key_have < state->key_len) continue;

      // Step 3: process request
      const uint8_t* key_ptr = state->discarding ? NULL : state->keybuf;
      if (!state->discarding && state->key_len == 0) key_ptr = (const uint8_t*)"";
      if (!conn_handle_request(state, key_ptr)) return 0;

      // Reset for next request
      state->hdr_have = 0;
      state->key_have = 0;
      state->key_len = 0;
      state->discarding = 0;
    }
  }
}

static int conn_handle_request(struct conn_state_t* state, const uint8_t* key_ptr) {
  Server* server = state->server;
  const uint8_t* rptr = 0;
  unsigned rlen = 0;
  unsigned rfmt = 0;
  unsigned tab = (unsigned)-1;
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
      rptr = (const uint8_t*)bye;
      rlen = strlen(bye);
      server_stop(server);
      break;
    }
    case MELIAN_ACTION_FETCH:
      tab = state->table_id;
      break;
    default:
      break;
  }

  if (tab != (unsigned)-1) {
    const Bucket* bucket = data_fetch(server->data, tab, state->index_id, key_ptr, state->key_len);
    if (bucket) {
      const Table* table = server->data->lookup[tab];
      const Arena* arena = table->slots[table->current_slot].arena;
      rptr = arena_get_ptr(arena, bucket->frame_idx);
      rlen = bucket->frame_len;
      rfmt = 1;
    }
  }

  state->out_idx = state->out_off = 0;
  state->out_count = 0;
  if (rptr && rlen) {
    if (!rfmt) {
      uint32_t l = htonl(rlen);
      memcpy(state->hdr_buf, &l, sizeof(l));
      state->outv[0].iov_base = state->hdr_buf;
      state->outv[0].iov_len = sizeof(l);
      state->outv[1].iov_base = (void*)rptr;
      state->outv[1].iov_len = rlen;
      state->out_count = 2;
    } else {
      state->outv[0].iov_base = (void*)rptr;
      state->outv[0].iov_len = rlen;
      state->out_count = 1;
    }
  } else {
    state->outv[0].iov_base = (void*)ZERO_HDR;
    state->outv[0].iov_len = sizeof(ZERO_HDR);
    state->out_count = 1;
  }
  if (!conn_flush(state)) return 0;
  return 1;
}

static int conn_flush(struct conn_state_t* state) {
  if (!state->out_count) return 1;
  Server* server = state->server;
  while (state->out_idx < state->out_count) {
    struct iovec iov[2];
    int iovcnt = 0;
    for (size_t i = state->out_idx; i < state->out_count && iovcnt < 2; ++i) {
      iov[iovcnt].iov_base = (uint8_t*)state->outv[i].iov_base + (i == state->out_idx ? state->out_off : 0);
      iov[iovcnt].iov_len = state->outv[i].iov_len - (i == state->out_idx ? state->out_off : 0);
      ++iovcnt;
    }
    ssize_t n = writev(state->fd, iov, iovcnt);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        mel_loop_mod(server->loop, state->fd, MEL_LOOP_READ | MEL_LOOP_WRITE);
        return 1;
      }
      return 0;
    }
    size_t sent = (size_t)n;
    while (sent > 0 && state->out_idx < state->out_count) {
      size_t remain = state->outv[state->out_idx].iov_len - state->out_off;
      if (sent < remain) {
        state->out_off += sent;
        sent = 0;
      } else {
        sent -= remain;
        state->out_idx++;
        state->out_off = 0;
      }
    }
  }
  state->out_count = state->out_idx = state->out_off = 0;
  mel_loop_mod(server->loop, state->fd, MEL_LOOP_READ);
  return 1;
}

static int set_nonblock(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
  return 0;
}

static int tune_socket(int fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  return 0;
}
