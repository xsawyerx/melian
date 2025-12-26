#include <assert.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
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
  MELIAN_MAX_KEY_LEN = 256, // max key length in bytes
};

// Bufferevent options: always close fd on free, and defer callbacks so libevent
// can batch work and avoid re-entrancy in hot paths.
#define MELIAN_BEV_OPTS (BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS)

// State for each client connection
struct conn_state_t {
  Server* server;
  MelianRequestHeader hdr;
  uint32_t hdr_have;
  uint8_t action;
  uint8_t table_id;
  uint8_t index_id;
  uint32_t key_len;
  uint8_t keybuf[MELIAN_MAX_KEY_LEN];
  uint32_t key_have;
  unsigned discarding;
  struct bufferevent *bev;
  struct conn_state_t* next;
};

static void on_read(struct bufferevent *bev, void *ctx);
static void on_event(struct bufferevent *bev, short events, void *ctx);
static void on_accept(struct evconnlistener *lev, evutil_socket_t fd,
                      struct sockaddr *addr, int socklen, void *ctx);
static void on_quit(evutil_socket_t fd, short what, void *ctx);
static void on_signal(int signal, short events, void *ctx);

Server* server_build(void) {
  Server* server = 0;
  unsigned bad = 0;
  do {
    server = calloc(1, sizeof(Server));
    if (!server) {
      LOG_WARN("Could not allocate a Server object");
      break;
    }
    server->base = event_base_new();
    if (!server->base) {
      LOG_WARN("Could not allocate a Server event_base object");
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

    server->status = status_build(server->base, server->db);
    if (!server->status) {
      ++bad;
      break;
    }
    status_log(server->status);

    server->sev = evsignal_new(server->base, SIGINT, on_signal, server);
    event_add(server->sev, NULL);
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

  unsigned size = 0;
  for (struct conn_state_t* p = server->conn_free; p; ) {
    ++size;
    struct conn_state_t* q = p;
    p = p->next;
    bufferevent_free(q->bev);
    free(q);
  }
  if (size) {
    LOG_INFO("Cleared conn free list with %u elements", size);
  }

  if (server->listeners) {
    for (size_t i = 0; i < server->num_listeners; i++) {
      evconnlistener_free(server->listeners[i]);
    }
    free(server->listeners);
  }
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
  ConfigSocket** sockets = server->config->listeners.sockets;
  if (!sockets || !sockets[0]) {
    LOG_WARN("Server has no configured sockets to listen on");
    return 0;
  }

  size_t num_sockets = 0;
  for(ConfigSocket* socket = sockets[num_sockets++]; socket; socket = sockets[num_sockets++]);
  server->listeners = calloc(num_sockets, sizeof(struct evconnlistener*));

  size_t i = 0;
  for(ConfigSocket* socket = sockets[i++]; socket; socket = sockets[i++]) {
    const char* path = socket->path;
    if (path && path[0]) {
      unlink(path);
      struct sockaddr_un sun;
      memset(&sun, 0, sizeof(sun));
      sun.sun_family = AF_UNIX;
      /* TODO: Check for truncation: if (... >= (int)sizeof(sun.sun_path)) {...} */
      snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", path);
      server->listeners[i] = evconnlistener_new_bind(server->base, on_accept, server,
                                                 LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
                                                 (struct sockaddr*)&sun, sizeof(sun));
      mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP; // 0660
      chmod(path, mode);
      LOG_INFO("Listening on UNIX socket [%s]", path);
    }

    const char* host = socket->host;
    unsigned port = socket->port;
    if (host && host[0] && port) {
      struct sockaddr_in sin;
      memset(&sin, 0, sizeof(sin));
      sin.sin_family = AF_INET;
      sin.sin_port = htons(port);
      inet_pton(AF_INET, host, &sin.sin_addr);
      unsigned flags = LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE | LEV_OPT_REUSEABLE_PORT;
      server->listeners[i] = evconnlistener_new_bind(server->base, on_accept, server, flags, -1,
                                                 (struct sockaddr*)&sin, sizeof(sin));
      LOG_INFO("Listening on TCP socket [%s:%u]", host, port);
    }
  }

  if (num_sockets == i) {
    return 1;
  }

  return 0;
}

unsigned server_run(Server* server) {
  do {
    if (server->running) break;
    server->running = 1;

    cron_run(server->cron);
    LOG_INFO("Running event loop");
    event_base_dispatch(server->base);
  } while (0);
  return 0;
}

unsigned server_stop(Server* server) {
  do {
    if (!server->running) break;
    server->running = 0;

    cron_stop(server->cron);
    LOG_INFO("Stopping event loop");
    event_base_loopexit(server->base, 0);
  } while (0);
  return 0;
}

// Read callback: parse requests, send replies
static void on_read(struct bufferevent *bev, void *ctx) {
  struct conn_state_t *state = ctx;
  Server* server = state->server;
  struct evbuffer *in = bufferevent_get_input(bev);
  struct evbuffer *out = bufferevent_get_output(bev);
  while (1) {
    // Step 1 (zero-copy): ensure full header is available, then parse in place
    if (state->hdr_have < sizeof(state->hdr)) {
      if (evbuffer_get_length(in) < sizeof(MelianRequestHeader)) return; // need more bytes
      const uint8_t *hp = evbuffer_pullup(in, sizeof(MelianRequestHeader));
      if (!hp) return; // defensive
      const MelianRequestHeader *H = (const MelianRequestHeader *)hp;
      assert(H->data.version == MELIAN_HEADER_VERSION);
      state->action = H->data.action;
      state->table_id = H->data.table_id;
      state->index_id = H->data.index_id;
      state->key_len = ntohl(H->data.length);
      evbuffer_drain(in, sizeof(MelianRequestHeader)); // consume header
      state->hdr_have = sizeof(MelianRequestHeader);
      state->discarding = (state->key_len > MELIAN_MAX_KEY_LEN);
      state->key_have = 0;
    }

    // Step 2 (zero-copy): ensure full key payload is available
    if (evbuffer_get_length(in) < state->key_len) {
      return; // wait for more bytes
    }
    const uint8_t *key_ptr = NULL;
    if (!state->discarding) {
      if (state->key_len > 0) {
        key_ptr = evbuffer_pullup(in, state->key_len);      // contiguous view of key
        if (!key_ptr) return;                               // defensive
      } else {
        key_ptr = (const uint8_t*)"";                       // no payload needed
      }
    }

    // Step 3: lookup & reply
    static const uint8_t zero_hdr[4] = {0};
    const uint8_t* rptr = 0;
    unsigned rlen = 0;
    unsigned rfmt = 0;
    unsigned tab = -1;
    if (!state->discarding) {
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
          rptr =  (uint8_t*)server->status->json.jbuf;
          rlen = server->status->json.jlen;
          break;
        }

        case MELIAN_ACTION_QUIT: {
          const char* bye =  "{\"BYE\":true}";
          rptr = (uint8_t*) bye;
          rlen = strlen(bye);

          const struct timeval one_sec = { 1, 0 }; // sec, usec
          server->tev = evtimer_new(server->base, on_quit, server);
          evtimer_add(server->tev, &one_sec);
          break;
        }

        case MELIAN_ACTION_FETCH:
          tab = state->table_id;
          break;

        default:
          break;
      }
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
    if (rptr && rlen) {
      LOG_DEBUG("Writing response with %u bytes", rlen);
      if (!rfmt) {
        uint32_t l = htonl(rlen);
        // Rare path: non-arena reply (e.g., status/QUIT). Add 4B length into evbuffer.
        evbuffer_add(out, &l, sizeof(l));
      }
      // Zero-copy send of arena-backed frame (or static buffer)
      evbuffer_add_reference(out, rptr, rlen, NULL, NULL);
    } else {
      switch (state->action) {
        case MELIAN_ACTION_FETCH: {
          unsigned key = 0;
          if (!state->discarding && state->key_len >= sizeof(unsigned) && key_ptr) {
            memcpy(&key, key_ptr, sizeof(unsigned));
          }
          break;
        }

        case MELIAN_ACTION_DESCRIBE_SCHEMA:
          LOG_WARN("Describe schema returned empty data");
          break;

        default:
          break;
      }
      LOG_DEBUG("Writing ZERO response");
      // Zero-copy reference to static 4B zero header
      evbuffer_add_reference(out, zero_hdr, sizeof(zero_hdr), NULL, NULL);
    }

    // Consume key bytes (or discarded payload) from input buffer
    evbuffer_drain(in, state->key_len);

    // Reset for next request
    state->hdr_have = 0;
    state->key_have = 0;
    state->discarding = 0;
  }
}

// Event callback: handle disconnects and errors
static void on_event(struct bufferevent *bev, short events, void *ctx) {
  UNUSED(bev);
  struct conn_state_t *state = ctx;
  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    LOG_DEBUG("Reusing state and bufferevent");
    bufferevent_setfd(state->bev, -1);
    Server* server = state->server;
    state->next = server->conn_free;
    server->conn_free = state;
  }
}

// Accept callback: create bufferevent for new client
static void on_accept(struct evconnlistener *lev, evutil_socket_t fd,
                      struct sockaddr *addr, int socklen, void *ctx) {
  UNUSED(lev);
  UNUSED(addr);
  UNUSED(socklen);
#if __APPLE__
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
  Server* server = ctx;
  struct event_base *base = server->base;
  struct conn_state_t *state = 0;
  if (server->conn_free) {
    state = server->conn_free;
    server->conn_free = server->conn_free->next;
    state->next = 0;
    bufferevent_setfd(state->bev, fd);
    LOG_DEBUG("REUSED conn and bev");
  } else {
    state = calloc(1, sizeof(struct conn_state_t));
    state->server = server;
    state->bev = bufferevent_socket_new(base, fd, MELIAN_BEV_OPTS);
    LOG_DEBUG("CREATED conn and bev");
  }
  bufferevent_setcb(state->bev, on_read, NULL, on_event, state);
  bufferevent_enable(state->bev, EV_READ | EV_WRITE);
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
