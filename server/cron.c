#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <sys/socket.h>
#include <pthread.h>
#include <unistd.h>
#include "util.h"
#include "log.h"
#include "data.h"
#include "server.h"
#include "cron.h"

enum {
  CRON_TICK_PERIOD = 5,
};

enum ThreadMessage {
  THREAD_MESSAGE_WAKEUP = 'W',
  THREAD_MESSAGE_QUIT = 'Q',
};

static void poke_thread(Cron* cron, uint8_t message);
static void* loader_main(void *arg);

Cron* cron_build(struct Server* server) {
  Cron* cron = 0;
  do {
    cron = calloc(1, sizeof(Cron));
    if (!cron) {
      LOG_WARN("Could not allocate Cron object");
      break;
    }
    cron->server = server;
  } while (0);
  return cron;
}

void cron_destroy(Cron* cron) {
  if (!cron) return;
  cron_stop(cron);

  free(cron);
}

unsigned cron_run(Cron* cron) {
  do {
    if (cron->running) break;
    cron->running = 1;

    LOG_INFO("Starting up cron");
    // TODO: do we need to call shutdown() on each socket?
    socketpair(AF_UNIX, SOCK_STREAM, 0, cron->pair);
    // evutil_make_socket_nonblocking(cron->pair[0]);
    // evutil_make_socket_nonblocking(cron->pair[1]);

    pthread_t thread;
    pthread_create(&thread, 0, loader_main, cron);
    pthread_detach(thread);
    cron->thread = (void*) thread;
  } while (0);
  return 1;
}

unsigned cron_stop(Cron* cron) {
  do {
    if (!cron->running) break;
    cron->running = 0;

    if (!cron->thread) break;
    poke_thread(cron, THREAD_MESSAGE_QUIT);
    sleep(1); // TODO: needed?
    LOG_DEBUG("Poked thread to quit");
    pthread_t thread = (pthread_t) cron->thread;
    pthread_join(thread, 0);
    LOG_DEBUG("Joined thread");
    cron->thread = 0;
  } while (0);
  return 1;
}

static void poke_thread(Cron* cron, uint8_t message) {
  ssize_t wrote = 0;
  do {
    wrote = write(cron->pair[1], &message, 1);
  } while (wrote < 0 && errno == EINTR);

  if (wrote < 0) {
    LOG_ERROR("Failed to poke cron thread: %s", strerror(errno));
  } else if (wrote != 1) {
    LOG_ERROR("Failed to deliver cron message, wrote %zd bytes", wrote);
  }
}

static void* loader_main(void *arg) {
  Cron* cron = arg;
  LOG_INFO("THREAD: running loader, cron: %p", (void*)cron);
  while (1) {
    struct pollfd pfd;
    pfd.fd = cron->pair[0];
    pfd.events = POLLIN;
    pfd.revents = 0;
    int rc = poll(&pfd, 1, CRON_TICK_PERIOD * 1000);
    if (rc < 0) {
      if (errno == EINTR) continue;
      LOG_ERROR("THREAD: poll failed: %s", strerror(errno));
      break;
    }
    if (rc > 0 && (pfd.revents & POLLIN)) {
      uint8_t b;
      int nread = read(cron->pair[0], &b, 1);
      LOG_DEBUG("THREAD: read %u bytes: %c", nread, b);
      if (nread != 1) break;
      if (b == THREAD_MESSAGE_QUIT) {
        LOG_DEBUG("THREAD: got a quit message");
        break;
      }
    } else {
      LOG_DEBUG("THREAD: tick timeout");
    }
    LOG_DEBUG("THREAD: woke up");
    data_load_all_tables_from_db(cron->server->data, cron->server->db);
  }
  LOG_INFO("THREAD: stopping, cron: %p", (void*)cron);
  return 0;
}
