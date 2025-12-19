#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include "util.h"
#include "log.h"
#include "data.h"
#include "server.h"
#include "cron.h"

enum {
  CRON_TICK_PERIOD = 5, // seconds
};

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
    cron->period_sec = CRON_TICK_PERIOD;
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
    pthread_t thread;
    pthread_create(&thread, 0, loader_main, cron);
    cron->thread = (void*) thread;
  } while (0);
  return 1;
}

unsigned cron_stop(Cron* cron) {
  do {
    if (!cron->running) break;
    cron->running = 0;

    if (cron->thread) {
      pthread_t thread = (pthread_t) cron->thread;
      pthread_join(thread, 0);
      cron->thread = 0;
      LOG_DEBUG("Joined cron thread");
    }
  } while (0);
  return 1;
}

static void* loader_main(void *arg) {
  Cron* cron = arg;
  LOG_INFO("THREAD: running loader, cron: %p", (void*)cron);
  while (1) {
    for (unsigned i = 0; i < cron->period_sec; ++i) {
      if (!cron->running) break;
      sleep(1);
    }
    if (!cron->running) break;
    data_load_all_tables_from_db(cron->server->data, cron->server->db);
  }
  LOG_INFO("THREAD: stopping, cron: %p", (void*)cron);
  return 0;
}
