#pragma once

// A Cron has an ongoing clock tick which periodically wakes up a thread.
// This thread performs the work of reloading data from MySQL.

typedef struct Cron {
  struct Server* server;
  void* thread;
  unsigned period_sec;
  unsigned running;
} Cron;

Cron* cron_build(struct Server* server);
void cron_destroy(Cron* cron);
unsigned cron_run(Cron* cron);
unsigned cron_stop(Cron* cron);
