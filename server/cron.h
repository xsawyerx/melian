#pragma once

// A Cron has an ongoing clock tick which periodically wakes up a thread.
// This thread performs the work of reloading data from MySQL.

typedef struct Cron {
  struct event_base *base;
  struct event *tick;
  int pair[2];
  struct Server* server;
  void* thread;
  unsigned running;
} Cron;

Cron* cron_build(struct Server* server);
void cron_destroy(Cron* cron);
unsigned cron_run(Cron* cron);
unsigned cron_stop(Cron* cron);

// Poke the cron thread to wake up and reload data.
// Used by io_uring backend's timer implementation.
void cron_poke(Cron* cron);
