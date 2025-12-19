#pragma once

// A Server embodies the Melian server.

struct conn_state_t;
struct MelLoop;

#include "event_loop.h"

// A running server.
typedef struct Server {
  struct MelLoop *loop;
  int tcp_fd;
  int unix_fd;
  int ctrl_pipe[2];
  struct Config* config;
  struct Status* status;
  struct Data* data;
  struct DB* db;
  struct Cron* cron;
  char loop_backend[32];
  struct conn_state_t* conn_free;
  unsigned running;
} Server;

Server* server_build(void);
void server_destroy(Server* server);

unsigned server_initial_load(Server* server);
unsigned server_listen(Server* server);
unsigned server_run(Server* server);
unsigned server_stop(Server* server);
