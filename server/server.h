#pragma once

// A Server embodies the Melian server.

struct conn_state_t;

// A running server.
typedef struct Server {
  struct event_base *base;
  struct evconnlistener **listeners;
  unsigned num_listeners;
  struct event *tev;
  struct event *sev;
  struct Config* config;
  struct Status* status;
  struct Data* data;
  struct DB* db;
  struct Cron* cron;
  struct conn_state_t* conn_free;
  unsigned running;
} Server;

Server* server_build(void);
void server_destroy(Server* server);

unsigned server_initial_load(Server* server);
unsigned server_listen(Server* server);
unsigned server_run(Server* server);
unsigned server_stop(Server* server);
