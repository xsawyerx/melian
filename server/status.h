#pragma once

// A Status gathers data about the server status.
// It can either log this data or format it as JSON.

// TODO: make these limits dynamic? Arena?
enum {
  MAX_JSON_LEN = 10240,
  MAX_STR_LEN = 1024,
};

struct Config;
struct DB;
struct Data;

typedef struct StatusServer {
  char host[MAX_STR_LEN];
  char system[MAX_STR_LEN];
  char release[MAX_STR_LEN];
  char machine[MAX_STR_LEN];
} StatusServer;

typedef struct StatusLibevent {
  char version[MAX_STR_LEN];
  char method[MAX_STR_LEN];
} StatusLibevent;

typedef struct StatusProcess {
  unsigned pid;
  unsigned birth;
} StatusProcess;

typedef struct StatusJson {
  char jbuf[MAX_JSON_LEN];
  unsigned jlen;
} StatusJson;

typedef struct Status {
  struct DB* db;
  StatusProcess process;
  StatusServer server;
  StatusLibevent libevent;
  StatusJson json;
} Status;

Status* status_build(const char* loop_version, const char* loop_method, struct DB* db);
void status_destroy(Status* status);
void status_log(Status* status);
void status_json(Status* status, struct Config* config, struct Data* data);
