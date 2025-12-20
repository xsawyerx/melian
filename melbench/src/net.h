#pragma once
#include <stdint.h>

typedef enum { DSN_UNIX = 0, DSN_TCP = 1 } dsn_kind_t;

typedef struct {
  dsn_kind_t kind;
  char host[128];
  int port;
  char path[256];
} dsn_t;

int dsn_parse(const char *dsn_str, dsn_t *out);
int net_connect_nonblocking(const dsn_t *dsn);
int net_set_nodelay(int fd, int enabled);
int net_set_nonblocking(int fd);
int net_check_connect(const dsn_t *dsn, int timeout_ms); // blocking availability probe
