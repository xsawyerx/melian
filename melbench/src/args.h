#pragma once
#include "proto.h"
#include <stdint.h>

typedef enum { PROTO_MELIAN = 0, PROTO_REDIS = 1 } proto_kind_t;

typedef struct {
  char label[64];
  proto_kind_t proto;
  char dsn[256];          // unix:///path or tcp://host:port
} bench_target_t;

typedef struct {
  // Targets to benchmark (sequential)
  bench_target_t targets[8];
  int target_count;

  int threads;            // worker threads
  int conns_per_thread;   // active connections per thread (derived per sweep)
  int total_concurrency;  // optional for sweeps
  int duration_ms;
  int warmup_ms;
  int runs;               // repeat per target/concurrency

  // Melian-specific (ignored for Redis)
  uint8_t melian_action;  // default 'F'
  uint8_t table_id;
  uint8_t column_id;

  // Key
  key_type_t key_type;
  char key_str[256];
  int64_t key_int;

  // Timeouts
  int io_timeout_ms;      // per-request timeout

  // Concurrency sweep
  int sweep_count;
  int sweep_concurrency[16]; // total connections
} bench_args_t;

int args_parse(int argc, char **argv, bench_args_t *out);
void args_print_usage(const char *prog);
