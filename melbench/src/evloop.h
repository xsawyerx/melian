#pragma once

#include "args.h"
#include "proto.h"
#include "hist.h"
#include "stats.h"

int evloop_run_benchmark_thread(
  int thread_index,
  const bench_args_t *args,
  const proto_plan_t *plan,
  const char* dsn,
  thread_stats_t *out_stats);
