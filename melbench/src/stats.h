#pragma once
#include "hist.h"
#include <stdint.h>

typedef struct {
  uint64_t requests;
  uint64_t responses;
  uint64_t errors;
  uint64_t timeouts;
  uint64_t connect_errors;
  hist_t hist;
} thread_stats_t;

