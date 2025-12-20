#pragma once
#include <stdint.h>
#include <stddef.h>

typedef struct {
  // Log2 buckets over microseconds:
  // bucket 0 => [0..1]
  // bucket 1 => (1..2]
  // ...
  // bucket 63 => huge
  uint64_t buckets[64];
  uint64_t count;
  uint64_t min_us;
  uint64_t max_us;
  long double sum_us;
  long double sumsq_us;
} hist_t;

void hist_init(hist_t *h);
void hist_record_us(hist_t *h, uint64_t us);
double hist_percentile_us(const hist_t *h, double p); // p in [0..100]
void hist_merge(hist_t *dst, const hist_t *src);
double hist_mean_us(const hist_t *h);
double hist_stddev_us(const hist_t *h);
