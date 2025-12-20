#include "hist.h"
#include <string.h>
#include <math.h>

static int bucket_for_us(uint64_t us) {
  if (us <= 1) return 0;
  int b = 0;
  uint64_t v = us;
  while (v > 1 && b < 63) { v >>= 1; b++; }
  return b;
}

void hist_init(hist_t *h) {
  memset(h, 0, sizeof(*h));
  h->min_us = UINT64_MAX;
  h->max_us = 0;
}

void hist_record_us(hist_t *h, uint64_t us) {
  int b = bucket_for_us(us);
  h->buckets[b]++;
  h->count++;
  h->sum_us += (long double)us;
  h->sumsq_us += (long double)us * (long double)us;
  if (us < h->min_us) h->min_us = us;
  if (us > h->max_us) h->max_us = us;
}

void hist_merge(hist_t *dst, const hist_t *src) {
  if (!src->count) return;
  for (int i = 0; i < 64; i++) dst->buckets[i] += src->buckets[i];
  dst->count += src->count;
  if (src->min_us < dst->min_us) dst->min_us = src->min_us;
  if (src->max_us > dst->max_us) dst->max_us = src->max_us;
  dst->sum_us += src->sum_us;
  dst->sumsq_us += src->sumsq_us;
}

double hist_percentile_us(const hist_t *h, double p) {
  if (h->count == 0) return 0.0;
  if (p <= 0.0) return (double)h->min_us;
  if (p >= 100.0) return (double)h->max_us;

  uint64_t target = (uint64_t)((p / 100.0) * (double)h->count);
  if (target == 0) target = 1;

  uint64_t cum = 0;
  for (int i = 0; i < 64; i++) {
    cum += h->buckets[i];
    if (cum >= target) {
      // approximate: return upper bound of bucket
      uint64_t upper = (i == 0) ? 1 : (1ull << i);
      return (double)upper;
    }
  }
  return (double)h->max_us;
}

double hist_mean_us(const hist_t *h) {
  if (!h->count) return 0.0;
  return (double)(h->sum_us / (long double)h->count);
}

double hist_stddev_us(const hist_t *h) {
  if (!h->count) return 0.0;
  long double mean = h->sum_us / (long double)h->count;
  long double var = (h->sumsq_us / (long double)h->count) - (mean * mean);
  if (var < 0) var = 0;
  return (double)sqrt(var);
}
