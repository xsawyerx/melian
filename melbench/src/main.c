#include "args.h"
#include "proto.h"
#include "evloop.h"
#include "stats.h"
#include "net.h"
#include "hist.h"
#include "timeutil.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

typedef struct {
  int idx;
  const bench_args_t *args;
  const proto_plan_t *plan;
  const char *dsn;
  thread_stats_t stats;
  int rc;
} thr_ctx_t;

typedef struct {
  char label[64];
  char proto[8];
  int total_conns;
  double rps;
  double p50, p95, p99;
  double mean, stddev, cv;
} summary_t;

static void *thr_main(void *p) {
  thr_ctx_t *ctx = (thr_ctx_t*)p;
  ctx->rc = evloop_run_benchmark_thread(ctx->idx, ctx->args, ctx->plan, ctx->dsn, &ctx->stats);
  return NULL;
}

int main(int argc, char **argv) {
  bench_args_t args;
  int prc = args_parse(argc, argv, &args);
  if (prc != 0) return prc > 0 ? 0 : 2;

  // Determine concurrency sweep
  int sweep_vals[16];
  int sweep_n = 0;
  if (args.sweep_count > 0) {
    sweep_n = args.sweep_count;
    for (int i = 0; i < sweep_n; i++) sweep_vals[i] = args.sweep_concurrency[i];
  } else {
    sweep_vals[0] = args.threads * args.conns_per_thread;
    sweep_n = 1;
  }

  int bad = 0;
  summary_t summaries[64];
  int summaries_n = 0;

  for (int ti = 0; ti < args.target_count; ti++) {
    bench_target_t *t = &args.targets[ti];

    // Connectivity check (fail fast)
    dsn_t dsn;
    if (dsn_parse(t->dsn, &dsn) != 0 || net_check_connect(&dsn, 500) != 0) {
      fprintf(stderr, "Target %s not reachable at %s\n", t->label, t->dsn);
      bad = 1;
      continue;
    }

    proto_plan_t plan;
    memset(&plan, 0, sizeof(plan));
    int rc = 0;
    const char *k = (args.key_type == KEY_STRING) ? args.key_str : NULL;
    if (t->proto == PROTO_MELIAN) {
      rc = proto_melian_build_plan(&plan,
        args.melian_action, args.table_id, args.column_id,
        args.key_type, k, args.key_int);
    } else {
      rc = proto_redis_build_plan(&plan, args.key_type, k, args.key_int);
    }
    if (rc != 0) {
      fprintf(stderr, "Failed to build request plan for target %s: %d\n", t->label, rc);
      bad = 1;
      continue;
    }

    for (int si = 0; si < sweep_n; si++) {
      int total_conns = sweep_vals[si];
      int per_thread = total_conns / args.threads;
      if (per_thread < 1) per_thread = 1;

      printf("=== target=%s proto=%s dsn=%s total_conns=%d threads=%d conns/thread=%d runs=%d ===\n",
        t->label, t->proto == PROTO_MELIAN ? "melian" : "redis", t->dsn,
        total_conns, args.threads, per_thread, args.runs);
      thread_stats_t agg;
      memset(&agg, 0, sizeof(agg));
      hist_init(&agg.hist);

      for (int run = 0; run < args.runs; run++) {
        bench_args_t run_args = args;
        run_args.conns_per_thread = per_thread;

        int T = run_args.threads;
        pthread_t *ths = (pthread_t*)calloc((size_t)T, sizeof(pthread_t));
        thr_ctx_t *ctxs = (thr_ctx_t*)calloc((size_t)T, sizeof(thr_ctx_t));
        if (!ths || !ctxs) { fprintf(stderr, "OOM\n"); bad = 1; break; }

        for (int i = 0; i < T; i++) {
          ctxs[i].idx = i;
          ctxs[i].args = &run_args;
          ctxs[i].plan = &plan;
          ctxs[i].dsn = t->dsn;
          ctxs[i].rc = 0;
          pthread_create(&ths[i], NULL, thr_main, &ctxs[i]);
        }
        for (int i = 0; i < T; i++) pthread_join(ths[i], NULL);

        for (int i = 0; i < T; i++) {
          if (ctxs[i].rc != 0) bad = 1;
          agg.requests += ctxs[i].stats.requests;
          agg.responses += ctxs[i].stats.responses;
          agg.errors += ctxs[i].stats.errors;
          agg.timeouts += ctxs[i].stats.timeouts;
          agg.connect_errors += ctxs[i].stats.connect_errors;
          hist_merge(&agg.hist, &ctxs[i].stats.hist);
        }

        free(ths);
        free(ctxs);
      }

      double secs = (double)args.duration_ms / 1000.0;
      double rps = secs > 0 ? ((double)agg.responses / secs) : 0.0;

      double mean = hist_mean_us(&agg.hist);
      double stddev = hist_stddev_us(&agg.hist);
      double cv = (mean > 0.0) ? (stddev / mean) : 0.0;

      printf("responses: %llu  rps: %.2f  errors: %llu timeouts: %llu connect_errors: %llu\n",
        (unsigned long long)agg.responses, rps,
        (unsigned long long)agg.errors,
        (unsigned long long)agg.timeouts,
        (unsigned long long)agg.connect_errors);
      if (agg.hist.count) {
        printf("latency(us): p50=%.0f p95=%.0f p99=%.0f mean=%.1f stddev=%.1f cv=%.4f min=%llu max=%llu\n",
          hist_percentile_us(&agg.hist, 50.0),
          hist_percentile_us(&agg.hist, 95.0),
          hist_percentile_us(&agg.hist, 99.0),
          mean, stddev, cv,
          (unsigned long long)agg.hist.min_us,
          (unsigned long long)agg.hist.max_us);
      } else {
        printf("latency(us): no samples\n");
      }
      printf("\n");
      // store summary for later comparison
      if (summaries_n < (int)(sizeof(summaries)/sizeof(summaries[0]))) {
        strncpy(summaries[summaries_n].label, t->label, sizeof(summaries[summaries_n].label)-1);
        strncpy(summaries[summaries_n].proto, t->proto == PROTO_MELIAN ? "melian" : "redis",
                sizeof(summaries[summaries_n].proto)-1);
        summaries[summaries_n].total_conns = total_conns;
        summaries[summaries_n].rps = rps;
        summaries[summaries_n].p50 = hist_percentile_us(&agg.hist, 50.0);
        summaries[summaries_n].p95 = hist_percentile_us(&agg.hist, 95.0);
        summaries[summaries_n].p99 = hist_percentile_us(&agg.hist, 99.0);
        summaries[summaries_n].mean = mean;
        summaries[summaries_n].stddev = stddev;
        summaries[summaries_n].cv = cv;
        summaries_n++;
      }
    }
    free(plan.req);
  }

  // Comparison summary
  if (summaries_n > 1) {
    printf("=== Comparison summary ===\n");
    // For each concurrency group, show best and deltas
    for (int i = 0; i < summaries_n; i++) {
      for (int j = i+1; j < summaries_n; j++) {
        if (summaries[j].total_conns < summaries[i].total_conns) {
          summary_t tmp = summaries[i];
          summaries[i] = summaries[j];
          summaries[j] = tmp;
        }
      }
    }
    int idx = 0;
    while (idx < summaries_n) {
      int conn = summaries[idx].total_conns;
      double best_rps = summaries[idx].rps;
      for (int k = idx; k < summaries_n && summaries[k].total_conns == conn; k++) {
        if (summaries[k].rps > best_rps) best_rps = summaries[k].rps;
      }
      printf("concurrency=%d:\n", conn);
      for (int k = idx; k < summaries_n && summaries[k].total_conns == conn; k++) {
        double delta = summaries[k].rps - best_rps;
        double pct = best_rps > 0 ? (summaries[k].rps / best_rps * 100.0) : 0.0;
        printf("  %s (%s): rps=%.2f (%+.0f vs best, %.1f%% of best) p95=%.0f p99=%.0f cv=%.4f\n",
          summaries[k].label, summaries[k].proto, summaries[k].rps, delta, pct,
          summaries[k].p95, summaries[k].p99, summaries[k].cv);
      }
      while (idx < summaries_n && summaries[idx].total_conns == conn) idx++;
    }
    printf("\n");
  }

  return bad ? 3 : 0;
}
