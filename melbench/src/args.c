#include "args.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int parse_duration_ms(const char *s) {
  // supports plain ms or suffix s (seconds)
  size_t n = strlen(s);
  if (n == 0) return -1;
  if (s[n-1] == 's') {
    int v = atoi(s);
    return v * 1000;
  }
  return atoi(s);
}

void args_print_usage(const char *prog) {
  fprintf(stderr,
    "Usage: %s [--target=label:melian|redis:dsn]... [options]\n"
    "\n"
    "Targets (repeatable):\n"
    "  --target=name:melian:DSN   benchmark Melian instance with label\n"
    "  --target=name:redis:DSN    benchmark Redis with label\n"
    "  (if omitted, uses --proto/--dsn for a single target)\n"
    "\n"
    "Core options:\n"
    "  --threads=N           (default 1)\n"
    "  --conns=N             connections per thread (default 16)\n"
    "  --concurrency=CSV     total connections sweep (overrides --conns)\n"
    "  --runs=N              repeat each scenario N times (default 1)\n"
    "  --duration=30s|30000  (default 10s)\n"
    "  --warmup=5s|5000      (default 2s)\n"
    "  --timeout=1000        per-request timeout ms (default 1000)\n"
    "\n"
    "Key options:\n"
    "  --key-type=string|int (default string)\n"
    "  --key=VALUE           string key\n"
    "  --key-int=N           int key (Melian payload is 4B little-endian)\n"
    "\n"
    "Melian options:\n"
    "  --table-id=N          (required for melian)\n"
    "  --column-id=N         (required for melian)\n"
    "  --action=F            (default F)\n"
    "\n"
    "Examples:\n"
    "  %s --target=mel_new:melian:unix:///tmp/melian.sock --table-id=1 --column-id=1 --key=Pixel --threads=2 --concurrency=32,128 --duration=30s\n"
    "  %s --target=mel_old:melian:unix:///tmp/melian_old.sock --target=redis:redis:tcp://127.0.0.1:6379 --key=Pixel --threads=2 --conns=128 --duration=30s\n",
    prog, prog, prog
  );
}

int args_parse(int argc, char **argv, bench_args_t *out) {
  memset(out, 0, sizeof(*out));
  out->target_count = 0;
  out->threads = 1;
  out->conns_per_thread = 16;
  out->total_concurrency = 0;
  out->duration_ms = 10 * 1000;
  out->warmup_ms = 2 * 1000;
  out->io_timeout_ms = 1000;
  out->runs = 1;

  out->melian_action = (uint8_t)'F';
  out->table_id = 0;
  out->column_id = 0;

  out->key_type = KEY_STRING;
  out->key_str[0] = '\0';
  out->key_int = 0;

  for (int i = 1; i < argc; i++) {
    const char *a = argv[i];

    if (!strcmp(a, "--help") || !strcmp(a, "-h")) {
      args_print_usage(argv[0]);
      return 1;
    }

    if (!strncmp(a, "--proto=", 8)) {
      const char *v = a + 8;
      if (out->target_count == 0) out->target_count = 1;
      if (!strcmp(v, "melian")) out->targets[0].proto = PROTO_MELIAN;
      else if (!strcmp(v, "redis")) out->targets[0].proto = PROTO_REDIS;
      else { fprintf(stderr, "Unknown proto: %s\n", v); return -1; }
      continue;
    }
    if (!strncmp(a, "--dsn=", 6)) {
      if (out->target_count == 0) out->target_count = 1;
      strncpy(out->targets[0].dsn, a + 6, sizeof(out->targets[0].dsn)-1);
      continue;
    }
    if (!strncmp(a, "--target=", 9)) {
      if (out->target_count >= (int)(sizeof(out->targets)/sizeof(out->targets[0]))) {
        fprintf(stderr, "Too many targets\n");
        return -1;
      }
      const char *v = a + 9;
      char buf[512];
      strncpy(buf, v, sizeof(buf)-1);
      buf[sizeof(buf)-1] = '\0';
      char *p1 = strchr(buf, ':');
      char *p2 = p1 ? strchr(p1 + 1, ':') : NULL;
      if (!p1 || !p2) { fprintf(stderr, "Invalid target format: %s\n", v); return -1; }
      *p1 = '\0'; *p2 = '\0';
      bench_target_t *t = &out->targets[out->target_count++];
      size_t label_len = strlen(buf);
      if (label_len >= sizeof(t->label)) {
        fprintf(stderr, "Target label too long\n");
        return -1;
      }
      memcpy(t->label, buf, label_len + 1);
      const char *proto = p1 + 1;
      const char *dsn = p2 + 1;
      if (!strcmp(proto, "melian")) t->proto = PROTO_MELIAN;
      else if (!strcmp(proto, "redis")) t->proto = PROTO_REDIS;
      else { fprintf(stderr, "Unknown target proto: %s\n", proto); return -1; }
      size_t dsn_len = strlen(dsn);
      if (dsn_len >= sizeof(t->dsn)) {
        fprintf(stderr, "Target DSN too long\n");
        return -1;
      }
      memcpy(t->dsn, dsn, dsn_len + 1);
      continue;
    }
    if (!strncmp(a, "--threads=", 10)) {
      out->threads = atoi(a + 10);
      continue;
    }
    if (!strncmp(a, "--conns=", 8)) {
      out->conns_per_thread = atoi(a + 8);
      continue;
    }
    if (!strncmp(a, "--concurrency=", 14)) {
      const char *v = a + 14;
      char tmp[256];
      strncpy(tmp, v, sizeof(tmp)-1);
      tmp[sizeof(tmp)-1] = '\0';
      char *save = NULL;
      char *tok = strtok_r(tmp, ",", &save);
      while (tok && out->sweep_count < (int)(sizeof(out->sweep_concurrency)/sizeof(out->sweep_concurrency[0]))) {
        out->sweep_concurrency[out->sweep_count++] = atoi(tok);
        tok = strtok_r(NULL, ",", &save);
      }
      continue;
    }
    if (!strncmp(a, "--runs=", 7)) {
      out->runs = atoi(a + 7);
      continue;
    }
    if (!strncmp(a, "--duration=", 11)) {
      out->duration_ms = parse_duration_ms(a + 11);
      continue;
    }
    if (!strncmp(a, "--warmup=", 9)) {
      out->warmup_ms = parse_duration_ms(a + 9);
      continue;
    }
    if (!strncmp(a, "--timeout=", 10)) {
      out->io_timeout_ms = atoi(a + 10);
      continue;
    }

    if (!strncmp(a, "--key-type=", 11)) {
      const char *v = a + 11;
      if (!strcmp(v, "string")) out->key_type = KEY_STRING;
      else if (!strcmp(v, "int")) out->key_type = KEY_INT32_LE;
      else { fprintf(stderr, "Unknown key-type: %s\n", v); return -1; }
      continue;
    }
    if (!strncmp(a, "--key=", 6)) {
      strncpy(out->key_str, a + 6, sizeof(out->key_str)-1);
      continue;
    }
    if (!strncmp(a, "--key-int=", 10)) {
      out->key_int = atoll(a + 10);
      continue;
    }

    if (!strncmp(a, "--table-id=", 11)) {
      out->table_id = (uint8_t)atoi(a + 11);
      continue;
    }
    if (!strncmp(a, "--column-id=", 12)) {
      out->column_id = (uint8_t)atoi(a + 12);
      continue;
    }
    if (!strncmp(a, "--action=", 9)) {
      out->melian_action = (uint8_t)a[9];
      continue;
    }

    fprintf(stderr, "Unknown argument: %s\n", a);
    return -1;
  }

  if (out->threads <= 0 || out->conns_per_thread <= 0) {
    fprintf(stderr, "threads and conns must be > 0\n");
    return -1;
  }
  if (out->duration_ms <= 0) {
    fprintf(stderr, "duration must be > 0\n");
    return -1;
  }
  if (out->target_count == 0) {
    // legacy default if no --target/--proto/--dsn provided
    strncpy(out->targets[0].label, "default", sizeof(out->targets[0].label)-1);
    out->targets[0].proto = PROTO_MELIAN;
    strncpy(out->targets[0].dsn, "unix:///tmp/melian.sock", sizeof(out->targets[0].dsn)-1);
    out->target_count = 1;
  }
  return 0;
}
