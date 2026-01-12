// Benchmark JSON row serialization: manual escaping vs libjansson.
// This is a synthetic test (no SQLite required); lengths are supplied explicitly.
// gcc -std=c11 -Wall -Wextra -Wpedantic -g -g -O2  -L/opt/homebrew/lib utils/benchmarks/json_row_bench.c -O2 -ljansson -o json_row_bench
// ./json_row_bench 200000 12 64 5
#include <errno.h>
#include <jansson.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
  TYPE_NULL = 0,
  TYPE_INT = 1,
  TYPE_FLOAT = 2,
  TYPE_STRING = 3,
};

static uint32_t rng_state = 0x12345678u;

static uint32_t rng_u32(void) {
  // xorshift32
  uint32_t x = rng_state;
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  rng_state = x;
  return x;
}

static double now_sec(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static int json_append_escaped(char *buf, unsigned buf_len, unsigned *pos,
                               const char *src, unsigned src_len) {
  if (!buf || !pos) return 0;
  if (*pos + 2 > buf_len) return 0;
  buf[(*pos)++] = '"';
  for (unsigned i = 0; i < src_len; ++i) {
    unsigned char c = (unsigned char)src[i];
    const char *esc = NULL;
    char tmp[7];
    unsigned need = 1;
    switch (c) {
      case '\"': esc = "\\\""; need = 2; break;
      case '\\': esc = "\\\\"; need = 2; break;
      case '\b': esc = "\\b";  need = 2; break;
      case '\f': esc = "\\f";  need = 2; break;
      case '\n': esc = "\\n";  need = 2; break;
      case '\r': esc = "\\r";  need = 2; break;
      case '\t': esc = "\\t";  need = 2; break;
      default:
        if (c < 0x20) {
          snprintf(tmp, sizeof(tmp), "\\u%04x", c);
          esc = tmp;
          need = 6;
        }
        break;
    }
    if (*pos + need > buf_len) return 0;
    if (esc) {
      memcpy(buf + *pos, esc, need);
      *pos += need;
    } else {
      buf[(*pos)++] = (char)c;
    }
  }
  if (*pos + 1 > buf_len) return 0;
  buf[(*pos)++] = '"';
  return 1;
}

static unsigned build_manual(char *out, unsigned out_len,
                             char **names, char **vals, unsigned *lens,
                             int *types, unsigned cols) {
  unsigned pos = 0;
  if (pos + 1 > out_len) return 0;
  out[pos++] = '{';
  unsigned emitted = 0;
  for (unsigned i = 0; i < cols; ++i) {
    if (types[i] == TYPE_NULL) {
      // still include NULL field
    }
    if (emitted > 0) {
      if (pos + 1 > out_len) return 0;
      out[pos++] = ',';
    }
    if (!json_append_escaped(out, out_len, &pos, names[i], (unsigned)strlen(names[i]))) return 0;
    if (pos + 1 > out_len) return 0;
    out[pos++] = ':';
    switch (types[i]) {
      case TYPE_NULL: {
        static const char nullv[] = "null";
        if (pos + sizeof(nullv) - 1 > out_len) return 0;
        memcpy(out + pos, nullv, sizeof(nullv) - 1);
        pos += sizeof(nullv) - 1;
        break;
      }
      case TYPE_INT: {
        char num[32];
        int wrote = snprintf(num, sizeof(num), "%u", (unsigned)(rng_u32() % 1000000));
        if (wrote < 0 || (unsigned)wrote >= sizeof(num)) return 0;
        if (pos + (unsigned)wrote > out_len) return 0;
        memcpy(out + pos, num, (unsigned)wrote);
        pos += (unsigned)wrote;
        break;
      }
      case TYPE_FLOAT: {
        char num[64];
        double v = (double)(rng_u32() % 1000000) / 100.0;
        int wrote = snprintf(num, sizeof(num), "%.2f", v);
        if (wrote < 0 || (unsigned)wrote >= sizeof(num)) return 0;
        if (pos + (unsigned)wrote > out_len) return 0;
        memcpy(out + pos, num, (unsigned)wrote);
        pos += (unsigned)wrote;
        break;
      }
      case TYPE_STRING:
      default:
        if (!json_append_escaped(out, out_len, &pos, vals[i], lens[i])) return 0;
        break;
    }
    emitted++;
  }
  if (pos + 1 > out_len) return 0;
  out[pos++] = '}';
  return pos;
}

static unsigned build_jansson(char **out,
                              char **names, char **vals, unsigned *lens,
                              int *types, unsigned cols) {
  json_t *obj = json_object();
  if (!obj) return 0;
  for (unsigned i = 0; i < cols; ++i) {
    json_t *jv = NULL;
    switch (types[i]) {
      case TYPE_NULL:
        jv = json_null();
        break;
      case TYPE_INT:
        jv = json_integer((json_int_t)(rng_u32() % 1000000));
        break;
      case TYPE_FLOAT: {
        double v = (double)(rng_u32() % 1000000) / 100.0;
        jv = json_real(v);
        break;
      }
      case TYPE_STRING:
      default:
        jv = json_stringn(vals[i], lens[i]);
        break;
    }
    if (!jv) {
      json_decref(obj);
      return 0;
    }
    if (json_object_set_new(obj, names[i], jv) != 0) {
      json_decref(jv);
      json_decref(obj);
      return 0;
    }
  }
  *out = json_dumps(obj, JSON_COMPACT | JSON_ENSURE_ASCII);
  json_decref(obj);
  if (!*out) return 0;
  return (unsigned)strlen(*out);
}

static unsigned build_jansson_nocheck(char **out,
                                      char **names, char **vals, unsigned *lens,
                                      int *types, unsigned cols) {
  json_t *obj = json_object();
  if (!obj) return 0;
  for (unsigned i = 0; i < cols; ++i) {
    json_t *jv = NULL;
    switch (types[i]) {
      case TYPE_NULL:
        jv = json_null();
        break;
      case TYPE_INT:
        jv = json_integer((json_int_t)(rng_u32() % 1000000));
        break;
      case TYPE_FLOAT: {
        double v = (double)(rng_u32() % 1000000) / 100.0;
        jv = json_real(v);
        break;
      }
      case TYPE_STRING:
      default:
#if defined(JSON_INTEGER_IS_LONG_LONG)
        jv = json_stringn_nocheck(vals[i], lens[i]);
#else
        jv = json_stringn_nocheck(vals[i], lens[i]);
#endif
        break;
    }
    if (!jv) {
      json_decref(obj);
      return 0;
    }
    if (json_object_set_new(obj, names[i], jv) != 0) {
      json_decref(jv);
      json_decref(obj);
      return 0;
    }
  }
  *out = json_dumps(obj, JSON_COMPACT | JSON_ENSURE_ASCII);
  json_decref(obj);
  if (!*out) return 0;
  return (unsigned)strlen(*out);
}

static void fill_value(char *buf, unsigned len, int control_ratio) {
  for (unsigned i = 0; i < len; ++i) {
    uint32_t r = rng_u32() % 100;
    if (r < (unsigned)control_ratio) {
      // sprinkle control/special chars
      static const char specials[] = "\"\\\n\r\t\b\f";
      buf[i] = specials[rng_u32() % (unsigned)(sizeof(specials) - 1)];
    } else {
      buf[i] = (char)('a' + (rng_u32() % 26));
    }
  }
}

int main(int argc, char **argv) {
  unsigned rows = 100000;
  unsigned cols = 10;
  unsigned val_len = 32;
  int control_ratio = 5; // percent
  if (argc > 1) rows = (unsigned)strtoul(argv[1], NULL, 10);
  if (argc > 2) cols = (unsigned)strtoul(argv[2], NULL, 10);
  if (argc > 3) val_len = (unsigned)strtoul(argv[3], NULL, 10);
  if (argc > 4) control_ratio = atoi(argv[4]);

  char **names = calloc(cols, sizeof(char*));
  char **vals = calloc(cols, sizeof(char*));
  unsigned *lens = calloc(cols, sizeof(unsigned));
  int *types = calloc(cols, sizeof(int));
  if (!names || !vals || !lens || !types) {
    fprintf(stderr, "alloc failed\n");
    return 1;
  }

  for (unsigned i = 0; i < cols; ++i) {
    char namebuf[32];
    snprintf(namebuf, sizeof(namebuf), "col%u", i);
    names[i] = strdup(namebuf);
    vals[i] = malloc(val_len);
    if (!names[i] || !vals[i]) {
      fprintf(stderr, "alloc failed\n");
      return 1;
    }
    fill_value(vals[i], val_len, control_ratio);
    lens[i] = val_len;
    types[i] = (i % 4); // rotate types
  }

  const unsigned out_len = 1u << 20;
  char *out = malloc(out_len);
  if (!out) {
    fprintf(stderr, "alloc failed\n");
    return 1;
  }

  double t0 = now_sec();
  unsigned ok = 0;
  for (unsigned i = 0; i < rows; ++i) {
    unsigned wrote = build_manual(out, out_len, names, vals, lens, types, cols);
    if (wrote == 0) {
      fprintf(stderr, "manual build failed\n");
      return 1;
    }
    ok += wrote > 0;
  }
  double t1 = now_sec();

  double t2 = now_sec();
  for (unsigned i = 0; i < rows; ++i) {
    char *dump = NULL;
    unsigned wrote = build_jansson(&dump, names, vals, lens, types, cols);
    if (wrote == 0 || !dump) {
      fprintf(stderr, "jansson build failed\n");
      return 1;
    }
    free(dump);
  }
  double t3 = now_sec();

#if JANSSON_VERSION_HEX >= 0x020A00
  double t4 = now_sec();
  for (unsigned i = 0; i < rows; ++i) {
    char *dump = NULL;
    unsigned wrote = build_jansson_nocheck(&dump, names, vals, lens, types, cols);
    if (wrote == 0 || !dump) {
      fprintf(stderr, "jansson nocheck build failed\n");
      return 1;
    }
    free(dump);
  }
  double t5 = now_sec();
#endif

  double manual_sec = t1 - t0;
  double jansson_sec = t3 - t2;
  printf("rows=%u cols=%u val_len=%u control_ratio=%d%%\n",
         rows, cols, val_len, control_ratio);
  printf("manual:  %.6f s, %.2f rows/s\n", manual_sec, rows / manual_sec);
  printf("jansson: %.6f s, %.2f rows/s\n", jansson_sec, rows / jansson_sec);
#if JANSSON_VERSION_HEX >= 0x020A00
  double jansson_nc_sec = t5 - t4;
  printf("jansson_nocheck: %.6f s, %.2f rows/s\n", jansson_nc_sec, rows / jansson_nc_sec);
#endif
  (void)ok;

  for (unsigned i = 0; i < cols; ++i) {
    free(names[i]);
    free(vals[i]);
  }
  free(names);
  free(vals);
  free(lens);
  free(types);
  free(out);
  return 0;
}
