#include "proto.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

// RESP framing for a single reply:
// - Bulk string: $<len>\r\n<data>\r\n  (len can be -1 for nil)
// - Simple string: +OK\r\n
// - Error: -ERR ...\r\n
// - Integer: :1\r\n
// We implement enough to know "full reply length".
static int find_crlf(const uint8_t *buf, size_t len, size_t start) {
  for (size_t i = start; i + 1 < len; i++) {
    if (buf[i] == '\r' && buf[i+1] == '\n') return (int)i;
  }
  return -1;
}

static int parse_int(const uint8_t *buf, size_t start, size_t end, int *out) {
  // parse ASCII int in buf[start..end-1]
  int sign = 1;
  size_t i = start;
  if (i < end && buf[i] == '-') { sign = -1; i++; }
  int v = 0;
  if (i >= end) return -1;
  for (; i < end; i++) {
    if (buf[i] < '0' || buf[i] > '9') return -1;
    v = v * 10 + (buf[i] - '0');
  }
  *out = v * sign;
  return 0;
}

static int redis_frame_len(const uint8_t *buf, size_t buf_len) {
  if (buf_len < 1) return 0;
  uint8_t t = buf[0];

  // Simple forms: +, -, : end at first CRLF
  if (t == '+' || t == '-' || t == ':') {
    int e = find_crlf(buf, buf_len, 1);
    if (e < 0) return 0;
    return e + 2;
  }

  // Bulk string: $<len>\r\n<data>\r\n or $-1\r\n
  if (t == '$') {
    int e = find_crlf(buf, buf_len, 1);
    if (e < 0) return 0;
    int n = 0;
    if (parse_int(buf, 1, (size_t)e, &n) != 0) return -1;
    size_t header_len = (size_t)e + 2;
    if (n == -1) {
      return (int)header_len; // nil bulk string
    }
    size_t total = header_len + (size_t)n + 2; // + data + CRLF
    if (total > (64ull * 1024ull * 1024ull)) return -1;
    if (buf_len < total) return 0;
    return (int)total;
  }

  // Not handling arrays here (not needed for GET baseline)
  return -1;
}

static int redis_validate(const uint8_t *buf, size_t frame_len) {
  (void)frame_len;
  if (!buf) return -1;
  // Treat -ERR as an error for benchmark accounting
  if (buf[0] == '-') return -1;
  return 0;
}

int proto_redis_build_plan(
  proto_plan_t *out,
  key_type_t key_type,
  const char *key_str,
  int64_t key_int
) {
  if (!out) return -EINVAL;

  char keybuf[64];
  const char *k = key_str ? key_str : "";
  if (key_type == KEY_INT32_LE) {
    // Redis keys are strings: use decimal form
    snprintf(keybuf, sizeof(keybuf), "%lld", (long long)key_int);
    k = keybuf;
  }

  size_t klen = strlen(k);

  // Build: *2\r\n$3\r\nGET\r\n$<klen>\r\n<key>\r\n
  char head[128];
  int head_len = snprintf(head, sizeof(head),
    "*2\r\n$3\r\nGET\r\n$%zu\r\n", klen);
  if (head_len <= 0) return -EINVAL;

  size_t req_len = (size_t)head_len + klen + 2; // + key + \r\n
  uint8_t *req = (uint8_t*)malloc(req_len);
  if (!req) return -ENOMEM;

  memcpy(req, head, (size_t)head_len);
  memcpy(req + head_len, k, klen);
  req[head_len + klen + 0] = '\r';
  req[head_len + klen + 1] = '\n';

  out->req = req;
  out->req_len = req_len;
  out->frame_len = redis_frame_len;
  out->validate = redis_validate;
  return 0;
}

