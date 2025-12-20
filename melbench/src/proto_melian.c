#include "proto.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static void u32_to_be(uint32_t v, uint8_t out[4]) {
  out[0] = (uint8_t)((v >> 24) & 0xff);
  out[1] = (uint8_t)((v >> 16) & 0xff);
  out[2] = (uint8_t)((v >> 8) & 0xff);
  out[3] = (uint8_t)(v & 0xff);
}

static void u32_to_le(uint32_t v, uint8_t out[4]) {
  out[0] = (uint8_t)(v & 0xff);
  out[1] = (uint8_t)((v >> 8) & 0xff);
  out[2] = (uint8_t)((v >> 16) & 0xff);
  out[3] = (uint8_t)((v >> 24) & 0xff);
}

// Melian response: 4-byte big-endian length, then payload bytes.
// Frame = 4 + len
static int melian_frame_len(const uint8_t *buf, size_t buf_len) {
  if (buf_len < 4) return 0;
  uint32_t len = ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) | ((uint32_t)buf[2] << 8) | (uint32_t)buf[3];
  uint64_t total = 4ull + (uint64_t)len;
  if (total > (uint64_t) (64ull * 1024ull * 1024ull)) return -1; // sanity cap: 64MB
  if (buf_len < (size_t)total) return 0;
  return (int)total;
}

static int melian_validate(const uint8_t *buf, size_t frame_len) {
  (void)buf;
  (void)frame_len;
  return 0; // payload may be empty; don't parse JSON here
}

int proto_melian_build_plan(
  proto_plan_t *out,
  uint8_t action,
  uint8_t table_id,
  uint8_t column_id,
  key_type_t key_type,
  const char *key_str,
  int64_t key_int
) {
  if (!out) return -EINVAL;

  // Melian request header: version(0x11), action, table_id, column_id, payload_len (uint32 BE)
  const uint8_t version = 0x11;

  uint8_t payload_tmp[4];
  const uint8_t *payload = NULL;
  size_t payload_len = 0;

  if (key_type == KEY_STRING) {
    if (!key_str) key_str = "";
    payload = (const uint8_t*)key_str;
    payload_len = strlen(key_str);
  } else if (key_type == KEY_INT32_LE) {
    u32_to_le((uint32_t)key_int, payload_tmp);
    payload = payload_tmp;
    payload_len = 4;
  } else {
    return -EINVAL;
  }

  size_t req_len = 1 + 1 + 1 + 1 + 4 + payload_len;
  uint8_t *req = (uint8_t*)malloc(req_len);
  if (!req) return -ENOMEM;

  req[0] = version;
  req[1] = action;
  req[2] = table_id;
  req[3] = column_id;
  u32_to_be((uint32_t)payload_len, &req[4]);
  if (payload_len) memcpy(&req[8], payload, payload_len);

  out->req = req;
  out->req_len = req_len;
  out->frame_len = melian_frame_len;
  out->validate = melian_validate;
  return 0;
}

