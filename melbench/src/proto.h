#pragma once
#include <stddef.h>
#include <stdint.h>

typedef enum {
  KEY_STRING = 0,
  KEY_INT32_LE = 1, // Melian int key: 4 bytes little-endian
} key_type_t;

typedef struct {
  // Request bytes to send (prebuilt)
  uint8_t *req;
  size_t req_len;

  // How to determine when a full response has been read.
  // The harness reads into a buffer; the protocol decides framing.
  // Return:
  //  - 0 if need more bytes
  //  - >0: full response length in bytes (total frame bytes consumed from buf)
  //  - <0: protocol error
  int (*frame_len)(const uint8_t *buf, size_t buf_len);

  // Optional: validate or quickly scan response (may be NULL).
  // Return 0 ok, <0 error.
  int (*validate)(const uint8_t *buf, size_t frame_len);
} proto_plan_t;

// Build a plan for Melian / Redis.
// Caller owns returned req buffer (free()).
int proto_melian_build_plan(
  proto_plan_t *out,
  uint8_t action,              // 'F' etc.
  uint8_t table_id,
  uint8_t column_id,
  key_type_t key_type,
  const char *key_str,
  int64_t key_int
);

int proto_redis_build_plan(
  proto_plan_t *out,
  key_type_t key_type,
  const char *key_str,
  int64_t key_int
);

