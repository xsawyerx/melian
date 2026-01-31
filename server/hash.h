#pragma once

// A Hash keeps an index for data stored in an Arena.
// Keys are variable length byte arrays.
// Values are variable length byte arrays.

#include <stdint.h>

enum {
  MAX_PROBE_COUNT = 1024,
};

struct HashStats {
  unsigned queries;       // total number of queries
  unsigned probes[MAX_PROBE_COUNT];  // histogram with number of probes
};

// Preframed value: [4-byte BE length][binary value]
typedef struct Bucket {
  uint64_t hash;          // hash of the key for quick reject
  uint8_t  tag;           // top 8 bits of hash as a tiny fingerprint
  uint32_t key_len;       // length of key in bytes
  uint8_t* key_ptr;       // pointer to key bytes (or index cast during load)
  uint8_t* frame_ptr;     // pointer to preframed value (or index cast during load)
  uint32_t frame_len;     // = 4 + value_len
} Bucket;

typedef struct Hash {
  unsigned cap;           // power-of-two capacity
  unsigned used;          // number of items stored
  Bucket *tab;            // array of buckets
  struct Arena* arena;    // pointer to common arena
  struct HashStats stats;
} Hash;

Hash* hash_build(unsigned cap_pow2, struct Arena* arena);
void hash_destroy(Hash* hash);
unsigned hash_insert(Hash *hash, const void *key, uint32_t key_len, unsigned frame, uint32_t frame_len);
const Bucket* hash_get(Hash *hash, const void *key, uint32_t key_len);

// Convert stored indices to actual arena pointers after load is complete.
// Must be called BEFORE making the hash visible to readers.
void hash_finalize_pointers(Hash *hash);
