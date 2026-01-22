#include <stdlib.h>
#include <string.h>
#include "log.h"
#include "arena.h"
#include "xxhash.h"
#include "hash.h"

#define USE_XXH3_32 1
#define USE_XXH3_64 0

#if USE_XXH3_32
#define HASH_FUNC(d, l) XXH32(d, l, 0)
#endif

#if USE_XXH3_64
#define HASH_FUNC(d, l) XXH3_64bits(d, l, 0)
#endif

#if !USE_XXH3_32 && !USE_XXH3_64
static inline uint64_t fast_hash(const void *data, unsigned len);
#define HASH_FUNC(d, l) fast_hash(d, l)
#endif

// Initialize hash table with arena for storage
Hash* hash_build(unsigned cap_pow2, struct Arena* arena) {
  Hash* hash = 0;
  unsigned bad = 0;
  do {
    if (!arena) {
      LOG_WARN("Cannot create a Hash object without a valid Arena");
      break;
    }

    hash = calloc(1, sizeof(Hash));
    if (!hash) {
      LOG_WARN("Could not allocate a Hash object");
      break;
    }

    hash->tab = calloc(cap_pow2, sizeof(Bucket));
    if (!hash->tab) {
      LOG_WARN("Could not allocate a Hash table object");
      ++bad;
      break;
    }
    hash->cap = cap_pow2;
    hash->arena = arena;
  } while (0);
  if (bad) {
    hash_destroy(hash);
    hash  = 0;
  }
  return hash;
}

void hash_destroy(Hash* hash) {
  if (!hash) return;
  if (hash->tab) free(hash->tab);
  free(hash);
}

// Insert preframed value
unsigned hash_insert(Hash *hash, const void *key, uint32_t key_len, unsigned frame, uint32_t frame_len) {
  uint64_t h = HASH_FUNC(key, key_len);
  uint8_t tag = (uint8_t)(h >> 56);
  uint64_t mask = hash->cap - 1;
  uint64_t idx = h & mask;
  while (1) {
    if (hash->tab[idx].key_len == 0) {
      // Store key in arena
      unsigned kindex = arena_store(hash->arena, key, key_len);
      if (kindex == (unsigned)-1) return 0;

      // Assume frame value was already stored in arena
      hash->tab[idx].hash = h;
      hash->tab[idx].tag = tag;
      hash->tab[idx].key_len = key_len;
      hash->tab[idx].key_idx = kindex;
      hash->tab[idx].frame_len = frame_len;
      hash->tab[idx].frame_idx = frame;
      hash->used++;
      return 1;
    }
    idx = (idx + 1) & mask;
  }
}

// Lookup by key
const Bucket* hash_get(Hash *hash, const void *key, uint32_t key_len) {
  ++hash->stats.queries;
  uint64_t h = HASH_FUNC(key, key_len);
  uint8_t tag = (uint8_t)(h >> 56);
  LOG_DEBUG("Looking up %u bytes, [%.*s], hash %llu", key_len, key_len, key, h);
  uint64_t mask = hash->cap - 1;
  uint64_t idx = h & mask;
  unsigned probes = 0;
  const Bucket *bucket = 0;
  while (1) {
    ++probes;
    LOG_DEBUG(">> PROBE");
    bucket = &hash->tab[idx];
    if (bucket->key_len == 0) {
      bucket = 0;
      break;
    }
    if (bucket->tag == tag && bucket->hash == h && bucket->key_len == key_len) {
      uint8_t* key_ptr = arena_get_ptr(hash->arena, bucket->key_idx);
      if (memcmp(key_ptr, key, key_len) == 0) break;
    }
    idx = (idx + 1) & mask;
  }
  if (probes < MAX_PROBE_COUNT) {
    ++hash->stats.probes[probes];
  } else {
    LOG_WARN("Discarding probe count %u -- higher than maximum: %u", probes, MAX_PROBE_COUNT);
  }
  return bucket;
}

#if !USE_XXH3_32 && !USE_XXH3_64
// Simple fast hash (xxhash64-like) for variable length binary keys
static inline uint64_t fast_hash(const void *data, unsigned len) {
  const uint64_t prime1 = 11400714785074694791ULL;
  uint64_t h = 0x9E3779B97F4A7C15ULL ^ (len * prime1);
  const uint8_t *p = data;
  while (len >= 8) {
    uint64_t v;
    memcpy(&v, p, 8);
    h ^= v;
    h *= prime1;
    p += 8; len -= 8;
  }
  while (len--) {
    h ^= *p++;
    h *= prime1;
  }
  return h ^ (h >> 33);
}
#endif
