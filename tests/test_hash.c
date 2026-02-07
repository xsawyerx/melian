#include "test.h"
#include "arena.h"
#include "hash.h"
#include "log.h"
#include <stdlib.h>
#include <string.h>

TEST(build_and_destroy) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(16, a);
  ASSERT_PTR_NOT_NULL(h);
  ASSERT_EQ(h->cap, 16);
  ASSERT_EQ(h->used, 0);
  hash_destroy(h);
  arena_destroy(a);
}

TEST(build_requires_arena) {
  log_reset(1, 1);
  Hash* h = hash_build(16, NULL);
  ASSERT_PTR_NULL(h);
}

TEST(insert_and_finalize_and_get) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(64, a);

  uint8_t val[] = {0xDE, 0xAD};
  unsigned frame = arena_store_framed(a, val, sizeof(val));
  uint32_t frame_len = 4 + sizeof(val);
  const char* key = "mykey";
  uint32_t key_len = 5;

  unsigned ok = hash_insert(h, key, key_len, frame, frame_len);
  ASSERT_EQ(ok, 1);
  hash_finalize_pointers(h);

  const Bucket* b = hash_get(h, key, key_len);
  ASSERT_PTR_NOT_NULL(b);
  ASSERT_EQ(b->key_len, key_len);
  ASSERT_MEM_EQ(b->key_ptr, key, key_len);

  hash_destroy(h);
  arena_destroy(a);
}

TEST(get_missing_key_returns_null) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(64, a);

  uint8_t val[] = {1};
  unsigned frame = arena_store_framed(a, val, sizeof(val));
  hash_insert(h, "exists", 6, frame, 4 + sizeof(val));
  hash_finalize_pointers(h);

  const Bucket* b = hash_get(h, "missing", 7);
  ASSERT_PTR_NULL(b);

  hash_destroy(h);
  arena_destroy(a);
}

TEST(insert_multiple_keys) {
  Arena* a = arena_build(8192);
  Hash* h = hash_build(64, a);

  const char* keys[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
  unsigned key_count = 5;

  for (unsigned i = 0; i < key_count; ++i) {
    uint8_t val = (uint8_t)i;
    unsigned frame = arena_store_framed(a, &val, 1);
    hash_insert(h, keys[i], (uint32_t)strlen(keys[i]), frame, 5);
  }
  hash_finalize_pointers(h);

  for (unsigned i = 0; i < key_count; ++i) {
    const Bucket* b = hash_get(h, keys[i], (uint32_t)strlen(keys[i]));
    ASSERT_PTR_NOT_NULL(b);
    ASSERT_EQ(b->key_len, (uint32_t)strlen(keys[i]));
  }

  hash_destroy(h);
  arena_destroy(a);
}

TEST(integer_key_4byte) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(64, a);

  uint32_t key = 42;
  uint8_t val[] = {0xFF};
  unsigned frame = arena_store_framed(a, val, sizeof(val));
  hash_insert(h, &key, sizeof(key), frame, 4 + sizeof(val));
  hash_finalize_pointers(h);

  const Bucket* b = hash_get(h, &key, sizeof(key));
  ASSERT_PTR_NOT_NULL(b);
  ASSERT_EQ(b->key_len, 4);

  // Different key should not match
  uint32_t other = 99;
  const Bucket* miss = hash_get(h, &other, sizeof(other));
  ASSERT_PTR_NULL(miss);

  hash_destroy(h);
  arena_destroy(a);
}

TEST(string_key_variable_length) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(64, a);

  const char* k1 = "short";
  const char* k2 = "a_much_longer_key_value";
  uint8_t val[] = {1};
  unsigned f1 = arena_store_framed(a, val, sizeof(val));
  unsigned f2 = arena_store_framed(a, val, sizeof(val));
  hash_insert(h, k1, (uint32_t)strlen(k1), f1, 5);
  hash_insert(h, k2, (uint32_t)strlen(k2), f2, 5);
  hash_finalize_pointers(h);

  const Bucket* b1 = hash_get(h, k1, (uint32_t)strlen(k1));
  const Bucket* b2 = hash_get(h, k2, (uint32_t)strlen(k2));
  ASSERT_PTR_NOT_NULL(b1);
  ASSERT_PTR_NOT_NULL(b2);
  ASSERT_EQ(b1->key_len, strlen(k1));
  ASSERT_EQ(b2->key_len, strlen(k2));

  hash_destroy(h);
  arena_destroy(a);
}

TEST(bucket_fields_correct) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(64, a);

  const char* key = "testkey";
  uint32_t key_len = 7;
  uint8_t val[] = {0xAA, 0xBB, 0xCC};
  unsigned frame = arena_store_framed(a, val, sizeof(val));
  uint32_t frame_len = 4 + sizeof(val);
  hash_insert(h, key, key_len, frame, frame_len);
  hash_finalize_pointers(h);

  const Bucket* b = hash_get(h, key, key_len);
  ASSERT_PTR_NOT_NULL(b);
  ASSERT_EQ(b->key_len, key_len);
  ASSERT_EQ(b->frame_len, frame_len);
  ASSERT_MEM_EQ(b->key_ptr, key, key_len);
  // frame_ptr points to the framed data (4-byte header + value)
  ASSERT_MEM_EQ(b->frame_ptr + 4, val, sizeof(val));

  hash_destroy(h);
  arena_destroy(a);
}

TEST(stats_track_queries) {
  Arena* a = arena_build(4096);
  Hash* h = hash_build(64, a);

  uint8_t val[] = {1};
  unsigned frame = arena_store_framed(a, val, sizeof(val));
  hash_insert(h, "k", 1, frame, 5);
  hash_finalize_pointers(h);

  ASSERT_EQ(h->stats.queries, 0);
  hash_get(h, "k", 1);
  ASSERT_EQ(h->stats.queries, 1);
  hash_get(h, "k", 1);
  hash_get(h, "x", 1);
  ASSERT_EQ(h->stats.queries, 3);

  hash_destroy(h);
  arena_destroy(a);
}

TEST(collision_linear_probe) {
  // With a small table, collisions are inevitable
  Arena* a = arena_build(8192);
  Hash* h = hash_build(4, a);  // very small table

  uint8_t val[] = {1};
  const char* keys[] = {"aaa", "bbb", "ccc"};
  for (unsigned i = 0; i < 3; ++i) {
    unsigned frame = arena_store_framed(a, val, sizeof(val));
    hash_insert(h, keys[i], 3, frame, 5);
  }
  hash_finalize_pointers(h);

  for (unsigned i = 0; i < 3; ++i) {
    const Bucket* b = hash_get(h, keys[i], 3);
    ASSERT_PTR_NOT_NULL(b);
    ASSERT_MEM_EQ(b->key_ptr, keys[i], 3);
  }

  hash_destroy(h);
  arena_destroy(a);
}

TEST(high_load_factor) {
  Arena* a = arena_build(65536);
  unsigned cap = 64;
  Hash* h = hash_build(cap, a);

  // Insert ~50% capacity
  unsigned count = cap / 2;
  for (unsigned i = 0; i < count; ++i) {
    uint32_t key = i + 1;
    uint8_t val = (uint8_t)i;
    unsigned frame = arena_store_framed(a, &val, 1);
    hash_insert(h, &key, sizeof(key), frame, 5);
  }
  hash_finalize_pointers(h);
  ASSERT_EQ(h->used, count);

  for (unsigned i = 0; i < count; ++i) {
    uint32_t key = i + 1;
    const Bucket* b = hash_get(h, &key, sizeof(key));
    ASSERT_PTR_NOT_NULL(b);
  }

  hash_destroy(h);
  arena_destroy(a);
}

TEST(destroy_null_safe) {
  hash_destroy(NULL);
  // If we got here, it didn't crash
  ASSERT_TRUE(1);
}

TEST_MAIN_BEGIN
  log_reset(1, 1);
  TEST_RUN(build_and_destroy);
  TEST_RUN(build_requires_arena);
  TEST_RUN(insert_and_finalize_and_get);
  TEST_RUN(get_missing_key_returns_null);
  TEST_RUN(insert_multiple_keys);
  TEST_RUN(integer_key_4byte);
  TEST_RUN(string_key_variable_length);
  TEST_RUN(bucket_fields_correct);
  TEST_RUN(stats_track_queries);
  TEST_RUN(collision_linear_probe);
  TEST_RUN(high_load_factor);
  TEST_RUN(destroy_null_safe);
TEST_MAIN_END
