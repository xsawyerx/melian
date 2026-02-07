#include "test.h"
#include "arena.h"
#include "log.h"
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

TEST(build_and_destroy) {
  Arena* a = arena_build(1024);
  ASSERT_PTR_NOT_NULL(a);
  ASSERT_EQ(a->capacity, 1024);
  ASSERT_EQ(a->used, 0);
  ASSERT_PTR_NOT_NULL(a->buffer);
  arena_destroy(a);
}

TEST(store_returns_zero_index_first) {
  Arena* a = arena_build(1024);
  ASSERT_PTR_NOT_NULL(a);
  uint8_t data[] = {0xAA, 0xBB};
  unsigned idx = arena_store(a, data, sizeof(data));
  ASSERT_EQ(idx, 0);
  arena_destroy(a);
}

TEST(store_data_retrievable) {
  Arena* a = arena_build(1024);
  uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF};
  unsigned idx = arena_store(a, data, sizeof(data));
  uint8_t* ptr = arena_get_ptr(a, idx);
  ASSERT_PTR_NOT_NULL(ptr);
  ASSERT_MEM_EQ(ptr, data, sizeof(data));
  arena_destroy(a);
}

TEST(store_multiple_sequential) {
  Arena* a = arena_build(1024);
  uint8_t d1[] = {1, 2, 3};
  uint8_t d2[] = {4, 5};
  uint8_t d3[] = {6, 7, 8, 9};

  unsigned i1 = arena_store(a, d1, sizeof(d1));
  unsigned i2 = arena_store(a, d2, sizeof(d2));
  unsigned i3 = arena_store(a, d3, sizeof(d3));

  ASSERT_EQ(i1, 0);
  ASSERT_EQ(i2, 3);
  ASSERT_EQ(i3, 5);

  ASSERT_MEM_EQ(arena_get_ptr(a, i1), d1, sizeof(d1));
  ASSERT_MEM_EQ(arena_get_ptr(a, i2), d2, sizeof(d2));
  ASSERT_MEM_EQ(arena_get_ptr(a, i3), d3, sizeof(d3));
  arena_destroy(a);
}

TEST(store_framed_has_be_header) {
  Arena* a = arena_build(1024);
  uint8_t data[] = {0xCA, 0xFE};
  unsigned idx = arena_store_framed(a, data, sizeof(data));
  uint8_t* ptr = arena_get_ptr(a, idx);
  ASSERT_PTR_NOT_NULL(ptr);

  // First 4 bytes are big-endian length
  uint32_t len_be;
  memcpy(&len_be, ptr, 4);
  ASSERT_EQ(ntohl(len_be), sizeof(data));
  arena_destroy(a);
}

TEST(store_framed_data_follows_header) {
  Arena* a = arena_build(1024);
  uint8_t data[] = {0xCA, 0xFE, 0xBA, 0xBE};
  unsigned idx = arena_store_framed(a, data, sizeof(data));
  uint8_t* ptr = arena_get_ptr(a, idx);
  ASSERT_PTR_NOT_NULL(ptr);

  // Data follows the 4-byte header
  ASSERT_MEM_EQ(ptr + 4, data, sizeof(data));
  arena_destroy(a);
}

TEST(grow_on_overflow) {
  Arena* a = arena_build(16);
  // Store more than 16 bytes total to trigger growth
  uint8_t buf[32];
  memset(buf, 0xAB, sizeof(buf));
  unsigned idx = arena_store(a, buf, sizeof(buf));
  ASSERT_TRUE(a->capacity >= 32);
  uint8_t* ptr = arena_get_ptr(a, idx);
  ASSERT_PTR_NOT_NULL(ptr);
  ASSERT_MEM_EQ(ptr, buf, sizeof(buf));
  arena_destroy(a);
}

TEST(reset_sets_used_to_zero) {
  Arena* a = arena_build(1024);
  uint8_t data[] = {1, 2, 3};
  arena_store(a, data, sizeof(data));
  ASSERT_TRUE(a->used > 0);
  arena_reset(a);
  ASSERT_EQ(a->used, 0);

  // Next store should return index 0 again
  unsigned idx = arena_store(a, data, sizeof(data));
  ASSERT_EQ(idx, 0);
  arena_destroy(a);
}

TEST(get_ptr_with_invalid_index) {
  Arena* a = arena_build(1024);
  uint8_t* ptr = arena_get_ptr(a, (unsigned)-1);
  ASSERT_PTR_NULL(ptr);
  arena_destroy(a);
}

TEST_MAIN_BEGIN
  log_reset(1, 1);
  TEST_RUN(build_and_destroy);
  TEST_RUN(store_returns_zero_index_first);
  TEST_RUN(store_data_retrievable);
  TEST_RUN(store_multiple_sequential);
  TEST_RUN(store_framed_has_be_header);
  TEST_RUN(store_framed_data_follows_header);
  TEST_RUN(grow_on_overflow);
  TEST_RUN(reset_sets_used_to_zero);
  TEST_RUN(get_ptr_with_invalid_index);
TEST_MAIN_END
