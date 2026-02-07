#include "test.h"
#include "log.h"

TEST(reset_clears_counts) {
  log_reset(1, 1);
  LOG_DEBUG("test debug");
  LOG_INFO("test info");
  LOG_WARN("test warn");

  // Now reset -- all counters should be zero
  log_reset(1, 1);
  const LogInfo* info = log_get_info();
  for (int i = 0; i < LOG_LEVEL_LAST; ++i) {
    ASSERT_EQ(info->count[i], 0);
  }
}

TEST(skip_abort_on_fatal) {
  log_reset(1, 1);
  // With skip_abort_on_error=1, LOG_FATAL should not abort
  LOG_FATAL("this should not abort");
  // If we got here, it worked
  ASSERT_TRUE(1);
}

TEST(skip_print_output) {
  log_reset(1, 1);
  // With skip_print_output=1, no output is produced
  // We can't easily verify no output, but we verify it doesn't crash
  LOG_INFO("this should be silent");
  ASSERT_TRUE(1);
}

TEST(count_increments) {
  log_reset(1, 1);
  const LogInfo* info = log_get_info();
  ASSERT_EQ(info->count[LOG_LEVEL_INFO], 0);
  LOG_INFO("one");
  ASSERT_EQ(info->count[LOG_LEVEL_INFO], 1);
  LOG_INFO("two");
  ASSERT_EQ(info->count[LOG_LEVEL_INFO], 2);
}

TEST(count_per_level) {
  log_reset(1, 1);
  const LogInfo* info = log_get_info();

  LOG_DEBUG("d1");
  LOG_INFO("i1");
  LOG_INFO("i2");
  LOG_WARN("w1");
  LOG_ERROR("e1");
  LOG_FATAL("f1");

  // With LOG_LEVEL=0 (compile-time), all levels are active
  ASSERT_EQ(info->count[LOG_LEVEL_DEBUG], 1);
  ASSERT_EQ(info->count[LOG_LEVEL_INFO], 2);
  ASSERT_EQ(info->count[LOG_LEVEL_WARN], 1);
  ASSERT_EQ(info->count[LOG_LEVEL_ERROR], 1);
  ASSERT_EQ(info->count[LOG_LEVEL_FATAL], 1);
}

TEST(get_info_returns_valid) {
  log_reset(1, 1);
  const LogInfo* info = log_get_info();
  ASSERT_PTR_NOT_NULL(info);
  ASSERT_EQ(info->level_compile_time, LOG_LEVEL_COMPILE_TIME);
}

TEST_MAIN_BEGIN
  log_reset(1, 1);
  TEST_RUN(reset_clears_counts);
  TEST_RUN(skip_abort_on_fatal);
  TEST_RUN(skip_print_output);
  TEST_RUN(count_increments);
  TEST_RUN(count_per_level);
  TEST_RUN(get_info_returns_valid);
TEST_MAIN_END
