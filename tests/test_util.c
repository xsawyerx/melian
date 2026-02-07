#include "test.h"
#include "util.h"
#include "log.h"
#include <string.h>

TEST(next_power_of_two_basic) {
  ASSERT_EQ(next_power_of_two(5, 1), 8);
  ASSERT_EQ(next_power_of_two(1, 1), 1);
  ASSERT_EQ(next_power_of_two(9, 1), 16);
}

TEST(next_power_of_two_exact) {
  ASSERT_EQ(next_power_of_two(8, 1), 8);
  ASSERT_EQ(next_power_of_two(16, 1), 16);
  ASSERT_EQ(next_power_of_two(256, 1), 256);
}

TEST(next_power_of_two_start) {
  ASSERT_EQ(next_power_of_two(5, 16), 16);
  ASSERT_EQ(next_power_of_two(1, 64), 64);
}

TEST(next_power_of_two_large) {
  ASSERT_EQ(next_power_of_two(1025, 1), 2048);
  ASSERT_EQ(next_power_of_two(65000, 1), 65536);
}

TEST(format_timestamp_output) {
  // 2024-01-15 00:00:00 UTC = epoch 1705276800
  // We use localtime_r so the exact output depends on TZ.
  // Just verify the format: YYYY/MM/DD HH:MM:SS (19 chars)
  char buf[MAX_STAMP_LEN];
  unsigned len = format_timestamp(1705276800, buf, sizeof(buf));
  // Should be 19 chars: "YYYY/MM/DD HH:MM:SS"
  ASSERT_EQ(len, 19);
  // Check format structure: slashes at 4,7 and colons at 13,16, space at 10
  ASSERT_EQ(buf[4], '/');
  ASSERT_EQ(buf[7], '/');
  ASSERT_EQ(buf[10], ' ');
  ASSERT_EQ(buf[13], ':');
  ASSERT_EQ(buf[16], ':');
}

TEST(format_timestamp_length) {
  char buf[MAX_STAMP_LEN];
  unsigned len = format_timestamp(0, buf, sizeof(buf));
  ASSERT_EQ(len, 19);
}

TEST(now_sec_positive) {
  double t = now_sec();
  ASSERT_TRUE(t > 0.0);
}

TEST(alen_macro) {
  int arr[] = {10, 20, 30, 40, 50};
  ASSERT_EQ(ALEN(arr), 5);

  char arr2[] = {'a', 'b', 'c'};
  ASSERT_EQ(ALEN(arr2), 3);
}

TEST_MAIN_BEGIN
  log_reset(1, 1);
  TEST_RUN(next_power_of_two_basic);
  TEST_RUN(next_power_of_two_exact);
  TEST_RUN(next_power_of_two_start);
  TEST_RUN(next_power_of_two_large);
  TEST_RUN(format_timestamp_output);
  TEST_RUN(format_timestamp_length);
  TEST_RUN(now_sec_positive);
  TEST_RUN(alen_macro);
TEST_MAIN_END
