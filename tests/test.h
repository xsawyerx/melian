#pragma once

// Minimal unit test harness -- no external dependencies.

#include <stdio.h>
#include <string.h>

static int _test_pass_count;
static int _test_fail_count;

#define TEST(name) static void test_##name(void)

#define RUN_TEST(name) do { \
    test_##name(); \
    (void)0; \
  } while (0)

#define _TEST_FAIL(file, line, msg) do { \
    printf("  FAIL %s:%d: %s\n", file, line, msg); \
    _test_fail_count++; \
    return; \
  } while (0)

#define ASSERT_TRUE(expr) do { \
    if (!(expr)) { \
      _TEST_FAIL(__FILE__, __LINE__, "expected true: " #expr); \
    } \
  } while (0)

#define ASSERT_FALSE(expr) do { \
    if ((expr)) { \
      _TEST_FAIL(__FILE__, __LINE__, "expected false: " #expr); \
    } \
  } while (0)

#define ASSERT_EQ(a, b) do { \
    unsigned long long _a = (unsigned long long)(a); \
    unsigned long long _b = (unsigned long long)(b); \
    if (_a != _b) { \
      char _buf[256]; \
      snprintf(_buf, sizeof(_buf), "expected %llu == %llu (%s == %s)", _a, _b, #a, #b); \
      _TEST_FAIL(__FILE__, __LINE__, _buf); \
    } \
  } while (0)

#define ASSERT_NEQ(a, b) do { \
    unsigned long long _a = (unsigned long long)(a); \
    unsigned long long _b = (unsigned long long)(b); \
    if (_a == _b) { \
      char _buf[256]; \
      snprintf(_buf, sizeof(_buf), "expected %llu != %llu (%s != %s)", _a, _b, #a, #b); \
      _TEST_FAIL(__FILE__, __LINE__, _buf); \
    } \
  } while (0)

#define ASSERT_STR_EQ(a, b) do { \
    const char* _a = (a); \
    const char* _b = (b); \
    if (!_a || !_b || strcmp(_a, _b) != 0) { \
      char _buf[512]; \
      snprintf(_buf, sizeof(_buf), "expected \"%s\" == \"%s\" (%s == %s)", \
               _a ? _a : "(null)", _b ? _b : "(null)", #a, #b); \
      _TEST_FAIL(__FILE__, __LINE__, _buf); \
    } \
  } while (0)

#define ASSERT_PTR_NULL(p) do { \
    if ((p) != NULL) { \
      char _buf[256]; \
      snprintf(_buf, sizeof(_buf), "expected NULL: %s", #p); \
      _TEST_FAIL(__FILE__, __LINE__, _buf); \
    } \
  } while (0)

#define ASSERT_PTR_NOT_NULL(p) do { \
    if ((p) == NULL) { \
      char _buf[256]; \
      snprintf(_buf, sizeof(_buf), "expected non-NULL: %s", #p); \
      _TEST_FAIL(__FILE__, __LINE__, _buf); \
    } \
  } while (0)

#define ASSERT_MEM_EQ(a, b, len) do { \
    const void* _ma = (a); \
    const void* _mb = (b); \
    if (!_ma || !_mb || memcmp(_ma, _mb, (len)) != 0) { \
      char _buf[256]; \
      snprintf(_buf, sizeof(_buf), "memory mismatch (%u bytes): %s vs %s", \
               (unsigned)(len), #a, #b); \
      _TEST_FAIL(__FILE__, __LINE__, _buf); \
    } \
  } while (0)

#define TEST_MAIN_BEGIN \
  int main(void) { \
    _test_pass_count = 0; \
    _test_fail_count = 0;

#define TEST_RUN(name) do { \
    int _before = _test_fail_count; \
    printf("  %s...", #name); \
    test_##name(); \
    if (_test_fail_count == _before) { \
      _test_pass_count++; \
      printf(" ok\n"); \
    } \
  } while (0)

#define TEST_MAIN_END \
    printf("%d passed, %d failed\n", _test_pass_count, _test_fail_count); \
    return _test_fail_count ? 1 : 0; \
  }
