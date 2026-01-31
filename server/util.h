#pragma once

// Some generic utility macros and functions.

// TODO: make these limits dynamic? Arena?
enum {
  MAX_STAMP_LEN = 128,
};

// Compiler hints for hot paths
#ifdef __GNUC__
#define HOT_FUNC __attribute__((hot))
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define HOT_FUNC
#define likely(x)   (x)
#define unlikely(x) (x)
#endif

#define UNUSED(var) (void)var
#define ALEN(array) (unsigned) (sizeof(array) / sizeof(array[0]))

double now_sec(void);
unsigned next_power_of_two(unsigned value, unsigned start);
unsigned format_timestamp(unsigned epoch, char* buf, unsigned len);
