#pragma once

#include <jansson.h>

// Some generic utility macros and functions.

// TODO: make these limits dynamic? Arena?
enum {
  MAX_STAMP_LEN = 128,
};

#define UNUSED(var) (void)var
#define ALEN(array) (unsigned) (sizeof(array) / sizeof(array[0]))

double now_sec(void);
unsigned next_power_of_two(unsigned value, unsigned start);
unsigned format_timestamp(unsigned epoch, char* buf, unsigned len);
const char* jansson_type_to_string(json_t *value);
