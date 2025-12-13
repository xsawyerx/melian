#include <stdio.h>
#include <time.h>
#include "util.h"

double now_sec(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}

unsigned next_power_of_two(unsigned value, unsigned start) {
  unsigned power = start;
  while (value > power) {
    power <<= 1;
  }
  return power;
}

unsigned format_timestamp(unsigned epoch, char* buf, unsigned len) {
  struct tm local;
  time_t time = epoch;
#if defined(_WIN32)
  localtime_s(&local, &time);
#else
  localtime_r(&time, &local);
#endif
  unsigned l = snprintf(buf, len, "%04u/%02u/%02u %02u:%02u:%02u"
                        ,local.tm_year + 1900, local.tm_mon + 1, local.tm_mday
                        ,local.tm_hour, local.tm_min, local.tm_sec
                        );
  return l;
}
