#pragma once

#include <stdint.h>
#include <stddef.h>

#if defined(__linux__) && defined(MELIAN_HAVE_IO_URING)
#include <liburing.h>
#endif

// Minimal, server-specific event loop focused on read/write readiness and
// predictable wakeups. Backends:
// - Linux: io_uring (if available) with epoll fallback
// - macOS/BSD: kqueue

enum {
  MEL_LOOP_READ  = 0x01,
  MEL_LOOP_WRITE = 0x02,
  MEL_LOOP_ERR   = 0x04,
  MEL_LOOP_HUP   = 0x08,
};

typedef struct MelLoop MelLoop;
typedef void (*mel_loop_cb)(int fd, uint32_t events, void *arg);

typedef struct MelLoopConfig {
  size_t fd_hint;       // initial fd table size (auto-grows)
  int force_epoll;      // force epoll on Linux (testing/fallback)
  int prefer_uring;     // try io_uring on Linux (default on)
} MelLoopConfig;

struct mel_fd_slot;

struct MelLoop {
  int backend;
  int running;
  int wake_fd;
  size_t cap;
  struct mel_fd_slot *slots;

#if defined(__linux__)
  int epfd;
  int epoll_ready;
#  ifdef MELIAN_HAVE_IO_URING
  struct io_uring ring;
  int uring_ready;
#  endif
#endif
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  int kq;
  int wake_pipe[2];
#endif
};

const char* mel_loop_backend_name(const MelLoop* loop);

int mel_loop_init(MelLoop* loop, const MelLoopConfig* cfg);
void mel_loop_close(MelLoop* loop);

int mel_loop_add(MelLoop* loop, int fd, uint32_t events, mel_loop_cb cb, void *arg);
int mel_loop_mod(MelLoop* loop, int fd, uint32_t events);
int mel_loop_del(MelLoop* loop, int fd);

int mel_loop_run(MelLoop* loop);
void mel_loop_stop(MelLoop* loop);
