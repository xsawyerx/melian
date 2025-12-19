#include "event_loop.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "log.h"
#include "util.h"

#if defined(__linux__) && defined(MELIAN_HAVE_IO_URING)
struct io_uring;
#endif

enum MelBackend {
  MEL_BACKEND_NONE = 0,
  MEL_BACKEND_IO_URING,
  MEL_BACKEND_EPOLL,
  MEL_BACKEND_KQUEUE,
};

struct mel_fd_slot {
  mel_loop_cb cb;
  void *arg;
  uint32_t events;
  uint8_t active;
  uint8_t polling; // only used by io_uring to re-arm POLL
};

static int loop_register_wake(MelLoop* loop);
static void loop_on_wake(int fd, uint32_t events, void *arg);
static int loop_resize(MelLoop* loop, size_t need_fd);

#if defined(__linux__)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#  ifdef MELIAN_HAVE_IO_URING
#    include <liburing.h>
#  endif

#  ifdef MELIAN_HAVE_IO_URING
static short to_poll_mask(uint32_t events) {
  short mask = 0;
  if (events & MEL_LOOP_READ) mask |= POLLIN;
  if (events & MEL_LOOP_WRITE) mask |= POLLOUT;
  return mask;
}

static uint32_t from_poll_mask(uint32_t res) {
  uint32_t ev = 0;
  if (res & (POLLIN | POLLPRI)) ev |= MEL_LOOP_READ;
  if (res & POLLOUT) ev |= MEL_LOOP_WRITE;
  if (res & POLLHUP) ev |= MEL_LOOP_HUP;
  if (res & POLLERR) ev |= MEL_LOOP_ERR;
  return ev;
}
#  endif

static int add_epoll(MelLoop* loop, int fd, uint32_t events) {
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.data.fd = fd;
  ev.events = 0;
  if (events & MEL_LOOP_READ) ev.events |= EPOLLIN;
  if (events & MEL_LOOP_WRITE) ev.events |= EPOLLOUT;
  ev.events |= EPOLLET;
  return epoll_ctl(loop->epfd, EPOLL_CTL_ADD, fd, &ev);
}

static int mod_epoll(MelLoop* loop, int fd, uint32_t events) {
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.data.fd = fd;
  ev.events = 0;
  if (events & MEL_LOOP_READ) ev.events |= EPOLLIN;
  if (events & MEL_LOOP_WRITE) ev.events |= EPOLLOUT;
  ev.events |= EPOLLET;
  return epoll_ctl(loop->epfd, EPOLL_CTL_MOD, fd, &ev);
}

#  ifdef MELIAN_HAVE_IO_URING
static int submit_poll(struct MelLoop* loop, int fd, struct mel_fd_slot* slot) {
  if (!loop->uring_ready) return -1;
  struct io_uring_sqe* sqe = io_uring_get_sqe(&loop->ring);
  if (!sqe) {
    int sub = io_uring_submit(&loop->ring);
    if (sub < 0) return sub;
    sqe = io_uring_get_sqe(&loop->ring);
    if (!sqe) return -1;
  }
  short mask = to_poll_mask(slot->events);
  io_uring_prep_poll_add(sqe, fd, mask);
  io_uring_sqe_set_data(sqe, slot);
  slot->polling = 1;
  int sub = io_uring_submit(&loop->ring);
  if (sub < 0) {
    slot->polling = 0;
    return sub;
  }
  return 0;
}
#  endif
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
#include <sys/event.h>
#include <sys/time.h>

static int kevent_update(int kq, int fd, uint32_t new_events, uint32_t old_events, uintptr_t udata) {
  struct kevent ev[2];
  int n = 0;
  if (new_events & MEL_LOOP_READ) {
    EV_SET(&ev[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, (void*)udata);
  } else if (old_events & MEL_LOOP_READ) {
    EV_SET(&ev[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, (void*)udata);
  }
  if (new_events & MEL_LOOP_WRITE) {
    EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, (void*)udata);
  } else if (old_events & MEL_LOOP_WRITE) {
    EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, (void*)udata);
  }
  if (!n) return 0;
  return kevent(kq, ev, n, NULL, 0, NULL);
}
#endif

const char* mel_loop_backend_name(const MelLoop* loop) {
  if (!loop) return "none";
  switch (loop->backend) {
    case MEL_BACKEND_IO_URING: return "io_uring";
    case MEL_BACKEND_EPOLL: return "epoll";
    case MEL_BACKEND_KQUEUE: return "kqueue";
    default: return "none";
  }
}

static int set_nonblock(int fd) __attribute__((unused));
static int set_nonblock(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  if (flags & O_NONBLOCK) return 0;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int mel_loop_init(MelLoop* loop, const MelLoopConfig* cfg) {
  if (!loop) return -1;
  memset(loop, 0, sizeof(*loop));
  loop->wake_fd = -1;
#if defined(__linux__)
  loop->epfd = -1;
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  loop->kq = -1;
  loop->wake_pipe[0] = loop->wake_pipe[1] = -1;
#endif
  size_t hint = cfg && cfg->fd_hint ? cfg->fd_hint : 1024;
  loop->cap = hint;
  loop->slots = calloc(loop->cap, sizeof(struct mel_fd_slot));
  if (!loop->slots) return -1;

#if defined(__linux__)
  loop->backend = MEL_BACKEND_EPOLL;
  loop->epfd = epoll_create1(EPOLL_CLOEXEC);
  if (loop->epfd < 0) {
    LOG_ERROR("epoll_create1 failed: %s", strerror(errno));
    return -1;
  }
  loop->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (loop->wake_fd < 0) {
    LOG_ERROR("eventfd failed: %s", strerror(errno));
    return -1;
  }

#  ifdef MELIAN_HAVE_IO_URING
  int want_uring = !(cfg && cfg->force_epoll) && (!cfg || cfg->prefer_uring);
  if (want_uring) {
    unsigned flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
    int rc = io_uring_queue_init(1024, &loop->ring, flags);
    if (rc == -EINVAL || rc == -ENOSYS || rc == -EOPNOTSUPP || rc == -EPERM) {
      // Older kernels or locked-down environments may not support requested flags.
      LOG_INFO("io_uring not available with optimized flags (%s), retrying without flags",
               strerror(-rc));
      rc = io_uring_queue_init(1024, &loop->ring, 0);
    }
    if (rc == 0) {
      loop->backend = MEL_BACKEND_IO_URING;
      loop->uring_ready = 1;
      loop->epoll_ready = 0;
    } else {
      LOG_INFO("io_uring unavailable (%s), falling back to epoll", strerror(-rc));
      loop->uring_ready = 0;
    }
  }
#  endif

  if (loop->backend == MEL_BACKEND_EPOLL) {
    loop->epoll_ready = 1;
  }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  loop->backend = MEL_BACKEND_KQUEUE;
  loop->kq = kqueue();
  if (loop->kq < 0) return -1;
  loop->wake_pipe[0] = loop->wake_pipe[1] = -1;
  if (pipe(loop->wake_pipe) < 0) return -1;
  set_nonblock(loop->wake_pipe[0]);
  set_nonblock(loop->wake_pipe[1]);
  loop->wake_fd = loop->wake_pipe[0];
#else
#error "Unsupported platform for MelLoop"
#endif

  // Self-wakeup slot
  loop_register_wake(loop);
  return 0;
}

void mel_loop_close(MelLoop* loop) {
  if (!loop) return;
  if (loop->slots) free(loop->slots);
#if defined(__linux__)
  if (loop->backend == MEL_BACKEND_IO_URING) {
#  ifdef MELIAN_HAVE_IO_URING
    io_uring_queue_exit(&loop->ring);
#  endif
  }
  if (loop->epfd >= 0) close(loop->epfd);
  if (loop->wake_fd >= 0) close(loop->wake_fd);
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  if (loop->wake_pipe[0] >= 0) close(loop->wake_pipe[0]);
  if (loop->wake_pipe[1] >= 0) close(loop->wake_pipe[1]);
  if (loop->kq >= 0) close(loop->kq);
#endif
}

static int loop_resize(MelLoop* loop, size_t need_fd) {
  if (need_fd < loop->cap) return 0;
  size_t new_cap = loop->cap;
  while (need_fd >= new_cap) new_cap <<= 1;
  struct mel_fd_slot* n = realloc(loop->slots, new_cap * sizeof(*n));
  if (!n) return -1;
  memset(n + loop->cap, 0, (new_cap - loop->cap) * sizeof(*n));
  loop->slots = n;
  loop->cap = new_cap;
  return 0;
}

int mel_loop_add(MelLoop* loop, int fd, uint32_t events, mel_loop_cb cb, void *arg) {
  if (loop_resize(loop, (size_t)fd + 1) < 0) return -1;
  struct mel_fd_slot* slot = &loop->slots[fd];
  slot->cb = cb;
  slot->arg = arg;
  slot->events = events;
  slot->active = 1;

#if defined(__linux__)
  if (loop->backend == MEL_BACKEND_EPOLL) {
    return add_epoll(loop, fd, events);
  } else {
#  ifdef MELIAN_HAVE_IO_URING
    return submit_poll(loop, fd, slot);
#  else
    return -1;
#  endif
  }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  return kevent_update(loop->kq, fd, events, 0, (uintptr_t)fd);
#endif
}

int mel_loop_mod(MelLoop* loop, int fd, uint32_t events) {
  if (fd < 0 || (size_t)fd >= loop->cap) return -1;
  struct mel_fd_slot* slot = &loop->slots[fd];
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  uint32_t old_events = slot->events;
#endif
  slot->events = events;
  if (!slot->active) return -1;
#if defined(__linux__)
  if (loop->backend == MEL_BACKEND_EPOLL) {
    return mod_epoll(loop, fd, events);
  } else {
#  ifdef MELIAN_HAVE_IO_URING
    slot->polling = 0;
    return submit_poll(loop, fd, slot);
#  else
    return -1;
#  endif
  }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  return kevent_update(loop->kq, fd, events, old_events, (uintptr_t)fd);
#endif
}

int mel_loop_del(MelLoop* loop, int fd) {
  if (fd < 0 || (size_t)fd >= loop->cap) return -1;
  struct mel_fd_slot* slot = &loop->slots[fd];
  slot->active = 0;
  slot->cb = NULL;
  slot->arg = NULL;
  slot->events = 0;
#if defined(__linux__)
  if (loop->backend == MEL_BACKEND_EPOLL) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    return epoll_ctl(loop->epfd, EPOLL_CTL_DEL, fd, &ev);
  } else {
    // io_uring: we simply stop re-arming; outstanding polls will be ignored.
    return 0;
  }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  struct kevent ev[2];
  EV_SET(&ev[0], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
  EV_SET(&ev[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
  kevent(loop->kq, ev, 2, NULL, 0, NULL);
  return 0;
#endif
}

static void handle_slot_event(struct MelLoop* loop, int fd, uint32_t ev) {
  if (fd < 0 || (size_t)fd >= loop->cap) return;
  struct mel_fd_slot* slot = &loop->slots[fd];
  if (!slot->active || !slot->cb) return;
  slot->cb(fd, ev, slot->arg);
}

#if defined(__linux__)
static int run_epoll(MelLoop* loop) {
  struct epoll_event events[256];
  int n = epoll_wait(loop->epfd, events, ALEN(events), -1);
  if (n < 0) {
    if (errno == EINTR) return 0;
    return -1;
  }
  for (int i = 0; i < n; ++i) {
    uint32_t evmask = 0;
    if (events[i].events & (EPOLLIN | EPOLLPRI)) evmask |= MEL_LOOP_READ;
    if (events[i].events & EPOLLOUT) evmask |= MEL_LOOP_WRITE;
    if (events[i].events & EPOLLHUP) evmask |= MEL_LOOP_HUP;
    if (events[i].events & EPOLLERR) evmask |= MEL_LOOP_ERR;
    handle_slot_event(loop, events[i].data.fd, evmask);
  }
  return 0;
}

#  ifdef MELIAN_HAVE_IO_URING
static int run_uring(MelLoop* loop) {
  struct io_uring_cqe* cqe = NULL;
  int rc = io_uring_wait_cqe(&loop->ring, &cqe);
  if (rc == -EINTR) return 0;
  if (rc < 0) return rc;
  while (cqe) {
    struct mel_fd_slot* slot = io_uring_cqe_get_data(cqe);
    int fd = -1;
    if (slot) {
      fd = (int)(slot - loop->slots);
      slot->polling = 0;
    }
    uint32_t evmask = 0;
    if (cqe->res < 0) {
      evmask = MEL_LOOP_ERR;
    } else {
      evmask = from_poll_mask((uint32_t)cqe->res);
    }
    io_uring_cqe_seen(&loop->ring, cqe);
    if (slot && evmask) handle_slot_event(loop, fd, evmask);
    // Re-arm if still interested
    if (slot && slot->active && slot->events && !slot->polling) {
      submit_poll(loop, fd, slot);
    }
    rc = io_uring_peek_cqe(&loop->ring, &cqe);
    if (rc == -EAGAIN) break;
    if (rc < 0) break;
  }
  return 0;
}
#  endif
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
static int run_kqueue(MelLoop* loop) {
  struct kevent kev[256];
  int n = kevent(loop->kq, NULL, 0, kev, ALEN(kev), NULL);
  if (n < 0) {
    if (errno == EINTR) return 0;
    return -1;
  }
  for (int i = 0; i < n; ++i) {
    uint32_t evmask = 0;
    if (kev[i].filter == EVFILT_READ) evmask |= MEL_LOOP_READ;
    if (kev[i].filter == EVFILT_WRITE) evmask |= MEL_LOOP_WRITE;
    if (kev[i].flags & EV_EOF) evmask |= MEL_LOOP_HUP;
    if (kev[i].flags & EV_ERROR) evmask |= MEL_LOOP_ERR;
    handle_slot_event(loop, (int)kev[i].ident, evmask);
  }
  return 0;
}
#endif

int mel_loop_run(MelLoop* loop) {
  if (!loop) return -1;
  loop->running = 1;
  while (loop->running) {
#if defined(__linux__)
    if (loop->backend == MEL_BACKEND_IO_URING) {
#  ifdef MELIAN_HAVE_IO_URING
      if (run_uring(loop) < 0) return -1;
#  else
      return -1;
#  endif
    } else {
      if (run_epoll(loop) < 0) return -1;
    }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
    if (run_kqueue(loop) < 0) return -1;
#endif
  }
  return 0;
}

void mel_loop_stop(MelLoop* loop) {
  if (!loop) return;
  loop->running = 0;
  uint64_t one = 1;
#if defined(__linux__)
  if (loop->wake_fd >= 0) write(loop->wake_fd, &one, sizeof(one));
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
  if (loop->wake_pipe[1] >= 0) write(loop->wake_pipe[1], &one, sizeof(one));
#endif
}

static void loop_on_wake(int fd, uint32_t events, void *arg) {
  UNUSED(events);
  UNUSED(arg);
  uint64_t tmp;
  while (read(fd, &tmp, sizeof(tmp)) > 0) {}
}

static int loop_register_wake(MelLoop* loop) {
  if (loop->wake_fd < 0) return -1;
  return mel_loop_add(loop, loop->wake_fd, MEL_LOOP_READ, loop_on_wake, loop);
}
