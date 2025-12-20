#include "kq.h"
#include "net.h"
#include "timeutil.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/socket.h>

typedef enum {
  C_CONNECTING = 0,
  C_WRITING = 1,
  C_READING = 2,
} conn_state_t;

typedef struct {
  int fd;
  conn_state_t st;

  // write state
  size_t woff;

  // read buffer
  uint8_t *rbuf;
  size_t rcap;
  size_t rlen;

  // timing
  uint64_t t0_ns;          // request start
  uint64_t deadline_ns;    // timeout deadline
} conn_t;

static int fd_connect_done(int fd) {
  int err = 0;
  socklen_t len = (socklen_t)sizeof(err);
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) return -1;
  return err == 0 ? 0 : -1;
}

static int ensure_rcap(conn_t *c, size_t need) {
  if (need <= c->rcap) return 0;
  size_t ncap = c->rcap ? c->rcap : 4096;
  while (ncap < need) ncap *= 2;
  uint8_t *nb = (uint8_t*)realloc(c->rbuf, ncap);
  if (!nb) return -1;
  c->rbuf = nb;
  c->rcap = ncap;
  return 0;
}

static int kq_add(int kq, int fd, int16_t filter, uint16_t flags, void *udata) {
  struct kevent ev;
  EV_SET(&ev, fd, filter, flags, 0, 0, udata);
  return kevent(kq, &ev, 1, NULL, 0, NULL);
}

static int kq_set_rw(int kq, int fd, int want_read, int want_write, void *udata) {
  // Simpler: add both filters; kqueue ignores duplicates if identical; we explicitly delete as needed.
  if (want_read) {
    if (kq_add(kq, fd, EVFILT_READ, EV_ADD | EV_ENABLE, udata) != 0) return -1;
  } else {
    (void)kq_add(kq, fd, EVFILT_READ, EV_DELETE, udata);
  }
  if (want_write) {
    if (kq_add(kq, fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, udata) != 0) return -1;
  } else {
    (void)kq_add(kq, fd, EVFILT_WRITE, EV_DELETE, udata);
  }
  return 0;
}

int kq_run_benchmark_thread(
  int thread_index,
  const bench_args_t *args,
  const proto_plan_t *plan,
  thread_stats_t *out_stats
) {
  (void)thread_index;
  memset(out_stats, 0, sizeof(*out_stats));
  hist_init(&out_stats->hist);

  dsn_t dsn;
  if (dsn_parse(args->dsn, &dsn) != 0) return -1;

  int kq = kqueue();
  if (kq < 0) return -1;

  int n = args->conns_per_thread;
  conn_t *cs = (conn_t*)calloc((size_t)n, sizeof(conn_t));
  if (!cs) { close(kq); return -1; }

  uint64_t start_ns = now_ns_monotonic();
  uint64_t warmup_end_ns = start_ns + (uint64_t)args->warmup_ms * 1000000ull;
  uint64_t end_ns = warmup_end_ns + (uint64_t)args->duration_ms * 1000000ull;
  uint64_t timeout_ns = (uint64_t)args->io_timeout_ms * 1000000ull;

  // Create and register connections
  for (int i = 0; i < n; i++) {
    int fd = net_connect_nonblocking(&dsn);
    if (fd < 0) { out_stats->connect_errors++; continue; }
    cs[i].fd = fd;
    cs[i].st = C_CONNECTING;
    cs[i].woff = 0;
    cs[i].rbuf = NULL;
    cs[i].rcap = 0;
    cs[i].rlen = 0;
    cs[i].t0_ns = 0;
    cs[i].deadline_ns = now_ns_monotonic() + timeout_ns;

    // We need write readiness to complete connect()
    if (kq_set_rw(kq, fd, 0, 1, &cs[i]) != 0) {
      close(fd);
      cs[i].fd = -1;
      out_stats->connect_errors++;
    }
  }

  const int MAXEV = 1024;
  struct kevent evs[MAXEV];

  while (1) {
    uint64_t now = now_ns_monotonic();
    if (now >= end_ns) break;

    // Small timeout to allow periodic timeout checks
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 10 * 1000000L; // 10ms
    int nev = kevent(kq, NULL, 0, evs, MAXEV, &ts);
    if (nev < 0) {
      if (errno == EINTR) continue;
      break;
    }

    now = now_ns_monotonic();

    // Timeout scan (O(n)); acceptable for first version; optimize later if needed.
    for (int i = 0; i < n; i++) {
      conn_t *c = &cs[i];
      if (c->fd <= 0) continue;
      if (c->deadline_ns && now > c->deadline_ns) {
        out_stats->timeouts++;
        close(c->fd);
        c->fd = -1;
      }
    }

    for (int i = 0; i < nev; i++) {
      struct kevent *ev = &evs[i];
      conn_t *c = (conn_t*)ev->udata;
      if (!c || c->fd <= 0) continue;

      if (ev->flags & EV_EOF) {
        out_stats->errors++;
        close(c->fd);
        c->fd = -1;
        continue;
      }

      if (c->st == C_CONNECTING) {
        if (ev->filter != EVFILT_WRITE) continue;
        if (fd_connect_done(c->fd) != 0) {
          out_stats->connect_errors++;
          close(c->fd);
          c->fd = -1;
          continue;
        }
        // Connected: start first request
        c->st = C_WRITING;
        c->woff = 0;
        c->rlen = 0;
        c->t0_ns = now_ns_monotonic();
        c->deadline_ns = c->t0_ns + timeout_ns;
        out_stats->requests++;

        // Want write readiness
        (void)kq_set_rw(kq, c->fd, 0, 1, c);
        continue;
      }

      if (c->st == C_WRITING && ev->filter == EVFILT_WRITE) {
        while (c->woff < plan->req_len) {
          ssize_t w = write(c->fd, plan->req + c->woff, plan->req_len - c->woff);
          if (w > 0) { c->woff += (size_t)w; continue; }
          if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
          out_stats->errors++;
          close(c->fd);
          c->fd = -1;
          break;
        }
        if (c->fd <= 0) continue;
        if (c->woff == plan->req_len) {
          c->st = C_READING;
          c->rlen = 0;
          // Want read readiness
          (void)kq_set_rw(kq, c->fd, 1, 0, c);
        }
        continue;
      }

      if (c->st == C_READING && ev->filter == EVFILT_READ) {
        // Read some bytes
        if (ensure_rcap(c, c->rlen + 4096) != 0) {
          out_stats->errors++;
          close(c->fd);
          c->fd = -1;
          continue;
        }
        ssize_t r = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
        if (r > 0) {
          c->rlen += (size_t)r;
        } else if (r == 0) {
          out_stats->errors++;
          close(c->fd);
          c->fd = -1;
          continue;
        } else {
          if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
          out_stats->errors++;
          close(c->fd);
          c->fd = -1;
          continue;
        }

        int fl = plan->frame_len(c->rbuf, c->rlen);
        if (fl < 0) {
          out_stats->errors++;
          close(c->fd);
          c->fd = -1;
          continue;
        }
        if (fl == 0) continue;

        // We have a full frame in [0..fl)
        if (plan->validate && plan->validate(c->rbuf, (size_t)fl) != 0) {
          out_stats->errors++;
          close(c->fd);
          c->fd = -1;
          continue;
        }

        uint64_t t1 = now_ns_monotonic();
        uint64_t us = (t1 - c->t0_ns) / 1000ull;

        // Ignore warmup samples
        if (t1 >= warmup_end_ns) {
          hist_record_us(&out_stats->hist, us);
          out_stats->responses++;
        }

        // Start next request immediately (fixed in-flight)
        c->st = C_WRITING;
        c->woff = 0;
        c->rlen = 0;
        c->t0_ns = now_ns_monotonic();
        c->deadline_ns = c->t0_ns + timeout_ns;
        out_stats->requests++;

        (void)kq_set_rw(kq, c->fd, 0, 1, c);
        continue;
      }
    }
  }

  // Cleanup
  for (int i = 0; i < n; i++) {
    if (cs[i].fd > 0) close(cs[i].fd);
    free(cs[i].rbuf);
  }
  free(cs);
  close(kq);
  return 0;
}

