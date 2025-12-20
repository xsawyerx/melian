#include "net.h"
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <stdlib.h>

int net_set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
  return 0;
}

int net_set_nodelay(int fd, int enabled) {
  int v = enabled ? 1 : 0;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &v, (socklen_t)sizeof(v));
}

int net_check_connect(const dsn_t *dsn, int timeout_ms) {
  int fd = -1;
  if (dsn->kind == DSN_UNIX) {
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    strncpy(sa.sun_path, dsn->path, sizeof(sa.sun_path)-1);
    int rc = connect(fd, (struct sockaddr*)&sa, (socklen_t)sizeof(sa));
    close(fd);
    return rc == 0 ? 0 : -1;
  }

  struct addrinfo hints, *res = NULL;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char portbuf[16];
  snprintf(portbuf, sizeof(portbuf), "%d", dsn->port);
  int gai = getaddrinfo(dsn->host, portbuf, &hints, &res);
  if (gai != 0) return -1;
  int ok = -1;
  for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) continue;
    // simple blocking connect with timeout via alarm-like setsockopt
    if (timeout_ms > 0) {
      struct timeval tv;
      tv.tv_sec = timeout_ms / 1000;
      tv.tv_usec = (timeout_ms % 1000) * 1000;
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }
    int rc = connect(fd, ai->ai_addr, (socklen_t)ai->ai_addrlen);
    close(fd);
    if (rc == 0) { ok = 0; break; }
  }
  freeaddrinfo(res);
  return ok;
}

int dsn_parse(const char *dsn_str, dsn_t *out) {
  memset(out, 0, sizeof(*out));

  if (!strncmp(dsn_str, "unix://", 7)) {
    out->kind = DSN_UNIX;
    strncpy(out->path, dsn_str + 7, sizeof(out->path)-1);
    if (out->path[0] == '\0') return -EINVAL;
    return 0;
  }

  if (!strncmp(dsn_str, "tcp://", 6)) {
    out->kind = DSN_TCP;
    const char *p = dsn_str + 6;
    const char *colon = strrchr(p, ':');
    if (!colon) return -EINVAL;

    size_t hostlen = (size_t)(colon - p);
    if (hostlen == 0 || hostlen >= sizeof(out->host)) return -EINVAL;
    memcpy(out->host, p, hostlen);
    out->host[hostlen] = '\0';
    out->port = atoi(colon + 1);
    if (out->port <= 0) return -EINVAL;
    return 0;
  }

  return -EINVAL;
}

int net_connect_nonblocking(const dsn_t *dsn) {
  if (dsn->kind == DSN_UNIX) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (net_set_nonblocking(fd) != 0) { close(fd); return -1; }

    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    strncpy(sa.sun_path, dsn->path, sizeof(sa.sun_path)-1);

    int rc = connect(fd, (struct sockaddr*)&sa, (socklen_t)sizeof(sa));
    if (rc == 0) return fd;
    if (rc < 0 && (errno == EINPROGRESS || errno == EALREADY)) return fd;

    close(fd);
    return -1;
  }

  // TCP
  struct addrinfo hints, *res = NULL;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  char portbuf[16];
  snprintf(portbuf, sizeof(portbuf), "%d", dsn->port);
  int gai = getaddrinfo(dsn->host, portbuf, &hints, &res);
  if (gai != 0) return -1;

  int fd = -1;
  for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) continue;
    if (net_set_nonblocking(fd) != 0) { close(fd); fd = -1; continue; }

    int rc = connect(fd, ai->ai_addr, (socklen_t)ai->ai_addrlen);
    if (rc == 0 || (rc < 0 && (errno == EINPROGRESS || errno == EALREADY))) {
      // nodelay for TCP
      if (ai->ai_family == AF_INET || ai->ai_family == AF_INET6) {
        net_set_nodelay(fd, 1);
      }
      break;
    }
    close(fd);
    fd = -1;
  }

  freeaddrinfo(res);
  return fd;
}
