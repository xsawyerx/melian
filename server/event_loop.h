#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct EventLoop EventLoop;
typedef struct EventConn EventConn;

enum {
  EVENT_LOOP_EVENT_EOF = 1u << 0,
  EVENT_LOOP_EVENT_ERROR = 1u << 1,
};

typedef void (*event_loop_accept_cb)(int fd, void* ctx);
typedef void (*event_loop_read_cb)(EventConn* conn, void* ctx);
typedef void (*event_loop_event_cb)(EventConn* conn, void* ctx, unsigned events);
typedef void (*event_loop_timer_cb)(void* ctx);
typedef void (*event_loop_signal_cb)(int signum, void* ctx);

EventLoop* event_loop_build(void);
void event_loop_destroy(EventLoop* loop);

int event_loop_listen_unix(EventLoop* loop, const char* path,
                           event_loop_accept_cb cb, void* ctx);
int event_loop_listen_tcp(EventLoop* loop, const char* host, unsigned port,
                          event_loop_accept_cb cb, void* ctx);

int event_loop_run(EventLoop* loop);
void event_loop_stop(EventLoop* loop);

EventConn* event_loop_conn_build(EventLoop* loop, int fd);
void event_loop_conn_free(EventConn* conn);
void event_loop_conn_set_cb(EventConn* conn, event_loop_read_cb rcb,
                            event_loop_event_cb ecb, void* ctx);
void event_loop_conn_enable(EventConn* conn);

size_t event_loop_conn_in_len(EventConn* conn);
const uint8_t* event_loop_conn_in_peek(EventConn* conn, size_t len);
void event_loop_conn_in_drain(EventConn* conn, size_t len);

int event_loop_conn_out_add(EventConn* conn, const void* data, size_t len);
int event_loop_conn_out_add_ref(EventConn* conn, const void* data, size_t len);

int event_loop_add_signal(EventLoop* loop, int signum,
                          event_loop_signal_cb cb, void* ctx);
int event_loop_add_timer(EventLoop* loop, unsigned ms,
                         event_loop_timer_cb cb, void* ctx);

const char* event_loop_backend_name(EventLoop* loop);
const char* event_loop_backend_version(EventLoop* loop);
