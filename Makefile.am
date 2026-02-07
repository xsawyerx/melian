AUTOMAKE_OPTIONS = foreign subdir-objects

bin_PROGRAMS = melian-server melian-client

AM_CPPFLAGS = \
	-I$(top_srcdir) \
	-I$(top_srcdir)/server \
	-I$(top_srcdir)/clients/c \
	-I$(top_srcdir)/common \
	$(MYSQL_CFLAGS) \
	$(SQLITE_CFLAGS) \
	$(POSTGRESQL_CFLAGS) \
	$(PLATFORM_CPPFLAGS)

AM_CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -g

melian_server_SOURCES = \
	server/util.c \
	server/log.c \
	server/arena.c \
	server/xxhash.c \
	server/hash.c \
	server/server.c \
	server/config.c \
	server/status.c \
	server/data.c \
	server/db.c \
	server/cron.c \
	server/melian-server.c

melian_server_LDADD = \
	$(LIBEVENT_LIBS) \
	$(MYSQL_LIBS) \
	$(SQLITE_LIBS) \
	$(POSTGRESQL_LIBS) \
	$(PTHREAD_LIBS) \
	$(JANSSON_LIBS) \
	$(LIBM)

melian_client_SOURCES = \
	clients/c/client.c \
	clients/c/melian-client.c

melian_client_LDADD = \
	$(JANSSON_LIBS) \
	$(LIBM)

# Header files to include in distribution
EXTRA_DIST = \
	protocol.h \
	server/util.h \
	server/log.h \
	server/arena.h \
	server/xxhash.h \
	server/hash.h \
	server/config.h \
	server/server.h \
	server/status.h \
	server/data.h \
	server/db.h \
	server/cron.h \
	clients/c/client.h \
	tests/test.h

# --- Test suite ---
check_PROGRAMS = \
	tests/test_arena \
	tests/test_hash \
	tests/test_util \
	tests/test_log \
	tests/test_protocol \
	tests/test_data

TESTS = $(check_PROGRAMS)

tests_test_arena_SOURCES = tests/test_arena.c server/arena.c server/util.c server/log.c
tests_test_arena_CFLAGS = $(AM_CFLAGS) -DLOG_LEVEL=0

tests_test_hash_SOURCES = tests/test_hash.c server/hash.c server/xxhash.c server/arena.c server/util.c server/log.c
tests_test_hash_CFLAGS = $(AM_CFLAGS) -DLOG_LEVEL=0

tests_test_util_SOURCES = tests/test_util.c server/util.c server/log.c
tests_test_util_CFLAGS = $(AM_CFLAGS) -DLOG_LEVEL=0

tests_test_log_SOURCES = tests/test_log.c server/log.c
tests_test_log_CFLAGS = $(AM_CFLAGS) -DLOG_LEVEL=0

tests_test_protocol_SOURCES = tests/test_protocol.c
tests_test_protocol_CFLAGS = $(AM_CFLAGS)

tests_test_data_SOURCES = tests/test_data.c server/data.c server/config.c server/hash.c server/xxhash.c server/arena.c server/util.c server/log.c
tests_test_data_CFLAGS = $(AM_CFLAGS) -DLOG_LEVEL=0
tests_test_data_LDADD = $(JANSSON_LIBS)

test: check
