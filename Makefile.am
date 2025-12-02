AUTOMAKE_OPTIONS = foreign subdir-objects

bin_PROGRAMS = melian-server melian-client

AM_CPPFLAGS = \
	-I$(top_srcdir) \
	-I$(top_srcdir)/server \
	-I$(top_srcdir)/clients/c \
	-I$(top_srcdir)/common \
	$(MYSQL_CFLAGS) \
	$(SQLITE_CFLAGS) \
	$(PLATFORM_CPPFLAGS)

AM_CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -g

melian_server_SOURCES = \
	server/util.c \
	server/log.c \
	server/json.c \
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
	$(PTHREAD_LIBS) \
	$(LIBM)

melian_client_SOURCES = \
	clients/c/client.c \
	clients/c/melian-client.c

melian_client_LDADD = \
	$(JANSSON_LIBS) \
	$(LIBM)
