CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?= -lm

SRC = \
  src/main.c \
  src/args.c \
  src/timeutil.c \
  src/hist.c \
  src/net.c \
  src/evloop.c \
  src/proto_melian.c \
  src/proto_redis.c

BIN = melbench

all: $(BIN)

$(BIN): $(SRC)
	$(CC) $(CFLAGS) -o $@ $(SRC) $(LDFLAGS)

clean:
	rm -f $(BIN)

.PHONY: all clean
