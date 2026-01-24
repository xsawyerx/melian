#pragma once

#include <stdint.h>
#include "protocol.h"

// TODO: make these limits dynamic? Arena?
enum {
  MAX_RESPONSE_LEN = 10240,
};

// Options available when running a client.
struct Options {
  const char *host;
  unsigned port;
  const char *unix;
  unsigned fetches[26*2+10]; // lowercase, uppercase, digits
  unsigned stats;
  unsigned quit;
  unsigned verbose;
};

struct TableData {
  unsigned rows;
  unsigned min_id;
  unsigned max_id;
};

typedef struct ClientField {
  char* name;
  uint8_t type;
  uint32_t len;
  union {
    int64_t i64;
    double f64;
    uint8_t b;
    uint8_t* bytes;
  } value;
} ClientField;

typedef struct ClientRow {
  uint32_t field_count;
  ClientField* fields;
} ClientRow;

// A running client.
typedef struct Client {
  struct Options options;
  int fd;
  unsigned rlen;
  char rbuf[MAX_RESPONSE_LEN];
  struct TableData tables[DATA_TABLE_LAST];
} Client;

Client* client_build(void);
void client_destroy(Client* client);
unsigned client_configure(Client* client, int argc, char* argv[]);

int client_read_response(Client* client);

ClientRow* client_decode_row(const uint8_t* payload, unsigned length);
void client_row_free(ClientRow* row);

void client_run(Client* client);
