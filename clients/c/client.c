#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <jansson.h>
#include "client.h"

#define ALEN(a) (sizeof(a) / sizeof((a)[0]))

enum FetchKeyMode {
  KEY_MODE_NUMERIC_ID,
  KEY_MODE_HOSTNAME,
};

struct FetchBinding {
  char action;
  const char* table_name;
  const char* index_name;
  enum FetchKeyMode key_mode;
  unsigned legacy_table;
  unsigned table_id;
  unsigned index_id;
  unsigned resolved;
};

static struct FetchBinding fetch_bindings[] = {
  { MELIAN_ACTION_QUERY_TABLE1_BY_ID, "table1", "id",        KEY_MODE_NUMERIC_ID, DATA_TABLE_TABLE1, 0, 0, 0 },
  { MELIAN_ACTION_QUERY_TABLE2_BY_ID, "table2", "id",        KEY_MODE_NUMERIC_ID, DATA_TABLE_TABLE2, 0, 0, 0 },
  { MELIAN_ACTION_QUERY_TABLE2_BY_HOST, "table2", "hostname", KEY_MODE_HOSTNAME,   DATA_TABLE_TABLE2, 0, 0, 0 },
};

// TODO: make these limits dynamic? Arena?
enum {
  US_IN_ONE_SECOND = 1000000,
  MS_IN_ONE_SECOND = 1000,
  MAX_HOST_LEN = 128,
};

static void create_socket(Client* client);
static double now_sec(void);
static void terminate(const char* msg, unsigned use_perr);
static unsigned action_to_index(char action);
static void client_send_request(Client* client, uint8_t action, uint8_t table_id, uint8_t index_id, const uint8_t* key, unsigned key_len);
static json_t* client_describe_schema(Client* client);
static void resolve_fetch_bindings(json_t* schema);
static int resolve_fetch_binding(struct FetchBinding* binding, json_t* tables);
static uint16_t read_le16(const uint8_t *buf);
static uint32_t read_le32(const uint8_t *buf);
static uint64_t read_le64(const uint8_t *buf);
static unsigned parse_fetch_args(Client* client, int argc, char* argv[], int start);
static int resolve_adhoc_fetch(Client* client, json_t* schema, unsigned* out_table_id, unsigned* out_index_id, const char** out_index_type);
static void print_row_json(ClientRow* row);
static void client_run_adhoc_fetch(Client* client);
static void client_run_schema(Client* client);
static void client_run_adhoc_stats(Client* client);
static void client_run_bench(Client* client);

Client* client_build(void) {
  Client* client = calloc(1, sizeof(Client));

  client->options.host = MELIAN_DEFAULT_DB_HOST;
  client->options.unix = MELIAN_DEFAULT_SOCKET_PATH;
  client->options.fetch.table_id = -1;
  client->options.fetch.index_id = -1;

  return client;
}

void client_destroy(Client* client) {
  free(client);
}

unsigned client_configure(Client* client, int argc, char* argv[]) {
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:p:u:UCHOsqv")) != -1) {
    switch (opt) {
      case 'h':
        client->options.host = optarg;
        break;
      case 'p':
        client->options.port = atoi(optarg);
        break;
      case 'u':
        client->options.unix = optarg;
        break;
      case MELIAN_ACTION_QUERY_TABLE1_BY_ID:
        client->options.fetches[action_to_index(opt)] = 1;
        break;
      case MELIAN_ACTION_QUERY_TABLE2_BY_ID:
        client->options.fetches[action_to_index(opt)] = 1;
        break;
      case MELIAN_ACTION_QUERY_TABLE2_BY_HOST:
        client->options.fetches[action_to_index(opt)] = 1;
        break;
      case 's':
        client->options.stats = 1;
        break;
      case 'q':
        client->options.quit = 1;
        break;
      case 'v':
        client->options.verbose = 1;
        break;
      default:
        return 0;
    }
  }

  if (optind < argc) {
    const char* subcmd = argv[optind];
    if (strcmp(subcmd, "fetch") == 0) {
      client->options.mode = CLIENT_MODE_FETCH;
      return parse_fetch_args(client, argc, argv, optind + 1);
    } else if (strcmp(subcmd, "schema") == 0) {
      client->options.mode = CLIENT_MODE_SCHEMA;
      return 1;
    } else if (strcmp(subcmd, "stats") == 0) {
      client->options.mode = CLIENT_MODE_STATS;
      return 1;
    }
    fprintf(stderr, "Unknown subcommand: %s\n", subcmd);
    return 0;
  }

  return 1;
}

static void client_send_request(Client* client, uint8_t action, uint8_t table_id, uint8_t index_id, const uint8_t* key, unsigned key_len) {
  MelianRequestHeader hdr;
  hdr.data.version = MELIAN_HEADER_VERSION;
  hdr.data.action = action;
  hdr.data.table_id = table_id;
  hdr.data.index_id = index_id;
  hdr.data.length = htonl(key_len);
  if (write(client->fd, &hdr, sizeof(MelianRequestHeader)) != sizeof(MelianRequestHeader)) terminate("write hdr", 1);
  if (key_len > 0 && key) {
    if (write(client->fd, key, key_len) != (ssize_t)key_len) terminate("write key", 1);
  }
}

int client_read_response(Client* client) {
  /* Read response: LEN(4B BE) + VALUE(LEN) */
  client->rlen = 0;
  MelianResponseHeader hdr;
  ssize_t n = read(client->fd, &hdr, sizeof(MelianResponseHeader));
  if (n <= 0) return -1;
  // TODO: do we need to fix this?
  if (n != sizeof(MelianResponseHeader)) terminate("short read len", 0);
  client->rlen = ntohl(hdr.data.length);
  if (client->rlen > 0) {
    unsigned got = 0;
    while (got < client->rlen) {
      ssize_t r = read(client->fd, client->rbuf + got, client->rlen - got);
      if (r <= 0) terminate("read val", 1);
      got += r;
    }
    // printf("Got %u bytes: [%.*s]\n", got, got, client->rbuf);
  } else {
    // printf("Response: <empty>\n");
  }
  return client->rlen;
}

static uint16_t read_le16(const uint8_t *buf) {
  return (uint16_t)(buf[0] | ((uint16_t)buf[1] << 8));
}

static uint32_t read_le32(const uint8_t *buf) {
  return (uint32_t)(buf[0] | ((uint32_t)buf[1] << 8) | ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24));
}

static uint64_t read_le64(const uint8_t *buf) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= ((uint64_t)buf[i]) << (8 * i);
  }
  return v;
}

ClientRow* client_decode_row(const uint8_t* payload, unsigned length) {
  if (!payload || length < 4) return NULL;
  unsigned offset = 0;
  uint32_t field_count = read_le32(payload);
  offset += 4;

  ClientRow* row = calloc(1, sizeof(ClientRow));
  if (!row) return NULL;
  row->field_count = field_count;
  row->fields = calloc(field_count, sizeof(ClientField));
  if (!row->fields) {
    free(row);
    return NULL;
  }

  for (uint32_t i = 0; i < field_count; ++i) {
    if (offset + 2 > length) goto fail;
    uint16_t name_len = read_le16(payload + offset);
    offset += 2;
    if (offset + name_len + 1 + 4 > length) goto fail;
    row->fields[i].name = calloc(1, (size_t)name_len + 1);
    if (!row->fields[i].name) goto fail;
    memcpy(row->fields[i].name, payload + offset, name_len);
    offset += name_len;
    uint8_t type = payload[offset++];
    uint32_t value_len = read_le32(payload + offset);
    offset += 4;
    if (offset + value_len > length) goto fail;

    row->fields[i].type = type;
    row->fields[i].len = value_len;

    if (type == MELIAN_VALUE_NULL) {
      offset += value_len;
      continue;
    }
    if (type == MELIAN_VALUE_BOOL) {
      row->fields[i].value.b = value_len ? payload[offset] : 0;
      offset += value_len;
      continue;
    }
    if (type == MELIAN_VALUE_INT64) {
      if (value_len != 8) goto fail;
      row->fields[i].value.i64 = (int64_t)read_le64(payload + offset);
      offset += 8;
      continue;
    }
    if (type == MELIAN_VALUE_FLOAT64) {
      if (value_len != 8) goto fail;
      uint64_t bits = read_le64(payload + offset);
      memcpy(&row->fields[i].value.f64, &bits, sizeof(bits));
      offset += 8;
      continue;
    }

    if (value_len > 0) {
      row->fields[i].value.bytes = malloc(value_len);
      if (!row->fields[i].value.bytes) goto fail;
      memcpy(row->fields[i].value.bytes, payload + offset, value_len);
    }
    offset += value_len;
  }
  return row;

fail:
  client_row_free(row);
  return NULL;
}

void client_row_free(ClientRow* row) {
  if (!row) return;
  if (row->fields) {
    for (uint32_t i = 0; i < row->field_count; ++i) {
      free(row->fields[i].name);
      if (row->fields[i].len > 0 &&
          row->fields[i].type != MELIAN_VALUE_INT64 &&
          row->fields[i].type != MELIAN_VALUE_FLOAT64 &&
          row->fields[i].type != MELIAN_VALUE_BOOL &&
          row->fields[i].type != MELIAN_VALUE_NULL) {
        free(row->fields[i].value.bytes);
      }
    }
    free(row->fields);
  }
  free(row);
}

static void create_socket(Client* client) {
  client->fd = -1;
  do {
    if (client->options.port > 0) {
      client->fd = socket(AF_INET, SOCK_STREAM, 0);
      if (client->fd < 0) terminate("socket", 1);
      struct sockaddr_in sin;
      memset(&sin, 0, sizeof(sin));
      sin.sin_family = AF_INET;
      sin.sin_port = htons(client->options.port);
      inet_pton(AF_INET, client->options.host, &sin.sin_addr);
      if (connect(client->fd, (struct sockaddr*)&sin, sizeof(sin)) < 0) terminate("connect", 1);
      if (client->options.verbose) printf("Connected on TCP socket to host %s port %u\n", client->options.host, client->options.port);
      break;
    }

    if (client->options.unix != 0) {
      client->fd = socket(AF_UNIX, SOCK_STREAM, 0);
      if (client->fd < 0) terminate("socket", 1);
      struct sockaddr_un sun;
      memset(&sun, 0, sizeof(sun));
      sun.sun_family = AF_UNIX;
      strncpy(sun.sun_path, client->options.unix, sizeof(sun.sun_path)-1);
      if (connect(client->fd, (struct sockaddr*)&sun, sizeof(sun)) < 0) terminate("connect", 1);
      if (client->options.verbose) printf("Connected on UNIX socket %s\n", client->options.unix);
      break;
    }
  } while (0);
}

static void client_send_action(Client* client, char action) {
  client_send_request(client, action, 0, 0, NULL, 0);
  client_read_response(client);
  if ((action == MELIAN_ACTION_GET_STATISTICS && client->options.stats) ||
      (action == MELIAN_ACTION_QUIT && client->options.quit)) {
    printf("%.*s\n", client->rlen, client->rbuf);
  }
  // printf("ACTION %c: got %u bytes => [%.*s]\n", action, client->rlen, client->rlen, client->rbuf);
}

static json_t* client_describe_schema(Client* client) {
  client_send_request(client, MELIAN_ACTION_DESCRIBE_SCHEMA, 0, 0, NULL, 0);
  if (client_read_response(client) <= 0) {
    fprintf(stderr, "Failed to read schema response\n");
    return NULL;
  }
  json_error_t error;
  json_t* schema = json_loadb(client->rbuf, client->rlen, JSON_DECODE_ANY, &error);
  if (!schema) {
    fprintf(stderr, "Failed to parse schema JSON: %s\n", error.text);
  }
  return schema;
}

static void resolve_fetch_bindings(json_t* schema) {
  json_t* tables = json_object_get(schema, "tables");
  if (!json_is_array(tables)) {
    fprintf(stderr, "Schema missing 'tables' array\n");
    return;
  }

  for (unsigned f = 0; f < ALEN(fetch_bindings); ++f) {
    struct FetchBinding* binding = &fetch_bindings[f];
    binding->resolved = resolve_fetch_binding(binding, tables);
    if (!binding->resolved) {
      fprintf(stderr, "Warning: could not resolve table '%s' index '%s'\n",
              binding->table_name, binding->index_name);
    }
  }
}

static int resolve_fetch_binding(struct FetchBinding* binding, json_t* tables) {
  size_t idx = 0;
  json_t* table = 0;
  json_array_foreach(tables, idx, table) {
    const char* name = json_string_value(json_object_get(table, "name"));
    if (!name || strcmp(name, binding->table_name) != 0) continue;

    json_t* tid = json_object_get(table, "id");
    if (!json_is_integer(tid)) continue;
    binding->table_id = (unsigned)json_integer_value(tid);

    json_t* indexes = json_object_get(table, "indexes");
    if (!json_is_array(indexes)) continue;
    size_t jdx = 0;
    json_t* index = 0;
    json_array_foreach(indexes, jdx, index) {
      const char* column = json_string_value(json_object_get(index, "column"));
      if (!column || strcmp(column, binding->index_name) != 0) continue;
      json_t* iid = json_object_get(index, "id");
      if (!json_is_integer(iid)) continue;
      binding->index_id = (unsigned)json_integer_value(iid);
      return 1;
    }
  }
  return 0;
}

static int cmp_ulong(const void *lv, const void *rv) {
  const unsigned long* lu = lv;
  const unsigned long* ru = rv;
  return *lu - *ru;
}

static void client_fetch(Client* client, struct FetchBinding* binding) {
  if (!binding->resolved) {
    fprintf(stderr, "Skipping action %c: unresolved binding for %s.%s\n",
            binding->action, binding->table_name, binding->index_name);
    return;
  }
  unsigned table = binding->legacy_table;
  unsigned count = client->tables[table].rows;
  unsigned long good = 0;
  unsigned long bad = 0;
  double sum = 0;
  double sum2 = 0;
  unsigned long* dbuf = calloc(1, count * sizeof(unsigned long));
  unsigned dpos = 0;
  for (unsigned id = 1; id <= count; ++id) {
    unsigned long elapsed_us = 0;
    unsigned bytes = 0;
    switch (binding->key_mode) {
      case KEY_MODE_NUMERIC_ID: {
        unsigned key = id;
        double t0 = now_sec();
        client_send_request(client, MELIAN_ACTION_FETCH,
                            binding->table_id, binding->index_id,
                            (uint8_t*)&key, sizeof(unsigned));
        bytes = client_read_response(client);
        double t1 = now_sec();
        elapsed_us = (t1 - t0) * US_IN_ONE_SECOND;
        break;
      }
      case KEY_MODE_HOSTNAME: {
        char host[MAX_HOST_LEN];
        unsigned len = snprintf(host, MAX_HOST_LEN, "host-%05u", id);
        double t0 = now_sec();
        client_send_request(client, MELIAN_ACTION_FETCH,
                            binding->table_id, binding->index_id,
                            (uint8_t*)host, len);
        bytes = client_read_response(client);
        double t1 = now_sec();
        elapsed_us = (t1 - t0) * US_IN_ONE_SECOND;
        break;
      }
      default:
        fprintf(stderr, "Unsupported key mode for action %c\n", binding->action);
        return;
    }
    if (bytes <= 0) {
      ++bad;
      continue;
    }
    ++good;
    dbuf[dpos++] = elapsed_us;
    sum += elapsed_us;
    sum2 += elapsed_us * elapsed_us;
  }
  double elapsed_s = sum / US_IN_ONE_SECOND;
  unsigned elapsed_ms = (unsigned) (elapsed_s * MS_IN_ONE_SECOND);
  double rps = (double) good / elapsed_s;
  double mean = sum / good;
  double variance = (sum2 - (sum * sum) / good) / (sum - 1);
  double stddev = sqrt(variance);
  unsigned long cv = (unsigned long) (stddev * 100 / mean);
  // printf("good %lu bad %lu sum %f sum2 %f mean %f variance %f\n", good, bad, sum, sum2, mean, variance);

  unsigned long p95 = 0;
  if (dpos > 0) {
    qsort(dbuf, dpos, sizeof(unsigned long), cmp_ulong);
    unsigned pos_95 = 95 * dpos / 100;
    p95 = dbuf[pos_95];
  }
  free(dbuf);

  printf("%c: %6u reqs, %6lu good, %3lu bad, %4u ms → %7.0f req/s, %9.5f ± %8.5f μs/req, CV: %3lu%%, P95: %3lu μs\n",
         binding->action, count, good, bad, elapsed_ms, rps, mean, stddev, cv, p95);
}

static void client_get_table_data(Client* client) {
  client_send_action(client, MELIAN_ACTION_GET_STATISTICS);
  if (!client->rlen || client->rbuf[0] != '{') return;

  unsigned flags = JSON_DECODE_ANY;
  json_t* json = json_loadb(client->rbuf, client->rlen, flags, 0);
  // fprintf(stderr, "Converted to JSON %p: %u\n", json, json_is_object(json));
  json_t* tables = json_object_get(json, "tables");
  const char *name = 0;
  json_t *data = 0;
  json_object_foreach(tables, name, data) {
    int rows = 0;
    int min_id = 0;
    int max_id = 0;
    json_unpack(data, "{s:i,s:i,s:i}",
                "rows", &rows, "min_id", &min_id, "max_id", &max_id);
    // fprintf(stderr, "Table [%s]: rows %u, min_id %u, max_id %u\n", name, rows, min_id, max_id);
    unsigned t = -1;
    if (strcmp(name, "table1") == 0) t = DATA_TABLE_TABLE1;
    if (strcmp(name, "table2") == 0) t = DATA_TABLE_TABLE2;
    if (t == (unsigned)-1) continue;
    client->tables[t].rows = rows;
    client->tables[t].min_id = min_id;
    client->tables[t].max_id = max_id;
  }
}

static unsigned parse_fetch_args(Client* client, int argc, char* argv[], int start) {
  for (int i = start; i < argc; i++) {
    if (strcmp(argv[i], "--table") == 0 && i + 1 < argc) {
      client->options.fetch.table_name = argv[++i];
    } else if (strcmp(argv[i], "--table-id") == 0 && i + 1 < argc) {
      client->options.fetch.table_id = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--index") == 0 && i + 1 < argc) {
      client->options.fetch.index_name = argv[++i];
    } else if (strcmp(argv[i], "--index-id") == 0 && i + 1 < argc) {
      client->options.fetch.index_id = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--key") == 0 && i + 1 < argc) {
      client->options.fetch.key = argv[++i];
    } else {
      fprintf(stderr, "Unknown fetch option: %s\n", argv[i]);
      return 0;
    }
  }

  if (!client->options.fetch.table_name && client->options.fetch.table_id < 0) {
    fprintf(stderr, "fetch: --table or --table-id is required\n");
    return 0;
  }
  if (client->options.fetch.table_name && client->options.fetch.table_id >= 0) {
    fprintf(stderr, "fetch: --table and --table-id are mutually exclusive\n");
    return 0;
  }
  if (!client->options.fetch.index_name && client->options.fetch.index_id < 0) {
    fprintf(stderr, "fetch: --index or --index-id is required\n");
    return 0;
  }
  if (client->options.fetch.index_name && client->options.fetch.index_id >= 0) {
    fprintf(stderr, "fetch: --index and --index-id are mutually exclusive\n");
    return 0;
  }
  if (!client->options.fetch.key) {
    fprintf(stderr, "fetch: --key is required\n");
    return 0;
  }
  return 1;
}

static int resolve_adhoc_fetch(Client* client, json_t* schema,
                               unsigned* out_table_id, unsigned* out_index_id,
                               const char** out_index_type) {
  struct FetchOptions* fo = &client->options.fetch;
  json_t* tables = json_object_get(schema, "tables");
  if (!json_is_array(tables)) {
    fprintf(stderr, "Schema missing 'tables' array\n");
    return 0;
  }

  json_t* table = NULL;
  size_t idx;
  json_t* val;
  json_array_foreach(tables, idx, val) {
    if (fo->table_name) {
      const char* name = json_string_value(json_object_get(val, "name"));
      if (name && strcmp(name, fo->table_name) == 0) { table = val; break; }
    } else {
      json_t* tid = json_object_get(val, "id");
      if (json_is_integer(tid) && json_integer_value(tid) == fo->table_id) { table = val; break; }
    }
  }
  if (!table) {
    if (fo->table_name)
      fprintf(stderr, "Table '%s' not found in schema\n", fo->table_name);
    else
      fprintf(stderr, "Table ID %d not found in schema\n", fo->table_id);
    return 0;
  }
  *out_table_id = (unsigned)json_integer_value(json_object_get(table, "id"));

  json_t* indexes = json_object_get(table, "indexes");
  if (!json_is_array(indexes)) {
    fprintf(stderr, "Table has no indexes\n");
    return 0;
  }

  json_t* index = NULL;
  json_array_foreach(indexes, idx, val) {
    if (fo->index_name) {
      const char* col = json_string_value(json_object_get(val, "column"));
      if (col && strcmp(col, fo->index_name) == 0) { index = val; break; }
    } else {
      json_t* iid = json_object_get(val, "id");
      if (json_is_integer(iid) && json_integer_value(iid) == fo->index_id) { index = val; break; }
    }
  }
  if (!index) {
    if (fo->index_name)
      fprintf(stderr, "Index '%s' not found in table\n", fo->index_name);
    else
      fprintf(stderr, "Index ID %d not found in table\n", fo->index_id);
    return 0;
  }
  *out_index_id = (unsigned)json_integer_value(json_object_get(index, "id"));
  *out_index_type = json_string_value(json_object_get(index, "type"));
  return 1;
}

static void print_row_json(ClientRow* row) {
  json_t* obj = json_object();
  for (uint32_t i = 0; i < row->field_count; i++) {
    ClientField* f = &row->fields[i];
    switch (f->type) {
      case MELIAN_VALUE_NULL:
        json_object_set_new(obj, f->name, json_null());
        break;
      case MELIAN_VALUE_INT64:
        json_object_set_new(obj, f->name, json_integer(f->value.i64));
        break;
      case MELIAN_VALUE_FLOAT64:
        json_object_set_new(obj, f->name, json_real(f->value.f64));
        break;
      case MELIAN_VALUE_BYTES:
      case MELIAN_VALUE_DECIMAL: {
        char* str = calloc(1, f->len + 1);
        memcpy(str, f->value.bytes, f->len);
        json_object_set_new(obj, f->name, json_string(str));
        free(str);
        break;
      }
      case MELIAN_VALUE_BOOL:
        json_object_set_new(obj, f->name, f->value.b ? json_true() : json_false());
        break;
    }
  }
  char* s = json_dumps(obj, JSON_INDENT(2) | JSON_PRESERVE_ORDER);
  printf("%s\n", s);
  free(s);
  json_decref(obj);
}

static void client_run_adhoc_fetch(Client* client) {
  json_t* schema = client_describe_schema(client);
  if (!schema) terminate("describe schema", 0);

  unsigned table_id, index_id;
  const char* index_type;
  if (!resolve_adhoc_fetch(client, schema, &table_id, &index_id, &index_type)) {
    json_decref(schema);
    exit(1);
  }

  if (client->options.verbose) {
    fprintf(stderr, "Resolved: table_id=%u, index_id=%u, type=%s\n",
            table_id, index_id, index_type);
  }

  int is_int_key = strcmp(index_type, "int") == 0;
  json_decref(schema);

  const char* key = client->options.fetch.key;
  if (is_int_key) {
    char* endptr;
    long val = strtol(key, &endptr, 10);
    if (*endptr != '\0') {
      fprintf(stderr, "Key '%s' is not a valid integer for index type 'int'\n", key);
      exit(1);
    }
    unsigned ikey = (unsigned)val;
    if (client->options.verbose) {
      fprintf(stderr, "Fetching: table_id=%u index_id=%u key=%u (int, %zu bytes)\n",
              table_id, index_id, ikey, sizeof(unsigned));
    }
    client_send_request(client, MELIAN_ACTION_FETCH, table_id, index_id,
                        (uint8_t*)&ikey, sizeof(unsigned));
  } else {
    if (client->options.verbose) {
      fprintf(stderr, "Fetching: table_id=%u index_id=%u key=\"%s\" (string, %zu bytes)\n",
              table_id, index_id, key, strlen(key));
    }
    client_send_request(client, MELIAN_ACTION_FETCH, table_id, index_id,
                        (uint8_t*)key, strlen(key));
  }

  int bytes = client_read_response(client);
  if (bytes <= 0) {
    fprintf(stderr, "No row found (table_id=%u, index_id=%u, key=%s, type=%s)\n",
            table_id, index_id, key, is_int_key ? "int" : "string");
    return;
  }

  ClientRow* row = client_decode_row((uint8_t*)client->rbuf, client->rlen);
  if (!row) {
    fprintf(stderr, "Failed to decode response (%u bytes)\n", client->rlen);
    exit(1);
  }
  print_row_json(row);
  client_row_free(row);
}

static void client_run_schema(Client* client) {
  json_t* schema = client_describe_schema(client);
  if (!schema) terminate("describe schema", 0);
  char* s = json_dumps(schema, JSON_INDENT(2) | JSON_PRESERVE_ORDER);
  printf("%s\n", s);
  free(s);
  json_decref(schema);
}

static void client_run_adhoc_stats(Client* client) {
  client_send_request(client, MELIAN_ACTION_GET_STATISTICS, 0, 0, NULL, 0);
  if (client_read_response(client) <= 0) {
    fprintf(stderr, "Failed to read statistics\n");
    return;
  }
  json_error_t error;
  json_t* stats = json_loadb(client->rbuf, client->rlen, JSON_DECODE_ANY, &error);
  if (!stats) {
    fprintf(stderr, "Failed to parse statistics: %s\n", error.text);
    return;
  }
  char* s = json_dumps(stats, JSON_INDENT(2) | JSON_PRESERVE_ORDER);
  printf("%s\n", s);
  free(s);
  json_decref(stats);
}

static void client_run_bench(Client* client) {
  json_t* schema = client_describe_schema(client);
  if (!schema) terminate("describe schema", 0);
  resolve_fetch_bindings(schema);
  json_decref(schema);

  client_get_table_data(client);

  for (unsigned f = 0; f < ALEN(fetch_bindings); ++f) {
    struct FetchBinding* binding = &fetch_bindings[f];
    if (!client->options.fetches[action_to_index(binding->action)]) continue;
    client_fetch(client, binding);
  }

  if (client->options.quit) {
    client_send_action(client, MELIAN_ACTION_QUIT);
  }
}

void client_run(Client* client) {
  create_socket(client);
  if (client->fd < 0) terminate("create_socket", 0);

  switch (client->options.mode) {
    case CLIENT_MODE_FETCH:
      client_run_adhoc_fetch(client);
      break;
    case CLIENT_MODE_SCHEMA:
      client_run_schema(client);
      break;
    case CLIENT_MODE_STATS:
      client_run_adhoc_stats(client);
      break;
    case CLIENT_MODE_BENCH:
    default:
      client_run_bench(client);
      break;
  }

  close(client->fd);
}

static double now_sec(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void terminate(const char* msg, unsigned use_perr) {
  if (use_perr) {
    perror(msg);
  } else {
    fprintf(stderr, "%s\n", msg);
  }
  exit(1);
}

static unsigned action_to_index(char action) {
  static struct Range {
    char beg;
    char end;
  } ranges[] = {
    { 'A', 'Z' },
    { 'a', 'z' },
    { '0', '9' },
  };
  unsigned offset = 0;
  for (unsigned r = 0; r < sizeof(ranges) / sizeof(ranges[0]); ++r) {
    struct Range* range = &ranges[r];
    if (action >= range->beg && action <= range->end) {
      unsigned index = action - range->beg + offset;
      return index;
    }
    offset += range->end - range->beg + 1;
  }
  return -1;
}
