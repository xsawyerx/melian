#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <event2/event.h>
#include <jansson.h>
#include "util.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "config.h"
#include "data.h"
#include "db.h"
#include "status.h"

static unsigned get_uptime(Status* status);
static const char* safe_string(const char* value);
static json_t* json_epoch_object(unsigned epoch);
static json_t* json_table(Table* table);
static json_t* json_table_arena(Arena* arena, unsigned rows);
static json_t* json_table_hashes(Table* table, struct TableSlot* slot);
static json_t* json_table_hash(const char* tname, Hash* hash, const char* iname);
static int json_object_set_new_take(json_t* obj, const char* key, json_t* value);
static int json_object_set_string(json_t* obj, const char* key, const char* value);
static int json_object_set_uint(json_t* obj, const char* key, unsigned value);
static int json_object_set_real(json_t* obj, const char* key, double value);
static int json_object_set_bool(json_t* obj, const char* key, unsigned value);

Status* status_build(struct event_base *base, DB* db) {
  Status* status = 0;
  do {
    status = calloc(1, sizeof(Status));
    if (!status) {
      LOG_WARN("Could not allocate Status object");
      break;
    }
    status->db = db;

    status->process.pid = getpid();
    status->process.birth = time(0);

    struct utsname uts;
    uname(&uts);
    strcpy(status->server.host, uts.nodename);
    strcpy(status->server.system, uts.sysname);
    strcpy(status->server.machine, uts.machine);
    strcpy(status->server.release, uts.release);

    strcpy(status->libevent.version, event_get_version());
    strcpy(status->libevent.method, event_base_get_method(base));
  } while (0);

  return status;
}

void status_destroy(Status* status) {
  if (!status) return;
  free(status);
}

void status_log(Status* status) {
  LOG_INFO("Running on host %s, system %s, release %s, hardware %s",
           status->server.host, status->server.system,
           status->server.release, status->server.machine);
  LOG_INFO("Using libevent version %s with method %s",
           status->libevent.version, status->libevent.method);

  const char* driver_name = config_db_driver_name(status->db->config->db.driver);
  const char* c = status->db->client_version;
  if (c && c[0]) {
    LOG_INFO("Using %s client version %s", driver_name, c);
  }
  const char* s = status->db->server_version;
  if (s && s[0]) {
    LOG_INFO("Using %s server version %s", driver_name, s);
  }

  char birth[MAX_STAMP_LEN];
  format_timestamp(status->process.birth, birth, MAX_STAMP_LEN);
  LOG_INFO("Process pid %u, started on %s, uptime %u",
           status->process.pid, birth, get_uptime(status));
}

void status_json(Status* status, Config* config, Data* data) {
  json_t* root = NULL;
  char* dump = NULL;
  unsigned success = 0;
  const char* driver_key = config_db_driver_name(config->db.driver);

  status->json.jlen = 0;
  status->json.jbuf[0] = '\0';

  root = json_object();
  if (!root) {
    LOG_WARN("Failed to allocate status root JSON object");
    goto done;
  }

  json_t* server_obj = json_object();
  if (!server_obj) goto done;
  if (json_object_set_string(server_obj, "host", status->server.host) < 0 ||
      json_object_set_string(server_obj, "system", status->server.system) < 0 ||
      json_object_set_string(server_obj, "machine", status->server.machine) < 0 ||
      json_object_set_string(server_obj, "release", status->server.release) < 0) {
    json_decref(server_obj);
    goto done;
  }
  if (json_object_set_new_take(root, "server", server_obj) < 0) goto done;

  json_t* software_obj = json_object();
  if (!software_obj) goto done;

  json_t* libevent_obj = json_object();
  if (!libevent_obj) {
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_string(libevent_obj, "version", status->libevent.version) < 0 ||
      json_object_set_string(libevent_obj, "method", status->libevent.method) < 0) {
    json_decref(libevent_obj);
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_new_take(software_obj, "libevent", libevent_obj) < 0) {
    json_decref(software_obj);
    goto done;
  }

  json_t* driver_obj = json_object();
  if (!driver_obj) {
    json_decref(software_obj);
    goto done;
  }

  json_t* client_obj = json_object();
  if (!client_obj) {
    json_decref(driver_obj);
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_string(client_obj, "version", safe_string(status->db->client_version)) < 0) {
    json_decref(client_obj);
    json_decref(driver_obj);
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_new_take(driver_obj, "client", client_obj) < 0) {
    json_decref(driver_obj);
    json_decref(software_obj);
    goto done;
  }

  json_t* server_version_obj = json_object();
  if (!server_version_obj) {
    json_decref(driver_obj);
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_string(server_version_obj, "version", safe_string(status->db->server_version)) < 0) {
    json_decref(server_version_obj);
    json_decref(driver_obj);
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_new_take(driver_obj, "server", server_version_obj) < 0) {
    json_decref(driver_obj);
    json_decref(software_obj);
    goto done;
  }

  if (json_object_set_new_take(software_obj, driver_key, driver_obj) < 0) {
    json_decref(software_obj);
    goto done;
  }
  if (json_object_set_new_take(root, "software", software_obj) < 0) goto done;

  json_t* config_obj = json_object();
  if (!config_obj) goto done;

  json_t* config_driver_obj = json_object();
  if (!config_driver_obj) {
    json_decref(config_obj);
    goto done;
  }

  if (config->db.driver == CONFIG_DB_DRIVER_MYSQL ||
      config->db.driver == CONFIG_DB_DRIVER_POSTGRESQL) {
    if (json_object_set_string(config_driver_obj, "host", config->db.host) < 0 ||
        json_object_set_uint(config_driver_obj, "port", config->db.port) < 0 ||
        json_object_set_string(config_driver_obj, "database", config->db.database) < 0 ||
        json_object_set_string(config_driver_obj, "user", config->db.user) < 0) {
      json_decref(config_driver_obj);
      json_decref(config_obj);
      goto done;
    }
  } else if (config->db.driver == CONFIG_DB_DRIVER_SQLITE) {
    if (json_object_set_string(config_driver_obj, "filename",
                               safe_string(config->db.sqlite_filename)) < 0) {
      json_decref(config_driver_obj);
      json_decref(config_obj);
      goto done;
    }
  }

  if (json_object_set_new_take(config_obj, driver_key, config_driver_obj) < 0) {
    json_decref(config_obj);
    goto done;
  }

  json_t* socket_obj = json_object();
  if (!socket_obj) {
    json_decref(config_obj);
    goto done;
  }
  if (json_object_set_string(socket_obj, "host", config->socket.host) < 0 ||
      json_object_set_uint(socket_obj, "port", config->socket.port) < 0 ||
      json_object_set_string(socket_obj, "path", config->socket.path) < 0) {
    json_decref(socket_obj);
    json_decref(config_obj);
    goto done;
  }
  if (json_object_set_new_take(config_obj, "socket", socket_obj) < 0) {
    json_decref(config_obj);
    goto done;
  }

  json_t* table_obj = json_object();
  if (!table_obj) {
    json_decref(config_obj);
    goto done;
  }
  if (json_object_set_uint(table_obj, "period", config->table.period) < 0 ||
      json_object_set_string(table_obj, "schema", safe_string(config->table.schema)) < 0 ||
      json_object_set_bool(table_obj, "strip_null", config->table.strip_null) < 0) {
    json_decref(table_obj);
    json_decref(config_obj);
    goto done;
  }
  if (json_object_set_new_take(config_obj, "table", table_obj) < 0) {
    json_decref(config_obj);
    goto done;
  }

  json_t* server_cfg_obj = json_object();
  if (!server_cfg_obj) {
    json_decref(config_obj);
    goto done;
  }
  if (json_object_set_bool(server_cfg_obj, "show_msgs", config->server.show_msgs) < 0) {
    json_decref(server_cfg_obj);
    json_decref(config_obj);
    goto done;
  }
  if (json_object_set_new_take(config_obj, "server", server_cfg_obj) < 0) {
    json_decref(config_obj);
    goto done;
  }

  if (json_object_set_new_take(root, "config", config_obj) < 0) goto done;

  json_t* process_obj = json_object();
  if (!process_obj) goto done;
  if (json_object_set_uint(process_obj, "uptime", get_uptime(status)) < 0 ||
      json_object_set_new_take(process_obj, "birth", json_epoch_object(status->process.birth)) < 0) {
    json_decref(process_obj);
    goto done;
  }
  if (json_object_set_new_take(root, "process", process_obj) < 0) goto done;

  json_t* tables_obj = json_object();
  if (!tables_obj) goto done;
  for (unsigned t = 0; t < data->table_count; ++t) {
    Table* table = data->tables[t];
    if (!table) continue;
    json_t* table_json = json_table(table);
    if (!table_json) {
      json_decref(tables_obj);
      goto done;
    }
    if (json_object_set_new_take(tables_obj, table_name(table), table_json) < 0) {
      json_decref(tables_obj);
      goto done;
    }
  }
  if (json_object_set_new_take(root, "tables", tables_obj) < 0) {
    goto done;
  }

  dump = json_dumps(root, JSON_COMPACT | JSON_ENSURE_ASCII);
  if (!dump) {
    LOG_WARN("Failed to serialize status JSON");
    goto done;
  }

  size_t dump_len = strlen(dump);
  if (dump_len >= sizeof(status->json.jbuf)) {
    LOG_WARN("Status JSON truncated from %zu bytes to %zu", dump_len,
             sizeof(status->json.jbuf) - 1);
    dump_len = sizeof(status->json.jbuf) - 1;
  }
  memcpy(status->json.jbuf, dump, dump_len);
  status->json.jbuf[dump_len] = '\0';
  status->json.jlen = (unsigned)dump_len;
  success = 1;
  LOG_DEBUG("JSON %u %u [%.*s]", (unsigned)sizeof(status->json.jbuf), status->json.jlen,
            status->json.jlen, status->json.jbuf);

done:
  if (dump) free(dump);
  if (root) json_decref(root);
  if (!success) {
    status->json.jlen = 0;
    status->json.jbuf[0] = '\0';
    LOG_WARN("Building status JSON failed");
  }
}

static unsigned get_uptime(Status* status) {
  unsigned now = time(0);
  unsigned uptime = now - status->process.birth;
  return uptime;
}

static json_t* json_table(Table* table) {
  struct TableSlot* slot = &table->slots[table->current_slot];
  json_t* obj = json_object();
  if (!obj) return NULL;
  if (json_object_set_uint(obj, "id", table->table_id) < 0 ||
      json_object_set_uint(obj, "period", table->period) < 0 ||
      json_object_set_uint(obj, "rows", table->stats.rows) < 0 ||
      json_object_set_uint(obj, "min_id", table->stats.min_id) < 0 ||
      json_object_set_uint(obj, "max_id", table->stats.max_id) < 0 ||
      json_object_set_new_take(obj, "last_loaded",
                               json_epoch_object(table->stats.last_loaded)) < 0) {
    json_decref(obj);
    return NULL;
  }

  json_t* arena = json_table_arena(slot->arena, table->stats.rows);
  if (!arena || json_object_set_new_take(obj, "arena", arena) < 0) {
    if (arena) json_decref(arena);
    json_decref(obj);
    return NULL;
  }

  json_t* hashes = json_table_hashes(table, slot);
  if (!hashes || json_object_set_new_take(obj, "hashes", hashes) < 0) {
    if (hashes) json_decref(hashes);
    json_decref(obj);
    return NULL;
  }

  return obj;
}

static json_t* json_table_arena(Arena* arena, unsigned rows) {
  json_t* obj = json_object();
  if (!obj) return NULL;
  unsigned arena_cap = arena->capacity;
  unsigned arena_used = arena->used;
  unsigned arena_free = arena_cap - arena_used;
  double arena_bpr_avg = 0;
  if (rows) arena_bpr_avg = (double)arena->used / (double)rows;

  if (json_object_set_uint(obj, "capacity_bytes", arena_cap) < 0 ||
      json_object_set_uint(obj, "used_bytes", arena_used) < 0 ||
      json_object_set_uint(obj, "free_bytes", arena_free) < 0 ||
      json_object_set_real(obj, "row_avg_size_bytes", arena_bpr_avg) < 0) {
    json_decref(obj);
    return NULL;
  }
  return obj;
}

static json_t* json_table_hashes(Table* table, struct TableSlot* slot) {
  json_t* obj = json_object();
  if (!obj) return NULL;
  for (unsigned idx = 0; idx < table->index_count; ++idx) {
    Hash* hash = slot->indexes[idx];
    if (!hash) continue;
    json_t* hash_obj = json_table_hash(table_name(table), hash, table->indexes[idx].column);
    if (!hash_obj || json_object_set_new_take(obj, table->indexes[idx].column, hash_obj) < 0) {
      if (hash_obj) json_decref(hash_obj);
      json_decref(obj);
      return NULL;
    }
  }
  return obj;
}

struct Percentile {
  unsigned needed;
  unsigned pos;
  unsigned found;
  unsigned shown;
};
enum PercentileRange {
  PERC_50,
  PERC_95,
  PERC_99,
  PERC_LAST,
};

static json_t* json_table_hash(const char* tname, Hash* hash, const char* iname) {
  unsigned free = hash->cap - hash->used;
  double fill_factor = hash->cap ? (double)hash->used / (double)hash->cap : 0;
  unsigned probe_cnt = 0;
  unsigned probe_min = (unsigned)-1;
  unsigned probe_max = 0;
  for (unsigned h = 0; h < MAX_PROBE_COUNT; ++h) {
    unsigned val = h * hash->stats.probes[h];
    if (!val) continue;
    if (probe_min > h) probe_min = h;
    if (probe_max < h) probe_max = h;
    probe_cnt += val;
  }
  static unsigned levels[PERC_LAST] = {50, 95, 99};
  static const char* symbols[PERC_LAST] = {"M", "5", "9"};
  struct Percentile stats[PERC_LAST] = {0};
  for (unsigned s = 0; s < PERC_LAST; ++s) {
    stats[s].needed = levels[s] * probe_cnt / 100;
  }
  double ppq = 0;
  if (probe_cnt) {
    ppq = (double)probe_cnt / (double)hash->stats.queries;
    LOG_INFO("For table %s index %s: queries %u, probes %u (from %u to %u)",
             tname, iname, hash->stats.queries, probe_cnt, probe_min, probe_max);
    LOG_INFO("  Mean is %.1f probes/query", ppq);
    for (unsigned s = 0; s < PERC_LAST; ++s) {
      LOG_INFO("  P%02u needs %8u probes  - shown as %s", levels[s], stats[s].needed, symbols[s]);
    }
    unsigned sum_all = 0;
    for (unsigned h = probe_min; h <= probe_max; ++h) {
      unsigned val = h * hash->stats.probes[h];
      sum_all += val;
      for (unsigned s = 0; s < PERC_LAST; ++s) {
        if (stats[s].found) continue;
        if (stats[s].needed > sum_all) continue;
        stats[s].found = 1;
        stats[s].pos = h;
      }

      char mbuf[128];
      unsigned mpos = 0;
      for (unsigned s = 0; s < PERC_LAST; ++s) {
        if (stats[s].found && !stats[s].shown) {
          mpos += snprintf(mbuf + mpos, 128 - mpos, "%s", symbols[s]);
          stats[s].shown = 1;
        } else {
          mpos += snprintf(mbuf + mpos, 128 - mpos, "%s", " ");
        }
      }
      LOG_INFO("Probes %4u: num = %8u, acc = %8u ║ %.*s", h, val, sum_all, mpos, mbuf);
    }
  }

  json_t* obj = json_object();
  if (!obj) return NULL;
  if (json_object_set_uint(obj, "total_slots", hash->cap) < 0 ||
      json_object_set_uint(obj, "used_slots", hash->used) < 0 ||
      json_object_set_uint(obj, "free_slots", free) < 0 ||
      json_object_set_real(obj, "fill_factor_perc", fill_factor * 100) < 0 ||
      json_object_set_uint(obj, "queries", hash->stats.queries) < 0 ||
      json_object_set_uint(obj, "probes", probe_cnt) < 0 ||
      json_object_set_real(obj, "probes_per_query_avg", ppq) < 0 ||
      json_object_set_uint(obj, "probes_p50", stats[PERC_50].pos) < 0 ||
      json_object_set_uint(obj, "probes_p95", stats[PERC_95].pos) < 0 ||
      json_object_set_uint(obj, "probes_p99", stats[PERC_99].pos) < 0) {
    json_decref(obj);
    return NULL;
  }
  return obj;
}

static const char* safe_string(const char* value) {
  return value ? value : "";
}

static json_t* json_epoch_object(unsigned epoch) {
  char formatted[MAX_STAMP_LEN];
  format_timestamp(epoch, formatted, sizeof(formatted));
  json_t* obj = json_object();
  if (!obj) return NULL;
  if (json_object_set_string(obj, "formatted", formatted) < 0 ||
      json_object_set_uint(obj, "epoch", epoch) < 0) {
    json_decref(obj);
    return NULL;
  }
  return obj;
}

static int json_object_set_new_take(json_t* obj, const char* key, json_t* value) {
  if (!value) return -1;
  if (json_object_set_new(obj, key, value) < 0) {
    json_decref(value);
    return -1;
  }
  return 0;
}

static int json_object_set_string(json_t* obj, const char* key, const char* value) {
  return json_object_set_new_take(obj, key, json_string(safe_string(value)));
}

static int json_object_set_uint(json_t* obj, const char* key, unsigned value) {
  return json_object_set_new_take(obj, key, json_integer(value));
}

static int json_object_set_real(json_t* obj, const char* key, double value) {
  return json_object_set_new_take(obj, key, json_real(value));
}

static int json_object_set_bool(json_t* obj, const char* key, unsigned value) {
  return json_object_set_new_take(obj, key, json_boolean(value ? 1 : 0));
}
