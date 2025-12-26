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
static json_t* json_server_info(Status* status);
static json_t* json_software_info(Status* status, const char* driver_key);
static json_t* json_config_info(Config* config, const char* driver_key);
static json_t* json_process_info(Status* status);
static json_t* json_table(Table* table);
static json_t* json_table_arena(Arena* arena, unsigned rows);
static json_t* json_table_hashes(Table* table, struct TableSlot* slot);
static json_t* json_table_hash(const char* tname, Hash* hash, const char* iname);

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
  json_t* tables_obj = NULL;
  json_t* server_obj = NULL;
  json_t* software_obj = NULL;
  json_t* config_obj = NULL;
  json_t* process_obj = NULL;
  char* dump = NULL;
  unsigned success = 0;
  const char* driver_key = config_db_driver_name(config->db.driver);

  status->json.jlen = 0;
  status->json.jbuf[0] = '\0';

  server_obj = json_server_info(status);
  software_obj = json_software_info(status, driver_key);
  config_obj = json_config_info(config, driver_key);
  process_obj = json_process_info(status);
  if (!server_obj || !software_obj || !config_obj || !process_obj) goto done;

  tables_obj = json_object();
  if (!tables_obj) goto done;
  for (unsigned t = 0; t < data->table_count; ++t) {
    Table* table = data->tables[t];
    if (!table) continue;
    json_t* table_json = json_table(table);
    if (!table_json) goto done;
    if (json_object_set_new(tables_obj, table_name(table), table_json) < 0) {
      json_decref(table_json);
      goto done;
    }
  }

  root = json_pack("{s:O,s:O,s:O,s:O,s:O}",
                   "server", server_obj,
                   "software", software_obj,
                   "config", config_obj,
                   "process", process_obj,
                   "tables", tables_obj);
  if (!root) goto done;
  server_obj = software_obj = config_obj = process_obj = NULL;
  tables_obj = NULL;

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

done:
  if (dump) free(dump);
  if (root) json_decref(root);
  if (tables_obj) json_decref(tables_obj);
  if (server_obj) json_decref(server_obj);
  if (software_obj) json_decref(software_obj);
  if (config_obj) json_decref(config_obj);
  if (process_obj) json_decref(process_obj);
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
  json_t* last_loaded = json_epoch_object(table->stats.last_loaded);
  json_t* arena = json_table_arena(slot->arena, table->stats.rows);
  json_t* hashes = json_table_hashes(table, slot);
  if (!last_loaded || !arena || !hashes) {
    if (last_loaded) json_decref(last_loaded);
    if (arena) json_decref(arena);
    if (hashes) json_decref(hashes);
    return NULL;
  }
  json_t* obj = json_pack("{s:s,s:i,s:i,s:i,s:i,s:i,s:O,s:O,s:O}",
                          "name", table_name(table),
                          "id", (int)table->table_id,
                          "period", (int)table->period,
                          "rows", (int)table->stats.rows,
                          "min_id", (int)table->stats.min_id,
                          "max_id", (int)table->stats.max_id,
                          "last_loaded", last_loaded,
                          "arena", arena,
                          "hashes", hashes);
  if (!obj) {
    json_decref(last_loaded);
    json_decref(arena);
    json_decref(hashes);
  }
  return obj;
}

static json_t* json_table_arena(Arena* arena, unsigned rows) {
  unsigned arena_cap = arena->capacity;
  unsigned arena_used = arena->used;
  unsigned arena_free = arena_cap - arena_used;
  double arena_bpr_avg = 0;
  if (rows) arena_bpr_avg = (double)arena->used / (double)rows;
  return json_pack("{s:i,s:i,s:i,s:f}",
                   "capacity_bytes", (int)arena_cap,
                   "used_bytes", (int)arena_used,
                   "free_bytes", (int)arena_free,
                   "row_avg_size_bytes", arena_bpr_avg);
}

static json_t* json_table_hashes(Table* table, struct TableSlot* slot) {
  json_t* obj = json_object();
  if (!obj) return NULL;
  for (unsigned idx = 0; idx < table->index_count; ++idx) {
    Hash* hash = slot->indexes[idx];
    if (!hash) continue;
    json_t* hash_obj = json_table_hash(table_name(table), hash, table->indexes[idx].column);
    if (!hash_obj) {
      json_decref(obj);
      return NULL;
    }
    if (json_object_set_new(obj, table->indexes[idx].column, hash_obj) < 0) {
      json_decref(hash_obj);
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

  return json_pack("{s:i,s:i,s:i,s:f,s:i,s:i,s:f,s:i,s:i,s:i}",
                   "total_slots", (int)hash->cap,
                   "used_slots", (int)hash->used,
                   "free_slots", (int)free,
                   "fill_factor_perc", fill_factor * 100,
                   "queries", (int)hash->stats.queries,
                   "probes", (int)probe_cnt,
                   "probes_per_query_avg", ppq,
                   "probes_p50", (int)stats[PERC_50].pos,
                   "probes_p95", (int)stats[PERC_95].pos,
                   "probes_p99", (int)stats[PERC_99].pos);
}

static const char* safe_string(const char* value) {
  return value ? value : "";
}

static json_t* json_server_info(Status* status) {
  return json_pack("{s:s,s:s,s:s,s:s}",
                   "host", status->server.host,
                   "system", status->server.system,
                   "machine", status->server.machine,
                   "release", status->server.release);
}

static json_t* json_software_info(Status* status, const char* driver_key) {
  json_t* libevent = json_pack("{s:s,s:s}",
                               "version", status->libevent.version,
                               "method", status->libevent.method);
  if (!libevent) return NULL;
  json_t* client = json_pack("{s:s}", "version", safe_string(status->db->client_version));
  if (!client) {
    json_decref(libevent);
    return NULL;
  }
  json_t* server = json_pack("{s:s}", "version", safe_string(status->db->server_version));
  if (!server) {
    json_decref(libevent);
    json_decref(client);
    return NULL;
  }
  json_t* driver = json_pack("{s:O,s:O}", "client", client, "server", server);
  if (!driver) {
    json_decref(libevent);
    json_decref(client);
    json_decref(server);
    return NULL;
  }
  json_t* software = json_pack("{s:O,s:O}", "libevent", libevent, driver_key, driver);
  if (!software) {
    json_decref(libevent);
    json_decref(driver);
  }
  return software;
}

static json_t* json_config_info(Config* config, const char* driver_key) {
  json_t* driver_cfg = NULL;
  if (config->db.driver == CONFIG_DB_DRIVER_SQLITE) {
    driver_cfg = json_pack("{s:s}", "filename", safe_string(config->db.sqlite_filename));
  } else {
    driver_cfg = json_pack("{s:s,s:i,s:s,s:s}",
                           "host", config->db.host,
                           "port", (int)config->db.port,
                           "database", config->db.database,
                           "user", config->db.user);
  }
  if (!driver_cfg) return NULL;

  json_t* sockets_cfg = json_array();
  ConfigSocket** sockets = config->listeners.sockets;
  if (sockets) {
    size_t i = 0;
    for (ConfigSocket* socket = sockets[i++]; socket; socket = sockets[i++]) {
      json_t* socket_cfg = json_pack("{s:s,s:i,s:s}",
                                     "host", socket->host,
                                     "port", (int) socket->port,
                                     "path", socket->path);
      json_array_append_new(sockets_cfg, socket_cfg);
    }
  } 
  
  if (!sockets_cfg) {
    json_decref(driver_cfg);
    return NULL;
  }

  json_t* table_cfg = json_pack("{s:i,s:s,s:b}",
                                "period", (int)config->table.period,
                                "schema", safe_string(config->table.schema),
                                "strip_null", config->table.strip_null ? 1 : 0);
  if (!table_cfg) {
    json_decref(driver_cfg);
    json_decref(sockets_cfg);
    return NULL;
  }

  json_t* server_cfg = json_pack("{s:b}", "show_msgs", config->server.show_msgs ? 1 : 0);
  if (!server_cfg) {
    json_decref(driver_cfg);
    json_decref(sockets_cfg);
    json_decref(table_cfg);
    return NULL;
  }

  json_t* config_obj = json_pack("{s:O,s:O,s:O,s:O}",
                                 driver_key, driver_cfg,
                                 "sockets", sockets_cfg,
                                 "table", table_cfg,
                                 "server", server_cfg);
  if (!config_obj) {
    json_decref(driver_cfg);
    json_decref(sockets_cfg);
    json_decref(table_cfg);
    json_decref(server_cfg);
  }
  return config_obj;
}

static json_t* json_process_info(Status* status) {
  json_t* birth = json_epoch_object(status->process.birth);
  if (!birth) return NULL;
  json_t* process = json_pack("{s:i,s:O}",
                              "uptime", (int)get_uptime(status),
                              "birth", birth);
  if (!process) {
    json_decref(birth);
  }
  return process;
}

static json_t* json_epoch_object(unsigned epoch) {
  char formatted[MAX_STAMP_LEN];
  format_timestamp(epoch, formatted, sizeof(formatted));
  return json_pack("{s:s,s:i}", "formatted", formatted, "epoch", (int)epoch);
}
