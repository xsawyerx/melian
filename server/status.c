#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <event2/event.h>
#include "util.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "config.h"
#include "data.h"
#include "db.h"
#include "json.h"
#include "status.h"

static unsigned get_uptime(Status* status);
static unsigned json_table(unsigned pos, Table* table, char* p, unsigned c, unsigned l);
static unsigned json_table_arena(unsigned pos, const char* tname, Arena* arena, unsigned rows, char* p, unsigned c, unsigned l);
static unsigned json_table_hashes(unsigned pos, Table* table, struct TableSlot* slot, char* p, unsigned c, unsigned l);
static unsigned json_table_hash(unsigned pos, const char* tname, Hash* hash, const char* iname, char* p, unsigned c, unsigned l);

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
  char* p = status->json.jbuf;
  unsigned c = sizeof(status->json.jbuf);
  unsigned l = 0;
  const char* driver_key = config_db_driver_name(config->db.driver);
  do {
    unsigned N = 0;
    l = json_obj_beg(N++, 0, p, c, l);
    {
      unsigned N = 0;
      l = json_obj_beg(N++, "server", p, c, l);
      {
        unsigned N = 0;
        l = json_string(N++, "host", status->server.host, p, c, l);
        l = json_string(N++, "system", status->server.system, p, c, l);
        l = json_string(N++, "machine", status->server.machine, p, c, l);
        l = json_string(N++, "release", status->server.release, p, c, l);
      }
      l = json_obj_end(p, c, l);

      l = json_obj_beg(N++, "software", p, c, l);
      {
        unsigned N = 0;
        l = json_obj_beg(N++, "libevent", p, c, l);
        {
          unsigned N = 0;
          l = json_string(N++, "version", status->libevent.version, p, c, l);
          l = json_string(N++, "method", status->libevent.method, p, c, l);
        }
        l = json_obj_end(p, c, l);

        l = json_obj_beg(N++, driver_key, p, c, l);
        {
          unsigned N = 0;
          l = json_obj_beg(N++, "client", p, c, l);
          {
            unsigned N = 0;
            l = json_string(N++, "version", status->db->client_version, p, c, l);
          }
          l = json_obj_end(p, c, l);

          l = json_obj_beg(N++, "server", p, c, l);
          {
            unsigned N = 0;
            l = json_string(N++, "version", status->db->server_version, p, c, l);
          }
          l = json_obj_end(p, c, l);
        }
        l = json_obj_end(p, c, l);
      }
      l = json_obj_end(p, c, l);

      l = json_obj_beg(N++, "config", p, c, l);
      {
        unsigned N = 0;

        l = json_obj_beg(N++, driver_key, p, c, l);
        {
          unsigned N = 0;
          if (config->db.driver == CONFIG_DB_DRIVER_MYSQL ||
              config->db.driver == CONFIG_DB_DRIVER_POSTGRESQL) {
            l = json_string(N++, "host", config->db.host, p, c, l);
            l = json_integer(N++, "port", config->db.port, p, c, l);
            l = json_string(N++, "database", config->db.database, p, c, l);
            l = json_string(N++, "user", config->db.user, p, c, l);
          } else if (config->db.driver == CONFIG_DB_DRIVER_SQLITE) {
            l = json_string(N++, "filename",
                            config->db.sqlite_filename ? config->db.sqlite_filename : "",
                            p, c, l);
          }
        }
        l = json_obj_end(p, c, l);

        l = json_obj_beg(N++, "socket", p, c, l);
        {
          unsigned N = 0;
          l = json_string(N++, "host", config->socket.host, p, c, l);
          l = json_integer(N++, "port", config->socket.port, p, c, l);
          l = json_string(N++, "path", config->socket.path, p, c, l);
        }
        l = json_obj_end(p, c, l);

        l = json_obj_beg(N++, "table", p, c, l);
        {
          unsigned N = 0;
          l = json_integer(N++, "period", config->table.period, p, c, l);
          l = json_string(N++, "schema", config->table.schema ? config->table.schema : "", p, c, l);
          l = json_bool(N++, "strip_null", config->table.strip_null, p, c, l);
        }
        l = json_obj_end(p, c, l);

        l = json_obj_beg(N++, "server", p, c, l);
        {
          unsigned N = 0;
          l = json_bool(N++, "show_msgs", config->server.show_msgs, p, c, l);
        }
        l = json_obj_end(p, c, l);
      }
      l = json_obj_end(p, c, l);

      l = json_obj_beg(N++, "process", p, c, l);
      {
        unsigned N = 0;
        l = json_integer(N++, "uptime", get_uptime(status), p, c, l);
        l = json_epoch(N++, "birth", status->process.birth, p, c, l);
      }
      l = json_obj_end(p, c, l);

      l = json_obj_beg(N++, "tables", p, c, l);
      {
        unsigned N = 0;
        for (unsigned t = 0; t < data->table_count; ++t) {
          Table* table = data->tables[t];
          if (!table) continue;
          l = json_table(N++, table, p, c, l);
        }
      }
      l = json_obj_end(p, c, l);
    }
    l = json_obj_end(p, c, l);
  } while (0);
  status->json.jlen = l;
  LOG_DEBUG("JSON %u %u [%.*s]", c, l, l, status->json.jbuf);
}

static unsigned get_uptime(Status* status) {
  unsigned now = time(0);
  unsigned uptime = now - status->process.birth;
  return uptime;
}

static unsigned json_table(unsigned pos, Table* table, char* p, unsigned c, unsigned l) {
  do {
    const char* tname = table_name(table);
    struct TableSlot* slot = &table->slots[table->current_slot];
    l = json_obj_beg(pos, tname, p, c, l);
    {
      unsigned N = 0;

      l = json_integer(N++, "id", table->table_id, p, c, l);
      l = json_integer(N++, "period", table->period, p, c, l);
      l = json_integer(N++, "rows", table->stats.rows, p, c, l);
      l = json_integer(N++, "min_id", table->stats.min_id, p, c, l);
      l = json_integer(N++, "max_id", table->stats.max_id, p, c, l);
      l = json_epoch(N++, "last_loaded", table->stats.last_loaded, p, c, l);

      l = json_table_arena(N++, tname, slot->arena, table->stats.rows, p, c, l);
      l = json_table_hashes(N++, table, slot, p, c, l);
    }
    l = json_obj_end(p, c, l);
  } while (0);
  return l;
}

static unsigned json_table_arena(unsigned pos, const char* tname, Arena* arena, unsigned rows, char* p, unsigned c, unsigned l) {
  UNUSED(tname);
  do {
    unsigned arena_cap = arena->capacity;
    unsigned arena_used = arena->used;
    unsigned arena_free = arena_cap - arena_used;
    double arena_bpr_avg = 0;
    if (rows) arena_bpr_avg = (double) arena->used / (double) rows;

    l = json_obj_beg(pos, "arena", p, c, l);
    {
      unsigned N = 0;
      l = json_integer(N++, "capacity_bytes", arena_cap, p, c, l);
      l = json_integer(N++, "used_bytes", arena_used, p, c, l);
      l = json_integer(N++, "free_bytes", arena_free, p, c, l);
      l = json_real(N++, "row_avg_size_bytes", arena_bpr_avg, 1, p, c, l);
    }
    l = json_obj_end(p, c, l);
  } while (0);
  return l;
}

static unsigned json_table_hashes(unsigned pos, Table* table, struct TableSlot* slot, char* p, unsigned c, unsigned l) {
  do {
    l = json_obj_beg(pos, "hashes", p, c, l);
    {
      unsigned N = 0;
      for (unsigned idx = 0; idx < table->index_count; ++idx) {
        Hash* hash = slot->indexes[idx];
        if (!hash) continue;
        l = json_table_hash(N++, table_name(table), hash, table->indexes[idx].column, p, c, l);
      }
    }
    l = json_obj_end(p, c, l);
  } while (0);
  return l;
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

static unsigned json_table_hash(unsigned pos, const char* tname, Hash* hash, const char* iname, char* p, unsigned c, unsigned l) {
  do {
    unsigned free = hash->cap - hash->used;
    double fill_factor = (double) hash->used / (double) hash->cap;
    unsigned probe_cnt = 0;
    unsigned probe_min = -1;
    unsigned probe_max = 0;
    for (unsigned h = 0; h < MAX_PROBE_COUNT; ++h) {
      unsigned val = h * hash->stats.probes[h];
      if (!val) continue;
      if (probe_min > h) probe_min = h;
      if (probe_max < h) probe_max = h;
      probe_cnt += val;
    }
    static unsigned levels[PERC_LAST] = {
      50,
      95,
      99,
    };
    static const char* symbols[PERC_LAST] = {
      "M",
      "5",
      "9",
    };
    struct Percentile stats[PERC_LAST] = {0};
    for (unsigned s = 0; s < PERC_LAST; ++s) {
      stats[s].needed = levels[s] * probe_cnt / 100;
    }
    double ppq = 0;
    if (probe_cnt) {
      ppq = (double) probe_cnt / (double) hash->stats.queries;
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

    l = json_obj_beg(pos, iname, p, c, l);
    {
      unsigned N = 0;
      l = json_integer(N++, "total_slots", hash->cap, p, c, l);
      l = json_integer(N++, "used_slots", hash->used, p, c, l);
      l = json_integer(N++, "free_slots", free, p, c, l);
      l = json_real(N++, "fill_factor_perc", fill_factor * 100, 1, p, c, l);
      l = json_integer(N++, "queries", hash->stats.queries, p, c, l);
      l = json_integer(N++, "probes", probe_cnt, p, c, l);
      l = json_real(N++, "probes_per_query_avg", ppq, 2, p, c, l);
      l = json_integer(N++, "probes_p50", stats[PERC_50].pos, p, c, l);
      l = json_integer(N++, "probes_p95", stats[PERC_95].pos, p, c, l);
      l = json_integer(N++, "probes_p99", stats[PERC_99].pos, p, c, l);
    }
    l = json_obj_end(p, c, l);
  } while (0);
  return l;
}
