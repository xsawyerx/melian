#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_MYSQL
#include <mysql/mysql.h>
#endif
#ifdef HAVE_SQLITE3
#include <sqlite3.h>
#endif
#ifdef HAVE_POSTGRESQL
#include <libpq-fe.h>
#endif
#include "util.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "config.h"
#include "db.h"
#include "data.h"

// TODO: make these limits dynamic? Arena?
enum {
  MAX_FIELDS = 99,
  MAX_FIELD_NAME_LEN = 100,
  MAX_JSON_LEN = 10240,
  MAX_SQL_LEN = 1024,
};

#ifdef HAVE_MYSQL
static void mysql_refresh_versions(DB* db);
static void db_mysql_connect(DB* db);
static void db_mysql_disconnect(DB* db);
static unsigned db_mysql_get_table_size(DB* db, const char* table);
static unsigned db_mysql_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                            unsigned* min_id, unsigned* max_id);
#endif

#ifdef HAVE_SQLITE3
static void sqlite_refresh_versions(DB* db);
static void db_sqlite_connect(DB* db);
static void db_sqlite_disconnect(DB* db);
static unsigned db_sqlite_get_table_size(DB* db, const char* table);
static unsigned db_sqlite_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                            unsigned* min_id, unsigned* max_id);
#endif

#ifdef HAVE_POSTGRESQL
static void postgres_refresh_versions(DB* db);
static void db_postgresql_connect(DB* db);
static void db_postgresql_disconnect(DB* db);
static unsigned db_postgresql_get_table_size(DB* db, const char* table);
static unsigned db_postgresql_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                            unsigned* min_id, unsigned* max_id);
#endif

#if !defined(HAVE_MYSQL) || !defined(HAVE_SQLITE3) || !defined(HAVE_POSTGRESQL)
static void driver_not_supported(ConfigDbDriver driver);
#endif

DB* db_build(Config* config) {
  DB* db = 0;
  do {
    db = calloc(1, sizeof(DB));
    if (!db) {
      LOG_WARN("Could not allocate DB object");
      break;
    }
    db->config = config;
    db->client_version[0] = '\0';
    db->server_version[0] = '\0';

    switch (config->db.driver) {
      case CONFIG_DB_DRIVER_MYSQL:
#ifdef HAVE_MYSQL
        if (mysql_library_init(0, 0, 0) != 0) {
          LOG_WARN("mysql_library_init failed");
        } else {
          db->mysql_initialized = 1;
        }
        mysql_refresh_versions(db);
        break;
#else
        driver_not_supported(config->db.driver);
        break;
#endif
      case CONFIG_DB_DRIVER_SQLITE:
#ifdef HAVE_SQLITE3
        sqlite_refresh_versions(db);
        break;
#else
        driver_not_supported(config->db.driver);
        break;
#endif
      case CONFIG_DB_DRIVER_POSTGRESQL:
#ifdef HAVE_POSTGRESQL
        postgres_refresh_versions(db);
        break;
#else
        driver_not_supported(config->db.driver);
        break;
#endif
      default:
        LOG_FATAL("Unknown database driver id %d", config->db.driver);
        break;
    }
  } while (0);
  return db;
}

void db_destroy(DB* db) {
  if (!db) return;
  db_disconnect(db);
#ifdef HAVE_MYSQL
  if (db->mysql_initialized) {
    mysql_library_end();
    db->mysql_initialized = 0;
  }
#endif
  free(db);
}

void db_connect(DB* db) {
  if (!db) return;
  switch (db->config->db.driver) {
    case CONFIG_DB_DRIVER_MYSQL:
#ifdef HAVE_MYSQL
      db_mysql_connect(db);
      break;
#else
      driver_not_supported(db->config->db.driver);
      break;
#endif
    case CONFIG_DB_DRIVER_SQLITE:
#ifdef HAVE_SQLITE3
      db_sqlite_connect(db);
      break;
#else
      driver_not_supported(db->config->db.driver);
      break;
#endif
    case CONFIG_DB_DRIVER_POSTGRESQL:
#ifdef HAVE_POSTGRESQL
      db_postgresql_connect(db);
      break;
#else
      driver_not_supported(db->config->db.driver);
      break;
#endif
    default:
      LOG_FATAL("Unknown database driver id %d", db->config->db.driver);
      break;
  }
}

void db_disconnect(DB* db) {
  if (!db) return;
  switch (db->config->db.driver) {
    case CONFIG_DB_DRIVER_MYSQL:
#ifdef HAVE_MYSQL
      db_mysql_disconnect(db);
      break;
#else
      break;
#endif
    case CONFIG_DB_DRIVER_SQLITE:
#ifdef HAVE_SQLITE3
      db_sqlite_disconnect(db);
      break;
#else
      break;
#endif
    case CONFIG_DB_DRIVER_POSTGRESQL:
#ifdef HAVE_POSTGRESQL
      db_postgresql_disconnect(db);
      break;
#else
      break;
#endif
    default:
      break;
  }
}

unsigned db_get_table_size(DB* db, const char* table) {
  if (!db) return 0;
  switch (db->config->db.driver) {
    case CONFIG_DB_DRIVER_MYSQL:
#ifdef HAVE_MYSQL
      return db_mysql_get_table_size(db, table);
#else
      driver_not_supported(db->config->db.driver);
      return 0;
#endif
    case CONFIG_DB_DRIVER_SQLITE:
#ifdef HAVE_SQLITE3
      return db_sqlite_get_table_size(db, table);
#else
      driver_not_supported(db->config->db.driver);
      return 0;
#endif
    case CONFIG_DB_DRIVER_POSTGRESQL:
#ifdef HAVE_POSTGRESQL
      return db_postgresql_get_table_size(db, table);
#else
      driver_not_supported(db->config->db.driver);
      return 0;
#endif
    default:
      return 0;
  }
}

unsigned db_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                            unsigned* min_id, unsigned* max_id) {
  if (!db) return 0;
  switch (db->config->db.driver) {
    case CONFIG_DB_DRIVER_MYSQL:
#ifdef HAVE_MYSQL
      return db_mysql_query_into_hash(db, table, slot, min_id, max_id);
#else
      driver_not_supported(db->config->db.driver);
      return 0;
#endif
    case CONFIG_DB_DRIVER_SQLITE:
#ifdef HAVE_SQLITE3
      return db_sqlite_query_into_hash(db, table, slot, min_id, max_id);
#else
      driver_not_supported(db->config->db.driver);
      return 0;
#endif
    case CONFIG_DB_DRIVER_POSTGRESQL:
#ifdef HAVE_POSTGRESQL
      return db_postgresql_query_into_hash(db, table, slot, min_id, max_id);
#else
      driver_not_supported(db->config->db.driver);
      return 0;
#endif
    default:
      return 0;
  }
}

#if !defined(HAVE_MYSQL) || !defined(HAVE_SQLITE3) || !defined(HAVE_POSTGRESQL)
static void driver_not_supported(ConfigDbDriver driver) {
  LOG_FATAL("Database driver %s requested but not available in this build",
            config_db_driver_name(driver));
}
#endif

#ifdef HAVE_MYSQL

static void mysql_refresh_versions(DB* db) {
  db->client_version[0] = '\0';
  const char* c = mysql_get_client_info();
  if (c) snprintf(db->client_version, sizeof(db->client_version), "%s", c);

  db->server_version[0] = '\0';
  if (db->mysql) {
    const char* s = mysql_get_server_info((MYSQL*) db->mysql);
    if (s) snprintf(db->server_version, sizeof(db->server_version), "%s", s);
  }
}

static void db_mysql_connect(DB* db) {
  MYSQL* handle = (MYSQL*) mysql_init(NULL);
  if (!handle) {
    LOG_WARN("Could not initialize MySQL client");
    return;
  }

  ConfigDb* cfg = &db->config->db;
  MYSQL* conn = mysql_real_connect(handle, cfg->host, cfg->user, cfg->password,
                                   cfg->database, cfg->port, NULL, 0);
  if (!conn) {
    LOG_WARN("Could not connect to MySQL server at %s:%u as user %s",
             cfg->host, cfg->port, cfg->user);
    mysql_close(handle);
    return;
  }

  db->mysql = handle;
  mysql_refresh_versions(db);
  LOG_INFO("Connected to MySQL server version [%s] at %s:%u as user %s",
           db->server_version, cfg->host, cfg->port, cfg->user);
}

static void db_mysql_disconnect(DB* db) {
  if (!db->mysql) return;
  ConfigDb* cfg = &db->config->db;
  mysql_close((MYSQL*) db->mysql);
  db->mysql = 0;
  LOG_INFO("Disconnected from MySQL server at %s:%u", cfg->host, cfg->port);
}

static unsigned db_mysql_get_table_size(DB* db, const char* table) {
  unsigned rows = 0;
  unsigned count = 0;
  MYSQL_RES *result = 0;
  do {
    if (!db->mysql) {
      LOG_WARN("Cannot get table size for %s, invalid MySQL connection", table);
      break;
    }

    LOG_DEBUG("Counting rows from table %s", table);
    char sql[MAX_SQL_LEN];
    snprintf(sql, MAX_SQL_LEN, "SELECT COUNT(*) FROM %s", table);
    if (mysql_query((MYSQL*) db->mysql, sql)) {
      LOG_WARN("Cannot run query [%s] for table %s", sql, table);
      break;
    }

    result = mysql_store_result((MYSQL*) db->mysql);
    if (!result) {
      LOG_WARN("Cannot store MySQL result for COUNT query for table %s", table);
      break;
    }

    unsigned num_fields = mysql_num_fields(result);
    if (num_fields != 1) {
      LOG_WARN("Expected %u number of fields for COUNT query for table %s, got %u",
               1, table, num_fields);
      break;
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
      for (unsigned col = 0; col < num_fields; col++) {
        count = atoi(row[col]);
      }
      ++rows;
    }
    if (rows != 1) {
      LOG_WARN("Expected %u rows for COUNT query for table %s, got %u",
               1, table, rows);
      break;
    }
  } while (0);

  if (result) mysql_free_result(result);
  LOG_DEBUG("Counted %u rows from table %s", count, table);
  return count;
}

static unsigned db_mysql_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                                         unsigned* min_id, unsigned* max_id) {
  unsigned rows = 0;
  MYSQL_RES *result = 0;
  do {
    if (!db->mysql) {
      LOG_WARN("Cannot query table data for %s, invalid MySQL connection", table_name(table));
      break;
    }

    double t0 = now_sec();
    LOG_DEBUG("Fetching from table %s", table_name(table));
    char sql[MAX_SQL_LEN];
    snprintf(sql, MAX_SQL_LEN, "SELECT * FROM %s", table_name(table));
    if (mysql_query((MYSQL*) db->mysql, sql)) {
      LOG_WARN("Cannot run query [%s] for table %s", sql, table_name(table));
      break;
    }

    result = mysql_store_result((MYSQL*) db->mysql);
    if (!result) {
      LOG_WARN("Cannot store MySQL result for SELECT query for table %s", table_name(table));
      break;
    }

    unsigned num_fields = mysql_num_fields(result);
    if (num_fields > MAX_FIELDS) {
      LOG_WARN("Expected at most %u number of fields for SELECT query for table %s, got %u",
               MAX_FIELDS, table_name(table), num_fields);
      break;
    }

    char names[MAX_FIELDS][MAX_FIELD_NAME_LEN];
    enum enum_field_types types[MAX_FIELDS];
    int index_pos[MELIAN_MAX_INDEXES];
    for (unsigned idx = 0; idx < MELIAN_MAX_INDEXES; ++idx) index_pos[idx] = -1;
    unsigned bad = 0;
    for (unsigned col = 0; col < num_fields; ++col) {
      MYSQL_FIELD *field = mysql_fetch_field(result);
      if (!field) {
        LOG_WARN("Could not fetch field %u for SELECT query for table %s",
                 col, table_name(table));
        ++bad;
        break;
      }

      types[col] = field->type;
      LOG_DEBUG("Column %u type %u", col, (unsigned) field->type);
      snprintf(names[col], MAX_FIELD_NAME_LEN, "%s", field->name);
      for (unsigned idx = 0; idx < table->index_count; ++idx) {
        if (strcmp(field->name, table->indexes[idx].column) == 0) {
          index_pos[idx] = col;
        }
      }
    }
    if (bad) break;

    *min_id = (unsigned) -1;
    *max_id = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
      char jbuf[MAX_JSON_LEN];
      unsigned jpos = 0;
      jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "{");
      unsigned cols = 0;
      for (unsigned col = 0; col < num_fields; col++) {
        unsigned col_is_null = !row[col] || types[col] == MYSQL_TYPE_NULL;
        if (db->config->table.strip_null && col_is_null) continue;

        if (cols > 0) jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, ",");
        jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "\"%s\":", names[col]);
        if (col_is_null) {
          jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "null");
        } else {
          switch (types[col]) {
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_TIME2:
            case MYSQL_TYPE_JSON:
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_GEOMETRY:
              jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "\"%s\"", row[col]);
              break;
            default:
              jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "%s", row[col]);
              break;
          }
        }
        ++cols;
      }
      jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "}");
      ++rows;
      LOG_DEBUG("Fetched row %u: %p %u [%.*s]", rows, row, jpos, jpos, jbuf);

      unsigned frame = arena_store_framed(slot->arena, (uint8_t*)jbuf, jpos);
      LOG_DEBUG("Stored frame %p", frame);
      if (frame == (unsigned)-1) {
        LOG_WARN("Could not store framed JSON for SELECT query for table %s", table_name(table));
        break;
      }

      int insert_error = 0;
      for (unsigned idx = 0; idx < table->index_count; ++idx) {
        int col_pos = index_pos[idx];
        if (col_pos < 0) continue;
        if (!slot->indexes[idx]) continue;
        const char* value = row[col_pos];
        if (!value) continue;
        if (table->indexes[idx].type == CONFIG_INDEX_TYPE_INT) {
          unsigned key_int = (unsigned) atoi(value);
          if (!hash_insert(slot->indexes[idx], &key_int, sizeof(unsigned),
                           frame, jpos + sizeof(unsigned))) {
            LOG_WARN("Could not insert row for table %s key %u index %u",
                     table_name(table), key_int, idx);
            insert_error = 1;
            break;
          }
          if (idx == 0) {
            if (*min_id > key_int) *min_id = key_int;
            if (*max_id < key_int) *max_id = key_int;
          }
        } else {
          unsigned hlen = strlen(value);
          if (!hlen) continue;
          if (!hash_insert(slot->indexes[idx], value, hlen, frame, jpos + sizeof(unsigned))) {
            LOG_WARN("Could not insert row for table %s key %.*s index %u",
                     table_name(table), hlen, value, idx);
            insert_error = 1;
            break;
          }
        }
      }
      if (insert_error) break;
    }
    double t1 = now_sec();
    unsigned long elapsed = (t1 - t0) * 1000000;
    LOG_INFO("Fetched %u rows from table %s in %lu us", rows, table_name(table), elapsed);
  } while (0);

  if (result) mysql_free_result(result);
  return rows;
}

#endif  // HAVE_MYSQL

#ifdef HAVE_SQLITE3

static void sqlite_refresh_versions(DB* db) {
  db->client_version[0] = '\0';
  snprintf(db->client_version, sizeof(db->client_version), "%s", sqlite3_libversion());

  db->server_version[0] = '\0';
  if (!db->sqlite) return;

  sqlite3_stmt* stmt = NULL;
  if (sqlite3_prepare_v2(db->sqlite, "SELECT sqlite_version()", -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      const unsigned char* text = sqlite3_column_text(stmt, 0);
      if (text) snprintf(db->server_version, sizeof(db->server_version), "%s", text);
    }
  }
  if (stmt) sqlite3_finalize(stmt);
}

static void db_sqlite_connect(DB* db) {
  ConfigDb* cfg = &db->config->db;
  if (!cfg->sqlite_filename || !cfg->sqlite_filename[0]) {
    LOG_WARN("MELIAN_SQLITE_FILENAME must be set when using the sqlite driver");
    return;
  }
  sqlite3* handle = NULL;
  int rc = sqlite3_open_v2(cfg->sqlite_filename, &handle, SQLITE_OPEN_READONLY, NULL);
  if (rc != SQLITE_OK) {
    LOG_WARN("Could not open SQLite database %s: %s",
             cfg->sqlite_filename,
             handle ? sqlite3_errmsg(handle) : "unable to allocate sqlite handle");
    if (handle) sqlite3_close(handle);
    return;
  }
  db->sqlite = handle;
  sqlite_refresh_versions(db);
  LOG_INFO("Opened SQLite database %s (SQLite %s)", cfg->sqlite_filename,
           db->client_version[0] ? db->client_version : sqlite3_libversion());
}

static void db_sqlite_disconnect(DB* db) {
  if (!db->sqlite) return;
  const char* filename = db->config->db.sqlite_filename ? db->config->db.sqlite_filename : "<unknown>";
  sqlite3_close(db->sqlite);
  LOG_INFO("Closed SQLite database %s", filename);
  db->sqlite = NULL;
}

static unsigned db_sqlite_get_table_size(DB* db, const char* table) {
  unsigned count = 0;
  sqlite3_stmt* stmt = NULL;
  do {
    if (!db->sqlite) {
      LOG_WARN("Cannot get table size for %s, SQLite database not open", table);
      break;
    }
    char sql[MAX_SQL_LEN];
    snprintf(sql, MAX_SQL_LEN, "SELECT COUNT(*) FROM %s", table);
    if (sqlite3_prepare_v2(db->sqlite, sql, -1, &stmt, NULL) != SQLITE_OK) {
      LOG_WARN("Cannot run query [%s] for table %s: %s", sql, table, sqlite3_errmsg(db->sqlite));
      break;
    }
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      count = (unsigned) sqlite3_column_int64(stmt, 0);
    } else {
      LOG_WARN("COUNT query for table %s returned no rows", table);
    }
  } while (0);
  if (stmt) sqlite3_finalize(stmt);
  return count;
}

static unsigned db_sqlite_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                                          unsigned* min_id, unsigned* max_id) {
  unsigned rows = 0;
  sqlite3_stmt* stmt = NULL;
  do {
    if (!db->sqlite) {
      LOG_WARN("Cannot query table data for %s, SQLite database not open", table_name(table));
      break;
    }

    double t0 = now_sec();
    char sql[MAX_SQL_LEN];
    snprintf(sql, MAX_SQL_LEN, "SELECT * FROM %s", table_name(table));
    if (sqlite3_prepare_v2(db->sqlite, sql, -1, &stmt, NULL) != SQLITE_OK) {
      LOG_WARN("Cannot run query [%s] for table %s: %s", sql, table_name(table), sqlite3_errmsg(db->sqlite));
      break;
    }

    int num_fields = sqlite3_column_count(stmt);
    if (num_fields > MAX_FIELDS) {
      LOG_WARN("Expected at most %u number of fields for SELECT query for table %s, got %d",
               MAX_FIELDS, table_name(table), num_fields);
      break;
    }

    char names[MAX_FIELDS][MAX_FIELD_NAME_LEN];
    int index_pos[MELIAN_MAX_INDEXES];
    for (unsigned idx = 0; idx < MELIAN_MAX_INDEXES; ++idx) index_pos[idx] = -1;
    for (int col = 0; col < num_fields; ++col) {
      const char* name = sqlite3_column_name(stmt, col);
      snprintf(names[col], MAX_FIELD_NAME_LEN, "%s", name ? name : "");
      for (unsigned idx = 0; idx < table->index_count; ++idx) {
        if (strcmp(names[col], table->indexes[idx].column) == 0) {
          index_pos[idx] = col;
        }
      }
    }

    *min_id = (unsigned)-1;
    *max_id = 0;
    int rc = SQLITE_OK;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      char jbuf[MAX_JSON_LEN];
      unsigned jpos = 0;
      jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "{");
      unsigned cols = 0;
      for (int col = 0; col < num_fields; ++col) {
        int col_type = sqlite3_column_type(stmt, col);
        int col_is_null = (col_type == SQLITE_NULL);
        if (db->config->table.strip_null && col_is_null) continue;

        if (cols > 0) jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, ",");
        jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "\"%s\":", names[col]);
        if (col_is_null) {
          jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "null");
        } else if (col_type == SQLITE_INTEGER || col_type == SQLITE_FLOAT) {
          const unsigned char* text = sqlite3_column_text(stmt, col);
          jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "%s", text ? (const char*)text : "0");
        } else {
          const unsigned char* text = sqlite3_column_text(stmt, col);
          jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "\"%s\"", text ? (const char*)text : "");
        }
        ++cols;
      }
      jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "}");
      ++rows;

      unsigned frame = arena_store_framed(slot->arena, (uint8_t*)jbuf, jpos);
      if (frame == (unsigned)-1) {
        LOG_WARN("Could not store framed JSON for SELECT query for table %s", table_name(table));
        break;
      }

      int insert_error = 0;
      for (unsigned idx = 0; idx < table->index_count; ++idx) {
        int col_pos = index_pos[idx];
        if (col_pos < 0) continue;
        if (!slot->indexes[idx]) continue;
        if (table->indexes[idx].type == CONFIG_INDEX_TYPE_INT) {
          unsigned key_int = (unsigned) sqlite3_column_int64(stmt, col_pos);
          if (!hash_insert(slot->indexes[idx], &key_int, sizeof(unsigned), frame, jpos + sizeof(unsigned))) {
            LOG_WARN("Could not insert row for table %s key %u index %u",
                     table_name(table), key_int, idx);
            insert_error = 1;
            break;
          }
          if (idx == 0) {
            if (*min_id > key_int) *min_id = key_int;
            if (*max_id < key_int) *max_id = key_int;
          }
        } else {
          const unsigned char* value = sqlite3_column_text(stmt, col_pos);
          unsigned hlen = (unsigned) sqlite3_column_bytes(stmt, col_pos);
          if (!value || !hlen) continue;
          if (!hash_insert(slot->indexes[idx], value, hlen, frame, jpos + sizeof(unsigned))) {
            LOG_WARN("Could not insert row for table %s key %.*s index %u",
                     table_name(table), hlen, value, idx);
            insert_error = 1;
            break;
          }
        }
      }
      if (insert_error) break;
    }
    if (rc != SQLITE_DONE) {
      LOG_WARN("Error fetching rows from table %s: %s", table_name(table), sqlite3_errmsg(db->sqlite));
    }
    double t1 = now_sec();
    unsigned long elapsed = (t1 - t0) * 1000000;
    LOG_INFO("Fetched %u rows from table %s in %lu us", rows, table_name(table), elapsed);
  } while (0);

  if (stmt) sqlite3_finalize(stmt);
  return rows;
}

#endif  // HAVE_SQLITE3

#ifdef HAVE_POSTGRESQL

static void postgres_refresh_versions(DB* db) {
  db->client_version[0] = '\0';
  int lv = PQlibVersion();
  if (lv > 0) {
    int major = lv / 10000;
    int minor = (lv / 100) % 100;
    int patch = lv % 100;
    snprintf(db->client_version, sizeof(db->client_version), "%d.%d.%d", major, minor, patch);
  }

  db->server_version[0] = '\0';
  if (!db->postgres) return;
  const char* ver = PQparameterStatus(db->postgres, "server_version");
  if (ver && ver[0]) {
    snprintf(db->server_version, sizeof(db->server_version), "%s", ver);
    return;
  }
  int sv = PQserverVersion(db->postgres);
  if (sv > 0) {
    int major = sv / 10000;
    int minor = (sv / 100) % 100;
    int patch = sv % 100;
    snprintf(db->server_version, sizeof(db->server_version), "%d.%d.%d", major, minor, patch);
  }
}

static void db_postgresql_connect(DB* db) {
  ConfigDb* cfg = &db->config->db;
  char portbuf[16];
  const char* portstr = NULL;
  if (cfg->port > 0) {
    snprintf(portbuf, sizeof(portbuf), "%u", cfg->port);
    portstr = portbuf;
  }
  PGconn* conn = PQsetdbLogin(
      cfg->host && cfg->host[0] ? cfg->host : NULL,
      portstr,
      NULL, NULL,
      cfg->database && cfg->database[0] ? cfg->database : NULL,
      cfg->user && cfg->user[0] ? cfg->user : NULL,
      cfg->password && cfg->password[0] ? cfg->password : NULL);
  if (!conn) {
    LOG_WARN("Could not allocate PostgreSQL client");
    return;
  }
  if (PQstatus(conn) != CONNECTION_OK) {
    LOG_WARN("Could not connect to PostgreSQL server: %s", PQerrorMessage(conn));
    PQfinish(conn);
    return;
  }
  db->postgres = conn;
  postgres_refresh_versions(db);
  LOG_INFO("Connected to PostgreSQL server version [%s] at %s:%u as user %s",
           db->server_version[0] ? db->server_version : "unknown",
           cfg->host ? cfg->host : "localhost",
           cfg->port,
           cfg->user ? cfg->user : "");
}

static void db_postgresql_disconnect(DB* db) {
  if (!db->postgres) return;
  const char* host = db->config->db.host ? db->config->db.host : "localhost";
  unsigned port = db->config->db.port;
  PQfinish(db->postgres);
  db->postgres = NULL;
  LOG_INFO("Disconnected from PostgreSQL server at %s:%u", host, port);
}

static unsigned db_postgresql_get_table_size(DB* db, const char* table) {
  unsigned count = 0;
  if (!db->postgres) {
    LOG_WARN("Cannot get table size for %s, PostgreSQL connection not established", table);
    return 0;
  }
  char sql[MAX_SQL_LEN];
  snprintf(sql, MAX_SQL_LEN, "SELECT COUNT(*) FROM %s", table);
  PGresult* res = PQexec(db->postgres, sql);
  if (!res) {
    LOG_WARN("Cannot run query [%s] for table %s: %s", sql, table, PQerrorMessage(db->postgres));
    return 0;
  }
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    LOG_WARN("Cannot run query [%s] for table %s: %s", sql, table, PQerrorMessage(db->postgres));
    PQclear(res);
    return 0;
  }
  if (PQntuples(res) >= 1) {
    const char* value = PQgetvalue(res, 0, 0);
    count = (unsigned) strtoul(value ? value : "0", 0, 10);
  }
  PQclear(res);
  return count;
}

static unsigned db_postgresql_query_into_hash(DB* db, Table* table, struct TableSlot* slot,
                                              unsigned* min_id, unsigned* max_id) {
  unsigned rows = 0;
  if (!db->postgres) {
    LOG_WARN("Cannot query table data for %s, PostgreSQL connection not established", table_name(table));
    return 0;
  }
  char sql[MAX_SQL_LEN];
  snprintf(sql, MAX_SQL_LEN, "SELECT * FROM %s", table_name(table));
  PGresult* res = PQexec(db->postgres, sql);
  if (!res) {
    LOG_WARN("Cannot run query [%s] for table %s: %s", sql, table_name(table), PQerrorMessage(db->postgres));
    return 0;
  }
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    LOG_WARN("Cannot run query [%s] for table %s: %s", sql, table_name(table), PQerrorMessage(db->postgres));
    PQclear(res);
    return 0;
  }
  int num_fields = PQnfields(res);
  if (num_fields > MAX_FIELDS) {
    LOG_WARN("Expected at most %u number of fields for SELECT query for table %s, got %d",
             MAX_FIELDS, table_name(table), num_fields);
    PQclear(res);
    return 0;
  }
  char names[MAX_FIELDS][MAX_FIELD_NAME_LEN];
  int index_pos[MELIAN_MAX_INDEXES];
  for (unsigned idx = 0; idx < MELIAN_MAX_INDEXES; ++idx) index_pos[idx] = -1;
  for (int col = 0; col < num_fields; ++col) {
    const char* fname = PQfname(res, col);
    snprintf(names[col], MAX_FIELD_NAME_LEN, "%s", fname ? fname : "");
    for (unsigned idx = 0; idx < table->index_count; ++idx) {
      if (strcmp(names[col], table->indexes[idx].column) == 0) {
        index_pos[idx] = col;
      }
    }
  }
  *min_id = (unsigned)-1;
  *max_id = 0;
  double t0 = now_sec();
  int num_rows = PQntuples(res);
  for (int row = 0; row < num_rows; ++row) {
    char jbuf[MAX_JSON_LEN];
    unsigned jpos = 0;
    jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "{");
    unsigned cols = 0;
    for (int col = 0; col < num_fields; ++col) {
      int col_is_null = PQgetisnull(res, row, col);
      if (db->config->table.strip_null && col_is_null) continue;
      if (cols > 0) jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, ",");
      jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "\"%s\":", names[col]);
      if (col_is_null) {
        jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "null");
      } else {
        const char* value = PQgetvalue(res, row, col);
        if (!value) value = "";
        switch (PQftype(res, col)) {
          case 20:
          case 21:
          case 23:
          case 700:
          case 701:
          case 1700:
            jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "%s", value);
            break;
          default:
            jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "\"%s\"", value);
            break;
        }
      }
      ++cols;
    }
    jpos += snprintf(jbuf + jpos, MAX_JSON_LEN - jpos, "}");
    ++rows;
    unsigned frame = arena_store_framed(slot->arena, (uint8_t*)jbuf, jpos);
    if (frame == (unsigned)-1) {
      LOG_WARN("Could not store framed JSON for SELECT query for table %s", table_name(table));
      break;
    }
    int insert_error = 0;
    for (unsigned idx = 0; idx < table->index_count; ++idx) {
      int col_pos = index_pos[idx];
      if (col_pos < 0) continue;
      if (!slot->indexes[idx]) continue;
      if (table->indexes[idx].type == CONFIG_INDEX_TYPE_INT) {
        const char* value = PQgetvalue(res, row, col_pos);
        unsigned key_int = (unsigned) strtoul(value ? value : "0", 0, 10);
        if (!hash_insert(slot->indexes[idx], &key_int, sizeof(unsigned),
                         frame, jpos + sizeof(unsigned))) {
          LOG_WARN("Could not insert row for table %s key %u index %u",
                   table_name(table), key_int, idx);
          insert_error = 1;
          break;
        }
        if (idx == 0) {
          if (*min_id > key_int) *min_id = key_int;
          if (*max_id < key_int) *max_id = key_int;
        }
      } else {
        const char* value = PQgetvalue(res, row, col_pos);
        int hlen = PQgetlength(res, row, col_pos);
        if (!value || !hlen) continue;
        if (!hash_insert(slot->indexes[idx], value, (unsigned) hlen, frame, jpos + sizeof(unsigned))) {
          LOG_WARN("Could not insert row for table %s key %.*s index %u",
                   table_name(table), hlen, value, idx);
          insert_error = 1;
          break;
        }
      }
    }
    if (insert_error) break;
  }
  double t1 = now_sec();
  unsigned long elapsed = (t1 - t0) * 1000000;
  LOG_INFO("Fetched %u rows from table %s in %lu us", rows, table_name(table), elapsed);
  PQclear(res);
  return rows;
}

#endif  // HAVE_POSTGRESQL
