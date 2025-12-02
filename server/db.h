#pragma once

// A Db knows how to query the configured database to load all data for a table.

// TODO: make these limits dynamic? Arena?
enum {
  MAX_VERSION_LEN = 1024,
};

#include "config.h"

struct Arena;
struct Hash;
struct Table;
struct TableSlot;

#ifdef HAVE_MYSQL
struct MYSQL;
#endif
#ifdef HAVE_SQLITE3
typedef struct sqlite3 sqlite3;
#endif

typedef struct DB {
  Config *config;
#ifdef HAVE_MYSQL
  struct MYSQL *mysql;
  unsigned mysql_initialized;
#endif
#ifdef HAVE_SQLITE3
  sqlite3 *sqlite;
#endif
  char client_version[MAX_VERSION_LEN];
  char server_version[MAX_VERSION_LEN];
} DB;

DB* db_build(struct Config* config);
void db_destroy(DB* db);

void db_connect(DB* db);
void db_disconnect(DB* db);
unsigned db_get_table_size(DB* db, const char* table);
unsigned db_query_into_hash(DB* db, struct Table* table, struct TableSlot* slot,
                            unsigned* min_id, unsigned* max_id);
