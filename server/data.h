#pragma once

// Data stores the indexed data for all configured tables.
// Each table has a period, indicating how often to refresh the data.
// Each table has an arena for the actual data, and up to two hashes as indexes.
// Each table stores two slots of data, to allow lock-free data refreshes.

#include <stdatomic.h>
#include "protocol.h"

struct Bucket;
struct Config;
struct DB;

struct TableStats {
  unsigned last_loaded;
  unsigned rows;
  unsigned min_id;
  unsigned max_id;
};

#include "config.h"

struct TableSlot {
  struct Arena* arena;
  struct Hash** indexes;
};

typedef struct TableIndex {
  unsigned id;
  char column[MELIAN_MAX_NAME_LEN];
  ConfigIndexType type;
} TableIndex;

typedef struct Table {
  unsigned table_id;
  char name[MELIAN_MAX_NAME_LEN];
  char select_stmt[MELIAN_MAX_SELECT_LEN];
  unsigned period;
  unsigned index_count;
  TableIndex indexes[MELIAN_MAX_INDEXES];
  struct TableStats stats;
  atomic_uint current_slot;
  struct TableSlot slots[2];
} Table;

typedef struct DataSchema {
  char json[8192];
  unsigned len;
} DataSchema;

typedef struct Data {
  unsigned table_count;
  Table* tables[MELIAN_MAX_TABLES];
  Table* lookup[256];
  DataSchema schema;
} Data;

Table* table_build(const ConfigTableSpec* spec, unsigned arena_cap);
void table_destroy(Table* table);
const char* table_name(Table* table);
unsigned table_load_from_db(Table* table, struct DB* db, unsigned now, unsigned load);
const struct Bucket* table_fetch(Table* table, unsigned index_id, const void *key, unsigned len);

Data* data_build(struct Config* config);
void data_destroy(Data* data);
unsigned data_load_all_tables_from_db(Data* data, struct DB* db);
const struct Bucket* data_fetch(Data* data, unsigned table_id, unsigned index_id, const void *key, unsigned len);
void data_show_usage(void);
const char* data_schema_json(Data* data, unsigned* len);
