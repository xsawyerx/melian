#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <jansson.h>
#include "util.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "config.h"
#include "db.h"
#include "data.h"

enum {
  DATA_REFRESH_PERIOD = 20,
  ARENA_INITIAL_CAPACITY = 1024,
};

static void data_refresh_schema(Data* data);
static json_t* schema_table_json(Table* table);
static const char* index_type_name(ConfigIndexType type);

Table* table_build(const ConfigTableSpec* spec, unsigned arena_cap) {
  Table* table = 0;
  unsigned bad = 0;
  do {
    table = calloc(1, sizeof(Table));
    if (!table) {
      LOG_WARN("Could not allocate Table object id %u", spec->id);
      break;
    }

    table->table_id = spec->id;
    strncpy(table->name, spec->name, sizeof(table->name)-1);
    table->period = spec->period ? spec->period : DATA_REFRESH_PERIOD;
    strncpy(table->select_stmt, spec->select_stmt, sizeof(table->select_stmt) - 1);
    table->index_count = spec->index_count;
    for (unsigned idx = 0; idx < spec->index_count; ++idx) {
      table->indexes[idx].id = spec->indexes[idx].id;
      table->indexes[idx].type = spec->indexes[idx].type;
      strncpy(table->indexes[idx].column, spec->indexes[idx].column,
              sizeof(table->indexes[idx].column)-1);
    }

    for (unsigned b = 0; b < 2; ++b) {
      struct TableSlot* slot = &table->slots[b];
      slot->arena = arena_build(arena_cap);
      if (!slot->arena) {
        LOG_WARN("Could not allocate arena %u for Table id %u", b, spec->id);
        ++bad;
      }
      slot->indexes = calloc(table->index_count, sizeof(struct Hash*));
      if (!slot->indexes) {
        LOG_WARN("Could not allocate index array %u for Table id %u", b, spec->id);
        ++bad;
      }
    }
    if (bad) {
      break;
    }

    LOG_DEBUG("Built table id %u name %s period %u indexes %u",
              table->table_id, table->name, table->period, table->index_count);
  } while (0);
  if (bad) {
    table_destroy(table);
    table = 0;
  }
  return table;
}

void table_destroy(Table* table) {
  if (!table) return;
  LOG_DEBUG("Destroying table id %u name %s period %u",
            table->table_id, table->name, table->period);
  for (unsigned b = 0; b < 2; ++b) {
    struct TableSlot* slot = &table->slots[b];
    if (slot->indexes) {
      for (unsigned i = 0; i < table->index_count; ++i) {
        if (slot->indexes[i]) hash_destroy(slot->indexes[i]);
      }
      free(slot->indexes);
    }
    if (slot->arena) arena_destroy(slot->arena);
  }
  free(table);
}

const char* table_name(Table* table) {
  assert(table);
  return table->name;
}

unsigned table_load_from_db(Table* table, struct DB* db, unsigned now, unsigned load) {
  unsigned elapsed = now - table->stats.last_loaded;
  LOG_DEBUG("NOW %u LAST %u ELAPSED %u", now, table->stats.last_loaded, elapsed);
  if (elapsed < table->period) {
    LOG_DEBUG("TOO SOON!");
    return 0;
  }

  if (!load) return 1;

  unsigned pos = 1 - table->current_slot;
  struct TableSlot* slot = &table->slots[pos];
  arena_reset(slot->arena);

  unsigned size = db_get_table_size(db, table);
  unsigned hash_cap = 2 * next_power_of_two(size, 1);
  LOG_DEBUG("Building hash tables for %s, size %u, capacity %u", table->name, size, hash_cap);

  for (unsigned idx = 0; idx < table->index_count; ++idx) {
    if (slot->indexes[idx]) hash_destroy(slot->indexes[idx]);
    slot->indexes[idx] = hash_build(hash_cap, slot->arena);
  }

  unsigned min_id = (unsigned) -1;
  unsigned max_id = 0;
  unsigned rows = db_query_into_hash(db, table, slot, &min_id, &max_id);
  LOG_INFO("Loaded %u rows for table %s at slot %u", rows, table->name, pos);

  table->stats.last_loaded = now;
  table->stats.rows = rows;
  if (table->index_count && table->indexes[0].type == CONFIG_INDEX_TYPE_INT) {
    table->stats.min_id = min_id == (unsigned)-1 ? 0 : min_id;
    table->stats.max_id = max_id;
  } else {
    table->stats.min_id = 0;
    table->stats.max_id = 0;
  }
  table->current_slot = pos;
  return rows;
}

const Bucket* table_fetch(Table* table, unsigned index_id, const void *key, unsigned len) {
  if (index_id >= table->index_count) {
    LOG_WARN("Invalid index %u for table %s", index_id, table->name);
    return NULL;
  }
  unsigned current_slot = table->current_slot;
  struct TableSlot* slot = &table->slots[current_slot];
  Hash* hash = slot->indexes[index_id];
  if (!hash) {
    LOG_FATAL("Unexpected null hash for table %s index %u current %u",
              table->name, index_id, current_slot);
  }
  return hash_get(hash, key, len);
}

Data* data_build(Config* config) {
  Data* data = 0;
  unsigned bad = 0;
  do {
    if (!config->table.table_count) {
      LOG_WARN("No tables configured");
      break;
    }
    data = calloc(1, sizeof(Data));
    if (!data) {
      LOG_WARN("Could not allocate Data object");
      break;
    }

    for (unsigned t = 0; t < config->table.table_count; ++t) {
      const ConfigTableSpec* spec = &config->table.tables[t];
      Table* table = table_build(spec, ARENA_INITIAL_CAPACITY);
      if (!table) {
        ++bad;
        break;
      }
      data->tables[data->table_count++] = table;
      LOG_INFO("Configured table id=%u name=%s period=%u indexes=%u",
               table->table_id, table->name, table->period, table->index_count);
      if (spec->id < ALEN(data->lookup)) {
        data->lookup[spec->id] = table;
      } else {
        LOG_WARN("Table id %u exceeds lookup table, skipping mapping", spec->id);
      }
    }
    if (bad) {
      break;
    }
    data_refresh_schema(data);
  } while (0);
  if (bad) {
    data_destroy(data);
    data = 0;
  }
  return data;
}

void data_destroy(Data* data) {
  if (!data) return;
  for (unsigned t = 0; t < data->table_count; ++t) {
    table_destroy(data->tables[t]);
  }
  free(data);
}

unsigned data_load_all_tables_from_db(Data* data, struct DB* db) {
  unsigned rows = 0;
  unsigned now = time(0);
  do {
    unsigned tables = 0;
    for (unsigned t = 0; t < data->table_count; ++t) {
      Table* table = data->tables[t];
      if (!table) continue;
      tables += table_load_from_db(table, db, now, 0);
    }
    if (!tables) {
      LOG_DEBUG("No tables to refresh");
      break;
    }

    LOG_DEBUG("Refreshing %u tables", tables);
    rows = 0;
    db_connect(db);
    for (unsigned t = 0; t < data->table_count; ++t) {
      Table* table = data->tables[t];
      if (!table) continue;
      rows += table_load_from_db(table, db, now, 1);
    }
    db_disconnect(db);
  } while (0);

  return rows;
}

const Bucket* data_fetch(Data* data, unsigned table_id, unsigned index_id, const void *key, unsigned len) {
  if (table_id >= ALEN(data->lookup)) return NULL;
  Table* table = data->lookup[table_id];
  if (!table) return NULL;
  return table_fetch(table, index_id, key, len);
}

void data_show_usage(void) {
	printf("\nTable schema is configured via MELIAN_TABLE_TABLES (dynamic).\n");
}

const char* data_schema_json(Data* data, unsigned* len) {
  if (len) *len = data->schema.len;
  return data->schema.json;
}

static json_t* schema_table_json(Table* table) {
  json_t* indexes = json_array();
  if (!indexes) return NULL;
  for (unsigned idx = 0; idx < table->index_count; ++idx) {
    TableIndex* index = &table->indexes[idx];
    json_t* idx_obj = json_pack("{s:i,s:s,s:s}",
                                "id", index->id,
                                "column", index->column,
                                "type", index_type_name(index->type));
    if (!idx_obj || json_array_append_new(indexes, idx_obj) < 0) {
      if (idx_obj) json_decref(idx_obj);
      json_decref(indexes);
      return NULL;
    }
  }
  json_t* table_obj = json_pack("{s:s,s:i,s:i,s:O}",
                                "name", table->name,
                                "id", table->table_id,
                                "period", table->period,
                                "indexes", indexes);
  if (!table_obj) {
    json_decref(indexes);
  }
  return table_obj;
}

static void data_refresh_schema(Data* data) {
  json_t* tables = json_array();
  json_t* root = NULL;
  char* dump = NULL;
  unsigned success = 0;

  data->schema.len = 0;
  data->schema.json[0] = '\0';

  if (!tables) {
    LOG_WARN("Failed to allocate schema table array");
    goto done;
  }

  for (unsigned t = 0; t < data->table_count; ++t) {
    json_t* table_json = schema_table_json(data->tables[t]);
    if (!table_json || json_array_append_new(tables, table_json) < 0) {
      if (table_json) json_decref(table_json);
      LOG_WARN("Could not serialize schema for table %s", data->tables[t]->name);
      goto done;
    }
  }

  root = json_pack("{s:O}", "tables", tables);
  if (!root) {
    LOG_WARN("Failed to build schema root");
    goto done;
  }
  tables = NULL;

  dump = json_dumps(root, JSON_COMPACT | JSON_ENSURE_ASCII);
  if (!dump) {
    LOG_WARN("Failed to serialize schema JSON");
    goto done;
  }

  size_t dump_len = strlen(dump);
  if (dump_len >= sizeof(data->schema.json)) {
    LOG_WARN("Schema JSON truncated from %zu bytes to %zu", dump_len,
             sizeof(data->schema.json) - 1);
    dump_len = sizeof(data->schema.json) - 1;
  }
  memcpy(data->schema.json, dump, dump_len);
  data->schema.json[dump_len] = '\0';
  data->schema.len = (unsigned)dump_len;
  success = 1;

done:
  if (dump) free(dump);
  if (tables) json_decref(tables);
  if (root) json_decref(root);
  if (success) {
    LOG_INFO("Schema JSON built with %u tables, len=%u", data->table_count, data->schema.len);
  } else {
    data->schema.len = 0;
    data->schema.json[0] = '\0';
    LOG_WARN("Schema JSON build failed");
  }
}

static const char* index_type_name(ConfigIndexType type) {
  switch (type) {
    case CONFIG_INDEX_TYPE_STRING:
      return "string";
    case CONFIG_INDEX_TYPE_INT:
    default:
      return "int";
  }
}
