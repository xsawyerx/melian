#include "test.h"
#include "log.h"
#include "arena.h"
#include "hash.h"
#include "config.h"
#include "db.h"
#include "data.h"
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>

// Stub DB functions (data.c references them but we don't use DB in tests)
void db_connect(DB* db) { (void)db; }
void db_disconnect(DB* db) { (void)db; }
unsigned db_get_table_size(DB* db, struct Table* table) { (void)db; (void)table; return 0; }
unsigned db_query_into_hash(DB* db, struct Table* table, struct TableSlot* slot,
                            unsigned* min_id, unsigned* max_id) {
  (void)db; (void)table; (void)slot; (void)min_id; (void)max_id; return 0;
}

// Helper: build a minimal ConfigTableSpec for testing
static ConfigTableSpec make_spec(unsigned id, const char* name, unsigned period,
                                 unsigned index_count) {
  ConfigTableSpec spec;
  memset(&spec, 0, sizeof(spec));
  spec.id = id;
  snprintf(spec.name, sizeof(spec.name), "%s", name);
  spec.period = period;
  spec.index_count = index_count;
  for (unsigned i = 0; i < index_count; ++i) {
    spec.indexes[i].id = i;
    snprintf(spec.indexes[i].column, sizeof(spec.indexes[i].column), "col%u", i);
    spec.indexes[i].type = CONFIG_INDEX_TYPE_INT;
  }
  snprintf(spec.select_stmt, sizeof(spec.select_stmt), "SELECT * FROM %s", name);
  return spec;
}

TEST(table_build_valid_spec) {
  ConfigTableSpec spec = make_spec(1, "test_table", 30, 1);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);
  table_destroy(t);
}

TEST(table_build_sets_fields) {
  ConfigTableSpec spec = make_spec(5, "users", 45, 2);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);
  ASSERT_EQ(t->table_id, 5);
  ASSERT_STR_EQ(t->name, "users");
  ASSERT_EQ(t->period, 45);
  ASSERT_EQ(t->index_count, 2);
  table_destroy(t);
}

TEST(table_build_default_period) {
  ConfigTableSpec spec = make_spec(1, "test", 0, 1);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);
  // Period 0 in spec -> uses default (20)
  ASSERT_EQ(t->period, 20);
  table_destroy(t);
}

TEST(table_build_dual_slots) {
  ConfigTableSpec spec = make_spec(1, "test", 30, 2);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);
  ASSERT_PTR_NOT_NULL(t->slots[0].arena);
  ASSERT_PTR_NOT_NULL(t->slots[1].arena);
  ASSERT_PTR_NOT_NULL(t->slots[0].indexes);
  ASSERT_PTR_NOT_NULL(t->slots[1].indexes);
  table_destroy(t);
}

TEST(table_destroy_null_safe) {
  table_destroy(NULL);
  ASSERT_TRUE(1);
}

TEST(table_name_returns_name) {
  ConfigTableSpec spec = make_spec(1, "mytable", 30, 1);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);
  ASSERT_STR_EQ(table_name(t), "mytable");
  table_destroy(t);
}

TEST(table_fetch_invalid_index) {
  ConfigTableSpec spec = make_spec(1, "test", 30, 1);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);

  // Build a hash for slot 0 so table_fetch doesn't hit a null hash for valid indexes
  struct TableSlot* slot = &t->slots[0];
  slot->indexes[0] = hash_build(16, slot->arena);
  hash_finalize_pointers(slot->indexes[0]);

  // Index 99 is out of range (only 1 index configured)
  uint32_t key = 1;
  const Bucket* b = table_fetch(t, 99, &key, sizeof(key));
  ASSERT_PTR_NULL(b);
  table_destroy(t);
}

TEST(manual_slot_load_and_fetch) {
  ConfigTableSpec spec = make_spec(1, "test", 30, 1);
  Table* t = table_build(&spec, 1024);
  ASSERT_PTR_NOT_NULL(t);

  // Manually populate slot 0
  unsigned slot_idx = 0;
  struct TableSlot* slot = &t->slots[slot_idx];
  arena_reset(slot->arena);

  Hash* h = hash_build(64, slot->arena);
  ASSERT_PTR_NOT_NULL(h);

  // Insert a key-value pair
  uint32_t key = 42;
  uint8_t val[] = {0xAA, 0xBB, 0xCC};
  unsigned frame = arena_store_framed(slot->arena, val, sizeof(val));
  hash_insert(h, &key, sizeof(key), frame, 4 + sizeof(val));
  hash_finalize_pointers(h);

  slot->indexes[0] = h;
  t->current_slot = slot_idx;

  // Now fetch through the table API
  const Bucket* b = table_fetch(t, 0, &key, sizeof(key));
  ASSERT_PTR_NOT_NULL(b);
  ASSERT_EQ(b->key_len, sizeof(key));
  ASSERT_MEM_EQ(b->frame_ptr + 4, val, sizeof(val));

  table_destroy(t);
}

TEST(data_build_with_config) {
  // Build a minimal Config struct manually (bypass config_build which reads env/files)
  Config config;
  memset(&config, 0, sizeof(config));
  config.table.table_count = 2;
  config.table.tables[0] = make_spec(0, "table_a", 30, 1);
  config.table.tables[1] = make_spec(1, "table_b", 60, 1);

  Data* d = data_build(&config);
  ASSERT_PTR_NOT_NULL(d);
  ASSERT_EQ(d->table_count, 2);
  ASSERT_PTR_NOT_NULL(d->tables[0]);
  ASSERT_PTR_NOT_NULL(d->tables[1]);
  data_destroy(d);
}

TEST(data_fetch_invalid_table) {
  Config config;
  memset(&config, 0, sizeof(config));
  config.table.table_count = 1;
  config.table.tables[0] = make_spec(0, "test", 30, 1);

  Data* d = data_build(&config);
  ASSERT_PTR_NOT_NULL(d);

  uint32_t key = 1;
  const Bucket* b = data_fetch(d, 255, 0, &key, sizeof(key));
  ASSERT_PTR_NULL(b);
  data_destroy(d);
}

TEST(data_schema_json_format) {
  Config config;
  memset(&config, 0, sizeof(config));
  config.table.table_count = 1;
  config.table.tables[0] = make_spec(0, "widgets", 30, 1);

  Data* d = data_build(&config);
  ASSERT_PTR_NOT_NULL(d);

  unsigned len = 0;
  const char* json = data_schema_json(d, &len);
  ASSERT_PTR_NOT_NULL(json);
  ASSERT_TRUE(len > 0);

  // Should contain "tables" key and our table name
  ASSERT_PTR_NOT_NULL(strstr(json, "\"tables\""));
  ASSERT_PTR_NOT_NULL(strstr(json, "\"widgets\""));

  data_destroy(d);
}

TEST_MAIN_BEGIN
  log_reset(1, 1);
  TEST_RUN(table_build_valid_spec);
  TEST_RUN(table_build_sets_fields);
  TEST_RUN(table_build_default_period);
  TEST_RUN(table_build_dual_slots);
  TEST_RUN(table_destroy_null_safe);
  TEST_RUN(table_name_returns_name);
  TEST_RUN(table_fetch_invalid_index);
  TEST_RUN(manual_slot_load_and_fetch);
  TEST_RUN(data_build_with_config);
  TEST_RUN(data_fetch_invalid_table);
  TEST_RUN(data_schema_json_format);
TEST_MAIN_END
