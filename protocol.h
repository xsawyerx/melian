#pragma once

#include <stdint.h>

// These have to be all strings.
#define MELIAN_DEFAULT_DB_HOST          "127.0.0.1"
#define MELIAN_DEFAULT_DB_PORT          "3306"
#define MELIAN_DEFAULT_DB_NAME          "melian"
#define MELIAN_DEFAULT_DB_USER          "melian"
#define MELIAN_DEFAULT_DB_PASSWORD      "meliansecret"
#define MELIAN_DEFAULT_SQLITE_FILENAME  "/tmp/melian.db"
#define MELIAN_DEFAULT_SOCKET_HOST      "127.0.0.1"
#define MELIAN_DEFAULT_SOCKET_PORT      "0"
#define MELIAN_DEFAULT_SOCKET_PATH      "/tmp/melian.sock"
#define MELIAN_DEFAULT_LISTENERS        "unix:///tmp/melian.sock,tcp://127.0.0.1:0"
#define MELIAN_DEFAULT_TABLE_PERIOD     "60"
#define MELIAN_DEFAULT_TABLE_STRIP_NULL "false"
#define MELIAN_DEFAULT_TABLE_TABLES     "table1#0|60|id:int,table2#1|60|id:int;hostname:string"

typedef union MelianRequestHeader {
  uint8_t bytes[8];
  struct {
    uint8_t version;
    uint8_t action;
    uint8_t table_id;
    uint8_t index_id;
    uint32_t length;
  } data;
} MelianRequestHeader;

typedef union MelianResponseHeader {
  uint8_t bytes[4];
  struct {
    uint32_t length;
  } data;
} MelianResponseHeader;

enum {
  MELIAN_HEADER_VERSION = 0x11,
};

// Legacy data identifiers (kept for client compatibility; dynamic tables are configured at runtime).
enum DataTable {
  DATA_TABLE_TABLE1,
  DATA_TABLE_TABLE2,
  DATA_TABLE_LAST,
};

// All possible actions for a request.
enum MelianAction {
  MELIAN_ACTION_FETCH               = 'F',
  MELIAN_ACTION_DESCRIBE_SCHEMA     = 'D',
  MELIAN_ACTION_GET_STATISTICS      = 's',
  MELIAN_ACTION_QUIT                = 'q',
};

// Legacy action aliases (deprecated).
#define MELIAN_ACTION_QUERY_TABLE1_BY_ID   'U'
#define MELIAN_ACTION_QUERY_TABLE2_BY_ID   'C'
#define MELIAN_ACTION_QUERY_TABLE2_BY_HOST 'H'
#define MELIAN_ACTION_GET_LIVENESS         'l'
