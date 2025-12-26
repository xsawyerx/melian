#pragma once

// A Config stores the configuration for the system.
// The default values can be modified by using environment variables.

#include <stddef.h>

#define MELIAN_DEFAULT_CONFIG_FILE "/etc/melian.json"

typedef enum ConfigDbDriver {
  CONFIG_DB_DRIVER_MYSQL = 0,
  CONFIG_DB_DRIVER_SQLITE = 1,
  CONFIG_DB_DRIVER_POSTGRESQL = 2,
} ConfigDbDriver;

typedef struct ConfigDb {
  ConfigDbDriver driver;
  const char* host;
  unsigned port;
  const char* database;
  const char* user;
  const char* password;
  const char* sqlite_filename;
} ConfigDb;

typedef struct ConfigSocket {
  const char* host;
  unsigned port;
  const char* path;
} ConfigSocket;

typedef struct ConfigListeners {
  ConfigSocket** sockets;
} ConfigListeners;

#define MELIAN_MAX_TABLES 64
#define MELIAN_MAX_INDEXES 16
#define MELIAN_MAX_NAME_LEN 256
#define MELIAN_MAX_SELECT_LEN 4096

typedef enum ConfigIndexType {
  CONFIG_INDEX_TYPE_INT,
  CONFIG_INDEX_TYPE_STRING,
} ConfigIndexType;

typedef struct ConfigIndexSpec {
  unsigned id;
  char column[MELIAN_MAX_NAME_LEN];
  ConfigIndexType type;
} ConfigIndexSpec;

typedef struct ConfigTableSpec {
  unsigned id;
  char name[MELIAN_MAX_NAME_LEN];
  unsigned period;
  unsigned index_count;
  char select_stmt[MELIAN_MAX_SELECT_LEN];
  ConfigIndexSpec indexes[MELIAN_MAX_INDEXES];
} ConfigTableSpec;

typedef struct ConfigTable {
  unsigned period;
  unsigned strip_null;
  char* schema;
  unsigned table_count;
  ConfigTableSpec tables[MELIAN_MAX_TABLES];
} ConfigTable;

typedef struct ConfigServer {
  unsigned show_msgs;
} ConfigServer;

typedef struct ConfigFileData {
  char* path;
  char* contents;
  size_t length;
} ConfigFileData;

typedef struct ConfigArray {
  const char** elems;
  size_t length;
} ConfigArray;

typedef struct Config {
  ConfigFileData file;
  ConfigDb db;
  ConfigListeners listeners;
  ConfigTable table;
  ConfigServer server;
} Config;

const char* config_db_driver_name(ConfigDbDriver driver);

typedef enum ConfigFileSource {
  CONFIG_FILE_SOURCE_DEFAULT = 0,
  CONFIG_FILE_SOURCE_ENV,
  CONFIG_FILE_SOURCE_CLI,
} ConfigFileSource;

void config_set_config_file_path(const char* path, ConfigFileSource source);
void config_set_cli_overrides(const char* listeners);

Config* config_build(void);
void config_destroy(Config* config);
void config_show_usage(void);
