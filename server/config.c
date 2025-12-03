#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "util.h"
#include "log.h"
#include "protocol.h"
#include "config.h"

static const char* get_config_string(const char* name, const char* def);
static int get_config_number(const char* name, const char* def);
static unsigned get_config_bool(const char* name, const char* def);
static char* trim(char* s);
static unsigned parse_table_specs(Config* config, const char* raw);
static ConfigIndexType parse_index_type(const char* value);
static ConfigDbDriver parse_db_driver(const char* value);
static const char* get_db_env_string(const char* primary, const char* legacy, const char* def);
static int get_db_env_number(const char* primary, const char* legacy, const char* def);

Config* config_build(void) {
  Config* config = 0;
  do {
    config = calloc(1, sizeof(Config));
    if (!config) {
      LOG_WARN("Could not allocate Config object");
      break;
    }

    const char* driver_raw = getenv("MELIAN_DB_DRIVER");
    if (!driver_raw || !driver_raw[0]) {
      LOG_FATAL("MELIAN_DB_DRIVER must be set to mysql, sqlite, or postgresql");
    }
    ConfigDbDriver driver = parse_db_driver(driver_raw);
#if !defined(HAVE_MYSQL)
    if (driver == CONFIG_DB_DRIVER_MYSQL) {
      LOG_FATAL("MySQL driver requested but not available in this build");
    }
#endif
#ifndef HAVE_SQLITE3
    if (driver == CONFIG_DB_DRIVER_SQLITE) {
      LOG_FATAL("SQLite driver requested but not available in this build");
    }
#endif
    config->db.driver = driver;
    LOG_INFO("Database driver selected: %s", config_db_driver_name(config->db.driver));
    config->db.host = get_db_env_string("MELIAN_DB_HOST", "MELIAN_MYSQL_HOST", MELIAN_DEFAULT_DB_HOST);
    config->db.port = get_db_env_number("MELIAN_DB_PORT", "MELIAN_MYSQL_PORT", MELIAN_DEFAULT_DB_PORT);
    config->db.database = get_db_env_string("MELIAN_DB_NAME", "MELIAN_MYSQL_DATABASE", MELIAN_DEFAULT_DB_NAME);
    config->db.user = get_db_env_string("MELIAN_DB_USER", "MELIAN_MYSQL_USER", MELIAN_DEFAULT_DB_USER);
    config->db.password = get_db_env_string("MELIAN_DB_PASSWORD", "MELIAN_MYSQL_PASSWORD", MELIAN_DEFAULT_DB_PASSWORD);
    config->db.sqlite_filename = get_config_string("MELIAN_SQLITE_FILENAME", MELIAN_DEFAULT_SQLITE_FILENAME);

    config->socket.host = get_config_string("MELIAN_SOCKET_HOST", MELIAN_DEFAULT_SOCKET_HOST);
    config->socket.port = get_config_number("MELIAN_SOCKET_PORT", MELIAN_DEFAULT_SOCKET_PORT);
    config->socket.path = get_config_string("MELIAN_SOCKET_PATH", MELIAN_DEFAULT_SOCKET_PATH);

    config->table.period = get_config_number("MELIAN_TABLE_PERIOD", MELIAN_DEFAULT_TABLE_PERIOD);
    config->table.strip_null = get_config_bool("MELIAN_TABLE_STRIP_NULL", MELIAN_DEFAULT_TABLE_STRIP_NULL);
    const char* table_raw = get_config_string("MELIAN_TABLE_TABLES", MELIAN_DEFAULT_TABLE_TABLES);
    config->table.schema = strdup(table_raw);
    if (!config->table.schema) {
      LOG_WARN("Could not allocate schema copy");
      break;
    }
    parse_table_specs(config, config->table.schema);
  } while (0);

  return config;
}

void config_show_usage(void) {
	printf("\n");
	printf("Behavior can be controlled using the following environment variables:\n");
	printf("  MELIAN_DB_DRIVER       : database driver to use (mysql, sqlite, postgresql) [required]\n");
	printf("  MELIAN_DB_HOST         : database host name (default: %s)\n", MELIAN_DEFAULT_DB_HOST);
	printf("  MELIAN_DB_PORT         : database listening port (default: %s)\n", MELIAN_DEFAULT_DB_PORT);
	printf("  MELIAN_DB_NAME         : database/schema name (default: %s)\n", MELIAN_DEFAULT_DB_NAME);
	printf("  MELIAN_DB_USER         : database user name (default: %s)\n", MELIAN_DEFAULT_DB_USER);
	printf("  MELIAN_DB_PASSWORD     : database user password (default: %s)\n", MELIAN_DEFAULT_DB_PASSWORD);
	printf("  MELIAN_SQLITE_FILENAME : SQLite database filename (default: %s)\n", MELIAN_DEFAULT_SQLITE_FILENAME);
	printf("  MELIAN_SOCKET_HOST     : host name where server will listen for TCP connections (default: %s)\n", MELIAN_DEFAULT_SOCKET_HOST);
	printf("  MELIAN_SOCKET_PORT     : port where server will listen for TCP connections -- 0 to disable (default: %s)\n", MELIAN_DEFAULT_SOCKET_PORT);
	printf("  MELIAN_SOCKET_PATH     : name of UNIX socket file to create -- empty to disable (default: %s)\n", MELIAN_DEFAULT_SOCKET_PATH);
	printf("  MELIAN_TABLE_PERIOD    : how often (seconds) to refresh the data by default (default: %s)\n", MELIAN_DEFAULT_TABLE_PERIOD);
	printf("  MELIAN_TABLE_STRIP_NULL: whether to strip null values in returned payloads (default: %s)\n", MELIAN_DEFAULT_TABLE_STRIP_NULL);
	printf("  MELIAN_TABLE_TABLES    : schema spec (default: %s); format per entry:\n", MELIAN_DEFAULT_TABLE_TABLES);
	printf("      name[#id][|period][|column#idx[:type];column#idx[:type]...]\n");
	printf("    Example: users#1|60|id:int;email:string,hosts#2|30|id:int;hostname:string\n");
	printf("    Supported index types: int, string (default: int)\n");
}

void config_destroy(Config* config) {
  if (!config) return;
  if (config->table.schema) free(config->table.schema);
  free(config);
}

static const char* get_config_string(const char* name, const char* def) {
  const char* value = getenv(name);
  if (!value) return def;
  return value;
}

static int get_config_number(const char* name, const char* def) {
  const char* value = get_config_string(name, def);
  return atoi(value);
}

static unsigned get_config_bool(const char* name, const char* def) {
  static const char* positive[] = {
    "1",
    "t",
    "T",
    "true",
    "True",
    "TRUE",
    "y",
    "Y",
    "yes",
    "Yes",
    "YES",
  };
  const char* value = get_config_string(name, def);
  for (unsigned p = 0; p < ALEN(positive); ++p) {
    if (strcmp(value, positive[p]) == 0) return 1;
  }
  return 0;
}

static const char* get_db_env_string(const char* primary, const char* legacy, const char* def) {
  const char* value = getenv(primary);
  if (value && value[0]) return value;
  if (legacy && legacy[0]) {
    value = getenv(legacy);
    if (value && value[0]) return value;
  }
  return def;
}

static int get_db_env_number(const char* primary, const char* legacy, const char* def) {
  const char* value = get_db_env_string(primary, legacy, def);
  return atoi(value);
}

static unsigned parse_table_specs(Config* config, const char* raw) {
  if (!raw || !raw[0]) {
    LOG_WARN("Empty table schema specification");
    return 0;
  }
  unsigned char used_ids[256] = {0};
  char* spec_copy = strdup(raw);
  if (!spec_copy) {
    LOG_WARN("Could not duplicate schema specification");
    return 0;
  }
  char* ctx = 0;
  for (char* token = strtok_r(spec_copy, ",", &ctx); token; token = strtok_r(NULL, ",", &ctx)) {
    if (config->table.table_count >= MELIAN_MAX_TABLES) {
      LOG_WARN("Maximum number of tables (%u) exceeded, skipping remaining specs", MELIAN_MAX_TABLES);
      break;
    }
    char* trimmed = trim(token);
    if (!trimmed[0]) continue;
    ConfigTableSpec* spec = &config->table.tables[config->table.table_count];
    memset(spec, 0, sizeof(*spec));
    spec->period = config->table.period;
    unsigned char used_index_ids[256] = {0};

    char* section_ctx = 0;
    unsigned section = 0;
    char* part = 0;
    unsigned skip_spec = 0;
    for (part = strtok_r(trimmed, "|", &section_ctx); part; part = strtok_r(NULL, "|", &section_ctx)) {
      char* value = trim(part);
      if (!value[0]) {
        ++section;
        continue;
      }
      if (section == 0) {
        char* hash = strchr(value, '#');
        if (hash) {
          *hash = '\0';
          unsigned id = atoi(hash + 1);
          if (id > 255) {
            LOG_WARN("Table id %u out of range (0-255) for spec [%s]", id, value);
            id = 0;
          }
          if (used_ids[id]) {
            LOG_WARN("Duplicate table id %u, skipping spec [%s]", id, value);
            skip_spec = 1;
            break;
          }
          spec->id = id;
        } else {
          LOG_WARN("Missing table id in specification [%s], skipping", value);
          skip_spec = 1;
          break;
        }
        used_ids[spec->id] = 1;
        strncpy(spec->name, value, sizeof(spec->name)-1);
      } else if (section == 1) {
        unsigned maybe = atoi(value);
        if (!maybe) {
          LOG_WARN("Ignoring non-numeric period [%s] for table %s", value, spec->name);
        } else {
          spec->period = maybe;
        }
      } else if (section == 2) {
        char* idx_ctx = 0;
        for (char* idx = strtok_r(value, ";", &idx_ctx); idx; idx = strtok_r(NULL, ";", &idx_ctx)) {
          if (spec->index_count >= MELIAN_MAX_INDEXES) {
            LOG_WARN("Maximum indexes (%u) reached for table %s", MELIAN_MAX_INDEXES, spec->name);
            break;
          }
          char* idx_part = trim(idx);
          if (!idx_part[0]) continue;
          ConfigIndexSpec* ispec = &spec->indexes[spec->index_count];
          char* type_sep = strchr(idx_part, ':');
          const char* type_val = 0;
          if (type_sep) {
            *type_sep = '\0';
            type_val = type_sep + 1;
          }
          char* hash = strchr(idx_part, '#');
          if (!hash) {
            LOG_WARN("Missing column id in index specification [%s] for table %s", idx_part, spec->name);
            continue;
          }
          *hash = '\0';
          unsigned column_id = atoi(hash + 1);
          if (column_id > 255) {
            LOG_WARN("Column id %u out of range (0-255) for table %s", column_id, spec->name);
            continue;
          }
          if (used_index_ids[column_id]) {
            LOG_WARN("Duplicate column id %u in table %s", column_id, spec->name);
            continue;
          }
          used_index_ids[column_id] = 1;
          ispec->id = column_id;
          strncpy(ispec->column, trim(idx_part), sizeof(ispec->column)-1);
          if (!ispec->column[0]) {
            LOG_WARN("Empty column name in index specification for table %s", spec->name);
            used_index_ids[column_id] = 0;
            continue;
          }
          if (type_val) {
            ispec->type = parse_index_type(type_val);
          } else {
            ispec->type = CONFIG_INDEX_TYPE_INT;
          }
          ++spec->index_count;
        }
      }
      if (skip_spec) break;
      ++section;
    }
    if (skip_spec) continue;
    if (!spec->name[0]) {
      LOG_WARN("Missing table name in specification, skipping");
      continue;
    }
    if (!spec->index_count) {
      LOG_WARN("Table %s missing index specification, skipping", spec->name);
      continue;
    }
    ++config->table.table_count;
  }
  free(spec_copy);
  return config->table.table_count;
}

static char* trim(char* s) {
  if (!s) return s;
  while (*s && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')) ++s;
  size_t len = strlen(s);
  while (len && (s[len-1] == ' ' || s[len-1] == '\t' || s[len-1] == '\n' || s[len-1] == '\r')) {
    s[--len] = '\0';
  }
  return s;
}

static ConfigIndexType parse_index_type(const char* value) {
  if (!value) return CONFIG_INDEX_TYPE_INT;
  char lower[16];
  snprintf(lower, sizeof(lower), "%s", value);
  for (char* p = lower; *p; ++p) {
    if (*p >= 'A' && *p <= 'Z') *p = *p - 'A' + 'a';
  }
  if (strcmp(lower, "string") == 0) return CONFIG_INDEX_TYPE_STRING;
  return CONFIG_INDEX_TYPE_INT;
}

static ConfigDbDriver parse_db_driver(const char* value) {
  char tmp[64];
  if (value && value[0]) {
    snprintf(tmp, sizeof(tmp), "%s", value);
    char* cleaned = trim(tmp);
    for (char* p = cleaned; *p; ++p) {
      if (*p >= 'A' && *p <= 'Z') *p = *p - 'A' + 'a';
    }
    if (strcmp(cleaned, "mysql") == 0) return CONFIG_DB_DRIVER_MYSQL;
    if (strcmp(cleaned, "sqlite") == 0) return CONFIG_DB_DRIVER_SQLITE;
    if (strcmp(cleaned, "postgresql") == 0) return CONFIG_DB_DRIVER_POSTGRESQL;
    LOG_WARN("Unknown database driver %s, defaulting to mysql", cleaned);
  }
  return CONFIG_DB_DRIVER_MYSQL;
}

const char* config_db_driver_name(ConfigDbDriver driver) {
  switch (driver) {
    case CONFIG_DB_DRIVER_MYSQL: return "mysql";
    case CONFIG_DB_DRIVER_SQLITE: return "sqlite";
    case CONFIG_DB_DRIVER_POSTGRESQL: return "postgresql";
    default: return "unknown";
  }
}
