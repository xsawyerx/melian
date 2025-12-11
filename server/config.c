#include <errno.h>
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
static void apply_select_overrides(Config* config);
static ConfigTableSpec* find_table_spec(Config* config, const char* name);
static unsigned load_config_file(Config* config);
static char* read_entire_file(const char* path, size_t* len);
static const char* resolved_config_file_path(void);
static unsigned config_file_required(void);

static char* config_file_path = NULL;
static ConfigFileSource config_file_source = CONFIG_FILE_SOURCE_DEFAULT;

void config_set_config_file_path(const char* path, ConfigFileSource source) {
  const char* final_path = (path && path[0]) ? path : MELIAN_DEFAULT_CONFIG_FILE;
  char* copy = strdup(final_path);
  if (!copy) {
    LOG_WARN("Could not store config file path %s", final_path);
    return;
  }
  if (config_file_path) free(config_file_path);
  config_file_path = copy;
  config_file_source = source;
}

Config* config_build(void) {
  Config* config = 0;
  do {
    config = calloc(1, sizeof(Config));
    if (!config) {
      LOG_WARN("Could not allocate Config object");
      break;
    }
    if (!load_config_file(config)) {
      config_destroy(config);
      config = NULL;
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
    config->db.host = get_config_string("MELIAN_DB_HOST", MELIAN_DEFAULT_DB_HOST);
    config->db.port = get_config_number("MELIAN_DB_PORT", MELIAN_DEFAULT_DB_PORT);
    config->db.database = get_config_string("MELIAN_DB_NAME", MELIAN_DEFAULT_DB_NAME);
    config->db.user = get_config_string("MELIAN_DB_USER", MELIAN_DEFAULT_DB_USER);
    config->db.password = get_config_string("MELIAN_DB_PASSWORD", MELIAN_DEFAULT_DB_PASSWORD);
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
    apply_select_overrides(config);
  } while (0);

  return config;
}

void config_show_usage(void) {
	printf("\n");
	printf("Behavior can be controlled using the following environment variables:\n");
	printf("  MELIAN_CONFIG_FILE     : path to JSON configuration file (default: %s)\n", MELIAN_DEFAULT_CONFIG_FILE);
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
	printf("  MELIAN_TABLE_SELECTS   : semicolon-separated list of table=SELECT ... overrides\n");
	printf("  MELIAN_TABLE_STRIP_NULL: whether to strip null values in returned payloads (default: %s)\n", MELIAN_DEFAULT_TABLE_STRIP_NULL);
	printf("  MELIAN_TABLE_TABLES    : schema spec (default: %s); format per entry:\n", MELIAN_DEFAULT_TABLE_TABLES);
	printf("      name[#id][|period][|column#idx[:type];column#idx[:type]...]\n");
	printf("    Example: users#1|60|id:int;email:string,hosts#2|30|id:int;hostname:string\n");
	printf("    Supported index types: int, string (default: int)\n");
}

void config_destroy(Config* config) {
  if (!config) return;
  if (config->table.schema) free(config->table.schema);
  if (config->file.contents) free(config->file.contents);
  if (config->file.path) free(config->file.path);
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
    if (!spec->select_stmt[0]) {
      snprintf(spec->select_stmt, sizeof(spec->select_stmt), "SELECT * FROM %s", spec->name);
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

static ConfigTableSpec* find_table_spec(Config* config, const char* name) {
  if (!name) return NULL;
  for (unsigned i = 0; i < config->table.table_count; ++i) {
    ConfigTableSpec* spec = &config->table.tables[i];
    if (strcasecmp(spec->name, name) == 0) return spec;
  }
  return NULL;
}

static void apply_select_overrides(Config* config) {
  const char* raw = getenv("MELIAN_TABLE_SELECTS");
  if (!raw || !raw[0]) return;
  char* copy = strdup(raw);
  if (!copy) {
    LOG_WARN("Could not duplicate MELIAN_TABLE_SELECTS");
    return;
  }
  char* ctx = 0;
  for (char* entry = strtok_r(copy, ";", &ctx); entry; entry = strtok_r(NULL, ";", &ctx)) {
    char* trimmed = trim(entry);
    if (!trimmed[0]) continue;
    char* eq = strchr(trimmed, '=');
    if (!eq) {
      LOG_WARN("Invalid select override [%s], missing '='", trimmed);
      continue;
    }
    *eq = '\0';
    char* name = trim(trimmed);
    char* stmt = trim(eq + 1);
    if (!name[0] || !stmt[0]) {
      LOG_WARN("Invalid select override entry [%s]", entry);
      continue;
    }
    ConfigTableSpec* spec = find_table_spec(config, name);
    if (!spec) {
      LOG_WARN("Select override references unknown table %s", name);
      continue;
    }
    snprintf(spec->select_stmt, sizeof(spec->select_stmt), "%s", stmt);
  }
  free(copy);
}

static unsigned load_config_file(Config* config) {
  const char* path = resolved_config_file_path();
  if (!path || !path[0]) return 1;
  config->file.path = strdup(path);
  if (!config->file.path) {
    LOG_WARN("Could not duplicate config file path %s", path);
    return config_file_required() ? 0 : 1;
  }
  size_t len = 0;
  char* data = read_entire_file(path, &len);
  if (!data) {
    int err = errno;
    if (config_file_required()) {
      LOG_WARN("Failed to read config file %s: %s", path, err ? strerror(err) : "unknown error");
      return 0;
    }
    LOG_INFO("Config file %s not loaded (missing or unreadable); continuing with environment configuration", path);
    return 1;
  }
  config->file.contents = data;
  config->file.length = len;
  LOG_INFO("Loaded config file %s (%zu bytes)", path, len);
  return 1;
}

static char* read_entire_file(const char* path, size_t* len) {
  FILE* f = fopen(path, "rb");
  if (!f) return NULL;
  if (fseek(f, 0, SEEK_END) != 0) {
    fclose(f);
    return NULL;
  }
  long size = ftell(f);
  if (size < 0) {
    fclose(f);
    return NULL;
  }
  if (fseek(f, 0, SEEK_SET) != 0) {
    fclose(f);
    return NULL;
  }
  char* data = malloc((size_t)size + 1);
  if (!data) {
    fclose(f);
    return NULL;
  }
  size_t read = fread(data, 1, (size_t)size, f);
  fclose(f);
  if (read != (size_t)size) {
    free(data);
    return NULL;
  }
  data[size] = '\0';
  if (len) *len = (size_t)size;
  return data;
}

static const char* resolved_config_file_path(void) {
  if (config_file_path && config_file_path[0]) {
    return config_file_path;
  }
  return MELIAN_DEFAULT_CONFIG_FILE;
}

static unsigned config_file_required(void) {
  return config_file_source != CONFIG_FILE_SOURCE_DEFAULT;
}
