![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/xsawyerx/melian/ci.yml)
![Issues](https://img.shields.io/github/issues/xsawyerx/melian)
![License](https://img.shields.io/github/license/xsawyerx/melian)

![C](https://img.shields.io/badge/C-blue?logo=c&logoColor=ffffff)
![Python](https://img.shields.io/badge/Python-blue?logo=python&logoColor=ffffff)
![Javscript](https://img.shields.io/badge/Javascript-blue?logo=javascript&logoColor=ffffff)
![PHP](https://img.shields.io/badge/PHP-blue?logo=php&logoColor=ffffff)
![Perl](https://img.shields.io/badge/Perl-blue?logo=perl)
![Raku](https://img.shields.io/badge/Raku-blue)

![MySQL](https://img.shields.io/badge/MySQL-red?logo=mysql&logoColor=ffffff)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-red?logo=postgresql&logoColor=ffffff)
![SQLite](https://img.shields.io/badge/SQLite-red?logo=sqlite&logoColor=ffffff)

# Melian

Melian is a blazing-fast, in-memory cache server written in C.

It keeps entire database tables in memory and automatically refreshes them on a schedule, making read-heavy lookups near instantaneous (sub-millisecond) while staying simple and predictable.

Example:

```bash
melian-client -u /tmp/melian.sock -C
C:  10000 reqs,  10000 good,   0 bad,   68 ms →  145516 req/s,   6.87210 ±  0.58135 μs/req, CV:   8%, P95:   9 μs
```

* 145.5K RPS using over 10K records.
* 9 microseconds for P95.

## Features

* Blazing fast lookups: Sub-millisecond query latency, zero-copy networking, low CPU use, stable memory.
* Concurrency: Lock-free reads; atomic pointer swap on reload.
* Periodic refresh: automatic data reload via a background thread, not event-driven.
* Data model: Full- or partial-table snapshots, but not individual keys.
* Consistency: Always serves complete, coherent snapshots - no half-updated data.
* Dual-key indexing: look up entries by numeric or string key.
* Clients in [C](https://github.com/xsawyerx/melian/tree/main/clients/c), [Node.js](https://github.com/xsawyerx/melian/tree/main/clients/js), [Perl](https://metacpan.org/pod/Melian), [PHP](https://github.com/xsawyerx/melian/tree/main/clients/php/Melian), and [Python](https://github.com/xsawyerx/melian/tree/main/clients/python).
* Runtime performance statistics: query table size, min/max ID, and memory usage.

## Why

Most applications just need specific tables to always be in memory for fast reads.

Traditional caches ([Redis](https://github.com/redis/redis), [Memcached](https://github.com/memcached/memcached)) require your app to manage keys manually (and often individually) and synchronize them with the database. That adds complexity and risks serving stale or inconsistent data. Being flexible to these changes also forces them to perform slower than they could.

## Should I or Shouldn't I?

If:

* You have one or more tables in MySQL/MariaDB/etc.
* These change at some frequency (yearly/monthly/hourly/minutely)
* You want them available for very fast reads

Melian will solve your problem. It will also allow you to maintain consistency, ensuring that all data comes from the same read snapshot (atomicity).

Typical examples:

* Reference tables (countries, currencies, plans, services, permissions).
* Host or customer routing maps.
* User or organizational metadata used across many services.
* Materialized data sets refreshed periodically.
* Read-mostly microservices.

You probably want something else if:

* You need instant propagation of updates (e.g., user profile changes reflected immediately).
* You only need to cache a subset of rows or arbitrary keys (not expressed in a simple `SELECT`).
* You expect to perform frequent writes, updates, or deletes on the cache itself.
* You need distributed clustering or replication (Melian is a single-node in-memory cache).

In those cases, a general-purpose cache, or a write-through layer, is a better fit.

## Authors

* Gonzalo Diethelm (@Gonzo)
* Sawyer X (@xsawyerx)

## Building

```bash
# add flags when deps are not in the default paths
$ ./configure --with-mysql=/path/to/mysql \        # enable MySQL/MariaDB support
               --with-postgresql=/path/to/pgsql \  # enable PostgreSQL support
               --with-sqlite3=/path/to/sqlite \    # enable SQLite support
               --with-libevent=/path/to/libevent \
               --with-jansson=/path/to/jansson
$ make
$ make install
```

The configure step fails explicitly if the required headers/libraries cannot be located. At least one database backend (MySQL, PostgreSQL, or SQLite) must be available; configure stops early otherwise. Pass whichever `--with-*` flags match the drivers you intend to compile in.

## Running

1. Start the server

```bash
# Run listening on a UNIX socket
$ MELIAN_SOCKET_PATH=/tmp/melian.sock ./melian-server

# Run listening on a TCP socket
$ MELIAN_SOCKET_HOST=localhost MELIAN_SOCKET_PORT=9999 ./melian-server

# Display server options
$ ./melian-server --help
```

Set the database driver explicitly and adjust shared settings via config file or environment variables.

### Configuration file

```
{
    "database": {
        "driver": "sqlite",
        "name": "melian",
        "host": "localhost",
        "port": 9000,
        "username": "melian",
        "password": "melian",
        "sqlite": {
            "filename": "/var/lib/melian.db"
        }
    },
    "tables": [
        {
            "name": "table1",
            "id": 0,
            "period": 60,
            "indexes": [
                {
                    "id": 0,
                    "column": "id",
                    "type": "int"
                }
            ]
        },
        {
            "name": "table2",
            "id": 1,
            "period": 60,
            "indexes": [
                {
                    "id": 0,
                    "column": "id",
                    "type": "int"
                },
                {
                    "id": 1,
                    "column": "hostname",
                    "type": "string"
                }
            ]
        }
    ]
}
```

You can store the entire configuration in a JSON file and tell the server to load it at startup:

```bash
$ ./melian-server -c /path/to/melian-config.json
# or
$ ./melian-server --configfile /path/to/melian-config.json
```

Configuration sources are consulted in this order:

1. Command-line `-c/--configfile`.
2. Environment variable `MELIAN_CONFIG_FILE`.
3. Default `/etc/melian.json`.

Any values from the configuration file are still overridable by environment variables so you can keep secrets out of the file (e.g., inject `MELIAN_DB_PASSWORD` at runtime while other settings live in JSON).

### Environment variables

These will override any values in the config file.

* `MELIAN_DB_DRIVER` (config: `database.driver`): `mysql`, `postgresql`, or `sqlite` (required)
* `MELIAN_DB_HOST` (config: `database.host`): database host (default `127.0.0.1`)
* `MELIAN_DB_PORT` (config: `database.port`): database port (default `3306`)
* `MELIAN_DB_NAME` (config: `database.name`): database/schema name (default `melian`)
* `MELIAN_DB_USER` (config: `database.username`): username (default `melian`)
* `MELIAN_DB_PASSWORD` (config: `database.password`): password (default `meliansecret`)
* `MELIAN_SQLITE_FILENAME` (config: `database.sqlite.filename`): SQLite database filename (default `/etc/melian.db`)
* `MELIAN_TABLE_SELECTS`: semicolon-separated overrides (`table=SELECT ...;table2=SELECT ...`) to customize per-table SELECT statements
* `MELIAN_SOCKET_PATH`: `/tmp/melian.sock`
* `MELIAN_TABLE_TABLES` (config: `tables`): `table1,table2`
* `MELIAN_TABLE_PERIOD`: `60` seconds (reload interval)

When using `MELIAN_TABLE_SELECTS`, ensure each entry follows `table_name=SELECT ...` and separate multiple entries with `;`. The SQL is used verbatim, so double-check statements for the intended tables.

2. Use the test client

```bash
# Connect to a UNIX socket
$ ./melian-client -u /tmp/melian.sock

# Connect to a TCP socket
$ ./melian-client -p 8765

# Display client options
$ ./melian-client -?
$ ./melian-client -h
```

### Options

* `-U`: Query table1 by ID
* `-C`: Query table2 by ID
* `-H`: Query table2 by hostname
* `-s`: Request server statistics
* `-q`: Quit the server
* `-v`: Verbose logging
