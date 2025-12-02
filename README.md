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
* Clients in C, Node.js, Perl, PHP, and Python.
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
# add flags when deps are in default paths
$ ./configure --with-mysql=/path/to/mysql/prefix \ # If you want MySQL/MariaDB support
               --with-libevent=/path/to/libevent \
               --with-jansson=/path/to/jansson \
               --with-sqlite3=/path/to/sqlite # if you want SQLite support
$ make
$ make install
```

The configure step fails explicitly if the MySQL/MariaDB client, libevent, or libjansson headers/libraries cannot be located, ensuring the resulting binaries are always linked against working dependencies. SQLite is detected when available (or via `--with-sqlite3=PREFIX`) so we can start linking against it for future backend work.

At least one database driver (MySQL or SQLite) must be available; configure stops early if neither can be found.

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

By default, it uses the MySQL/MariaDB driver on `127.0.0.1:3306`. You can switch drivers and adjust settings via these environment variables:

* `MELIAN_DB_DRIVER`: `mysql` (default) or `sqlite`
* `MELIAN_MYSQL_HOST`: `127.0.0.1`
* `MELIAN_MYSQL_PORT`: `3306`
* `MELIAN_MYSQL_DATABASE`: `mydb`
* `MELIAN_MYSQL_USER`: `root`
* `MELIAN_MYSQL_PASSWORD`: `root`
* `MELIAN_SQLITE_FILENAME`: `/etc/melian.db` (used when `MELIAN_DB_DRIVER=sqlite`)
* `MELIAN_SOCKET_PATH`: `/tmp/melian.sock`
* `MELIAN_TABLE_TABLES`: `table1,table2`
* `MELIAN_TABLE_PERIOD`: `60` seconds (reload interval)

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
