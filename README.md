![Version](https://img.shields.io/badge/Version-0.5.0-9cf)

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
![MariaDB](https://img.shields.io/badge/MariaDB-red?logo=mariadb&logoColor=ffffff)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-red?logo=postgresql&logoColor=ffffff)
![SQLite](https://img.shields.io/badge/SQLite-red?logo=sqlite&logoColor=ffffff)

# Melian

Melian is a blazing-fast, in-memory cache server written in C.

It keeps entire or partial database tables in memory and automatically refreshes them on a schedule, making read-heavy lookups near instantaneous (sub-millisecond) while staying simple and predictable.

## Performance

Melian vs Redis:

* Melian plateaus at ~580k RPS with p99 ≤ 256 µs
* Redis reaches ~300k RPS with p99 up to 1024 µs
* Melian is ~2× higher peak throughput and 2–4× lower p99 latency at all concurrency levels
* Stable throughput plateau from 16–256 connections

Benchmark details, run on a MacBook Pro M3 36GB:

```
Concurrency | Melian RPS | Redis RPS | Melian p99 | Redis p99
------------+------------+-----------+------------+-----------
1           | 157k       | 97k       | 8 µs       | 16 µs
16          | 564k       | 265k      | 32 µs      | 64 µs
32          | 586k       | 285k      | 32 µs      | 128 µs
64          | 578k       | 297k      | 128 µs     | 256 µs
256         | 578k       | 309k      | 256 µs     | 1024 µs
```

Analyzed:

```
Concurrency | Throughput (Melian > Redis) | Tail latency (p99)
------------+------------------------------+--------------------
1           | 1.6×                         | 2× lower
16          | 2.1×                         | 2× lower
32          | 2.1×                         | 4× lower
64          | 1.9×                         | 2× lower
256         | 1.9×                         | 4× lower
```

## Features

* Blazing fast lookups: Sub-millisecond query latency, zero-copy networking, low CPU use, stable memory.
* Concurrency: Lock-free reads; atomic pointer swap on reload.
* Periodic refresh: automatic data reload via a background thread, not event-driven.
* Data model: Full- or partial-table snapshots, but not individual keys.
* Consistency: Always serves complete, coherent snapshots - no half-updated data.
* Dual-key indexing: look up entries by numeric or string key.
* Clients in [Node.js](https://github.com/xsawyerx/melian-nodejs), [Python](https://github.com/xsawyerx/melian-python), [C](https://github.com/xsawyerx/melian/tree/main/clients/c), [Perl](https://metacpan.org/pod/Melian), [PHP](https://github.com/xsawyerx/melian-php/), and [Raku](https://github.com/xsawyerx/melian-raku).
* Runtime performance statistics: query table size, min/max ID, and memory usage.
* Binary row payloads: length-prefixed field name/type/value encoding for fast decode and byte-accurate values.

## Binary Row Format

Fetch responses return a binary payload with the following layout (all integer
fields are little-endian):

- `u32 field_count`
- Repeated `field_count` times:
  - `u16 name_len`
  - `bytes name[name_len]` (UTF-8 column name)
  - `u8 type`
  - `u32 value_len`
  - `bytes value[value_len]` (absent for NULL types)

Type IDs:

- `0` NULL (no value bytes)
- `1` INT64 (8 bytes, signed)
- `2` FLOAT64 (8 bytes, IEEE-754)
- `3` BYTES (raw bytes)
- `4` DECIMAL (ASCII bytes)
- `5` BOOL (1 byte, 0 or 1)

Clients decode this payload into per-field `{type, value}` pairs.

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
# Run listening on a UNIX socket (default)
$ ./melian-server

# Run listening on a TCP socket only
$ MELIAN_SOCKET_PATH= MELIAN_SOCKET_PORT=9999 ./melian-server

# Run listening on both UNIX and TCP simultaneously
$ MELIAN_SOCKET_PORT=9999 ./melian-server

# Display server options
$ ./melian-server --help

# Display server version
$ ./melian-server --version
```

Set the database driver explicitly and adjust shared settings via config file or environment variables.

### Configuration file

```
{
    "database": {
        "driver": "sqlite",
        "name": "melian",
        "host": "localhost",
        "port": 42123,
        "username": "melian",
        "password": "melian",
        "sqlite": {
            "filename": "/var/lib/melian.db"
        }
    },
    "socket": {
        "path": "/tmp/melian.sock",
        "host": "127.0.0.1",
        "port": 42123
    },
    "table": {
        "period": 60,
        "selects": {
            "table1": "SELECT id, email FROM table1",
            "table2": "SELECT id, hostname FROM table2 WHERE active = 1"
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
* `MELIAN_SOCKET_HOST` (config: `socket.host`): TCP bind address (default `127.0.0.1`)
* `MELIAN_SOCKET_PORT` (config: `socket.port`): TCP port -- `0` to disable (default `0`)
* `MELIAN_SOCKET_PATH` (config: `socket.path`): UNIX socket path -- empty to disable (default `/tmp/melian.sock`)

Both UNIX and TCP listeners can be active simultaneously. By default only the UNIX socket is enabled. Set `MELIAN_SOCKET_PORT` to a non-zero value to also enable TCP.
* `MELIAN_SERVER_TOKENS` (config: `server.tokens`): whether to advertise the server version in status JSON (default `true`)
* `MELIAN_TABLE_PERIOD` (config: `table.period`): `60` seconds (reload interval)
* `MELIAN_TABLE_SELECTS` (config: `table.selects`): semicolon-separated overrides (`table=SELECT ...;table2=SELECT ...`) to customize per-table SELECT statements
* `MELIAN_TABLE_TABLES` (config: `tables`): `table1,table2`

When using `MELIAN_TABLE_SELECTS`, ensure each entry follows `table_name=SELECT ...` and separate multiple entries with `;`. The SQL is used verbatim, so double-check statements for the intended tables.

In JSON, use `table.selects` with a mapping of table names to their statements:

```json
"table": {
  "selects": {
    "table1": "SELECT ...",
    "table2": "SELECT ..."
  }
}
```

### Versioning

The server version is compiled in `protocol.h` as `MELIAN_SERVER_VERSION`. Use `--version` to print it. If you want to hide the version from the status JSON, set `MELIAN_SERVER_TOKENS=false` or `server.tokens: false` in the config file.

2. Use the test client

Ths describes the test client we have in C.

```bash
# Connect to a UNIX socket
$ ./melian-client -u /tmp/melian.sock

# Connect to a TCP socket (start server with TCP enabled)
$ MELIAN_SOCKET_PORT=42123 ./melian-server
$ ./melian-client -p 42123

# Display client options
$ ./melian-client -?
$ ./melian-client -h
```

### Subcommands

* `fetch`: Fetch a single row (see [Ad-hoc querying](#ad-hoc-querying))
* `schema`: Show the server schema as JSON
* `stats`: Show server statistics as JSON

### Benchmark options

When no subcommand is given, the client runs in benchmark mode:

* `-U`: Benchmark table1 by ID
* `-C`: Benchmark table2 by ID
* `-H`: Benchmark table2 by hostname
* `-s`: Print server statistics
* `-q`: Quit the server
* `-v`: Verbose logging

### Ad-hoc querying

Melian uses a binary protocol, so `curl` won't work. The C client supports subcommands for quick, ad-hoc queries against a running server.

All examples below assume a UNIX socket at `/tmp/melian.sock`. For TCP, replace `-u /tmp/melian.sock` with `-p PORT` (and optionally `-h HOST`).

**Describe schema** (discover tables and indexes):

```bash
./melian-client -u /tmp/melian.sock schema
```

**Fetch a row by integer key** (table `table1`, index `id`, key `42`):

```bash
./melian-client -u /tmp/melian.sock fetch --table table1 --index id --key 42
```

**Fetch a row by string key** (table `table2`, index `hostname`, key `host-00002`):

```bash
./melian-client -u /tmp/melian.sock fetch --table table2 --index hostname --key host-00002
```

**Mix names and IDs freely** (table by ID, index by name):

```bash
./melian-client -u /tmp/melian.sock fetch --table-id 1 --index hostname --key host-00002
```

**Server statistics:**

```bash
./melian-client -u /tmp/melian.sock stats
```

The key type (integer or string) is detected automatically from the schema - no need to specify it. Output is JSON, suitable for piping through `jq`.

## Docker images

The provided `Dockerfile` builds a self-contained image (SQLite + bundled clients). Build it locally:

```bash
docker build -t melian:latest .
```

### With UNIX socket (default)

This keeps the UNIX domain socket enabled and bind-mounts it to the host so native clients can connect:

```bash
mkdir -p $(pwd)/socket
docker run --rm \
  -p 42123:42123 \
  -v $(pwd)/socket:/run/melian \
  melian:latest
```

The server listens on `/run/melian/melian.sock` inside the container. On the host you’ll find the mirrored socket at `./socket/melian.sock`.

### Both UNIX and TCP

Enable both listeners by setting a port while keeping the default socket path:

```bash
docker run --rm \
  -v $(pwd)/socket:/run/melian \
  -e MELIAN_SOCKET_HOST=0.0.0.0 \
  -e MELIAN_SOCKET_PORT=42123 \
  -p 42123:42123 \
  melian:latest
```

### TCP only (disable UNIX socket)

To accept only TCP connections, unset `MELIAN_SOCKET_PATH` and map the port:

```bash
docker run --rm \
  -e MELIAN_SOCKET_PATH= \
  -e MELIAN_SOCKET_HOST=0.0.0.0 \
  -e MELIAN_SOCKET_PORT=42123 \
  -p 42123:42123 \
  melian:latest
```

Clients can then connect to `tcp://localhost:42123`.

## Security

### Unix socket (default)

The default Unix socket is created with mode `0660` (owner + group read/write). Only processes running as the same user or group can connect. No additional configuration is needed.

### Network restrictions (TCP)

When exposing Melian over TCP, bind to a specific interface rather than `0.0.0.0` and use firewall rules to restrict access to trusted hosts:

```bash
# Bind Melian to an internal interface
MELIAN_SOCKET_HOST=10.0.1.5 MELIAN_SOCKET_PORT=42123 ./melian-server
```

```bash
# Allow only your application subnet, drop everything else
iptables -A INPUT -p tcp --dport 42123 -s 10.0.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 42123 -j DROP
```

This is handled at the kernel level with zero performance overhead.

### Encryption (TLS via proxy)

Melian does not include built-in TLS. Its performance comes from zero-copy direct I/O on raw sockets. In-process TLS would add a 30-50% throughput penalty (the same cost Redis 6+ pays for native TLS).

For encrypted connections, run a TLS termination proxy in front of Melian. [stunnel](https://www.stunnel.org/), HAProxy, and nginx all work. Example stunnel configuration:

```ini
[melian]
accept = 0.0.0.0:42123
connect = /tmp/melian.sock
cert = /etc/melian/server.pem
key = /etc/melian/server.key
```

Clients connect to `tls://host:42123`; stunnel decrypts and forwards to Melian's Unix socket. For containerized deployments, run the TLS proxy as a sidecar container alongside Melian.
