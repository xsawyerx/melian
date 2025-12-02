# Hacking Guide to Melian

Melian has a few secret options used for functionality testing, benchmarks, and performance testing. You can create suitable tables to have those functions working.

## Updating autotools

If you're updating `configure.ac`, regenerate `configure` with:

```bash
./bootstrap
```

## Defining Your Own Tables

Tables are declared in `MELIAN_TABLE_TABLES`, using a comma-separated list with optional per-table reload periods:

```C
MELIAN_TABLE_TABLES='table1#0|60|id#0:int,table2#1|60|id#0:int;hostname#1:string'
```

Each table must have at least one column, which can be int or string. You can have more than one, each of whatever type.

For example:

```sql
--- Create table1
CREATE TABLE table1 (
 id INT PRIMARY KEY,
  name VARCHAR(100),
 category VARCHAR(50),
  value VARCHAR(255),
  description TEXT,
 created_at DATETIME,
 updated_at DATETIME,
 active TINYINT(1)
);

--- Add some values
INSERT INTO table1 (id, name, category, value, description, created_at, updated_at, active)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 1000
)
SELECT
 n,
  CONCAT('item_', n),
 ELT(1 + n % 5, 'alpha', 'beta', 'gamma', 'delta', 'epsilon'),
  CONCAT('VAL_', LPAD(n, 4, '0')),
  CONCAT('Mock description for item ', n),
  NOW() - INTERVAL (n % 100) DAY,
  NOW(),
 (n % 2)
FROM seq;

--- Create table2
CREATE TABLE table2 (
 id INT PRIMARY KEY,
 hostname VARCHAR(255) UNIQUE,
 ip VARCHAR(45),
  status VARCHAR(20)
);

--- Add some values
INSERT INTO table2 (id, hostname, ip, status)
WITH RECURSIVE seq AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 1000
)
SELECT
  -- make it 1..10000
 (d.k * 1000) + s.n AS id,
  CONCAT('host-', LPAD((d.k * 1000) + s.n, 5, '0')) AS hostname,
  CONCAT(
    '10.',
    FLOOR(((d.k * 1000) + s.n) / 256),
    '.',
 ((d.k * 1000) + s.n) % 256,
    '.',
 (((d.k * 1000) + s.n) DIV 10) % 255
 ) AS ip,
  CASE
    WHEN ((d.k * 1000) + s.n) % 3 = 0 THEN 'active'
    WHEN ((d.k * 1000) + s.n) % 3 = 1 THEN 'inactive'
    ELSE 'maintenance'
  END AS status
FROM seq AS s
CROSS JOIN (
  SELECT 0 AS k UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) AS d
ORDER BY id;
```

Now you can try it out.

```bash
./melian-client -u /tmp/melian.sock -s
U: 3600 reqs, 3600 good, 0 bad, 120 ms → 30000 req/s, 38.5 ± 2.1 μs/req
```

## Comparing with Redis

The `-C` option in `melian-client` issues sequential lookups on `table2` by ID. To compare that benchmark with Redis using the same dataset as the SQL example above:

1. Seed Redis with matching rows (keys `table2:id:<id>` storing JSON payloads) using `clients/perl/redis_seed_table2.pl`:

   ```bash
   clients/perl/redis_seed_table2.pl --rows 10000 --flush
   ```

2. Start Melian with `table2` loaded from MySQL.

3. Run `clients/perl/benchmark_table2_vs_redis.pl` to fetch the same IDs via Melian (equivalent to `-C`) and via Redis `GET`, and print the throughput for both:

   ```bash
   clients/perl/benchmark_table2_vs_redis.pl --rows 10000
   ```

The benchmark script first validates that a few sample IDs return identical JSON from both stores before timing the full run.

## Technical Design

* Arena allocator: Pre-allocated, contiguous memory buffer. Each dataset swap allocates a new arena; old one is destroyed atomically after the swap.
* Hash table: Open addressing with linear probing using XXH32 (xxHash). Collisions are extremely rare.
* Double buffering: Two slots per table: one live, one loading. A swap pointer makes replacement atomic.
* Event loop: Uses `libevent2` for async I/O and signal handling.
* Cron thread: Separate thread periodically wakes up and reloads data from MySQL.
* Zero-copy I/O: Requests and responses are read and written directly from libevent buffers and arena memory without memcpy.
* Logging system: Color-coded logs with runtime log-level control.
* Binary protocol: Compact, endian-safe, 8-byte request header -> 4-byte length prefix -> payload.
* Melian automatically introspects all columns, serializes each row into JSON, and caches it as a preframed binary value.

## Internals Summary

* `db.c` MySQL integration
* `data.c` Table orchestration and atomic slot swapping
* `hash.c` High-speed xxHash + open addressing
* `arena.c` Continuous memory region management
* `cron.c` Background refresh thread
* `json.c` Minimal JSON builder
* `log.c` Colorized structured logging
* `protocol.h` Binary protocol definition
* `config.c` Environment configuration parser

## Example Query Workflow

1. Client sends:
   `Header(version=0x11, action='U', length=4)` + key (e.g. `id=42`)
2. Server finds the entry in memory and returns:
   `[4-byte length prefix] + {"id":42,"hostname":"host_42",...}`
3. Client reads, prints, or benchmarks the response.
