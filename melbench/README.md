# melbench

`wrk`-like benchmark harness for:
* Melian binary protocol
* Redis GET (RESP)
* Multiple targets (old/new Melian binaries or Redis) in one run
* Epoll on Linux, kqueue on macOS/BSD

## Build

```
cd melbench && make
```

## Key features

- Run multiple targets in one invocation via `--target=name:proto:dsn`
- Concurrency sweeps (`--concurrency=32,128,256`) and repeated runs (`--runs=N`)
- Reports RPS and latency p50/p95/p99 per target per concurrency
- Supports UNIX sockets and TCP DSNs

## Examples

### Compare two Melian builds and Redis at two concurrency points

```
./melbench \
  --target=mel_old:melian:unix:///tmp/melian_old.sock \
  --target=mel_new:melian:unix:///tmp/melian_new.sock \
  --target=redis:redis:tcp://127.0.0.1:6379 \
  --table-id=1 --column-id=1 \
  --key-type=string --key="Pixel" \
  --threads=2 --concurrency=32,128 --duration=30s --warmup=5s --runs=2
```

### Single-target sweep for Melian over TCP with int key

```
./melbench \
  --proto=melian \
  --dsn=tcp://127.0.0.1:31337 \
  --table-id=1 --column-id=0 \
  --key-type=int --key-int=42 \
  --threads=2 --concurrency=64,256 --duration=20s --warmup=3s
```

### Redis baseline

```
./melbench \
  --proto=redis \
  --dsn=tcp://127.0.0.1:6379 \
  --key-type=string --key="Pixel" \
  --threads=4 --conns=128 --duration=30s --warmup=5s
```

## Runbook

1. Build melbench: `cd melbench && make`
2. Start targets on distinct sockets/ports (e.g., `melian-old`, `melian-new`, `redis`).
3. Choose key/table/column for Melian; Redis uses `GET` with the same key when comparing.
4. Run melbench with:
   - `--target` entries for each binary you want to compare.
   - `--concurrency` sweep values (total open connections across all threads).
   - `--runs` to repeat and stabilize results (histograms are merged).
5. Inspect output blocks per target/concurrency:
   - `responses`, `rps`, `errors/timeouts`
   - Latency p50/p95/p99/min/max (us)
6. Adjust threads/concurrency to find peak RPS and where latency degrades.

## Output interpretation

For each target/concurrency:
- `rps`: throughput during the measured window (excludes warmup)
- `latency`: p50/p95/p99 in microseconds; watch for p99 blow-ups as concurrency rises
- `errors/timeouts`: should be near zero; rising numbers mean overload or dropped connections

Use matching DSNs and keys to compare Melian builds vs Redis:
- To check if Melian is faster, compare RPS and p95/p99 across targets at the same concurrency.
- To check which Melian build is faster, compare labels (e.g., `mel_old` vs `mel_new`).
- To check capacity, scan concurrency sweep to find the highest rps before p99/timeout growth.
