# JiHuan Perf / Load Tests (Phase 6a)

Two complementary surfaces:

1. **Microbenchmarks** — `cargo bench -p jihuan-bench`. Criterion-driven,
   baseline-friendly, measures engine-internal paths (chunking, compression,
   dedup, put/get, stats).
2. **End-to-end load** — Python scripts under `tests/perf/`. Exercise the
   HTTP surface at realistic concurrency, report QPS / latency percentiles.

---

## Microbench quick start

```powershell
# full suite
cargo bench -p jihuan-bench

# one target
cargo bench -p jihuan-bench --bench stats_bench

# save a baseline for regression comparison
cargo bench -p jihuan-bench -- --save-baseline v0.4

# later, compare
cargo bench -p jihuan-bench -- --baseline v0.4
```

Benchmarks currently shipped:

| Bench | Focus |
|---|---|
| `storage_bench` | `put_bytes` / `get_bytes` across 1 KB → 4 MB |
| `stream_bench` | `put_stream` vs `put_bytes` (Phase 6b-P1 tracking) |
| `stats_bench`  | `Engine::stats()` cold + cached, 100 & 1k files |
| `dedup_bench`  | dedup hit path |
| `compression_bench` | zstd levels |

Planned (not yet landed):
- `chunking_bench` (P1 ratelimited chunker comparison)
- `concurrent_writers_bench` (P2 sharded writer tracking)

---

## Load scripts

All scripts are stdlib-only and accept `--help`. Defaults are safe for a
local server with `auth.enabled = true` — pass the admin key via
`JIHUAN_ADMIN_KEY` or `--key`.

### load_mixed.py

Mixed PUT/GET workload, configurable ratio & object sizes.

```powershell
$env:JIHUAN_ADMIN_KEY = "jh_xxxxxxxx"
python tests/perf/load_mixed.py --smoke                    # 30s @ 4 workers
python tests/perf/load_mixed.py --concurrency 32 --duration 120 --write-ratio 0.5
python tests/perf/load_mixed.py --size-min 1024 --size-max 10240  # small files
```

Output (sample):

```
running for 30.0s with 4 workers, write ratio 30.0%, size 4096-262144 bytes
elapsed 30.1s, total 1842 ops
  PUT:    561 ops  ok=   561  qps=    18.7  p50=  24.5ms  p95=  83.1ms  p99= 142.6ms
  GET:   1281 ops  ok=  1281  qps=    42.6  p50=   4.1ms  p95=  18.3ms  p99=  44.8ms
```

Planned (next session):
- `load_small_files.py` — millions of 1-4 KB objects; stress dedup + metadata
- `load_large_file.py` — single 10 GB streaming upload
- `load_dedup.py` — repeated identical content, measure hit ratio
- `load_range.py` — random Range requests

---

## Flamegraph / profiling

```powershell
# Windows: use samply (https://github.com/mstange/samply)
cargo install samply
samply record target/release/jihuan-server
# ... run load_mixed.py in another terminal ...
# Ctrl-C samply → HTML report opens in browser
```

On Linux use `cargo flamegraph`.

Build the server with the `profiling` profile to keep debug symbols:

```powershell
cargo build --profile profiling --bin jihuan-server
```

---

## Comparison harness (roadmap)

Phase 6c will add a `tests/perf/compare/` harness with docker-compose
targets for MinIO, SeaweedFS, Garage, and matching Python load scripts.
The same `load_mixed.py` parameters will drive every backend via a small
adapter layer. Not implemented yet — tracked in
`C:/Users/Administrator/.windsurf/plans/jihuan-hardening-merge-83ae5f.md §2.3`.
