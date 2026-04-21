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

### load_small_files.py

Small-file / dedup stress. Pure PUT workload with a bounded pool of
duplicate payloads so dedup hits are measurable. Targets the metadata
+ chunk-index path that dominates wall time at tiny file sizes.

```powershell
python tests/perf/load_small_files.py --smoke                          # 15s @ 4 workers
python tests/perf/load_small_files.py --concurrency 32 --duration 60 `
    --dup-ratio 0.7 --size-min 512 --size-max 65536
```

The script fetches `/api/status` before and after the run and prints
the dedup-ratio delta so you can verify the dedup path actually fires.

### load_large_file.py

Single-threaded streaming upload of one very large file, followed by a
sha256-verified download. Proves `put_stream` really is O(chunk_size)
RAM and that block rollover works mid-upload.

```powershell
python tests/perf/load_large_file.py --smoke            # 128 MiB
python tests/perf/load_large_file.py --size-mb 2048     # 2 GiB, full verify
python tests/perf/load_large_file.py --size-mb 512 --no-verify
```

### load_range.py

Hammers one seed file with random `Range: bytes=a-b` requests under
concurrency and byte-compares every response against the locally-retained
seed. Any 206 whose payload differs from `seed[a..=b]` is flagged as a
bad-bytes failure.

```powershell
python tests/perf/load_range.py --smoke
python tests/perf/load_range.py --size-mb 64 --concurrency 16 --duration 30
```

### load_concurrent_dedup.py

N writers race uploading **identical** content. Baseline for future P2
(`active_writer` sharding). Reports logical-vs-physical bytes delta from
`/api/status`, which lets you verify the dedup index kicked in.

```powershell
python tests/perf/load_concurrent_dedup.py --smoke
python tests/perf/load_concurrent_dedup.py --concurrency 64 --duration 30 --size-kb 64
```

Planned (next session):
- `load_mixed_dedup.py` — mix of unique / duplicate content at a fixed ratio

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
