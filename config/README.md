# JiHuan Configuration Presets

Drop-in TOML profiles tuned for common scenarios. Pass the chosen file via
`jihuan-server --config config/<profile>.toml` or copy it to
`./jihuan.toml` (the default lookup path).

## Profile matrix

| Profile           | Target workload                                     | Files  | Compression | Auto-compact | Notes                                   |
|-------------------|-----------------------------------------------------|--------|-------------|--------------|-----------------------------------------|
| `default.toml`    | Mixed / unknown                                     | any    | zstd:1      | on (1 h)     | Balanced baseline. Start here.          |
| `speed.toml`      | Hot cache, low-latency gets/puts                    | any    | none        | **off**      | MD5, no compression, no verify.         |
| `space.toml`      | Cold tier, strong compression                       | medium | zstd:9      | on (1 h)     | 2 GB blocks, 80% threshold.             |
| `small-files.toml`| Thumbnails, icons, tiny documents (< 10 KB each)    | tiny   | zstd:3      | on (1 h)     | 2 MB chunks, 60% threshold, cross-block merge quickly collapses partial blocks. |
| `large-files.toml`| Videos, backups, ISOs (> 100 MB each)               | huge   | zstd:1      | on (1 h)     | 8 MB chunks, 2 GB blocks.               |
| `archive.toml`    | WORM / backup / evidence store (rare deletes)       | huge   | zstd:19     | on (1 day)   | 4 GB blocks, compact daily only.        |
| `dedup-heavy.toml`| Container images, VM snapshots, CI caches           | varied | zstd:3      | on (30 min)  | 1 MB chunks, frequent compaction.       |

## Key v0.4.5 knobs

All compaction behaviour is controlled by six `[storage]` keys:

| Key                                | Meaning                                                         |
|------------------------------------|-----------------------------------------------------------------|
| `auto_compact_enabled`             | Master switch for the background loop.                          |
| `auto_compact_threshold`           | A sealed block is a *candidate* when `live_bytes / size < threshold`. |
| `auto_compact_min_size_bytes`      | Skip blocks below this size (no point rewriting trivial data).  |
| `auto_compact_every_gc_ticks`      | Run every N GC passes (actual cadence = `gc_interval_secs × N`).|
| `auto_compact_min_benefit_bytes`   | Skip a compaction group if projected bytes-saved < this floor.  |
| `auto_compact_disk_headroom_bytes` | Require `available(data_dir) ≥ group_live_bytes + headroom` before writing the new block. 0 disables the check. |

### How the scanner decides what to compact

1. **List sealed, non-pinned blocks** (active writer is never compacted).
2. **Compute `live_bytes`** per block as the sum of compressed sizes of
   the *unique* chunk hashes still referenced by any live file. Dedup
   references contribute once. This matches the `utilization` column
   shown in the UI at `/ui/blocks`.
3. **Keep candidates** where `live_bytes / size < threshold`
   AND `size ≥ min_size_bytes`.
4. **Bin-pack** candidates (ascending by `live_bytes`) into groups whose
   combined live bytes fit within one `block_file_size`.
5. **Skip groups** whose projected savings fall below
   `min_benefit_bytes`, or whose new block wouldn't fit with
   `disk_headroom_bytes` free.
6. **Merge** each surviving group into one new block via a single redb
   transaction, then delete the old block files.

### Tuning cheatsheet

| Symptom                                              | Knob to turn                                            |
|------------------------------------------------------|---------------------------------------------------------|
| Compaction runs but saves only a few bytes           | Raise `auto_compact_min_benefit_bytes`.                 |
| Disk fills up during compaction                      | Raise `auto_compact_disk_headroom_bytes`.               |
| Too many half-empty blocks after bulk deletes        | Lower `auto_compact_threshold` (e.g. 0.6 → 0.8).        |
| Background CPU spikes interfering with requests      | Raise `auto_compact_every_gc_ticks` (less often) or disable. |
| A specific block is stuck at low utilisation         | `POST /api/admin/compact {"block_id": "<id>"}` — bypasses all filters. |

### Deployment tip

For new deployments treat the non-default profiles as *starting points* —
observe the `jihuan_bytes_written_total` counter and the block list
utilisation column for a week, then adjust `threshold` / `min_benefit_bytes`
to match your actual deletion pattern. `threshold` is a pre-filter (fast
but may miss edge cases); `min_benefit_bytes` is the real savings gate.

---

## Troubleshooting with logs (v0.4.5)

`jihuan-server` uses [`tracing`](https://docs.rs/tracing) with the standard
`RUST_LOG` env var honoured via `EnvFilter`. There is **no separate
"debug mode" toggle** — the standard Rust conventions give you per-module
control at any verbosity level. Every example below assumes the
`RUST_LOG=…` prefix on your launch command (or a `docker run -e` / k8s
env var in production).

### Recipe: watch a compaction pass

```bash
RUST_LOG=info,jihuan_core::engine=debug ./jihuan-server --config ./default.toml
```

You'll see one log line per:

- **Candidate block** considered by the scanner — shows `block_id`,
  `size`, `live_bytes`, computed `utilization`, and whether it was
  `kept = true/false` relative to the `threshold`.
- **Group** the candidates were bin-packed into, with `old_sum`,
  `live_sum`, `benefit`, and the `min_benefit` floor that gated it.
- **Disk-headroom** rejection (at `warn!`): shows `avail` vs. `required`
  if a group would have exceeded the free-space safety rail.
- **Merge completion** (at `info!`): `group_size`, `new_block_id`,
  and whether extra references slipped in during the redb commit.

Together these reproduce every decision the scanner made, so you can
replay them against a staging dataset with the same config.

### Recipe: watch the GC loop

```bash
RUST_LOG=info,jihuan_core::gc=debug ./jihuan-server --config ./default.toml
```

Each tick produces:

- `GC: tick starting` with a monotonic `tick` counter and the configured
  `interval_secs` — useful for correlating with Prometheus scrape gaps.
- `GC: beginning reclaim pass` with `unreferenced` block count +
  `pinned_count` (= active writer count).
- Per-block: either `GC: skipping pinned active block` (safe), or
  `GC: deleted block file` (reclaim), or `GC: failed to delete block
  file` (requires attention).
- Completion line tagged with the same `tick` number, `blocks_deleted`,
  `bytes_reclaimed`, `duration_ms`.

### Recipe: startup crash recovery forensics

```bash
RUST_LOG=info,jihuan_core=warn ./jihuan-server ...
```

This keeps the normal floor quiet but surfaces the two startup
conditions most operators need to know about:

- `Startup: unsealed block referenced by metadata — QUARANTINED` →
  a `.blk.orphan` was just produced; run `jihuan reseal-orphan` after
  stopping the server (see `reseal-orphan --help`).
- `Crash recovery: block-file cleanup` summary with
  `deleted_orphans` / `quarantined_referenced` counts.

### Recipe: trace a single file read

```bash
RUST_LOG=info,jihuan_core::engine=trace ./jihuan-server ...
```

Heavy — use only to reproduce a specific bug. Produces a chunk-level
log per `get_bytes` / `get_range` call.

### Other targets

| Target                    | What it covers                                |
|---------------------------|-----------------------------------------------|
| `jihuan_core::engine`     | put / get / compact / repair / reseal         |
| `jihuan_core::gc`         | background GC loop + startup cleanup          |
| `jihuan_core::wal`        | WAL append / replay                           |
| `jihuan_core::metadata`   | redb reads and writes                         |
| `jihuan_server::http`     | axum handlers, error mapping                  |
| `tower_http::trace`       | access log (already on at `info` by default)  |
| `tonic`                   | gRPC request/response lines                   |

### One-liners to keep in your runbook

```bash
# Is compaction making progress but savings are trivial?
RUST_LOG=info,jihuan_core::engine=debug …
# Does the UI show blocks stuck at low util but nothing gets compacted?
# → look for "group skipped (benefit < min_benefit)"
RUST_LOG=info,jihuan_core::engine=debug …

# Is disk filling up unexpectedly?
# → look for "compact_low_utilization: group skipped due to disk headroom"
RUST_LOG=info,jihuan_core::engine=debug …

# Why didn't GC reclaim this block?
RUST_LOG=info,jihuan_core::gc=debug …
# → "GC: skipping pinned active block" means it's the live writer;
#   run `jihuan seal` or wait for the next seal.
```

### Why no dynamic `/api/admin/log-level` endpoint?

Restarting `jihuan-server` is cheap (~1 s on a warm metadata DB) and
the existing graceful shutdown path always seals the active block, so
`RUST_LOG=… kubectl rollout restart` is the right production workflow.
A runtime reload endpoint is tracked as a future enhancement but is
intentionally not part of v0.4.5.

---

## Custom `file_id` rules (v0.4.6)

`POST /api/v1/files` (and the gRPC `PutFile` RPC) accept an optional
caller-supplied `file_id`. If you omit it the server mints a UUID as
before. The field is validated and **NFC-normalised** before storage.

### Accepted characters

- Any valid UTF-8 (ASCII, CJK, emoji-free text, etc.)
- S3 key style paths are allowed: `images/2024/cat.jpg`
- Chinese is fine: `订单-2024/发票.pdf`

### Rejected inputs (→ HTTP 400)

| Pattern                    | Why                                                |
|----------------------------|----------------------------------------------------|
| empty string               | must have at least one byte                        |
| any `c.is_control()` char  | tabs / newlines / NUL / DEL would break HTTP paths |
| byte length > 1024         | redb key inflation; CJK ~340 chars is plenty       |
| leading `/`                | reserved for future filesystem-export features     |
| trailing `/`               | ambiguous with directory prefix                    |
| consecutive `//`           | same                                               |
| standalone `.` or `..` segment | path-traversal defence                         |

### NFC normalisation

macOS APIs often hand you **NFD** (decomposed) strings; Windows /
Linux typically return **NFC** (composed). The same visible
`"café"` can therefore be two different byte sequences depending on
the client OS. JiHuan normalises every `file_id` to NFC **on the way
in**, so clients can upload under `"café"` from a Mac and retrieve
under `"café"` from Windows without knowing about Unicode internals.

### Conflict policy

Supply `on_conflict` in the multipart form or gRPC `PutFileInfo` to
control what happens when your `file_id` already exists. Default is
**`error`** (explicit is safer than silent).

| Value        | Behaviour                                                  | HTTP status |
|--------------|------------------------------------------------------------|-------------|
| `error` (default) | reject with `AlreadyExists`                           | **409**     |
| `skip`       | keep existing record, return its metadata + `outcome=skipped` | 200       |
| `overwrite`  | atomically replace with the uploaded bytes, return `outcome=overwritten` | 200 |

Success responses carry an `outcome` field (`created` / `skipped` /
`overwritten`) so clients that need to distinguish the three paths
can match on the body rather than inspecting what they sent. Error
responses (409 / 400 / 507) use the standard `{error, code}` shape —
no `outcome` on 4xx.

### URL encoding

`GET` / `DELETE /api/v1/files/{file_id}` expects the id in the path,
so non-ASCII or reserved characters must be percent-encoded by the
client. cURL examples:

```bash
# Upload with a Chinese id and skip-on-conflict.
curl -X POST http://localhost:8080/api/v1/files \
     -H "X-API-Key: $JIHUAN_KEY" \
     -F "file_id=订单-2024/发票.pdf" \
     -F "on_conflict=skip" \
     -F "file=@invoice.pdf"

# Download the same file (client must percent-encode the id).
curl -H "X-API-Key: $JIHUAN_KEY" \
     "http://localhost:8080/api/v1/files/%E8%AE%A2%E5%8D%95-2024%2F%E5%8F%91%E7%A5%A8.pdf" \
     -o invoice.pdf

# Overwrite an existing record.
curl -X POST http://localhost:8080/api/v1/files \
     -H "X-API-Key: $JIHUAN_KEY" \
     -F "file_id=orders/42" \
     -F "on_conflict=overwrite" \
     -F "file=@new-receipt.pdf"
```

### Idempotent overwrite

Overwriting with **identical bytes** is cheap: deduplication means
every chunk ref-count nets to zero across old and new, so the swap
commit rewrites the `FileMeta` row but touches no block data.
Uploading the same content under `on_conflict=overwrite` is
effectively free and safe to do in retry loops.
