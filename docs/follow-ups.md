# JiHuan — Follow-ups & Tech Debt

A living ledger of known-but-not-yet-done work. Each entry names a
**concrete owner phase**, the **why**, and a **suggested size**. When
you ship an item, move its row to the "Recently closed" section at the
bottom and cite the commit.

Last audited: 2026-04-23 (after Phase 4 closeout).

---

## Phase 4 hardening residuals

| Item | Why it matters | Size |
|---|---|---|
| **WAL multi-segment rotation** (`keep_old_logs`) | Current `checkpoint_wal` truncates in place. For forensic replay we want `jihuan.wal.1/.2/.3` kept side-by-side, oldest pruned once `keep_old_logs` is exceeded. | ~2-3 h (new segment file naming + replay path) |
| **Backup incremental / delta** | `jihuan export` is full-snapshot only; big stores re-ship every byte. Delta requires a manifest of source block ids + timestamps and skip-writes on import. | ~1 day |
| **tower_governor XFF extractor** | Default uses peer IP; behind a reverse proxy every client collapses to the proxy's IP. Swap to `SmartIpKeyExtractor` or similar and pick up `X-Forwarded-For` / `X-Real-IP`. | ~1 h + docs |
| **Audit log auto-purge scheduler** | `audit::purge_older_than` exists; no tick calls it. Drives unbounded growth on long-lived instances. | ~30 lines |

---

## Phase 7 IAM enforcement gaps

| Item | Why it matters | Size |
|---|---|---|
| **UI audit-log page** | `GET /api/admin/audit` returns structured records but the SPA has no reader. Operators have to `curl` to see lockouts / SA use. | ~1 day |
| **gRPC session cookies** | Intentionally **not planned** — gRPC = API keys only. Delete this row once README/operator-guide confirms the stance. | docs-only |

---

## Phase 6b performance

| Item | Why it matters | Size |
|---|---|---|
| **P1: unify `put_bytes` through `put_stream`** | Currently two code paths; `put_bytes` can't benefit from streaming quota checks. Straightforward inline. | ~1 h |
| **P2: shard `active_writer` across chunk-size buckets** | Single `Mutex<Option<ActiveBlock>>` pins all uploads; large-file writers block small-file traffic. | ~4-6 h |
| **P3: parallel chunk hashing** | Per-chunk SHA256 is the current bottleneck for dedup-heavy workloads. Move into rayon pool with tuned chunk group size. | ~half-day |
| **P5: compression level auto-tune** | Default zstd level is a fixed constant. Profile-based autotune (file size / type) could trade 5-10% latency for 20% size on large media. | exploratory |

---

## Documentation / UX drift

| Item | Why it matters |
|---|---|
| `docs/operator-guide.md` § 10 table is now 95% populated — drop the "Phase 4 owner" column once 7.2 ships |
| Chinese developer doc (`docs/小文件存储系统开发设计文档.md`) has not been updated since v0.4 — lags English operator guide by several phases |
| `config/default.toml` now has 4 Phase-4 stanzas; consider a dedicated `reliability.toml` preset so first-time users get sane defaults out of the box |

---

## Recently closed (stop worrying about these)

| Item | Shipped in |
|---|---|
| Phase 4.2 `/healthz` + `/readyz` | v0.5.1 |
| Phase 4.3 tower-governor rate limit | v0.5.1 |
| Phase 4.4 WAL checkpoint + rotation (size + time) | v0.5.1 |
| Phase 4.5 `POST /api/admin/scrub` | v0.5.1 |
| Phase 4.6 `jihuan export/import/backup-verify` | v0.5.1 |
| Phase 4.5 follow-up: scrub scheduler (`storage.scrub_interval_hours`) | v0.5.1 |
| Phase 7.2 `allowed_partitions` enforcement (upload/get/delete/stat/list) | v0.5.1 |
