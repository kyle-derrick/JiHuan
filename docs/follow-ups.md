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

---

## Phase 7 IAM enforcement gaps

| Item | Why it matters | Size |
|---|---|---|

---

## Phase 6b performance

| Item | Why it matters | Size |
|---|---|---|
| **P2: shard `active_writer` across chunk-size buckets** | Already **partly shipped** — rollover fsync happens outside the lock so sealing a 1 GiB block no longer blocks the next writer. Full per-bucket sharding **deferred** pending a benchmark showing append-path contention. Append itself is microsecond-scale on a buffered writer, so this is speculative until measured. | deferred |
| **P5: compression level auto-tune** | Default zstd level is a fixed constant. Profile-based autotune (file size / type) could trade 5-10% latency for 20% size on large media. **Intentionally deferred** — shipping naive heuristics (e.g. "small chunks get level 1, big chunks get level 5") risks regressing operators who deliberately picked their level via `compression_level`. Needs real profile data + per-chunk decision API before it's worth doing. | deferred |

---

## Documentation / UX drift

| Item | Why it matters |
|---|---|
| `docs/operator-guide.md` § 10 table is now 95% populated — drop the "Phase 4 owner" column once 7.2 ships |

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
| Audit log auto-purge (verified: already runs inline on each GC tick when `auth.audit_retention_days > 0`; tested in `gc::tests`) | pre-v0.5.1 (docs drift only) |
| Phase 4.4 follow-up: WAL multi-segment rotation (`storage.wal.keep_old_logs`, default 3) | v0.5.1 |
| Phase 4.3 follow-up: tower_governor Smart-IP / XFF extractor (`server.rate_limit.trust_forwarded_for`) | v0.5.1 |
| Phase 6b P1: `put_bytes` unified through `put_stream` (verified already in place; byte-slice keeps precise quota check then delegates to streaming path) | pre-v0.5.1 (docs drift only) |
| Phase 6b P2 (minimal): rollover `finish()` + `sync_all()` released from `active_writer` lock | pre-v0.5.1 (docs drift only) |
| Phase 6b P3: rayon-parallel scrub at per-block granularity (`Engine::scrub`) | v0.5.1 |
| Phase 7 UI: `/ui/audit` page already shipped — filters by action / actor / time range | pre-v0.5.1 (docs drift only) |
| Phase 4.6 follow-up: incremental `jihuan export --against <parent.tar.gz>` (skip unchanged files by `(path, size)`; MANIFEST records `ParentRef`) | v0.5.1 |
| `config/reliability.toml` preset with all Phase 4 knobs pinned explicitly | v0.5.1 |
| README gRPC auth stance: API Key only; cookies are HTTP-UI only | v0.5.1 |
| Chinese design doc (`docs/小文件存储系统开发设计文档.md`): §12 v0.4→v0.5 change log appendix added | v0.5.1 |
