# JiHuan Operator Guide

Production operations cheat-sheet for the hardened build (Phase 1 + 2 + 6a base).
Covers bootstrap, authentication, password management, capacity limits,
audit log, and gRPC usage.

---

## 1. First Boot / Bootstrap Admin Key

When `auth.enabled = true` (the default) and **no admin-scoped API key**
exists in the metadata store, the server mints one on startup and prints it
to **stderr** inside a banner:

```
════════════════════════════════════════════════════════════════════════
  JiHuan bootstrap: no API keys found — creating initial admin key
  key_id = 3c7a1f2d...
  API KEY (save now — it will NEVER be shown again):
      jh_1a2b3c4d5e6f...
  Log in at  http://127.0.0.1:8080/ui/login  with this value as the password.
  You can change the password later at  /ui/settings  → 修改登录密码.
════════════════════════════════════════════════════════════════════════
```

**Recovering a lost admin key**

1. Stop the server.
2. Delete all admin-scoped keys from the metadata store (or temporarily
   revoke them via another admin key).
3. Restart. The bootstrap path detects "0 admin keys" and prints a fresh
   recovery key.

Rationale: the bootstrap logic checks the count of **enabled admin-scoped
keys**, not "no keys at all". That way a user who accidentally revoked
their only admin key can recover by restarting, without wiping other keys.

---

## 2. UI Login & Password Management

- **Login URL:** `http://<host>:8080/ui/login`
- **Credential:** any API key with a `read` scope (admins too). Paste the
  raw key value into the password field.
- **Session:** set as HTTP-only cookie `jh_session`, 7-day TTL. Survives
  browser reload; requires re-login after TTL or manual logout.
- **Change password:** `/ui/settings` → "修改登录密码" card. Enter the
  same password twice (≥ 8 chars, no control characters). Old raw key
  stops authenticating **immediately**; current session stays valid
  because it's bound to the `key_id`, not the hash.

Keep at least one other admin key or write the new password somewhere safe
before changing — otherwise a typo + logged-out session means re-bootstrap.

---

## 3. Authentication Channels

HTTP accepts three credential channels; the server tries them in order:

| Channel | Header / mechanism | Typical user |
|---|---|---|
| Bearer | `Authorization: Bearer <key>` | CLIs, server-to-server |
| API key | `X-API-Key: <key>` | Legacy / simple clients |
| Session | `Cookie: jh_session=<token>` | Browser UI |

gRPC accepts only the first two (`authorization` / `x-api-key` metadata).
The gRPC interceptor validates the key and attaches an `AuthedKey`
extension that service methods use for scope enforcement.

**Scopes**: `read`, `write`, `admin`. `admin` implicitly satisfies
`read` and `write`. New keys default to `["read", "write"]` unless
explicitly overridden at creation.

**Always-exempt routes** (auth bypass, cannot be disabled):
- `/api/auth/login` — required so an un-authenticated browser can log in.

Optional exempt list in `config.toml`:
```toml
[auth]
enabled = true
exempt_routes = ["/healthz", "/readyz", "/api/metrics"]
```

---

## 4. Capacity Management

Optional global cap in `config.toml`:

```toml
[storage]
# Hard cap on physical disk usage under data_dir. None / absent = unlimited.
# Uploads that would push disk usage past this return HTTP 507.
max_storage_bytes = 107374182400   # 100 GiB
```

Behaviour:
- Pre-upload check: server measures `disk_usage_bytes` (walk of `data_dir`)
  and rejects if `used + incoming > cap` (or `used >= cap` for streaming
  uploads where size isn't known up front).
- Response: `507 Insufficient Storage` with `{ available: u64, needed: u64 }`.
- Dashboard shows a usage bar that changes colour at 80% / 95%.

Future (Phase 7.3) will replace this with a richer `[storage.on_full]`
policy engine (`reject` / `auto_evict` / `overwrite_oldest` / `soft_warn`);
`max_storage_bytes` will become a deprecated alias for the global cap with
`policy = reject`.

---

## 5. Block File Sizing (Explainer)

The `storage.block_file_size` parameter is an **upper bound**, not a
fixed allocation. Block files grow by appending compressed chunks; a
block seals when it first exceeds the cap and a new one opens on the
next write. This is why on-disk block sizes are never exact multiples
of `block_file_size` — the last append triggers the seal, and the
boundary falls wherever the chunk happened to cross.

A partially-written ("active") block keeps growing between writes and
is visible on disk with its current size (the `Blocks` admin page uses
`fs::metadata` for unsealed blocks so you see the live figure, not 0).

---

## 6. Audit Log

All security-relevant events are persisted to the `audit` table in redb.
Query via `GET /api/admin/audit` (admin scope required):

| Query param | Meaning | Default |
|---|---|---|
| `since` | Unix seconds lower bound | 0 |
| `until` | Unix seconds upper bound | now |
| `actor` | exact `key_id` match | any |
| `action` | **prefix** match (e.g. `auth.` → all auth events) | any |
| `limit` | cap (max 1000) | 200 |

Recorded actions (as of Phase 2.6):
- `auth.login` / `auth.login_failed` / `auth.logout`
- `auth.change_password`
- `key.create` / `key.delete`

Each event includes `ts`, `actor_key_id` (optional), `actor_ip`
(best-effort from `X-Forwarded-For` or the socket), `action`, `target`,
`result` (`ok` / `denied` / `error`), and `http_status`.

**Retention**: the default is 90 days. Call
`MetadataStore::purge_audit_events_before(cutoff)` from a scheduled task
or future lifecycle executor. (A standalone purge endpoint is not yet
exposed — tracked in roadmap Phase 4.)

---

## 7. Metrics

- Prometheus-exported at `:9090/metrics` (configurable via
  `server.metrics_addr`, set to empty to disable).
- Same-origin proxy at `GET /api/metrics` (admin scope) for UI / CORS-
  restricted scrapers.
- Key series:
  - `jihuan_puts_total` / `jihuan_gets_total` / `jihuan_deletes_total`
  - `jihuan_bytes_written_total` / `jihuan_bytes_read_total`
  - `jihuan_dedup_hits_total`
  - `jihuan_put_duration_seconds` / `jihuan_get_duration_seconds` (histograms)

---

## 8. Quick Verification

```powershell
# Get the bootstrap key from the server's stderr, then:
$KEY = "jh_xxxxxxxxxxxxxxxx"

# Status sanity check
curl -H "X-API-Key: $KEY" http://127.0.0.1:8080/api/status

# Login → session cookie
curl -c cookie.txt -X POST http://127.0.0.1:8080/api/auth/login `
    -H "Content-Type: application/json" `
    -d "{`"key`":`"$KEY`"}"

# Who am I?
curl -b cookie.txt http://127.0.0.1:8080/api/auth/me

# Change password
curl -b cookie.txt -X POST http://127.0.0.1:8080/api/auth/change-password `
    -H "Content-Type: application/json" `
    -d "{`"new_password`":`"s3cret-passphrase`"}"

# Audit log
curl -H "X-API-Key: $KEY" "http://127.0.0.1:8080/api/admin/audit?action=auth.&limit=20"
```

Automated end-to-end check:
```powershell
python check_phase27.py      # requires JIHUAN_ADMIN_KEY env var
```

---

## 9. Recovery after Unclean Shutdown

Prior builds (before the Phase 2.7 data-safety fix) could lose data when the
process was terminated without sealing the active block. If your deployment
shows these symptoms on startup:

- A block appears as `0 B` in the Blocks page
- Some files return HTTP 500 on download
- Log line `Startup: unsealed block referenced by metadata — QUARANTINED to .blk.orphan`

…you have two recovery options.

### Option A — Purge dangling metadata (irreversible)

Use when you've accepted that the quarantined / missing block can't be
recovered, and you just want the dashboard to stop returning 500s for
unrelated files.

```powershell
# Stop the server, then:
$env:JIHUAN_REPAIR = "1"
./jihuan-server --config config/default.toml
# Watch stderr for:
#   JIHUAN_REPAIR=1: purged dangling metadata files_removed=N blocks_removed=M dedup_removed=K
# Then unset and restart normally:
Remove-Item Env:\JIHUAN_REPAIR
```

What it does (strictly safe-on-retry):

1. Scans `data_dir` for `.blk` files that are actually present.
2. For every `blocks` row whose file is missing:
   - Deletes all file records whose chunks reference that block.
   - Deletes dedup entries pointing at that block.
   - Deletes the block row itself.
3. Leaves everything else untouched.

It is **idempotent** — running it repeatedly does nothing once the store
is consistent.

### Option B — Manually reseal a `.blk.orphan` (advanced)

If you want to attempt recovery of the quarantined block, the `.blk.orphan`
file still contains all the raw compressed chunk bytes — only the index
and footer are missing. The block metadata in redb tells you exactly
which chunks are in there (their offsets, sizes, hashes). Rebuilding the
footer is a one-off operation; open a ticket if you need it, and bring:

- The quarantined `.blk.orphan` path
- The output of `SELECT * FROM files` for every file whose chunks point to
  that block (use `GET /api/v1/files` and filter client-side)
- The `blocks` row for the block id

---

## 9b. TLS / HTTPS (Phase 3, v0.5.0)

`jihuan-server` can terminate TLS in-process for **both** the HTTP REST
API and the gRPC endpoint using a single certificate pair. Plaintext
remains the default so upgrades from v0.4.x don't change behaviour
until you opt in.

### 9b.1 Production: static PEM files

Put a certificate chain + private key on disk (obtained from Let's
Encrypt, your internal CA, etc.) and point the config at them:

```toml
[tls]
enabled        = true
cert_path      = "/etc/jihuan/tls/fullchain.pem"
key_path       = "/etc/jihuan/tls/privkey.pem"
auto_selfsigned = false
```

Accepted key formats: **PKCS#8**, **PKCS#1 RSA**, and **SEC1 EC**. The
certificate file may contain intermediate CAs in chain order (leaf
first). Rotation is done out-of-band — after replacing the PEM files
just restart the server (`systemctl restart jihuan`). A hot reload is
tracked under the Phase 4 roadmap.

Also flip `auth.cookie_secure = true` once you're behind TLS, otherwise
browsers silently drop the session cookie during HTTPS redirects.

### 9b.2 Dev: auto-generated self-signed cert

Useful when you want to exercise the HTTPS code path locally without
touching a CA:

```toml
[tls]
enabled         = true
auto_selfsigned = true
```

`jihuan-server` generates a fresh cert for `localhost` / `127.0.0.1`
/ `::1` on every boot (via the `rcgen` crate) and prints its SHA-256
fingerprint to the logs:

```
INFO jihuan_server: TLS enabled for HTTP + gRPC
  fingerprint="ab12…cd34" source="auto-generated self-signed (dev-only, ephemeral)"
```

Clients will see `ERR_CERT_AUTHORITY_INVALID` until they pin the cert
manually — do **not** use this in production. Use `curl -k` or import
the printed fingerprint into your browser's trust store for day-to-day
dev work.

### 9b.3 Reverse proxy (recommended at scale)

For multi-node deployments, SNI routing, OCSP stapling, or staple TLS
termination on a dedicated edge tier, put `jihuan-server` behind
Caddy / nginx / Traefik and leave `tls.enabled = false`:

```caddy
# Caddy
jihuan.example.com {
    reverse_proxy /api/* localhost:8080
    reverse_proxy /ui/*  localhost:8080
}
```

```nginx
# nginx — HTTP + gRPC share the backend so we just need one upstream.
server {
    listen 443 ssl http2;
    server_name jihuan.example.com;

    ssl_certificate     /etc/letsencrypt/live/jihuan/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/jihuan/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        # Large uploads: disable buffering + extend timeouts.
        client_max_body_size    2G;
        proxy_request_buffering off;
        proxy_read_timeout      600s;
    }
}

# gRPC needs its own vhost on a dedicated port.
server {
    listen 50443 ssl http2;
    server_name jihuan.example.com;
    ssl_certificate     /etc/letsencrypt/live/jihuan/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/jihuan/privkey.pem;

    location / {
        grpc_pass grpc://127.0.0.1:50051;
    }
}
```

Behind any proxy, remember to also set `auth.cookie_secure = true` so
the UI session cookie carries the `Secure` flag.

### 9b.4 Smoke test

```powershell
# Static PEM path — fingerprint visible at startup
$env:JIHUAN_CONFIG = "config/default.toml"  # with [tls] enabled = true
./target/release/jihuan-server &
curl -k --cacert /etc/jihuan/tls/fullchain.pem https://localhost:8080/healthz
grpcurl -cacert /etc/jihuan/tls/fullchain.pem \
        -H "authorization: Bearer $env:JH_KEY" \
        localhost:50051 jihuan.admin.v1.AdminService/GetStatus
```

---

## 9c. Reliability surface (Phase 4, v0.5.1)

### 9c.1 Health probes (`/healthz` + `/readyz`)

Two Kubernetes-compatible JSON endpoints served **outside** auth/CORS
so orchestrators can hit them without a credential:

| Path | Meaning | Status |
|---|---|---|
| `GET /healthz` | Liveness — the HTTP task is alive | **200** `{"status":"ok","version":"…"}` (always) |
| `GET /readyz` | Readiness — engine bootstrap complete, listener bound | **200** `{"status":"ready"}` once ready, else **503** `{"status":"starting"}` |

`/readyz` flips from 503 → 200 after `Engine::open()` succeeds, GC +
auto-compaction are spawned, and the HTTP/gRPC listeners are about to
accept. Use `/readyz` to gate rolling deploys; keep `/healthz` for
liveness so k8s restarts hung processes but doesn't flap during the
seconds-long startup window.

Minimal k8s probe snippet:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 8080 }
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
  initialDelaySeconds: 2
  periodSeconds: 3
  failureThreshold: 20  # allow ≤60 s for engine bootstrap
```

### 9c.2 Rate limiting (tower_governor)

Opt-in per-IP token-bucket limiter. Probe, login, and metrics routes
are exempt (they're routed on a separate Router that never sees the
layer) — this guarantees a flooded instance stays reachable for
orchestrators and humans.

```toml
[server.rate_limit]
enabled        = true
per_ip_per_sec = 50   # steady-state refill
burst          = 100  # initial bucket + headroom
```

Over-limit requests get **429 Too Many Requests** with a `Retry-After`
header set by tower_governor. Behind a reverse proxy, set
`proxy_set_header X-Forwarded-For $remote_addr` so the limiter can see
the real client IP — the default key extractor reads the peer socket
address, which would otherwise collapse to the proxy's IP and punish
everyone equally.

### 9c.3 WAL checkpoint + rotation

A background task checkpoints the write-ahead log so it doesn't grow
without bound. Every entry in the WAL is a mirror of a metadata
mutation that has already been committed to redb (per-transaction
fsync), so the log is safe to truncate whenever:

```toml
[storage.wal]
checkpoint_enabled       = true    # master switch; default on
checkpoint_interval_secs = 300     # time trigger (5 min)
max_file_size_mb         = 64      # size trigger
```

The task fires **both** triggers: every `checkpoint_interval_secs` it
wakes and calls `checkpoint_wal`, which truncates the log only if its
on-disk size already exceeds `max_file_size_mb`. Disable by setting
`checkpoint_enabled = false` — useful when an external tool ships the
raw WAL for forensic replay.

Replay after a crash still works correctly: the engine scans every
WAL entry on `Engine::open`, ignoring duplicates that redb already
has. Checkpoint just resets the file to a single `Checkpoint` marker
so future scans are faster.

### 9c.4 Backup / restore (`jihuan export` / `import`)

The `jihuan-cli` binary ships an offline backup workflow. The server
**must be stopped** for both operations — redb takes an exclusive
lock, so the tools refuse to run against a live instance instead of
silently producing a torn snapshot.

**Export** → single `.tar.gz` containing `MANIFEST.json`, `data/`,
`meta/meta.db`, and `wal/`:

```powershell
$env:JIHUAN_CONFIG = "config/default.toml"
jihuan export --config config/default.toml --out ./backup/jh-2026-04-23.tar.gz
```

**Restore** into a fresh host (must point `--config` at a TOML whose
`data_dir` / `meta_dir` / `wal_dir` are either empty or `--force`):

```powershell
jihuan import --in ./backup/jh-2026-04-23.tar.gz --config config/new.toml
# add --force to overwrite a non-empty target
```

**Verify** (safe while the server is running — reads only the leading
manifest, not the full archive):

```powershell
jihuan backup-verify --in ./backup/jh-2026-04-23.tar.gz
```

Archive layout:

```
MANIFEST.json        # version, producer, created_at, counts
data/<...>.blk       # every block file, tree preserved
meta/meta.db         # redb store
wal/*.wal            # WAL segments (usually small after checkpointing)
```

### 9c.5 Block-integrity scrub (`POST /api/admin/scrub`)

Walks every sealed block, verifies the header/footer CRC32s, then
re-reads every chunk with per-chunk CRC32 check + content-hash
cross-check (when `storage.hash_algorithm ≠ none`). Returns a JSON
`ScrubReport`:

```json
{
  "blocks_checked": 42,
  "blocks_failed": 0,
  "chunks_checked": 15832,
  "chunks_corrupt": 0,
  "elapsed_ms": 418,
  "errors": []
}
```

**Status is always 200**, even when corruption is found — consumers
should inspect `chunks_corrupt` and `errors[]` rather than the HTTP
status. `errors[]` is capped at 100 entries; full error output lands
in the server log at `WARN` level with `block_id` / `chunk_hash`.

Expected cadence: weekly in production, or before every backup. The
scan is I/O-bound — expect ~1 GB/s throughput on a warm SSD. An
automatic scheduler (`gc.scrub_interval_hours`) is tracked under the
Phase 4.5 follow-up.

```powershell
curl -X POST -H "authorization: Bearer $env:JH_KEY" https://localhost:8080/api/admin/scrub
```

---

## 9d. Partition ACL enforcement (Phase 7.2)

Each `User` and `ServiceAccount` carries an
`allowed_partitions: Option<Vec<String>>` allow-list. Semantics:

| Value | Effect |
|---|---|
| `None` | No restriction (legacy / pre-IAM; also the admin default). |
| `Some([])` | Explicitly zero partitions → **denies every request** including uploads (MinIO-style "SA pinned to no bucket"). |
| `Some(["<id>", …])` | Target partition's `partition_id.to_string()` must appear. |

Enforcement points (all return **403** on mismatch):

- `POST /api/v1/files` — target = current time partition (server computes).
- `GET /api/v1/files/:id` — target = file's own `partition_id`.
- `GET /api/v1/files/:id/meta` — same as download.
- `DELETE /api/v1/files/:id` — same as download.
- `GET /api/v1/files` — silently filters results; `total` reflects
  the caller's view, not the raw store.

The child-grant check at `POST /api/auth/keys` refuses to mint a SA
whose `allowed_partitions` exceeds the parent user's set — the
intersection is enforced before persistence.

---

## 10. Known Limits / Next Steps

| Area | Status | Owner phase |
|---|---|---|
| TLS | **implemented** (Phase 3; in-process rustls for HTTP + gRPC, static PEM or dev auto-selfsigned) | v0.5.0 |
| Graceful shutdown | **implemented** (SIGINT/SIGTERM → seal block + fsync WAL, 30 s drain) | — |
| Health probes | **implemented** (`/healthz` always 200, `/readyz` 503→200) | v0.5.1 |
| Rate limiting | **implemented** (tower_governor, per-IP, probes/login exempt) | v0.5.1 |
| Block scrub | **implemented** (`POST /api/admin/scrub`, CRC + content-hash) | v0.5.1 |
| Scrub scheduler | **implemented** (`storage.scrub_interval_hours`, weekly default) | v0.5.1 |
| Partition ACL enforcement | **implemented** (`allowed_partitions`; upload/get/delete/stat/list) | v0.5.1 |
| WAL checkpoint/rotation | **implemented** (periodic + size-based truncate; default on) | v0.5.1 |
| Backup/restore CLI | **implemented** (`jihuan export/import/backup-verify`, offline tar.gz) | v0.5.1 |
| Audit retention auto-purge | partial (purge fn exists, no scheduler) | Phase 4 |
| Performance optimisation (P1/P2/P3/P5) | planned | Phase 6b continuation |
| UI audit-log page | planned | Phase 2.7 follow-up |
| gRPC session cookies | **not planned** (API keys only) | — |

See `C:/Users/Administrator/.windsurf/plans/jihuan-hardening-merge-83ae5f.md`
for the full roadmap and priorities.
