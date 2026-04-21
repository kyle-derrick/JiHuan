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

## 10. Known Limits / Next Steps

| Area | Status | Owner phase |
|---|---|---|
| TLS | planned | Phase 3 |
| Graceful shutdown | **implemented** (SIGINT/SIGTERM → seal block + fsync WAL, 30 s drain) | — |
| Health probes | planned | Phase 4 |
| Rate limiting | planned | Phase 4 |
| Audit retention auto-purge | partial (purge fn exists, no scheduler) | Phase 4 |
| Performance optimisation (P1/P2/P3/P5) | planned | Phase 6b continuation |
| UI audit-log page | planned | Phase 2.7 follow-up |
| gRPC session cookies | **not planned** (API keys only) | — |

See `C:/Users/Administrator/.windsurf/plans/jihuan-hardening-merge-83ae5f.md`
for the full roadmap and priorities.
