#!/usr/bin/env python3
"""Phase 2.7 verification: session auth, password change, audit log, capacity.

Requires env var ``JIHUAN_ADMIN_KEY`` — the bootstrap admin key printed by
the server on first start (or any existing admin-scoped key). Run against a
live server with ``auth.enabled = true`` (the default).
"""
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("JIHUAN_BASE", "http://127.0.0.1:8080")
KEY = os.environ.get("JIHUAN_ADMIN_KEY")

if not KEY:
    print("ERROR: set JIHUAN_ADMIN_KEY to an admin-scoped API key", file=sys.stderr)
    sys.exit(2)

ok = True
def check(name, cond, detail=""):
    global ok
    mark = "PASS" if cond else "FAIL"
    if not cond:
        ok = False
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))


def req(method, path, body=None, key=None, opener=None, headers=None):
    url = BASE + path
    hdrs = dict(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    if key:
        hdrs["X-API-Key"] = key
    r = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    open_ = opener.open if opener else urllib.request.urlopen
    try:
        with open_(r) as resp:
            b = resp.read()
            try:
                return resp.status, json.loads(b)
            except Exception:
                return resp.status, b.decode(errors="replace")
    except urllib.error.HTTPError as e:
        b = e.read()
        try:
            return e.code, json.loads(b)
        except Exception:
            return e.code, b.decode(errors="replace")


print("=== JiHuan Phase 2.7 Verification ===\n")

# ── Session cookie flow ───────────────────────────────────────────────────────
jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

s, r = req("POST", "/api/auth/login", {"key": KEY}, opener=opener)
check("POST /api/auth/login → 200", s == 200)
check("  response has key_id", isinstance(r, dict) and "key_id" in r)
check("  jh_session cookie set", any(c.name == "jh_session" for c in jar))

s, r = req("GET", "/api/auth/me", opener=opener)
check("GET /api/auth/me → 200", s == 200)
check("  includes scopes", isinstance(r, dict) and "scopes" in r)
check("  admin scope", "admin" in r.get("scopes", []))

# ── /api/auth/login must be reachable WITHOUT credentials ────────────────────
s, _ = req("POST", "/api/auth/login", {"key": "bogus"})
check("login reachable unauthenticated → 401 (not 401+routed-off)", s == 401)

# ── Status: logical + physical bytes ──────────────────────────────────────────
s, r = req("GET", "/api/status", key=KEY)
check("GET /api/status → 200", s == 200)
check("  logical_bytes field", "logical_bytes" in r)
check("  disk_usage_bytes field", "disk_usage_bytes" in r)
check("  max_storage_bytes field", "max_storage_bytes" in r)

# ── Audit log: login should have left at least one event ──────────────────────
s, r = req("GET", "/api/admin/audit?action=auth.&limit=50", key=KEY)
check("GET /api/admin/audit → 200", s == 200)
events = r.get("events", []) if isinstance(r, dict) else []
check("  at least one auth.login event", any(e["action"] == "auth.login" for e in events))

# ── Password change (then restore) ────────────────────────────────────────────
TMP_PW = "phase27-temporary-0x"
s, _ = req("POST", "/api/auth/change-password", {"new_password": TMP_PW}, opener=opener)
check("POST /api/auth/change-password → 204", s == 204)

# Old session still valid (cookie is bound to key_id)
s, _ = req("GET", "/api/auth/me", opener=opener)
check("  existing session survives password change → 200", s == 200)

# Old raw key no longer authenticates
s, _ = req("GET", "/api/status", key=KEY)
check("  old raw key → 401 after change", s == 401)

# New password authenticates
s, _ = req("GET", "/api/status", key=TMP_PW)
check("  new password authenticates → 200", s == 200)

# Restore
s, _ = req("POST", "/api/auth/change-password", {"new_password": KEY}, opener=opener)
check("  restore original key → 204", s == 204)

# ── Audit log shows change_password ──────────────────────────────────────────
s, r = req("GET", "/api/admin/audit?action=auth.change_password&limit=10", key=KEY)
events = r.get("events", []) if isinstance(r, dict) else []
check("  audit contains auth.change_password", len(events) >= 1)

# ── Capacity quota: reading back the configured cap ───────────────────────────
s, r = req("GET", "/api/config", key=KEY)
check("GET /api/config → 200", s == 200)
mx = r.get("storage", {}).get("max_storage_bytes", None) if isinstance(r, dict) else None
check("  config.storage.max_storage_bytes present (None or integer)",
      mx is None or isinstance(mx, int))

# ── Logout ────────────────────────────────────────────────────────────────────
s, _ = req("POST", "/api/auth/logout", opener=opener)
check("POST /api/auth/logout → 204", s == 204)

s, _ = req("GET", "/api/auth/me", opener=opener)
check("  me after logout → 401", s == 401)

print()
print("=" * 44)
print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")
sys.exit(0 if ok else 1)
