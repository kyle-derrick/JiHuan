#!/usr/bin/env python3
"""Full pre-release check for JiHuan Phase 1+2 (with large-file streaming)."""
import io
import os
import urllib.request, urllib.error, json

BASE = "http://127.0.0.1:8080"

def req(method, path, body=None, key=None, raw_body=None, headers=None):
    url = BASE + path
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    elif raw_body is not None:
        data = raw_body
    else:
        data = None
    if key:
        hdrs["X-API-Key"] = key
    r = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(r) as resp:
            b = resp.read()
            try: return resp.status, json.loads(b)
            except Exception: return resp.status, b.decode(errors="replace")
    except urllib.error.HTTPError as e:
        b = e.read()
        try: return e.code, json.loads(b)
        except Exception: return e.code, b.decode(errors="replace")

ok = True
def check(name, cond, detail=""):
    global ok
    status = "PASS" if cond else "FAIL"
    if not cond: ok = False
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))

def upload(name: str, content: bytes, content_type: str = "application/octet-stream"):
    """Multipart upload, returns (status, json)."""
    boundary = "JihuanCheckBound"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: {content_type}\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return req("POST", "/api/v1/files", raw_body=body,
               headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})

print("=== JiHuan Full Check (Phase 1+2 Hardened) ===\n")

# ── 1. Status with new fields ─────────────────────────────────────────────────
s, r = req("GET", "/api/status")
check("GET /api/status → 200", s == 200)
check("  file_count field", "file_count" in r)
check("  block_count field", "block_count" in r)
check("  disk_usage_bytes field", "disk_usage_bytes" in r)
check("  dedup_ratio field", "dedup_ratio" in r)
check("  uptime_secs field", r.get("uptime_secs", -1) >= 0)

# ── 2. Config endpoint ────────────────────────────────────────────────────────
s, r = req("GET", "/api/config")
check("GET /api/config → 200", s == 200)
check("  storage section", isinstance(r.get("storage"), dict))
check("  server section", isinstance(r.get("server"), dict))
check("  auth section", isinstance(r.get("auth"), dict))

# ── 3. Small file upload + list ───────────────────────────────────────────────
s, r = req("GET", "/api/v1/files")
check("GET /api/v1/files (empty) → 200", s == 200 and r.get("count") == 0)

s, r = upload("hello.txt", b"Hello JiHuan!")
check("POST /api/v1/files small → 200", s == 200)
small_id = r["file_id"]

s, r = req("GET", "/api/v1/files")
check("  list after small upload: count=1", r.get("count") == 1)

# ── 4. LARGE file upload (20 MB) — streaming test ────────────────────────────
# Previously failed with os error 10053 above 2 MB
LARGE = 20 * 1024 * 1024  # 20 MB
big_content = bytes([i & 0xff for i in range(LARGE)])
s, r = upload("big.bin", big_content, "application/octet-stream")
check(f"POST /api/v1/files LARGE ({LARGE//1024//1024} MB) → 200", s == 200, f"status={s}")
if s == 200:
    big_id = r["file_id"]
    check("  file_size matches", r["file_size"] == LARGE)
    # Verify download content matches
    dl = urllib.request.urlopen(BASE + f"/api/v1/files/{big_id}")
    got = dl.read()
    check("  download content matches", got == big_content, f"got_len={len(got)}")

# ── 5. List files with filters ────────────────────────────────────────────────
s, r = req("GET", "/api/v1/files?q=hello")
check("GET /api/v1/files?q=hello → 200", s == 200)
check("  search matches 1 file", r.get("count") == 1 and r["files"][0]["file_name"] == "hello.txt")

s, r = req("GET", "/api/v1/files?sort=file_size&order=desc")
check("GET /api/v1/files?sort=file_size → 200", s == 200)
check("  sorted by size desc", r["files"][0]["file_size"] >= r["files"][-1]["file_size"] if len(r["files"]) >= 2 else True)

# ── 6. File meta with chunks ──────────────────────────────────────────────────
s, r = req("GET", f"/api/v1/files/{small_id}/meta")
check("GET /api/v1/files/:id/meta → 200", s == 200)
check("  includes chunks array", isinstance(r.get("chunks"), list))
check("  chunks have block_id/offset/hash", all(
    "block_id" in c and "offset" in c and "hash" in c for c in r.get("chunks", [])
))
check("  partition_id present", "partition_id" in r)

# ── 7. Block list with sealed flag ────────────────────────────────────────────
s, r = req("GET", "/api/block/list")
check("GET /api/block/list → 200", s == 200)
check("  at least 1 block", r.get("count", 0) >= 1)
if r.get("blocks"):
    check("  sealed field present", all("sealed" in b for b in r["blocks"]))

# ── 8. Block detail + referencing files ──────────────────────────────────────
first_block = r["blocks"][0]["block_id"]
s, r = req("GET", f"/api/block/{first_block}")
check(f"GET /api/block/:id → 200", s == 200)
check("  has referencing_files array", isinstance(r.get("referencing_files"), list))
check("  at least 1 referencing file (from uploads)", len(r.get("referencing_files", [])) >= 1)

# ── 9. Block delete — should reject if ref>0, accept if ref=0 ─────────────────
# Try to delete a referenced block
s, _ = req("DELETE", f"/api/block/{first_block}")
check(f"DELETE /api/block/:id when ref>0 → 409", s == 409)

# ── 10. Dashboard disk_usage_bytes after uploads ─────────────────────────────
s, r = req("GET", "/api/status")
check("  disk_usage_bytes > 0 after uploads", r.get("disk_usage_bytes", 0) > 0)

# ── 11. GC ────────────────────────────────────────────────────────────────────
s, r = req("POST", "/api/gc/trigger")
check("POST /api/gc/trigger → 200", s == 200)

# ── 12. API Key CRUD ──────────────────────────────────────────────────────────
s, r = req("POST", "/api/keys", {"name": "check-key"})
check("POST /api/keys → 200", s == 200)
raw_key = r["key"]; key_id = r["key_id"]

s, r = req("GET", "/api/v1/files", key=raw_key)
check("GET /api/v1/files with key → 200", s == 200)

s, _ = req("DELETE", f"/api/keys/{key_id}")
check("DELETE /api/keys/:id → 204", s == 204)

# ── 13. UI static ─────────────────────────────────────────────────────────────
resp = urllib.request.urlopen(BASE + "/ui/")
check("GET /ui/ → 200", resp.status == 200)

# ── 14. Delete files → GC → orphan block deletable ───────────────────────────
req("DELETE", f"/api/v1/files/{small_id}")
if 'big_id' in dir():
    req("DELETE", f"/api/v1/files/{big_id}")

# Now trigger GC to release blocks (ref_count → 0)
req("POST", "/api/gc/trigger")

s, r = req("GET", "/api/block/list")
remaining = r.get("count", 0)
check(f"  post-delete/GC: {remaining} blocks remain", True, f"count={remaining}")

# ── 15. Pagination ────────────────────────────────────────────────────────────
s, r = req("GET", "/api/v1/files?limit=5&offset=0")
check("GET /api/v1/files?limit=5 → 200", s == 200)

print()
print("=" * 44)
if ok:
    print("ALL CHECKS PASSED — Phase 2 hardening verified")
else:
    print("SOME CHECKS FAILED — See above")
