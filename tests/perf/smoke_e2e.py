#!/usr/bin/env python3
"""One-shot end-to-end smoke test.

Designed to run AGAINST AN ALREADY-RUNNING jihuan-server. Does not launch
the binary itself — that's the orchestrator's job.

Checks:
  1. /healthz returns 200
  2. /api/status returns 200 and sane JSON
  3. multipart upload of a 512 KiB random payload → 200
  4. list files → can find our upload
  5. download by file_id → byte-identical
  6. delete → /api/v1/files/{id} returns 204 and subsequent GET → 404

Exit code is 0 on full success; any failure prints a one-line reason and
returns non-zero.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request


def _req(method: str, url: str, key: str, body: bytes | None = None,
         ctype: str | None = None) -> tuple[int, bytes, dict[str, str]]:
    req = urllib.request.Request(url, data=body, method=method)
    if key:
        req.add_header("X-API-Key", key)
    if ctype:
        req.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read() if hasattr(e, "read") else b"", dict(e.headers or {})


def check_healthz(base: str) -> bool:
    status, _, _ = _req("GET", f"{base}/healthz", "")
    if status != 200:
        print(f"FAIL: /healthz returned {status}")
        return False
    print("OK: /healthz → 200")
    return True


def check_status(base: str, key: str) -> bool:
    status, body, _ = _req("GET", f"{base}/api/status", key)
    if status != 200:
        print(f"FAIL: /api/status returned {status}")
        return False
    try:
        js = json.loads(body)
    except Exception as e:
        print(f"FAIL: /api/status body not JSON: {e}")
        return False
    for field in ("file_count", "block_count", "disk_usage_bytes", "version"):
        if field not in js:
            print(f"FAIL: /api/status missing field {field}")
            return False
    print(f"OK: /api/status → files={js['file_count']} blocks={js['block_count']} "
          f"disk={js['disk_usage_bytes']} ver={js['version']}")
    return True


def upload_multipart(base: str, key: str, name: str, content: bytes
                     ) -> tuple[int, str | None]:
    boundary = "JhSmokeBound"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    status, resp, _ = _req("POST", f"{base}/api/v1/files", key, body,
                           f"multipart/form-data; boundary={boundary}")
    if status != 200:
        print(f"FAIL: upload → {status}: {resp[:200]!r}")
        return status, None
    try:
        return status, json.loads(resp).get("file_id")
    except Exception:
        return status, None


def download(base: str, key: str, file_id: str) -> tuple[int, bytes]:
    return _req("GET", f"{base}/api/v1/files/{file_id}", key)[:2]


def list_files(base: str, key: str) -> tuple[int, list[dict]]:
    status, body, _ = _req("GET", f"{base}/api/v1/files?limit=1000", key)
    if status != 200:
        return status, []
    return status, json.loads(body).get("files", [])


def delete_file(base: str, key: str, file_id: str) -> int:
    return _req("DELETE", f"{base}/api/v1/files/{file_id}", key)[0]


def main() -> int:
    base = os.environ.get("JIHUAN_BASE", "http://127.0.0.1:8080")
    key = os.environ.get("JIHUAN_ADMIN_KEY", "")
    if not key:
        print("ERROR: set JIHUAN_ADMIN_KEY", file=sys.stderr)
        return 2

    # Wait up to 15 s for the server to accept requests.
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            s, _, _ = _req("GET", f"{base}/healthz", "")
            if s == 200:
                break
        except Exception:
            pass
        time.sleep(0.3)
    else:
        print("FAIL: server never reached /healthz within 15s")
        return 1

    ok = True
    ok &= check_healthz(base)
    ok &= check_status(base, key)

    # Roundtrip
    payload = os.urandom(512 * 1024)
    name = f"smoke-{int(time.time())}.bin"
    up_status, file_id = upload_multipart(base, key, name, payload)
    if not file_id:
        return 1
    print(f"OK: upload → 200 file_id={file_id}")

    ls_status, files = list_files(base, key)
    if not any(f.get("file_id") == file_id for f in files):
        print(f"FAIL: uploaded file not visible in list (status={ls_status})")
        return 1
    print(f"OK: list_files → {len(files)} entries, ours is present")

    dl_status, dl_body = download(base, key, file_id)
    if dl_status != 200:
        print(f"FAIL: download → {dl_status}")
        return 1
    if dl_body != payload:
        print(f"FAIL: download returned {len(dl_body)} B, expected {len(payload)} B; "
              f"bytes mismatch")
        return 1
    print(f"OK: download → 200, {len(dl_body)} B, byte-identical")

    del_status = delete_file(base, key, file_id)
    if del_status not in (200, 204):
        print(f"FAIL: delete → {del_status}")
        return 1
    print(f"OK: delete → {del_status}")

    gone_status, _ = download(base, key, file_id)
    if gone_status != 404:
        print(f"FAIL: deleted file still returns {gone_status}")
        return 1
    print("OK: deleted file → 404")

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
