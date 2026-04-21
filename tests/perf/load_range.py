#!/usr/bin/env python3
"""Phase 6a Range-request stress test.

Uploads one large seed file, then hammers it with random `Range: bytes=a-b`
GETs under concurrency. Validates every returned slice byte-by-byte against
the locally-retained seed so any off-by-one in the engine's range splitter
shows up immediately.

Targets `@crates/jihuan-server/src/http/files.rs::download_file` (Range
parsing) + `@crates/jihuan-core/src/engine.rs::get_range`.

Stdlib only.

Usage:
    python tests/perf/load_range.py --smoke
    python tests/perf/load_range.py --size-mb 64 --concurrency 16 --duration 30
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import os
import random
import statistics as _  # silence unused-import lint without keeping the name
import sys
import time
import urllib.error
import urllib.request


def upload(base: str, key: str, name: str, content: bytes) -> tuple[int, str | None]:
    boundary = "JhRangeBound"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        base + "/api/v1/files",
        data=body,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-API-Key": key,
        },
    )
    try:
        with urllib.request.urlopen(req) as r:
            import json
            js = json.loads(r.read())
            return r.status, js.get("file_id")
    except urllib.error.HTTPError as e:
        return e.code, None


def range_get(base: str, key: str, file_id: str, start: int, end: int
              ) -> tuple[int, bytes, float]:
    """Inclusive-inclusive HTTP Range, same semantics as RFC 7233."""
    t0 = time.perf_counter()
    req = urllib.request.Request(
        base + f"/api/v1/files/{file_id}",
        headers={
            "X-API-Key": key,
            "Range": f"bytes={start}-{end}",
        },
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read(), time.perf_counter() - t0
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b""
        return e.code, body, time.perf_counter() - t0


def worker(base: str, key: str, deadline: float, file_id: str, seed: bytes,
           max_range: int) -> list[tuple[int, float, bool]]:
    """Returns list of (status, elapsed_s, byte_correct)."""
    rng = random.Random(os.urandom(8))
    results: list[tuple[int, float, bool]] = []
    size = len(seed)
    while time.perf_counter() < deadline:
        a = rng.randint(0, size - 1)
        b = rng.randint(a, min(size - 1, a + max_range - 1))
        status, body, el = range_get(base, key, file_id, a, b)
        ok = status == 206 and body == seed[a: b + 1]
        results.append((status, el, ok))
    return results


def pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round(p / 100.0 * (len(vals) - 1)))))
    return vals[k]


def fmt_bytes(n: float) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("JIHUAN_BASE", "http://127.0.0.1:8080"))
    ap.add_argument("--key", default=os.environ.get("JIHUAN_ADMIN_KEY"))
    ap.add_argument("--size-mb", type=int, default=16, help="seed file size in MiB")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--duration", type=float, default=20.0, help="seconds")
    ap.add_argument("--max-range", type=int, default=64 * 1024,
                    help="max range length in bytes (default 64 KiB)")
    ap.add_argument("--smoke", action="store_true", help="4 MiB, 10s, 4 workers")
    args = ap.parse_args()

    if args.smoke:
        args.size_mb = 4
        args.concurrency = 4
        args.duration = 10.0

    if not args.key:
        print("ERROR: pass --key or set JIHUAN_ADMIN_KEY", file=sys.stderr)
        return 2

    size = args.size_mb * 1024 * 1024
    seed = os.urandom(size)
    name = f"range-seed-{hashlib.sha1(seed[:256]).hexdigest()[:8]}.bin"
    print(f"uploading {fmt_bytes(size)} seed...")
    status, file_id = upload(args.base, args.key, name, seed)
    if not file_id or not (200 <= status < 300):
        print(f"seed upload failed: status={status}", file=sys.stderr)
        return 1
    print(f"  file_id={file_id}")

    deadline = time.perf_counter() + args.duration
    print(f"running for {args.duration}s with {args.concurrency} workers, "
          f"max_range={fmt_bytes(args.max_range)}")

    t0 = time.perf_counter()
    all_results: list[tuple[int, float, bool]] = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [
            ex.submit(worker, args.base, args.key, deadline, file_id, seed, args.max_range)
            for _ in range(args.concurrency)
        ]
        for fut in cf.as_completed(futures):
            all_results.extend(fut.result())
    elapsed = time.perf_counter() - t0

    ok = sum(1 for r in all_results if r[2])
    two06 = sum(1 for r in all_results if r[0] == 206)
    bad_bytes = sum(1 for r in all_results if r[0] == 206 and not r[2])
    lats = [r[1] for r in all_results]
    qps = len(all_results) / elapsed if elapsed > 0 else 0

    print(f"\nelapsed {elapsed:.1f}s, total {len(all_results)} range GETs")
    print(f"  QPS          : {qps:>8.1f}")
    print(f"  206 Partial  : {two06}")
    print(f"  bad bytes    : {bad_bytes}  (206 but payload != seed[a..=b])")
    print(f"  correct      : {ok}")
    print(f"  latency p50  : {pct(lats, 50) * 1000:>6.2f} ms")
    print(f"  latency p95  : {pct(lats, 95) * 1000:>6.2f} ms")
    print(f"  latency p99  : {pct(lats, 99) * 1000:>6.2f} ms")

    codes: dict[int, int] = {}
    for r in all_results:
        codes[r[0]] = codes.get(r[0], 0) + 1
    print("\nHTTP status breakdown:")
    for code in sorted(codes):
        label = "Partial-OK" if code == 206 else ("OK" if 200 <= code < 300 else "FAIL")
        print(f"  {code:>4}  {codes[code]:>6}  ({label})")

    return 0 if bad_bytes == 0 and ok == len(all_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
