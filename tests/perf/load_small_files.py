#!/usr/bin/env python3
"""Phase 6a small-file stress test.

Targets the workload JiHuan sees under heavy metadata load: tens of
thousands of tiny files (512B - 64KB) uploaded concurrently, with a
configurable duplicate ratio so we also exercise the dedup + chunk-index
paths. Pure PUT — no GETs — so latency numbers reflect the write path
alone.

Reports:
  * PUT QPS, p50/p95/p99 latency
  * Observed dedup ratio via /api/status (should approach
    `--dup-ratio` once dedup settles)

Stdlib only; runs on the same Python as load_mixed.py.

Usage:
    python tests/perf/load_small_files.py --smoke
    python tests/perf/load_small_files.py --concurrency 32 --duration 60 \
        --dup-ratio 0.7 --size-min 512 --size-max 65536
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request


# ── Shared dup pool ────────────────────────────────────────────────────────

def build_dup_pool(count: int, size_min: int, size_max: int, rng: random.Random) -> list[bytes]:
    """Pre-generate a fixed pool of payloads that workers will pick from
    whenever they want to emit a duplicate upload. Keeping the pool small
    (default 16) guarantees dedup hits at the chunk level."""
    return [os.urandom(rng.randint(size_min, size_max)) for _ in range(count)]


# ── HTTP helpers ───────────────────────────────────────────────────────────

def upload(base: str, key: str, name: str, content: bytes) -> tuple[int, float]:
    boundary = "JhSmallBound"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    t0 = time.perf_counter()
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
            r.read()
            return r.status, time.perf_counter() - t0
    except urllib.error.HTTPError as e:
        return e.code, time.perf_counter() - t0
    except urllib.error.URLError:
        return 0, time.perf_counter() - t0


def fetch_status(base: str, key: str) -> dict | None:
    req = urllib.request.Request(base + "/api/status", headers={"X-API-Key": key})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except Exception:
        return None


# ── Worker ─────────────────────────────────────────────────────────────────

def worker(base: str, key: str, deadline: float, dup_pool: list[bytes],
           dup_ratio: float, size_min: int, size_max: int
           ) -> list[tuple[int, float, int]]:
    """Returns list of (status, elapsed_s, bytes_sent)."""
    results: list[tuple[int, float, int]] = []
    rng = random.Random(os.urandom(8))
    i = 0
    while time.perf_counter() < deadline:
        if dup_pool and rng.random() < dup_ratio:
            content = rng.choice(dup_pool)
        else:
            content = os.urandom(rng.randint(size_min, size_max))
        i += 1
        # Include a hash prefix in the filename so we can cross-reference
        # logs later without needing to re-read the server's file list.
        fname = f"small-{hashlib.sha1(content[:32]).hexdigest()[:8]}-{i}.bin"
        status, el = upload(base, key, fname, content)
        results.append((status, el, len(content)))
    return results


# ── Stats ──────────────────────────────────────────────────────────────────

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


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("JIHUAN_BASE", "http://127.0.0.1:8080"))
    ap.add_argument("--key", default=os.environ.get("JIHUAN_ADMIN_KEY"))
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--duration", type=float, default=30.0, help="seconds")
    ap.add_argument("--size-min", type=int, default=512)
    ap.add_argument("--size-max", type=int, default=64 * 1024)
    ap.add_argument("--dup-ratio", type=float, default=0.5,
                    help="probability each upload is a duplicate of a pool payload")
    ap.add_argument("--dup-pool", type=int, default=16,
                    help="number of distinct payloads in the duplicate pool")
    ap.add_argument("--smoke", action="store_true", help="15s @ 4 concurrency")
    args = ap.parse_args()

    if args.smoke:
        args.concurrency = 4
        args.duration = 15.0

    if not args.key:
        print("ERROR: pass --key or set JIHUAN_ADMIN_KEY", file=sys.stderr)
        return 2

    master_rng = random.Random(0xABCDEF)
    pool = build_dup_pool(args.dup_pool, args.size_min, args.size_max, master_rng)
    print(f"dup pool: {len(pool)} payloads "
          f"({fmt_bytes(sum(len(p) for p in pool))} total)")

    status_before = fetch_status(args.base, args.key)
    deadline = time.perf_counter() + args.duration
    print(f"running for {args.duration}s with {args.concurrency} workers, "
          f"dup_ratio={args.dup_ratio:.0%}, size {args.size_min}-{args.size_max} B")

    t0 = time.perf_counter()
    all_results: list[tuple[int, float, int]] = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [
            ex.submit(worker, args.base, args.key, deadline,
                      pool, args.dup_ratio, args.size_min, args.size_max)
            for _ in range(args.concurrency)
        ]
        for fut in cf.as_completed(futures):
            all_results.extend(fut.result())
    elapsed = time.perf_counter() - t0
    status_after = fetch_status(args.base, args.key)

    lats = [r[1] for r in all_results]
    ok = sum(1 for r in all_results if 200 <= r[0] < 300)
    bytes_sent = sum(r[2] for r in all_results)
    qps = len(all_results) / elapsed if elapsed > 0 else 0
    throughput = bytes_sent / elapsed if elapsed > 0 else 0

    print(f"\nelapsed {elapsed:.1f}s, total {len(all_results)} PUTs, "
          f"ok={ok}, failed={len(all_results) - ok}")
    print(f"  QPS          : {qps:>8.1f}")
    print(f"  throughput   : {fmt_bytes(throughput)}/s ({fmt_bytes(bytes_sent)} sent)")
    print(f"  latency p50  : {pct(lats, 50) * 1000:>6.2f} ms")
    print(f"  latency p95  : {pct(lats, 95) * 1000:>6.2f} ms")
    print(f"  latency p99  : {pct(lats, 99) * 1000:>6.2f} ms")
    print(f"  latency max  : {max(lats) * 1000 if lats else 0:>6.2f} ms")

    if status_before and status_after:
        print("\ndedup_ratio (from /api/status):")
        print(f"  before : {status_before.get('dedup_ratio', 0):.3f}")
        print(f"  after  : {status_after.get('dedup_ratio', 0):.3f}")
        files_delta = status_after.get("file_count", 0) - status_before.get("file_count", 0)
        disk_delta = status_after.get("disk_usage_bytes", 0) - status_before.get("disk_usage_bytes", 0)
        print(f"  files  : +{files_delta}")
        print(f"  disk   : +{fmt_bytes(disk_delta)} (vs {fmt_bytes(bytes_sent)} sent)")

    # Break status-code breakdown out separately so CI can eyeball 5xx spikes.
    codes: dict[int, int] = {}
    for r in all_results:
        codes[r[0]] = codes.get(r[0], 0) + 1
    print("\nHTTP status breakdown:")
    for code in sorted(codes):
        label = "OK" if 200 <= code < 300 else "NETWORK-ERR" if code == 0 else "FAIL"
        print(f"  {code:>4}  {codes[code]:>6}  ({label})")

    return 0 if ok == len(all_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
