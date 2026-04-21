#!/usr/bin/env python3
"""Phase 6a `active_writer`-lock baseline.

N writers race to upload **identical** payloads. Two things we measure:

  1. Dedup hit ratio — should approach 1.0 as concurrency grows (same
     content hash → after the first writer lands, everyone else short-
     circuits at the dedup index without touching the block file).
  2. PUT latency distribution — the residual contention after dedup kicks
     in lets us characterise the raw `active_writer` + `meta` cost. This
     is the baseline that future P2 (active_writer sharding) must beat.

Stdlib only.

Usage:
    python tests/perf/load_concurrent_dedup.py --smoke
    python tests/perf/load_concurrent_dedup.py --concurrency 64 --duration 30 --size-kb 64
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request


def upload(base: str, key: str, name: str, content: bytes) -> tuple[int, float]:
    boundary = "JhDedupBound"
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


def worker(base: str, key: str, deadline: float, payload: bytes, worker_id: int
           ) -> list[tuple[int, float]]:
    results: list[tuple[int, float]] = []
    i = 0
    while time.perf_counter() < deadline:
        i += 1
        # Distinct filenames (→ distinct FileMeta rows) but identical
        # content → every chunk after the first upload is a dedup hit.
        name = f"dedup-{worker_id}-{i}.bin"
        results.append(upload(base, key, name, payload))
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
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--size-kb", type=int, default=64,
                    help="payload size in KiB (default 64, same as one small chunk)")
    ap.add_argument("--smoke", action="store_true", help="4 workers, 10s")
    args = ap.parse_args()

    if args.smoke:
        args.concurrency = 4
        args.duration = 10.0

    if not args.key:
        print("ERROR: pass --key or set JIHUAN_ADMIN_KEY", file=sys.stderr)
        return 2

    rng = random.Random(0xDECAFBAD)
    payload = bytes(rng.randint(0, 255) for _ in range(args.size_kb * 1024))
    print(f"shared payload: {fmt_bytes(len(payload))}")

    status_before = fetch_status(args.base, args.key)
    deadline = time.perf_counter() + args.duration
    print(f"running for {args.duration}s with {args.concurrency} workers, "
          f"all uploading the SAME {fmt_bytes(len(payload))} payload")

    t0 = time.perf_counter()
    all_results: list[tuple[int, float]] = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [
            ex.submit(worker, args.base, args.key, deadline, payload, wid)
            for wid in range(args.concurrency)
        ]
        for fut in cf.as_completed(futures):
            all_results.extend(fut.result())
    elapsed = time.perf_counter() - t0
    status_after = fetch_status(args.base, args.key)

    lats = [r[1] for r in all_results]
    ok = sum(1 for r in all_results if 200 <= r[0] < 300)
    qps = len(all_results) / elapsed if elapsed > 0 else 0
    bytes_sent = args.size_kb * 1024 * len(all_results)

    print(f"\nelapsed {elapsed:.1f}s, {len(all_results)} PUTs, ok={ok}")
    print(f"  QPS          : {qps:>8.1f}")
    print(f"  logical sent : {fmt_bytes(bytes_sent)}")
    print(f"  latency p50  : {pct(lats, 50) * 1000:>6.2f} ms")
    print(f"  latency p95  : {pct(lats, 95) * 1000:>6.2f} ms")
    print(f"  latency p99  : {pct(lats, 99) * 1000:>6.2f} ms")
    print(f"  latency max  : {max(lats) * 1000 if lats else 0:>6.2f} ms")

    if status_before and status_after:
        disk_delta = (status_after.get("disk_usage_bytes", 0)
                      - status_before.get("disk_usage_bytes", 0))
        files_delta = (status_after.get("file_count", 0)
                       - status_before.get("file_count", 0))
        dedup_ratio = 1.0 - (disk_delta / bytes_sent) if bytes_sent else 0
        print("\ndedup evidence:")
        print(f"  files   : +{files_delta}")
        print(f"  disk    : +{fmt_bytes(disk_delta)}  (vs {fmt_bytes(bytes_sent)} logical)")
        print(f"  inferred dedup rate : {dedup_ratio:.3f}")
        print(f"  server dedup_ratio  : {status_after.get('dedup_ratio', 0):.3f}")

    codes: dict[int, int] = {}
    for r in all_results:
        codes[r[0]] = codes.get(r[0], 0) + 1
    print("\nHTTP status breakdown:")
    for code in sorted(codes):
        label = "OK" if 200 <= code < 300 else ("NETWORK-ERR" if code == 0 else "FAIL")
        print(f"  {code:>4}  {codes[code]:>6}  ({label})")

    return 0 if ok == len(all_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
