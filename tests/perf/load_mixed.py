#!/usr/bin/env python3
"""Phase 6a mixed-workload load test.

Quick smoke / baseline runner. Not a full rig — sufficient to catch
regressions and populate the dashboard under non-trivial concurrency.
Intentionally uses only the standard library so it runs anywhere.

Usage:
    python tests/perf/load_mixed.py --help
    python tests/perf/load_mixed.py --smoke      # 30s, low concurrency
    python tests/perf/load_mixed.py --concurrency 32 --duration 60
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import os
import random
import statistics as st
import sys
import time
import urllib.error
import urllib.request


def upload(base: str, key: str, name: str, content: bytes) -> tuple[int, float]:
    boundary = "JhPerfBound"
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


def download(base: str, key: str, file_id: str) -> tuple[int, float]:
    t0 = time.perf_counter()
    req = urllib.request.Request(
        base + f"/api/v1/files/{file_id}",
        headers={"X-API-Key": key},
    )
    try:
        with urllib.request.urlopen(req) as r:
            r.read()
            return r.status, time.perf_counter() - t0
    except urllib.error.HTTPError as e:
        return e.code, time.perf_counter() - t0


def worker(base: str, key: str, deadline: float, mix_write: float,
           size_min: int, size_max: int, known_ids: list[str]) -> list[tuple[str, int, float]]:
    """Returns list of (op, status, elapsed_s)."""
    results: list[tuple[str, int, float]] = []
    rng = random.Random(os.urandom(8))
    while time.perf_counter() < deadline:
        if not known_ids or rng.random() < mix_write:
            sz = rng.randint(size_min, size_max)
            content = os.urandom(sz)
            status, el = upload(base, key, f"perf-{rng.random()}.bin", content)
            results.append(("PUT", status, el))
        else:
            fid = rng.choice(known_ids)
            status, el = download(base, key, fid)
            results.append(("GET", status, el))
    return results


def seed_ids(base: str, key: str, n: int, size: int) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        content = os.urandom(size)
        status, _ = upload(base, key, f"seed-{i}.bin", content)
        if status == 200:
            # Best-effort: list files to find seeded ids.
            pass
    # Pull the list to get ids
    req = urllib.request.Request(base + "/api/v1/files?limit=1000",
                                 headers={"X-API-Key": key})
    with urllib.request.urlopen(req) as r:
        data = r.read()
        import json
        js = json.loads(data)
        for f in js.get("files", []):
            ids.append(f["file_id"])
    return ids


def pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round(p / 100.0 * (len(vals) - 1)))))
    return vals[k]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("JIHUAN_BASE", "http://127.0.0.1:8080"))
    ap.add_argument("--key", default=os.environ.get("JIHUAN_ADMIN_KEY"))
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--duration", type=float, default=30.0, help="seconds")
    ap.add_argument("--write-ratio", type=float, default=0.3)
    ap.add_argument("--size-min", type=int, default=4 * 1024)
    ap.add_argument("--size-max", type=int, default=256 * 1024)
    ap.add_argument("--seed", type=int, default=50, help="files to seed before starting")
    ap.add_argument("--smoke", action="store_true", help="30s @ 4 concurrency")
    args = ap.parse_args()

    if args.smoke:
        args.concurrency = 4
        args.duration = 30.0

    if not args.key:
        print("ERROR: pass --key or set JIHUAN_ADMIN_KEY", file=sys.stderr)
        return 2

    print(f"seeding {args.seed} files...")
    seed_size = (args.size_min + args.size_max) // 2
    ids = seed_ids(args.base, args.key, args.seed, seed_size)
    print(f"  seeded, {len(ids)} known file_ids")

    deadline = time.perf_counter() + args.duration
    print(f"running for {args.duration}s with {args.concurrency} workers, "
          f"write ratio {args.write_ratio:.1%}, size {args.size_min}-{args.size_max} bytes")

    t0 = time.perf_counter()
    all_results: list[tuple[str, int, float]] = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [
            ex.submit(worker, args.base, args.key, deadline,
                      args.write_ratio, args.size_min, args.size_max, list(ids))
            for _ in range(args.concurrency)
        ]
        for fut in cf.as_completed(futures):
            all_results.extend(fut.result())
    elapsed = time.perf_counter() - t0

    puts = [r for r in all_results if r[0] == "PUT"]
    gets = [r for r in all_results if r[0] == "GET"]

    def summarise(label: str, rs: list[tuple[str, int, float]]) -> None:
        if not rs:
            print(f"  {label}: 0 ops")
            return
        lats = [r[2] for r in rs]
        ok = sum(1 for r in rs if 200 <= r[1] < 300)
        qps = len(rs) / elapsed
        print(f"  {label}: {len(rs):>6} ops  "
              f"ok={ok:>6}  "
              f"qps={qps:>8.1f}  "
              f"p50={pct(lats,50)*1000:>6.1f}ms  "
              f"p95={pct(lats,95)*1000:>6.1f}ms  "
              f"p99={pct(lats,99)*1000:>6.1f}ms")

    print(f"\nelapsed {elapsed:.1f}s, total {len(all_results)} ops")
    summarise("PUT", puts)
    summarise("GET", gets)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
