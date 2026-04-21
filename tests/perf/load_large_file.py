#!/usr/bin/env python3
"""Phase 6a large-file streaming test.

Single-threaded (or tunable) streaming upload of one very large file. The
point is NOT throughput numbers — it's to prove:

  1. Memory never blows up (verified externally; watch RSS).
     Phase 6b-P1 unified put_stream so RAM ≤ chunk_size.
  2. Block rollover works mid-upload (expected when size > block_file_size).
  3. Download round-trips byte-identical (verified via sha256 of the
     streamed payload vs. the downloaded payload).

Stdlib only.

Usage:
    python tests/perf/load_large_file.py --smoke           # 128 MiB quick check
    python tests/perf/load_large_file.py --size-mb 2048    # 2 GiB
"""
from __future__ import annotations

import argparse
import hashlib
import io
import os
import sys
import time
import urllib.error
import urllib.request


CHUNK = 4 * 1024 * 1024  # 4 MiB per read; server's chunk_size default is also 4 MiB


class ChunkedPayload:
    """A file-like object that yields `size_bytes` of pseudo-random data
    without ever materialising the whole buffer in RAM. Also records a
    sha256 of the exact bytes delivered, so the caller can compare the
    server's returned bytes against it after download."""

    def __init__(self, size_bytes: int, seed: int = 0xC0FFEE):
        self.remaining = size_bytes
        self.rng = hashlib.sha256(seed.to_bytes(8, "little"))
        self.digest = hashlib.sha256()
        self._buf = b""

    def read(self, n: int = -1) -> bytes:
        if n < 0 or n > self.remaining:
            n = self.remaining
        if n == 0:
            return b""
        # Generate a deterministic pseudo-random stream by repeatedly
        # hashing the running state. Not crypto-grade; good enough to
        # keep zstd from compressing everything to ~0 bytes.
        out = bytearray()
        while len(out) < n:
            if not self._buf:
                self.rng.update(b".")
                self._buf = self.rng.digest()
            take = min(n - len(out), len(self._buf))
            out.extend(self._buf[:take])
            self._buf = self._buf[take:]
        data = bytes(out)
        self.digest.update(data)
        self.remaining -= len(data)
        return data


def multipart_streaming_upload(base: str, key: str, name: str,
                                size_bytes: int) -> tuple[int, float, str]:
    """Uploads `size_bytes` as a multipart form-data POST without buffering
    the whole body in RAM. Returns (status, elapsed_s, sha256_of_payload)."""
    boundary = "JhLargeBound"
    preamble = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{name}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode()
    epilogue = f"\r\n--{boundary}--\r\n".encode()
    content_length = len(preamble) + size_bytes + len(epilogue)

    payload = ChunkedPayload(size_bytes)

    def body_iter() -> "io.BytesIO | bytes":
        # urllib needs a file-like with .read(size). Compose preamble +
        # payload + epilogue via a tiny stateful class.
        class Stream:
            def __init__(self):
                self.stage = 0  # 0 = preamble, 1 = payload, 2 = epilogue, 3 = done
                self.pre_off = 0
                self.epi_off = 0

            def read(self, n: int = -1) -> bytes:
                if n is None or n < 0:
                    n = CHUNK
                if self.stage == 0:
                    remaining = len(preamble) - self.pre_off
                    take = min(n, remaining)
                    chunk = preamble[self.pre_off: self.pre_off + take]
                    self.pre_off += take
                    if self.pre_off >= len(preamble):
                        self.stage = 1
                    return chunk
                if self.stage == 1:
                    chunk = payload.read(n)
                    if payload.remaining == 0:
                        self.stage = 2
                    return chunk
                if self.stage == 2:
                    remaining = len(epilogue) - self.epi_off
                    take = min(n, remaining)
                    chunk = epilogue[self.epi_off: self.epi_off + take]
                    self.epi_off += take
                    if self.epi_off >= len(epilogue):
                        self.stage = 3
                    return chunk
                return b""
        return Stream()

    t0 = time.perf_counter()
    req = urllib.request.Request(
        base + "/api/v1/files",
        data=body_iter(),
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(content_length),
            "X-API-Key": key,
        },
    )
    try:
        with urllib.request.urlopen(req) as r:
            r.read()
            return r.status, time.perf_counter() - t0, payload.digest.hexdigest()
    except urllib.error.HTTPError as e:
        return e.code, time.perf_counter() - t0, payload.digest.hexdigest()


def list_files(base: str, key: str) -> list[dict]:
    req = urllib.request.Request(base + "/api/v1/files?limit=1000",
                                  headers={"X-API-Key": key})
    with urllib.request.urlopen(req) as r:
        import json
        return json.loads(r.read()).get("files", [])


def download_sha256(base: str, key: str, file_id: str) -> tuple[int, str, int]:
    """Streaming download; returns (status, sha256_hex, bytes_read)."""
    req = urllib.request.Request(
        base + f"/api/v1/files/{file_id}",
        headers={"X-API-Key": key},
    )
    h = hashlib.sha256()
    size = 0
    with urllib.request.urlopen(req) as r:
        status = r.status
        while True:
            buf = r.read(CHUNK)
            if not buf:
                break
            h.update(buf)
            size += len(buf)
    return status, h.hexdigest(), size


def fmt_bytes(n: float) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{u}"
        n /= 1024
    return f"{n:.2f}PB"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("JIHUAN_BASE", "http://127.0.0.1:8080"))
    ap.add_argument("--key", default=os.environ.get("JIHUAN_ADMIN_KEY"))
    ap.add_argument("--size-mb", type=int, default=512, help="payload size in MiB")
    ap.add_argument("--name", default=None)
    ap.add_argument("--no-verify", action="store_true",
                    help="skip the download + sha256 verification step")
    ap.add_argument("--smoke", action="store_true", help="128 MiB quick check")
    args = ap.parse_args()

    if args.smoke:
        args.size_mb = 128

    if not args.key:
        print("ERROR: pass --key or set JIHUAN_ADMIN_KEY", file=sys.stderr)
        return 2

    size = args.size_mb * 1024 * 1024
    name = args.name or f"large-{args.size_mb}mb.bin"
    print(f"uploading {fmt_bytes(size)} as {name}...")

    status, el_up, sent_sha = multipart_streaming_upload(args.base, args.key, name, size)
    throughput = size / el_up if el_up > 0 else 0
    print(f"  upload : status={status}  elapsed={el_up:.1f}s  "
          f"throughput={fmt_bytes(throughput)}/s")
    print(f"           sent sha256={sent_sha}")
    if not (200 <= status < 300):
        print("upload failed — aborting")
        return 1

    if args.no_verify:
        return 0

    # Look up the just-uploaded file's id. We don't get it back from the
    # upload response body (multipart route returns a small JSON envelope
    # but this script stays stdlib-clean); simplest: find by name.
    files = list_files(args.base, args.key)
    match = next((f for f in files if f.get("name") == name), None)
    if not match:
        print(f"could not find uploaded file by name={name}", file=sys.stderr)
        return 1
    file_id = match["file_id"]
    print(f"  file_id={file_id}")

    print("downloading to verify...")
    t0 = time.perf_counter()
    status, dl_sha, dl_size = download_sha256(args.base, args.key, file_id)
    el_dl = time.perf_counter() - t0
    print(f"  download: status={status}  elapsed={el_dl:.1f}s  "
          f"throughput={fmt_bytes(dl_size / el_dl if el_dl else 0)}/s  "
          f"size={fmt_bytes(dl_size)}")
    print(f"            recv sha256={dl_sha}")

    if dl_size != size:
        print(f"SIZE MISMATCH: sent {size} B, received {dl_size} B", file=sys.stderr)
        return 1
    if dl_sha != sent_sha:
        print("SHA256 MISMATCH — data corruption!", file=sys.stderr)
        return 1
    print("OK: byte-identical round-trip")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
