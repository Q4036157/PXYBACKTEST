"""Measure verified Tick decoding only; this is not an EA parity benchmark."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import psutil

from app.mt5_tick_loader import iter_mt5_ticks, read_mt5_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark immutable MT5 Tick decoding")
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-ms", required=True, type=int)
    parser.add_argument("--end-ms", required=True, type=int)
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot)
    manifest = read_mt5_snapshot(snapshot_path, expected_sha256=args.manifest_sha256)
    process = psutil.Process()
    peak_rss = process.memory_info().rss
    count = 0
    first_ms = last_ms = None
    started = time.perf_counter()
    for tick in iter_mt5_ticks(
        data_root=args.data_root,
        manifest=manifest,
        symbol=args.symbol,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
    ):
        time_ms = tick["time_msc"]
        if first_ms is None:
            first_ms = time_ms
        last_ms = time_ms
        count += 1
        if count % 10_000 == 0:
            peak_rss = max(peak_rss, process.memory_info().rss)
    elapsed = time.perf_counter() - started
    peak_rss = max(peak_rss, process.memory_info().rss)
    print(json.dumps({
        "scope": "verified_tick_decode_only",
        "snapshot_id": manifest.get("snapshot_id"),
        "manifest_sha256": args.manifest_sha256.lower(),
        "symbol": args.symbol,
        "tick_count": count,
        "first_time_msc": first_ms,
        "last_time_msc": last_ms,
        "elapsed_seconds": round(elapsed, 3),
        "ticks_per_second": round(count / elapsed) if elapsed else None,
        "peak_rss_mb": round(peak_rss / 1024**2, 1),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
