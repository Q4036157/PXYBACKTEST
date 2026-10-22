"""通过 CCXT 的官方公共行情适配器冻结分钟成交价与标记价。"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import time


def retry_network(call, network_error, attempts: int = 4):
    for attempt in range(attempts):
        try:
            return call()
        except network_error:
            if attempt + 1 == attempts:
                raise
            time.sleep(2 ** attempt)


def collect_venue(venue: str, symbols: list[str], start: int, end: int,
                  completed: set[tuple[str, str]] | None = None) -> dict:
    import ccxt

    exchange = getattr(ccxt, {"binance": "binanceusdm", "okx": "okx", "gate": "gate"}[venue])(
        {"enableRateLimit": True, "timeout": 15_000, "options": {"defaultType": "swap"}})
    result = {"venue": venue, "datasets": [], "errors": []}
    try:
        retry_network(exchange.load_markets, ccxt.NetworkError)
        for symbol in symbols:
            base = symbol.split("_SWAP_")[0].removesuffix("USDT")
            instrument = f"{base}/USDT:USDT"
            for kind in ("trade", "mark"):
                if (symbol, kind) in (completed or set()):
                    continue
                bars, since = {}, start
                try:
                    while since < end:
                        batch = retry_network(lambda: exchange.fetch_ohlcv(
                            instrument, "1m", since=since, limit=1000,
                            params={"price": "mark"} if kind == "mark" else {}), ccxt.NetworkError)
                        if not batch:
                            break
                        last = max(int(row[0]) for row in batch)
                        if last < since:
                            raise ValueError("分钟行情分页未前进")
                        for row in batch:
                            if start <= int(row[0]) < end:
                                if int(row[0]) in bars and bars[int(row[0])] != row:
                                    raise ValueError("分钟行情同页键冲突")
                                bars[int(row[0])] = row
                        since = last + 60_000
                    result["datasets"].append({"symbol": symbol, "kind": kind,
                                                "source": f"ccxt.{exchange.id}.fetch_ohlcv",
                                                "bars": [bars[t] for t in sorted(bars)]})
                    print(json.dumps({"symbol": symbol, "kind": kind, "rows": len(bars)}), flush=True)
                except Exception as error:
                    result["errors"].append({"symbol": symbol, "kind": kind,
                                               "type": type(error).__name__})
                    print(json.dumps(result["errors"][-1]), flush=True)
    finally:
        exchange.close() if hasattr(exchange, "close") else None
    return result


def main() -> int:
    import ccxt

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume-input", type=Path, help="复用已冻结且完整的价格项，仅补缺项")
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("研究输入已经冻结，禁止覆盖")
    node = json.loads(args.node_input.read_text(encoding="utf-8"))
    symbols = sorted({row["symbol"] for row in node["predictions"]})
    first = min(row["observed_at_ms"] for row in node["predictions"])
    start, end = first // 60_000 * 60_000 - 3_600_000, node["end_ms"] + 60_000
    previous, completed, resume_identity = [], set(), None
    if args.resume_input:
        raw = args.resume_input.read_bytes()
        frozen = json.loads(raw)
        if (frozen.get("contract_version") != "pxybacktest.funding-price-input.v1"
                or (frozen["start_ms"], frozen["end_ms"]) != (start, end)):
            raise ValueError("续采输入契约或时间窗口不匹配")
        expected = set(range(start, end, 60_000))
        for row in frozen["datasets"]:
            key = row["symbol"], row["kind"]
            if row["symbol"] in symbols and {int(bar[0]) for bar in row["bars"]} == expected:
                if key in completed:
                    raise ValueError("续采输入存在重复价格项")
                previous.append(row)
                completed.add(key)
        resume_identity = {"path": str(args.resume_input.resolve()),
                           "sha256": hashlib.sha256(raw).hexdigest()}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(collect_venue, venue,
                               [symbol for symbol in symbols if symbol.endswith("_" + venue.upper())],
                               start, end, completed) for venue in ("binance", "okx", "gate")
                   if any((symbol, kind) not in completed
                          for symbol in symbols if symbol.endswith("_" + venue.upper())
                          for kind in ("trade", "mark"))]
        results = [future.result() for future in futures]
    data = {"contract_version": "pxybacktest.funding-price-input.v1",
            "ccxt_version": ccxt.__version__, "observed_at": datetime.now(UTC).isoformat(),
            "bar_time": "minute_open_utc_ms", "start_ms": start, "end_ms": end,
            "resume_input": resume_identity,
            "datasets": previous + [row for result in results for row in result["datasets"]],
            "errors": [row for result in results for row in result["errors"]]}
    raw = json.dumps(data, allow_nan=False).encode("utf-8")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as output:
        output.write(raw)
    print(json.dumps({"path": str(args.output.resolve()), "sha256": hashlib.sha256(raw).hexdigest(),
                      "datasets": len(data["datasets"]), "errors": data["errors"]}), flush=True)
    return 0 if not data["errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
