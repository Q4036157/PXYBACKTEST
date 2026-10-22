"""复用 DATA 官方适配器，补齐研究收益标签，不改写任何业务数据库。"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pxydata-root", type=Path, required=True)
    parser.add_argument("--node-input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("研究输入已冻结，禁止覆盖")
    sys.path.insert(0, str(args.pxydata_root / "backend"))
    from app.services.funding_sources import FundingHistorySource

    node = json.loads(args.node_input.read_text(encoding="utf-8"))
    symbols = sorted({row["symbol"] for row in node["predictions"]})
    result = {"contract_version": "pxybacktest.funding-final-labels.v1",
              "usage": "payoff_labels_only_not_historical_signals",
              "observed_at": datetime.now(UTC).isoformat(), "data": [], "errors": []}
    source = FundingHistorySource(timeout=12)
    try:
        for symbol in symbols:
            pair, venue = symbol.split("_SWAP_")
            venue = venue.lower()
            base = pair.removesuffix("USDT")
            native = f"{base}-USDT-SWAP" if venue == "okx" else (
                f"{base}_USDT" if venue == "gate" else pair)
            market = {"venue": venue, "native_symbol": native,
                      "instrument_id": symbol, "api_family": "fapi", "settle": "usdt"}
            cursor, seen = None, set()
            collected = []
            try:
                for _ in range(30):
                    # 期末平仓计入同刻结算，秒级源延迟留一分钟标签余量。
                    page = source.history_page(venue, market, node["start_ms"],
                                               node["end_ms"] + 60_000, cursor)
                    for row in page["rows"]:
                        row["symbol"] = symbol
                        collected.append(row)
                    cursor = page.get("next_cursor")
                    if cursor is None:
                        break
                    key = json.dumps(cursor, sort_keys=True)
                    if key in seen:
                        raise ValueError("官方接口分页重复")
                    seen.add(key)
                else:
                    raise ValueError("官方接口分页超过预算，不能认定完整")
                result["data"].extend(collected)
                print(json.dumps({"symbol": symbol, "rows": len(collected)},
                                 ensure_ascii=False), flush=True)
            except Exception as error:
                result["errors"].append({"symbol": symbol, "type": type(error).__name__})
                print(json.dumps({"symbol": symbol, "error": type(error).__name__}), flush=True)
    finally:
        source.close()
    raw = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as output:
        output.write(raw)
    print(json.dumps({"path": str(args.output.resolve()), "rows": len(result["data"]),
                      "errors": result["errors"], "sha256": hashlib.sha256(raw).hexdigest()}))
    return 0 if not result["errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
