"""通过 DATA 现有只读查询契约冻结资金费研究输入。"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path


def collect(client, *, dataset: str, symbols: list[str], start: str, end: str,
            output: Path) -> dict:
    """逐页保存真实响应；空页和失败不补造数据。"""
    if output.exists():
        raise FileExistsError("研究输入已存在，禁止覆盖已冻结的输入")
    rows, pages, offset = [], [], 0
    while True:
        result = client.request("POST", "/api/v1/agent/query", {
            "dataset": dataset, "symbols": symbols, "start": start,
            "end": end, "limit": 5000, "offset": offset,
        })
        if result.get("contract_version") != "pxydata.agent-query.v1":
            raise ValueError("DATA 查询契约不匹配")
        batch = result["data"]
        rows.extend(batch)
        pages.append({key: value for key, value in result.items() if key != "data"})
        print(json.dumps({"dataset": dataset, "page": len(pages), "rows": len(rows)},
                         ensure_ascii=False), flush=True)
        following = result.get("next_offset")
        if following is None:
            break
        if not batch:
            raise ValueError("DATA 空页仍声明后续数据，不能认定导出完整")
        if int(following) <= offset:
            raise ValueError("DATA 分页水位未前进")
        offset = int(following)
    payload = {"contract_version": "pxybacktest.funding-research-input.v1",
               "dataset": dataset, "start": start, "end": end, "symbols": symbols,
               "pages": pages, "data": rows}
    raw = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as target:
        target.write(raw)
    result = {"path": str(output.resolve()), "rows": len(rows),
              "sha256": hashlib.sha256(raw).hexdigest()}
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pxydata-root", type=Path, required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:3020")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.pxydata_root / "backend/app/agent_client.py"
    spec = importlib.util.spec_from_file_location("pxydata_research_client", source)
    if spec is None or spec.loader is None:
        raise ValueError("找不到 DATA 的现有查询客户端")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    client = module.PxyDataAgentClient(base_url=args.base_url, timeout=120)
    collect(client, dataset=args.dataset,
            symbols=[item.strip() for item in args.symbols.split(",") if item.strip()],
            start=args.start, end=args.end, output=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
