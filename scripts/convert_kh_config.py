"""离线转换看海量化 .kh 配置为 PXY 回测任务公共契约。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.legacy_config import translate_kh_config


def main() -> int:
    parser = argparse.ArgumentParser(description="将 .kh 配置转换为 PXYBACKTEST 任务契约")
    parser.add_argument("source", type=Path)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--strategy-source-hash")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.source.read_text(encoding="utf-8"))
    converted = translate_kh_config(
        payload,
        strategy_id=args.strategy_id,
        strategy_source_hash=args.strategy_source_hash,
    )
    text = json.dumps(converted, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8", newline="\n")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
