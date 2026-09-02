"""MT5 策略测试报告解析与逐笔成交一致性校验。

该模块只负责可重复的差异计算，不负责启动 MetaTrader。这样 ``mt5_native``
参考引擎和 ``mt5_compat`` 独立引擎可以共享同一份验收语义。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


MT5_PARITY_CONTRACT = "pxybacktest.mt5-parity.v1"


class Mt5ReportError(ValueError):
    """MT5 报告缺失关键结构或字段。"""


@dataclass(frozen=True, slots=True)
class NormalizedDeal:
    """MT5 和 PXY 共同使用的成交级规范模型。"""

    sequence: int
    time: datetime
    deal_id: str
    order_id: str
    symbol: str
    side: str
    entry: str
    volume: Decimal
    price: Decimal
    commission: Decimal
    swap: Decimal
    profit: Decimal
    balance: Decimal | None
    comment: str


@dataclass(frozen=True, slots=True)
class Mt5Report:
    """从 MT5 Strategy Tester HTML 中提取的权威基准。"""

    metadata: dict[str, str]
    inputs: dict[str, str]
    deals: tuple[NormalizedDeal, ...]


@dataclass(frozen=True, slots=True)
class ParityTolerance:
    """成交差异允许阈值；时间单位为毫秒。"""

    time_ms: int = 0
    price: Decimal = Decimal("0")
    volume: Decimal = Decimal("0")
    money: Decimal = Decimal("0.005")


@dataclass(frozen=True, slots=True)
class DealMismatch:
    sequence: int
    field: str
    mt5: str
    pxy: str
    delta: str | None = None


@dataclass(frozen=True, slots=True)
class ParityResult:
    """逐笔对齐结果；``matched`` 只有在成交数量和所有字段都一致时为真。"""

    contract_version: str
    matched: bool
    mt5_count: int
    pxy_count: int
    compared_count: int
    mismatch_count: int
    first_mismatch_sequence: int | None
    mismatches: tuple[DealMismatch, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["mismatches"] = [asdict(item) for item in self.mismatches]
        return payload


class _TableParser(HTMLParser):
    """仅提取表格行，避免引入 BeautifulSoup 运行依赖。"""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() == "tr":
            self._row = []
        elif tag.lower() in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._cell is not None:
            value = " ".join("".join(self._cell).split())
            if self._row is not None:
                self._row.append(value)
            self._cell = None
        elif lowered == "tr" and self._row is not None:
            if any(self._row):
                self.rows.append(self._row)
            self._row = None


def _read_report_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16")
    for encoding in ("utf-8-sig", "utf-16", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise Mt5ReportError(f"无法识别 MT5 报告编码: {path}")


def _decimal(value: Any, *, default: str = "0") -> Decimal:
    text = str(value if value is not None else "").strip()
    if not text:
        return Decimal(default)
    normalized = text.replace("\xa0", "").replace(" ", "").replace(",", "")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise Mt5ReportError(f"无法解析数值: {value!r}") from exc


def _datetime(value: Any) -> datetime:
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        # MT5 HTML 只保存经纪商服务器墙上时间。PXY 侧即使携带偏移，也必须先
        # 映射到同一服务器墙上时间；这里不擅自把报告时间转换为系统时区。
        return parsed.replace(tzinfo=None)
    except ValueError:
        pass
    for pattern in (
        "%Y.%m.%d %H:%M:%S.%f",
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(text.replace("Z", ""), pattern)
        except ValueError:
            continue
    raise Mt5ReportError(f"无法解析成交时间: {value!r}")


def _clean_label(value: str) -> str:
    return value.strip().rstrip(":：").strip()


def parse_mt5_report(path: str | Path) -> Mt5Report:
    """解析 MT5 Strategy Tester HTML，并排除入金等非交易成交。"""

    report_path = Path(path)
    parser = _TableParser()
    parser.feed(_read_report_text(report_path))

    metadata: dict[str, str] = {}
    inputs: dict[str, str] = {}
    deals: list[NormalizedDeal] = []
    section = "summary"
    deal_header_seen = False

    for row in parser.rows:
        cells = [cell.strip() for cell in row]
        nonempty = [cell for cell in cells if cell]
        if len(nonempty) == 1 and nonempty[0].lower() in {"订单", "orders"}:
            section = "orders"
            continue
        if len(nonempty) == 1 and nonempty[0].lower() in {"成交", "deals"}:
            section = "deals"
            deal_header_seen = False
            continue

        if section == "summary":
            for cell in nonempty:
                if "=" in cell:
                    key, value = cell.split("=", 1)
                    if key.strip():
                        inputs[key.strip()] = value.strip()
            for index, cell in enumerate(cells[:-1]):
                if not cell.endswith((":", "：")):
                    continue
                value = next((item for item in cells[index + 1 :] if item), "")
                if value and "=" not in value:
                    metadata.setdefault(_clean_label(cell), value)
            continue

        if section != "deals":
            continue
        if not deal_header_seen:
            if len(cells) >= 7 and _clean_label(cells[0]).lower() in {"时间", "time"}:
                deal_header_seen = True
            continue
        if len(cells) < 13:
            continue

        deal_type = cells[3].strip().lower()
        entry = cells[4].strip().lower()
        if deal_type not in {"buy", "sell"} or entry not in {"in", "out", "in/out", "out by"}:
            continue
        deals.append(
            NormalizedDeal(
                sequence=len(deals) + 1,
                time=_datetime(cells[0]),
                deal_id=cells[1],
                symbol=cells[2],
                side=deal_type,
                entry=entry,
                volume=_decimal(cells[5]),
                price=_decimal(cells[6]),
                order_id=cells[7],
                commission=_decimal(cells[8]),
                swap=_decimal(cells[9]),
                profit=_decimal(cells[10]),
                balance=_decimal(cells[11]) if cells[11] else None,
                comment=cells[12],
            )
        )

    if not deal_header_seen:
        raise Mt5ReportError(f"MT5 报告未找到成交表: {report_path}")
    return Mt5Report(metadata=metadata, inputs=inputs, deals=tuple(deals))


def _first(item: Mapping[str, Any], names: Sequence[str], default: Any = "") -> Any:
    for name in names:
        value = item.get(name)
        if value is not None and value != "":
            return value
    return default


def normalize_pxy_deals(items: Iterable[Mapping[str, Any]]) -> tuple[NormalizedDeal, ...]:
    """把不同 PXY 引擎的成交字段映射到 MT5 成交模型。"""

    normalized: list[NormalizedDeal] = []
    for item in items:
        side = str(_first(item, ("side", "direction", "type"))).strip().lower()
        if side in {"long", "多", "买"}:
            side = "buy"
        elif side in {"short", "空", "卖"}:
            side = "sell"

        entry = str(_first(item, ("entry", "offset"))).strip().lower()
        if entry in {"open", "开", "开仓"}:
            entry = "in"
        elif entry in {"close", "平", "平仓", "closetoday", "closeyesterday"}:
            entry = "out"

        normalized.append(
            NormalizedDeal(
                sequence=len(normalized) + 1,
                time=_datetime(_first(item, ("time", "datetime", "timestamp"))),
                deal_id=str(_first(item, ("deal_id", "trade_id", "id"))),
                order_id=str(_first(item, ("order_id", "orderid", "vt_orderid"))),
                symbol=str(_first(item, ("symbol", "vt_symbol"))).split(".", 1)[0],
                side=side,
                entry=entry,
                volume=_decimal(_first(item, ("volume", "qty", "size"), 0)),
                price=_decimal(_first(item, ("price", "fill_price"), 0)),
                commission=_decimal(_first(item, ("commission", "fee"), 0)),
                swap=_decimal(_first(item, ("swap", "funding"), 0)),
                profit=_decimal(_first(item, ("profit", "pnl", "realized_pnl"), 0)),
                balance=(
                    _decimal(_first(item, ("balance",)))
                    if _first(item, ("balance",), None) is not None
                    else None
                ),
                comment=str(_first(item, ("comment", "reason"))),
            )
        )
    return tuple(normalized)


def _append_text_mismatch(
    target: list[DealMismatch], sequence: int, field: str, mt5: str, pxy: str
) -> None:
    if mt5 != pxy:
        target.append(DealMismatch(sequence, field, mt5, pxy))


def _append_decimal_mismatch(
    target: list[DealMismatch],
    sequence: int,
    field: str,
    mt5: Decimal,
    pxy: Decimal,
    tolerance: Decimal,
) -> None:
    delta = abs(mt5 - pxy)
    if delta > tolerance:
        target.append(DealMismatch(sequence, field, str(mt5), str(pxy), str(delta)))


def compare_deals(
    mt5_deals: Sequence[NormalizedDeal],
    pxy_deals: Sequence[NormalizedDeal],
    *,
    tolerance: ParityTolerance | None = None,
    max_mismatches: int = 200,
) -> ParityResult:
    """按原始成交顺序比较，不按时间或票号重新排序以隐藏时序错误。"""

    accepted = tolerance or ParityTolerance()
    mismatches: list[DealMismatch] = []
    compared = min(len(mt5_deals), len(pxy_deals))

    for index in range(compared):
        mt5 = mt5_deals[index]
        pxy = pxy_deals[index]
        sequence = index + 1
        time_delta_ms = abs((mt5.time - pxy.time).total_seconds() * 1000)
        if time_delta_ms > accepted.time_ms:
            mismatches.append(
                DealMismatch(
                    sequence,
                    "time",
                    mt5.time.isoformat(),
                    pxy.time.isoformat(),
                    str(time_delta_ms),
                )
            )
        _append_text_mismatch(mismatches, sequence, "symbol", mt5.symbol, pxy.symbol)
        _append_text_mismatch(mismatches, sequence, "side", mt5.side, pxy.side)
        _append_text_mismatch(mismatches, sequence, "entry", mt5.entry, pxy.entry)
        _append_decimal_mismatch(
            mismatches, sequence, "volume", mt5.volume, pxy.volume, accepted.volume
        )
        _append_decimal_mismatch(
            mismatches, sequence, "price", mt5.price, pxy.price, accepted.price
        )
        _append_decimal_mismatch(
            mismatches,
            sequence,
            "commission",
            mt5.commission,
            pxy.commission,
            accepted.money,
        )
        _append_decimal_mismatch(mismatches, sequence, "swap", mt5.swap, pxy.swap, accepted.money)
        _append_decimal_mismatch(
            mismatches, sequence, "profit", mt5.profit, pxy.profit, accepted.money
        )
        if mt5.balance is not None and pxy.balance is not None:
            _append_decimal_mismatch(
                mismatches,
                sequence,
                "balance",
                mt5.balance,
                pxy.balance,
                accepted.money,
            )
        if len(mismatches) >= max_mismatches:
            break

    if len(mt5_deals) != len(pxy_deals) and len(mismatches) < max_mismatches:
        mismatches.append(
            DealMismatch(
                compared + 1,
                "deal_count",
                str(len(mt5_deals)),
                str(len(pxy_deals)),
                str(abs(len(mt5_deals) - len(pxy_deals))),
            )
        )

    first_sequence = min((item.sequence for item in mismatches), default=None)
    return ParityResult(
        contract_version=MT5_PARITY_CONTRACT,
        matched=not mismatches and len(mt5_deals) == len(pxy_deals),
        mt5_count=len(mt5_deals),
        pxy_count=len(pxy_deals),
        compared_count=compared,
        mismatch_count=len(mismatches),
        first_mismatch_sequence=first_sequence,
        mismatches=tuple(mismatches),
    )


def load_pxy_result(path: str | Path) -> tuple[NormalizedDeal, ...]:
    """读取统一结果 JSON，兼容顶层或 ``result`` 内的 deals/trades。"""

    result_path = Path(path)
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Mt5ReportError(f"无法读取 PXY 结果 JSON: {result_path}") from exc
    if not isinstance(payload, dict):
        raise Mt5ReportError("PXY 结果必须是 JSON object")
    nested = payload.get("result")
    candidate = nested if isinstance(nested, dict) else payload
    deals = candidate.get("deals") or candidate.get("trades") or []
    if not isinstance(deals, list):
        raise Mt5ReportError("PXY 结果 deals/trades 必须是数组")
    if not all(isinstance(item, dict) for item in deals):
        raise Mt5ReportError("PXY 结果包含非 object 成交")
    return normalize_pxy_deals(deals)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="逐笔比较 MT5 报告与 PXY 回测结果")
    parser.add_argument("--mt5-report", required=True, help="MT5 Strategy Tester HTML")
    parser.add_argument("--pxy-result", required=True, help="PXY 统一结果 JSON")
    parser.add_argument("--time-ms", type=int, default=0, help="允许时间差（毫秒）")
    parser.add_argument("--price", default="0", help="允许价格差")
    parser.add_argument("--volume", default="0", help="允许手数差")
    parser.add_argument("--money", default="0.005", help="允许货币字段差")
    parser.add_argument("--max-mismatches", type=int, default=200)
    args = parser.parse_args(argv)

    report = parse_mt5_report(args.mt5_report)
    pxy_deals = load_pxy_result(args.pxy_result)
    result = compare_deals(
        report.deals,
        pxy_deals,
        tolerance=ParityTolerance(
            time_ms=max(0, args.time_ms),
            price=_decimal(args.price),
            volume=_decimal(args.volume),
            money=_decimal(args.money),
        ),
        max_mismatches=max(1, args.max_mismatches),
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.matched else 1


__all__ = [
    "MT5_PARITY_CONTRACT",
    "DealMismatch",
    "Mt5Report",
    "Mt5ReportError",
    "NormalizedDeal",
    "ParityResult",
    "ParityTolerance",
    "compare_deals",
    "load_pxy_result",
    "main",
    "normalize_pxy_deals",
    "parse_mt5_report",
]


if __name__ == "__main__":
    raise SystemExit(main())
