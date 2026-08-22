"""Lighter 资金费、主动成交和多档盘口的快照回放。"""

from __future__ import annotations

import hashlib
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

from .learning import _parse_datetime, _safe_table_rows

LIGHTER_STRATEGY_ID = "lighter_flow_v1"
LIGHTER_STRATEGY_HASH = hashlib.sha256(b"pxybacktest.lighter-flow.v1").hexdigest()
LIGHTER_DATASETS = {
    "lighter_microstructure_factors",
    "lighter_trades",
    "lighter_order_book_events",
    "lighter_funding_history",
}


class LighterBacktestError(ValueError):
    """Lighter 快照或回放参数不满足执行要求。"""


def lighter_runtime_available() -> bool:
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return False
    return True


def load_manifest_rows(
    *,
    data_root: str | Path,
    manifest: dict[str, Any],
    dataset_name: str,
    symbols: list[str],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    try:
        from pyarrow import parquet
    except ImportError as exc:
        raise LighterBacktestError("Lighter 回放缺少 pyarrow") from exc
    dataset = next(
        (item for item in manifest.get("datasets") or [] if isinstance(item, dict) and item.get("name") == dataset_name),
        None,
    )
    if not isinstance(dataset, dict) or not isinstance(dataset.get("files"), list):
        return []
    root = Path(data_root).resolve()
    wanted = {str(symbol).strip().upper() for symbol in symbols}
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    rows: list[dict[str, Any]] = []
    for record in dataset["files"]:
        if not isinstance(record, dict):
            raise LighterBacktestError("Lighter manifest 文件记录格式无效")
        path = (root / str(record.get("path") or "")).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise LighterBacktestError("Lighter manifest 文件越出数据根目录") from exc
        if not path.is_file():
            raise LighterBacktestError(f"Lighter manifest 文件不存在: {record.get('path')}")
        if path.stat().st_size != int(record.get("size_bytes") or -1):
            raise LighterBacktestError("Lighter manifest 文件大小不一致")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != str(record.get("sha256") or ""):
            raise LighterBacktestError("Lighter manifest 文件 SHA256 不一致")
        table = parquet.read_table(path)
        for raw in _safe_table_rows(table):
            symbol = str(raw.get("symbol") or "").strip().upper()
            if wanted and symbol not in wanted:
                continue
            raw_time = raw.get("event_time") or raw.get("timestamp_utc") or raw.get("funding_time") or raw.get("exchange_ts")
            if raw_time is None and raw.get("ts_ms") is not None:
                raw_time = datetime.fromtimestamp(float(raw["ts_ms"]) / 1000.0).isoformat()
            try:
                event_dt = _parse_datetime(raw_time)
            except Exception:
                continue
            if start_dt <= event_dt <= end_dt:
                row = dict(raw)
                row["symbol"] = symbol
                row["event_time"] = event_dt.isoformat()
                rows.append(row)
    rows.sort(key=lambda row: (row["event_time"], row.get("symbol", "")))
    return rows


def rebuild_order_book(events: list[dict[str, Any]], *, depth: int = 10) -> list[dict[str, Any]]:
    """从 snapshot/update 事件重建指定档位的盘口。

    兼容 Lighter 常见的 ``bids/asks`` 数组、单条 ``side/price/size`` 更新和
    ``event_type=reset``。遇到 nonce 断档会丢弃后续状态，避免把坏盘口送入训练。
    """
    if depth < 1 or depth > 100:
        raise LighterBacktestError("盘口 depth 必须在 1 到 100 之间")
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    previous_nonce: int | None = None
    output: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda item: item.get("event_time", "")):
        nonce = event.get("nonce")
        if nonce is not None:
            try:
                nonce_int = int(nonce)
                if previous_nonce is not None and nonce_int > previous_nonce + 1:
                    bids.clear(); asks.clear(); previous_nonce = None
                    continue
                previous_nonce = nonce_int
            except (TypeError, ValueError):
                pass
        event_type = str(event.get("event_type") or event.get("type") or "").lower()
        if event_type in {"snapshot", "subscribed", "reset"}:
            bids.clear(); asks.clear()
        for side_name, book in (("bids", bids), ("asks", asks)):
            levels = event.get(side_name) or event.get(side_name[:-1] + "_levels") or []
            if isinstance(levels, dict):
                levels = [{"price": price, "size": size} for price, size in levels.items()]
            if isinstance(levels, list):
                for level in levels:
                    if isinstance(level, (list, tuple)) and len(level) >= 2:
                        price, size = level[0], level[1]
                    elif isinstance(level, dict):
                        price, size = level.get("price"), level.get("size", level.get("qty", level.get("quantity")))
                    else:
                        continue
                    _apply_level(book, price, size)
        side = str(event.get("side") or "").lower()
        if side in {"buy", "bid", "bids", "sell", "ask", "asks"} and event.get("price") is not None:
            book = bids if side in {"buy", "bid", "bids"} else asks
            _apply_level(book, event.get("price"), event.get("size", event.get("qty", event.get("quantity"))))
        if not bids or not asks:
            continue
        bid_levels = sorted(((price, size) for price, size in bids.items() if size > 0), reverse=True)[:depth]
        ask_levels = sorted(((price, size) for price, size in asks.items() if size > 0))[:depth]
        if not bid_levels or not ask_levels:
            continue
        bid_price, bid_size = bid_levels[0]
        ask_price, ask_size = ask_levels[0]
        if bid_price <= 0 or ask_price <= bid_price:
            continue
        bid_total = sum(size for _, size in bid_levels)
        ask_total = sum(size for _, size in ask_levels)
        output.append({
            "symbol": str(event.get("symbol") or "").upper(),
            "event_time": event.get("event_time"),
            "nonce": event.get("nonce"),
            "bid_price1": bid_price,
            "ask_price1": ask_price,
            "bid_volume1": bid_size,
            "ask_volume1": ask_size,
            "bid_depth": bid_total,
            "ask_depth": ask_total,
            "depth": depth,
            "depth_imbalance": (bid_total - ask_total) / (bid_total + ask_total) if bid_total + ask_total else 0.0,
        })
    return output


def run_lighter_backtest(
    *,
    task_id: str,
    task: dict[str, Any],
    manifest: dict[str, Any],
    data_root: str | Path,
) -> dict[str, Any]:
    parameters = dict(task.get("parameters") or {})
    symbols = list(dict(task.get("universe") or {}).get("symbols") or [])
    period = dict(task.get("period") or {})
    factors = load_manifest_rows(data_root=data_root, manifest=manifest, dataset_name="lighter_microstructure_factors", symbols=symbols, start=str(period.get("start") or ""), end=str(period.get("end") or ""))
    events = load_manifest_rows(data_root=data_root, manifest=manifest, dataset_name="lighter_order_book_events", symbols=symbols, start=str(period.get("start") or ""), end=str(period.get("end") or ""))
    funding_rows = load_manifest_rows(data_root=data_root, manifest=manifest, dataset_name="lighter_funding_history", symbols=symbols, start=str(period.get("start") or ""), end=str(period.get("end") or ""))
    rebuilt = rebuild_order_book(events, depth=int(parameters.get("book_depth") or 10)) if events else []
    if not factors and rebuilt:
        factors = rebuilt
    if funding_rows:
        by_time = {(str(row.get("symbol") or "").upper(), str(row.get("event_time"))): row for row in factors}
        for funding in funding_rows:
            key = (str(funding.get("symbol") or "").upper(), str(funding.get("event_time")))
            target = by_time.get(key)
            if target is None:
                target = {"symbol": funding.get("symbol"), "event_time": funding.get("event_time"), "mid_price": funding.get("mark_price") or funding.get("index_price")}
                factors.append(target)
                by_time[key] = target
            target["funding_rate"] = funding.get("funding_rate", funding.get("rate_decimal", funding.get("rate")))
        factors.sort(key=lambda row: (str(row.get("event_time") or ""), str(row.get("symbol") or "")))
    if not factors:
        raise LighterBacktestError("快照中没有可回放的 Lighter 因子或盘口事件")
    threshold = float(parameters.get("entry_threshold", 0.2))
    exit_threshold = float(parameters.get("exit_threshold", 0.0))
    hold_ms = int(parameters.get("max_hold_ms", 3_600_000))
    fee_bps = float(parameters.get("fee_bps_per_side", dict(task.get("execution") or {}).get("rate", 0.0) * 10_000))
    slippage_bps = float(parameters.get("slippage_bps_per_side", dict(task.get("execution") or {}).get("slippage", 0.0)))
    capital = float(dict(task.get("execution") or {}).get("capital") or 1_000_000)
    balance = capital
    position: dict[str, Any] | None = None
    deals: list[dict[str, Any]] = []
    equity: list[dict[str, Any]] = []
    active_buy = active_sell = funding_pnl = 0.0
    for row in factors:
        direct_mid = _number(row.get("mid_price") or row.get("mid"))
        bid = _number(row.get("bid_price1"))
        ask = _number(row.get("ask_price1"))
        mid = direct_mid or ((bid + ask) / 2 if bid > 0 and ask > 0 else 0.0)
        if mid <= 0:
            continue
        timestamp = _parse_datetime(row.get("event_time"))
        ts_ms = int(timestamp.timestamp() * 1000)
        buy = _number(row.get("buy_qty")); sell = _number(row.get("sell_qty"))
        active_buy += max(0.0, buy); active_sell += max(0.0, sell)
        funding_rate = _number(row.get("funding_rate") or row.get("rate_decimal"))
        if position is not None:
            notional = abs(float(position["quantity"]) * mid)
            funding_pnl += -float(position["side"]) * notional * funding_rate
            signal = _signal(row)
            if ts_ms - int(position["entry_ts_ms"]) >= hold_ms or position["side"] * signal <= exit_threshold:
                exit_price = mid * (1 - position["side"] * slippage_bps / 10_000)
                gross = position["side"] * (exit_price - position["entry_price"]) * position["quantity"]
                fees = (abs(position["entry_price"]) + abs(exit_price)) * position["quantity"] * fee_bps / 10_000
                net = gross - fees
                balance += net
                deals.append({"symbol": position["symbol"], "side": "long" if position["side"] > 0 else "short", "entry_time": position["entry_time"], "exit_time": row["event_time"], "pnl_amount": net, "funding_pnl": funding_pnl})
                equity.append({"date": row["event_time"], "value": balance})
                position = None; funding_pnl = 0.0
        if position is None:
            signal = _signal(row)
            if abs(signal) >= threshold:
                side = 1 if signal > 0 else -1
                position = {"symbol": str(row.get("symbol") or symbols[0]), "side": side, "entry_price": mid * (1 + side * slippage_bps / 10_000), "entry_time": row["event_time"], "entry_ts_ms": ts_ms, "quantity": float(parameters.get("quantity") or 1.0)}
    if not equity:
        equity = [{"date": factors[0].get("event_time"), "value": capital}]
    returns = [float(item["pnl_amount"]) / capital for item in deals]
    return {
        "schema_version": 2,
        "contract_version": "pxybacktest.task-result.v2",
        "task_id": task_id,
        "engine_type": "lighter_microstructure",
        "strategy": task.get("strategy") or {},
        "data_snapshot": dict((task.get("data") or {}).get("snapshot") or {}),
        "run": {"universe": task.get("universe") or {}, "period": period, "execution": task.get("execution") or {}, "parameters": parameters},
        "metrics": {"total_return": balance / capital - 1.0, "final_equity": balance, "net_profit": balance - capital, "n_trades": len(deals), "win_rate": sum(value > 0 for value in returns) / len(returns) if returns else 0.0, "active_buy_qty": active_buy, "active_sell_qty": active_sell, "funding_pnl": sum(float(item.get("funding_pnl") or 0.0) for item in deals)},
        "curves": {"equity": equity},
        "deals": deals,
        "diagnostics": {"matching_model": "lighter_factor_or_l2_rebuild", "book_depth": int(parameters.get("book_depth") or 10), "data_source_policy": "pxydata_snapshot_only", "snapshot_enforcement": "manifest_bound", "warnings": ["Lighter 回测只生成研究结果，不提交真实订单。"]},
        "artifacts": [],
    }


def _apply_level(book: dict[float, float], price: Any, size: Any) -> None:
    try:
        price_value = float(price); size_value = float(size)
    except (TypeError, ValueError):
        return
    if not math.isfinite(price_value) or not math.isfinite(size_value) or price_value <= 0:
        return
    if size_value <= 0:
        book.pop(price_value, None)
    else:
        book[price_value] = size_value


def _number(value: Any) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) else 0.0


def _signal(row: dict[str, Any]) -> float:
    return _number(row.get("trade_imbalance")) * 0.45 + _number(row.get("depth_imbalance_5") or row.get("depth_imbalance")) * 0.35 + _number(row.get("ofi_normalized")) * 0.20
