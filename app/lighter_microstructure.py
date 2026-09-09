"""Lighter 资金费、主动成交和多档盘口的快照回放。"""

from __future__ import annotations

import hashlib
import math
import statistics
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .learning import LearningBacktestError, _parse_datetime, _safe_table_rows

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


def _row_ready_time(row: dict[str, Any]) -> datetime:
    event_time = _parse_datetime(row.get("event_time"))
    available_at = _parse_datetime(row.get("available_at") or row.get("event_time"))
    return max(event_time, available_at)


def _row_ready_time_text(row: dict[str, Any]) -> str:
    event_time = _parse_datetime(row.get("event_time"))
    available_value = row.get("available_at") or row.get("event_time")
    available_at = _parse_datetime(available_value)
    return str(available_value if available_at > event_time else row.get("event_time"))


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
            symbol = str(
                raw.get("symbol") or raw.get("instrument_id") or ""
            ).strip().upper()
            if wanted and symbol not in wanted:
                continue
            raw_time = (
                raw.get("event_time")
                or raw.get("settlement_time")
                or raw.get("timestamp_utc")
                or raw.get("funding_time")
                or raw.get("exchange_ts")
            )
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
    trades = load_manifest_rows(
        data_root=data_root,
        manifest=manifest,
        dataset_name="lighter_trades",
        symbols=symbols,
        start=str(period.get("start") or ""),
        end=str(period.get("end") or ""),
    )
    footprint_coverage, footprint_trades = _footprint_coverage(
        trades,
        start=str(period.get("start") or ""),
        end=str(period.get("end") or ""),
    )
    rebuilt = rebuild_order_book(events, depth=int(parameters.get("book_depth") or 10)) if events else []
    if not factors and rebuilt:
        factors = rebuilt
    if funding_rows:
        factors = list(factors)
        for funding in funding_rows:
            funding_time = (
                funding.get("settlement_time")
                or funding.get("event_time")
                or funding.get("funding_time")
            )
            mark_price = funding.get("mark_price") or funding.get("index_price")
            factors.append(
                {
                    "symbol": funding.get("symbol"),
                    "event_time": funding_time,
                    "available_at": funding.get("available_at") or funding_time,
                    "mid_price": mark_price,
                    "funding_rate": funding.get(
                        "funding_rate",
                        funding.get("rate_decimal", funding.get("rate")),
                    ),
                    "_funding_only": True,
                    "_funding_event_time": funding_time,
                    "_funding_available_at": funding.get("available_at")
                    or funding_time,
                    "_funding_mark_price": mark_price,
                    "_funding_event": True,
                }
            )
    factors.sort(
        key=lambda row: (
            _row_ready_time(row),
            0 if row.get("_funding_event") else 1,
            _parse_datetime(row.get("event_time")),
            str(row.get("symbol") or ""),
        )
    )
    if not factors:
        raise LighterBacktestError("快照中没有可回放的 Lighter 因子或盘口事件")
    threshold = _decimal_number(parameters.get("entry_threshold", "0.2"))
    exit_threshold = _decimal_number(parameters.get("exit_threshold", "0"))
    hold_ms = int(parameters.get("max_hold_ms", 3_600_000))
    execution = dict(task.get("execution") or {})
    fee_bps_value = parameters.get("fee_bps_per_side")
    if fee_bps_value is None:
        fee_bps_value = _decimal_number(execution.get("rate")) * Decimal("10000")
    fee_bps = _decimal_number(fee_bps_value)
    slippage_bps = _decimal_number(
        parameters.get("slippage_bps_per_side", execution.get("slippage", 0))
    )
    capital = _decimal_number(execution.get("capital") or 1_000_000)
    quantity = _decimal_number(parameters.get("quantity") or 1)
    cash = capital
    position: dict[str, Any] | None = None
    pending: dict[str, Any] | None = None
    deals: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    equity: list[dict[str, Any]] = []
    position_events: list[dict[str, Any]] = []
    position_history: list[dict[str, Any]] = []
    active_buy = active_sell = total_funding_pnl = Decimal("0")
    marks: dict[str, Decimal] = {}
    last_event_time = period.get("start")
    final_equity = capital
    final_unrealized = Decimal("0")
    period_end = _parse_datetime(period.get("end"))

    def position_at(
        *, symbol: str, settlement_time: datetime
    ) -> dict[str, Any] | None:
        if (
            position is not None
            and symbol == str(position["symbol"])
            and position["entry_datetime"] <= settlement_time
        ):
            return position
        for closed in reversed(position_history):
            if (
                symbol == str(closed["symbol"])
                and closed["entry_datetime"] <= settlement_time
                and settlement_time < closed["exit_datetime"]
            ):
                return closed
        return None

    def append_fill(
        *,
        action: str,
        side: str,
        position_effect: str,
        signal_time: Any,
        fill_time: Any,
        price: Decimal,
        fee: Decimal,
        symbol: str,
    ) -> dict[str, Any]:
        fill_number = len(fills) + 1
        item = {
            "fill_id": f"{task_id}-fill-{fill_number}",
            "order_id": f"{task_id}-order-{fill_number}",
            "symbol": symbol,
            "side": side,
            "position_effect": position_effect,
            "action": action,
            "signal_time": signal_time,
            "fill_time": fill_time,
            "price": _decimal_float(price),
            "quantity": _decimal_float(quantity),
            "fee_amount": _decimal_float(fee),
            "fee_bps": _decimal_float(fee_bps),
            "slippage_bps": _decimal_float(slippage_bps),
            "status": "filled",
        }
        fills.append(item)
        return item

    for row_index, row in enumerate(factors):
        ready_time = _row_ready_time(row)
        if ready_time > period_end:
            continue
        symbol = str(row.get("symbol") or (symbols[0] if symbols else ""))
        direct_mid = _decimal_number(row.get("mid_price") or row.get("mid"))
        bid = _decimal_number(row.get("bid_price1"))
        ask = _decimal_number(row.get("ask_price1"))
        mid = direct_mid or ((bid + ask) / Decimal("2") if bid > 0 and ask > 0 else Decimal("0"))
        if mid <= 0 and row.get("_funding_event"):
            mid = marks.get(symbol, Decimal("0"))
        if mid <= 0:
            continue
        timestamp = ready_time
        ts_ms = int(timestamp.timestamp() * 1000)
        event_time = _row_ready_time_text(row)
        last_event_time = event_time
        is_execution_tick = not bool(row.get("_funding_only"))
        if is_execution_tick:
            marks[symbol] = mid
        buy = _decimal_number(row.get("buy_qty"))
        sell = _decimal_number(row.get("sell_qty"))
        active_buy += max(Decimal("0"), buy)
        active_sell += max(Decimal("0"), sell)
        funding_rate = (
            _decimal_number(
                row.get("funding_rate")
                if row.get("funding_rate") is not None
                else row.get("rate_decimal")
            )
            if row.get("_funding_event")
            else Decimal("0")
        )

        # 资金费在 available_at 到达账本，但按 settlement_time 的历史仓位结算。
        funding_position = None
        if row.get("_funding_event"):
            funding_position = position_at(
                symbol=symbol,
                settlement_time=_parse_datetime(
                    row.get("_funding_event_time") or row.get("event_time")
                ),
            )
        if funding_position is not None:
            position_mark = _decimal_number(row.get("_funding_mark_price"))
            if position_mark <= 0:
                position_mark = marks.get(str(funding_position["symbol"]), mid)
            funding_quantity = _decimal_number(funding_position.get("quantity"))
            notional = abs(funding_quantity * position_mark)
            funding_cashflow = (
                -Decimal(funding_position["side"]) * notional * funding_rate
            )
            if funding_cashflow:
                cash += funding_cashflow
                total_funding_pnl += funding_cashflow
                funding_position["funding_pnl"] += funding_cashflow
                row["_funding_payment"] = _decimal_float(funding_cashflow)
                deal_index = funding_position.get("deal_index")
                if deal_index is not None:
                    deal = deals[int(deal_index)]
                    deal["funding_pnl"] = _decimal_float(
                        funding_position["funding_pnl"]
                    )
                    deal["pnl_amount"] = _decimal_float(
                        _decimal_number(deal.get("pnl_amount")) + funding_cashflow
                    )

        # Task v2 的 Lighter 引擎是 Tick 模式：本 Tick 只能生成信号，挂单必须
        # 等到同品种的后续 Tick 才能成交，不能读取当前行后立即成交。
        if (
            pending is not None
            and is_execution_tick
            and row_index > int(pending["signal_index"])
            and symbol == str(pending["symbol"])
        ):
            action = str(pending["action"])
            order_side = Decimal("1") if action == "enter_long" else Decimal("-1")
            if action == "exit":
                if position is None:
                    pending = None
                else:
                    order_side = -Decimal(position["side"])
                    exit_price = mid * (
                        Decimal("1") + order_side * slippage_bps / Decimal("10000")
                    )
                    exit_fee = abs(exit_price * quantity) * fee_bps / Decimal("10000")
                    gross = Decimal(position["side"]) * (
                        exit_price - position["entry_price"]
                    ) * quantity
                    cash += gross - exit_fee
                    total_fees = position["entry_fee"] + exit_fee
                    net = gross - total_fees + position["funding_pnl"]
                    append_fill(
                        action="exit_long" if position["side"] > 0 else "exit_short",
                        side="sell" if position["side"] > 0 else "buy",
                        position_effect="close",
                        signal_time=pending["signal_time"],
                        fill_time=event_time,
                        price=exit_price,
                        fee=exit_fee,
                        symbol=symbol,
                    )
                    deals.append(
                        {
                            "symbol": position["symbol"],
                            "side": "long" if position["side"] > 0 else "short",
                            "entry_time": position["entry_time"],
                            "exit_time": event_time,
                            "entry_price": _decimal_float(position["entry_price"]),
                            "exit_price": _decimal_float(exit_price),
                            "quantity": _decimal_float(quantity),
                            "gross_pnl": _decimal_float(gross),
                            "fees": _decimal_float(total_fees),
                            "pnl_amount": _decimal_float(net),
                            "funding_pnl": _decimal_float(position["funding_pnl"]),
                        }
                    )
                    position_events.append(
                        {
                            "event_time": event_time,
                            "symbol": position["symbol"],
                            "payload": {
                                "symbol": position["symbol"],
                                "side": "flat",
                                "quantity": 0.0,
                                "mark_price": _decimal_float(mid),
                                "unrealized_pnl": 0.0,
                                "funding_pnl": _decimal_float(position["funding_pnl"]),
                            },
                        }
                    )
                    position["exit_datetime"] = timestamp
                    position["deal_index"] = len(deals) - 1
                    position_history.append(position)
                    position = None
                    pending = None
            else:
                entry_price = mid * (
                    Decimal("1") + order_side * slippage_bps / Decimal("10000")
                )
                entry_fee = abs(entry_price * quantity) * fee_bps / Decimal("10000")
                cash -= entry_fee
                position = {
                    "symbol": symbol,
                    "side": 1 if order_side > 0 else -1,
                    "entry_price": entry_price,
                    "entry_time": event_time,
                    "entry_ts_ms": ts_ms,
                    "entry_datetime": timestamp,
                    "quantity": quantity,
                    "entry_fee": entry_fee,
                    "funding_pnl": Decimal("0"),
                }
                append_fill(
                    action=action,
                    side="buy" if order_side > 0 else "sell",
                    position_effect="open",
                    signal_time=pending["signal_time"],
                    fill_time=event_time,
                    price=entry_price,
                    fee=entry_fee,
                    symbol=symbol,
                )
                pending = None

        signal = _signal_decimal(row)
        if pending is None and is_execution_tick:
            if position is None and abs(signal) >= threshold:
                pending = {
                    "action": "enter_long" if signal > 0 else "enter_short",
                    "symbol": symbol,
                    "signal_time": event_time,
                    "signal_index": row_index,
                }
            elif position is not None and symbol == str(position["symbol"]):
                if (
                    ts_ms - int(position["entry_ts_ms"]) >= hold_ms
                    or Decimal(position["side"]) * signal <= exit_threshold
                ):
                    pending = {
                        "action": "exit",
                        "symbol": symbol,
                        "signal_time": event_time,
                        "signal_index": row_index,
                    }

        unrealized = Decimal("0")
        if position is not None:
            position_mark = marks.get(str(position["symbol"]), position["entry_price"])
            unrealized = Decimal(position["side"]) * (
                position_mark - position["entry_price"]
            ) * quantity
            position_events.append(
                {
                    "event_time": event_time,
                    "symbol": position["symbol"],
                    "payload": {
                        "symbol": position["symbol"],
                        "side": "long" if position["side"] > 0 else "short",
                        "quantity": _decimal_float(Decimal(position["side"]) * quantity),
                        "entry_price": _decimal_float(position["entry_price"]),
                        "mark_price": _decimal_float(position_mark),
                        "unrealized_pnl": _decimal_float(unrealized),
                        "funding_pnl": _decimal_float(position["funding_pnl"]),
                    },
                }
            )
        final_unrealized = unrealized
        final_equity = cash + unrealized
        equity.append(
            {
                "date": event_time,
                "value": _decimal_float(final_equity),
                "cash": _decimal_float(cash),
                "unrealized_pnl": _decimal_float(unrealized),
                "funding_pnl": _decimal_float(total_funding_pnl),
            }
        )
    if not equity:
        equity = [{"date": last_event_time, "value": _decimal_float(capital)}]
    returns = [_decimal_number(item["pnl_amount"]) / capital for item in deals]
    final_positions = (
        [position_events[-1]["payload"]]
        if position is not None and position_events
        else []
    )
    from app.replay import build_replay_audit

    snapshot = dict((task.get("data") or {}).get("snapshot") or {})
    replay_events: list[dict[str, Any]] = [
        {
            "event_type": "footprint_coverage",
            "event_time": period.get("start"),
            "payload": footprint_coverage,
            "symbol": symbols[0] if symbols else None,
            "source_seq": 0,
        }
    ]
    for index, row in enumerate(factors, start=1):
        replay_events.append(
            {
                "event_type": "order_book" if any(
                    key in row for key in ("bid_price1", "ask_price1", "depth_imbalance")
                ) else "factor",
                "event_time": row.get("event_time"),
                "available_at": row.get("available_at"),
                "payload": {
                    key: value
                    for key, value in row.items()
                    if not key.startswith("_funding")
                },
                "symbol": row.get("symbol"),
                "source_seq": index,
            }
        )
        if row.get("_funding_event"):
            replay_events.append(
                {
                    "event_type": "funding",
                    "event_time": row.get("_funding_event_time")
                    or row.get("event_time"),
                    "available_at": row.get("_funding_available_at")
                    or row.get("available_at"),
                    "payload": {
                        "rate": row.get("funding_rate"),
                        "mark_price": row.get("_funding_mark_price"),
                        "payment": row.get("_funding_payment", 0.0),
                        "rate_direction": "positive_long_pays_short",
                    },
                    "symbol": row.get("symbol"),
                    "source_seq": len(replay_events),
                }
            )
    exposed_footprint_trades = (
        footprint_trades if footprint_coverage["available"] else []
    )
    for index, row in enumerate(exposed_footprint_trades, start=len(replay_events)):
        replay_events.append(
            {
                "event_type": "trade_print",
                "event_time": row.get("event_time"),
                "available_at": row.get("received_at"),
                "payload": {
                    **row,
                    "volume": row.get("qty"),
                    "real_trade": True,
                },
                "symbol": row.get("symbol"),
                "source_seq": index,
            }
        )
    for index, item in enumerate(fills):
        replay_events.append(
            {
                "event_type": "fill",
                "event_time": item.get("fill_time"),
                "payload": item,
                "symbol": item.get("symbol"),
                "source_seq": len(replay_events) + index,
            }
        )
    for index, item in enumerate(position_events):
        replay_events.append(
            {
                "event_type": "position",
                "event_time": item.get("event_time"),
                "payload": item.get("payload"),
                "symbol": item.get("symbol"),
                "source_seq": len(replay_events) + index,
            }
        )
    for index, item in enumerate(equity):
        replay_events.append(
            {
                "event_type": "account",
                "event_time": item.get("date"),
                "payload": item,
                "source_seq": len(replay_events) + index,
            }
        )
    return {
        "schema_version": 2,
        "contract_version": "pxybacktest.task-result.v2",
        "task_id": task_id,
        "engine_type": "lighter_microstructure",
        "strategy": task.get("strategy") or {},
        "data_snapshot": dict((task.get("data") or {}).get("snapshot") or {}),
        "run": {"universe": task.get("universe") or {}, "period": period, "execution": task.get("execution") or {}, "parameters": parameters},
        "metrics": {
            "total_return": _decimal_float(final_equity / capital - Decimal("1")),
            "final_equity": _decimal_float(final_equity),
            "final_cash": _decimal_float(cash),
            "unrealized_pnl": _decimal_float(final_unrealized),
            "net_profit": _decimal_float(final_equity - capital),
            "n_trades": len(deals),
            "n_fills": len(fills),
            "win_rate": (
                sum(value > 0 for value in returns) / len(returns) if returns else 0.0
            ),
            "active_buy_qty": _decimal_float(active_buy),
            "active_sell_qty": _decimal_float(active_sell),
            "funding_pnl": _decimal_float(total_funding_pnl),
        },
        "curves": {"equity": equity},
        "deals": deals,
        "fills": fills,
        "positions": final_positions,
        "diagnostics": {
            "matching_model": "lighter_factor_or_l2_rebuild",
            "execution_timing": "next_tick",
            "book_depth": int(parameters.get("book_depth") or 10),
            "data_source_policy": "pxydata_snapshot_only",
            "snapshot_enforcement": "manifest_bound",
            "funding_rate_unit": "decimal_notional_fraction_per_event",
            "fee_unit": "basis_points_per_fill",
            "slippage_unit": "basis_points_per_fill",
            "pending_order": pending,
            "footprint_coverage": footprint_coverage,
            "warnings": ["Lighter 回测只生成研究结果，不提交真实订单。"],
        },
        "artifacts": [],
        "replay_audit": build_replay_audit(
            run_id=task_id,
            snapshot_id=str(snapshot.get("snapshot_id") or task_id),
            events=replay_events,
        ),
        "_replay_events": replay_events,
    }


def _footprint_coverage(
    trades: list[dict[str, Any]],
    *,
    start: str,
    end: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """只在请求区间每个 UTC 小时都有真实逐笔时开放足迹。"""

    start_dt = _parse_datetime(start).astimezone(timezone.utc)
    end_dt = _parse_datetime(end).astimezone(timezone.utc)
    cursor = start_dt.replace(minute=0, second=0, microsecond=0)
    end_hour = end_dt.replace(minute=0, second=0, microsecond=0)
    expected_hours: set[str] = set()
    while cursor <= end_hour:
        expected_hours.add(cursor.strftime("%Y-%m-%dT%H:00:00Z"))
        cursor += timedelta(hours=1)

    valid: list[dict[str, Any]] = []
    covered_hours: set[str] = set()
    invalid = 0
    for row in trades:
        try:
            price = float(row.get("price"))
            qty = float(row.get("qty"))
            event_time = _parse_datetime(row.get("event_time")).astimezone(timezone.utc)
        except (TypeError, ValueError, LearningBacktestError):
            invalid += 1
            continue
        side = str(row.get("side") or "unknown").strip().lower()
        if price <= 0 or qty <= 0 or side not in {"buy", "sell", "unknown"}:
            invalid += 1
            continue
        valid.append(row)
        covered_hours.add(
            event_time.replace(minute=0, second=0, microsecond=0).strftime(
                "%Y-%m-%dT%H:00:00Z"
            )
        )

    missing = sorted(expected_hours - covered_hours)
    complete = bool(valid) and invalid == 0 and not missing
    if not trades:
        reason = "仅有 K 线，无法生成真实足迹"
    elif invalid:
        reason = f"真实逐笔存在 {invalid} 条无效记录，无法生成完整足迹"
    elif missing:
        reason = f"真实逐笔覆盖不完整，缺少 {len(missing)} 个小时，无法生成完整足迹"
    else:
        reason = ""
    return (
        {
            "contract": "pxybacktest.footprint.v1",
            "available": complete,
            "complete": complete,
            "source": "lighter_trades" if trades else "none",
            "interval": "1m",
            "interval_ms": 60_000,
            "reason": reason,
            "expected_hours": len(expected_hours),
            "covered_hours": len(covered_hours),
            "missing_hours": missing[:48],
            "trade_count": len(valid),
            "invalid_trade_count": invalid,
        },
        valid,
    )


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


def _decimal_number(value: Any) -> Decimal:
    """把外部数值稳定转换为 Decimal；缺失或非有限值按零处理。"""

    try:
        number = Decimal(str(value if value is not None else 0))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    return number if number.is_finite() else Decimal("0")


def _decimal_float(value: Decimal) -> float:
    """结果契约保留 JSON number，账本计算过程保持 Decimal。"""

    return float(value)


def _signal_decimal(row: dict[str, Any]) -> Decimal:
    return (
        _decimal_number(row.get("trade_imbalance")) * Decimal("0.45")
        + _decimal_number(row.get("depth_imbalance_5") or row.get("depth_imbalance"))
        * Decimal("0.35")
        + _decimal_number(row.get("ofi_normalized")) * Decimal("0.20")
    )


def _signal(row: dict[str, Any]) -> float:
    return _decimal_float(_signal_decimal(row))
