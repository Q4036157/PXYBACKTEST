"""基于不可变真实 Tick 快照的一档盘口事件回放。"""

from __future__ import annotations

import hashlib
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

MICROSTRUCTURE_STRATEGY_ID = "order_book_imbalance_v1"
MICROSTRUCTURE_STRATEGY_HASH = hashlib.sha256(
    b"pxybacktest.order-book-imbalance.v1"
).hexdigest()
_REQUIRED_COLUMNS = {
    "event_id",
    "event_type",
    "exchange",
    "symbol",
    "exchange_ts",
    "exchange_ts_ms",
    "received_ts_ms",
    "last_price",
    "bid_price1",
    "ask_price1",
    "bid_volume1",
    "ask_volume1",
}


class MicrostructureBacktestError(ValueError):
    """Tick 清单、行情完整性或策略参数不满足执行要求。"""


def microstructure_runtime_available() -> bool:
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return False
    return True


def load_manifest_ticks(
    *,
    data_root: str | Path,
    manifest: dict[str, Any],
    symbols: list[str],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    """校验清单文件身份后加载真实 Tick，禁止运行时目录扫描。"""
    try:
        from pyarrow import parquet
    except ImportError as exc:
        raise MicrostructureBacktestError("microstructure 缺少 pyarrow 运行依赖") from exc

    dataset = next(
        (
            item
            for item in manifest.get("datasets") or []
            if item.get("name") == "market_ticks"
        ),
        None,
    )
    if not isinstance(dataset, dict):
        raise MicrostructureBacktestError("执行快照缺少 market_ticks")
    files = dataset.get("files")
    if not isinstance(files, list) or not files:
        raise MicrostructureBacktestError("market_ticks 清单没有文件")
    root = Path(data_root).resolve()
    wanted = {str(symbol).strip().upper() for symbol in symbols}
    start_ms = _datetime_ms(start)
    end_ms = _datetime_ms(end)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in files:
        if not isinstance(record, dict):
            raise MicrostructureBacktestError("market_ticks 文件记录格式无效")
        path = (root / str(record.get("path") or "")).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise MicrostructureBacktestError("market_ticks 文件越出数据根目录") from exc
        if not path.is_file():
            raise MicrostructureBacktestError("market_ticks 清单文件不存在")
        if path.stat().st_size != int(record.get("size_bytes") or -1):
            raise MicrostructureBacktestError("market_ticks 清单文件大小不一致")
        if _sha256_file(path) != str(record.get("sha256") or ""):
            raise MicrostructureBacktestError("market_ticks 清单文件 SHA256 不一致")
        table = parquet.read_table(path)
        missing = _REQUIRED_COLUMNS - set(table.column_names)
        if missing:
            raise MicrostructureBacktestError(
                f"market_ticks 缺少字段: {', '.join(sorted(missing))}"
            )
        for row in table.to_pylist():
            if str(row.get("event_type") or "") != "tick":
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            timestamp = int(row.get("exchange_ts_ms") or 0)
            event_id = str(row.get("event_id") or "").strip()
            if symbol not in wanted or not (start_ms <= timestamp <= end_ms):
                continue
            if not event_id or event_id in seen:
                continue
            _validate_tick(row)
            seen.add(event_id)
            rows.append(row)
    rows.sort(
        key=lambda row: (
            int(row["exchange_ts_ms"]),
            int(row["received_ts_ms"]),
            str(row["event_id"]),
        )
    )
    if not rows:
        raise MicrostructureBacktestError("执行快照在请求区间内没有真实 Tick")
    return rows


def run_microstructure_backtest(
    *,
    task_id: str,
    task: dict[str, Any],
    manifest: dict[str, Any],
    data_root: str | Path,
    daa_root: str | Path | None = None,
) -> dict[str, Any]:
    strategy = dict(task.get("strategy") or {})
    strategy_id = str(strategy.get("id") or "")
    if strategy_id != MICROSTRUCTURE_STRATEGY_ID and not strategy_id.startswith("ai_"):
        raise MicrostructureBacktestError("microstructure 策略不存在")
    universe = dict(task.get("universe") or {})
    period = dict(task.get("period") or {})
    execution = dict(task.get("execution") or {})
    parameters = dict(task.get("parameters") or {})
    ticks = load_manifest_ticks(
        data_root=data_root,
        manifest=manifest,
        symbols=list(universe.get("symbols") or []),
        start=str(period.get("start") or ""),
        end=str(period.get("end") or ""),
    )
    if strategy_id.startswith("ai_"):
        if not daa_root:
            raise MicrostructureBacktestError("DAA portable 策略缺少 DAA 根目录")
        raw = replay_portable_daa_ticks(
            ticks,
            strategy=strategy,
            daa_root=daa_root,
            capital=float(execution.get("capital") or 1_000_000),
            fee_rate=float(execution.get("rate") or 0),
            slippage_bps=float(execution.get("slippage") or 0),
            parameters=parameters,
        )
    else:
        raw = replay_order_book_imbalance(
            ticks,
            capital=float(execution.get("capital") or 1_000_000),
            fee_rate=float(execution.get("rate") or 0),
            slippage_bps=float(execution.get("slippage") or 0),
            parameters=parameters,
        )
    from app.replay import build_replay_audit

    snapshot = dict((task.get("data") or {}).get("snapshot") or {})
    replay_events: list[dict[str, Any]] = [
        {
            "event_type": "market_tick",
            "event_time": row.get("exchange_ts"),
            "available_at": row.get("received_at") or row.get("received_ts_ms"),
            "payload": row,
            "symbol": row.get("symbol"),
            "source_seq": index,
        }
        for index, row in enumerate(ticks)
    ]
    for index, item in enumerate(raw.get("orders") or []):
        replay_events.append(
            {
                "event_type": "order",
                "event_time": item.get("signal_time") or item.get("fill_time"),
                "payload": item,
                "symbol": item.get("symbol"),
                "source_seq": len(replay_events) + index,
            }
        )
    for index, item in enumerate(raw.get("deals") or []):
        replay_events.append(
            {
                "event_type": "fill",
                "event_time": item.get("exit_time") or item.get("fill_time"),
                "payload": item,
                "symbol": item.get("symbol"),
                "source_seq": len(replay_events) + index,
            }
        )
    for index, item in enumerate(raw.get("equity_curve") or []):
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
        "engine_type": "microstructure",
        "strategy": strategy,
        "run": {
            "universe": universe,
            "period": period,
            "execution": execution,
            "parameters": parameters,
        },
        "data_snapshot": snapshot,
        "metrics": raw["metrics"],
        "curves": {"equity": raw["equity_curve"], "drawdown": raw["drawdown_curve"]},
        "orders": raw["orders"],
        "deals": raw["deals"],
        "diagnostics": {
            **raw["diagnostics"],
            "snapshot_enforcement": "manifest_bound",
            "data_source_policy": "pxydata_snapshot_only",
            "tick_contract": "pxydata.market_ticks.v1",
        },
        "versions": {
            "strategy_source_hash": strategy.get("source_hash"),
            "snapshot_id": snapshot.get("snapshot_id"),
            "manifest_sha256": snapshot.get("manifest_sha256"),
        },
        "replay_audit": build_replay_audit(
            run_id=task_id,
            snapshot_id=str(snapshot.get("snapshot_id") or task_id),
            events=replay_events,
        ),
        "_replay_events": replay_events,
    }


def replay_portable_daa_ticks(
    ticks: list[dict[str, Any]],
    *,
    strategy: dict[str, Any],
    daa_root: str | Path,
    capital: float,
    fee_rate: float,
    slippage_bps: float,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """以真实 Tick、下一 Tick 成交和一档盘口撮合 DAA 信号。"""
    from app.daa_portable import evaluate_filter, load_strategy, tick_frame

    module = load_strategy(strategy=strategy, daa_root=daa_root)
    meta = dict(getattr(module, "META", {}) or {})
    direction = str(meta.get("direction") or "long").lower()
    latency = max(1, int(parameters.get("latency_ticks", 1)))
    max_hold = max(1, int(parameters.get("max_hold_ticks", 100)))
    quantity = max(float(parameters.get("quantity", 1.0)), 0.0)
    balance = capital
    position: dict[str, Any] | None = None
    pending: tuple[int, int] | None = None
    orders: list[dict[str, Any]] = []
    deals: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    signals = 0
    for index, tick in enumerate(ticks):
        if pending and index >= pending[0]:
            target = pending[1]
            price = float(tick["ask_price1"] if target > 0 else tick["bid_price1"])
            price *= 1 + (slippage_bps / 10_000.0) * (1 if target > 0 else -1)
            if position is None and target:
                position = {"side": target, "entry_index": index, "entry_time": tick["exchange_ts"], "entry_price": price, "symbol": tick["symbol"]}
                orders.append({"order_id": f"daa-tick-{index}", "symbol": tick["symbol"], "action": "enter_long" if target > 0 else "enter_short", "fill_time": tick["exchange_ts"], "price": price, "quantity": quantity, "status": "filled"})
            elif position is not None and (target == 0 or target != int(position["side"])):
                pnl = int(position["side"]) * (price - float(position["entry_price"])) * quantity
                fees = (float(position["entry_price"]) + price) * quantity * fee_rate
                net = pnl - fees
                balance += net
                deals.append({"symbol": position["symbol"], "side": "long" if position["side"] > 0 else "short", "entry_time": position["entry_time"], "exit_time": tick["exchange_ts"], "entry_price": position["entry_price"], "exit_price": price, "quantity": quantity, "pnl_amount": net, "pnl_pct": net / capital if capital else 0.0, "holding_ticks": index - int(position["entry_index"])})
                orders.append({"order_id": f"daa-tick-{index}", "symbol": tick["symbol"], "action": "exit", "fill_time": tick["exchange_ts"], "price": price, "quantity": quantity, "status": "filled"})
                equity_curve.append({"date": tick["exchange_ts"], "value": balance})
                position = None
                if target:
                    pending = (index, target)
                    continue
            pending = None

        if pending is not None:
            continue
        if index + latency >= len(ticks):
            continue
        strategy_params = parameters.get("strategy_params") if isinstance(parameters.get("strategy_params"), dict) else parameters
        active = evaluate_filter(module, tick_frame(tick), strategy_params)
        signals += int(active)
        target = (1 if direction != "short" else -1) if active else 0
        if position is not None and index - int(position["entry_index"]) >= max_hold:
            target = 0
        if (position is None and target) or (position is not None and target != int(position["side"])):
            pending = (index + latency, target)

    if position is not None:
        final = ticks[-1]
        price = float(final["bid_price1"] if position["side"] > 0 else final["ask_price1"])
        pnl = int(position["side"]) * (price - float(position["entry_price"])) * quantity
        fees = (float(position["entry_price"]) + price) * quantity * fee_rate
        net = pnl - fees
        balance += net
        deals.append({"symbol": position["symbol"], "side": "long" if position["side"] > 0 else "short", "entry_time": position["entry_time"], "exit_time": final["exchange_ts"], "entry_price": position["entry_price"], "exit_price": price, "quantity": quantity, "pnl_amount": net, "pnl_pct": net / capital if capital else 0.0, "holding_ticks": len(ticks) - int(position["entry_index"]), "exit_reason": "end_of_snapshot"})
        equity_curve.append({"date": final["exchange_ts"], "value": balance})
    if not equity_curve:
        equity_curve = [{"date": ticks[0]["exchange_ts"], "value": capital}]
    return {
        "metrics": {"total_return": balance / capital - 1 if capital else 0.0, "final_equity": balance, "net_profit": balance - capital, "n_trades": len(deals), "signals": signals, "fill_rate": 1.0 if orders else 0.0, "matching_model": "next_tick_visible_l1_daa_polars_expr"},
        "equity_curve": equity_curve,
        "drawdown_curve": [],
        "orders": orders,
        "deals": deals,
        "diagnostics": {"tick_count": len(ticks), "signals_submitted": signals, "orders_filled": len(orders), "matching_model": "next_tick_visible_l1_daa_polars_expr", "daa_strategy": strategy.get("id")},
    }


def replay_order_book_imbalance(
    ticks: list[dict[str, Any]],
    *,
    capital: float,
    fee_rate: float,
    slippage_bps: float,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """按下一 Tick 的可见一档深度撮合盘口不平衡信号。"""
    threshold = float(parameters.get("entry_threshold", 0.2))
    exit_threshold = float(parameters.get("exit_threshold", 0.0))
    latency_ticks = int(parameters.get("latency_ticks", 1))
    max_hold_ticks = int(parameters.get("max_hold_ticks", 100))
    quantity = float(parameters.get("quantity", 1.0))
    multiplier = float(parameters.get("contract_multiplier", 1.0))
    if not 0 < threshold < 1:
        raise MicrostructureBacktestError("entry_threshold 必须在 (0, 1) 内")
    if not 0 <= exit_threshold < threshold:
        raise MicrostructureBacktestError("exit_threshold 必须在 [0, entry_threshold) 内")
    if latency_ticks < 1 or max_hold_ticks < 1 or quantity <= 0 or multiplier <= 0:
        raise MicrostructureBacktestError("延迟、持有 Tick、数量和合约乘数必须大于 0")

    spreads: list[float] = []
    imbalances: list[float] = []
    orders: list[dict[str, Any]] = []
    deals: list[dict[str, Any]] = []
    slippage_costs: list[float] = []
    equity_curve: list[dict[str, Any]] = []
    balance = capital
    peak = capital
    position: dict[str, Any] | None = None
    pending: dict[str, Any] | None = None
    submitted = 0
    filled = 0

    def schedule(action: str, index: int, row: dict[str, Any]) -> dict[str, Any]:
        nonlocal submitted
        submitted += 1
        return {
            "action": action,
            "signal_index": index,
            "execute_index": index + latency_ticks,
            "signal_time": row["exchange_ts"],
            "signal_mid": _mid(row),
        }

    for index, tick in enumerate(ticks):
        bid = float(tick["bid_price1"])
        ask = float(tick["ask_price1"])
        spread = ask - bid
        imbalance = _imbalance(tick)
        spreads.append(spread)
        imbalances.append(imbalance)

        if pending is not None and index >= int(pending["execute_index"]):
            action = str(pending["action"])
            side = 1 if action in {"enter_long", "exit_short"} else -1
            depth = float(tick["ask_volume1"] if side > 0 else tick["bid_volume1"])
            if depth >= quantity:
                base_price = ask if side > 0 else bid
                fill_price = base_price * (
                    1 + (slippage_bps / 10_000.0) * (1 if side > 0 else -1)
                )
                mid = _mid(tick)
                cost_bps = (
                    (fill_price - mid) / mid * 10_000
                    if side > 0
                    else (mid - fill_price) / mid * 10_000
                )
                slippage_costs.append(cost_bps)
                filled += 1
                order = {
                    "order_id": f"micro-{submitted}-{filled}",
                    "symbol": str(tick["symbol"]),
                    "side": "buy" if side > 0 else "sell",
                    "action": action,
                    "signal_time": pending["signal_time"],
                    "fill_time": tick["exchange_ts"],
                    "price": fill_price,
                    "quantity": quantity,
                    "visible_depth": depth,
                    "latency_ticks": index - int(pending["signal_index"]),
                    "status": "filled",
                }
                orders.append(order)
                if action.startswith("enter_"):
                    position = {
                        "side": 1 if action == "enter_long" else -1,
                        "entry_index": index,
                        "entry_time": tick["exchange_ts"],
                        "entry_price": fill_price,
                        "symbol": str(tick["symbol"]),
                    }
                elif position is not None:
                    pnl = (
                        position["side"]
                        * (fill_price - float(position["entry_price"]))
                        * quantity
                        * multiplier
                    )
                    fees = (
                        (float(position["entry_price"]) + fill_price)
                        * quantity
                        * multiplier
                        * fee_rate
                    )
                    net = pnl - fees
                    balance += net
                    peak = max(peak, balance)
                    deals.append(
                        {
                            "symbol": position["symbol"],
                            "side": "long" if position["side"] > 0 else "short",
                            "entry_time": position["entry_time"],
                            "exit_time": tick["exchange_ts"],
                            "entry_price": position["entry_price"],
                            "exit_price": fill_price,
                            "quantity": quantity,
                            "pnl_amount": net,
                            "pnl_pct": net / capital if capital else 0.0,
                            "holding_ticks": index - int(position["entry_index"]),
                        }
                    )
                    equity_curve.append({"date": tick["exchange_ts"], "value": balance})
                    position = None
            else:
                orders.append(
                    {
                        "order_id": f"micro-{submitted}-rejected",
                        "symbol": str(tick["symbol"]),
                        "action": action,
                        "signal_time": pending["signal_time"],
                        "fill_time": tick["exchange_ts"],
                        "quantity": quantity,
                        "visible_depth": depth,
                        "status": "rejected_depth",
                    }
                )
            pending = None

        if pending is not None:
            continue
        if index + latency_ticks >= len(ticks):
            continue
        if position is None:
            if imbalance >= threshold:
                pending = schedule("enter_long", index, tick)
            elif imbalance <= -threshold:
                pending = schedule("enter_short", index, tick)
            continue
        held = index - int(position["entry_index"])
        if position["side"] > 0 and (imbalance <= exit_threshold or held >= max_hold_ticks):
            pending = schedule("exit_long", index, tick)
        elif position["side"] < 0 and (imbalance >= -exit_threshold or held >= max_hold_ticks):
            pending = schedule("exit_short", index, tick)

    if position is not None:
        final = ticks[-1]
        side = -int(position["side"])
        fill_price = float(final["ask_price1"] if side > 0 else final["bid_price1"])
        pnl = (
            position["side"]
            * (fill_price - float(position["entry_price"]))
            * quantity
            * multiplier
        )
        fees = (
            (float(position["entry_price"]) + fill_price)
            * quantity
            * multiplier
            * fee_rate
        )
        net = pnl - fees
        balance += net
        peak = max(peak, balance)
        deals.append(
            {
                "symbol": position["symbol"],
                "side": "long" if position["side"] > 0 else "short",
                "entry_time": position["entry_time"],
                "exit_time": final["exchange_ts"],
                "entry_price": position["entry_price"],
                "exit_price": fill_price,
                "quantity": quantity,
                "pnl_amount": net,
                "pnl_pct": net / capital if capital else 0.0,
                "holding_ticks": len(ticks) - 1 - int(position["entry_index"]),
                "exit_reason": "end_of_snapshot",
            }
        )
        equity_curve.append({"date": final["exchange_ts"], "value": balance})

    if not equity_curve:
        equity_curve = [{"date": ticks[0]["exchange_ts"], "value": capital}]
    drawdown_curve: list[dict[str, Any]] = []
    running_peak = capital
    for point in equity_curve:
        running_peak = max(running_peak, float(point["value"]))
        drawdown = float(point["value"]) / running_peak - 1 if running_peak else 0.0
        drawdown_curve.append({"date": point["date"], "value": drawdown})
    returns = [float(deal["pnl_pct"]) for deal in deals]
    sharpe = 0.0
    if len(returns) > 1 and statistics.pstdev(returns) > 0:
        sharpe = statistics.mean(returns) / statistics.pstdev(returns) * math.sqrt(len(returns))
    return {
        "metrics": {
            "total_return": balance / capital - 1 if capital else 0.0,
            "final_equity": balance,
            "net_profit": balance - capital,
            "max_drawdown": min((point["value"] for point in drawdown_curve), default=0.0),
            "sharpe": sharpe,
            "win_rate": (
                sum(float(deal["pnl_amount"]) > 0 for deal in deals) / len(deals)
                if deals
                else 0.0
            ),
            "n_trades": len(deals),
            "fill_rate": filled / submitted if submitted else 0.0,
            "average_spread": statistics.fmean(spreads),
            "average_abs_imbalance": statistics.fmean(abs(value) for value in imbalances),
            "realized_slippage_bps": (
                statistics.fmean(slippage_costs) if slippage_costs else 0.0
            ),
        },
        "equity_curve": equity_curve,
        "drawdown_curve": drawdown_curve,
        "orders": orders,
        "deals": deals,
        "diagnostics": {
            "tick_count": len(ticks),
            "first_tick": ticks[0]["exchange_ts"],
            "last_tick": ticks[-1]["exchange_ts"],
            "signals_submitted": submitted,
            "orders_filled": filled,
            "matching_model": "next_tick_visible_l1_ioc",
        },
    }


def _validate_tick(row: dict[str, Any]) -> None:
    bid = float(row.get("bid_price1") or 0)
    ask = float(row.get("ask_price1") or 0)
    last = float(row.get("last_price") or 0)
    if bid <= 0 or ask <= 0 or last <= 0 or bid > ask:
        raise MicrostructureBacktestError("market_ticks 存在无效价格或倒挂盘口")
    if float(row.get("bid_volume1") or 0) < 0 or float(row.get("ask_volume1") or 0) < 0:
        raise MicrostructureBacktestError("market_ticks 一档深度不能为负")
    if int(row.get("received_ts_ms") or 0) < int(row.get("exchange_ts_ms") or 0):
        raise MicrostructureBacktestError("market_ticks 接收时间早于交易所时间")


def _mid(row: dict[str, Any]) -> float:
    return (float(row["bid_price1"]) + float(row["ask_price1"])) / 2.0


def _imbalance(row: dict[str, Any]) -> float:
    bid = float(row["bid_volume1"])
    ask = float(row["ask_volume1"])
    total = bid + ask
    return (bid - ask) / total if total > 0 else 0.0


def _datetime_ms(value: str) -> int:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise MicrostructureBacktestError(f"无效回测时间: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MicrostructureBacktestError("microstructure 回测时间必须包含时区")
    return int(parsed.timestamp() * 1000)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
