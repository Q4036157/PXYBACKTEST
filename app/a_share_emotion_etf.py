"""PXYDATA情绪极值驱动的ETF日线回测引擎。"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from .metrics import compute_metrics, drawdown_curve

EMOTION_ETF_STRATEGY_ID = "etf_emotion_extreme_c_v1"
EMOTION_ETF_STRATEGY_HASH = hashlib.sha256(
    b"pxybacktest.etf-emotion-extreme-c.v1|entry<30|exit>=80|next-open|t+1|lot100"
).hexdigest()
EMOTION_DATA_CONTRACT = "pxydata.market_emotion_daily.v1"


class EmotionEtfBacktestError(ValueError):
    """情绪ETF任务的数据或执行口径不完整。"""


def runtime_available() -> bool:
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return False
    return True


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _manifest_rows(
    *, data_root: str | Path, manifest: dict[str, Any], dataset_name: str
) -> tuple[list[dict[str, Any]], int, int]:
    try:
        from pyarrow import parquet
    except ImportError as exc:
        raise EmotionEtfBacktestError("情绪ETF引擎缺少pyarrow运行依赖") from exc
    dataset = next(
        (item for item in manifest.get("datasets") or [] if item.get("name") == dataset_name),
        None,
    )
    if not isinstance(dataset, dict):
        raise EmotionEtfBacktestError(f"执行快照缺少{dataset_name}")
    root = Path(data_root).resolve()
    rows: list[dict[str, Any]] = []
    verified_size = 0
    verified_files = 0
    for record in dataset.get("files") or []:
        path = (root / str(record.get("path") or "")).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise EmotionEtfBacktestError(f"{dataset_name}文件越出数据根目录") from exc
        if not path.is_file():
            raise EmotionEtfBacktestError(f"{dataset_name}清单文件不存在")
        expected_size = int(record.get("size_bytes") or -1)
        if path.stat().st_size != expected_size:
            raise EmotionEtfBacktestError(f"{dataset_name}清单文件大小不一致")
        if _sha256_file(path) != str(record.get("sha256") or "").lower():
            raise EmotionEtfBacktestError(f"{dataset_name}清单文件SHA256不一致")
        rows.extend(parquet.read_table(path).to_pylist())
        verified_size += expected_size
        verified_files += 1
    if not rows:
        raise EmotionEtfBacktestError(f"{dataset_name}清单没有可用记录")
    return rows, verified_files, verified_size


def _day(value: Any) -> str:
    return str(value or "")[:10]


def _number(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise EmotionEtfBacktestError(f"{field}不是有效数字") from exc
    if not math.isfinite(result):
        raise EmotionEtfBacktestError(f"{field}不是有限数字")
    return result


def replay_emotion_etf(
    kline_rows: list[dict[str, Any]],
    emotion_rows: list[dict[str, Any]],
    *,
    symbol: str,
    capital: float,
    commission_rate: float,
    slippage_bps: float,
    entry_threshold: float = 30.0,
    exit_threshold: float = 80.0,
    lot_size: int = 100,
    min_commission: float = 5.0,
) -> dict[str, Any]:
    """冰点次日开盘买入、过热次日开盘卖出；持仓期间不重复加仓。"""
    wanted = symbol.strip().upper()
    bars: dict[str, dict[str, Any]] = {}
    for row in kline_rows:
        if str(row.get("symbol") or "").strip().upper() != wanted:
            continue
        day = _day(row.get("date") or row.get("trade_date") or row.get("snapshot_date") or row.get("data_date"))
        close_value = row.get("close") if row.get("close") is not None else row.get("last_price")
        try:
            valid_price = float(row.get("open")) > 0 and float(close_value) > 0
        except (TypeError, ValueError):
            valid_price = False
        if day and valid_price:
            bars[day] = {**row, "close": close_value}
    emotions: dict[str, dict[str, Any]] = {}
    for row in emotion_rows:
        day = _day(row.get("trade_date") or row.get("date"))
        if not day or str(row.get("method") or "pxydata_breadth_v1") != "pxydata_breadth_v1":
            continue
        if row.get("trade_date_ok") is not True or row.get("coverage_ok") is not True:
            continue
        available_at = str(row.get("available_at") or "")
        if available_at and _day(available_at) > day:
            raise EmotionEtfBacktestError("market_emotion_daily存在未来可用时间")
        emotions[day] = row
    dates = sorted(bars)
    if len(dates) < 2:
        raise EmotionEtfBacktestError("ETF日线交易日少于2天")
    overlap_days = sorted(set(bars) & set(emotions))
    if not overlap_days:
        raise EmotionEtfBacktestError("ETF日线与市场情绪没有重叠交易日")

    cash = float(capital)
    quantity = 0
    pending: tuple[str, str, float, str] | None = None
    buy_fill: dict[str, Any] | None = None
    orders: list[dict[str, Any]] = []
    deals: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    benchmark_curve: list[dict[str, Any]] = []
    first_close = _number(bars[dates[0]].get("close"), "close")
    slip = float(slippage_bps) / 10_000.0

    for index, day in enumerate(dates):
        bar = bars[day]
        open_price = _number(bar.get("open"), "open")
        close_price = _number(bar.get("close"), "close")
        if open_price <= 0 or close_price <= 0:
            raise EmotionEtfBacktestError("ETF价格必须大于0")

        if pending is not None and pending[0] == day:
            _, side, signal_score, signal_day = pending
            if side == "BUY" and quantity == 0:
                fill_price = open_price * (1.0 + slip)
                affordable = max(cash - min_commission, 0.0) / (
                    fill_price * (1.0 + commission_rate)
                )
                fill_quantity = int(affordable // lot_size) * lot_size
                if fill_quantity >= lot_size:
                    notional = fill_price * fill_quantity
                    commission = max(min_commission, notional * commission_rate)
                    cash -= notional + commission
                    quantity = fill_quantity
                    buy_fill = {
                        "fill_date": day,
                        "price": fill_price,
                        "quantity": fill_quantity,
                        "commission": commission,
                        "signal_score": signal_score,
                    }
                    orders.append({
                        "order_id": f"{wanted}-{day}-BUY",
                        "symbol": wanted, "side": "BUY", "signal_date": signal_day,
                        "fill_date": day, "price": fill_price, "quantity": fill_quantity,
                        "commission": commission, "status": "FILLED",
                    })
            elif side == "SELL" and quantity > 0:
                fill_price = open_price * (1.0 - slip)
                notional = fill_price * quantity
                commission = max(min_commission, notional * commission_rate)
                cash += notional - commission
                assert buy_fill is not None
                invested = buy_fill["price"] * quantity + buy_fill["commission"]
                proceeds = notional - commission
                pnl = proceeds - invested
                orders.append({
                    "order_id": f"{wanted}-{day}-SELL",
                    "symbol": wanted, "side": "SELL", "signal_date": signal_day, "fill_date": day,
                    "price": fill_price, "quantity": quantity,
                    "commission": commission, "status": "FILLED",
                })
                deals.append({
                    "symbol": wanted,
                    "entry_date": buy_fill["fill_date"],
                    "exit_date": day,
                    "quantity": quantity,
                    "entry_price": buy_fill["price"],
                    "exit_price": fill_price,
                    "entry_score": buy_fill["signal_score"],
                    "exit_score": signal_score,
                    "commission": buy_fill["commission"] + commission,
                    "stamp_tax": 0.0,
                    "pnl": pnl,
                    "return": pnl / invested if invested > 0 else 0.0,
                })
                quantity = 0
                buy_fill = None
            pending = None

        emotion = emotions.get(day)
        score = _number(emotion.get("emotion_score"), "emotion_score") if emotion else None
        label = str(emotion.get("emotion_label") or "") if emotion else ""
        next_day = dates[index + 1] if index + 1 < len(dates) else None
        if emotion is not None and next_day and pending is None:
            if quantity == 0 and score is not None and score < entry_threshold:
                pending = (next_day, "BUY", score, day)
                signals.append({"signal_date": day, "fill_date": next_day, "side": "BUY", "score": score, "label": label})
            elif quantity > 0 and score is not None and score >= exit_threshold:
                pending = (next_day, "SELL", score, day)
                signals.append({"signal_date": day, "fill_date": next_day, "side": "SELL", "score": score, "label": label})

        equity = cash + quantity * close_price
        equity_curve.append({"date": day, "value": equity, "cash": cash, "position": quantity, "close": close_price, "emotion_score": score})
        benchmark_curve.append({"date": day, "value": capital * close_price / first_close})

    metrics = compute_metrics(equity_curve, deals=deals, benchmark=benchmark_curve)
    drawdowns = drawdown_curve(equity_curve)
    metrics.update({
        "signal_count": len(signals),
        "entry_count": sum(1 for item in orders if item["side"] == "BUY"),
        "exit_count": sum(1 for item in orders if item["side"] == "SELL"),
        "open_position": quantity,
        "ending_cash": cash,
        "ending_equity": equity_curve[-1]["value"],
    })
    return {
        "metrics": metrics,
        "equity_curve": equity_curve,
        "drawdown_curve": [
            {"date": point["date"], "value": value}
            for point, value in zip(equity_curve, drawdowns)
        ],
        "benchmark_curve": benchmark_curve,
        "orders": orders,
        "deals": deals,
        "signals": signals,
        "position": {"symbol": wanted, "quantity": quantity, "cost": buy_fill},
        "diagnostics": {
            "data_start": dates[0], "data_end": dates[-1], "data_count": len(dates),
            "emotion_days": len(emotions), "emotion_overlap_days": len(overlap_days), "kline_days": len(bars),
            "entry_threshold": entry_threshold, "exit_threshold": exit_threshold,
            "fill_policy": "next_bar_open", "t_plus_one": True,
            "lot_size": lot_size, "min_commission": min_commission,
            "stamp_tax_bps": 0.0,
        },
    }


def run_emotion_etf_backtest(
    *, task_id: str, task: dict[str, Any], manifest: dict[str, Any], data_root: str | Path
) -> dict[str, Any]:
    strategy = dict(task.get("strategy") or {})
    if strategy.get("id") != EMOTION_ETF_STRATEGY_ID:
        raise EmotionEtfBacktestError("情绪ETF策略标识不一致")
    universe = dict(task.get("universe") or {})
    symbols = list(universe.get("symbols") or [])
    if len(symbols) != 1:
        raise EmotionEtfBacktestError("情绪ETF回测要求单一ETF")
    period = dict(task.get("period") or {})
    execution = dict(task.get("execution") or {})
    parameters = dict(task.get("parameters") or {})
    kline_rows, k_files, k_size = _manifest_rows(data_root=data_root, manifest=manifest, dataset_name="etf_snapshots")
    emotion_rows, e_files, e_size = _manifest_rows(data_root=data_root, manifest=manifest, dataset_name="market_emotion_daily")
    start, end = _day(period.get("start")), _day(period.get("end"))
    kline_rows = [row for row in kline_rows if start <= _day(row.get("date") or row.get("trade_date") or row.get("snapshot_date") or row.get("data_date")) <= end]
    emotion_rows = [row for row in emotion_rows if start <= _day(row.get("trade_date") or row.get("date")) <= end]
    raw = replay_emotion_etf(
        kline_rows, emotion_rows, symbol=symbols[0],
        capital=float(execution.get("capital") or 1_000_000),
        commission_rate=float(execution.get("commission_bps") or 0) / 10_000.0,
        slippage_bps=float(execution.get("slippage_bps") or 0),
        entry_threshold=float(parameters.get("entry_threshold", 30)),
        exit_threshold=float(parameters.get("exit_threshold", 80)),
        lot_size=int(parameters.get("lot_size", 100)),
        min_commission=float(parameters.get("min_commission", 5)),
    )
    from .replay import build_replay_audit
    snapshot = dict((task.get("data") or {}).get("snapshot") or {})
    events = [
        {"event_type": "signal", "event_time": item["signal_date"], "symbol": symbols[0], "payload": item, "source_seq": i}
        for i, item in enumerate(raw["signals"])
    ]
    events.extend(
        {"event_type": "order", "event_time": item["fill_date"], "symbol": symbols[0], "payload": item, "source_seq": len(events) + i}
        for i, item in enumerate(raw["orders"])
    )
    events.extend(
        {"event_type": "account", "event_time": item["date"], "payload": item, "source_seq": len(events) + i}
        for i, item in enumerate(raw["equity_curve"])
    )
    warnings: list[str] = []
    if len(raw["deals"]) < 10:
        warnings.append("完整交易轮次少于10，结论属于小样本。")
    if abs(float(raw["metrics"]["max_drawdown"])) > 0.15:
        warnings.append("历史最大回撤超过账户15%目标。")
    return {
        "schema_version": 2,
        "contract_version": "pxybacktest.task-result.v2",
        "task_id": task_id,
        "engine_type": "a_share_emotion_etf",
        "strategy": strategy,
        "data_snapshot": snapshot,
        "run": {"universe": universe, "period": period, "execution": execution, "parameters": parameters},
        "metrics": raw["metrics"],
        "curves": {"equity": raw["equity_curve"], "drawdown": raw["drawdown_curve"], "benchmark": raw["benchmark_curve"]},
        "market": {"signals": raw["signals"]},
        "orders": raw["orders"],
        "deals": raw["deals"],
        "positions": [raw["position"]] if raw["position"]["quantity"] else [],
        "diagnostics": {
            **raw["diagnostics"],
            "verified_file_count": k_files + e_files,
            "verified_size_bytes": k_size + e_size,
            "snapshot_enforcement": "manifest_bound",
            "data_source_policy": "pxydata_snapshot_only",
            "emotion_contract": EMOTION_DATA_CONTRACT,
            "warnings": warnings,
        },
        "versions": {"strategy_source_hash": EMOTION_ETF_STRATEGY_HASH, "snapshot_id": snapshot.get("snapshot_id"), "manifest_sha256": snapshot.get("manifest_sha256")},
        "replay_audit": build_replay_audit(run_id=task_id, snapshot_id=str(snapshot.get("snapshot_id") or task_id), events=events),
        "_replay_events": events,
    }


__all__ = [
    "EMOTION_DATA_CONTRACT", "EMOTION_ETF_STRATEGY_HASH", "EMOTION_ETF_STRATEGY_ID",
    "EmotionEtfBacktestError", "replay_emotion_etf", "run_emotion_etf_backtest", "runtime_available",
]
