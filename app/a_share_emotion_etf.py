"""PXYDATA情绪极值驱动的ETF日线回测引擎。"""

from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .metrics import compute_metrics, drawdown_curve

EMOTION_ETF_STRATEGY_ID = "etf_emotion_extreme_c_v1"
EMOTION_ETF_STRATEGY_HASH = hashlib.sha256(
    b"pxybacktest.etf-emotion-extreme-c.v1|entry<30|exit>=80|next-open|t+1|lot100"
).hexdigest()
EMOTION_DATA_CONTRACT = "pxydata.market_emotion_daily.v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")


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
        rows.extend(parquet.ParquetFile(path).read().to_pylist())
        verified_size += expected_size
        verified_files += 1
    if not rows:
        raise EmotionEtfBacktestError(f"{dataset_name}清单没有可用记录")
    return rows, verified_files, verified_size


def _day(value: Any) -> str:
    return str(value or "")[:10]


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise EmotionEtfBacktestError(f"{field}不是有效数字")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise EmotionEtfBacktestError(f"{field}不是有效数字") from exc
    if not math.isfinite(result):
        raise EmotionEtfBacktestError(f"{field}不是有限数字")
    return result


def _trade_day(row: dict[str, Any]) -> str:
    value = row.get("date") or row.get("trade_date") or row.get("data_date")
    try:
        return date.fromisoformat(str(value or "")).isoformat()
    except ValueError as exc:
        raise EmotionEtfBacktestError("正式日线缺少有效交易日期") from exc


def _session_time(day: str, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(hour, minute), SHANGHAI)


def _available_time(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("missing timezone")
        return parsed.astimezone(SHANGHAI)
    except (ValueError, OverflowError) as exc:
        raise EmotionEtfBacktestError("market_emotion_daily.available_at必须是完整带时区时间") from exc


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
        day = _trade_day(row)
        normalized = {"date": day, "symbol": wanted}
        for field in ("open", "close"):
            value = _number(row.get(field), field)
            if value <= 0:
                raise EmotionEtfBacktestError(f"正式日线{field}必须为有限正数")
            normalized[field] = value
        for field in ("high", "low", "volume", "amount"):
            if row.get(field) is not None:
                normalized[field] = _number(row[field], field)
        if day in bars and bars[day] != normalized:
            raise EmotionEtfBacktestError(f"正式日线{day}重复记录不一致")
        bars[day] = normalized
    emotions: dict[str, dict[str, Any]] = {}
    for row in emotion_rows:
        if str(row.get("method") or "pxydata_breadth_v1") != "pxydata_breadth_v1":
            continue
        if row.get("trade_date_ok") is not True or row.get("coverage_ok") is not True:
            continue
        day = _trade_day(row)
        available_at = _available_time(row.get("available_at"))
        if available_at < _session_time(day, 0):
            raise EmotionEtfBacktestError("market_emotion_daily可用时间早于所属交易日")
        normalized_emotion = {
            "available_at": available_at,
            "emotion_score": _number(row.get("emotion_score"), "emotion_score"),
            "emotion_label": str(row.get("emotion_label") or ""),
        }
        if day in emotions and emotions[day] != normalized_emotion:
            raise EmotionEtfBacktestError(f"market_emotion_daily{day}重复记录不一致")
        emotions[day] = normalized_emotion
    dates = sorted(bars)
    if len(dates) < 2:
        raise EmotionEtfBacktestError("ETF日线交易日少于2天")
    overlap_days = sorted(set(bars) & set(emotions))
    if not overlap_days:
        raise EmotionEtfBacktestError("ETF日线与市场情绪没有重叠交易日")
    missing_emotion_dates = sorted(set(bars) - set(emotions))

    next_dates = dict(zip(dates, dates[1:]))
    timeline: list[tuple[datetime, int, str]] = []
    for day in dates:
        timeline.extend([(_session_time(day, 9, 30), 0, day), (_session_time(day, 15), 2, day)])
        emotion = emotions.get(day)
        if emotion is None:
            continue
        available_at = emotion["available_at"]
        next_day = next_dates.get(day)
        if next_day and available_at >= _session_time(next_day, 9, 30):
            raise EmotionEtfBacktestError(f"{day}情绪在目标next-open前不可用，禁止倒填成交")
        if available_at.date().isoformat() <= dates[-1]:
            timeline.append((available_at, 1, day))
    timeline.sort()

    cash = float(capital)
    quantity = 0
    pending: tuple[str, str, float, str, str] | None = None
    buy_fill: dict[str, Any] | None = None
    orders: list[dict[str, Any]] = []
    deals: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    benchmark_curve: list[dict[str, Any]] = []
    first_close = _number(bars[dates[0]].get("close"), "close")
    slip = float(slippage_bps) / 10_000.0
    visible_scores: dict[str, float] = {}
    events: list[dict[str, Any]] = []

    def record(event_type: str, instant: datetime, payload: dict[str, Any]) -> None:
        timestamp = instant.isoformat()
        events.append({
            "event_type": event_type, "event_time": timestamp, "available_at": timestamp,
            "symbol": wanted if event_type != "sentiment" else None,
            "payload": dict(payload), "source": "adapter", "source_seq": len(events),
            # 同刻使用执行顺序，避免类型优先级把后续信号移到成交之前。
            "priority": 100,
        })

    # 只在实际可用时产生信号；收盘价格仅进入15:00估值阶段。
    for instant, phase, day in timeline:
        bar = bars[day]
        if phase == 1:
            emotion = emotions[day]
            score = emotion["emotion_score"]
            visible_scores[day] = score
            record("sentiment", instant, {
                "trade_date": day, "available_at": instant.isoformat(),
                "emotion_score": score, "emotion_label": emotion["emotion_label"],
                "score": score, "title": f"市场情绪 {emotion['emotion_label']}",
                "method": "pxydata_breadth_v1", "contract": EMOTION_DATA_CONTRACT,
            })
            next_day = next_dates.get(day)
            if next_day and pending is None:
                side = "BUY" if quantity == 0 and score < entry_threshold else (
                    "SELL" if quantity > 0 and score >= exit_threshold else None
                )
                if side:
                    signal_time = instant.isoformat()
                    pending = (next_day, side, score, day, signal_time)
                    signals.append({
                        "signal_date": day, "signal_time": signal_time,
                        "available_at": signal_time, "fill_date": next_day,
                        "fill_time": _session_time(next_day, 9, 30).isoformat(),
                        "side": side, "score": score, "label": emotion["emotion_label"],
                    })
                    record("signal", instant, signals[-1])
            continue
        if phase == 2:
            close_price = bar["close"]
            equity = cash + quantity * close_price
            record("market_bar", instant, {**bar, "datetime": instant.isoformat()})
            equity_curve.append({
                "date": day, "event_time": instant.isoformat(), "value": equity,
                "cash": cash, "position": quantity, "close": close_price,
                "emotion_score": visible_scores.get(day),
            })
            benchmark_curve.append({"date": day, "value": capital * close_price / first_close})
            record("account", instant, equity_curve[-1])
            continue

        order_count = len(orders)
        if pending is not None and pending[0] == day:
            _, side, signal_score, signal_day, signal_time = pending
            open_price = bar["open"]
            fill_time = instant.isoformat()
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
                        "fill_time": fill_time,
                        "price": fill_price,
                        "quantity": fill_quantity,
                        "commission": commission,
                        "signal_score": signal_score,
                    }
                    orders.append({
                        "order_id": f"{wanted}-{day}-BUY",
                        "symbol": wanted, "side": "BUY", "signal_date": signal_day,
                        "signal_time": signal_time, "fill_time": fill_time,
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
                    "signal_time": signal_time, "fill_time": fill_time,
                    "price": fill_price, "quantity": quantity,
                    "commission": commission, "status": "FILLED",
                })
                deals.append({
                    "symbol": wanted,
                    "entry_date": buy_fill["fill_date"],
                    "exit_date": day,
                    "entry_time": buy_fill["fill_time"],
                    "exit_time": fill_time,
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

        if len(orders) > order_count:
            order = orders[-1]
            record("order", instant, order)
            record("fill", instant, {**order, "fill_id": f"{order['order_id']}-fill"})
            record("position", instant, {
                "symbol": wanted, "quantity": quantity, "cost": buy_fill,
                "event_time": instant.isoformat(),
            })
        record("account", instant, {
            "date": day, "event_time": instant.isoformat(), "cash": cash,
            "position": quantity, "mark_price": bar["open"],
            "value": cash + quantity * bar["open"], "valuation": "open",
        })

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
        "_replay_events": events,
        "diagnostics": {
            "data_start": dates[0], "data_end": dates[-1], "data_count": len(dates),
            "emotion_days": len(emotions), "emotion_overlap_days": len(overlap_days), "kline_days": len(bars),
            "missing_emotion_days": len(missing_emotion_dates),
            "missing_emotion_dates": missing_emotion_dates,
            "emotion_coverage_complete": not missing_emotion_dates,
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
    data = dict(task.get("data") or {})
    selection = dict(data.get("selection") or {})
    snapshot = dict(data.get("snapshot") or {})
    requested = set(selection.get("datasets") or [item["name"] for item in snapshot.get("datasets") or []])
    manifest_names = {item.get("name") for item in manifest.get("datasets") or []}
    if "etf_snapshots" in requested or "etf_snapshots" in manifest_names:
        raise EmotionEtfBacktestError("unsupported: etf_snapshots不是正式日线，请新建kline_etf_daily任务；禁止混用价格源")
    if "kline_daily" in requested or "kline_daily" in manifest_names:
        raise EmotionEtfBacktestError("禁止混用股票kline_daily与ETF价格源")
    if "kline_etf_daily" not in manifest_names:
        raise EmotionEtfBacktestError("执行快照缺少正式日线kline_etf_daily")
    kline_rows, k_files, k_size = _manifest_rows(data_root=data_root, manifest=manifest, dataset_name="kline_etf_daily")
    emotion_rows, e_files, e_size = _manifest_rows(data_root=data_root, manifest=manifest, dataset_name="market_emotion_daily")
    start, end = _day(period.get("start")), _day(period.get("end"))
    kline_rows = [row for row in kline_rows if str(row.get("symbol") or "").strip().upper() == symbols[0].strip().upper() and start <= _trade_day(row) <= end]
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
    from .replay import ReplayAudit
    events = raw["_replay_events"]
    audit = ReplayAudit(run_id=task_id, snapshot_id=str(snapshot.get("snapshot_id") or task_id))
    for event in events:
        audit.record(event["event_type"], event["payload"])
    warnings: list[str] = []
    if raw["diagnostics"]["missing_emotion_days"]:
        warnings.append(
            f"市场情绪缺失或质量不通过{raw['diagnostics']['missing_emotion_days']}个ETF交易日；"
            "缺失日不生成新信号，已有待成交不受影响。"
        )
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
        "market": {
            "signals": raw["signals"],
            "bars": [event["payload"] for event in events if event["event_type"] == "market_bar"],
            "sentiment": [event["payload"] for event in events if event["event_type"] == "sentiment"],
        },
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
            "price_dataset": "kline_etf_daily",
            "timezone": "Asia/Shanghai",
            "execution_model": "precomputed_result_replay",
            "replay_semantics": "result_replay",
            "warnings": warnings,
        },
        "versions": {"strategy_source_hash": EMOTION_ETF_STRATEGY_HASH, "snapshot_id": snapshot.get("snapshot_id"), "manifest_sha256": snapshot.get("manifest_sha256")},
        "replay_audit": audit.to_dict(),
        "_replay_events": events,
    }


__all__ = [
    "EMOTION_DATA_CONTRACT", "EMOTION_ETF_STRATEGY_HASH", "EMOTION_ETF_STRATEGY_ID",
    "EmotionEtfBacktestError", "replay_emotion_etf", "run_emotion_etf_backtest", "runtime_available",
]
