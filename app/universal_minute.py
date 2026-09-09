"""Task v2 adapter for the manifest-bound universal one-minute engine."""

from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .kernel import stable_hash
from .lighter_microstructure import load_manifest_rows
from .minute_replay import (
    FillRecord,
    FundingRateEvent,
    InstrumentConfig,
    MinuteBar,
    MinuteReplayConfig,
    MinuteReplayError,
    OrderIntent,
    SentimentEvent,
    StrategyContext,
    run_minute_replay,
)
from .replay import build_replay_audit

UNIVERSAL_ENGINE_TYPE = "universal_1m"
UNIVERSAL_STRATEGY_ID = "universal_signal_v1"
UNIVERSAL_STRATEGY_HASH = hashlib.sha256(
    b"pxybacktest.universal-signal.v1"
).hexdigest()
UNIVERSAL_ENGINE_VERSION = "pxybacktest.universal-1m.v1"


class UniversalMinuteBacktestError(ValueError):
    """The Task v2 request or bound datasets cannot drive a strict replay."""


def universal_minute_runtime_available() -> bool:
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return False
    return True


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise UniversalMinuteBacktestError(f"{name} 必须是有限数值") from exc
    if not number.is_finite() or (positive and number <= 0):
        raise UniversalMinuteBacktestError(
            f"{name} 必须是{'正数' if positive else '有限数值'}"
        )
    return number


def _float(value: Decimal | str | int | float) -> float:
    return float(value)


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    return value


def _symbol(row: Mapping[str, Any]) -> str:
    return str(row.get("symbol") or row.get("instrument_id") or "").strip().upper()


def _time(row: Mapping[str, Any], *names: str) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value)
    return None


class _UniversalSignalStrategy:
    """Built-in target-position strategy used to certify the generic adapter."""

    def __init__(
        self,
        *,
        config: MinuteReplayConfig,
        parameters: Mapping[str, Any],
    ) -> None:
        self.config = config
        self.signal_mode = str(parameters.get("signal_mode") or "sentiment").lower()
        self.entry_threshold = _decimal(
            parameters.get("entry_threshold", "0.2"), "entry_threshold"
        )
        self.exit_threshold = _decimal(
            parameters.get("exit_threshold", "0.05"), "exit_threshold"
        )
        self.quantity = _decimal(parameters.get("quantity", 1), "quantity", positive=True)
        self.return_weight = _decimal(
            parameters.get("return_weight", 1), "return_weight"
        )

    def _signal(self, context: StrategyContext, bar: MinuteBar) -> Decimal | None:
        sentiment = context.latest_sentiment(bar.symbol)
        sentiment_value = sentiment.value if sentiment is not None else None
        bar_return = bar.close / bar.open - Decimal("1")
        if self.signal_mode == "sentiment":
            return sentiment_value
        if self.signal_mode == "bar_return":
            return bar_return
        if self.signal_mode == "sentiment_plus_return":
            if sentiment_value is None:
                return None
            return sentiment_value + bar_return * self.return_weight
        raise UniversalMinuteBacktestError(f"不支持的 signal_mode: {self.signal_mode}")

    def on_bar(
        self, context: StrategyContext, bar: MinuteBar
    ) -> OrderIntent | None:
        signal = self._signal(context, bar)
        if signal is None:
            return None
        position = context.position(bar.symbol)
        current = position.quantity if position is not None else Decimal("0")
        spec = self.config.instrument_for(bar.symbol)
        desired = current
        if signal >= self.entry_threshold:
            desired = self.quantity
        elif signal <= -self.entry_threshold:
            desired = -self.quantity if spec.allow_short else Decimal("0")
        elif signal.copy_abs() <= self.exit_threshold:
            desired = Decimal("0")
        delta = desired - current
        if delta == 0:
            return None
        return OrderIntent(
            symbol=bar.symbol,
            side="buy" if delta > 0 else "sell",
            quantity=delta.copy_abs(),
            tag=f"{self.signal_mode}:{signal}",
            metadata={"signal": str(signal), "target_quantity": str(desired)},
        )


def _instrument_configs(task: Mapping[str, Any]) -> list[InstrumentConfig]:
    parameters = dict(task.get("parameters") or {})
    execution = dict(task.get("execution") or {})
    symbols = list(dict(task.get("universe") or {}).get("symbols") or [])
    overrides = parameters.get("instrument_overrides")
    override_map = overrides if isinstance(overrides, dict) else {}
    result: list[InstrumentConfig] = []
    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        item = override_map.get(symbol)
        per_symbol = item if isinstance(item, dict) else {}
        result.append(
            InstrumentConfig(
                symbol=symbol,
                asset_class=str(
                    per_symbol.get("asset_class")
                    or parameters.get("asset_class")
                    or "stock"
                ),
                settlement=per_symbol.get("settlement")
                or parameters.get("settlement"),
                contract_multiplier=per_symbol.get(
                    "contract_multiplier",
                    parameters.get("contract_multiplier", 1),
                ),
                leverage=per_symbol.get(
                    "leverage",
                    parameters.get("leverage", execution.get("leverage") or 1),
                ),
                commission_rate=per_symbol.get(
                    "commission_rate",
                    Decimal(str(execution.get("commission_bps") or 0))
                    / Decimal("10000"),
                ),
                minimum_commission=per_symbol.get(
                    "minimum_commission", parameters.get("minimum_commission", 0)
                ),
                fixed_commission=per_symbol.get(
                    "fixed_commission", parameters.get("fixed_commission", 0)
                ),
                commission_per_unit=per_symbol.get(
                    "commission_per_unit", parameters.get("commission_per_unit", 0)
                ),
                slippage_bps=per_symbol.get(
                    "slippage_bps", execution.get("slippage_bps") or 0
                ),
                quantity_precision=int(
                    per_symbol.get(
                        "quantity_precision", parameters.get("quantity_precision", 8)
                    )
                ),
                allow_short=per_symbol.get("allow_short", parameters.get("allow_short")),
            )
        )
    return result


def _bars(rows: list[dict[str, Any]]) -> list[MinuteBar]:
    output: list[MinuteBar] = []
    for row in rows:
        interval = str(row.get("interval") or "").strip().lower()
        if interval != "1m":
            raise UniversalMinuteBacktestError(
                f"bars 只接受 interval=1m，实际为 {interval or 'missing'}"
            )
        event_time = _time(row, "event_time", "datetime", "timestamp_utc")
        if not event_time:
            raise UniversalMinuteBacktestError("bars 缺少 event_time")
        try:
            output.append(
                MinuteBar(
                    symbol=_symbol(row),
                    event_time=event_time,
                    available_at=_time(row, "available_at"),
                    tradable_from=_time(row, "tradable_from"),
                    revision_id=str(row.get("revision_id") or "original"),
                    open=row.get("open"),
                    high=row.get("high"),
                    low=row.get("low"),
                    close=row.get("close"),
                    volume=row.get("volume") or 0,
                    source=str(row.get("source") or "PXYDATA"),
                )
            )
        except MinuteReplayError as exc:
            raise UniversalMinuteBacktestError(str(exc)) from exc
    if not output:
        raise UniversalMinuteBacktestError("快照中没有可回放的 bars 1m 数据")
    return output


def _funding(rows: list[dict[str, Any]]) -> list[FundingRateEvent]:
    output: list[FundingRateEvent] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        direction = str(row.get("rate_direction") or "").strip().lower()
        if direction not in {"long_pays_short", "positive_long_pays_short"}:
            raise UniversalMinuteBacktestError(
                "funding_rates.rate_direction 必须声明正费率由多头支付空头"
            )
        event_time = _time(row, "settlement_time", "event_time", "funding_time")
        symbol = _symbol(row)
        if not event_time:
            raise UniversalMinuteBacktestError("funding_rates 缺少 settlement_time")
        key = (symbol, event_time)
        if key in seen:
            raise UniversalMinuteBacktestError("同一品种同一结算时刻存在重复资金费")
        seen.add(key)
        try:
            output.append(
                FundingRateEvent(
                    symbol=symbol,
                    event_time=event_time,
                    available_at=_time(row, "available_at"),
                    tradable_from=_time(row, "tradable_from"),
                    revision_id=str(row.get("revision_id") or "original"),
                    rate=row.get("rate"),
                    mark_price=row.get("mark_price"),
                    source=str(row.get("source") or "PXYDATA"),
                )
            )
        except MinuteReplayError as exc:
            raise UniversalMinuteBacktestError(str(exc)) from exc
    return output


def _sentiments(rows: list[dict[str, Any]]) -> list[SentimentEvent]:
    output: list[SentimentEvent] = []
    for row in rows:
        scope = str(row.get("scope") or "instrument").strip().lower()
        instrument = _symbol(row)
        if scope not in {"instrument", "global"}:
            raise UniversalMinuteBacktestError(
                "sentiment_events.scope 必须是 instrument 或 global"
            )
        event_time = _time(row, "event_time")
        if not event_time:
            raise UniversalMinuteBacktestError("sentiment_events 缺少 event_time")
        value = row.get("score")
        if value is None:
            value = row.get("sentiment_score", row.get("value"))
        try:
            output.append(
                SentimentEvent(
                    symbol=None if scope == "global" else instrument,
                    event_time=event_time,
                    available_at=_time(row, "available_at"),
                    tradable_from=_time(row, "tradable_from"),
                    revision_id=str(row.get("revision_id") or "original"),
                    value=value,
                    source=str(row.get("source") or "PXYDATA"),
                    metadata={
                        "event_id": row.get("event_id"),
                        "scope": scope,
                        "instrument_id": instrument,
                    },
                )
            )
        except MinuteReplayError as exc:
            raise UniversalMinuteBacktestError(str(exc)) from exc
    return output


def _drawdown_curve(equity: list[dict[str, Any]]) -> list[dict[str, Any]]:
    peak: float | None = None
    output: list[dict[str, Any]] = []
    for point in equity:
        value = float(point["value"])
        peak = value if peak is None else max(peak, value)
        output.append(
            {
                "date": point["date"],
                "value": value / peak - 1.0 if peak else 0.0,
            }
        )
    return output


def _closed_trades(fills: tuple[FillRecord, ...]) -> list[dict[str, Any]]:
    """按平均成本法把单边 Fill 投影为已闭合往返，Fill 仍是成交事实。"""

    states: dict[str, dict[str, Any]] = {}
    deals: list[dict[str, Any]] = []
    for fill in fills:
        direction = Decimal("1") if fill.side == "buy" else Decimal("-1")
        state = states.get(fill.symbol)
        close_fee = Decimal("0")
        open_fee = fill.commission
        if fill.closed_quantity > 0 and state is not None:
            close_ratio = fill.closed_quantity / fill.quantity
            close_fee = fill.commission * close_ratio
            open_fee = fill.commission - close_fee
            entry_ratio = fill.closed_quantity / state["quantity"]
            entry_fee = state["commission"] * entry_ratio
            deal = {
                "trade_id": stable_hash(
                    {
                        "entry_fill_id": state["entry_fill_id"],
                        "exit_fill_id": fill.fill_id,
                        "quantity": str(fill.closed_quantity),
                        "index": len(deals) + 1,
                    }
                ),
                "symbol": fill.symbol,
                "side": "long" if state["direction"] > 0 else "short",
                "entry_time": state["entry_time"],
                "exit_time": fill.event_time,
                "entry_price": str(state["average_price"]),
                "exit_price": str(fill.price),
                "quantity": str(fill.closed_quantity),
                "gross_pnl": str(fill.realized_pnl),
                "commission": str(entry_fee + close_fee),
                "funding_pnl": "0",
                "pnl_amount": str(fill.realized_pnl - entry_fee - close_fee),
                "entry_fill_id": state["entry_fill_id"],
                "exit_fill_id": fill.fill_id,
            }
            deals.append(deal)
            state["quantity"] -= fill.closed_quantity
            state["commission"] -= entry_fee
            if state["quantity"] <= 0:
                states.pop(fill.symbol, None)

        if fill.opened_quantity <= 0:
            continue
        state = states.get(fill.symbol)
        if state is None or state["direction"] != direction:
            states[fill.symbol] = {
                "direction": direction,
                "quantity": fill.opened_quantity,
                "average_price": fill.price,
                "entry_time": fill.event_time,
                "entry_fill_id": fill.fill_id,
                "commission": open_fee,
            }
            continue
        combined_quantity = state["quantity"] + fill.opened_quantity
        state["average_price"] = (
            state["average_price"] * state["quantity"]
            + fill.price * fill.opened_quantity
        ) / combined_quantity
        state["quantity"] = combined_quantity
        state["commission"] += open_fee
    return deals


def run_universal_minute_backtest(
    *,
    task_id: str,
    task: dict[str, Any],
    manifest: dict[str, Any],
    data_root: str | Path,
) -> dict[str, Any]:
    parameters = dict(task.get("parameters") or {})
    execution = dict(task.get("execution") or {})
    universe = dict(task.get("universe") or {})
    period = dict(task.get("period") or {})
    symbols = [str(item).strip().upper() for item in universe.get("symbols") or []]
    requested_datasets = ["bars"]
    if execution.get("funding_fee"):
        requested_datasets.append("funding_rates")
    if str(parameters.get("signal_mode") or "sentiment").lower() != "bar_return":
        requested_datasets.append("sentiment_events")
    rows: dict[str, list[dict[str, Any]]] = {
        name: load_manifest_rows(
            data_root=data_root,
            manifest=manifest,
            dataset_name=name,
            symbols=symbols,
            start=str(period.get("start") or ""),
            end=str(period.get("end") or ""),
        )
        for name in requested_datasets
    }
    bars = _bars(rows["bars"])
    funding = _funding(rows.get("funding_rates", []))
    sentiments = _sentiments(rows.get("sentiment_events", []))
    snapshot = dict((task.get("data") or {}).get("snapshot") or {})
    config = MinuteReplayConfig(
        initial_cash=execution.get("capital") or 1_000_000,
        instruments=_instrument_configs(task),
        run_id=task_id,
        snapshot_id=str(snapshot.get("snapshot_id") or task_id),
    )
    replay = run_minute_replay(
        config=config,
        strategy=_UniversalSignalStrategy(config=config, parameters=parameters),
        bars=bars,
        funding_events=funding,
        sentiment_events=sentiments,
    )
    deals = _closed_trades(replay.fills)
    equity = [
        {
            "date": point.event_time,
            "value": _float(point.equity),
            "cash": _float(point.cash),
            "used_margin": _float(point.used_margin),
            "available_cash": _float(point.available_cash),
            "realized_pnl": _float(point.realized_pnl),
            "unrealized_pnl": _float(point.unrealized_pnl),
            "funding_pnl": _float(point.funding_pnl),
        }
        for point in replay.account_curve
    ]
    replay_events = [
        {
            "event_type": item.event_type,
            # ResultReplayController 的时钟使用可见时间；原始市场时间保留在载荷中。
            # 显式 priority 冻结纯内核已经完成的因果顺序，禁止第二次回放重排。
            "event_time": item.ready_time,
            "available_at": item.ready_time,
            "symbol": item.symbol,
            "payload": {
                "timeline_event_time": item.event_time,
                **_plain(item.payload),
            },
            "source_seq": item.seq,
            "priority": item.seq,
        }
        for item in replay.timeline
    ]
    audit = build_replay_audit(
        run_id=task_id,
        snapshot_id=config.snapshot_id,
        events=replay_events,
    )
    result = {
        "schema_version": 2,
        "contract_version": "pxybacktest.task-result.v2",
        "task_id": task_id,
        "engine_type": UNIVERSAL_ENGINE_TYPE,
        "engine_version": UNIVERSAL_ENGINE_VERSION,
        "strategy": task.get("strategy") or {},
        "data_snapshot": snapshot,
        "run": {
            "default_profile": task.get("default_profile"),
            "universe": universe,
            "period": period,
            "execution": execution,
            "parameters": parameters,
            "random_seed": task.get("random_seed"),
        },
        "metrics": {
            "total_return": _float(
                replay.equity / config.initial_cash - Decimal("1")
            ),
            "final_equity": _float(replay.equity),
            "final_cash": _float(replay.cash),
            "net_profit": _float(replay.total_pnl),
            "realized_pnl": _float(replay.realized_pnl),
            "unrealized_pnl": _float(replay.unrealized_pnl),
            "funding_pnl": _float(replay.funding_pnl),
            "commission": _float(replay.commission),
            "slippage_cost": _float(replay.slippage_cost),
            "n_orders": len(replay.orders),
            "n_fills": len(replay.fills),
            "n_trades": len(deals),
            "max_drawdown": min(
                (point["value"] for point in _drawdown_curve(equity)), default=0.0
            ),
        },
        "curves": {"equity": equity, "drawdown": _drawdown_curve(equity)},
        "accounts": [item.to_dict() for item in replay.account_curve],
        "market": {"bars": [item.to_dict() for item in bars]},
        "orders": [item.to_dict() for item in replay.orders],
        "fills": [item.to_dict() for item in replay.fills],
        "deals": deals,
        "positions": [item.to_dict() for item in replay.positions],
        "sentiment": [item.to_dict() for item in sentiments],
        "funding": [item.to_dict() for item in funding],
        "diagnostics": {
            "adapter": UNIVERSAL_ENGINE_VERSION,
            "data_source_policy": "pxydata_snapshot_only",
            "snapshot_enforcement": "manifest_bound",
            "strictly_reproducible": True,
            "matching_model": "bar_close_signal_next_actual_bar_open",
            "trade_counting_rule": "position_reducing_fills",
            "availability_rule": "max(event_time,available_at,tradable_from)",
            "funding_direction": "positive_rate_long_pays_short",
            "bars": len(bars),
            "funding_events": len(funding),
            "sentiment_events": len(sentiments),
            "warnings": [],
        },
        "replay_audit": audit,
        "reproducibility": {
            "input_contract_sha256": stable_hash(task),
            "manifest_sha256": snapshot.get("manifest_sha256"),
            "input_event_sha256": replay.input_sha256,
            "event_log_sha256": audit["chain_sha256"],
            "event_count": audit["event_count"],
            "engine_version": UNIVERSAL_ENGINE_VERSION,
            "kernel_result_sha256": replay.reproducibility_hash,
        },
        "artifacts": [],
        "_replay_events": replay_events,
    }
    result["reproducibility"]["result_sha256"] = stable_hash(
        {key: value for key, value in result.items() if key != "_replay_events"}
    )
    return result


__all__ = [
    "UNIVERSAL_ENGINE_TYPE",
    "UNIVERSAL_ENGINE_VERSION",
    "UNIVERSAL_STRATEGY_HASH",
    "UNIVERSAL_STRATEGY_ID",
    "UniversalMinuteBacktestError",
    "run_universal_minute_backtest",
    "universal_minute_runtime_available",
]
