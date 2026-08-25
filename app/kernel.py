"""统一回测事件、撮合口径和组合账本的最小内核。

该模块不读取行情，也不执行策略；适配器只负责把自己的输入转换为 FillEvent，
然后通过本模块得到一致的事件序列和账本结果。
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Literal

KERNEL_VERSION = "pxybacktest.kernel.v1"


class CanonicalizationError(ValueError):
    pass


def _canonicalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError("canonical JSON 不允许 NaN 或 Infinity")
        return format(value, ".12f").rstrip("0").rstrip(".") or "0"
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise CanonicalizationError("canonical JSON 的对象键必须为字符串")
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    raise CanonicalizationError(f"canonical JSON 不支持类型: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """生成跨进程稳定的 JSON 表示，作为 fingerprint 和 event_id 的输入。"""
    return json.dumps(
        _canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EventRecord:
    run_id: str
    seq: int
    event_type: str
    event_time: str
    decision_time: str | None
    symbol: str | None
    engine_id: str
    engine_version: str
    snapshot_id: str
    payload: dict[str, Any]
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.seq < 1:
            raise ValueError("事件 seq 必须从 1 开始")
        if not self.run_id or not self.event_type or not self.event_time:
            raise ValueError("事件缺少 run_id、event_type 或 event_time")
        if not self.snapshot_id:
            raise ValueError("事件必须绑定 snapshot_id")
        object.__setattr__(
            self,
            "event_id",
            stable_hash(
                {
                    "run_id": self.run_id,
                    "seq": self.seq,
                    "event_type": self.event_type,
                    "event_time": self.event_time,
                    "decision_time": self.decision_time,
                    "symbol": self.symbol,
                    "engine_id": self.engine_id,
                    "engine_version": self.engine_version,
                    "snapshot_id": self.snapshot_id,
                    "payload": self.payload,
                }
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_time": self.event_time,
            "decision_time": self.decision_time,
            "symbol": self.symbol,
            "engine_id": self.engine_id,
            "engine_version": self.engine_version,
            "snapshot_id": self.snapshot_id,
            "payload": self.payload,
        }


class EventLog:
    """进程内 append-only 事件日志；持久化适配器应按顺序写入 WAL/Parquet。"""

    def __init__(self, *, run_id: str, engine_id: str, snapshot_id: str):
        self.run_id = run_id
        self.engine_id = engine_id
        self.snapshot_id = snapshot_id
        self._events: list[EventRecord] = []

    def append(
        self,
        event_type: str,
        *,
        event_time: str,
        decision_time: str | None = None,
        symbol: str | None = None,
        payload: dict[str, Any] | None = None,
        engine_version: str = KERNEL_VERSION,
    ) -> EventRecord:
        event = EventRecord(
            run_id=self.run_id,
            seq=len(self._events) + 1,
            event_type=event_type,
            event_time=event_time,
            decision_time=decision_time,
            symbol=symbol,
            engine_id=self.engine_id,
            engine_version=engine_version,
            snapshot_id=self.snapshot_id,
            payload=dict(payload or {}),
        )
        self._events.append(event)
        return event

    @property
    def events(self) -> tuple[EventRecord, ...]:
        return tuple(self._events)

    def validate(self) -> None:
        for expected, event in enumerate(self._events, start=1):
            if event.seq != expected:
                raise ValueError("事件 seq 不连续")
            if event.event_id != stable_hash({key: value for key, value in event.to_dict().items() if key != "event_id"}):
                raise ValueError(f"事件 {event.seq} event_id 校验失败")

    def fingerprint(self) -> str:
        self.validate()
        return stable_hash([event.to_dict() for event in self._events])


@dataclass(frozen=True)
class LedgerSnapshot:
    event_seq: int
    cash: Decimal
    equity: Decimal
    available_cash: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    commission: Decimal
    stamp_tax: Decimal
    slippage_cost: Decimal
    positions: dict[str, dict[str, str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_seq": self.event_seq,
            "cash": str(self.cash),
            "equity": str(self.equity),
            "available_cash": str(self.available_cash),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "commission": str(self.commission),
            "stamp_tax": str(self.stamp_tax),
            "slippage_cost": str(self.slippage_cost),
            "positions": self.positions,
        }


@dataclass
class _Position:
    quantity: Decimal = Decimal("0")
    available_quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")
    last_price: Decimal = Decimal("0")


class PortfolioLedger:
    """使用 Decimal 结算现金和费用，行情价格仍可由适配器以 float 提供。"""

    def __init__(
        self,
        *,
        initial_cash: Decimal | int | float | str,
        t_plus_one: bool = False,
        quantity_precision: int = 8,
        money_precision: int = 2,
    ) -> None:
        self.initial_cash = Decimal(str(initial_cash))
        self.cash = self.initial_cash
        self.t_plus_one = t_plus_one
        self._quantity_quantum = Decimal(1).scaleb(-quantity_precision)
        self._money_quantum = Decimal(1).scaleb(-money_precision)
        self.positions: dict[str, _Position] = {}
        self.realized_pnl = Decimal("0")
        self.commission = Decimal("0")
        self.stamp_tax = Decimal("0")
        self.slippage_cost = Decimal("0")
        self._event_seq = 0

    def _money(self, value: Decimal) -> Decimal:
        return value.quantize(self._money_quantum, rounding=ROUND_HALF_UP)

    def _quantity(self, value: Decimal) -> Decimal:
        return value.quantize(self._quantity_quantum, rounding=ROUND_HALF_UP)

    def mark(self, symbol: str, price: Decimal | int | float | str) -> None:
        position = self.positions.setdefault(symbol, _Position())
        position.last_price = Decimal(str(price))

    def apply_fill(
        self,
        *,
        event_seq: int,
        symbol: str,
        side: Literal["buy", "sell"],
        quantity: Decimal | int | float | str,
        price: Decimal | int | float | str,
        commission: Decimal | int | float | str = 0,
        stamp_tax: Decimal | int | float | str = 0,
        slippage_cost: Decimal | int | float | str = 0,
    ) -> LedgerSnapshot:
        if event_seq <= self._event_seq:
            raise ValueError("账本 event_seq 必须严格递增")
        qty = self._quantity(Decimal(str(quantity)))
        fill_price = Decimal(str(price))
        if qty <= 0 or fill_price <= 0:
            raise ValueError("成交数量和价格必须大于 0")
        position = self.positions.setdefault(symbol, _Position())
        if side == "sell" and self.t_plus_one and qty > position.available_quantity:
            raise ValueError("T+1 规则禁止卖出当日新买入持仓")
        if side == "sell" and not self.t_plus_one and qty > position.quantity:
            raise ValueError("可卖持仓不足")
        commission_value = self._money(Decimal(str(commission)))
        stamp_value = self._money(Decimal(str(stamp_tax)))
        slippage_value = self._money(Decimal(str(slippage_cost)))
        notional = self._money(qty * fill_price)
        if side == "buy":
            total_cost = self._money(notional + commission_value + slippage_value)
            if total_cost > self.cash:
                raise ValueError("可用现金不足")
            previous = position.quantity * position.average_price
            position.quantity = self._quantity(position.quantity + qty)
            position.average_price = (previous + notional) / position.quantity
            if not self.t_plus_one:
                position.available_quantity = position.quantity
            self.cash = self._money(self.cash - total_cost)
        elif side == "sell":
            if qty > position.quantity:
                raise ValueError("可卖持仓不足")
            proceeds = self._money(notional - commission_value - stamp_value - slippage_value)
            self.realized_pnl = self._money(
                self.realized_pnl + qty * (fill_price - position.average_price) - commission_value - stamp_value - slippage_value
            )
            position.quantity = self._quantity(position.quantity - qty)
            position.available_quantity = self._quantity(position.available_quantity - qty)
            self.cash = self._money(self.cash + proceeds)
        else:
            raise ValueError(f"不支持的成交方向: {side}")
        self.commission = self._money(self.commission + commission_value)
        self.stamp_tax = self._money(self.stamp_tax + stamp_value)
        self.slippage_cost = self._money(self.slippage_cost + slippage_value)
        self._event_seq = event_seq
        return self.snapshot()

    def snapshot(self) -> LedgerSnapshot:
        market_value = Decimal("0")
        unrealized = Decimal("0")
        positions: dict[str, dict[str, str]] = {}
        for symbol, position in sorted(self.positions.items()):
            market_value += position.quantity * position.last_price
            unrealized += position.quantity * (position.last_price - position.average_price)
            positions[symbol] = {
                "quantity": str(position.quantity),
                "available_quantity": str(position.available_quantity),
                "average_price": str(position.average_price),
                "last_price": str(position.last_price),
            }
        equity = self._money(self.cash + market_value)
        return LedgerSnapshot(
            event_seq=self._event_seq,
            cash=self.cash,
            equity=equity,
            available_cash=self.cash,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self._money(unrealized),
            commission=self.commission,
            stamp_tax=self.stamp_tax,
            slippage_cost=self.slippage_cost,
            positions=positions,
        )


def replay_fills(
    *,
    initial_cash: Decimal | int | float | str,
    events: Iterable[EventRecord],
    t_plus_one: bool = False,
) -> LedgerSnapshot:
    ledger = PortfolioLedger(initial_cash=initial_cash, t_plus_one=t_plus_one)
    for event in events:
        if event.event_type == "MarketEvent":
            if event.symbol:
                ledger.mark(event.symbol, event.payload["close"])
        elif event.event_type == "FillEvent":
            payload = dict(event.payload)
            payload.setdefault("side", str(payload.pop("direction", "")).lower())
            payload.setdefault("symbol", event.symbol)
            ledger.apply_fill(event_seq=event.seq, **payload)
    return ledger.snapshot()


__all__ = [
    "EventLog",
    "EventRecord",
    "KERNEL_VERSION",
    "LedgerSnapshot",
    "PortfolioLedger",
    "CanonicalizationError",
    "canonical_json",
    "replay_fills",
    "stable_hash",
]
