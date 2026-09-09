"""确定性的通用 1 分钟事件回放内核。

本模块不读取数据库、文件或网络，也不接入任务队列。输入事件在构造后按
``max(event_time, available_at, tradable_from)`` 排序；策略只收到已经进入可见集的事件。
K 线的 ``event_time`` 表示分钟收盘时刻，策略在收盘后产生订单，订单只在
同品种下一根实际存在的 K 线开盘价成交。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from itertools import groupby
from types import MappingProxyType
from typing import Any, Iterable, Literal, Mapping, Protocol, Sequence

from .kernel import stable_hash
from .replay import ReplayError, canonical_time


MINUTE_REPLAY_VERSION = "pxybacktest.minute-replay.v1"
Side = Literal["buy", "sell"]
Settlement = Literal["spot", "derivative"]
Number = Decimal | int | float | str


class MinuteReplayError(ValueError):
    """输入或策略输出不满足 1 分钟回放契约。"""


def _time(value: str) -> str:
    try:
        return canonical_time(value)
    except ReplayError as exc:
        raise MinuteReplayError(str(exc)) from exc


def _decimal(value: Number, name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise MinuteReplayError(f"{name} 必须是有限数值")
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise MinuteReplayError(f"{name} 必须是有限数值") from exc
    if not result.is_finite() or (positive and result <= 0):
        condition = "正数" if positive else "有限数值"
        raise MinuteReplayError(f"{name} 必须是{condition}")
    return result


def _non_negative(value: Number, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise MinuteReplayError(f"{name} 不能为负数")
    return result


def _json_value(value: Any) -> Any:
    """把公开载荷转换为 ``stable_hash`` 可接受的稳定结构。"""

    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _frozen_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {
                str(key): _frozen_json(item)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        )
    if isinstance(value, (list, tuple)):
        return tuple(_frozen_json(item) for item in value)
    return value


def _ready_time(*values: str) -> str:
    return max(values)


@dataclass(frozen=True, slots=True)
class MinuteBar:
    """一根完整 1 分钟 K 线，``event_time`` 为分钟收盘时刻。"""

    symbol: str
    event_time: str
    open: Number
    high: Number
    low: Number
    close: Number
    volume: Number = 0
    available_at: str | None = None
    tradable_from: str | None = None
    revision_id: str = "original"
    source: str = "PXYDATA"

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").strip()
        if not symbol:
            raise MinuteReplayError("K 线 symbol 不能为空")
        event_time = _time(self.event_time)
        available_at = _time(self.available_at or event_time)
        tradable_from = _time(self.tradable_from or available_at)
        open_price = _decimal(self.open, "open", positive=True)
        high = _decimal(self.high, "high", positive=True)
        low = _decimal(self.low, "low", positive=True)
        close = _decimal(self.close, "close", positive=True)
        volume = _non_negative(self.volume, "volume")
        if low > min(open_price, close) or high < max(open_price, close) or low > high:
            raise MinuteReplayError("K 线 OHLC 关系无效")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "tradable_from", tradable_from)
        object.__setattr__(self, "open", open_price)
        object.__setattr__(self, "high", high)
        object.__setattr__(self, "low", low)
        object.__setattr__(self, "close", close)
        object.__setattr__(self, "volume", volume)

    @property
    def ready_time(self) -> str:
        return _ready_time(
            self.event_time, str(self.available_at), str(self.tradable_from)
        )

    @property
    def open_time(self) -> str:
        close_time = datetime.fromisoformat(self.event_time.replace("Z", "+00:00"))
        return _time((close_time - timedelta(minutes=1)).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "bar",
            "symbol": self.symbol,
            "event_time": self.event_time,
            "available_at": self.available_at,
            "tradable_from": self.tradable_from,
            "revision_id": self.revision_id,
            "ready_time": self.ready_time,
            "open_time": self.open_time,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class FundingRateEvent:
    """明确结算时刻的资金费率；正费率表示多头支付、空头收取。"""

    symbol: str
    event_time: str
    rate: Number
    available_at: str | None = None
    tradable_from: str | None = None
    revision_id: str = "original"
    mark_price: Number | None = None
    source: str = "PXYDATA"

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").strip()
        if not symbol:
            raise MinuteReplayError("资金费 symbol 不能为空")
        event_time = _time(self.event_time)
        available_at = _time(self.available_at or event_time)
        tradable_from = _time(self.tradable_from or available_at)
        mark_price = (
            _decimal(self.mark_price, "funding.mark_price", positive=True)
            if self.mark_price is not None
            else None
        )
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "tradable_from", tradable_from)
        object.__setattr__(self, "rate", _decimal(self.rate, "funding.rate"))
        object.__setattr__(self, "mark_price", mark_price)

    @property
    def ready_time(self) -> str:
        return _ready_time(
            self.event_time, str(self.available_at), str(self.tradable_from)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "funding",
            "symbol": self.symbol,
            "event_time": self.event_time,
            "available_at": self.available_at,
            "tradable_from": self.tradable_from,
            "revision_id": self.revision_id,
            "ready_time": self.ready_time,
            "rate": str(self.rate),
            "mark_price": str(self.mark_price) if self.mark_price is not None else None,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class SentimentEvent:
    """带 point-in-time 可见时刻的情绪事件。"""

    symbol: str | None
    event_time: str
    value: Number
    available_at: str | None = None
    tradable_from: str | None = None
    revision_id: str = "original"
    source: str = "PXYDATA"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        symbol = str(self.symbol).strip() if self.symbol is not None else None
        if symbol == "":
            symbol = None
        event_time = _time(self.event_time)
        available_at = _time(self.available_at or event_time)
        tradable_from = _time(self.tradable_from or available_at)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "tradable_from", tradable_from)
        sentiment_value = _decimal(self.value, "sentiment.value")
        if sentiment_value < -1 or sentiment_value > 1:
            raise MinuteReplayError("sentiment.value 必须在 -1 到 1 之间")
        object.__setattr__(self, "value", sentiment_value)
        object.__setattr__(self, "metadata", _frozen_json(dict(self.metadata)))

    @property
    def ready_time(self) -> str:
        return _ready_time(
            self.event_time, str(self.available_at), str(self.tradable_from)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "sentiment",
            "symbol": self.symbol,
            "event_time": self.event_time,
            "available_at": self.available_at,
            "tradable_from": self.tradable_from,
            "revision_id": self.revision_id,
            "ready_time": self.ready_time,
            "value": str(self.value),
            "source": self.source,
            "metadata": _json_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class InstrumentConfig:
    """品种的结算、合约和成交成本规则。"""

    symbol: str
    asset_class: str = "stock"
    settlement: Settlement | None = None
    contract_multiplier: Number = 1
    leverage: Number = 1
    commission_rate: Number = 0
    minimum_commission: Number = 0
    fixed_commission: Number = 0
    commission_per_unit: Number = 0
    slippage_bps: Number = 0
    quantity_precision: int = 8
    allow_short: bool | None = None

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").strip()
        if not symbol:
            raise MinuteReplayError("InstrumentConfig.symbol 不能为空")
        asset_class = str(self.asset_class or "").strip().lower()
        derivative_classes = {
            "future",
            "futures",
            "forex",
            "fx",
            "perpetual",
            "crypto_perpetual",
            "cfd",
        }
        settlement = self.settlement or (
            "derivative" if asset_class in derivative_classes else "spot"
        )
        if settlement not in {"spot", "derivative"}:
            raise MinuteReplayError("settlement 必须是 spot 或 derivative")
        precision = int(self.quantity_precision)
        if precision < 0 or precision > 18:
            raise MinuteReplayError("quantity_precision 必须在 0 到 18 之间")
        allow_short = self.allow_short
        if allow_short is None:
            allow_short = settlement == "derivative"
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "asset_class", asset_class)
        object.__setattr__(self, "settlement", settlement)
        object.__setattr__(
            self,
            "contract_multiplier",
            _decimal(self.contract_multiplier, "contract_multiplier", positive=True),
        )
        leverage = _decimal(self.leverage, "leverage", positive=True)
        if settlement == "spot" and leverage != 1:
            raise MinuteReplayError("现货品种 leverage 必须为 1")
        object.__setattr__(self, "leverage", leverage)
        for name in (
            "commission_rate",
            "minimum_commission",
            "fixed_commission",
            "commission_per_unit",
            "slippage_bps",
        ):
            object.__setattr__(self, name, _non_negative(getattr(self, name), name))
        object.__setattr__(self, "quantity_precision", precision)
        object.__setattr__(self, "allow_short", bool(allow_short))

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "asset_class": self.asset_class,
            "settlement": self.settlement,
            "contract_multiplier": str(self.contract_multiplier),
            "leverage": str(self.leverage),
            "commission_rate": str(self.commission_rate),
            "minimum_commission": str(self.minimum_commission),
            "fixed_commission": str(self.fixed_commission),
            "commission_per_unit": str(self.commission_per_unit),
            "slippage_bps": str(self.slippage_bps),
            "quantity_precision": self.quantity_precision,
            "allow_short": self.allow_short,
        }


@dataclass(frozen=True, slots=True)
class MinuteReplayConfig:
    initial_cash: Number
    instruments: Sequence[InstrumentConfig] | Mapping[str, InstrumentConfig] = ()
    run_id: str = "universal-1m"
    snapshot_id: str = "inline-1m"
    money_precision: int = 8

    def __post_init__(self) -> None:
        initial_cash = _decimal(self.initial_cash, "initial_cash", positive=True)
        if not self.run_id or not self.snapshot_id:
            raise MinuteReplayError("run_id 和 snapshot_id 不能为空")
        precision = int(self.money_precision)
        if precision < 0 or precision > 18:
            raise MinuteReplayError("money_precision 必须在 0 到 18 之间")
        if isinstance(self.instruments, Mapping):
            specs = []
            for symbol, spec in self.instruments.items():
                if not isinstance(spec, InstrumentConfig):
                    raise MinuteReplayError("instruments 必须包含 InstrumentConfig")
                specs.append(spec if spec.symbol == symbol else replace(spec, symbol=symbol))
        else:
            specs = list(self.instruments)
        if any(not isinstance(spec, InstrumentConfig) for spec in specs):
            raise MinuteReplayError("instruments 必须包含 InstrumentConfig")
        specs = sorted(specs, key=lambda item: item.symbol)
        if len({spec.symbol for spec in specs}) != len(specs):
            raise MinuteReplayError("InstrumentConfig.symbol 不能重复")
        object.__setattr__(self, "initial_cash", initial_cash)
        object.__setattr__(self, "instruments", tuple(specs))
        object.__setattr__(self, "money_precision", precision)

    def instrument_for(self, symbol: str) -> InstrumentConfig:
        for spec in self.instruments:
            if spec.symbol == symbol:
                return spec
        return InstrumentConfig(symbol=symbol)

    def to_dict(self) -> dict[str, Any]:
        return {
            "initial_cash": str(self.initial_cash),
            "instruments": [item.to_dict() for item in self.instruments],
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "money_precision": self.money_precision,
        }


@dataclass(frozen=True, slots=True)
class OrderIntent:
    symbol: str
    side: Side
    quantity: Number
    tag: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").strip()
        side = str(self.side or "").strip().lower()
        if not symbol:
            raise MinuteReplayError("订单 symbol 不能为空")
        if side not in {"buy", "sell"}:
            raise MinuteReplayError("订单 side 必须是 buy 或 sell")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        object.__setattr__(self, "metadata", _frozen_json(dict(self.metadata)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "tag": self.tag,
            "metadata": _json_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    symbol: str
    quantity: Decimal
    average_price: Decimal
    mark_price: Decimal
    market_value: Decimal
    used_margin: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "quantity": str(self.quantity),
            "average_price": str(self.average_price),
            "mark_price": str(self.mark_price),
            "market_value": str(self.market_value),
            "used_margin": str(self.used_margin),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
        }


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    event_time: str
    cash: Decimal
    equity: Decimal
    used_margin: Decimal
    available_cash: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    funding_pnl: Decimal
    commission: Decimal
    slippage_cost: Decimal
    total_pnl: Decimal
    positions: tuple[PositionSnapshot, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_time": self.event_time,
            "cash": str(self.cash),
            "equity": str(self.equity),
            "used_margin": str(self.used_margin),
            "available_cash": str(self.available_cash),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "funding_pnl": str(self.funding_pnl),
            "commission": str(self.commission),
            "slippage_cost": str(self.slippage_cost),
            "total_pnl": str(self.total_pnl),
            "positions": [position.to_dict() for position in self.positions],
        }


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """策略的只读 point-in-time 视图，不包含任何未就绪输入。"""

    current_time: str
    bars: tuple[MinuteBar, ...]
    sentiments: tuple[SentimentEvent, ...]
    funding_events: tuple[FundingRateEvent, ...]
    positions: tuple[PositionSnapshot, ...]
    cash: Decimal
    equity: Decimal

    def latest_bar(self, symbol: str) -> MinuteBar | None:
        return next((item for item in reversed(self.bars) if item.symbol == symbol), None)

    def latest_sentiment(self, symbol: str | None = None) -> SentimentEvent | None:
        return next(
            (
                item
                for item in reversed(self.sentiments)
                if symbol is None or item.symbol in {None, symbol}
            ),
            None,
        )

    def position(self, symbol: str) -> PositionSnapshot | None:
        return next((item for item in self.positions if item.symbol == symbol), None)


class MinuteStrategy(Protocol):
    def on_bar(
        self, context: StrategyContext, bar: MinuteBar
    ) -> OrderIntent | Iterable[OrderIntent] | None:
        """在已完成的分钟收盘后返回下一根 K 线开盘订单。"""


@dataclass(frozen=True, slots=True)
class OrderRecord:
    order_id: str
    created_at: str
    signal_bar_time: str
    symbol: str
    side: Side
    quantity: Decimal
    status: Literal["OPEN", "FILLED", "REJECTED"]
    fill_id: str | None = None
    reason: str | None = None
    tag: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "created_at": self.created_at,
            "signal_bar_time": self.signal_bar_time,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "status": self.status,
            "fill_id": self.fill_id,
            "reason": self.reason,
            "tag": self.tag,
            "metadata": _json_value(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class FillRecord:
    fill_id: str
    order_id: str
    event_time: str
    available_at: str
    market_bar_time: str
    symbol: str
    side: Side
    quantity: Decimal
    reference_price: Decimal
    price: Decimal
    notional: Decimal
    commission: Decimal
    slippage_cost: Decimal
    position_effect: Literal["open", "increase", "reduce", "close", "reverse"]
    closed_quantity: Decimal
    opened_quantity: Decimal
    realized_pnl: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "event_time": self.event_time,
            "fill_time": self.event_time,
            "available_at": self.available_at,
            "market_bar_time": self.market_bar_time,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "reference_price": str(self.reference_price),
            "price": str(self.price),
            "notional": str(self.notional),
            "commission": str(self.commission),
            "slippage_cost": str(self.slippage_cost),
            "position_effect": self.position_effect,
            "closed_quantity": str(self.closed_quantity),
            "opened_quantity": str(self.opened_quantity),
            "realized_pnl": str(self.realized_pnl),
        }


@dataclass(frozen=True, slots=True)
class TimelineEvent:
    seq: int
    event_type: str
    event_time: str
    available_at: str
    symbol: str | None
    payload: Mapping[str, Any]
    ready_time: str = field(init=False)
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        payload = _frozen_json(dict(self.payload))
        event_time = _time(self.event_time)
        available_at = _time(self.available_at)
        ready_time = _ready_time(event_time, available_at)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "ready_time", ready_time)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(
            self,
            "event_id",
            stable_hash(
                {
                    "version": MINUTE_REPLAY_VERSION,
                    "seq": self.seq,
                    "event_type": self.event_type,
                    "event_time": self.event_time,
                    "available_at": self.available_at,
                    "ready_time": ready_time,
                    "symbol": self.symbol,
                    "payload": _json_value(payload),
                }
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_time": self.event_time,
            "available_at": self.available_at,
            "ready_time": self.ready_time,
            "symbol": self.symbol,
            "payload": _json_value(self.payload),
        }


@dataclass(frozen=True, slots=True)
class MinuteReplayResult:
    orders: tuple[OrderRecord, ...]
    fills: tuple[FillRecord, ...]
    positions: tuple[PositionSnapshot, ...]
    account_curve: tuple[AccountSnapshot, ...]
    timeline: tuple[TimelineEvent, ...]
    cash: Decimal
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    funding_pnl: Decimal
    commission: Decimal
    slippage_cost: Decimal
    total_pnl: Decimal
    input_sha256: str
    reproducibility_hash: str

    def position(self, symbol: str) -> PositionSnapshot | None:
        return next((item for item in self.positions if item.symbol == symbol), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": MINUTE_REPLAY_VERSION,
            "orders": [item.to_dict() for item in self.orders],
            "fills": [item.to_dict() for item in self.fills],
            "positions": [item.to_dict() for item in self.positions],
            "account_curve": [item.to_dict() for item in self.account_curve],
            "timeline": [item.to_dict() for item in self.timeline],
            "cash": str(self.cash),
            "equity": str(self.equity),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "funding_pnl": str(self.funding_pnl),
            "commission": str(self.commission),
            "slippage_cost": str(self.slippage_cost),
            "total_pnl": str(self.total_pnl),
            "input_sha256": self.input_sha256,
            "reproducibility_hash": self.reproducibility_hash,
        }


@dataclass(slots=True)
class _Position:
    quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")
    mark_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class _Execution:
    quantity: Decimal
    fill_price: Decimal
    notional: Decimal
    commission: Decimal
    slippage_cost: Decimal
    position_effect: Literal["open", "increase", "reduce", "close", "reverse"]
    closed_quantity: Decimal
    opened_quantity: Decimal
    realized_pnl: Decimal


@dataclass(frozen=True, slots=True)
class _FundingSettlement:
    payment: Decimal
    quantity: Decimal
    mark_price: Decimal


class _Ledger:
    def __init__(self, config: MinuteReplayConfig) -> None:
        self.config = config
        self.initial_cash = config.initial_cash
        self.cash = config.initial_cash
        self.positions: dict[str, _Position] = {}
        self.realized_pnl = Decimal("0")
        self.funding_pnl = Decimal("0")
        self.commission = Decimal("0")
        self.slippage_cost = Decimal("0")
        self._position_history: dict[str, list[tuple[str, Decimal]]] = {}
        self._mark_history: dict[str, list[tuple[str, Decimal]]] = {}
        self._money_quantum = Decimal(1).scaleb(-config.money_precision)

    def money(self, value: Decimal) -> Decimal:
        return value.quantize(self._money_quantum, rounding=ROUND_HALF_UP)

    @staticmethod
    def _quantity(value: Decimal, precision: int) -> Decimal:
        return value.quantize(Decimal(1).scaleb(-precision), rounding=ROUND_HALF_UP)

    def mark(self, symbol: str, price: Decimal, *, event_time: str) -> None:
        self.positions.setdefault(symbol, _Position()).mark_price = price
        self._mark_history.setdefault(symbol, []).append((event_time, price))

    @staticmethod
    def _value_at(
        history: list[tuple[str, Decimal]], event_time: str, *, inclusive: bool
    ) -> Decimal:
        candidates = [
            (time_value, index, value)
            for index, (time_value, value) in enumerate(history)
            if (time_value <= event_time if inclusive else time_value < event_time)
        ]
        return max(candidates, default=("", -1, Decimal("0")))[2]

    def position_quantity_at(self, symbol: str, event_time: str) -> Decimal:
        # 资金费在同一时间点先结算旧仓，再处理该时间点的新开/平仓。
        return self._value_at(
            self._position_history.get(symbol, []), event_time, inclusive=False
        )

    def mark_price_at(self, symbol: str, event_time: str) -> Decimal:
        return self._value_at(
            self._mark_history.get(symbol, []), event_time, inclusive=True
        )

    def _unrealized(self, symbol: str, position: _Position) -> Decimal:
        spec = self.config.instrument_for(symbol)
        return (
            position.quantity
            * (position.mark_price - position.average_price)
            * spec.contract_multiplier
        )

    def _position_snapshot(
        self, symbol: str, position: _Position
    ) -> PositionSnapshot:
        spec = self.config.instrument_for(symbol)
        market_value = (
            position.quantity * position.mark_price * spec.contract_multiplier
            if spec.settlement == "spot"
            else Decimal("0")
        )
        used_margin = (
            position.quantity.copy_abs()
            * position.mark_price
            * spec.contract_multiplier
            / spec.leverage
            if spec.settlement == "derivative"
            else Decimal("0")
        )
        return PositionSnapshot(
            symbol=symbol,
            quantity=position.quantity,
            average_price=position.average_price,
            mark_price=position.mark_price,
            market_value=self.money(market_value),
            used_margin=self.money(used_margin),
            realized_pnl=self.money(position.realized_pnl),
            unrealized_pnl=self.money(self._unrealized(symbol, position)),
        )

    def position_snapshot(self, symbol: str) -> PositionSnapshot:
        return self._position_snapshot(
            symbol, self.positions.get(symbol, _Position())
        )

    def position_snapshots(self) -> tuple[PositionSnapshot, ...]:
        snapshots: list[PositionSnapshot] = []
        for symbol, position in sorted(self.positions.items()):
            if position.quantity != 0:
                snapshots.append(self._position_snapshot(symbol, position))
        return tuple(snapshots)

    def snapshot(self, event_time: str) -> AccountSnapshot:
        positions = self.position_snapshots()
        unrealized = self.money(sum((item.unrealized_pnl for item in positions), Decimal("0")))
        spot_value = sum((item.market_value for item in positions), Decimal("0"))
        derivative_unrealized = sum(
            (
                item.unrealized_pnl
                for item in positions
                if self.config.instrument_for(item.symbol).settlement == "derivative"
            ),
            Decimal("0"),
        )
        equity = self.money(self.cash + spot_value + derivative_unrealized)
        used_margin = self.money(
            sum((item.used_margin for item in positions), Decimal("0"))
        )
        return AccountSnapshot(
            event_time=event_time,
            cash=self.money(self.cash),
            equity=equity,
            used_margin=used_margin,
            available_cash=self.money(equity - used_margin),
            realized_pnl=self.money(self.realized_pnl),
            unrealized_pnl=unrealized,
            funding_pnl=self.money(self.funding_pnl),
            commission=self.money(self.commission),
            slippage_cost=self.money(self.slippage_cost),
            total_pnl=self.money(equity - self.initial_cash),
            positions=positions,
        )

    def execute(
        self,
        *,
        intent: OrderIntent,
        reference_price: Decimal,
        execution_time: str,
    ) -> _Execution:
        spec = self.config.instrument_for(intent.symbol)
        quantity = self._quantity(intent.quantity, spec.quantity_precision)
        if quantity <= 0:
            raise MinuteReplayError("订单数量按精度取整后必须大于 0")
        adverse = Decimal("1") if intent.side == "buy" else Decimal("-1")
        fill_price = reference_price * (
            Decimal("1") + adverse * spec.slippage_bps / Decimal("10000")
        )
        notional = quantity * fill_price * spec.contract_multiplier
        commission = max(
            notional.copy_abs() * spec.commission_rate
            + quantity * spec.commission_per_unit,
            spec.minimum_commission,
        ) + spec.fixed_commission
        slippage = (
            (fill_price - reference_price).copy_abs()
            * quantity
            * spec.contract_multiplier
        )
        position = self.positions.setdefault(
            intent.symbol, _Position(mark_price=reference_price)
        )
        position.mark_price = reference_price
        signed_delta = quantity if intent.side == "buy" else -quantity
        new_quantity = self._quantity(
            position.quantity + signed_delta, spec.quantity_precision
        )
        if not spec.allow_short and new_quantity < 0:
            raise MinuteReplayError("该品种不允许建立空头仓位")

        realized = Decimal("0")
        if position.quantity and position.quantity * signed_delta < 0:
            closing = min(position.quantity.copy_abs(), signed_delta.copy_abs())
            direction = Decimal("1") if position.quantity > 0 else Decimal("-1")
            realized = (
                closing
                * (fill_price - position.average_price)
                * direction
                * spec.contract_multiplier
            )

        old_quantity = position.quantity
        closed_quantity = (
            min(old_quantity.copy_abs(), signed_delta.copy_abs())
            if old_quantity and old_quantity * signed_delta < 0
            else Decimal("0")
        )
        opened_quantity = signed_delta.copy_abs() - closed_quantity
        if old_quantity == 0:
            position_effect = "open"
        elif old_quantity * signed_delta > 0:
            position_effect = "increase"
        elif new_quantity == 0:
            position_effect = "close"
        elif old_quantity * new_quantity < 0:
            position_effect = "reverse"
        else:
            position_effect = "reduce"
        if old_quantity == 0 or old_quantity * signed_delta > 0:
            combined_cost = (
                old_quantity.copy_abs() * position.average_price
                + signed_delta.copy_abs() * fill_price
            )
            new_average_price = combined_cost / new_quantity.copy_abs()
        elif new_quantity == 0:
            new_average_price = Decimal("0")
        elif old_quantity * new_quantity < 0:
            new_average_price = fill_price
        else:
            new_average_price = position.average_price

        if spec.settlement == "spot":
            cash_delta = -notional if intent.side == "buy" else notional
            candidate_cash = self.money(self.cash + cash_delta - commission)
        else:
            candidate_cash = self.money(self.cash + realized - commission)
        if spec.settlement == "spot" and candidate_cash < 0:
            raise MinuteReplayError("可用现金不足")

        if spec.settlement == "derivative":
            other_equity = Decimal("0")
            other_margin = Decimal("0")
            for symbol, other in self.positions.items():
                if symbol == intent.symbol:
                    continue
                other_spec = self.config.instrument_for(symbol)
                if other_spec.settlement == "spot":
                    other_equity += (
                        other.quantity
                        * other.mark_price
                        * other_spec.contract_multiplier
                    )
                else:
                    other_equity += self._unrealized(symbol, other)
                    other_margin += (
                        other.quantity.copy_abs()
                        * other.mark_price
                        * other_spec.contract_multiplier
                        / other_spec.leverage
                    )
            candidate_unrealized = (
                new_quantity
                * (reference_price - new_average_price)
                * spec.contract_multiplier
            )
            candidate_margin = (
                new_quantity.copy_abs()
                * reference_price
                * spec.contract_multiplier
                / spec.leverage
            )
            candidate_equity = candidate_cash + other_equity + candidate_unrealized
            if opened_quantity > 0 and candidate_equity < other_margin + candidate_margin:
                raise MinuteReplayError("衍生品可用保证金不足")

        position.average_price = new_average_price
        position.quantity = new_quantity
        position.mark_price = reference_price
        position.realized_pnl += realized
        self._position_history.setdefault(intent.symbol, []).append(
            (execution_time, new_quantity)
        )
        self.cash = candidate_cash
        self.realized_pnl += realized
        self.commission += commission
        self.slippage_cost += slippage
        return _Execution(
            quantity=quantity,
            fill_price=fill_price,
            notional=self.money(notional),
            commission=self.money(commission),
            slippage_cost=self.money(slippage),
            position_effect=position_effect,
            closed_quantity=closed_quantity,
            opened_quantity=opened_quantity,
            realized_pnl=self.money(realized),
        )

    def apply_funding(self, event: FundingRateEvent) -> _FundingSettlement:
        quantity = self.position_quantity_at(event.symbol, event.event_time)
        if quantity == 0:
            return _FundingSettlement(
                payment=Decimal("0"),
                quantity=Decimal("0"),
                mark_price=event.mark_price or Decimal("0"),
            )
        mark_price = event.mark_price or self.mark_price_at(
            event.symbol, event.event_time
        )
        if mark_price <= 0:
            raise MinuteReplayError(f"{event.symbol} 资金费结算缺少有效 mark_price")
        spec = self.config.instrument_for(event.symbol)
        payment = self.money(
            -quantity
            * mark_price
            * spec.contract_multiplier
            * event.rate
        )
        self.cash = self.money(self.cash + payment)
        self.funding_pnl = self.money(self.funding_pnl + payment)
        return _FundingSettlement(
            payment=payment,
            quantity=quantity,
            mark_price=mark_price,
        )


@dataclass(slots=True)
class _OrderState:
    order_id: str
    created_at: str
    signal_bar_time: str
    intent: OrderIntent
    status: Literal["OPEN", "FILLED", "REJECTED"] = "OPEN"
    fill_id: str | None = None
    reason: str | None = None

    def record(self) -> OrderRecord:
        return OrderRecord(
            order_id=self.order_id,
            created_at=self.created_at,
            signal_bar_time=self.signal_bar_time,
            symbol=self.intent.symbol,
            side=self.intent.side,
            quantity=self.intent.quantity,
            status=self.status,
            fill_id=self.fill_id,
            reason=self.reason,
            tag=self.intent.tag,
            metadata=self.intent.metadata,
        )


class _Timeline:
    def __init__(self) -> None:
        self.events: list[TimelineEvent] = []

    def append(
        self,
        event_type: str,
        *,
        event_time: str,
        available_at: str,
        symbol: str | None,
        payload: Mapping[str, Any],
    ) -> TimelineEvent:
        event = TimelineEvent(
            seq=len(self.events) + 1,
            event_type=event_type,
            event_time=event_time,
            available_at=available_at,
            symbol=symbol,
            payload=payload,
        )
        if self.events and event.ready_time < self.events[-1].ready_time:
            raise MinuteReplayError("回放时钟不允许倒退")
        self.events.append(event)
        return event


def _event_sort_key(event: MinuteBar | FundingRateEvent | SentimentEvent) -> tuple[str, str, int, str, str]:
    priority = 10 if isinstance(event, MinuteBar) else 20 if isinstance(event, FundingRateEvent) else 30
    return (
        event.ready_time,
        event.event_time,
        priority,
        event.symbol or "",
        stable_hash(event.to_dict()),
    )


def _normalize_intents(
    value: OrderIntent | Iterable[OrderIntent] | None,
) -> tuple[OrderIntent, ...]:
    if value is None:
        return ()
    if isinstance(value, OrderIntent):
        return (value,)
    try:
        result = tuple(value)
    except TypeError as exc:
        raise MinuteReplayError("策略必须返回 OrderIntent、可迭代订单或 None") from exc
    if any(not isinstance(item, OrderIntent) for item in result):
        raise MinuteReplayError("策略返回值包含非 OrderIntent 对象")
    return result


def run_minute_replay(
    *,
    config: MinuteReplayConfig,
    strategy: MinuteStrategy,
    bars: Iterable[MinuteBar],
    funding_events: Iterable[FundingRateEvent] = (),
    sentiment_events: Iterable[SentimentEvent] = (),
) -> MinuteReplayResult:
    """执行一次无外部副作用的 1 分钟回放。"""

    normalized_bars = tuple(bars)
    normalized_funding = tuple(funding_events)
    normalized_sentiment = tuple(sentiment_events)
    if any(not isinstance(item, MinuteBar) for item in normalized_bars):
        raise MinuteReplayError("bars 必须包含 MinuteBar")
    if any(not isinstance(item, FundingRateEvent) for item in normalized_funding):
        raise MinuteReplayError("funding_events 必须包含 FundingRateEvent")
    if any(not isinstance(item, SentimentEvent) for item in normalized_sentiment):
        raise MinuteReplayError("sentiment_events 必须包含 SentimentEvent")
    bar_keys = [(item.symbol, item.event_time) for item in normalized_bars]
    if len(set(bar_keys)) != len(bar_keys):
        raise MinuteReplayError("同一品种同一 event_time 只能有一根 K 线")

    events = sorted(
        (*normalized_bars, *normalized_funding, *normalized_sentiment),
        key=_event_sort_key,
    )
    input_sha256 = stable_hash(
        {
            "version": MINUTE_REPLAY_VERSION,
            "config": config.to_dict(),
            "events": [event.to_dict() for event in events],
        }
    )
    ledger = _Ledger(config)
    timeline = _Timeline()
    visible_bars: list[MinuteBar] = []
    visible_funding: list[FundingRateEvent] = []
    visible_sentiment: list[SentimentEvent] = []
    order_states: list[_OrderState] = []
    pending: list[_OrderState] = []
    fills: list[FillRecord] = []
    account_curve: list[AccountSnapshot] = []

    for replay_time, grouped in groupby(events, key=lambda item: item.ready_time):
        group = tuple(grouped)
        current_bars = sorted(
            (item for item in group if isinstance(item, MinuteBar)),
            key=_event_sort_key,
        )
        current_funding = sorted(
            (item for item in group if isinstance(item, FundingRateEvent)),
            key=_event_sort_key,
        )
        current_sentiment = sorted(
            (item for item in group if isinstance(item, SentimentEvent)),
            key=_event_sort_key,
        )

        # 同一可见时刻先把全部品种标到本分钟开盘，保证跨品种保证金
        # 检查不会因品种枚举顺序而引用另一品种的旧价格。
        for bar in current_bars:
            ledger.mark(bar.symbol, bar.open, event_time=bar.open_time)

        for bar in current_bars:
            timeline.append(
                "bar_open",
                event_time=bar.open_time,
                available_at=replay_time,
                symbol=bar.symbol,
                payload={
                    "bar_open_time": bar.open_time,
                    "market_bar_time": bar.event_time,
                    "price": str(bar.open),
                },
            )
            executable = [
                state
                for state in pending
                if state.intent.symbol == bar.symbol
                and bar.event_time > state.signal_bar_time
                and bar.open_time >= state.created_at
            ]
            for state in executable:
                try:
                    execution = ledger.execute(
                        intent=state.intent,
                        reference_price=bar.open,
                        execution_time=bar.open_time,
                    )
                except MinuteReplayError as exc:
                    state.status = "REJECTED"
                    state.reason = str(exc)
                    timeline.append(
                        "order_rejected",
                        event_time=replay_time,
                        available_at=replay_time,
                        symbol=bar.symbol,
                        payload={"order_id": state.order_id, "reason": state.reason},
                    )
                else:
                    fill_id = stable_hash(
                        {
                            "run_id": config.run_id,
                            "order_id": state.order_id,
                            "event_time": bar.open_time,
                            "available_at": replay_time,
                            "market_bar_time": bar.event_time,
                            "fill_index": len(fills) + 1,
                        }
                    )
                    fill = FillRecord(
                        fill_id=fill_id,
                        order_id=state.order_id,
                        event_time=bar.open_time,
                        available_at=replay_time,
                        market_bar_time=bar.event_time,
                        symbol=bar.symbol,
                        side=state.intent.side,
                        quantity=execution.quantity,
                        reference_price=bar.open,
                        price=execution.fill_price,
                        notional=execution.notional,
                        commission=execution.commission,
                        slippage_cost=execution.slippage_cost,
                        position_effect=execution.position_effect,
                        closed_quantity=execution.closed_quantity,
                        opened_quantity=execution.opened_quantity,
                        realized_pnl=execution.realized_pnl,
                    )
                    fills.append(fill)
                    state.status = "FILLED"
                    state.fill_id = fill_id
                    timeline.append(
                        "fill",
                        event_time=bar.open_time,
                        available_at=replay_time,
                        symbol=bar.symbol,
                        payload=fill.to_dict(),
                    )
                    position = ledger.position_snapshot(bar.symbol)
                    timeline.append(
                        "position",
                        event_time=bar.open_time,
                        available_at=replay_time,
                        symbol=bar.symbol,
                        payload=position.to_dict(),
                    )
                pending.remove(state)

        for bar in current_bars:
            ledger.mark(bar.symbol, bar.close, event_time=bar.event_time)
            visible_bars.append(bar)
            timeline.append(
                "market_bar",
                event_time=bar.event_time,
                available_at=bar.ready_time,
                symbol=bar.symbol,
                payload=bar.to_dict(),
            )

        for event in current_funding:
            settlement = ledger.apply_funding(event)
            visible_funding.append(event)
            timeline.append(
                "funding",
                event_time=event.event_time,
                available_at=event.ready_time,
                symbol=event.symbol,
                payload={
                    **event.to_dict(),
                    "payment": str(settlement.payment),
                    "settled_quantity": str(settlement.quantity),
                    "settlement_mark_price": str(settlement.mark_price),
                },
            )

        for event in current_sentiment:
            visible_sentiment.append(event)
            timeline.append(
                "sentiment",
                event_time=event.event_time,
                available_at=event.ready_time,
                symbol=event.symbol,
                payload=event.to_dict(),
            )

        for bar in current_bars:
            account_before_signal = ledger.snapshot(replay_time)
            context = StrategyContext(
                current_time=replay_time,
                bars=tuple(visible_bars),
                sentiments=tuple(visible_sentiment),
                funding_events=tuple(visible_funding),
                positions=account_before_signal.positions,
                cash=account_before_signal.cash,
                equity=account_before_signal.equity,
            )
            intents = _normalize_intents(strategy.on_bar(context, bar))
            for intent in intents:
                order_id = stable_hash(
                    {
                        "run_id": config.run_id,
                        "order_index": len(order_states) + 1,
                        "created_at": replay_time,
                        "signal_bar_time": bar.event_time,
                        "intent": intent.to_dict(),
                    }
                )
                state = _OrderState(
                    order_id=order_id,
                    created_at=replay_time,
                    signal_bar_time=bar.event_time,
                    intent=intent,
                )
                order_states.append(state)
                pending.append(state)
                timeline.append(
                    "signal",
                    event_time=replay_time,
                    available_at=replay_time,
                    symbol=intent.symbol,
                    payload={
                        "signal_bar_time": bar.event_time,
                        "intent": intent.to_dict(),
                    },
                )
                timeline.append(
                    "order",
                    event_time=replay_time,
                    available_at=replay_time,
                    symbol=intent.symbol,
                    payload=state.record().to_dict(),
                )

        if current_bars or current_funding:
            account = ledger.snapshot(replay_time)
            account_curve.append(account)
            timeline.append(
                "account",
                event_time=replay_time,
                available_at=replay_time,
                symbol=None,
                payload=account.to_dict(),
            )

    final_time = events[-1].ready_time if events else "1970-01-01T00:00:00.000000Z"
    final_account = ledger.snapshot(final_time)
    unsigned = {
        "version": MINUTE_REPLAY_VERSION,
        "run_id": config.run_id,
        "snapshot_id": config.snapshot_id,
        "input_sha256": input_sha256,
        "orders": [state.record().to_dict() for state in order_states],
        "fills": [item.to_dict() for item in fills],
        "positions": [item.to_dict() for item in final_account.positions],
        "account_curve": [item.to_dict() for item in account_curve],
        "timeline": [item.to_dict() for item in timeline.events],
        "final_account": final_account.to_dict(),
    }
    return MinuteReplayResult(
        orders=tuple(state.record() for state in order_states),
        fills=tuple(fills),
        positions=final_account.positions,
        account_curve=tuple(account_curve),
        timeline=tuple(timeline.events),
        cash=final_account.cash,
        equity=final_account.equity,
        realized_pnl=final_account.realized_pnl,
        unrealized_pnl=final_account.unrealized_pnl,
        funding_pnl=final_account.funding_pnl,
        commission=final_account.commission,
        slippage_cost=final_account.slippage_cost,
        total_pnl=final_account.total_pnl,
        input_sha256=input_sha256,
        reproducibility_hash=stable_hash(unsigned),
    )


__all__ = [
    "AccountSnapshot",
    "FillRecord",
    "FundingRateEvent",
    "InstrumentConfig",
    "MINUTE_REPLAY_VERSION",
    "MinuteBar",
    "MinuteReplayConfig",
    "MinuteReplayError",
    "MinuteReplayResult",
    "MinuteStrategy",
    "OrderIntent",
    "OrderRecord",
    "PositionSnapshot",
    "SentimentEvent",
    "StrategyContext",
    "TimelineEvent",
    "run_minute_replay",
]
