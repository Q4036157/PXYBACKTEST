from __future__ import annotations

from decimal import Decimal

import pytest

from app.minute_replay import (
    FundingRateEvent,
    InstrumentConfig,
    MinuteBar,
    MinuteReplayConfig,
    OrderIntent,
    SentimentEvent,
    StrategyContext,
    run_minute_replay,
)


def _bar(
    minute: int,
    *,
    open_price: int,
    close: int | None = None,
    available_at: str | None = None,
) -> MinuteBar:
    close_price = close if close is not None else open_price
    return MinuteBar(
        symbol="BTCUSDT",
        event_time=f"2026-01-01T00:{minute:02d}:00+00:00",
        open=open_price,
        high=max(open_price, close_price) + 1,
        low=min(open_price, close_price) - 1,
        close=close_price,
        volume=100,
        available_at=available_at,
    )


class _FirstBarOrder:
    def __init__(self, side: str = "buy", quantity: int = 1) -> None:
        self.side = side
        self.quantity = quantity
        self.calls = 0

    def on_bar(self, context: StrategyContext, bar: MinuteBar):
        self.calls += 1
        if self.calls == 1:
            return OrderIntent(
                symbol=bar.symbol,
                side=self.side,
                quantity=self.quantity,
            )
        return None


def _config(**instrument_options) -> MinuteReplayConfig:
    return MinuteReplayConfig(
        initial_cash=10_000,
        instruments=[InstrumentConfig(symbol="BTCUSDT", **instrument_options)],
        run_id="test-universal-1m",
        snapshot_id="snap-test",
    )


def test_out_of_order_inputs_produce_identical_timeline_and_hash() -> None:
    bars = [_bar(1, open_price=100), _bar(2, open_price=101), _bar(3, open_price=102)]
    sentiments = [
        SentimentEvent(
            symbol="BTCUSDT",
            event_time="2026-01-01T00:01:00Z",
            value="0.5",
        )
    ]

    first = run_minute_replay(
        config=_config(),
        strategy=_FirstBarOrder(),
        bars=bars,
        sentiment_events=sentiments,
    )
    second = run_minute_replay(
        config=_config(),
        strategy=_FirstBarOrder(),
        bars=reversed(bars),
        sentiment_events=reversed(sentiments),
    )

    assert first.input_sha256 == second.input_sha256
    assert first.reproducibility_hash == second.reproducibility_hash
    assert [item.to_dict() for item in first.timeline] == [
        item.to_dict() for item in second.timeline
    ]


def test_future_sentiment_is_not_visible_before_available_at() -> None:
    seen: list[Decimal | None] = []

    class SentimentStrategy:
        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            latest = context.latest_sentiment(bar.symbol)
            seen.append(latest.value if latest else None)
            if latest and latest.value > 0:
                return OrderIntent(symbol=bar.symbol, side="buy", quantity=1)
            return None

    result = run_minute_replay(
        config=_config(),
        strategy=SentimentStrategy(),
        bars=[_bar(1, open_price=100), _bar(2, open_price=101), _bar(3, open_price=102)],
        sentiment_events=[
            SentimentEvent(
                symbol="BTCUSDT",
                event_time="2026-01-01T00:00:30Z",
                available_at="2026-01-01T00:02:00Z",
                value="0.8",
            )
        ],
    )

    assert seen == [None, Decimal("0.8"), Decimal("0.8")]
    assert len(result.fills) == 1
    assert result.fills[0].market_bar_time == "2026-01-01T00:03:00.000000Z"


def test_sentiment_is_not_visible_before_tradable_from() -> None:
    seen: list[Decimal | None] = []

    class SentimentStrategy:
        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            latest = context.latest_sentiment(bar.symbol)
            seen.append(latest.value if latest else None)
            return None

    run_minute_replay(
        config=_config(),
        strategy=SentimentStrategy(),
        bars=[
            _bar(1, open_price=100),
            _bar(2, open_price=101),
            _bar(3, open_price=102),
        ],
        sentiment_events=[
            SentimentEvent(
                symbol="BTCUSDT",
                event_time="2026-01-01T00:00:30Z",
                available_at="2026-01-01T00:01:30Z",
                tradable_from="2026-01-01T00:02:30Z",
                value="0.8",
            )
        ],
    )

    assert seen == [None, None, Decimal("0.8")]


def test_signal_at_close_fills_only_at_next_bar_open() -> None:
    result = run_minute_replay(
        config=_config(),
        strategy=_FirstBarOrder(),
        bars=[
            _bar(1, open_price=90, close=100),
            _bar(2, open_price=123, close=125),
        ],
    )

    assert len(result.orders) == len(result.fills) == 1
    assert result.orders[0].signal_bar_time == "2026-01-01T00:01:00.000000Z"
    assert result.fills[0].reference_price == Decimal("123")
    assert result.fills[0].event_time == "2026-01-01T00:01:00.000000Z"
    assert result.fills[0].available_at == "2026-01-01T00:02:00.000000Z"
    assert result.fills[0].market_bar_time == "2026-01-01T00:02:00.000000Z"
    assert result.fills[0].position_effect == "open"
    assert result.orders[0].status == "FILLED"


def test_late_older_bar_cannot_fill_an_order_from_a_newer_market_time() -> None:
    result = run_minute_replay(
        config=_config(),
        strategy=_FirstBarOrder(),
        bars=[
            _bar(2, open_price=102),
            _bar(
                1,
                open_price=101,
                available_at="2026-01-01T00:03:00Z",
            ),
            _bar(
                3,
                open_price=103,
                available_at="2026-01-01T00:04:00Z",
            ),
        ],
    )

    assert len(result.fills) == 1
    assert result.orders[0].signal_bar_time == "2026-01-01T00:02:00.000000Z"
    assert result.fills[0].market_bar_time == "2026-01-01T00:03:00.000000Z"
    assert result.fills[0].reference_price == Decimal("103")


def test_delayed_signal_cannot_fill_at_an_open_that_already_happened() -> None:
    result = run_minute_replay(
        config=_config(),
        strategy=_FirstBarOrder(),
        bars=[
            _bar(
                1,
                open_price=101,
                available_at="2026-01-01T00:03:30Z",
            ),
            _bar(
                2,
                open_price=102,
                available_at="2026-01-01T00:04:00Z",
            ),
            _bar(
                4,
                open_price=104,
                available_at="2026-01-01T00:04:30Z",
            ),
            _bar(5, open_price=105),
        ],
    )

    assert len(result.fills) == 1
    assert result.orders[0].created_at == "2026-01-01T00:03:30.000000Z"
    assert result.fills[0].event_time == "2026-01-01T00:04:00.000000Z"
    assert result.fills[0].market_bar_time == "2026-01-01T00:05:00.000000Z"
    assert result.fills[0].reference_price == Decimal("105")


@pytest.mark.parametrize(
    ("side", "entry", "exit_price", "expected"),
    [("buy", 100, 110, Decimal("40")), ("sell", 110, 100, Decimal("40"))],
)
def test_derivative_long_and_short_realized_pnl(
    side: str, entry: int, exit_price: int, expected: Decimal
) -> None:
    closing_side = "sell" if side == "buy" else "buy"

    class RoundTrip:
        def __init__(self) -> None:
            self.calls = 0

        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            self.calls += 1
            if self.calls == 1:
                return OrderIntent(symbol=bar.symbol, side=side, quantity=2)
            if self.calls == 2:
                return OrderIntent(symbol=bar.symbol, side=closing_side, quantity=2)
            return None

    result = run_minute_replay(
        config=_config(
            asset_class="futures",
            settlement="derivative",
            contract_multiplier=2,
            allow_short=True,
        ),
        strategy=RoundTrip(),
        bars=[
            _bar(1, open_price=99),
            _bar(2, open_price=entry),
            _bar(3, open_price=exit_price),
        ],
    )

    assert result.realized_pnl == expected
    assert result.total_pnl == expected
    assert result.position("BTCUSDT") is None
    assert [fill.position_effect for fill in result.fills] == ["open", "close"]
    assert result.fills[-1].closed_quantity == 2


def test_derivative_leverage_enforces_margin_and_reports_available_cash() -> None:
    rejected = run_minute_replay(
        config=MinuteReplayConfig(
            initial_cash=1_000,
            instruments=[
                InstrumentConfig(
                    symbol="BTCUSDT",
                    asset_class="futures",
                    settlement="derivative",
                    leverage=5,
                )
            ],
        ),
        strategy=_FirstBarOrder(quantity=100),
        bars=[_bar(1, open_price=100), _bar(2, open_price=100)],
    )
    accepted = run_minute_replay(
        config=MinuteReplayConfig(
            initial_cash=1_000,
            instruments=[
                InstrumentConfig(
                    symbol="BTCUSDT",
                    asset_class="futures",
                    settlement="derivative",
                    leverage=10,
                )
            ],
        ),
        strategy=_FirstBarOrder(quantity=100),
        bars=[_bar(1, open_price=100), _bar(2, open_price=100)],
    )

    assert rejected.orders[0].status == "REJECTED"
    assert rejected.orders[0].reason == "衍生品可用保证金不足"
    assert accepted.orders[0].status == "FILLED"
    assert accepted.account_curve[-1].used_margin == Decimal("1000.00000000")
    assert accepted.account_curve[-1].available_cash == Decimal("0E-8")


def test_cross_symbol_margin_uses_all_current_open_marks() -> None:
    class CrossSymbolOrders:
        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            if bar.symbol == "BBB" and bar.event_time.endswith("00:01:00.000000Z"):
                return OrderIntent(symbol="BBB", side="buy", quantity=1)
            if bar.symbol == "AAA" and bar.event_time.endswith("00:02:00.000000Z"):
                return OrderIntent(symbol="AAA", side="buy", quantity=1)
            return None

    def bar(symbol: str, minute: int, price: int) -> MinuteBar:
        return MinuteBar(
            symbol=symbol,
            event_time=f"2026-01-01T00:0{minute}:00Z",
            open=price,
            high=price + 1,
            low=max(1, price - 1),
            close=price,
        )

    result = run_minute_replay(
        config=MinuteReplayConfig(
            initial_cash=100,
            instruments=[
                InstrumentConfig(
                    symbol="AAA", asset_class="futures", leverage=2
                ),
                InstrumentConfig(
                    symbol="BBB", asset_class="futures", leverage=2
                ),
            ],
        ),
        strategy=CrossSymbolOrders(),
        bars=[
            bar("BBB", 1, 100),
            bar("AAA", 2, 100),
            bar("BBB", 2, 100),
            bar("AAA", 3, 100),
            bar("BBB", 3, 50),
        ],
    )

    aaa_order = next(item for item in result.orders if item.symbol == "AAA")
    assert aaa_order.status == "REJECTED"
    assert aaa_order.reason == "衍生品可用保证金不足"
    assert result.account_curve[-1].available_cash >= 0


def test_equal_size_reversal_still_checks_new_position_margin() -> None:
    class ReverseAfterLoss:
        def __init__(self) -> None:
            self.calls = 0

        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            self.calls += 1
            if self.calls == 1:
                return OrderIntent(symbol=bar.symbol, side="buy", quantity=1)
            if self.calls == 2:
                return OrderIntent(symbol=bar.symbol, side="sell", quantity=2)
            return None

    result = run_minute_replay(
        config=MinuteReplayConfig(
            initial_cash=60,
            instruments=[
                InstrumentConfig(
                    symbol="BTCUSDT",
                    asset_class="futures",
                    settlement="derivative",
                    leverage=2,
                )
            ],
        ),
        strategy=ReverseAfterLoss(),
        bars=[
            _bar(1, open_price=100, close=100),
            _bar(2, open_price=100, close=50),
            _bar(3, open_price=50, close=50),
        ],
    )

    assert [item.status for item in result.orders] == ["FILLED", "REJECTED"]
    assert result.orders[-1].reason == "衍生品可用保证金不足"
    assert result.position("BTCUSDT").quantity == 1


def test_funding_is_booked_at_explicit_ready_time() -> None:
    result = run_minute_replay(
        config=_config(asset_class="perpetual", settlement="derivative"),
        strategy=_FirstBarOrder(),
        bars=[_bar(1, open_price=100), _bar(2, open_price=100), _bar(3, open_price=100)],
        funding_events=[
            FundingRateEvent(
                symbol="BTCUSDT",
                event_time="2026-01-01T00:02:00Z",
                available_at="2026-01-01T00:02:30Z",
                rate="0.01",
                mark_price=100,
            )
        ],
    )

    assert result.funding_pnl == Decimal("-1.00000000")
    assert result.cash == Decimal("9999.00000000")
    funding = next(item for item in result.timeline if item.event_type == "funding")
    assert funding.ready_time == "2026-01-01T00:02:30.000000Z"
    assert funding.payload["payment"] == "-1.00000000"


def test_delayed_funding_uses_the_position_held_at_settlement_time() -> None:
    class CloseBeforeFundingArrives:
        def __init__(self) -> None:
            self.calls = 0

        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            self.calls += 1
            if self.calls == 1:
                return OrderIntent(symbol=bar.symbol, side="buy", quantity=1)
            if self.calls == 2:
                return OrderIntent(symbol=bar.symbol, side="sell", quantity=1)
            return None

    result = run_minute_replay(
        config=_config(asset_class="perpetual", settlement="derivative"),
        strategy=CloseBeforeFundingArrives(),
        bars=[
            _bar(1, open_price=100),
            _bar(2, open_price=100),
            _bar(3, open_price=100),
            _bar(4, open_price=100),
        ],
        funding_events=[
            FundingRateEvent(
                symbol="BTCUSDT",
                event_time="2026-01-01T00:02:00Z",
                available_at="2026-01-01T00:03:30Z",
                rate="0.01",
                mark_price=100,
            )
        ],
    )

    assert result.position("BTCUSDT") is None
    assert result.funding_pnl == Decimal("-1.00000000")
    funding = next(item for item in result.timeline if item.event_type == "funding")
    assert funding.payload["settled_quantity"] == "1.00000000"


def test_commission_and_slippage_are_applied_once() -> None:
    result = run_minute_replay(
        config=_config(
            commission_rate="0.001",
            slippage_bps=100,
            allow_short=False,
        ),
        strategy=_FirstBarOrder(),
        bars=[_bar(1, open_price=100), _bar(2, open_price=100, close=100)],
    )

    fill = result.fills[0]
    assert fill.price == Decimal("101.00")
    assert fill.slippage_cost == Decimal("1.00000000")
    assert fill.commission == Decimal("0.10100000")
    assert result.cash == Decimal("9898.89900000")
    assert result.equity == Decimal("9998.89900000")
    assert result.total_pnl == Decimal("-1.10100000")


def test_market_gap_uses_next_actual_bar_without_synthetic_minutes() -> None:
    result = run_minute_replay(
        config=_config(),
        strategy=_FirstBarOrder(),
        bars=[_bar(1, open_price=100), _bar(40, open_price=120)],
    )

    assert len(result.fills) == 1
    assert result.fills[0].reference_price == Decimal("120")
    assert result.fills[0].market_bar_time == "2026-01-01T00:40:00.000000Z"
    assert [item.event_time for item in result.account_curve] == [
        "2026-01-01T00:01:00.000000Z",
        "2026-01-01T00:40:00.000000Z",
    ]
