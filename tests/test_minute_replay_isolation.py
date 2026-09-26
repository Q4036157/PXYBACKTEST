from __future__ import annotations

from decimal import Decimal

from app.minute_replay import (
    InstrumentConfig,
    MinuteBar,
    MinuteReplayConfig,
    OrderIntent,
    StrategyContext,
    run_minute_replay,
)
from test_minute_replay import _FirstBarOrder, _bar, _config


def test_multi_symbol_replay_keeps_instrument_ledgers_isolated() -> None:
    class BuyEachSymbol:
        def __init__(self) -> None:
            self.ordered: set[str] = set()

        def on_bar(self, context: StrategyContext, bar: MinuteBar):
            if bar.symbol in self.ordered:
                return None
            self.ordered.add(bar.symbol)
            return OrderIntent(symbol=bar.symbol, side="buy", quantity=1)

    bars = [
        MinuteBar(
            symbol=symbol,
            event_time=f"2026-01-01T00:0{minute}:00Z",
            open=price,
            high=price + 1,
            low=price - 1,
            close=price,
        )
        for minute, prices in ((1, {"AAA": 10, "BBB": 100}), (2, {"AAA": 11, "BBB": 110}))
        for symbol, price in prices.items()
    ]
    result = run_minute_replay(
        config=MinuteReplayConfig(
            initial_cash=10_000,
            instruments=[
                InstrumentConfig(symbol="AAA", asset_class="stock"),
                InstrumentConfig(
                    symbol="BBB",
                    asset_class="futures",
                    contract_multiplier=10,
                    leverage=10,
                ),
            ],
        ),
        strategy=BuyEachSymbol(),
        bars=bars,
    )

    assert [fill.symbol for fill in result.fills] == ["AAA", "BBB"]
    assert {position.symbol for position in result.positions} == {"AAA", "BBB"}
    assert result.position("AAA").market_value == Decimal("11.00000000")
    assert result.position("BBB").used_margin == Decimal("110.00000000")


def test_repeated_run_has_stable_events_and_reproducibility_hash() -> None:
    def run():
        return run_minute_replay(
            config=_config(asset_class="forex", settlement="derivative"),
            strategy=_FirstBarOrder(),
            bars=[_bar(1, open_price=100), _bar(2, open_price=101)],
        )

    first, second = run(), run()
    assert first.reproducibility_hash == second.reproducibility_hash
    assert [item.event_id for item in first.timeline] == [
        item.event_id for item in second.timeline
    ]
    assert first.to_dict() == second.to_dict()
