from __future__ import annotations

import pytest

from app.costs import calculate_fill_cost, cost_kwargs


def test_stock_costs_apply_minimum_commission_and_sell_taxes() -> None:
    buy = calculate_fill_cost(
        price=10,
        quantity=100,
        side="buy",
        symbol="600000.SH",
        commission_rate=0.0003,
        min_commission=5,
        slippage_mode="ticks",
        slippage_ticks=2,
        tick_size=0.01,
    )
    assert buy.fill_price == pytest.approx(10.02)
    assert buy.commission == 5
    assert buy.stamp_tax == 0
    assert buy.transfer_fee > 0

    sell = calculate_fill_cost(
        price=10,
        quantity=1000,
        side="sell",
        symbol="000001.SZ",
        commission_rate=0.0003,
        min_commission=5,
        stamp_tax_rate=0.001,
        slippage_mode="ratio",
        slippage_ratio=0.001,
    )
    assert sell.fill_price == pytest.approx(9.99)
    assert sell.stamp_tax == pytest.approx(9.99)
    assert sell.transfer_fee == 0


def test_cost_model_rejects_invalid_side_and_extracts_execution_options() -> None:
    with pytest.raises(ValueError, match="side"):
        calculate_fill_cost(price=1, quantity=1, side="hold")
    assert cost_kwargs({"rate": 0.001, "stamp_tax": 0.0005})["commission_rate"] == 0.001

