from __future__ import annotations

import math

import pytest

from app.metrics import (
    annualized_return,
    benchmark_metrics,
    calmar_ratio,
    compute_metrics,
    drawdown_curve,
    enrich_metrics,
    max_drawdown,
    sharpe_ratio,
    trade_metrics,
)


def test_equity_metrics_use_negative_drawdown_and_preserve_finite_values() -> None:
    equity = [100.0, 110.0, 105.0, 120.0]
    assert drawdown_curve(equity) == [0.0, 0.0, 105 / 110 - 1.0, 0.0]
    assert max_drawdown(equity) == 105 / 110 - 1.0
    assert annualized_return(equity, periods_per_year=3) == pytest.approx(0.2)
    assert calmar_ratio(equity, periods_per_year=3) > 0
    assert math.isfinite(sharpe_ratio([0.1, -0.0454545, 0.142857], periods_per_year=3))


def test_trade_and_benchmark_metrics_are_deterministic() -> None:
    deals = [{"pnl": 10}, {"net_pnl": -5}, {"pnl_amount": 3}, {"pnl": -2}]
    trade = trade_metrics(deals)
    assert trade["n_trades"] == 4
    assert trade["win_rate"] == 0.5
    assert trade["profit_factor"] == 13 / 7

    benchmark = benchmark_metrics([100, 110, 105, 120], [100, 105, 105, 110])
    assert benchmark["benchmark_return"] == pytest.approx(0.1)
    assert math.isfinite(benchmark["beta"])


def test_compute_and_enrich_metrics_keep_existing_engine_values() -> None:
    computed = compute_metrics([100, 105, 110], deals=[{"pnl": 2}, {"pnl": -1}])
    assert computed["total_return"] == pytest.approx(0.1)
    enriched = enrich_metrics({"total_return": 999}, [100, 105, 110])
    assert enriched["total_return"] == 999
    assert "max_drawdown" in enriched
