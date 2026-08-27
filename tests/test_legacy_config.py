from __future__ import annotations

import pytest

from app.legacy_config import LegacyConfigError, translate_kh_config


def test_translate_kh_config_maps_universe_period_costs_and_provenance() -> None:
    result = translate_kh_config(
        {
            "backtest": {
                "start_time": "20250101",
                "end_time": "20250703",
                "init_capital": 1_000_000,
                "trade_cost": {
                    "min_commission": 5,
                    "commission_rate": 0.0001,
                    "stamp_tax_rate": 0.0005,
                    "flow_fee": 0.2,
                    "slippage": {"type": "ratio", "ratio": 0.001},
                },
                "trigger": {"type": "1d"},
            },
            "data": {
                "stock_list": ["000001.sz", "000001.SZ", "600000.sh"],
                "kline_period": "1d",
                "dividend_type": "front",
            },
        },
        strategy_id="ma_cross_v1",
    )
    assert result["universe"]["symbols"] == ["000001.SZ", "600000.SH"]
    assert result["execution"]["commission_bps"] == pytest.approx(1.0)
    assert result["execution"]["stamp_tax_bps"] == pytest.approx(5.0)
    assert result["execution"]["slippage_ratio"] == pytest.approx(0.001)
    assert result["execution"]["price_adjustment"] == "forward"
    assert result["compatibility"]["requires_snapshot_binding"] is True


def test_translate_kh_config_rejects_missing_data_and_bad_slippage() -> None:
    with pytest.raises(LegacyConfigError, match="股票池"):
        translate_kh_config({"backtest": {"start_time": "20250101", "end_time": "20250102"}}, strategy_id="x")
    with pytest.raises(LegacyConfigError, match="slippage.type"):
        translate_kh_config(
            {
                "backtest": {"start_time": "20250101", "end_time": "20250102", "trade_cost": {"slippage": {"type": "bad"}}},
                "data": {"stock_list": ["000001.SZ"]},
            },
            strategy_id="x",
        )

