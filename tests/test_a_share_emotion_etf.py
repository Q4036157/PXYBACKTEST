from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.a_share_emotion_etf import EMOTION_ETF_STRATEGY_HASH, replay_emotion_etf
from app.models import SubmitBacktestRequestV2


def _bar(day: str, open_: float, close: float) -> dict:
    return {"date": day, "symbol": "159819.SZ", "open": open_, "high": max(open_, close), "low": min(open_, close), "close": close}


def _emotion(day: str, score: int) -> dict:
    return {
        "trade_date": day,
        "available_at": f"{day}T14:30:00+08:00",
        "emotion_score": score,
        "emotion_label": "冰点" if score < 30 else "强势",
        "trade_date_ok": True,
        "coverage_ok": True,
        "method": "pxydata_breadth_v1",
    }


def _payload() -> dict:
    return {
        "schema_version": 2,
        "engine_type": "a_share_emotion_etf",
        "strategy": {"id": "etf_emotion_extreme_c_v1", "version": "builtin-v1", "source_hash": EMOTION_ETF_STRATEGY_HASH, "entrypoint": "etf_emotion_extreme_c_v1"},
        "universe": {"symbols": ["159819.SZ"]},
        "period": {"start": "2026-01-05T00:00:00+08:00", "end": "2026-01-09T23:59:59+08:00", "interval": "1d", "timezone": "Asia/Shanghai"},
        "data": {"selection": {"datasets": ["etf_snapshots", "market_emotion_daily"], "decision_time": "2026-01-10T00:00:00+08:00", "quality_policy": "require_pass"}},
        "execution": {"capital": 100_000, "mode": "BAR", "t_plus_one": True, "rate": 0.0004, "commission_bps": 4, "stamp_tax_bps": 0, "slippage_bps": 0},
        "parameters": {"entry_threshold": 30, "exit_threshold": 80, "lot_size": 100, "min_commission": 5},
    }


def test_ice_and_overheat_fill_on_next_open() -> None:
    days = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]
    bars = [_bar(days[0], 1.0, 1.0), _bar(days[1], 1.1, 1.2), _bar(days[2], 1.3, 1.4), _bar(days[3], 1.5, 1.5)]
    emotions = [_emotion(days[0], 20), _emotion(days[1], 50), _emotion(days[2], 85), _emotion(days[3], 50)]
    result = replay_emotion_etf(bars, emotions, symbol="159819.SZ", capital=100_000, commission_rate=0, slippage_bps=0, min_commission=0)
    assert [(item["side"], item["fill_date"]) for item in result["orders"]] == [("BUY", days[1]), ("SELL", days[3])]
    assert result["deals"][0]["entry_price"] == 1.1
    assert result["deals"][0]["exit_price"] == 1.5
    assert result["metrics"]["open_position"] == 0


def test_holding_period_does_not_add_repeated_ice_signals() -> None:
    days = ["2026-01-05", "2026-01-06", "2026-01-07"]
    result = replay_emotion_etf(
        [_bar(day, 1.0, 1.0) for day in days],
        [_emotion(day, 20) for day in days],
        symbol="159819.SZ", capital=10_000, commission_rate=0, slippage_bps=0, min_commission=0,
    )
    assert sum(item["side"] == "BUY" for item in result["orders"]) == 1
    assert result["metrics"]["open_position"] > 0


def test_signal_fills_on_next_etf_bar_when_next_day_has_no_emotion_row() -> None:
    days = ["2026-01-05", "2026-01-06", "2026-01-07"]
    result = replay_emotion_etf(
        [_bar(days[0], 1.0, 1.0), _bar(days[1], 1.1, 1.1), _bar(days[2], 1.2, 1.2)],
        [_emotion(days[0], 20), _emotion(days[2], 50)],
        symbol="159819.SZ", capital=10_000, commission_rate=0, slippage_bps=0, min_commission=0,
    )
    assert result["orders"][0]["fill_date"] == days[1]
    assert result["orders"][0]["price"] == 1.1


def test_contract_requires_official_emotion_dataset_and_t_plus_one() -> None:
    model = SubmitBacktestRequestV2.model_validate(_payload())
    assert model.engine_type == "a_share_emotion_etf"
    invalid = _payload()
    invalid["execution"]["t_plus_one"] = False
    with pytest.raises(ValidationError, match="T\\+1"):
        SubmitBacktestRequestV2.model_validate(invalid)
