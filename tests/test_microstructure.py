from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.microstructure import replay_order_book_imbalance, run_microstructure_backtest
from app.models import SubmitBacktestRequestV2


def _tick(index: int, *, bid_depth: float, ask_depth: float) -> dict:
    timestamp = 1_786_425_600_000 + index
    return {
        "event_id": f"tick-{index}",
        "event_type": "tick",
        "exchange": "CZCE",
        "symbol": "CY609",
        "exchange_ts": f"2026-08-11T09:00:00.{index:03d}+08:00",
        "exchange_ts_ms": timestamp,
        "received_ts_ms": timestamp + 5,
        "last_price": 100.0,
        "bid_price1": 99.0,
        "ask_price1": 101.0,
        "bid_volume1": bid_depth,
        "ask_volume1": ask_depth,
    }


def _payload() -> dict:
    return {
        "schema_version": 2,
        "engine_type": "microstructure",
        "strategy": {
            "id": "order_book_imbalance_v1",
            "version": "builtin-v1",
            "source_hash": "a" * 64,
            "entrypoint": "order_book_imbalance_v1",
        },
        "universe": {"symbols": ["CY609"]},
        "period": {
            "start": "2026-08-11T09:00:00+08:00",
            "end": "2026-08-11T15:00:00+08:00",
            "interval": "tick",
            "timezone": "Asia/Shanghai",
        },
        "data": {
            "selection": {
                "datasets": ["market_ticks"],
                "decision_time": "2026-08-11T15:01:00+08:00",
                "quality_policy": "require_pass",
            }
        },
        "execution": {"capital": 100_000, "mode": "TICK"},
        "parameters": {
            "entry_threshold": 0.2,
            "exit_threshold": 0.0,
            "latency_ticks": 1,
            "max_hold_ticks": 10,
            "quantity": 1,
        },
    }


def test_next_tick_depth_matching_produces_microstructure_metrics() -> None:
    ticks = [
        _tick(0, bid_depth=10, ask_depth=2),
        _tick(1, bid_depth=10, ask_depth=2),
        _tick(2, bid_depth=2, ask_depth=10),
        _tick(3, bid_depth=2, ask_depth=10),
    ]

    result = replay_order_book_imbalance(
        ticks,
        capital=100_000,
        fee_rate=0,
        slippage_bps=0,
        parameters=_payload()["parameters"],
    )

    assert result["diagnostics"]["matching_model"] == "next_tick_visible_l1_ioc"
    assert result["metrics"]["n_trades"] == 1
    assert result["metrics"]["fill_rate"] == 1.0
    assert result["metrics"]["average_spread"] == 2.0
    assert result["orders"][0]["latency_ticks"] == 1
    assert result["deals"][0]["entry_price"] == 101.0
    assert result["deals"][0]["exit_price"] == 99.0


def test_depth_shortage_rejects_order_without_fake_fill() -> None:
    ticks = [
        _tick(0, bid_depth=10, ask_depth=2),
        _tick(1, bid_depth=10, ask_depth=2),
    ]
    parameters = {**_payload()["parameters"], "quantity": 3}

    result = replay_order_book_imbalance(
        ticks,
        capital=100_000,
        fee_rate=0,
        slippage_bps=0,
        parameters=parameters,
    )

    assert result["metrics"]["n_trades"] == 0
    assert result["metrics"]["fill_rate"] == 0.0
    assert result["orders"][0]["status"] == "rejected_depth"


def test_microstructure_contract_fails_closed_without_real_ticks() -> None:
    payload = _payload()
    payload["data"]["selection"]["datasets"] = ["kline_1m"]

    with pytest.raises(ValidationError, match="market_ticks"):
        SubmitBacktestRequestV2.model_validate(payload)


def test_microstructure_contract_accepts_tick_snapshot_selection() -> None:
    model = SubmitBacktestRequestV2.model_validate(_payload())

    assert model.execution.mode == "TICK"
    assert model.data.selection is not None


def test_microstructure_adapter_audits_all_real_ticks_and_fills(monkeypatch) -> None:
    payload = _payload()
    payload["data"] = {"snapshot": {"snapshot_id": "btsnap_v1_" + "a" * 32}}
    ticks = [
        _tick(0, bid_depth=10, ask_depth=2),
        _tick(1, bid_depth=10, ask_depth=2),
        _tick(2, bid_depth=2, ask_depth=10),
        _tick(3, bid_depth=2, ask_depth=10),
    ]
    monkeypatch.setattr("app.microstructure.load_manifest_ticks", lambda **_: ticks)

    result = run_microstructure_backtest(
        task_id="micro-audit",
        task=payload,
        manifest={"datasets": []},
        data_root=".",
    )

    assert result["replay_audit"]["event_count"] >= len(ticks)
    assert result["replay_audit"]["snapshot_id"] == payload["data"]["snapshot"]["snapshot_id"]
