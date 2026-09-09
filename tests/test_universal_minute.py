from __future__ import annotations

from unittest.mock import patch

import pytest

from app.universal_minute import (
    UNIVERSAL_STRATEGY_HASH,
    UniversalMinuteBacktestError,
    run_universal_minute_backtest,
)
from app.models import SubmitBacktestRequestV2
from app.replay import ResultReplayController


def _task() -> dict:
    return {
        "schema_version": 2,
        "engine_type": "universal_1m",
        "strategy": {
            "id": "universal_signal_v1",
            "version": "builtin-v1",
            "source_hash": "a" * 64,
            "entrypoint": "universal_signal_v1",
        },
        "universe": {"symbols": ["BTCUSDT"]},
        "period": {
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-01T00:04:00Z",
            "interval": "1m",
            "timezone": "UTC",
        },
        "data": {
            "snapshot": {
                "snapshot_id": "btsnap_v1_" + "b" * 32,
                "manifest_sha256": "c" * 64,
            }
        },
        "execution": {
            "capital": 1000,
            "commission_bps": 0,
            "slippage_bps": 0,
            "leverage": 10,
            "funding_fee": True,
        },
        "parameters": {
            "market": "crypto_perp",
            "asset_class": "perpetual",
            "settlement": "derivative",
            "base_currency": "USDT",
            "quantity": 1,
            "signal_mode": "sentiment",
            "entry_threshold": 0.2,
            "exit_threshold": 0.05,
        },
    }


def _bars() -> list[dict]:
    return [
        {
            "instrument_id": "BTCUSDT",
            "interval": "1m",
            "event_time": f"2026-01-01T00:0{minute}:00Z",
            "available_at": f"2026-01-01T00:0{minute}:00Z",
            "tradable_from": f"2026-01-01T00:0{minute}:00Z",
            "open": 99 + minute,
            "high": 101 + minute,
            "low": 98 + minute,
            "close": 100 + minute,
            "volume": 100,
            "source": "test",
        }
        for minute in range(1, 5)
    ]


def _sentiments() -> list[dict]:
    return [
        {
            "event_id": "positive",
            "instrument_id": "BTCUSDT",
            "scope": "instrument",
            "score": 0.8,
            "event_time": "2026-01-01T00:00:30Z",
            "available_at": "2026-01-01T00:00:40Z",
            "tradable_from": "2026-01-01T00:00:50Z",
            "revision_id": "v1",
            "source": "test",
        },
        {
            "event_id": "negative",
            "instrument_id": "BTCUSDT",
            "scope": "instrument",
            "score": -0.8,
            "event_time": "2026-01-01T00:02:10Z",
            "available_at": "2026-01-01T00:02:20Z",
            "tradable_from": "2026-01-01T00:02:30Z",
            "revision_id": "v1",
            "source": "test",
        },
    ]


def _funding() -> list[dict]:
    return [
        {
            "instrument_id": "BTCUSDT",
            "event_time": "2026-01-01T00:02:00Z",
            "settlement_time": "2026-01-01T00:02:00Z",
            "available_at": "2026-01-01T00:02:00Z",
            "tradable_from": "2026-01-01T00:02:00Z",
            "revision_id": "v1",
            "rate": 0.01,
            "rate_direction": "positive_long_pays_short",
            "mark_price": 102,
            "source": "test",
        }
    ]


def _run(*, bars=None, funding=None, sentiments=None) -> dict:
    rows = {
        "bars": bars if bars is not None else _bars(),
        "funding_rates": funding if funding is not None else _funding(),
        "sentiment_events": sentiments if sentiments is not None else _sentiments(),
    }
    with patch(
        "app.universal_minute.load_manifest_rows",
        side_effect=lambda **kwargs: rows[kwargs["dataset_name"]],
    ):
        return run_universal_minute_backtest(
            task_id="universal-test",
            task=_task(),
            manifest={"datasets": []},
            data_root="unused",
        )


def test_universal_task_replays_bars_sentiment_funding_and_account_in_order() -> None:
    result = _run()

    assert [item["status"] for item in result["orders"]] == ["FILLED", "FILLED"]
    assert [item["reference_price"] for item in result["fills"]] == ["101", "103"]
    assert result["metrics"]["funding_pnl"] == pytest.approx(-1.02)
    assert result["metrics"]["realized_pnl"] == pytest.approx(2)
    assert result["metrics"]["unrealized_pnl"] == pytest.approx(-1)
    assert result["metrics"]["n_fills"] == 2
    assert result["metrics"]["n_trades"] == 1
    assert result["metrics"]["final_equity"] == pytest.approx(999.98)
    assert result["positions"][0]["quantity"] == "-1.00000000"
    assert [item["position_effect"] for item in result["fills"]] == [
        "open",
        "reverse",
    ]
    assert len(result["deals"]) == 1
    assert result["deals"][0]["side"] == "long"
    assert result["deals"][0]["gross_pnl"] == "2.00000000"
    assert result["diagnostics"]["availability_rule"] == (
        "max(event_time,available_at,tradable_from)"
    )
    assert result["replay_audit"]["event_count"] == len(
        result["_replay_events"]
    )
    replay = ResultReplayController(
        run_id="universal-order-check",
        snapshot_id="snap-order-check",
        events=result["_replay_events"],
        mode="fast",
    )
    assert [event.source_seq for event in replay.feed.events] == list(
        range(1, len(result["_replay_events"]) + 1)
    )
    first_types = [event.event_type for event in replay.feed.events[:3]]
    assert first_types == ["sentiment", "bar_open", "market_bar"]
    assert len(result["reproducibility"]["result_sha256"]) == 64


def test_universal_adapter_rejects_non_minute_bars() -> None:
    bars = _bars()
    bars[0]["interval"] = "5m"

    with pytest.raises(UniversalMinuteBacktestError, match="interval=1m"):
        _run(bars=bars)


def test_universal_adapter_rejects_duplicate_funding_settlement() -> None:
    funding = _funding()

    with pytest.raises(UniversalMinuteBacktestError, match="重复资金费"):
        _run(funding=funding + [dict(funding[0], revision_id="v2")])


def test_universal_bars_only_task_does_not_read_optional_datasets() -> None:
    task = _task()
    task["execution"]["funding_fee"] = False
    task["parameters"]["signal_mode"] = "bar_return"
    requested: list[str] = []

    def load_only_bars(**kwargs):
        dataset = kwargs["dataset_name"]
        requested.append(dataset)
        if dataset != "bars":
            raise AssertionError(f"不应读取可选数据集: {dataset}")
        return _bars()

    with patch(
        "app.universal_minute.load_manifest_rows", side_effect=load_only_bars
    ):
        result = run_universal_minute_backtest(
            task_id="universal-bars-only",
            task=task,
            manifest={"datasets": []},
            data_root="unused",
        )

    assert requested == ["bars"]
    assert result["diagnostics"]["funding_events"] == 0
    assert result["diagnostics"]["sentiment_events"] == 0


def _contract_payload() -> dict:
    task = _task()
    task["strategy"]["source_hash"] = UNIVERSAL_STRATEGY_HASH
    task["data"] = {
        "selection": {
            "datasets": ["bars", "funding_rates", "sentiment_events"],
            "decision_time": "2026-01-01T00:05:00Z",
            "quality_policy": "require_pass",
        }
    }
    task["execution"].update(
        {
            "rate": 0,
            "mode": "BAR",
            "signal_time": "bar_close",
            "entry_fill": "next_bar_open",
            "exit_fill": "next_bar_open",
            "matching_policy": "bar_ohlc_conservative",
        }
    )
    return task


def test_task_v2_accepts_the_strict_universal_minute_contract() -> None:
    task = SubmitBacktestRequestV2.model_validate(_contract_payload())

    assert task.engine_type == "universal_1m"
    assert task.period.interval == "1m"
    assert task.execution.funding_fee is True


@pytest.mark.parametrize(
    ("market", "asset_class", "settlement", "funding_fee"),
    [
        ("cn_equity", "stock", "spot", False),
        ("cn_futures", "futures", "derivative", True),
        ("fx_cfd", "cfd", "derivative", True),
        ("crypto_perp", "perpetual", "derivative", True),
    ],
)
def test_task_v2_accepts_all_registered_market_domains(
    market: str,
    asset_class: str,
    settlement: str,
    funding_fee: bool,
) -> None:
    payload = _contract_payload()
    payload["parameters"].update(
        {
            "market": market,
            "asset_class": asset_class,
            "settlement": settlement,
            "leverage": 1 if settlement == "spot" else 10,
        }
    )
    payload["execution"]["leverage"] = payload["parameters"]["leverage"]
    payload["execution"]["funding_fee"] = funding_fee

    task = SubmitBacktestRequestV2.model_validate(payload)

    assert task.parameters["market"] == market
    assert task.parameters["asset_class"] == asset_class


def test_task_v2_rejects_missing_funding_dataset_when_enabled() -> None:
    payload = _contract_payload()
    payload["data"]["selection"]["datasets"].remove("funding_rates")

    with pytest.raises(ValueError, match="funding_rates"):
        SubmitBacktestRequestV2.model_validate(payload)


def test_task_v2_rejects_market_asset_mismatch() -> None:
    payload = _contract_payload()
    payload["parameters"]["market"] = "cn_equity"

    with pytest.raises(ValueError, match="market 与 asset_class"):
        SubmitBacktestRequestV2.model_validate(payload)


def test_task_v2_rejects_spot_funding() -> None:
    payload = _contract_payload()
    payload["parameters"].update(
        {
            "market": "cn_equity",
            "asset_class": "stock",
            "settlement": "spot",
            "leverage": 1,
        }
    )
    payload["execution"]["leverage"] = 1

    with pytest.raises(ValueError, match="不能启用 funding_fee"):
        SubmitBacktestRequestV2.model_validate(payload)
