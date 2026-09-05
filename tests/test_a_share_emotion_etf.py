from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import datetime

import pytest
from pydantic import ValidationError

from app.a_share_emotion_etf import (
    EMOTION_ETF_STRATEGY_HASH,
    EmotionEtfBacktestError,
    replay_emotion_etf,
    run_emotion_etf_backtest,
)
from app.models import SubmitBacktestRequestV2
from app.replay import ExecutionSnapshot, ResultReplayController, SnapshotDataFeed


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
        "data": {"selection": {"datasets": ["kline_etf_daily", "market_emotion_daily"], "decision_time": "2026-01-10T00:00:00+08:00", "quality_policy": "require_pass"}},
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
    assert result["diagnostics"]["missing_emotion_days"] == 1
    assert result["diagnostics"]["missing_emotion_dates"] == [days[1]]
    assert result["diagnostics"]["emotion_coverage_complete"] is False
    assert [signal["signal_date"] for signal in result["signals"]] == [days[0]]


def test_contract_requires_official_emotion_dataset_and_t_plus_one() -> None:
    model = SubmitBacktestRequestV2.model_validate(_payload())
    assert model.engine_type == "a_share_emotion_etf"
    invalid = _payload()
    invalid["execution"]["t_plus_one"] = False
    with pytest.raises(ValidationError, match="T\\+1"):
        SubmitBacktestRequestV2.model_validate(invalid)


def _replay(bars=None, emotions=None):
    return replay_emotion_etf(
        bars if bars is not None else [_bar(day, 1, 1) for day in ("2026-01-05", "2026-01-06", "2026-01-07")],
        emotions if emotions is not None else [_emotion("2026-01-05", 20)],
        symbol="159819.SZ", capital=10_000, commission_rate=0,
        slippage_bps=0, min_commission=0,
    )


@pytest.mark.parametrize("price_dataset", ["kline_etf_daily", "etf_snapshots"])
def test_contract_preserves_selected_price_source(price_dataset):
    payload = _payload()
    payload["data"]["selection"]["datasets"][0] = price_dataset
    model = SubmitBacktestRequestV2.model_validate(payload)
    assert model.data.selection.datasets == [price_dataset, "market_emotion_daily"]


@pytest.mark.parametrize("sources", [[], ["kline_etf_daily", "etf_snapshots"], ["kline_daily"], ["kline_daily", "kline_etf_daily"]])
def test_contract_requires_exactly_one_price_source(sources):
    payload = _payload()
    payload["data"]["selection"]["datasets"] = sources + ["market_emotion_daily"]
    with pytest.raises(ValidationError, match="只能选择一种价格源"):
        SubmitBacktestRequestV2.model_validate(payload)


@pytest.mark.parametrize("field", ["open", "close"])
@pytest.mark.parametrize("value", [None, 0, -1, float("nan"), float("inf"), float("-inf"), True, "invalid"])
def test_daily_prices_fail_closed(field, value):
    bars = [_bar("2026-01-05", 1, 1), _bar("2026-01-06", 1, 1)]
    bars[0][field] = value
    with pytest.raises(EmotionEtfBacktestError, match=field):
        _replay(bars)


def test_intraday_last_price_cannot_replace_close():
    bars = [
        {"symbol": "159819.SZ", "data_date": day, "snapshot_at": f"{day}T{instant}+08:00", "open": 1, "last_price": price}
        for day in ("2026-01-05", "2026-01-06")
        for instant, price in (("10:00:00", 1), ("15:00:00", 2))
    ]
    for rows in (bars, list(reversed(bars))):
        with pytest.raises(EmotionEtfBacktestError, match="close"):
            _replay(rows)


@pytest.mark.parametrize("field", ["open", "close", "high", "volume"])
def test_conflicting_duplicate_daily_rows_fail_in_any_order(field):
    first = _bar("2026-01-05", 1, 1)
    conflicting = {**first, field: 2}
    bars = [first, conflicting, _bar("2026-01-06", 1, 1)]
    for rows in (bars, list(reversed(bars))):
        with pytest.raises(EmotionEtfBacktestError, match="重复记录不一致"):
            _replay(rows)


def test_identical_daily_duplicates_are_order_independent():
    bars = [_bar("2026-01-05", 1, 1), _bar("2026-01-06", 1, 2), _bar("2026-01-07", 2, 3)]
    expected = _replay(bars)
    assert _replay(list(reversed(bars + deepcopy(bars)))) == expected
    assert expected["diagnostics"]["kline_days"] == 3


@pytest.mark.parametrize("day", [None, "2026-02-30", "invalid"])
def test_daily_rows_require_valid_dates(day):
    with pytest.raises(EmotionEtfBacktestError, match="有效交易日期"):
        _replay([_bar(day, 1, 1), _bar("2026-01-06", 1, 1)])


@pytest.mark.parametrize("value", [None, "", "2026-01-05", "2026-01-05T14:30:00", "invalid", "2026-01-05T14:30:00+25:00"])
def test_emotion_requires_complete_timezone_aware_availability(value):
    with pytest.raises(EmotionEtfBacktestError, match="完整带时区时间"):
        _replay(emotions=[{**_emotion("2026-01-05", 20), "available_at": value}])


@pytest.mark.parametrize("value", ["2026-01-06T09:30:00+08:00", "2026-01-06T10:00:00+08:00", "2026-01-05T23:30:00-10:00"])
def test_emotion_unavailable_before_next_open_cannot_drive_backdated_fill(value):
    with pytest.raises(EmotionEtfBacktestError, match="禁止倒填成交"):
        _replay(emotions=[{**_emotion("2026-01-05", 20), "available_at": value}])


@pytest.mark.parametrize("value, expected_time", [
    ("2026-01-05T06:30:00Z", "2026-01-05T14:30:00+08:00"),
    ("2026-01-06T08:00:00+08:00", "2026-01-06T08:00:00+08:00"),
])
def test_valid_availability_uses_actual_instant(value, expected_time):
    result = _replay(emotions=[{**_emotion("2026-01-05", 20), "available_at": value}])
    assert result["signals"][0]["signal_time"] == expected_time
    assert result["orders"][0]["fill_time"] == "2026-01-06T09:30:00+08:00"


def test_close_valuation_cannot_expose_late_emotion():
    result = _replay(emotions=[{**_emotion("2026-01-05", 20), "available_at": "2026-01-05T16:00:00+08:00"}])
    assert result["equity_curve"][0]["event_time"] == "2026-01-05T15:00:00+08:00"
    assert result["equity_curve"][0]["emotion_score"] is None
    assert result["signals"][0]["signal_time"] == "2026-01-05T16:00:00+08:00"


def test_emotion_before_open_uses_position_at_actual_signal_time():
    emotions = [_emotion("2026-01-05", 20), {**_emotion("2026-01-06", 85), "available_at": "2026-01-06T08:00:00+08:00"}]
    result = _replay(emotions=emotions)
    assert [item["side"] for item in result["signals"]] == ["BUY"]
    assert result["metrics"]["open_position"] == 10_000


@pytest.mark.parametrize("available", ["2026-01-05T14:30:00+08:00", "2026-01-06T08:00:00+08:00"])
def test_conflicting_emotion_duplicates_do_not_depend_on_row_order(available):
    first = _emotion("2026-01-05", 20)
    conflict = {**first, "available_at": available, "emotion_score": 85}
    for rows in ([first, conflict], [conflict, first]):
        with pytest.raises(EmotionEtfBacktestError, match="重复记录不一致"):
            _replay(emotions=rows)


def _manifest_result(monkeypatch):
    bars = [_bar(day, 1, 1) for day in ("2026-01-05", "2026-01-06", "2026-01-07")]
    emotions = [_emotion("2026-01-05", 20), _emotion("2026-01-06", 85)]
    monkeypatch.setattr("app.a_share_emotion_etf._manifest_rows", lambda **kw: (bars if kw["dataset_name"] == "kline_etf_daily" else emotions, 1, 1))
    return run_emotion_etf_backtest(
        task_id="etf-times", task=_payload(),
        manifest={"datasets": [{"name": name} for name in ("kline_etf_daily", "market_emotion_daily")]},
        data_root="unused",
    )


def test_adapter_replays_actual_signal_open_and_close_times(monkeypatch):
    result = _manifest_result(monkeypatch)
    events = result["_replay_events"]
    feed = SnapshotDataFeed(snapshot_id="etf-times", datasets={"events": events})
    assert [(event.event_type, event.event_time) for event in feed.events if event.event_type in {"signal", "order"} or (event.event_type == "account" and "close" in event.payload)] == [
        ("signal", "2026-01-05T06:30:00.000000Z"),
        ("account", "2026-01-05T07:00:00.000000Z"),
        ("order", "2026-01-06T01:30:00.000000Z"),
        ("signal", "2026-01-06T06:30:00.000000Z"),
        ("account", "2026-01-06T07:00:00.000000Z"),
        ("order", "2026-01-07T01:30:00.000000Z"),
        ("account", "2026-01-07T07:00:00.000000Z"),
    ]
    assert [item["source_seq"] for item in events] == list(range(len(events)))
    assert all(datetime.fromisoformat(item["available_at"]).utcoffset() is not None for item in events)
    assert {event.event_type for event in feed.events} == {"market_bar", "sentiment", "signal", "order", "fill", "position", "account"}
    outcome = ResultReplayController(run_id="etf-times", snapshot_id="etf-times", events=events, mode="fast").run()
    assert outcome["replay_audit"] == result["replay_audit"]
    snapshot = outcome["execution_snapshot"]
    assert snapshot["account"]["event_time"] == "2026-01-07T15:00:00+08:00"
    assert len(snapshot["fills"]) == 2
    assert snapshot["positions"]["159819.SZ"]["quantity"] == 0
    assert snapshot["bar_history_count"] == 3
    assert len(snapshot["sentiment"]) == 2
    assert [item["score"] for item in snapshot["sentiment"].values()] == [20, 85]
    assert all(item["score"] == item["emotion_score"] and item["title"] == f"市场情绪 {item['emotion_label']}" for item in snapshot["sentiment"].values())
    assert result["diagnostics"]["execution_model"] == "precomputed_result_replay"
    assert result["diagnostics"]["replay_semantics"] == "result_replay"
    assert result["diagnostics"]["missing_emotion_days"] == 1
    assert result["diagnostics"]["missing_emotion_dates"] == ["2026-01-07"]
    assert any("缺失或质量不通过1个ETF交易日" in warning for warning in result["diagnostics"]["warnings"])


def test_projection_discloses_only_data_available_at_each_event(monkeypatch):
    result = _manifest_result(monkeypatch)
    feed = SnapshotDataFeed(snapshot_id="etf-times", datasets={"events": result["_replay_events"]})
    projection = ExecutionSnapshot(run_id="etf-times", snapshot_id="etf-times")
    for event in feed.events:
        projection.apply(event)
        if event.event_time == "2026-01-06T01:30:00.000000Z":
            assert "close" not in event.payload
            assert projection.bars["159819.SZ"]["date"] == "2026-01-05"
            assert len(projection.sentiment) == 1
            if event.event_type == "account":
                assert projection.positions["159819.SZ"]["quantity"] > 0
                assert len(projection.fills) == 1
        if event.event_type == "market_bar":
            assert event.event_time.endswith("T07:00:00.000000Z")


def test_same_instant_fill_and_sentiment_preserve_execution_order():
    result = _replay(emotions=[_emotion("2026-01-05", 20), {**_emotion("2026-01-06", 85), "available_at": "2026-01-06T09:30:00+08:00"}])
    feed = SnapshotDataFeed(snapshot_id="boundary", datasets={"events": result["_replay_events"]})
    assert [event.event_type for event in feed.events if event.event_time == "2026-01-06T01:30:00.000000Z"] == ["order", "fill", "position", "account", "sentiment", "signal"]
    assert [(order["side"], order["fill_date"]) for order in result["orders"]] == [("BUY", "2026-01-06"), ("SELL", "2026-01-07")]


def test_future_close_changes_cannot_change_open_fills_or_early_projection():
    bars = [_bar("2026-01-05", 1, 1), _bar("2026-01-06", 1, 1), _bar("2026-01-07", 1, 1)]
    before = _replay(bars)
    bars[1]["close"] = 100
    after = _replay(bars)
    cutoff = "2026-01-06T15:00:00+08:00"
    assert before["orders"] == after["orders"]
    assert [event for event in before["_replay_events"] if event["event_time"] < cutoff] == [event for event in after["_replay_events"] if event["event_time"] < cutoff]
    assert before["equity_curve"][1]["value"] != after["equity_curve"][1]["value"]


def test_next_open_uses_next_etf_trading_bar_across_weekend():
    result = _replay([_bar("2026-01-09", 1, 1), _bar("2026-01-12", 2, 2)], [_emotion("2026-01-09", 20)])
    assert result["orders"][0]["fill_time"] == "2026-01-12T09:30:00+08:00"
    assert result["orders"][0]["price"] == 2


@pytest.mark.parametrize("legacy_in", ["selection", "snapshot", "manifest", "mixed_manifest"])
def test_adapter_refuses_legacy_prices_without_switching_sources(monkeypatch, legacy_in):
    task = _payload()
    names = ["kline_etf_daily", "market_emotion_daily"]
    if legacy_in == "selection":
        task["data"]["selection"]["datasets"][0] = "etf_snapshots"
    elif legacy_in == "snapshot":
        task["data"] = {"snapshot": {"datasets": [{"name": "etf_snapshots"}, {"name": "market_emotion_daily"}]}}
    elif legacy_in == "manifest":
        names[0] = "etf_snapshots"
    else:
        names.append("etf_snapshots")
    original = deepcopy(task)
    def unexpected_read(**kwargs):
        pytest.fail("旧快照拒绝前不应读取任何价格文件")
    monkeypatch.setattr("app.a_share_emotion_etf._manifest_rows", unexpected_read)
    with pytest.raises(EmotionEtfBacktestError, match="unsupported.*新建kline_etf_daily"):
        run_emotion_etf_backtest(task_id="legacy", task=task, manifest={"datasets": [{"name": name} for name in names]}, data_root="unused")
    assert task == original


def test_adapter_reads_verified_formal_daily_parquet(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    datasets = []
    for name, rows in (
        ("kline_etf_daily", [_bar("2026-01-05", 1, 1), _bar("2026-01-06", 1.1, 1.2)]),
        ("market_emotion_daily", [_emotion("2026-01-05", 20)]),
    ):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        datasets.append({"name": name, "files": [{"path": path.name, "size_bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}]})
    result = run_emotion_etf_backtest(task_id="formal", task=_payload(), manifest={"datasets": datasets}, data_root=tmp_path)
    assert result["orders"][0]["price"] == 1.1
    assert result["diagnostics"]["price_dataset"] == "kline_etf_daily"
    assert result["diagnostics"]["verified_file_count"] == 2
