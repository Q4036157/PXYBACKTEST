from __future__ import annotations

import pytest

from app.replay import (
    EventCursor,
    ExecutionSnapshot,
    ReplayClock,
    ReplayAudit,
    ReplayError,
    ReplayEvent,
    ResultReplayController,
    ReplaySession,
    SnapshotDataFeed,
    VisualProjectionGate,
    build_replay_audit,
    event_from_row,
)


SNAPSHOT = "btsnap_v1_" + "a" * 32


def test_feed_is_deterministic_and_binds_snapshot() -> None:
    rows = {
        "market": [
            {
                "event_type": "market_tick",
                "event_time": "2026-08-01T09:30:01+08:00",
                "symbol": "600000.SH",
                "last_price": 10.1,
            },
            {
                "event_type": "market_tick",
                "event_time": "2026-08-01T09:30:00+08:00",
                "symbol": "600000.SH",
                "last_price": 10.0,
            },
        ]
    }
    first = SnapshotDataFeed(snapshot_id=SNAPSHOT, datasets=rows)
    second = SnapshotDataFeed(snapshot_id=SNAPSHOT, datasets=rows)
    assert first.fingerprint == second.fingerprint
    assert [item.payload["last_price"] for item in first] == [10.0, 10.1]
    assert all(item.snapshot_id == SNAPSHOT for item in first)


def test_feed_infers_market_bar_type_from_snapshot_dataset_name() -> None:
    feed = SnapshotDataFeed(
        snapshot_id=SNAPSHOT,
        datasets={
            "kline_1m": [
                {
                    "datetime": "2026-08-01T09:30:00+08:00",
                    "symbol": "XAU.GLOBAL",
                    "open": 2000,
                    "high": 2001,
                    "low": 1999,
                    "close": 2000.5,
                }
            ]
        },
    )
    assert feed.events[0].event_type == "market_bar"


def test_feed_normalizes_pxydata_tick_timestamps_and_event_aliases() -> None:
    feed = SnapshotDataFeed(
        snapshot_id=SNAPSHOT,
        datasets={
            "market_ticks": [
                {
                    "event_type": "trade",
                    "exchange_ts_ms": 1_754_000_000_000,
                    "received_ts_ms": 1_754_000_000_010,
                    "symbol": "CU.SHF",
                    "last_price": 80000,
                }
            ]
        },
    )
    event = feed.events[0]
    assert event.event_type == "trade_print"
    assert event.event_time.endswith("Z")
    assert event.available_at.endswith("Z")
    assert event.ready_time >= event.event_time


def test_available_at_prevents_lookahead_and_orders_same_time_events() -> None:
    market = ReplayEvent(
        event_type="market_tick",
        event_time="2026-08-01T01:00:00Z",
        available_at="2026-08-01T01:00:00Z",
        snapshot_id=SNAPSHOT,
        symbol="BTCUSDT",
        payload={"last_price": 100},
    )
    news = ReplayEvent(
        event_type="news",
        event_time="2026-08-01T00:59:00Z",
        available_at="2026-08-01T01:00:05Z",
        snapshot_id=SNAPSHOT,
        payload={"sentiment": 0.8},
    )
    feed = SnapshotDataFeed(snapshot_id=SNAPSHOT, datasets={"m": [market], "n": [news]})
    cursor = EventCursor(feed)
    assert [event.event_type for event in cursor.pop_ready("2026-08-01T01:00:00Z")] == [
        "market_tick"
    ]
    assert cursor.pop_ready("2026-08-01T01:00:04Z") == []
    assert [event.event_type for event in cursor.pop_ready("2026-08-01T01:00:05Z")] == [
        "news"
    ]


def test_clock_is_monotonic_and_speed_only_changes_wall_delay() -> None:
    clock = ReplayClock(start_time="2026-08-01T00:00:00Z", speed=10)
    assert clock.wall_delay("2026-08-01T00:01:00Z") == pytest.approx(6.0)
    clock.advance_to("2026-08-01T00:01:00Z")
    with pytest.raises(ReplayError, match="时间倒退"):
        clock.advance_to("2026-08-01T00:00:59Z")
    clock.set_speed(100)
    assert clock.wall_delay("2026-08-01T00:02:00Z") == pytest.approx(0.6)


def test_execution_snapshot_has_all_projection_domains() -> None:
    snapshot = ExecutionSnapshot(run_id="run-1", snapshot_id=SNAPSHOT)
    events = [
        event_from_row(
            {"event_type": "market_bar", "datetime": "2026-08-01T00:00:00Z", "symbol": "XAU", "close": 2000},
            snapshot_id=SNAPSHOT,
        ),
        event_from_row(
            {"event_type": "sentiment", "event_time": "2026-08-01T00:00:01Z", "score": 0.4},
            snapshot_id=SNAPSHOT,
        ),
        event_from_row(
            {"event_type": "factor", "event_time": "2026-08-01T00:00:02Z", "factor_set_id": "v1", "value": 1.2},
            snapshot_id=SNAPSHOT,
        ),
        event_from_row(
            {"event_type": "trade_print", "event_time": "2026-08-01T00:00:03Z", "symbol": "XAU", "price": 2000.2, "volume": 2},
            snapshot_id=SNAPSHOT,
        ),
        event_from_row(
            {"event_type": "news", "event_time": "2026-08-01T00:00:04Z", "event_id": "news-1", "headline": "test"},
            snapshot_id=SNAPSHOT,
        ),
        event_from_row(
            {"event_type": "fundamental", "event_time": "2026-08-01T00:00:05Z", "report_id": "report-1", "available_at": "2026-08-01T00:00:06Z", "eps": 1.2},
            snapshot_id=SNAPSHOT,
        ),
    ]
    for index, event in enumerate(events, start=1):
        snapshot.apply(event, event_seq=index)
    payload = snapshot.to_dict()
    assert payload["bars"]["XAU"]["close"] == 2000
    assert payload["sentiment"]
    assert payload["factors"]["v1"]["value"] == 1.2
    assert payload["trades"][0]["price"] == 2000.2
    assert payload["bars"]["XAU"]["close"] == 2000
    assert payload["news"]["news-1"]["headline"] == "test"
    assert payload["fundamentals"]["report-1"]["eps"] == 1.2
    assert payload["event_seq"] == 6


def test_feed_infers_pit_events_and_signals_without_collapsing_domains() -> None:
    feed = SnapshotDataFeed(
        snapshot_id=SNAPSHOT,
        datasets={
            "financials_pit": [
                {
                    "event_time": "2026-08-01T00:00:00Z",
                    "available_at": "2026-08-01T01:00:00Z",
                    "report_id": "r1",
                }
            ],
            "strategy_signals": [
                {
                    "event_time": "2026-08-01T01:00:00Z",
                    "signal": "buy",
                }
            ],
        },
    )
    assert [event.event_type for event in feed.events] == ["fundamental", "signal"]


def test_visual_gate_coalesces_without_dropping_execution_events() -> None:
    gate = VisualProjectionGate(interval_ms=33)
    events = [
        ReplayEvent(
            event_type="market_tick",
            event_time=f"2026-08-01T00:00:0{i}Z",
            snapshot_id=SNAPSHOT,
            payload={"last_price": i},
            source_seq=i,
        )
        for i in range(1, 4)
    ]
    assert gate.offer(events[0], now=0.0) is not None
    assert gate.offer(events[1], now=0.01) is None
    assert gate.offer(events[2], now=0.04).payload["last_price"] == 3
    assert gate.flush() is None


def test_replay_audit_chain_is_deterministic_and_counts_all_events() -> None:
    def build() -> ReplayAudit:
        audit = ReplayAudit(run_id="run-1", snapshot_id=SNAPSHOT)
        audit.record("market_tick", {"price": 100})
        audit.record("news", {"sentiment": 0.5})
        return audit

    first, second = build(), build()
    assert first.event_count == 2
    assert first.to_dict() == second.to_dict()


def test_build_replay_audit_sorts_available_time_and_keeps_all_domains() -> None:
    audit = build_replay_audit(
        run_id="run-1",
        snapshot_id=SNAPSHOT,
        events=[
            {
                "event_type": "factor",
                "event_time": "2026-08-01T00:00:00Z",
                "available_at": "2026-08-01T00:00:02Z",
                "payload": {"name": "mom", "value": 1.0},
            },
            {
                "event_type": "market_tick",
                "event_time": "2026-08-01T00:00:01Z",
                "payload": {"last_price": 100},
            },
            {
                "event_type": "fill",
                "event_time": "2026-08-01T00:00:03Z",
                "payload": {"price": 100.1},
            },
        ],
    )
    assert audit["event_count"] == 3
    assert audit["contract_version"] == "pxybacktest.replay.v1"
    assert len(audit["chain_sha256"]) == 64


def test_replay_session_processes_all_domains_in_order_and_exposes_snapshot() -> None:
    feed = SnapshotDataFeed(
        snapshot_id=SNAPSHOT,
        datasets={
            "kline_1m": [
                {"datetime": "2026-08-01T00:00:00Z", "symbol": "XAU", "close": 2000}
            ],
            "news": [
                {
                    "event_time": "2026-08-01T00:00:01Z",
                    "available_at": "2026-08-01T00:00:02Z",
                    "symbol": "XAU",
                    "score": 0.6,
                }
            ],
            "factor_matrix_daily": [
                {"event_time": "2026-08-01T00:00:02Z", "symbol": "XAU", "value": 1.3}
            ],
        },
    )
    session = ReplaySession(run_id="run-1", feed=feed)
    seen: list[str] = []
    assert session.run(lambda event, _: seen.append(event.event_type)) == 3
    assert seen == ["market_bar", "news", "factor"]
    result = session.execution_snapshot()
    assert result["audit"]["event_count"] == 3
    assert result["replay"]["remaining_events"] == 0
    assert result["sentiment"]
    assert result["factors"]


def test_event_requires_timezone_and_snapshot() -> None:
    with pytest.raises(ReplayError, match="带时区"):
        ReplayEvent(
            event_type="market_bar",
            event_time="2026-08-01 09:30:00",
            snapshot_id=SNAPSHOT,
            payload={},
        )
    with pytest.raises(ReplayError, match="snapshot_id"):
        ReplayEvent(
            event_type="market_bar",
            event_time="2026-08-01T01:00:00Z",
            snapshot_id="",
            payload={},
        )


def test_result_replay_controller_accepts_daily_events_and_preserves_bar_updates() -> None:
    frames: list[dict] = []
    controller = ResultReplayController(
        run_id="run-daily",
        snapshot_id=SNAPSHOT,
        events=[
            {
                "event_type": "market_bar",
                "event_time": f"2026-08-0{day}",
                "symbol": "600000.SH",
                "payload": {
                    "symbol": "600000.SH",
                    "datetime": f"2026-08-0{day}",
                    "open": 10 + day,
                    "high": 11 + day,
                    "low": 9 + day,
                    "close": 10.5 + day,
                },
            }
            for day in range(1, 4)
        ],
        mode="fast",
        render_interval_ms=10_000,
    )

    outcome = controller.run(on_snapshot=frames.append)

    assert outcome["complete"] is True
    assert outcome["processed_events"] == 3
    assert outcome["execution_snapshot"]["bar_history_count"] == 3
    assert [
        bar["datetime"]
        for frame in frames
        for bar in frame.get("bar_updates", [])
    ] == ["2026-08-01", "2026-08-02", "2026-08-03"]


def test_result_replay_controller_applies_pause_resume_speed_and_cancel() -> None:
    commands = iter(
        [
            [{"action": "pause"}],
            [{"action": "resume"}, {"action": "speed", "speed": 50}],
            [],
            [{"action": "cancel"}],
        ]
    )
    states: list[dict] = []
    controller = ResultReplayController(
        run_id="run-control",
        snapshot_id=SNAPSHOT,
        events=[
            {
                "event_type": "account",
                "event_time": f"2026-08-01T00:00:0{index}Z",
                "payload": {"value": 1_000_000 + index},
            }
            for index in range(1, 6)
        ],
        mode="fast",
        speed=20,
    )

    outcome = controller.run(
        read_commands=lambda: next(commands, []),
        on_state=states.append,
        sleep=lambda _: None,
    )

    assert outcome["complete"] is False
    assert outcome["termination_reason"] == "cancelled"
    assert 0 < outcome["processed_events"] < outcome["total_events"]
    assert outcome["execution_snapshot"]["replay"]["speed"] == 50
    assert outcome["execution_snapshot"]["account_curve"]
    assert [state.get("status") for state in states[:2]] == ["paused", "running"]


def test_visual_replay_speed_changes_wall_time_without_changing_event_count() -> None:
    events = [
        {
            "event_type": "market_bar",
            "event_time": f"2026-08-01T00:00:0{index}Z",
            "symbol": "XAU",
            "payload": {"symbol": "XAU", "datetime": index, "close": 2000 + index},
        }
        for index in range(3)
    ]

    def replay_at(speed: float) -> tuple[float, int]:
        delays: list[float] = []
        controller = ResultReplayController(
            run_id=f"run-{speed}",
            snapshot_id=SNAPSHOT,
            events=events,
            mode="visual",
            speed=speed,
        )
        outcome = controller.run(sleep=delays.append)
        return sum(delays), outcome["processed_events"]

    delay_20x, count_20x = replay_at(20)
    delay_50x, count_50x = replay_at(50)

    assert delay_20x == pytest.approx(0.1)
    assert delay_50x == pytest.approx(0.04)
    assert count_20x == count_50x == 3


def test_large_tick_replay_executes_every_tick_but_only_projects_frames() -> None:
    tick_count = 100_000
    executed = 0
    frames: list[dict] = []
    controller = ResultReplayController(
        run_id="run-ticks",
        snapshot_id=SNAPSHOT,
        events=(
            {
                "event_type": "market_tick",
                "event_time": 1_754_000_000_000 + index,
                "symbol": "XAU.GLOBAL",
                "payload": {
                    "symbol": "XAU.GLOBAL",
                    "last_price": 2_000 + index / 100_000,
                },
                "source_seq": index,
            }
            for index in range(tick_count)
        ),
        mode="fast",
        render_interval_ms=10_000,
    )

    def execute(*_args) -> None:
        nonlocal executed
        executed += 1

    outcome = controller.run(handler=execute, on_snapshot=frames.append)

    assert executed == tick_count
    assert outcome["processed_events"] == tick_count
    assert outcome["replay_audit"]["event_count"] == tick_count
    assert len(frames) <= 2


def test_high_frequency_book_and_factor_updates_are_coalesced_per_frame() -> None:
    frames: list[dict] = []
    events = []
    for index in range(1000):
        timestamp = 1_754_000_000_000 + index
        events.extend(
            [
                {
                    "event_type": "order_book",
                    "event_time": timestamp,
                    "symbol": "BTCUSDT",
                    "payload": {"symbol": "BTCUSDT", "bid_price1": 100 + index},
                },
                {
                    "event_type": "factor",
                    "event_time": timestamp,
                    "symbol": "BTCUSDT",
                    "payload": {"symbol": "BTCUSDT", "name": "ofi", "value": index},
                },
            ]
        )
    controller = ResultReplayController(
        run_id="run-depth",
        snapshot_id=SNAPSHOT,
        events=events,
        mode="fast",
        render_interval_ms=10_000,
    )

    outcome = controller.run(on_snapshot=frames.append)

    assert outcome["processed_events"] == 2000
    assert len(frames) <= 2
    assert max(len(frame["order_book_updates"]) for frame in frames) == 1
    assert max(len(frame["factor_updates"]) for frame in frames) == 1
