from app.manager import MAX_WORKER_EVENTS_PER_CYCLE, compact_worker_event_batch


def _bar_event(datetime: str, replay_seq: int) -> tuple[str, dict]:
    return (
        "bar",
        {
            "bar": {"datetime": datetime, "close": float(replay_seq)},
            "replay_seq": replay_seq,
        },
    )


def test_tick_events_are_compacted_to_latest_event_per_bar() -> None:
    events: list[tuple[str, dict]] = []
    replay_seq = 0
    for minute in range(4):
        datetime = f"2026-08-01 09:{30 + minute}:00"
        for _ in range(25):
            replay_seq += 1
            events.append(_bar_event(datetime, replay_seq))

    compacted = compact_worker_event_batch(events)

    assert len(compacted) == 4
    assert [item[1]["replay_seq"] for item in compacted] == [25, 50, 75, 100]
    assert all(item[1]["coalesced"] is True for item in compacted)


def test_compaction_preserves_durable_and_unkeyed_events() -> None:
    events = [
        _bar_event("2026-08-01 09:30:00", 1),
        ("trade", {"trade": {"trade_id": "trade-1"}}),
        _bar_event("2026-08-01 09:30:00", 2),
        ("bar", {"bar": {"close": 101}, "replay_seq": 3}),
        ("state", {"progress": 1.0}),
    ]

    compacted = compact_worker_event_batch(events)

    assert [event_type for event_type, _ in compacted] == [
        "trade",
        "bar",
        "bar",
        "state",
    ]
    assert compacted[1][1]["replay_seq"] == 2
    assert compacted[1][1]["coalesced"] is True
    assert "coalesced" not in compacted[2][1]
    assert MAX_WORKER_EVENTS_PER_CYCLE >= 125
