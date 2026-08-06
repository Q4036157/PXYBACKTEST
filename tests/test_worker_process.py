from app.worker_process import build_replay_event_snapshot


def test_replay_event_snapshot_is_bounded_to_twenty_items() -> None:
    events = [{"seq": index} for index in range(100)]

    snapshot = build_replay_event_snapshot(events)

    assert len(snapshot) == 20
    assert snapshot[0]["seq"] == 80
    assert snapshot[-1]["seq"] == 99
