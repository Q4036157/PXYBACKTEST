import pytest
from pydantic import ValidationError

from app.default_profiles import default_profile_catalog
from app.models import ExecutionModelV2, SetSpeedRequest
from app.replay import ResultReplayController


def test_visual_speed_defaults_to_three_and_accepts_half_speed() -> None:
    assert ExecutionModelV2().speed == 3
    assert SetSpeedRequest(speed=0.5).speed == 0.5
    assert SetSpeedRequest(speed=100).speed == 100
    with pytest.raises(ValidationError):
        SetSpeedRequest(speed=0.49)
    with pytest.raises(ValidationError):
        SetSpeedRequest(speed=100.01)


def test_recommended_profiles_use_three_x() -> None:
    assert all(
        profile["defaults"]["execution"]["speed"] == 3
        for profile in default_profile_catalog()
    )


def test_result_replay_half_speed_and_live_speed_change() -> None:
    events = [
        {
            "event_type": "market_bar",
            "event_time": f"2026-08-01T00:00:0{index}Z",
            "symbol": "XAU",
            "payload": {"symbol": "XAU", "datetime": index, "close": 2000 + index},
        }
        for index in range(2)
    ]
    controller = ResultReplayController(
        run_id="run-half-speed", snapshot_id="speed-snapshot",
        events=events, mode="visual", speed=0.5,
    )
    delays: list[float] = []
    result = controller.run(sleep=delays.append)
    assert result["processed_events"] == 2
    assert sum(delays) == pytest.approx(2.0)

    controller = ResultReplayController(
        run_id="run-live-speed", snapshot_id="speed-snapshot",
        events=events, mode="visual", speed=0.5,
    )
    delays = []
    changed = False

    def read_commands() -> list[dict]:
        nonlocal changed
        if delays and not changed:
            changed = True
            return [{"action": "speed", "speed": 10}]
        return []

    result = controller.run(read_commands=read_commands, sleep=delays.append)
    assert result["processed_events"] == 2
    assert result["execution_snapshot"]["replay"]["speed"] == 10
    assert sum(delays) == pytest.approx(0.1)
