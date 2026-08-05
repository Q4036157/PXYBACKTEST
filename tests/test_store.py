from pathlib import Path

import pytest

from app.store import TaskNotFoundError, TaskStore
from app.config import Settings
from app.manager import TaskManager


def request_payload() -> dict:
    return {
        "strategy_class": "ExampleStrategy",
        "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
        "interval": "1m",
        "start_time": "2026-08-01 00:00:00",
        "end_time": "2026-08-02 00:00:00",
        "parameters": {},
        "capital": 100000,
        "rate": 0.0004,
        "slippage": 0,
        "speed": 50,
        "mode": "TICK",
    }


def test_store_applies_incremental_bar_events(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )

    store.mark_running(task_id)
    first_seq = store.append_event(
        task_id,
        "bar",
        {"bar": {"datetime": "2026-08-01 00:00:00", "open": 1, "close": 2}},
    )
    second_seq = store.append_event(
        task_id,
        "bar",
        {"bar": {"datetime": "2026-08-01 00:00:00", "open": 1, "close": 3}},
    )

    task = store.get_task("user-a", task_id)
    assert task["live_bars"] == [
        {"datetime": "2026-08-01 00:00:00", "open": 1, "close": 3}
    ]
    assert task["event_seq"] == second_seq
    events = store.events_after("user-a", task_id, first_seq)
    assert [event["seq"] for event in events] == [second_seq]


def test_store_enforces_task_ownership(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="109", request=request_payload()
    )

    with pytest.raises(TaskNotFoundError):
        store.get_task("user-b", task_id)


def test_store_hides_workstation_paths_and_tracebacks_from_events(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="109", request=request_payload()
    )
    store.append_event(
        task_id,
        "failed",
        {"error": "worker failed", "traceback": "D:\\private\\worker.py:10"},
    )
    store.append_event(
        task_id,
        "completed",
        {"result_path": "D:\\private\\result.json", "progress": 100},
    )

    task = store.get_task("user-a", task_id)
    events = store.events_after("user-a", task_id, 0)

    assert "result_path" not in task
    assert "traceback" not in events[0]["data"]
    assert "result_path" not in events[1]["data"]


def test_missing_result_file_changes_completed_task_to_failed(tmp_path: Path) -> None:
    settings = Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )
    store = TaskStore(settings.database_path)
    task_id = store.create_task(
        user_id="user-a", source_node="109", request=request_payload()
    )
    store.append_event(
        task_id,
        "completed",
        {"result_path": str(tmp_path / "missing.json"), "progress": 100},
    )
    manager = TaskManager(settings, store)

    task = manager.result("user-a", task_id)

    assert task["status"] == "failed"
    assert task["error"] == "回测结果文件不存在"
