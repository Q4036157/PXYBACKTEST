from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from app.store import (
    IdempotencyConflictError,
    QueueLimitReachedError,
    TaskNotFoundError,
    TaskStore,
)
from app import store as store_module
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


def test_create_task_if_queue_available_checks_and_inserts_atomically(
    tmp_path: Path,
) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    store.create_task_if_queue_available(
        user_id="user-a",
        source_node="204",
        request=request_payload(),
        max_queued=1,
    )

    with pytest.raises(QueueLimitReachedError):
        store.create_task_if_queue_available(
            user_id="user-a",
            source_node="204",
            request=request_payload(),
            max_queued=1,
        )

    assert store.count_queued_for_user("user-a") == 1


def test_create_task_if_queue_available_is_atomic_across_store_instances(
    tmp_path: Path,
) -> None:
    database = tmp_path / "backtest.sqlite3"
    stores = [TaskStore(database), TaskStore(database)]
    barrier = Barrier(2)

    def submit(store: TaskStore) -> bool:
        barrier.wait()
        try:
            store.create_task_if_queue_available(
                user_id="user-a",
                source_node="204",
                request=request_payload(),
                max_queued=1,
            )
        except QueueLimitReachedError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, stores))

    assert sorted(results) == [False, True]


def test_idempotency_key_reuses_same_task_and_rejects_changed_request(
    tmp_path: Path,
) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    first = store.create_task_if_queue_available(
        user_id="user-a",
        source_node="204",
        request=request_payload(),
        max_queued=3,
        idempotency_key="submit-1",
    )
    replay = store.create_task_if_queue_available(
        user_id="user-a",
        source_node="109",
        request=request_payload(),
        max_queued=3,
        idempotency_key="submit-1",
    )

    assert replay.task_id == first.task_id
    assert replay.idempotent_replay is True
    changed = request_payload()
    changed["capital"] = 200_000
    with pytest.raises(IdempotencyConflictError):
        store.create_task_if_queue_available(
            user_id="user-a",
            source_node="204",
            request=changed,
            max_queued=3,
            idempotency_key="submit-1",
        )


def test_same_idempotency_key_is_isolated_by_user(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    first = store.create_task_if_queue_available(
        user_id="user-a",
        source_node="204",
        request=request_payload(),
        max_queued=3,
        idempotency_key="shared-key",
    )
    second = store.create_task_if_queue_available(
        user_id="user-b",
        source_node="204",
        request=request_payload(),
        max_queued=3,
        idempotency_key="shared-key",
    )

    assert first.task_id != second.task_id


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


def test_terminal_task_publishes_seven_day_result_expiration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(store_module.time, "time", lambda: 1_000.0)
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )

    store.append_event(
        task_id,
        "completed",
        {"result_path": str(tmp_path / "result.json")},
    )

    task = store.get_task("user-a", task_id)
    listed = store.list_tasks("user-a")[0]
    assert task["completed_at"] == 1_000.0
    assert task["expires_at"] == 1_000.0 + 7 * 24 * 60 * 60
    assert listed["expires_at"] == task["expires_at"]


def test_expired_result_is_marked_unavailable_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(store_module.time, "time", lambda: 1_000.0)
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.append_event(
        task_id,
        "completed",
        {"result_path": str(tmp_path / "result.json")},
    )

    expired = store.expire_due_results(1_000.0 + 7 * 24 * 60 * 60 + 1)

    assert expired == [task_id]
    assert store.expire_due_results(9_999_999.0) == []
    task = store.get_task("user-a", task_id)
    assert task["result_available"] is False
    assert task["result_expired"] is True
    assert store.get_result_path(task_id) == Path("")


def test_store_keeps_all_replayed_bars_in_execution_snapshot(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(task_id)

    store.append_events(
        task_id,
        [
            (
                "bar",
                {
                    "bar": {
                        "datetime": f"2026-08-{1 + index // 1440:02d} "
                        f"{index % 1440 // 60:02d}:{index % 60:02d}:00",
                        "open": index,
                        "close": index + 1,
                    },
                    "replay_seq": index + 1,
                },
            )
            for index in range(750)
        ],
    )

    task = store.get_task("user-a", task_id)

    assert len(task["live_bars"]) == 750
    assert task["live_bars"][0]["datetime"] == "2026-08-01 00:00:00"
    assert task["live_bars"][-1]["datetime"] == "2026-08-01 12:29:00"


def test_mark_running_keeps_column_and_state_json_consistent(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )

    assert store.mark_running(task_id) is True
    store.append_event(task_id, "state", {"progress": 1})

    assert store.get_task("user-a", task_id)["status"] == "running"


def test_store_merges_reliable_trade_events_without_snapshot_loss(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(task_id)

    first_trade = {
        "trade_id": "trade-1",
        "datetime": "2026-08-01 09:30:00",
        "direction": "LONG",
        "offset": "OPEN",
        "price": 100,
        "volume": 1,
    }
    second_trade = {
        "trade_id": "trade-2",
        "datetime": "2026-08-01 09:31:00",
        "direction": "SHORT",
        "offset": "CLOSE",
        "price": 101,
        "volume": 1,
    }
    store.append_event(task_id, "trade", {"trade": first_trade})
    store.append_event(task_id, "trades_snapshot", {"items": [second_trade]})
    store.append_event(task_id, "trade", {"trade": {**first_trade, "pnl": 0}})

    task = store.get_task("user-a", task_id)

    assert [trade["trade_id"] for trade in task["live_trades"]] == [
        "trade-1",
        "trade-2",
    ]
    assert task["live_trades"][0]["pnl"] == 0


def test_store_applies_event_batch_in_order(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(task_id)

    sequences = store.append_events(
        task_id,
        [
            ("state", {"progress": 10, "processed_bars": 1}),
            (
                "bar",
                {
                    "bar": {
                        "datetime": "2026-08-01 09:30:00",
                        "open": 100,
                        "close": 101,
                    }
                },
            ),
            ("state", {"progress": 20, "processed_bars": 2}),
        ],
    )

    task = store.get_task("user-a", task_id)
    assert len(sequences) == 3
    assert sequences == sorted(sequences)
    assert task["progress"] == 20
    assert task["processed_bars"] == 2
    assert task["event_seq"] == sequences[-1]


def test_store_persists_unified_execution_snapshot_for_resync(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(task_id)
    payload = {
        "contract_version": "pxybacktest.replay.v1",
        "run_id": task_id,
        "snapshot_id": "btsnap_v1_" + "a" * 32,
        "event_seq": 42,
        "simulated_at": "2026-08-01T01:00:00Z",
        "bars": {"BTC": {"close": 100}},
        "sentiment": {"news-1": {"score": 0.7}},
        "factors": {"factor-v1": {"value": 1.1}},
    }
    store.append_event(task_id, "execution_snapshot", {"snapshot": payload})

    task = store.get_task("user-a", task_id)
    assert task["execution_snapshot"]["snapshot_id"] == payload["snapshot_id"]
    assert task["execution_snapshot"]["bars"]["BTC"]["close"] == 100


def test_store_keeps_complete_append_only_event_history(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )

    sequences = store.append_events(
        task_id, [("state", {"progress": index}) for index in range(5_100)]
    )
    events = store.events_after("user-a", task_id, 0, limit=6_000)

    assert sequences == list(range(1, 5_101))
    assert [event["seq"] for event in events] == sequences


def test_private_draft_is_versioned_and_user_isolated(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")

    first = store.save_draft("user-a", "workbench", {"symbol": "A"})
    second = store.save_draft("user-a", "workbench", {"symbol": "B"})
    store.save_draft("user-b", "workbench", {"symbol": "C"})

    assert first["version"] == 1
    assert second["version"] == 2
    assert store.get_draft("user-a", "workbench")["payload"] == {"symbol": "B"}
    assert store.get_draft("user-b", "workbench")["payload"] == {"symbol": "C"}
    assert store.delete_draft("user-a", "workbench") is True
    with pytest.raises(TaskNotFoundError):
        store.get_draft("user-a", "workbench")


def test_event_sequences_are_contiguous_per_task_when_tasks_interleave(
    tmp_path: Path,
) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    first_task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    second_task_id = store.create_task(
        user_id="user-b", source_node="109", request=request_payload()
    )

    first_sequences = [
        store.append_event(first_task_id, "state", {"progress": 10}),
        store.append_event(first_task_id, "state", {"progress": 20}),
    ]
    second_sequences = [
        store.append_event(second_task_id, "state", {"progress": 10}),
        store.append_event(second_task_id, "state", {"progress": 20}),
    ]
    first_sequences.append(
        store.append_event(first_task_id, "state", {"progress": 30})
    )

    first_page = store.event_page("user-a", first_task_id, after_seq=0, limit=10)
    second_page = store.event_page("user-b", second_task_id, after_seq=0, limit=10)

    assert first_sequences == [1, 2, 3]
    assert second_sequences == [1, 2]
    assert [event["seq"] for event in first_page["events"]] == [1, 2, 3]
    assert [event["seq"] for event in second_page["events"]] == [1, 2]
    assert first_page["history_truncated"] is False
    assert first_page["resync"] is None


def test_store_reports_global_queue_position_without_leaking_other_user(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "backtest.sqlite3")
    running_id = store.create_task(
        user_id="user-b", source_node="109", request=request_payload()
    )
    first_pending_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    current_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(running_id)
    store.append_event(
        running_id,
        "state",
        {"progress": 25, "processed_bars": 25, "total_bars": 100},
    )

    context = store.queue_context("user-a", current_id)

    assert context["queue_ahead"] == 2
    assert context["queue_position"] == 3
    assert context["active_task"] == {
        "status": "running",
        "progress": 25,
        "processed_bars": 25,
        "total_bars": 100,
        "owned_by_current_user": False,
    }
    assert running_id not in str(context)
    assert store.queue_context("user-a", first_pending_id)["queue_position"] == 2


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


def test_cancelled_task_keeps_and_returns_partial_result(tmp_path: Path) -> None:
    settings = Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )
    store = TaskStore(settings.database_path)
    task_id = store.create_task(
        user_id="user-a", source_node="109", request=request_payload()
    )
    result_path = settings.results_dir / task_id / "result.json"
    result_path.parent.mkdir(parents=True)
    result_path.write_text(
        '{"complete":false,"termination_reason":"cancelled","trades":[]}',
        encoding="utf-8",
    )
    store.append_event(
        task_id,
        "cancelled",
        {
            "result_path": str(result_path),
            "result_available": True,
            "progress": 3.91,
        },
    )

    listed = store.list_tasks("user-a")[0]
    task = TaskManager(settings, store).result("user-a", task_id)

    assert listed["result_available"] is True
    assert listed["progress"] == 3.91
    assert task["status"] == "cancelled"
    assert task["result_available"] is True
    assert task["result"]["complete"] is False


def test_cancelled_task_without_result_stays_cancelled(tmp_path: Path) -> None:
    settings = Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )
    store = TaskStore(settings.database_path)
    task_id = store.create_task(
        user_id="user-a", source_node="109", request=request_payload()
    )
    store.append_event(task_id, "cancelled", {})

    task = TaskManager(settings, store).result("user-a", task_id)

    assert task["status"] == "cancelled"
    assert task["result_available"] is False
