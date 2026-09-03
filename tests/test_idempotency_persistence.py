import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from app.store import IdempotencyConflictError, TaskStore


def _request_payload(*, capital: int = 100_000) -> dict:
    return {
        "strategy_class": "ExampleStrategy",
        "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
        "interval": "1m",
        "start_time": "2026-08-01 00:00:00",
        "end_time": "2026-08-02 00:00:00",
        "parameters": {},
        "capital": capital,
        "rate": 0.0004,
        "slippage": 0,
        "speed": 50,
        "mode": "TICK",
    }


def test_idempotency_is_atomic_across_independent_store_instances(
    tmp_path: Path,
) -> None:
    database = tmp_path / "backtest.sqlite3"
    stores = [TaskStore(database), TaskStore(database)]
    barrier = Barrier(2)

    def submit(store: TaskStore):
        barrier.wait()
        return store.create_task_if_queue_available(
            user_id="user-a",
            source_node="204",
            request=_request_payload(),
            max_queued=3,
            idempotency_key="persistent-submit-1",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        receipts = list(executor.map(submit, stores))

    assert len({receipt.task_id for receipt in receipts}) == 1
    assert sorted(receipt.idempotent_replay for receipt in receipts) == [False, True]
    assert stores[0].count_queued_for_user("user-a") == 1

    changed_request = _request_payload(capital=200_000)
    for store in stores:
        with pytest.raises(
            IdempotencyConflictError,
            match="同一幂等键已用于不同的回测请求",
        ):
            store.create_task_if_queue_available(
                user_id="user-a",
                source_node="109",
                request=changed_request,
                max_queued=3,
                idempotency_key="persistent-submit-1",
            )


def test_legacy_schema_is_migrated_without_losing_existing_tasks(
    tmp_path: Path,
) -> None:
    database = tmp_path / "backtest.sqlite3"
    legacy_request = _request_payload()
    legacy_state = {
        "task_id": "legacy-task",
        "status": "completed",
        "progress": 100.0,
        "result_available": False,
    }
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                source_node TEXT NOT NULL,
                status TEXT NOT NULL,
                request_json TEXT NOT NULL,
                state_json TEXT NOT NULL,
                result_path TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        connection.execute(
            """
            INSERT INTO tasks(
                task_id, user_id, source_node, status, request_json,
                state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-task",
                "legacy-user",
                "204",
                "completed",
                json.dumps(legacy_request),
                json.dumps(legacy_state),
                1_000.0,
                2_000.0,
            ),
        )

    store = TaskStore(database)

    legacy_task = store.get_task("legacy-user", "legacy-task")
    assert legacy_task["task_id"] == "legacy-task"
    assert legacy_task["status"] == "completed"
    assert legacy_task["progress"] == 100.0

    with sqlite3.connect(database) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(tasks)").fetchall()
        }
    assert {"idempotency_key", "request_sha256"} <= columns

    first = store.create_task_if_queue_available(
        user_id="new-user",
        source_node="204",
        request=_request_payload(),
        max_queued=3,
        idempotency_key="post-migration-submit",
    )
    replay = store.create_task_if_queue_available(
        user_id="new-user",
        source_node="109",
        request=_request_payload(),
        max_queued=3,
        idempotency_key="post-migration-submit",
    )

    assert replay.task_id == first.task_id
    assert replay.idempotent_replay is True
    assert store.count_queued_for_user("new-user") == 1
