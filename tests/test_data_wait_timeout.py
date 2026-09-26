from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import replace

from app.main import _data_requirement_payload
from app.manager import TaskManager
from app.store import TaskStore
from test_contract_v2 import _snapshot
from test_data_waiting import RequirementClient, _body, _settings


def test_waiting_data_deadline_is_visible_and_expires_without_provider_response(tmp_path):
    settings = replace(_settings(tmp_path), data_wait_timeout_seconds=60)
    store = TaskStore(settings.database_path)
    manager = TaskManager(settings, store)
    body = _body()
    receipt = asyncio.run(
        manager.submit(
            user_id="user-a",
            source_node="204",
            request={"_task_contract": body.model_dump(mode="json")},
            initial_status="waiting_for_data",
        )
    )
    task_id = receipt.task_id
    client = RequirementClient(_snapshot())
    manager.configure_data_waiting(
        client,
        lambda *_: None,
        lambda request, task_id: _data_requirement_payload(
            _body(), task_id
        ),
    )
    requirement = asyncio.run(
        manager.register_data_requirement(
            task_id, _data_requirement_payload(body, task_id)
        )
    )
    created_at = store.get_task("user-a", task_id)["created_at"]
    assert requirement["wait_deadline_at"] == created_at + 60

    with sqlite3.connect(settings.database_path) as connection:
        connection.execute(
            "UPDATE tasks SET created_at = ? WHERE task_id = ?",
            (time.time() - 120, task_id),
        )

    asyncio.run(manager._poll_data_requirements())
    task = store.get_task("user-a", task_id)
    assert task["status"] == "failed"
    assert "等待数据超时" in task["error"]
