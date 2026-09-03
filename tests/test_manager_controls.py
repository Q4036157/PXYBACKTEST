import asyncio
import copy
import queue
import time
from pathlib import Path
from unittest.mock import AsyncMock

from app import store as store_module
from app.config import Settings
from app.manager import MAX_WORKER_EVENTS_PER_CYCLE, TaskManager, WorkerHandle
from app.store import TaskStore

from test_store import request_payload


class AliveProcess:
    def is_alive(self) -> bool:
        return True


class AcknowledgingQueue:
    def __init__(self, store: TaskStore, task_id: str):
        self.store = store
        self.task_id = task_id

    def put_nowait(self, command: dict) -> None:
        action = command["action"]
        if action == "pause":
            self.store.append_event(self.task_id, "state", {"status": "paused"})
        elif action == "resume":
            self.store.append_event(self.task_id, "state", {"status": "running"})
        elif action == "speed":
            self.store.append_event(
                self.task_id, "state", {"speed": float(command["speed"])}
            )
        elif action == "step":
            task = self.store.get_task("user-a", self.task_id)
            self.store.append_event(
                self.task_id,
                "state",
                {
                    "status": "paused",
                    "processed_bars": int(task.get("processed_bars") or 0) + 1,
                },
            )


class FullQueue:
    def put_nowait(self, command: dict) -> None:
        raise queue.Full


class RecordingQueue:
    def __init__(self) -> None:
        self.commands: list[dict] = []

    def put_nowait(self, command: dict) -> None:
        self.commands.append(command)


class SlowStore(TaskStore):
    def append_events(self, task_id: str, events: list[tuple[str, dict]]) -> list[int]:
        time.sleep(0.15)
        return super().append_events(task_id, events)


def build_manager(tmp_path: Path) -> tuple[TaskManager, TaskStore, str]:
    configured = Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )
    store = TaskStore(configured.database_path)
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(task_id)
    return TaskManager(configured, store), store, task_id


def build_non_cta_manager(tmp_path: Path) -> tuple[TaskManager, TaskStore, str]:
    configured = Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )
    store = TaskStore(configured.database_path)
    request = copy.deepcopy(request_payload())
    request["_task_contract"] = {
        "schema_version": 2,
        "engine_type": "factor_matrix",
        "execution": {"speed": 50, "execution_mode": "visual"},
    }
    task_id = store.create_task(user_id="user-a", source_node="204", request=request)
    store.mark_running(task_id)
    return TaskManager(configured, store), store, task_id


def test_pause_resume_and_speed_wait_for_worker_ack(tmp_path: Path) -> None:
    manager, store, task_id = build_manager(tmp_path)
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=AcknowledgingQueue(store, task_id),
    )

    pause_result = asyncio.run(manager.pause("user-a", task_id))
    assert pause_result.accepted is True
    assert pause_result.confirmed is True
    assert pause_result.status == "paused"
    assert store.get_task("user-a", task_id)["status"] == "paused"
    resume_result = asyncio.run(manager.resume("user-a", task_id))
    assert resume_result.accepted is True
    assert resume_result.confirmed is True
    assert resume_result.status == "running"
    assert store.get_task("user-a", task_id)["status"] == "running"
    assert asyncio.run(manager.set_speed("user-a", task_id, 20)) is True
    assert store.get_task("user-a", task_id)["speed"] == 20


def test_pause_does_not_change_state_when_command_queue_is_full(tmp_path: Path) -> None:
    manager, store, task_id = build_manager(tmp_path)
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=FullQueue(),
    )

    result = asyncio.run(manager.pause("user-a", task_id))
    assert result.accepted is False
    assert result.confirmed is False
    assert result.status == "running"
    assert store.get_task("user-a", task_id)["status"] == "running"


def test_step_advances_once_and_keeps_task_paused(tmp_path: Path) -> None:
    manager, store, task_id = build_manager(tmp_path)
    command_queue = AcknowledgingQueue(store, task_id)
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=command_queue,
    )
    store.append_event(task_id, "state", {"status": "paused"})

    result = asyncio.run(manager.step("user-a", task_id))

    assert result.accepted is True
    assert result.confirmed is True
    assert result.status == "paused"
    task = store.get_task("user-a", task_id)
    assert task["processed_bars"] == 1


def test_step_requires_paused_task(tmp_path: Path) -> None:
    manager, _store, task_id = build_manager(tmp_path)

    result = asyncio.run(manager.step("user-a", task_id))

    assert result.accepted is False
    assert result.confirmed is False
    assert result.status == "running"


def test_pause_and_resume_are_idempotent(tmp_path: Path) -> None:
    manager, store, task_id = build_manager(tmp_path)
    command_queue = RecordingQueue()
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=command_queue,
    )

    store.append_event(task_id, "state", {"status": "paused"})
    pause_result = asyncio.run(manager.pause("user-a", task_id))
    assert pause_result.confirmed is True
    assert command_queue.commands == []

    store.append_event(task_id, "state", {"status": "running"})
    resume_result = asyncio.run(manager.resume("user-a", task_id))
    assert resume_result.confirmed is True
    assert command_queue.commands == []


def test_non_cta_visual_controls_confirm_when_command_is_queued(tmp_path: Path) -> None:
    manager, store, task_id = build_non_cta_manager(tmp_path)
    command_queue = RecordingQueue()
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=command_queue,
    )
    manager._wait_for_state = AsyncMock(return_value=False)  # type: ignore[method-assign]

    pause_result = asyncio.run(manager.pause("user-a", task_id))
    assert pause_result.confirmed is True
    assert pause_result.status == "paused"
    assert asyncio.run(manager.set_speed("user-a", task_id, 50)) is True
    resume_result = asyncio.run(manager.resume("user-a", task_id))

    assert resume_result.confirmed is True
    assert store.get_task("user-a", task_id)["status"] == "running"
    assert store.get_task("user-a", task_id)["speed"] == 50
    assert command_queue.commands == [
        {"action": "pause"},
        {"action": "speed", "speed": 50},
        {"action": "resume"},
    ]

def test_unconfirmed_pause_never_enqueues_reverse_compensation(tmp_path: Path) -> None:
    manager, _store, task_id = build_manager(tmp_path)
    command_queue = RecordingQueue()
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=command_queue,
    )
    manager._wait_for_state = AsyncMock(return_value=False)  # type: ignore[method-assign]

    result = asyncio.run(manager.pause("user-a", task_id))

    assert result.accepted is True
    assert result.confirmed is False
    assert result.status == "running"
    assert command_queue.commands == [{"action": "pause"}]


def test_worker_event_persistence_does_not_block_asyncio_loop(tmp_path: Path) -> None:
    configured = Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )
    store = SlowStore(configured.database_path)
    task_id = store.create_task(
        user_id="user-a", source_node="204", request=request_payload()
    )
    store.mark_running(task_id)
    event_queue: queue.Queue = queue.Queue()
    for index in range(MAX_WORKER_EVENTS_PER_CYCLE + 50):
        event_queue.put({"type": "state", "data": {"progress": index}})
    manager = TaskManager(configured, store)
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=event_queue,
        command_queue=queue.Queue(),
    )

    async def run_scenario() -> None:
        drain_task = asyncio.create_task(manager._drain_worker_events())
        started_at = time.perf_counter()
        await asyncio.sleep(0.02)
        assert time.perf_counter() - started_at < 0.1
        assert not drain_task.done()
        await drain_task

    asyncio.run(run_scenario())
    assert event_queue.qsize() == 50


def test_manager_removes_expired_result_and_job_directories(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(store_module.time, "time", lambda: 1_000.0)
    manager, store, task_id = build_manager(tmp_path)
    result_dir = manager.settings.results_dir / task_id
    job_dir = manager.settings.jobs_dir / task_id
    result_dir.mkdir(parents=True)
    job_dir.mkdir(parents=True)
    result_path = result_dir / "result.json"
    result_path.write_text("{}", encoding="utf-8")
    (job_dir / "worker.json").write_text("{}", encoding="utf-8")
    store.append_event(task_id, "completed", {"result_path": str(result_path)})
    monkeypatch.setattr(
        store_module.time,
        "time",
        lambda: 1_000.0 + store_module.RESULT_RETENTION_SECONDS + 1,
    )

    assert manager.expire_results() == [task_id]

    assert not result_dir.exists()
    assert not job_dir.exists()
    task = manager.result("user-a", task_id)
    assert task["result_expired"] is True
    assert "result" not in task
