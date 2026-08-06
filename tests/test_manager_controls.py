import asyncio
import queue
import time
from pathlib import Path

from app.config import Settings
from app.manager import TaskManager, WorkerHandle
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


class FullQueue:
    def put_nowait(self, command: dict) -> None:
        raise queue.Full


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


def test_pause_resume_and_speed_wait_for_worker_ack(tmp_path: Path) -> None:
    manager, store, task_id = build_manager(tmp_path)
    manager._workers[task_id] = WorkerHandle(
        user_id="user-a",
        process=AliveProcess(),  # type: ignore[arg-type]
        event_queue=None,
        command_queue=AcknowledgingQueue(store, task_id),
    )

    assert asyncio.run(manager.pause("user-a", task_id)) is True
    assert store.get_task("user-a", task_id)["status"] == "paused"
    assert asyncio.run(manager.resume("user-a", task_id)) is True
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

    assert asyncio.run(manager.pause("user-a", task_id)) is False
    assert store.get_task("user-a", task_id)["status"] == "running"


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
    for index in range(150):
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
