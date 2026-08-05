from __future__ import annotations

import asyncio
import json
import multiprocessing
import queue
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .store import TERMINAL_STATUSES, TaskNotFoundError, TaskStore
from .worker_process import run_backtest_worker


class QueueLimitError(ValueError):
    pass


@dataclass
class WorkerHandle:
    user_id: str
    process: multiprocessing.Process
    event_queue: Any
    command_queue: Any
    terminal_event_received: bool = False


class TaskManager:
    def __init__(self, settings: Settings, store: TaskStore | None = None):
        self.settings = settings
        self.store = store or TaskStore(settings.database_path)
        self._context = multiprocessing.get_context("spawn")
        self._workers: dict[str, WorkerHandle] = {}
        self._loop_task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        if self._loop_task is not None:
            return
        self.settings.ensure_directories()
        self.store.recover_interrupted_tasks()
        self._stopping = False
        self._loop_task = asyncio.create_task(self._loop(), name="pxybacktest-manager")

    async def stop(self) -> None:
        self._stopping = True
        for handle in self._workers.values():
            try:
                handle.command_queue.put_nowait({"action": "cancel"})
            except queue.Full:
                pass
        await asyncio.sleep(0.5)
        for handle in self._workers.values():
            if handle.process.is_alive():
                handle.process.terminate()
            handle.process.join(timeout=2.0)
        self._workers.clear()
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

    async def submit(
        self, *, user_id: str, source_node: str, request: dict[str, Any]
    ) -> str:
        if self.store.count_queued_for_user(user_id) >= self.settings.max_queued_per_user:
            raise QueueLimitError("当前用户的回测排队任务已达到上限")
        return self.store.create_task(
            user_id=user_id, source_node=source_node, request=request
        )

    async def pause(self, user_id: str, task_id: str) -> bool:
        task = self.store.get_task(user_id, task_id)
        if task["status"] != "running":
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            return False
        handle.command_queue.put_nowait({"action": "pause"})
        self.store.set_status(task_id, "paused")
        return True

    async def resume(self, user_id: str, task_id: str) -> bool:
        task = self.store.get_task(user_id, task_id)
        if task["status"] != "paused":
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            return False
        handle.command_queue.put_nowait({"action": "resume"})
        self.store.set_status(task_id, "running")
        return True

    async def set_speed(self, user_id: str, task_id: str, speed: float) -> bool:
        task = self.store.get_task(user_id, task_id)
        if task["status"] not in {"running", "paused"}:
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            return False
        handle.command_queue.put_nowait({"action": "speed", "speed": speed})
        return True

    async def cancel(self, user_id: str, task_id: str) -> bool:
        task = self.store.get_task(user_id, task_id)
        if task["status"] in TERMINAL_STATUSES:
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            self.store.set_status(task_id, "cancelled")
            self.store.append_event(task_id, "cancelled", {})
            return True
        handle.command_queue.put_nowait({"action": "cancel"})
        return True

    def result(self, user_id: str, task_id: str) -> dict[str, Any]:
        task = self.store.get_task(user_id, task_id)
        if task["status"] != "completed":
            return task
        result_path = self.store.get_result_path(task_id)
        if not result_path.is_file():
            self.store.append_event(
                task_id,
                "failed",
                {"error": "回测结果文件不存在"},
            )
            return self.store.get_task(user_id, task_id)
        task["result"] = json.loads(result_path.read_text(encoding="utf-8"))
        return task

    async def _loop(self) -> None:
        while not self._stopping:
            self._drain_worker_events()
            self._reap_workers()
            self._schedule_workers()
            await asyncio.sleep(0.05)

    def _drain_worker_events(self) -> None:
        for task_id, handle in list(self._workers.items()):
            for _ in range(500):
                try:
                    event = handle.event_queue.get_nowait()
                except queue.Empty:
                    break
                event_type = str(event.get("type") or "")
                payload = dict(event.get("data") or {})
                if not event_type:
                    continue
                self.store.append_event(task_id, event_type, payload)
                if event_type in {"completed", "failed", "cancelled"}:
                    handle.terminal_event_received = True

    def _reap_workers(self) -> None:
        for task_id, handle in list(self._workers.items()):
            if handle.process.is_alive():
                continue
            handle.process.join(timeout=0.1)
            self._drain_single_worker(task_id, handle)
            if not handle.terminal_event_received:
                self.store.append_event(
                    task_id,
                    "failed",
                    {"error": f"回测Worker异常退出，exit_code={handle.process.exitcode}"},
                )
            self._workers.pop(task_id, None)

    def _drain_single_worker(self, task_id: str, handle: WorkerHandle) -> None:
        deadline = time.monotonic() + 0.5
        while True:
            try:
                event = handle.event_queue.get(timeout=0.05)
            except queue.Empty:
                if time.monotonic() >= deadline:
                    break
                continue
            event_type = str(event.get("type") or "")
            if not event_type:
                continue
            self.store.append_event(task_id, event_type, dict(event.get("data") or {}))
            if event_type in {"completed", "failed", "cancelled"}:
                handle.terminal_event_received = True

    def _schedule_workers(self) -> None:
        if len(self._workers) >= self.settings.max_concurrent_tasks:
            return
        active_users = {handle.user_id for handle in self._workers.values()}
        for pending in self.store.pending_tasks():
            if pending["user_id"] in active_users:
                continue
            if not self.store.mark_running(pending["task_id"]):
                continue
            self._start_worker(pending)
            if len(self._workers) >= self.settings.max_concurrent_tasks:
                break

    def _start_worker(self, pending: dict[str, Any]) -> None:
        task_id = pending["task_id"]
        event_queue = self._context.Queue(maxsize=2000)
        command_queue = self._context.Queue(maxsize=100)
        result_path = self.settings.results_dir / task_id / "result.json"
        process = self._context.Process(
            target=run_backtest_worker,
            args=(
                task_id,
                pending["request"],
                str(self.settings.pxylh_root),
                str(result_path),
                self.settings.render_interval_ms,
                event_queue,
                command_queue,
            ),
            name=f"pxybacktest-{task_id[:8]}",
            daemon=True,
        )
        process.start()
        self._workers[task_id] = WorkerHandle(
            user_id=pending["user_id"],
            process=process,
            event_queue=event_queue,
            command_queue=command_queue,
        )
