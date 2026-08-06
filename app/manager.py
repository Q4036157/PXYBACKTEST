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
from .worker_process import run_preloaded_worker


class QueueLimitError(ValueError):
    pass


@dataclass
class WorkerHandle:
    user_id: str
    process: multiprocessing.Process
    event_queue: Any
    command_queue: Any
    terminal_event_received: bool = False


@dataclass
class WarmWorkerHandle:
    process: multiprocessing.Process
    event_queue: Any
    command_queue: Any
    job_queue: Any
    ready_queue: Any
    ready: bool = False


class TaskManager:
    def __init__(self, settings: Settings, store: TaskStore | None = None):
        self.settings = settings
        self.store = store or TaskStore(settings.database_path)
        self._context = multiprocessing.get_context("spawn")
        self._workers: dict[str, WorkerHandle] = {}
        self._warm_worker: WarmWorkerHandle | None = None
        self._loop_task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        if self._loop_task is not None:
            return
        self.settings.ensure_directories()
        await asyncio.to_thread(self.store.recover_interrupted_tasks)
        self._stopping = False
        self._warm_worker = self._spawn_warm_worker()
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
        if self._warm_worker is not None:
            if self._warm_worker.process.is_alive():
                self._warm_worker.process.terminate()
            self._warm_worker.process.join(timeout=2.0)
            self._warm_worker = None
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
        queued_count = await asyncio.to_thread(self.store.count_queued_for_user, user_id)
        if queued_count >= self.settings.max_queued_per_user:
            raise QueueLimitError("当前用户的回测排队任务已达到上限")
        return await asyncio.to_thread(
            self.store.create_task,
            user_id=user_id,
            source_node=source_node,
            request=request,
        )

    async def pause(self, user_id: str, task_id: str) -> bool:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        if task["status"] != "running":
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            return False
        try:
            handle.command_queue.put_nowait({"action": "pause"})
        except queue.Full:
            return False
        if await self._wait_for_state(user_id, task_id, status="paused"):
            return True
        try:
            handle.command_queue.put_nowait({"action": "resume"})
        except queue.Full:
            pass
        return False

    async def resume(self, user_id: str, task_id: str) -> bool:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        if task["status"] != "paused":
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            return False
        try:
            handle.command_queue.put_nowait({"action": "resume"})
        except queue.Full:
            return False
        if await self._wait_for_state(user_id, task_id, status="running"):
            return True
        try:
            handle.command_queue.put_nowait({"action": "pause"})
        except queue.Full:
            pass
        return False

    async def set_speed(self, user_id: str, task_id: str, speed: float) -> bool:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        if task["status"] not in {"running", "paused"}:
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            return False
        try:
            handle.command_queue.put_nowait({"action": "speed", "speed": speed})
        except queue.Full:
            return False
        return await self._wait_for_state(user_id, task_id, speed=speed)

    async def cancel(self, user_id: str, task_id: str) -> bool:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        if task["status"] in TERMINAL_STATUSES:
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            await asyncio.to_thread(self.store.set_status, task_id, "cancelled")
            await asyncio.to_thread(self.store.append_event, task_id, "cancelled", {})
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
            await self._drain_worker_events()
            await asyncio.to_thread(self._reap_workers)
            await asyncio.to_thread(self._schedule_workers)
            await asyncio.sleep(0.05)

    async def _wait_for_state(
        self,
        user_id: str,
        task_id: str,
        *,
        status: str | None = None,
        speed: float | None = None,
        timeout: float = 2.0,
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
            if task["status"] in TERMINAL_STATUSES:
                return False
            status_matches = status is None or task["status"] == status
            speed_matches = speed is None or abs(float(task.get("speed") or 0) - speed) < 1e-9
            if status_matches and speed_matches:
                return True
            await asyncio.sleep(0.025)
        return False

    async def _drain_worker_events(self) -> None:
        for task_id, handle in list(self._workers.items()):
            batch: list[tuple[str, dict[str, Any]]] = []
            for _ in range(100):
                try:
                    event = handle.event_queue.get_nowait()
                except queue.Empty:
                    break
                event_type = str(event.get("type") or "")
                payload = dict(event.get("data") or {})
                if not event_type:
                    continue
                batch.append((event_type, payload))
                if event_type in {"completed", "failed", "cancelled"}:
                    handle.terminal_event_received = True
            if batch:
                await asyncio.to_thread(self.store.append_events, task_id, batch)

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
            if self._warm_worker is None and not self._stopping:
                self._warm_worker = self._spawn_warm_worker()

    def _drain_single_worker(self, task_id: str, handle: WorkerHandle) -> None:
        deadline = time.monotonic() + 0.5
        batch: list[tuple[str, dict[str, Any]]] = []
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
            batch.append((event_type, dict(event.get("data") or {})))
            if event_type in {"completed", "failed", "cancelled"}:
                handle.terminal_event_received = True
        if batch:
            self.store.append_events(task_id, batch)

    def _schedule_workers(self) -> None:
        self._refresh_warm_worker()
        if len(self._workers) >= self.settings.max_concurrent_tasks:
            return
        warm_worker = self._warm_worker
        if warm_worker is None or not warm_worker.ready:
            return
        active_users = {handle.user_id for handle in self._workers.values()}
        for pending in self.store.pending_tasks():
            if pending["user_id"] in active_users:
                continue
            if not self.store.mark_running(pending["task_id"]):
                continue
            self._start_worker(pending, warm_worker)
            if len(self._workers) >= self.settings.max_concurrent_tasks:
                break

    def _refresh_warm_worker(self) -> None:
        warm_worker = self._warm_worker
        if warm_worker is None:
            return
        if warm_worker.ready:
            return
        try:
            ready = warm_worker.ready_queue.get_nowait()
        except queue.Empty:
            if not warm_worker.process.is_alive():
                self._warm_worker = self._spawn_warm_worker()
            return
        if ready.get("ready"):
            warm_worker.ready = True
        else:
            warm_worker.process.join(timeout=0.1)
            self._warm_worker = self._spawn_warm_worker()

    def _spawn_warm_worker(self) -> WarmWorkerHandle:
        event_queue = self._context.Queue(maxsize=2000)
        command_queue = self._context.Queue(maxsize=100)
        job_queue = self._context.Queue(maxsize=1)
        ready_queue = self._context.Queue(maxsize=1)
        process = self._context.Process(
            target=run_preloaded_worker,
            args=(
                str(self.settings.pxylh_root),
                self.settings.render_interval_ms,
                event_queue,
                command_queue,
                job_queue,
                ready_queue,
            ),
            name="pxybacktest-warm-worker",
            daemon=True,
        )
        process.start()
        return WarmWorkerHandle(
            process=process,
            event_queue=event_queue,
            command_queue=command_queue,
            job_queue=job_queue,
            ready_queue=ready_queue,
        )

    def _start_worker(
        self, pending: dict[str, Any], warm_worker: WarmWorkerHandle
    ) -> None:
        task_id = pending["task_id"]
        result_path = self.settings.results_dir / task_id / "result.json"
        warm_worker.job_queue.put_nowait(
            (task_id, pending["request"], str(result_path))
        )
        self._workers[task_id] = WorkerHandle(
            user_id=pending["user_id"],
            process=warm_worker.process,
            event_queue=warm_worker.event_queue,
            command_queue=warm_worker.command_queue,
        )
        self._warm_worker = None
