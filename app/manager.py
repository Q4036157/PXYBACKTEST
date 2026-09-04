from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import queue
import shutil
import time
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any

from .config import Settings
from .pxydata_client import DataRequirementManifestV1, SnapshotProviderError
from .store import (
    IdempotencyConflictError,
    TERMINAL_STATUSES,
    QueueLimitReachedError,
    TaskCreationReceipt,
    TaskStore,
)
from .worker_process import run_preloaded_worker


class QueueLimitError(ValueError):
    pass


MAX_WORKER_EVENTS_PER_CYCLE = 500
MAX_DATA_REQUIREMENTS_PER_CYCLE = 20
MAX_CONCURRENT_DATA_REQUIREMENTS = 4
DATA_REQUIREMENT_POLL_TIMEOUT_SECONDS = 35.0
DATA_REQUIREMENT_RETRY_MAX_SECONDS = 60.0
logger = logging.getLogger("backtest_service")


def compact_worker_event_batch(
    events: list[tuple[str, dict[str, Any]]],
) -> list[tuple[str, dict[str, Any]]]:
    """同一轮排空中每根 K 线只保留最新 Tick，持久事件全部保留。"""
    last_bar_indexes: dict[str, int] = {}
    bar_event_counts: dict[str, int] = {}
    for index, (event_type, payload) in enumerate(events):
        if event_type != "bar":
            continue
        bar = payload.get("bar")
        datetime = str(bar.get("datetime") or "") if isinstance(bar, dict) else ""
        if datetime:
            last_bar_indexes[datetime] = index
            bar_event_counts[datetime] = bar_event_counts.get(datetime, 0) + 1

    compacted: list[tuple[str, dict[str, Any]]] = []
    for index, event in enumerate(events):
        event_type, payload = event
        if event_type == "bar":
            bar = payload.get("bar")
            datetime = str(bar.get("datetime") or "") if isinstance(bar, dict) else ""
            if datetime and last_bar_indexes.get(datetime) != index:
                continue
            if datetime and bar_event_counts.get(datetime, 0) > 1:
                event = (event_type, {**payload, "coalesced": True})
        compacted.append(event)
    return compacted


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


@dataclass(frozen=True)
class PlaybackControlResult:
    accepted: bool
    confirmed: bool
    status: str


class TaskManager:
    def __init__(self, settings: Settings, store: TaskStore | None = None):
        self.settings = settings
        self.store = store or TaskStore(settings.database_path)
        self._context = multiprocessing.get_context("spawn")
        self._workers: dict[str, WorkerHandle] = {}
        self._warm_worker: WarmWorkerHandle | None = None
        self._loop_task: asyncio.Task | None = None
        self._data_loop_task: asyncio.Task | None = None
        self._stopping = False
        self._last_expiry_scan = 0.0
        self._last_data_requirement_scan = 0.0
        self._data_requirement_cursor = 0
        self._data_snapshot_client: Any | None = None
        self._data_ready_resolver: Callable[
            [dict[str, Any], DataRequirementManifestV1],
            Awaitable[tuple[dict[str, Any], dict[str, Any]]],
        ] | None = None
        self._data_requirement_builder: Callable[
            [dict[str, Any], str], dict[str, Any]
        ] | None = None

    def configure_data_waiting(
        self,
        snapshot_client: Any,
        resolver: Callable[
            [dict[str, Any], DataRequirementManifestV1],
            Awaitable[tuple[dict[str, Any], dict[str, Any]]],
        ],
        requirement_builder: Callable[
            [dict[str, Any], str], dict[str, Any]
        ],
    ) -> None:
        self._data_snapshot_client = snapshot_client
        self._data_ready_resolver = resolver
        self._data_requirement_builder = requirement_builder

    async def start(self) -> None:
        if self._loop_task is not None:
            return
        self.settings.ensure_directories()
        await asyncio.to_thread(self.store.recover_interrupted_tasks)
        await asyncio.to_thread(self.expire_results)
        self._stopping = False
        self._warm_worker = self._spawn_warm_worker()
        self._loop_task = asyncio.create_task(self._loop(), name="pxybacktest-manager")
        self._data_loop_task = asyncio.create_task(
            self._data_requirement_loop(), name="pxybacktest-data-requirements"
        )

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
        if self._data_loop_task is not None:
            self._data_loop_task.cancel()
            try:
                await self._data_loop_task
            except asyncio.CancelledError:
                pass
            self._data_loop_task = None

    async def submit(
        self,
        *,
        user_id: str,
        source_node: str,
        request: dict[str, Any],
        idempotency_key: str | None = None,
        initial_status: str = "pending",
        idempotency_payload: dict[str, Any] | None = None,
    ) -> TaskCreationReceipt:
        try:
            return await asyncio.to_thread(
                self.store.create_task_if_queue_available,
                user_id=user_id,
                source_node=source_node,
                request=request,
                max_queued=self.settings.max_queued_per_user,
                idempotency_key=idempotency_key,
                initial_status=initial_status,
                idempotency_payload=idempotency_payload,
            )
        except IdempotencyConflictError:
            raise
        except QueueLimitReachedError as exc:
            raise QueueLimitError(str(exc)) from exc
        except ValueError as exc:
            raise QueueLimitError(str(exc)) from exc

    async def pause(self, user_id: str, task_id: str) -> PlaybackControlResult:
        return await self._set_paused(user_id, task_id, paused=True)

    async def resume(self, user_id: str, task_id: str) -> PlaybackControlResult:
        return await self._set_paused(user_id, task_id, paused=False)

    async def _set_paused(
        self, user_id: str, task_id: str, *, paused: bool
    ) -> PlaybackControlResult:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        target_status = "paused" if paused else "running"
        current_status = str(task["status"])
        if current_status == target_status:
            return PlaybackControlResult(True, True, current_status)
        if current_status not in {"running", "paused"}:
            return PlaybackControlResult(False, False, current_status)
        handle = self._workers.get(task_id)
        if handle is None:
            return PlaybackControlResult(False, False, current_status)
        try:
            handle.command_queue.put_nowait(
                {"action": "pause" if paused else "resume"}
            )
        except queue.Full:
            return PlaybackControlResult(False, False, current_status)

        if str(task.get("engine_type") or "") != "vnpy_cta":
            # 非 CTA 引擎的暂停语义是冻结统一 ReplayClock/可视化事件游标；
            # 适配器计算可以继续完成，命令会由 worker 在回放前保留并执行。
            await asyncio.to_thread(
                self.store.append_event,
                task_id,
                "state",
                {"status": target_status},
            )
            return PlaybackControlResult(True, True, target_status)

        confirmed = await self._wait_for_state(
            user_id, task_id, status=target_status
        )
        latest = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        latest_status = str(latest["status"])
        return PlaybackControlResult(
            accepted=True,
            confirmed=confirmed and latest_status == target_status,
            status=latest_status,
        )

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
        if str(task.get("engine_type") or "") != "vnpy_cta":
            await asyncio.to_thread(
                self.store.append_event,
                task_id,
                "state",
                {"speed": float(speed)},
            )
            return True
        return await self._wait_for_state(user_id, task_id, speed=speed)

    async def step(self, user_id: str, task_id: str) -> PlaybackControlResult:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        current_status = str(task["status"])
        if current_status != "paused":
            return PlaybackControlResult(False, False, current_status)
        handle = self._workers.get(task_id)
        if handle is None:
            return PlaybackControlResult(False, False, current_status)
        processed_before = max(
            int(task.get("processed_bars") or 0),
            int(task.get("processed_events") or 0),
        )
        try:
            handle.command_queue.put_nowait({"action": "step"})
        except queue.Full:
            return PlaybackControlResult(False, False, current_status)

        confirmed = await self._wait_for_progress(
            user_id, task_id, processed_before=processed_before
        )
        latest = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        return PlaybackControlResult(True, confirmed, str(latest["status"]))

    async def cancel(self, user_id: str, task_id: str) -> bool:
        task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
        if task["status"] in TERMINAL_STATUSES:
            return False
        handle = self._workers.get(task_id)
        if handle is None:
            cancelled = await asyncio.to_thread(
                self.store.cancel_unstarted_task, task_id
            )
            if cancelled:
                return True
            # 调度器可能在上面的数据库事务前刚把 pending 任务交给 worker。
            handle = self._workers.get(task_id)
            if handle is None:
                return False
        handle.command_queue.put_nowait({"action": "cancel"})
        return True

    def result(self, user_id: str, task_id: str) -> dict[str, Any]:
        self.expire_results()
        task = self.store.get_task(user_id, task_id)
        if task["status"] not in {"completed", "cancelled", "failed"}:
            return task
        if task.get("result_expired"):
            return task
        result_path = self.store.get_result_path(task_id)
        if not result_path.is_file():
            if task["status"] in {"cancelled", "failed"}:
                task["result_available"] = False
                return task
            self.store.append_event(
                task_id,
                "failed",
                {"error": "回测结果文件不存在"},
            )
            return self.store.get_task(user_id, task_id)
        task["result"] = json.loads(result_path.read_text(encoding="utf-8"))
        task["result_available"] = True
        return task

    async def _loop(self) -> None:
        while not self._stopping:
            if time.monotonic() - self._last_expiry_scan >= 60:
                await asyncio.to_thread(self.expire_results)
            await self._drain_worker_events()
            await asyncio.to_thread(self._reap_workers)
            await asyncio.to_thread(self._schedule_workers)
            await asyncio.sleep(0.05)

    async def _data_requirement_loop(self) -> None:
        while not self._stopping:
            try:
                await self._poll_data_requirements()
            except Exception:
                # 单次协调器缺陷不能永久终止后台恢复循环。
                logger.exception("数据需求协调循环发生未处理异常")
                await asyncio.sleep(1.0)
            await asyncio.sleep(1.0)

    async def register_data_requirement(
        self, task_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """先持久化可重试请求，再向 PXYDATA 幂等登记。"""

        stub = {
            "contract_version": "pxydata.data-requirement.v1",
            "requirement_id": str(payload["requirement_id"]),
            "consumer_task_id": str(payload["consumer_task_id"]),
            "request_fingerprint": str(payload["request_fingerprint"]),
            "datasets": list(payload["datasets"]),
            "quality_policy": str(payload["quality_policy"]),
            "snapshot_kind": str(payload.get("snapshot_kind") or "snapshot"),
            "factor_set_id": payload.get("factor_set_id"),
            "status": "pending",
            "failure_reason": "",
            "snapshot": None,
            "registration_confirmed": False,
            "request": {key: value for key, value in payload.items() if key != "requirement_id"},
        }
        persisted = await asyncio.to_thread(
            self.store.append_waiting_event,
            task_id,
            "state",
            {
                "status": "waiting_for_data",
                "phase": "registering_data_requirement",
                "data_requirement": stub,
            },
        )
        if not persisted:
            return stub
        return await self._refresh_data_requirement(task_id, stub)

    async def _poll_data_requirements(self) -> None:
        if (
            self._data_snapshot_client is None
            or self._data_ready_resolver is None
            or self._data_requirement_builder is None
        ):
            return
        now = time.monotonic()
        if now - self._last_data_requirement_scan < 1.0:
            return
        self._last_data_requirement_scan = now
        waiting = await asyncio.to_thread(self.store.waiting_tasks)
        if not waiting:
            self._data_requirement_cursor = 0
            return
        start = self._data_requirement_cursor % len(waiting)
        ordered = waiting[start:] + waiting[:start]
        selected = ordered[:MAX_DATA_REQUIREMENTS_PER_CYCLE]
        self._data_requirement_cursor = (
            start + len(selected)
        ) % len(waiting)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DATA_REQUIREMENTS)

        async def poll_one(task: dict[str, Any]) -> None:
            async with semaphore:
                try:
                    await asyncio.wait_for(
                        self._poll_data_requirement(task),
                        timeout=DATA_REQUIREMENT_POLL_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    latest = await asyncio.to_thread(
                        self.store.get_task, task["user_id"], task["task_id"]
                    )
                    current = dict(latest.get("data_requirement") or {})
                    await self._record_data_requirement_error(
                        task["task_id"], current, "PXYDATA 数据需求轮询超时"
                    )
                except Exception as exc:
                    latest = await asyncio.to_thread(
                        self.store.get_task, task["user_id"], task["task_id"]
                    )
                    current = dict(latest.get("data_requirement") or {})
                    await self._record_data_requirement_error(
                        task["task_id"],
                        current,
                        f"数据需求协调异常: {type(exc).__name__}: {exc}",
                    )

        await asyncio.gather(*(poll_one(task) for task in selected))

    async def _poll_data_requirement(self, task: dict[str, Any]) -> None:
        assert self._data_requirement_builder is not None
        assert self._data_ready_resolver is not None
        current = dict(task["state"].get("data_requirement") or {})
        if float(current.get("next_poll_at") or 0) > time.time():
            return
        if not current:
            try:
                rebuilt = self._data_requirement_builder(task["request"], task["task_id"])
            except ValueError as exc:
                await asyncio.to_thread(
                    self.store.append_waiting_event,
                    task["task_id"],
                    "failed",
                    {"error": f"数据需求恢复失败: {exc}"},
                )
                return
            await self.register_data_requirement(task["task_id"], rebuilt)
            return
        manifest = await self._refresh_data_requirement(task["task_id"], current)
        status = str(manifest.get("status") or "pending")
        if status == "failed":
            await asyncio.to_thread(
                self.store.append_waiting_event,
                task["task_id"],
                "failed",
                {
                    "error": "数据补齐失败: "
                    + str(manifest.get("failure_reason") or "未知原因")
                },
            )
            return
        if status != "ready":
            return
        if not await asyncio.to_thread(
            self.store.is_waiting_task, task["task_id"]
        ):
            return
        try:
            parsed = DataRequirementManifestV1.model_validate(
                {
                    key: value
                    for key, value in manifest.items()
                    if key
                    not in {
                        "request",
                        "registration_confirmed",
                        "last_error",
                        "retry_count",
                        "next_poll_at",
                    }
                }
            )
            worker_request, data_snapshot = await self._data_ready_resolver(
                task["request"], parsed
            )
        except (ValueError, SnapshotProviderError) as exc:
            provider_status = getattr(exc, "status_code", 422)
            if provider_status in {409, 422}:
                await asyncio.to_thread(
                    self.store.append_waiting_event,
                    task["task_id"],
                    "failed",
                    {"error": f"数据快照验收失败: {exc}"},
                )
            else:
                await self._record_data_requirement_error(
                    task["task_id"], manifest, str(exc)
                )
            return
        await asyncio.to_thread(
            self.store.activate_waiting_task,
            task["task_id"],
            request=worker_request,
            data_snapshot=data_snapshot,
            data_requirement=manifest,
        )

    async def _refresh_data_requirement(
        self, task_id: str, current: dict[str, Any]
    ) -> dict[str, Any]:
        client = self._data_snapshot_client
        if client is None:
            return current
        request = dict(current.get("request") or {})
        try:
            if current.get("registration_confirmed"):
                manifest = await client.get_data_requirement(
                    current["requirement_id"], expected=request
                )
            else:
                manifest = await client.create_data_requirement(request)
        except SnapshotProviderError as exc:
            if exc.status_code == 404 and current.get("registration_confirmed"):
                retryable = {
                    **current,
                    "registration_confirmed": False,
                    "last_error": "PXYDATA 未找到已登记需求，准备重新登记",
                    "retry_count": 0,
                    "next_poll_at": 0,
                }
                await asyncio.to_thread(
                    self.store.append_waiting_event,
                    task_id,
                    "state",
                    {
                        "status": "waiting_for_data",
                        "phase": "registering_data_requirement",
                        "data_requirement": retryable,
                    },
                )
                return retryable
            if exc.status_code in {400, 401, 403, 409, 422}:
                await asyncio.to_thread(
                    self.store.append_waiting_event,
                    task_id,
                    "failed",
                    {"error": f"数据需求登记失败: {exc}"},
                )
                return {**current, "status": "failed", "failure_reason": str(exc)}
            await self._record_data_requirement_error(task_id, current, str(exc))
            return current
        payload = {
            **manifest.model_dump(mode="json"),
            "registration_confirmed": True,
            "request": request,
            "retry_count": 0,
            "next_poll_at": 0,
        }
        if (
            current.get("status") != payload["status"]
            or not current.get("registration_confirmed")
            or bool(current.get("last_error"))
        ):
            await asyncio.to_thread(
                self.store.append_waiting_event,
                task_id,
                "state",
                {
                    "status": "waiting_for_data",
                    "phase": f"data_{payload['status']}",
                    "data_requirement": payload,
                },
            )
        return payload

    async def _record_data_requirement_error(
        self, task_id: str, current: dict[str, Any], error: str
    ) -> None:
        retry_count = int(current.get("retry_count") or 0) + 1
        retry_delay = min(
            DATA_REQUIREMENT_RETRY_MAX_SECONDS,
            float(2 ** min(retry_count, 6)),
        )
        payload = {
            **current,
            "last_error": error[:500],
            "retry_count": retry_count,
            "next_poll_at": time.time() + retry_delay,
        }
        await asyncio.to_thread(
            self.store.append_waiting_event,
            task_id,
            "state",
            {
                "status": "waiting_for_data",
                "phase": "data_provider_unavailable",
                "data_requirement": payload,
            },
        )

    def expire_results(self) -> list[str]:
        self._last_expiry_scan = time.monotonic()
        expired = self.store.expire_due_results()
        for task_id in expired:
            for root in (self.settings.results_dir, self.settings.jobs_dir):
                target = (root / task_id).resolve()
                try:
                    target.relative_to(root.resolve())
                except ValueError:
                    continue
                if target.is_dir():
                    shutil.rmtree(target)
        return expired

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
            speed_matches = (
                speed is None or abs(float(task.get("speed") or 0) - speed) < 1e-9
            )
            if status_matches and speed_matches:
                return True
            await asyncio.sleep(0.025)
        return False

    async def _wait_for_progress(
        self,
        user_id: str,
        task_id: str,
        *,
        processed_before: int,
        timeout: float = 2.0,
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            task = await asyncio.to_thread(self.store.get_task, user_id, task_id)
            if task["status"] in TERMINAL_STATUSES:
                return False
            processed = max(
                int(task.get("processed_bars") or 0),
                int(task.get("processed_events") or 0),
            )
            if task["status"] == "paused" and processed > processed_before:
                return True
            await asyncio.sleep(0.025)
        return False

    async def _drain_worker_events(self) -> None:
        for task_id, handle in list(self._workers.items()):
            batch: list[tuple[str, dict[str, Any]]] = []
            for _ in range(MAX_WORKER_EVENTS_PER_CYCLE):
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
                compacted = compact_worker_event_batch(batch)
                await asyncio.to_thread(self.store.append_events, task_id, compacted)

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
                    {
                        "error": f"回测Worker异常退出，exit_code={handle.process.exitcode}"
                    },
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
                str(self.settings.daa_root),
                str(self.settings.pxydata_data_root),
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
        job_dir = self.settings.jobs_dir / task_id
        warm_worker.job_queue.put_nowait(
            (task_id, pending["request"], str(result_path), str(job_dir))
        )
        self._workers[task_id] = WorkerHandle(
            user_id=pending["user_id"],
            process=warm_worker.process,
            event_queue=warm_worker.event_queue,
            command_queue=warm_worker.command_queue,
        )
        self._warm_worker = None
