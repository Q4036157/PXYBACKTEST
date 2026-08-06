from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Query, status

from .auth import TrustedIdentity, build_identity_dependency
from .config import Settings
from .manager import QueueLimitError, TaskManager
from .models import SetSpeedRequest, SubmitBacktestRequest
from .store import TaskNotFoundError


def create_app(
    settings: Settings | None = None, manager: TaskManager | None = None
) -> FastAPI:
    configured = settings or Settings.from_env()
    configured.ensure_directories()
    task_manager = manager or TaskManager(configured)
    identity_dependency = build_identity_dependency(configured)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await task_manager.start()
        try:
            yield
        finally:
            await task_manager.stop()

    app = FastAPI(title="PXYBACKTEST", version="0.1.0", lifespan=lifespan)
    app.state.settings = configured
    app.state.manager = task_manager

    @app.get("/health")
    async def health() -> dict:
        return {
            "ok": True,
            "service": "pxy-backtest",
            "executionNode": "app-win-01",
            "serviceTokenConfigured": bool(configured.service_token),
            "maxConcurrentTasks": configured.max_concurrent_tasks,
            "computeLocation": "workstation-only",
        }

    @app.post("/api/v1/tasks", status_code=status.HTTP_202_ACCEPTED)
    async def submit_task(
        body: SubmitBacktestRequest,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            task_id = await task_manager.submit(
                user_id=identity.user_id,
                source_node=identity.source_node,
                request=body.model_dump(),
            )
        except QueueLimitError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        return {
            "success": True,
            "task_id": task_id,
            "message": "回测任务已提交到工作站",
            "execution_backend": "workstation",
            "event_stream": "delta-poll",
        }

    @app.get("/api/v1/tasks")
    async def list_tasks(
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        tasks = await asyncio.to_thread(task_manager.store.list_tasks, identity.user_id)
        return {"success": True, "tasks": tasks, "total": len(tasks)}

    @app.get("/api/v1/tasks/{task_id}")
    async def get_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            return await asyncio.to_thread(task_manager.result, identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc

    @app.get("/api/v1/tasks/{task_id}/events")
    async def get_events(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
        after_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=500),
    ) -> dict:
        def load_event_page() -> tuple[dict, list[dict], dict]:
            task_status = task_manager.store.get_task_status(identity.user_id, task_id)
            page_events = task_manager.store.events_after(
                identity.user_id, task_id, after_seq, limit
            )
            context = task_manager.store.queue_context(identity.user_id, task_id)
            return task_status, page_events, context

        try:
            task, events, queue_context = await asyncio.to_thread(load_event_page)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        next_seq = events[-1]["seq"] if events else after_seq
        return {
            "success": True,
            "task_id": task_id,
            "status": task["status"],
            "error": task.get("error", ""),
            "events": events,
            "next_seq": next_seq,
            **queue_context,
        }

    @app.post("/api/v1/tasks/{task_id}/pause")
    async def pause_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.pause(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {"success": success, "message": "回测已暂停" if success else "只能暂停运行中的任务"}

    @app.post("/api/v1/tasks/{task_id}/resume")
    async def resume_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.resume(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {"success": success, "message": "回测继续" if success else "只能继续暂停的任务"}

    @app.post("/api/v1/tasks/{task_id}/speed")
    async def speed_task(
        task_id: str,
        body: SetSpeedRequest,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.set_speed(identity.user_id, task_id, body.speed)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {"success": success, "message": f"速度已设置为 {body.speed}x" if success else "任务当前不能调速"}

    @app.delete("/api/v1/tasks/{task_id}")
    async def cancel_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.cancel(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {"success": success, "message": "回测已取消" if success else "任务已经结束"}

    return app


app = create_app()
