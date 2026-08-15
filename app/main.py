from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import Depends, FastAPI, HTTPException, Query, status

from .auth import TrustedIdentity, build_identity_dependency
from .config import Settings
from .daa_client import DaaAdapterClient, DaaCapabilitiesClient, DaaCapabilitiesError
from .manager import QueueLimitError, TaskManager
from .microstructure import (
    MICROSTRUCTURE_STRATEGY_HASH,
    MICROSTRUCTURE_STRATEGY_ID,
    microstructure_runtime_available,
)
from .models import (
    DAA_ENGINE_TYPES,
    DataSnapshotRefV2,
    SetSpeedRequest,
    SubmitBacktestRequest,
    SubmitBacktestRequestV2,
)
from .pxydata_client import (
    PxyDataSnapshotClient,
    SnapshotClient,
    SnapshotProviderError,
)
from .store import TaskNotFoundError

A_SHARE_WARMUP_CALENDAR_DAYS = 120
MANIFEST_ENGINE_TYPES = {*DAA_ENGINE_TYPES, "microstructure"}


async def _verify_factor_set_binding(
    client: SnapshotClient,
    body: SubmitBacktestRequestV2,
) -> dict[str, object] | None:
    """真实 PXYDATA provider 必须返回 active factor_set；测试替身可省略该能力。"""
    if body.engine_type not in {"factor_matrix", "event_sentiment"}:
        return None
    factor_set_id = str(body.parameters.get("factor_set_id") or "").strip()
    getter = getattr(client, "get_factor_set", None)
    if getter is None:
        if isinstance(client, PxyDataSnapshotClient):
            raise SnapshotProviderError("PXYDATA provider 缺少 factor_set 注册接口", status_code=503)
        return None
    return await getter(factor_set_id, None)


def _bind_factor_set_to_manifest(
    body: SubmitBacktestRequestV2,
    manifest: dict | None,
    factor_set: dict[str, object] | None,
) -> dict[str, object]:
    if factor_set is None:
        return {}
    derivation = manifest.get("derivation") if isinstance(manifest, dict) else None
    if not isinstance(derivation, dict):
        raise SnapshotProviderError("执行快照缺少 factor_set derivation", status_code=409)
    expected = {
        "factor_set_id": str(factor_set["factor_set_id"]),
        "factor_set_hash": str(factor_set["factor_set_hash"]),
        "feature_code_hash": str(factor_set["feature_code_hash"]),
    }
    for key, value in expected.items():
        if str(derivation.get(key) or "") != value:
            raise SnapshotProviderError(f"执行快照 {key} 与 factor_set 不一致", status_code=409)
    return {
        **expected,
        "factor_set_version": int(factor_set["version"]),
    }


def _engine_strategies(catalog: dict, engine_type: str) -> list[dict]:
    if engine_type not in DAA_ENGINE_TYPES:
        return []
    return [
        item
        for item in catalog.get("strategies") or []
        if isinstance(item, dict)
        and engine_type in (item.get("engine_types") or ["a_share_portfolio"])
    ]


def _snapshot_date_range(body: SubmitBacktestRequestV2) -> tuple[str, str]:
    start = date.fromisoformat(body.period.start[:10])
    end = date.fromisoformat(body.period.end[:10])
    if body.engine_type not in DAA_ENGINE_TYPES:
        return start.isoformat(), end.isoformat()

    if body.engine_type == "a_share_portfolio":
        start -= timedelta(days=A_SHARE_WARMUP_CALENDAR_DAYS)
    mode = str(body.parameters.get("mode") or "position")
    if mode == "full" or body.engine_type in {"factor_matrix", "event_sentiment"}:
        overrides = body.parameters.get("overrides")
        max_hold_days = (
            overrides.get("max_hold_days") if isinstance(overrides, dict) else None
        )
        holding_days = int(max_hold_days or body.parameters.get("holding_days") or 5)
        end += timedelta(days=(max(holding_days, 1) + 5) * 2)
    return start.isoformat(), end.isoformat()


def create_app(
    settings: Settings | None = None,
    manager: TaskManager | None = None,
    snapshot_client: SnapshotClient | None = None,
    daa_client: DaaCapabilitiesClient | None = None,
) -> FastAPI:
    configured = settings or Settings.from_env()
    configured.ensure_directories()
    task_manager = manager or TaskManager(configured)
    identity_dependency = build_identity_dependency(configured)
    data_snapshots = snapshot_client or PxyDataSnapshotClient.from_settings(configured)
    daa_adapter = daa_client or DaaAdapterClient(configured)
    microstructure_available = (
        configured.pxydata_data_root.is_dir() and microstructure_runtime_available()
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await task_manager.start()
        try:
            yield
        finally:
            await task_manager.stop()

    app = FastAPI(title="PXYBACKTEST", version="0.2.0", lifespan=lifespan)
    app.state.settings = configured
    app.state.manager = task_manager
    app.state.snapshot_client = data_snapshots

    @app.get("/health")
    async def health() -> dict:
        return {
            "ok": True,
            "service": "pxy-backtest",
            "executionNode": "app-win-01",
            "serviceTokenConfigured": bool(configured.service_token),
            "maxConcurrentTasks": configured.max_concurrent_tasks,
            "computeLocation": "workstation-only",
            "pxydataSnapshotConfigured": data_snapshots.configured,
        }

    @app.get("/api/v2/capabilities")
    async def capabilities(
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        a_share_catalog: dict = {}
        if daa_adapter.configured:
            try:
                a_share_catalog = await daa_adapter.get_capabilities()
            except DaaCapabilitiesError:
                a_share_catalog = {}
        return {
            "task_contract": "pxybacktest.task-result.v2",
            "data_contract": "pxydata.backtest-data-snapshot.v1",
            "optimization": {
                "methods": ["optuna", "walk_forward"],
                "multi_objective": True,
                "max_objectives": 3,
                "snapshot_bound": True,
            },
            "engines": [
                {
                    "id": "vnpy_cta",
                    "available": True,
                    "data_source_policy": "pxydata_preferred_with_runtime_fallback",
                    "snapshot_enforcement": "provenance_only",
                },
                {
                    "id": "a_share_portfolio",
                    "available": bool(a_share_catalog),
                    "adapter_contract": "pxybacktest.engine-adapter.a-share.v1",
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "price_adjustment": "none",
                    "worker_version": a_share_catalog.get("worker_version"),
                    "strategies": _engine_strategies(
                        a_share_catalog,
                        "a_share_portfolio",
                    ),
                },
                {
                    "id": "factor_matrix",
                    "available": bool(
                        _engine_strategies(a_share_catalog, "factor_matrix")
                    ),
                    "adapter_contract": "pxybacktest.engine-adapter.a-share.v1",
                    "factor_contract": "pxydata.factor_matrix_daily.v1",
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "strategies": _engine_strategies(
                        a_share_catalog,
                        "factor_matrix",
                    ),
                },
                {
                    "id": "event_sentiment",
                    "available": bool(
                        _engine_strategies(a_share_catalog, "event_sentiment")
                    ),
                    "adapter_contract": "pxybacktest.engine-adapter.a-share.v1",
                    "factor_contract": "pxydata.factor_matrix_daily.v1",
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "strategies": _engine_strategies(
                        a_share_catalog,
                        "event_sentiment",
                    ),
                },
                {
                    "id": "microstructure",
                    "available": microstructure_available,
                    "intervals": ["tick"],
                    "snapshot_enforcement": "manifest_bound",
                    "tick_contract": "pxydata.market_ticks.v1",
                    "strategies": [
                        {
                            "id": MICROSTRUCTURE_STRATEGY_ID,
                            "name": "一档盘口不平衡",
                            "version": "builtin-v1",
                            "source_hash": MICROSTRUCTURE_STRATEGY_HASH,
                            "entrypoint": MICROSTRUCTURE_STRATEGY_ID,
                            "parameters": [
                                {"id": "entry_threshold", "default": 0.2},
                                {"id": "exit_threshold", "default": 0.0},
                                {"id": "latency_ticks", "default": 1},
                                {"id": "max_hold_ticks", "default": 100},
                                {"id": "quantity", "default": 1},
                            ],
                        }
                    ],
                },
                {"id": "mt5_native", "available": False},
            ],
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

    @app.post("/api/v2/tasks", status_code=status.HTTP_202_ACCEPTED)
    async def submit_task_v2(
        body: SubmitBacktestRequestV2,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            body.validate_contract()
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        a_share_catalog: dict = {}
        if body.engine_type in DAA_ENGINE_TYPES and daa_adapter.configured:
            try:
                a_share_catalog = await daa_adapter.get_capabilities()
            except DaaCapabilitiesError:
                a_share_catalog = {}
        engine_strategies = _engine_strategies(a_share_catalog, body.engine_type)
        supported_engine = (
            body.engine_type == "vnpy_cta"
            or bool(engine_strategies)
            or (body.engine_type == "microstructure" and microstructure_available)
        )
        if not supported_engine:
            raise HTTPException(
                status_code=501,
                detail=f"回测引擎尚未安装: {body.engine_type}",
            )
        if body.engine_type in DAA_ENGINE_TYPES:
            selected_strategy = next(
                (
                    item
                    for item in engine_strategies
                    if item.get("id") == body.strategy.id
                ),
                None,
            )
            if selected_strategy is None:
                raise HTTPException(
                    status_code=422,
                    detail=f"DAA {body.engine_type} 策略不存在",
                )
            if (
                selected_strategy.get("source_hash")
                != body.strategy.source_hash.lower()
            ):
                raise HTTPException(
                    status_code=409, detail="DAA A 股策略源码版本已变化"
                )

        if body.engine_type == "microstructure" and (
            body.strategy.id != MICROSTRUCTURE_STRATEGY_ID
            or body.strategy.source_hash.lower() != MICROSTRUCTURE_STRATEGY_HASH
        ):
            raise HTTPException(status_code=409, detail="microstructure 策略版本不一致")

        try:
            start_date, end_date = _snapshot_date_range(body)
            factor_set = await _verify_factor_set_binding(data_snapshots, body)
            snapshot: DataSnapshotRefV2
            snapshot_manifest: dict | None = None
            factor_input_snapshot: DataSnapshotRefV2 | None = None
            if body.data.selection is not None:
                if body.engine_type in {"factor_matrix", "event_sentiment"}:
                    factor_input_snapshot, snapshot = (
                        await data_snapshots.create_factor_bundle(
                            selection=body.data.selection,
                            start_date=start_date,
                            end_date=end_date,
                            symbols=body.universe.symbols,
                            factor_set_id=str(body.parameters["factor_set_id"]),
                        )
                    )
                else:
                    snapshot = await data_snapshots.create_snapshot(
                        selection=body.data.selection,
                        start_date=start_date,
                        end_date=end_date,
                        symbols=body.universe.symbols,
                    )
                if body.engine_type in MANIFEST_ENGINE_TYPES:
                    snapshot, snapshot_manifest = await data_snapshots.resolve_snapshot(
                        snapshot
                    )
            else:
                assert body.data.snapshot is not None
                if body.engine_type in MANIFEST_ENGINE_TYPES:
                    snapshot, snapshot_manifest = await data_snapshots.resolve_snapshot(
                        body.data.snapshot
                    )
                else:
                    snapshot = await data_snapshots.verify_snapshot(body.data.snapshot)
            factor_updates = _bind_factor_set_to_manifest(body, snapshot_manifest, factor_set)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except SnapshotProviderError as exc:
            provider_status = (
                exc.status_code
                if 400 <= exc.status_code < 500 or exc.status_code == 503
                else 502
            )
            raise HTTPException(status_code=provider_status, detail=str(exc)) from exc

        parameter_updates = (
            {"factor_input_snapshot_id": factor_input_snapshot.snapshot_id}
            if factor_input_snapshot is not None
            else None
        )
        if factor_set is not None:
            parameter_updates = {**(parameter_updates or {}), **factor_updates}
        resolved = body.with_snapshot(snapshot, parameter_updates=parameter_updates)
        try:
            task_id = await task_manager.submit(
                user_id=identity.user_id,
                source_node=identity.source_node,
                request=resolved.to_worker_request(snapshot_manifest=snapshot_manifest),
            )
        except QueueLimitError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        return {
            "success": True,
            "task_id": task_id,
            "schema_version": 2,
            "contract_version": "pxybacktest.task-result.v2",
            "engine_type": body.engine_type,
            "data_snapshot": snapshot.model_dump(mode="json"),
            "input_snapshot": (
                factor_input_snapshot.model_dump(mode="json")
                if factor_input_snapshot is not None
                else None
            ),
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
            return await asyncio.to_thread(
                task_manager.result, identity.user_id, task_id
            )
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
        return {
            "success": success,
            "message": "回测已暂停" if success else "只能暂停运行中的任务",
        }

    @app.post("/api/v1/tasks/{task_id}/resume")
    async def resume_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.resume(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {
            "success": success,
            "message": "回测继续" if success else "只能继续暂停的任务",
        }

    @app.post("/api/v1/tasks/{task_id}/speed")
    async def speed_task(
        task_id: str,
        body: SetSpeedRequest,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.set_speed(
                identity.user_id, task_id, body.speed
            )
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {
            "success": success,
            "message": f"速度已设置为 {body.speed}x" if success else "任务当前不能调速",
        }

    @app.delete("/api/v1/tasks/{task_id}")
    async def cancel_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            success = await task_manager.cancel(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {
            "success": success,
            "message": "回测已取消" if success else "任务已经结束",
        }

    return app


app = create_app()
