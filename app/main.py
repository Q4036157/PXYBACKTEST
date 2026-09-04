from __future__ import annotations

import asyncio
import hashlib
import io
import json
import time
import zipfile
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse, Response

from .auth import TrustedIdentity, build_identity_dependency
from .config import Settings
from .daa_client import (
    AI_CAPABLE_ENGINE_TYPES,
    DaaAdapterClient,
    DaaCapabilitiesClient,
    DaaCapabilitiesError,
)
from .default_profiles import (
    DEFAULT_PROFILE_CONTRACT,
    default_profile_catalog,
    engine_catalog_metadata,
    profile_ids_for_engine,
)
from .manager import QueueLimitError, TaskManager
from .microstructure import (
    MICROSTRUCTURE_STRATEGY_HASH,
    MICROSTRUCTURE_STRATEGY_ID,
    microstructure_runtime_available,
)
from .a_share_emotion_etf import (
    EMOTION_DATA_CONTRACT,
    EMOTION_ETF_STRATEGY_HASH,
    EMOTION_ETF_STRATEGY_ID,
    runtime_available as emotion_etf_runtime_available,
)
from .learning import (
    ML_ENGINE_TYPES,
    ML_STRATEGY_HASH,
    learning_runtime_available,
    learning_runtime_capabilities,
)
from .lighter_microstructure import (
    LIGHTER_STRATEGY_HASH,
    LIGHTER_STRATEGY_ID,
    lighter_runtime_available,
)
from .llm_signal import LLMRealtimeSignalRequest, LLMSignalError, generate_realtime_signal
from .custom_nodes import CustomDataNodeRunRequest, CustomDataNodeSpec, CustomNodeError, run_custom_data_node, validate_custom_data_node
from .models import (
    DAA_ENGINE_TYPES,
    DataSnapshotRefV2,
    SetSpeedRequest,
    SubmitBacktestRequest,
    SubmitBacktestRequestV2,
)
from .workflow import WorkflowSpec, validate_workflow
from .pxydata_client import (
    DataRequirementManifestV1,
    PxyDataSnapshotClient,
    SnapshotClient,
    SnapshotProviderError,
)
from .runner_registry import (
    RunnerProbeConfig,
    build_runner_registry,
    runner_contract_capabilities,
)
from .store import (
    IdempotencyConflictError,
    RESULT_RETENTION_SECONDS,
    TaskCreationReceipt,
    TaskNotFoundError,
)
from .strategy_package import StrategyPackage
from .tqsdk_submission import TqSdkTaskSubmission

A_SHARE_WARMUP_CALENDAR_DAYS = 120
LIGHTER_ENGINE_TYPES = {"lighter_microstructure"}
MANIFEST_ENGINE_TYPES = {
    *DAA_ENGINE_TYPES, "a_share_emotion_etf", "microstructure", *ML_ENGINE_TYPES, *LIGHTER_ENGINE_TYPES
}
ENGINE_REQUIRED_DATASETS: dict[str, list[str]] = {
    "vnpy_cta": [],
    "a_share_portfolio": ["kline_daily"],
    "a_share_emotion_etf": ["etf_snapshots", "market_emotion_daily"],
    "factor_matrix": ["kline_daily", "factor_matrix_daily"],
    "event_sentiment": ["kline_daily", "factor_matrix_daily", "events"],
    "microstructure": ["market_ticks"],
    "ml_factor": ["factor_matrix_daily"],
    "deep_learning": ["factor_matrix_daily"],
    "lighter_microstructure": [
        "lighter_microstructure_factors",
        "lighter_order_book_events",
        "lighter_funding_history",
    ],
    "mt5_native": [],
}


def _submission_receipt(value: str | TaskCreationReceipt) -> TaskCreationReceipt:
    if isinstance(value, TaskCreationReceipt):
        return value
    return TaskCreationReceipt(
        task_id=str(value),
        idempotent_replay=False,
        idempotency_key=None,
    )


async def _submit_with_idempotency(
    task_manager: TaskManager,
    *,
    user_id: str,
    source_node: str,
    request: dict,
    idempotency_key: str | None,
    initial_status: str = "pending",
    idempotency_payload: dict | None = None,
) -> TaskCreationReceipt:
    kwargs = {
        "user_id": user_id,
        "source_node": source_node,
        "request": request,
    }
    if idempotency_key is not None:
        kwargs["idempotency_key"] = idempotency_key
    if initial_status != "pending":
        kwargs["initial_status"] = initial_status
    if idempotency_payload is not None:
        kwargs["idempotency_payload"] = idempotency_payload
    return _submission_receipt(await task_manager.submit(**kwargs))


def _data_requirement_payload(
    body: SubmitBacktestRequestV2, task_id: str
) -> dict[str, object]:
    assert body.data.selection is not None
    canonical = body.model_dump(mode="json")
    request_fingerprint = hashlib.sha256(
        json.dumps(
            canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    requirement_hash = hashlib.sha256(
        f"{task_id}\0{request_fingerprint}".encode("utf-8")
    ).hexdigest()[:32]
    start_date, end_date = _snapshot_date_range(body)
    data_fields = body.parameters.get("data_fields")
    field_map = data_fields if isinstance(data_fields, dict) else {}
    market = (
        "cn_equity"
        if body.engine_type in DAA_ENGINE_TYPES | {"ml_factor", "deep_learning"}
        else "lighter"
        if body.engine_type == "lighter_microstructure"
        else "global"
    )
    pit_datasets = {
        "financials_pit",
        "events",
        "factor_matrix_daily",
        "ml_features_daily",
    }
    datasets = [
        {
            "name": name,
            "fields": sorted(
                {
                    str(value).strip()
                    for value in (
                        field_map.get(name, [])
                        if isinstance(field_map.get(name, []), list)
                        else []
                    )
                    if str(value).strip()
                }
            ),
            "symbols": body.universe.symbols,
            "start": start_date,
            "end": end_date,
            "frequency": body.period.interval,
            "market": market,
            "pit_required": name in pit_datasets,
        }
        for name in body.data.selection.datasets
    ]
    return {
        "requirement_id": f"datareq_v1_{requirement_hash}",
        "contract_version": "pxydata.data-requirement.v1",
        "consumer_task_id": task_id,
        "request_fingerprint": request_fingerprint,
        "datasets": datasets,
        "quality_policy": body.data.selection.quality_policy,
        "snapshot_kind": (
            "factor_bundle"
            if body.engine_type in {"factor_matrix", "event_sentiment"}
            else "snapshot"
        ),
        "factor_set_id": (
            str(body.parameters.get("factor_set_id") or "")
            if body.engine_type in {"factor_matrix", "event_sentiment"}
            else None
        ),
    }


def _waiting_worker_request(body: SubmitBacktestRequestV2) -> dict[str, object]:
    return {
        "speed": body.execution.speed,
        "execution_mode": body.execution.execution_mode,
        "_task_contract": body.model_dump(mode="json"),
    }


def _validate_waiting_snapshot_manifest(
    body: SubmitBacktestRequestV2,
    requirement: DataRequirementManifestV1,
    snapshot: DataSnapshotRefV2,
    manifest: dict,
) -> None:
    if body.data.selection is None:
        raise SnapshotProviderError("等待任务缺少原始数据选择", status_code=409)
    selection = manifest.get("selection")
    if not isinstance(selection, dict):
        raise SnapshotProviderError("就绪快照缺少选择条件", status_code=409)
    start_date, end_date = _snapshot_date_range(body)
    if str(selection.get("start_date") or "") != start_date:
        raise SnapshotProviderError("就绪快照开始日期与需求不一致", status_code=409)
    if str(selection.get("end_date") or "") != end_date:
        raise SnapshotProviderError("就绪快照结束日期与需求不一致", status_code=409)
    expected_symbols = {symbol.upper() for symbol in body.universe.symbols}
    actual_symbols = {
        str(symbol).upper() for symbol in selection.get("symbols") or []
    }
    if actual_symbols != expected_symbols:
        raise SnapshotProviderError("就绪快照标的范围与需求不一致", status_code=409)
    if str(selection.get("decision_time") or "") != body.data.selection.decision_time:
        raise SnapshotProviderError("就绪快照决策时点与需求不一致", status_code=409)
    expected_datasets = (
        {"kline_daily", "factor_matrix_daily"}
        if requirement.snapshot_kind == "factor_bundle"
        else set(body.data.selection.datasets)
    )
    actual_datasets = {
        str(item.get("name") or "")
        for item in manifest.get("datasets") or []
        if isinstance(item, dict)
    }
    if actual_datasets != expected_datasets:
        raise SnapshotProviderError("就绪快照数据集与需求不一致", status_code=409)
    if snapshot.quality_policy != body.data.selection.quality_policy:
        raise SnapshotProviderError("就绪快照质量策略与需求不一致", status_code=409)
    if requirement.snapshot_kind == "factor_bundle":
        derivation = manifest.get("derivation")
        if not isinstance(derivation, dict):
            raise SnapshotProviderError("因子执行快照缺少派生身份", status_code=409)
        if str(derivation.get("factor_set_id") or "") != str(
            requirement.factor_set_id or ""
        ):
            raise SnapshotProviderError("因子执行快照集合与需求不一致", status_code=409)
        if not str(derivation.get("input_snapshot_id") or "").startswith(
            "btsnap_v1_"
        ):
            raise SnapshotProviderError("因子执行快照缺少输入快照身份", status_code=409)
    if requirement.request_fingerprint != str(
        _data_requirement_payload(body, requirement.consumer_task_id)[
            "request_fingerprint"
        ]
    ):
        raise SnapshotProviderError("就绪快照原始请求指纹不一致", status_code=409)


def _runner_state(catalog: list[dict], runner_id: str) -> dict:
    return next(
        (item for item in catalog if item.get("runner_id") == runner_id),
        {},
    )


def _apply_engine_readiness(
    engines: list[dict],
    *,
    runner_catalog: list[dict],
    quality_payload: dict | None,
    quality_error: str | None,
    quality_enforced: bool,
) -> list[dict]:
    report = (
        quality_payload.get("report")
        if isinstance(quality_payload, dict)
        and isinstance(quality_payload.get("report"), dict)
        else {}
    )
    datasets = report.get("datasets") if isinstance(report.get("datasets"), dict) else {}
    certification_ready = bool(
        report.get("certification_available")
        and report.get("scan_complete")
        and not report.get("stale")
    )
    report_id = report.get("report_id")
    checked_at = report.get("generated_at_utc")

    for engine in engines:
        engine_id = str(engine.get("id") or "")
        required = list(ENGINE_REQUIRED_DATASETS.get(engine_id, []))
        runtime_available = bool(engine.get("available"))
        blockers: list[str] = []

        if engine_id in {"vnpy_cta", "mt5_native"}:
            runner = _runner_state(runner_catalog, engine_id)
            if runner:
                runtime_available = bool(runner.get("runtime_detected"))
                if not runner.get("submit_ready"):
                    blockers.append(
                        str(runner.get("reason") or "平台运行器尚未通过提交门禁")
                    )
        elif not runtime_available:
            blockers.append("引擎运行时或策略适配器不可用")

        if required and quality_enforced:
            if quality_error:
                blockers.append(f"PXYDATA 质量认证不可用: {quality_error}")
            elif not certification_ready:
                blockers.append(
                    str(report.get("reason") or "PXYDATA 全量质量认证缺失或已过期")
                )
            for name in required:
                item = datasets.get(name) if isinstance(datasets, dict) else None
                if not isinstance(item, dict):
                    blockers.append(f"缺少数据集质量结果: {name}")
                    continue
                grade = str(item.get("quality_grade") or "FAIL").upper()
                status = str(item.get("status") or "unknown")
                if grade == "FAIL" or status in {"empty", "blocked"}:
                    blockers.append(f"数据集 {name} 未通过认证（{status}/{grade}）")

        submit_ready = runtime_available and not blockers
        engine["runtime_available"] = runtime_available
        engine["available"] = submit_ready
        engine["submit_ready"] = submit_ready
        engine["blockers"] = list(dict.fromkeys(blockers))
        engine["required_datasets"] = required
        engine["quality_gate_enforced"] = quality_enforced and bool(required)
        engine["quality_report_id"] = report_id
        engine["quality_checked_at"] = checked_at
    return engines


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
    if engine_type not in AI_CAPABLE_ENGINE_TYPES:
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


def _workflow_editor_html() -> str:
    """轻量 BeeAgent 风格 JSON 工作流编辑器；完整画布仍由上层前端负责。"""
    return """<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><title>PXYBACKTEST Workflow Editor</title>
<style>body{font:14px system-ui;background:#171717;color:#eee;margin:24px}textarea{width:100%;height:420px;background:#222;color:#eee;border:1px solid #555;padding:12px;font-family:monospace}button{padding:9px 16px;margin:8px 8px 8px 0;background:#d6a83b;border:0;border-radius:4px;cursor:pointer}pre{white-space:pre-wrap;background:#222;padding:12px}</style>
<h2>PXYBACKTEST 工作流编辑器</h2>
<p>这是后端自带的轻量 JSON 编辑器，保存/运行仍由受控 API 完成。</p>
<textarea id="graph">{
  "schema_version": 1,
  "workflow_id": "demo",
  "name": "研究工作流",
  "mode": "research",
  "nodes": [
    {"id":"source","type":"data_source","depends_on":[]},
    {"id":"features","type":"feature_engineering","depends_on":["source"]},
    {"id":"backtest","type":"backtest","depends_on":["features"]}
  ]
}</textarea>
<button onclick="validateGraph()">验证工作流</button><button onclick="addNode('custom_data')">添加自定义节点</button><button onclick="addNode('llm_signal')">添加 LLM 信号节点</button>
<pre id="result">尚未验证</pre>
<script>
function value(){return JSON.parse(document.getElementById('graph').value)}
function addNode(type){const x=value();const id=type+'_'+Date.now();const last=x.nodes[x.nodes.length-1].id;x.nodes.push({id,type,depends_on:[last],config:{}});document.getElementById('graph').value=JSON.stringify(x,null,2)}
async function validateGraph(){try{const r=await fetch('/api/v2/workflows/validate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(value())});document.getElementById('result').textContent=await r.text()}catch(e){document.getElementById('result').textContent=e}}
</script></html>"""


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
    emotion_etf_available = (
        configured.pxydata_data_root.is_dir() and emotion_etf_runtime_available()
    )
    learning_available = (
        configured.pxydata_data_root.is_dir() and learning_runtime_available()
    )
    lighter_available = (
        configured.pxydata_data_root.is_dir() and lighter_runtime_available()
    )
    runner_registry = build_runner_registry(
        RunnerProbeConfig.from_environment(pxylh_root=configured.pxylh_root)
    )

    async def resolve_waiting_task(
        request: dict[str, object], requirement: DataRequirementManifestV1
    ) -> tuple[dict[str, object], dict[str, object]]:
        if requirement.snapshot is None:
            raise SnapshotProviderError("已就绪的数据需求缺少快照", status_code=502)
        contract = request.get("_task_contract")
        if not isinstance(contract, dict):
            raise SnapshotProviderError("等待任务缺少 Task v2 契约", status_code=409)
        body = SubmitBacktestRequestV2.model_validate(contract)
        snapshot, full_manifest = await data_snapshots.resolve_snapshot(
            requirement.snapshot
        )
        _validate_waiting_snapshot_manifest(
            body, requirement, snapshot, full_manifest
        )
        snapshot_manifest = (
            full_manifest if body.engine_type in MANIFEST_ENGINE_TYPES else None
        )
        factor_set = await _verify_factor_set_binding(data_snapshots, body)
        factor_updates = _bind_factor_set_to_manifest(
            body, snapshot_manifest, factor_set
        )
        parameter_updates: dict[str, object] = dict(factor_updates)
        if body.engine_type in {"factor_matrix", "event_sentiment"}:
            derivation = (
                snapshot_manifest.get("derivation")
                if isinstance(snapshot_manifest, dict)
                else None
            )
            input_snapshot_id = (
                str(derivation.get("input_snapshot_id") or "")
                if isinstance(derivation, dict)
                else ""
            )
            if not input_snapshot_id.startswith("btsnap_v1_"):
                raise SnapshotProviderError(
                    "执行快照缺少因子输入快照身份", status_code=409
                )
            parameter_updates["factor_input_snapshot_id"] = input_snapshot_id
        resolved = body.with_snapshot(snapshot, parameter_updates=parameter_updates)
        return (
            resolved.to_worker_request(snapshot_manifest=snapshot_manifest),
            snapshot.model_dump(mode="json"),
        )

    if isinstance(task_manager, TaskManager):
        def rebuild_data_requirement(
            request: dict[str, object], task_id: str
        ) -> dict[str, object]:
            contract = request.get("_task_contract")
            if not isinstance(contract, dict):
                raise ValueError("等待任务缺少 Task v2 契约")
            return _data_requirement_payload(
                SubmitBacktestRequestV2.model_validate(contract), task_id
            )

        task_manager.configure_data_waiting(
            data_snapshots, resolve_waiting_task, rebuild_data_requirement
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
        runner_catalog = runner_registry.catalog()
        quality_payload: dict | None = None
        quality_error: str | None = None
        quality_reader = getattr(data_snapshots, "get_data_quality", None)
        quality_enforced = callable(quality_reader)
        if quality_enforced:
            if data_snapshots.configured:
                required = sorted(
                    {
                        dataset
                        for datasets in ENGINE_REQUIRED_DATASETS.values()
                        for dataset in datasets
                    }
                )
                try:
                    quality_payload = await asyncio.wait_for(
                        quality_reader(required), timeout=5.0
                    )
                except (SnapshotProviderError, TimeoutError) as exc:
                    quality_error = str(exc) or type(exc).__name__
            else:
                quality_error = "PXYDATA 服务地址或 API Key 未配置"
        payload = {
            "task_contract": "pxybacktest.task-result.v2",
            "data_contract": "pxydata.backtest-data-snapshot.v1",
            "result_retention": {
                "default_days": RESULT_RETENTION_SECONDS // 86_400,
                "starts_at": "task_terminal",
            },
            "default_profile_contract": DEFAULT_PROFILE_CONTRACT,
            "default_profiles": default_profile_catalog(),
            "strategy_runtime_contracts": runner_contract_capabilities(),
            "runners": runner_catalog,
            "replay": {
                "contract": "pxybacktest.replay.v1",
                "execution_stream": "complete_ordered_audited",
                "visual_projection": {
                    "mode": "framed_incremental_state",
                    "default_frame_interval_ms": 33,
                    "min_frame_interval_ms": 8,
                    "speed_scaled": True,
                    "preserves_execution_events": True,
                },
                "modes": ["real_tick", "bar", "pseudo_tick"],
                "availability_time_enforced": True,
            },
            "optimization": {
                "methods": ["optuna", "walk_forward"],
                "multi_objective": True,
                "max_objectives": 3,
                "snapshot_bound": True,
            },
            "workflow": {
                "contract": "pxybacktest.workflow.v1",
                "node_types": [
                    "data_source", "feature_engineering", "model_training",
                    "model_ensemble", "custom_data", "portfolio", "risk", "backtest",
                    "report", "live_signal", "llm_signal",
                ],
                "execution_boundary": "signal_only_no_order_submission",
            },
            "learning": learning_runtime_capabilities(),
            "llm": {
                "enabled": bool(configured.llm_base_url and configured.llm_api_key),
                "modes": ["paper", "live_signal"],
                "historical_backtest": False,
                "execution_boundary": "signal_only_no_order_submission",
            },
            "custom_nodes": {
                "enabled": configured.custom_nodes_root.is_dir(),
                "contract": "pxybacktest.custom-data-node.v1",
                "trusted_local_code_only": True,
            },
            "engines": [
                {
                    "id": "vnpy_cta",
                    "available": True,
                    "data_source_policy": "pxydata_preferred_with_runtime_fallback",
                    "snapshot_enforcement": "provenance_only",
                    "replay_modes": ["bar", "pseudo_tick"],
                    "event_domains": ["market_bar", "market_tick", "order", "fill", "position", "account"],
                    "execution_stream": "complete_ordered_audited",
                    "strategies": _engine_strategies(a_share_catalog, "vnpy_cta"),
                },
                {
                    "id": "a_share_portfolio",
                    "available": bool(a_share_catalog),
                    "adapter_contract": "pxybacktest.engine-adapter.a-share.v1",
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "replay_modes": ["bar"],
                    "event_domains": ["market_bar", "fundamental", "factor", "order", "fill", "position", "account"],
                    "execution_stream": "complete_ordered_audited",
                    "price_adjustment": "none",
                    "worker_version": a_share_catalog.get("worker_version"),
                    "strategies": _engine_strategies(
                        a_share_catalog,
                        "a_share_portfolio",
                    ),
                },
                {
                    "id": "a_share_emotion_etf",
                    "available": emotion_etf_available,
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "replay_modes": ["bar"],
                    "event_domains": ["market_bar", "market_emotion", "signal", "order", "fill", "position", "account"],
                    "execution_stream": "complete_ordered_audited",
                    "data_contracts": ["pxydata.etf_snapshots.v1", EMOTION_DATA_CONTRACT],
                    "strategies": [{
                        "id": EMOTION_ETF_STRATEGY_ID,
                        "name": "ETF情绪极值C（冰点买、过热卖）",
                        "version": "builtin-v1",
                        "source_hash": EMOTION_ETF_STRATEGY_HASH,
                        "entrypoint": EMOTION_ETF_STRATEGY_ID,
                        "parameters": [
                            {"id": "entry_threshold", "default": 30, "locked": True},
                            {"id": "exit_threshold", "default": 80, "locked": True},
                            {"id": "lot_size", "default": 100, "locked": True},
                            {"id": "min_commission", "default": 5.0},
                        ],
                    }],
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
                    "replay_modes": ["bar"],
                    "event_domains": ["market_bar", "factor", "order", "fill", "position", "account"],
                    "execution_stream": "complete_ordered_audited",
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
                    "replay_modes": ["bar"],
                    "event_domains": ["market_bar", "news", "sentiment", "factor", "order", "fill", "position", "account"],
                    "execution_stream": "complete_ordered_audited",
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
                    "replay_modes": ["real_tick"],
                    "event_domains": ["market_tick", "order_book", "order", "fill", "position", "account"],
                    "execution_stream": "complete_ordered_audited",
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
                    ] + _engine_strategies(a_share_catalog, "microstructure"),
                },
                {
                    "id": "ml_factor",
                    "available": learning_available,
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "replay_modes": ["bar"],
                    "event_domains": ["market_bar", "factor", "signal", "account"],
                    "execution_stream": "complete_ordered_audited",
                    "feature_contracts": [
                        "pxydata.ml_features_daily.v1",
                        "pxydata.factor_matrix_daily.v1",
                        "pxydata.lighter_microstructure_factors.v1",
                    ],
                    "models": learning_runtime_capabilities()["models"],
                    "research_adapters": ["qlib", "rd-agent"],
                    "strategies": [
                        {
                            "id": "temporal_ml_rank_v1",
                            "name": "时间序列 ML 横截面排序",
                            "version": "builtin-v1",
                            "source_hash": ML_STRATEGY_HASH,
                            "entrypoint": "temporal_ml_rank_v1",
                            "parameters": [
                                {"id": "feature_columns", "required": True},
                                {"id": "label_column", "default": "label"},
                                {"id": "task_type", "choices": ["binary", "ranking", "regression"]},
                                {"id": "seq_len", "default": 1},
                                {"id": "train_days", "default": 252},
                                {"id": "test_days", "default": 63},
                                {"id": "purge_days", "default": 5},
                                {"id": "embargo_days", "default": 1},
                            ],
                        }
                    ],
                },
                {
                    "id": "deep_learning",
                    "available": learning_available and learning_runtime_capabilities()["optional"]["torch"],
                    "intervals": ["1d"],
                    "snapshot_enforcement": "manifest_bound",
                    "replay_modes": ["bar"],
                    "event_domains": ["market_bar", "factor", "signal", "account"],
                    "execution_stream": "complete_ordered_audited",
                    "models": [item for item in learning_runtime_capabilities()["models"] if item in {"lstm", "transformer", "transformer_seq", "ensemble"}],
                    "research_adapters": ["qlib", "rd-agent"],
                    "execution_mode": "offline_train_then_signal_only",
                },
                {
                    "id": "lighter_microstructure",
                    "available": lighter_available,
                    "intervals": ["tick", "1s", "100ms"],
                    "snapshot_enforcement": "manifest_bound",
                    "replay_modes": ["real_tick"],
                    "event_domains": ["market_tick", "order_book", "factor", "order", "fill", "account"],
                    "execution_stream": "complete_ordered_audited",
                    "data_contracts": [
                        "pxydata.lighter_microstructure_factors.v1",
                        "pxydata.lighter_order_book_events.v1",
                        "pxydata.lighter_funding_history.v1",
                    ],
                    "features": [
                        "funding_rate", "active_buy_sell", "l2_depth", "ofi",
                    ],
                    "strategies": [{
                        "id": LIGHTER_STRATEGY_ID,
                        "name": "Lighter 主动成交+资金费+多档盘口",
                        "version": "builtin-v1",
                        "source_hash": LIGHTER_STRATEGY_HASH,
                        "entrypoint": LIGHTER_STRATEGY_ID,
                        "parameters": [
                            {"id": "book_depth", "default": 10},
                            {"id": "entry_threshold", "default": 0.2},
                            {"id": "max_hold_ms", "default": 3600000},
                            {"id": "fee_bps_per_side", "default": 1.0},
                            {"id": "slippage_bps_per_side", "default": 1.0},
                        ],
                    }],
                },
                {"id": "mt5_native", "available": False},
            ],
        }
        payload["engines"] = _apply_engine_readiness(
            payload["engines"],
            runner_catalog=runner_catalog,
            quality_payload=quality_payload,
            quality_error=quality_error,
            quality_enforced=quality_enforced,
        )
        for engine in payload["engines"]:
            engine_id = str(engine.get("id") or "")
            engine["engine_id"] = engine_id
            engine.update(engine_catalog_metadata(engine_id))
            engine["default_profile_ids"] = profile_ids_for_engine(engine_id)
            engine["availability"] = {
                "submit_ready": bool(engine.get("submit_ready")),
                "reason_codes": list(engine.get("blockers") or []),
            }
        payload["data_quality"] = {
            "enforced": quality_enforced,
            "available": quality_payload is not None,
            "report_id": (
                (quality_payload.get("report") or {}).get("report_id")
                if quality_payload
                else None
            ),
            "checked_at": (
                (quality_payload.get("report") or {}).get("generated_at_utc")
                if quality_payload
                else None
            ),
            "error": quality_error,
        }
        return payload

    @app.post("/api/v2/strategy-packages/validate")
    async def validate_strategy_package_route(
        body: StrategyPackage,
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        """只校验并解析策略包，不创建任务或执行第三方策略。"""

        return runner_registry.resolve(body)

    @app.post("/api/v2/tqsdk/tasks", status_code=status.HTTP_202_ACCEPTED)
    async def submit_tqsdk_task(
        body: TqSdkTaskSubmission,
        identity: TrustedIdentity = Depends(identity_dependency),
        idempotency_key: str | None = Header(
            default=None, alias="Idempotency-Key", max_length=200
        ),
    ) -> dict:
        """提交天勤原生策略；安全和三维验收未通过时保持关闭。"""

        resolution = runner_registry.resolve(body.package)
        if not resolution.get("resolved"):
            raise HTTPException(
                status_code=422,
                detail=str(resolution.get("reason") or "天勤运行器不匹配"),
            )
        if not resolution.get("submit_ready"):
            raise HTTPException(
                status_code=409,
                detail=str(
                    resolution.get("reason")
                    or "天勤逐笔成交、账户、可视化或安全门禁尚未全部通过"
                ),
            )
        try:
            receipt = await _submit_with_idempotency(
                task_manager,
                user_id=identity.user_id,
                source_node=identity.source_node,
                request=body.to_worker_request(),
                idempotency_key=idempotency_key,
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except QueueLimitError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        return {
            "success": True,
            "task_id": receipt.task_id,
            "idempotency_key": receipt.idempotency_key,
            "idempotent_replay": receipt.idempotent_replay,
            "schema_version": 2,
            "contract_version": "pxybacktest.task-result.v2",
            "engine_type": "tqsdk_native",
            "execution_backend": "windows-restricted-sandbox",
            "event_stream": "delta-poll",
            "pause_scope": "replay_only",
        }

    @app.post("/api/v2/workflows/validate")
    async def validate_workflow_route(
        body: WorkflowSpec,
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        return validate_workflow(body.model_dump(mode="python"))

    @app.post("/api/v2/signals/llm")
    async def llm_signal_route(
        body: LLMRealtimeSignalRequest,
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        if not configured.llm_base_url or not configured.llm_api_key:
            raise HTTPException(status_code=501, detail="LLM provider 尚未配置")
        try:
            return await generate_realtime_signal(
                body,
                base_url=configured.llm_base_url,
                api_key=configured.llm_api_key,
            )
        except LLMSignalError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/api/v2/custom-nodes/validate")
    async def validate_custom_node_route(
        body: CustomDataNodeSpec,
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            return validate_custom_data_node(body, root=configured.custom_nodes_root)
        except CustomNodeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/api/v2/custom-nodes/run")
    async def run_custom_node_route(
        body: CustomDataNodeRunRequest,
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            result = run_custom_data_node(
                body.spec,
                root=configured.custom_nodes_root,
                datas=body.datas,
                context=body.context,
            )
        except CustomNodeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"ok": True, "result": result, "execution_boundary": "trusted_local_code_only"}

    @app.get("/api/v2/workflows/editor", response_class=HTMLResponse)
    async def workflow_editor_route(
        _: TrustedIdentity = Depends(identity_dependency),
    ) -> str:
        return _workflow_editor_html()

    @app.post("/api/v1/tasks", status_code=status.HTTP_202_ACCEPTED)
    async def submit_task(
        body: SubmitBacktestRequest,
        identity: TrustedIdentity = Depends(identity_dependency),
        idempotency_key: str | None = Header(
            default=None, alias="Idempotency-Key", max_length=200
        ),
    ) -> dict:
        try:
            receipt = await _submit_with_idempotency(
                task_manager,
                user_id=identity.user_id,
                source_node=identity.source_node,
                request=body.model_dump(),
                idempotency_key=idempotency_key,
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except QueueLimitError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        return {
            "success": True,
            "task_id": receipt.task_id,
            "idempotency_key": receipt.idempotency_key,
            "idempotent_replay": receipt.idempotent_replay,
            "message": "回测任务已提交到工作站",
            "execution_backend": "workstation",
            "event_stream": "delta-poll",
        }

    @app.post("/api/v2/tasks", status_code=status.HTTP_202_ACCEPTED)
    async def submit_task_v2(
        body: SubmitBacktestRequestV2,
        identity: TrustedIdentity = Depends(identity_dependency),
        idempotency_key: str | None = Header(
            default=None, alias="Idempotency-Key", max_length=200
        ),
    ) -> dict:
        try:
            body.validate_contract()
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        a_share_catalog: dict = {}
        if body.engine_type in AI_CAPABLE_ENGINE_TYPES and daa_adapter.configured:
            try:
                a_share_catalog = await daa_adapter.get_capabilities()
            except DaaCapabilitiesError:
                a_share_catalog = {}
        engine_strategies = _engine_strategies(a_share_catalog, body.engine_type)
        supported_engine = (
            body.engine_type == "vnpy_cta"
            or (body.engine_type == "a_share_emotion_etf" and emotion_etf_available)
            or bool(engine_strategies)
            or (body.engine_type == "microstructure" and microstructure_available)
            or (body.engine_type in ML_ENGINE_TYPES and learning_available)
            or (body.engine_type in LIGHTER_ENGINE_TYPES and lighter_available)
        )
        if not supported_engine:
            raise HTTPException(
                status_code=501,
                detail=f"回测引擎尚未安装: {body.engine_type}",
            )
        if body.engine_type in AI_CAPABLE_ENGINE_TYPES:
            selected_strategy = next(
                (
                    item
                    for item in engine_strategies
                    if item.get("id") == body.strategy.id
                ),
                None,
            )
            # CTA/真实 Tick 的内置策略仍走各自 legacy/runtime 校验；只有
            # DAA 目录中的 AI 策略才进入这里的哈希门禁。
            builtin_strategy = (
                body.engine_type == "microstructure"
                and body.strategy.id == MICROSTRUCTURE_STRATEGY_ID
            ) or (
                body.engine_type == "vnpy_cta"
                and not body.strategy.id.startswith("ai_")
            )
            if selected_strategy is None and not builtin_strategy:
                raise HTTPException(
                    status_code=422,
                    detail=f"DAA {body.engine_type} 策略不存在或未注册",
                )
            if selected_strategy is not None and (
                selected_strategy.get("source_hash")
                != body.strategy.source_hash.lower()
            ):
                raise HTTPException(
                    status_code=409, detail="DAA 策略源码版本已变化"
                )
            if selected_strategy is not None and body.engine_type in {"vnpy_cta", "microstructure"}:
                if selected_strategy.get("source") != "ai" or selected_strategy.get("execution_backend") != "polars_expr":
                    raise HTTPException(
                        status_code=422,
                        detail="CTA/真实 Tick 只接受已验证的 DAA polars_expr 策略",
                    )

        if body.engine_type == "microstructure" and not body.strategy.id.startswith("ai_") and (
            body.strategy.id != MICROSTRUCTURE_STRATEGY_ID
            or body.strategy.source_hash.lower() != MICROSTRUCTURE_STRATEGY_HASH
        ):
            raise HTTPException(status_code=409, detail="microstructure 策略版本不一致")
        if body.engine_type == "a_share_emotion_etf" and (
            body.strategy.id != EMOTION_ETF_STRATEGY_ID
            or body.strategy.entrypoint != EMOTION_ETF_STRATEGY_ID
            or body.strategy.source_hash.lower() != EMOTION_ETF_STRATEGY_HASH
        ):
            raise HTTPException(status_code=409, detail="情绪ETF策略版本不一致")
        if body.engine_type in ML_ENGINE_TYPES and (
            body.strategy.id != "temporal_ml_rank_v1"
            or body.strategy.entrypoint != "temporal_ml_rank_v1"
            or body.strategy.source_hash.lower() != ML_STRATEGY_HASH
        ):
            raise HTTPException(status_code=409, detail="学习策略版本不一致")
        if body.engine_type in LIGHTER_ENGINE_TYPES and (
            body.strategy.id != LIGHTER_STRATEGY_ID
            or body.strategy.entrypoint != LIGHTER_STRATEGY_ID
            or body.strategy.source_hash.lower() != LIGHTER_STRATEGY_HASH
        ):
            raise HTTPException(status_code=409, detail="Lighter 策略版本不一致")

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
            if body.data.selection is not None and exc.status_code == 422:
                submission_payload = body.model_dump(mode="json")
                try:
                    receipt = await _submit_with_idempotency(
                        task_manager,
                        user_id=identity.user_id,
                        source_node=identity.source_node,
                        request=_waiting_worker_request(body),
                        idempotency_key=idempotency_key,
                        initial_status="waiting_for_data",
                        idempotency_payload=submission_payload,
                    )
                except IdempotencyConflictError as conflict:
                    raise HTTPException(status_code=409, detail=str(conflict)) from conflict
                except QueueLimitError as queue_error:
                    raise HTTPException(status_code=429, detail=str(queue_error)) from queue_error
                task = task_manager.store.get_task(identity.user_id, receipt.task_id)
                requirement = dict(task.get("data_requirement") or {})
                if task["status"] == "waiting_for_data" and not requirement:
                    registrar = getattr(task_manager, "register_data_requirement", None)
                    if registrar is None:
                        raise HTTPException(
                            status_code=503,
                            detail="回测管理器未启用数据补齐协调器",
                        ) from exc
                    requirement = await registrar(
                        receipt.task_id,
                        _data_requirement_payload(body, receipt.task_id),
                    )
                return {
                    "success": True,
                    "task_id": receipt.task_id,
                    "idempotency_key": receipt.idempotency_key,
                    "idempotent_replay": receipt.idempotent_replay,
                    "schema_version": 2,
                    "contract_version": "pxybacktest.task-result.v2",
                    "engine_type": body.engine_type,
                    "status": task["status"],
                    "data_snapshot": task.get("data_snapshot"),
                    "data_requirement": requirement or task.get("data_requirement"),
                    "execution_backend": "workstation",
                    "event_stream": "delta-poll",
                }
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
            worker_request = resolved.to_worker_request(snapshot_manifest=snapshot_manifest)
            receipt = await _submit_with_idempotency(
                task_manager,
                user_id=identity.user_id,
                source_node=identity.source_node,
                request=worker_request,
                idempotency_key=idempotency_key,
                idempotency_payload=body.model_dump(mode="json"),
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except QueueLimitError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        return {
            "success": True,
            "task_id": receipt.task_id,
            "idempotency_key": receipt.idempotency_key,
            "idempotent_replay": receipt.idempotent_replay,
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
        await asyncio.to_thread(task_manager.expire_results)
        tasks = await asyncio.to_thread(task_manager.store.list_tasks, identity.user_id)
        return {"success": True, "tasks": tasks, "total": len(tasks)}

    @app.put("/api/v1/drafts/{draft_id}")
    async def save_draft(
        draft_id: str,
        body: dict,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        draft = await asyncio.to_thread(
            task_manager.store.save_draft,
            identity.user_id,
            draft_id,
            body,
        )
        return {"success": True, **draft}

    @app.get("/api/v1/drafts/{draft_id}")
    async def get_draft(
        draft_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            draft = await asyncio.to_thread(
                task_manager.store.get_draft, identity.user_id, draft_id
            )
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测草稿不存在") from exc
        return {"success": True, **draft}

    @app.delete("/api/v1/drafts/{draft_id}")
    async def delete_draft(
        draft_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        deleted = await asyncio.to_thread(
            task_manager.store.delete_draft, identity.user_id, draft_id
        )
        return {"success": True, "deleted": deleted}

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
        def load_event_page() -> tuple[dict, dict]:
            page = task_manager.store.event_page(
                identity.user_id, task_id, after_seq, limit
            )
            context = task_manager.store.queue_context(identity.user_id, task_id)
            return page, context

        try:
            page, queue_context = await asyncio.to_thread(load_event_page)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {
            "success": True,
            "task_id": task_id,
            **page,
            **queue_context,
        }

    @app.get("/api/v1/tasks/{task_id}/export.zip")
    async def export_task_zip(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> Response:
        def build_archive() -> bytes:
            task = task_manager.result(identity.user_id, task_id)
            request = task_manager.store.get_request(task_id)
            events = task_manager.store.events_after(
                identity.user_id, task_id, after_seq=0, limit=1_000_000
            )
            files: dict[str, bytes] = {
                "task.json": json.dumps(
                    task, ensure_ascii=False, sort_keys=True, default=str, indent=2
                ).encode("utf-8"),
                "request.json": json.dumps(
                    request, ensure_ascii=False, sort_keys=True, default=str, indent=2
                ).encode("utf-8"),
                "events.ndjson": (
                    "\n".join(
                        json.dumps(
                            event,
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                            separators=(",", ":"),
                        )
                        for event in events
                    )
                    + ("\n" if events else "")
                ).encode("utf-8"),
            }
            if isinstance(task.get("result"), dict):
                files["result.json"] = json.dumps(
                    task["result"],
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                    indent=2,
                ).encode("utf-8")
            manifest = {
                "contract_version": "pxybacktest.export.v1",
                "task_id": task_id,
                "generated_at": time.time(),
                "files": [
                    {
                        "path": name,
                        "size": len(content),
                        "sha256": hashlib.sha256(content).hexdigest(),
                    }
                    for name, content in sorted(files.items())
                ],
            }
            files["manifest.json"] = json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            ).encode("utf-8")
            buffer = io.BytesIO()
            with zipfile.ZipFile(
                buffer, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as archive:
                for name, content in files.items():
                    archive.writestr(name, content)
            return buffer.getvalue()

        try:
            content = await asyncio.to_thread(build_archive)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return Response(
            content=content,
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="backtest-{task_id}.zip"'
            },
        )

    @app.post("/api/v1/tasks/{task_id}/pause")
    async def pause_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            result = await task_manager.pause(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        message = (
            "回测已暂停"
            if result.confirmed
            else (
                "暂停指令已提交，正在等待工作站确认"
                if result.accepted
                else f"任务当前状态为 {result.status}，无法暂停"
            )
        )
        return {
            "success": result.accepted,
            "confirmed": result.confirmed,
            "status": result.status,
            "message": message,
        }

    @app.post("/api/v1/tasks/{task_id}/resume")
    async def resume_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            result = await task_manager.resume(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        message = (
            "回测继续"
            if result.confirmed
            else (
                "继续指令已提交，正在等待工作站确认"
                if result.accepted
                else f"任务当前状态为 {result.status}，无法继续"
            )
        )
        return {
            "success": result.accepted,
            "confirmed": result.confirmed,
            "status": result.status,
            "message": message,
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

    @app.post("/api/v1/tasks/{task_id}/step")
    async def step_task(
        task_id: str,
        identity: TrustedIdentity = Depends(identity_dependency),
    ) -> dict:
        try:
            result = await task_manager.step(identity.user_id, task_id)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail="回测任务不存在") from exc
        return {
            "success": result.accepted,
            "confirmed": result.confirmed,
            "status": result.status,
            "message": (
                "已前进一个回放事件"
                if result.confirmed
                else "单步指令已提交，正在等待工作站确认"
                if result.accepted
                else "任务必须先暂停才能单步执行"
            ),
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
