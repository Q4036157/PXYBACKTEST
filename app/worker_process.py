from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any

A_SHARE_ADAPTER_CONTRACT = "pxybacktest.engine-adapter.a-share.v1"
DAA_ENGINE_TYPES = {"a_share_portfolio", "factor_matrix", "event_sentiment"}
ML_ENGINE_TYPES = {"ml_factor", "deep_learning"}
LIGHTER_ENGINE_TYPES = {"lighter_microstructure"}
TQSDK_ENGINE_TYPES = {"tqsdk_native"}
RELIABLE_EVENT_TIMEOUT_SECONDS = 5.0
logger = logging.getLogger("backtest_service")


def _parse_request_rate(request: dict[str, Any], default: float = 0.0004) -> float:
    """解析手续费率，保留调用方显式传入的 0。"""

    value = request.get("rate")
    if value is None:
        return float(default)
    return float(value)


def _is_completed_visual_bar(task: Any) -> bool:
    """只在当前 K 线完整形成时强制保留最终显示帧。"""

    try:
        return float(getattr(task, "replay_bar_progress", 0.0) or 0.0) >= 0.999
    except (TypeError, ValueError):
        return False


def _configure_backtest_worker_logging() -> None:
    """让工作进程输出回测数据链路的 INFO 日志。"""
    logger = logging.getLogger("backtest_service")
    if not any(
        getattr(handler, "name", "") == "pxybacktest-worker"
        for handler in logger.handlers
    ):
        handler = logging.StreamHandler()
        handler.name = "pxybacktest-worker"
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _configure_pxylh_cta_worker_environment() -> dict[str, Any]:
    """在隔离 worker 内桥接 PXYBACKTEST 与 PXYLH CTA loader 的配置名。"""

    from .config import pxylh_cta_worker_environment

    mapped = pxylh_cta_worker_environment()
    for name, value in mapped.items():
        os.environ[name] = value

    credential_source = "unconfigured"
    if "PXYDATA_API_KEY_FILE" in mapped:
        credential_source = "pxybacktest_api_key_file"
    elif str(os.getenv("PXYDATA_API_KEY_FILE") or "").strip():
        credential_source = "inherited_api_key_file"
    elif str(os.getenv("PXYDATA_API_KEY") or "").strip():
        credential_source = "inherited_direct_api_key"

    return {
        "loader": "pxylh.services.backtest_service.kline_loader",
        "execution_source": "vnpy_database_compat_cache",
        "population_policy": "pxydata_preferred_with_vnpy_ccxt_fallback",
        "actual_upstream": "not_reported_by_loader",
        "credential_source": credential_source,
        "immutable_snapshot_verified": False,
    }


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _atomic_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8"
    )
    temporary.replace(path)


def build_replay_event_snapshot(events: list[Any], limit: int = 20) -> list[Any]:
    return list(events[-max(1, limit) :])


def preload_pxylh_runtime(pxylh_root: str) -> None:
    backend_root = Path(pxylh_root).resolve() / "backend"
    if not backend_root.is_dir():
        raise FileNotFoundError(
            f"PXYLH backend directory does not exist: {backend_root}"
        )
    backend_path = str(backend_root)
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)
    from services.backtest_service.engine_runner import run_backtest_sync  # noqa: F401
    from services.backtest_service.kline_loader import (
        ensure_backtest_kline_data,  # noqa: F401
    )
    from services.backtest_service.models import (  # noqa: F401
        BacktestTask,
        convert_numpy_types,
    )
    from services.backtest_service.replay_runtime_utils import (  # noqa: F401
        build_replay_state,
        extract_live_orders,
        extract_live_positions,
        extract_live_trades,
    )
    from services.backtest_service.strategy_line_utils import (
        extract_strategy_lines,  # noqa: F401
    )


def _daa_python(daa_root: Path) -> Path:
    windows_python = daa_root / "backend" / ".venv" / "Scripts" / "python.exe"
    if windows_python.is_file():
        return windows_python
    return daa_root / "backend" / ".venv" / "bin" / "python"


def _safe_adapter_error(error: Any, *private_roots: Path) -> str:
    message = " ".join(str(error or "DAA worker failed").split())[:500]
    for root in private_roots:
        text = str(root)
        if text:
            message = message.replace(text, "<internal>")
    return message


def _stop_subprocess(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=0.25)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=1.0)


def _drain_worker_commands(command_queue, deferred: list[dict[str, Any]]) -> bool:
    """读取取消状态，同时保留暂停、继续和调速命令供 ReplayClock 使用。"""

    cancelled = False
    try:
        while True:
            command = command_queue.get_nowait()
            if str(command.get("action") or "").strip().lower() == "cancel":
                cancelled = True
                if not any(item.get("action") == "cancel" for item in deferred):
                    deferred.append({"action": "cancel"})
            elif isinstance(command, dict):
                deferred.append(command)
    except queue.Empty:
        pass
    return cancelled


def run_a_share_worker(
    task_id: str,
    request: dict[str, Any],
    daa_root: str,
    pxydata_root: str,
    result_path: str,
    job_dir: str,
    event_queue,
    command_queue,
) -> None:
    """通过 DAA 自有 Python 进程执行 manifest-bound A 股回测。"""
    daa_path = Path(daa_root).resolve()
    backend_root = daa_path / "backend"
    data_root = Path(pxydata_root).resolve()
    python = _daa_python(daa_path)
    work_dir = Path(job_dir)
    final_path = Path(result_path)
    cancelled = False
    deferred_commands: list[dict[str, Any]] = []

    try:
        if (
            not python.is_file()
            or not (backend_root / "app" / "backtest" / "pxy_adapter.py").is_file()
        ):
            raise RuntimeError("DAA A 股适配器未安装")
        if not data_root.is_dir():
            raise RuntimeError("PXYDATA 数据根目录不可用")
        task_contract = request.get("_task_contract")
        manifest = request.get("_snapshot_manifest")
        if not isinstance(task_contract, dict) or not isinstance(manifest, dict):
            raise ValueError("A 股 worker 请求缺少任务契约或完整快照清单")

        _emit(
            event_queue,
            "state",
            {"status": "running", "phase": "verifying_snapshot", "progress": 2.0},
        )
        _emit(
            event_queue,
            "state",
            {"status": "running", "phase": "running_engine", "progress": 10.0},
        )

        def cancel_requested() -> bool:
            nonlocal cancelled
            cancelled = cancelled or _drain_worker_commands(
                command_queue, deferred_commands
            )
            return cancelled

        from app.optimization import run_task_optimization
        from app.result_contract import build_a_share_result_v2

        evaluation_index = 0

        def evaluate(candidate: dict[str, Any]) -> dict[str, Any]:
            nonlocal evaluation_index
            evaluation_index += 1
            adapter_request_path = work_dir / f"a-share-request-{evaluation_index}.json"
            adapter_result_path = work_dir / f"a-share-result-{evaluation_index}.json"
            candidate_request = {**request, "_task_contract": candidate}
            _atomic_json_write(
                adapter_request_path,
                {
                    "contract_version": A_SHARE_ADAPTER_CONTRACT,
                    "task_id": task_id,
                    "task": candidate,
                    "manifest": manifest,
                },
            )
            command = [
                str(python),
                "-m",
                "app.backtest.pxy_adapter",
                "--request",
                str(adapter_request_path),
                "--result",
                str(adapter_result_path),
                "--pxydata-root",
                str(data_root),
            ]
            process = subprocess.Popen(
                command,
                cwd=backend_root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            while process.poll() is None:
                if cancel_requested():
                    _stop_subprocess(process)
                    raise InterruptedError("A 股回测任务已取消")
                time.sleep(0.05)
            if not adapter_result_path.is_file():
                raise RuntimeError("DAA worker 未生成结果")
            adapter_response = json.loads(
                adapter_result_path.read_text(encoding="utf-8")
            )
            if not isinstance(adapter_response, dict):
                raise RuntimeError("DAA worker 返回格式无效")
            if not adapter_response.get("success"):
                error = _safe_adapter_error(
                    adapter_response.get("error"), daa_path, data_root, work_dir
                )
                raise RuntimeError(f"DAA A 股回测失败: {error}")
            raw_result = adapter_response.get("result")
            if not isinstance(raw_result, dict):
                raise RuntimeError("DAA worker 结果缺少 result 对象")
            return build_a_share_result_v2(
                task_id=task_id,
                request=candidate_request,
                raw_result=raw_result,
            )

        result = run_task_optimization(
            task_contract,
            evaluate,
            cancel_check=cancel_requested,
        )
        outcome = _replay_non_cta_result(
            event_queue,
            command_queue,
            task_id=task_id,
            request=request,
            result=result,
            engine_type=str(task_contract.get("engine_type") or "a_share_portfolio"),
            deferred_commands=deferred_commands,
        )
        _atomic_json_write(final_path, result)
        if not outcome["complete"]:
            _emit(
                event_queue,
                "cancelled",
                {
                    "result_path": str(final_path),
                    "result_available": True,
                    "partial": True,
                    "processed_events": outcome["processed_events"],
                },
                terminal=True,
            )
            return
        _emit(
            event_queue,
            "completed",
            {"result_path": str(final_path), "progress": 100.0},
            terminal=True,
        )
    except Exception as exc:
        if cancelled:
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        error = _safe_adapter_error(exc, daa_path, data_root, work_dir)
        _emit(
            event_queue,
            "failed",
            {"error": f"DAA A 股回测失败: {error}"},
            terminal=True,
        )


def run_preloaded_worker(
    pxylh_root: str,
    daa_root: str,
    pxydata_root: str,
    render_interval_ms: int,
    event_queue,
    command_queue,
    job_queue,
    ready_queue,
) -> None:
    _configure_backtest_worker_logging()
    _configure_pxylh_cta_worker_environment()
    try:
        preload_pxylh_runtime(pxylh_root)
    except BaseException as exc:
        pxylh_preload_error = f"{type(exc).__name__}: {exc}"
    else:
        pxylh_preload_error = ""
    ready_queue.put({"ready": True, "pxylh_preload_error": pxylh_preload_error})

    job = job_queue.get()
    if job is None:
        return
    task_id, request, result_path, job_dir = job
    engine_type = str(
        (request.get("_task_contract") or {}).get("engine_type") or "vnpy_cta"
    )
    if engine_type in DAA_ENGINE_TYPES:
        run_a_share_worker(
            task_id,
            request,
            daa_root,
            pxydata_root,
            result_path,
            job_dir,
            event_queue,
            command_queue,
        )
        return
    if engine_type == "microstructure":
        run_microstructure_worker(
            task_id,
            request,
            pxydata_root,
            result_path,
            event_queue,
            command_queue,
            daa_root=daa_root,
        )
        return
    if engine_type in ML_ENGINE_TYPES:
        run_learning_worker(
            task_id,
            request,
            pxydata_root,
            result_path,
            event_queue,
            command_queue,
        )
        return
    if engine_type in LIGHTER_ENGINE_TYPES:
        run_lighter_worker(
            task_id,
            request,
            pxydata_root,
            result_path,
            event_queue,
            command_queue,
        )
        return
    if engine_type in TQSDK_ENGINE_TYPES:
        run_tqsdk_queue_worker(
            task_id,
            request,
            result_path,
            job_dir,
            event_queue,
            command_queue,
        )
        return
    run_backtest_worker(
        task_id,
        request,
        pxylh_root,
        result_path,
        render_interval_ms,
        event_queue,
        command_queue,
        daa_root=daa_root,
    )


def _tqsdk_result_v2(
    *,
    task_id: str,
    task: dict[str, Any],
    native: dict[str, Any],
) -> dict[str, Any]:
    """把天勤原生结果封装为统一 task-result.v2。"""

    execution = dict(task.get("execution") or {})
    initial_capital = float(execution.get("capital") or 1_000_000)
    final_account = dict(native.get("final_account") or {})
    final_equity = float(
        final_account.get("balance")
        or final_account.get("equity")
        or initial_capital
    )
    deals = list(native.get("deals") or [])
    orders_raw = native.get("orders") or {}
    orders = (
        list(orders_raw.values()) if isinstance(orders_raw, dict) else list(orders_raw)
    )
    positions_raw = native.get("positions") or {}
    if isinstance(positions_raw, dict):
        positions = [
            {"symbol": str(symbol), **dict(value)}
            for symbol, value in positions_raw.items()
            if isinstance(value, dict)
        ]
    else:
        positions = list(positions_raw)
    account_curve = list(native.get("account_curve") or [])
    manifest_sha256 = str(native.get("data_manifest_sha256") or "")
    snapshot_id = f"tqsdk_v1_{manifest_sha256[:32]}"
    strategy = dict(task.get("strategy") or {})
    replay_events = list(native.get("replay_events") or [])
    result = {
        "schema_version": 2,
        "contract_version": "pxybacktest.task-result.v2",
        "task_id": task_id,
        "engine_type": "tqsdk_native",
        "strategy": strategy,
        "data_snapshot": {
            "provider": "tqsdk",
            "snapshot_id": snapshot_id,
            "manifest_sha256": manifest_sha256,
            "captured_event_count": int(
                (native.get("visual") or {}).get("market_event_count") or 0
            ),
            "capture_policy": "materialize_subscribed_market_events",
        },
        "run": {
            "universe": task.get("universe") or {},
            "period": task.get("period") or {},
            "execution": execution,
            "parameters": task.get("parameters") or {},
        },
        "metrics": {
            "initial_capital": initial_capital,
            "final_equity": final_equity,
            "net_profit": final_equity - initial_capital,
            "total_return": final_equity / initial_capital - 1.0,
            "n_trades": len(deals),
            "commission": sum(float(item.get("commission") or 0) for item in deals),
            **dict(native.get("native_metrics") or {}),
        },
        "curves": {"equity": account_curve},
        "orders": orders,
        "deals": deals,
        "positions": positions,
        "execution_snapshot": native.get("execution_snapshot") or {},
        "replay_audit": native.get("replay_audit") or {},
        "diagnostics": {
            "adapter": "tqsdk.native.v1",
            "runtime_identity": native.get("runtime_identity"),
            "sandbox": native.get("sandbox") or {},
            "native_trade_log_sha256": native.get("native_trade_log_sha256"),
            "data_manifest_sha256": manifest_sha256,
            "pause_scope": "replay_only",
            "native_execution_pause_supported": False,
            "warnings": [
                "暂停和调速只驱动原生计算完成后的 ReplayClock；取消会终止整个受限进程树。"
            ],
        },
        "reproducibility": {
            "strategy_source_sha256": strategy.get("source_hash"),
            "data_manifest_sha256": manifest_sha256,
            "runtime_identity": native.get("runtime_identity"),
            "event_log_sha256": (native.get("replay_audit") or {}).get(
                "chain_sha256"
            ),
            "event_count": len(replay_events),
        },
        "_replay_events": replay_events,
    }
    result["reproducibility"]["result_sha256"] = _stable_hash(
        {key: value for key, value in result.items() if key != "_replay_events"}
    )
    return result


def run_tqsdk_queue_worker(
    task_id: str,
    request: dict[str, Any],
    result_path: str,
    job_dir: str,
    event_queue,
    command_queue,
) -> None:
    """在现有用户隔离队列中执行天勤原生安全沙箱。"""

    cancelled = False
    deferred_commands: list[dict[str, Any]] = []
    work_dir = Path(job_dir).resolve()
    final_path = Path(result_path)
    try:
        task = request.get("_task_contract")
        source_code = request.get("_tqsdk_source_code")
        permissions = request.get("_tqsdk_permissions")
        if not isinstance(task, dict) or not isinstance(source_code, str):
            raise ValueError("天勤 worker 缺少任务契约或策略源码")
        if not isinstance(permissions, dict):
            raise ValueError("天勤 worker 缺少沙箱权限契约")
        strategy = dict(task.get("strategy") or {})
        expected_hash = str(strategy.get("source_hash") or "").lower()
        actual_hash = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
        if not expected_hash or actual_hash != expected_hash:
            raise ValueError("天勤策略源码 SHA256 与任务契约不一致")
        entrypoint = str(strategy.get("entrypoint") or "strategy.py")
        if Path(entrypoint).name != entrypoint or not entrypoint.lower().endswith(".py"):
            raise ValueError("天勤策略入口必须是任务目录内的 Python 文件名")

        work_dir.mkdir(parents=True, exist_ok=True)
        strategy_path = work_dir / entrypoint
        strategy_path.write_text(source_code, encoding="utf-8", newline="\n")
        native_result_path = work_dir / "tqsdk-native-result.json"
        period = dict(task.get("period") or {})
        execution = dict(task.get("execution") or {})
        python_raw = os.getenv("PXYBACKTEST_TQSDK_PYTHON", "").strip()
        python_path = (
            Path(python_raw)
            if python_raw
            else Path(__file__).resolve().parents[1]
            / ".venv"
            / "Scripts"
            / "python.exe"
        )

        def cancel_requested() -> bool:
            nonlocal cancelled
            cancelled = cancelled or _drain_worker_commands(
                command_queue, deferred_commands
            )
            return cancelled

        if cancel_requested():
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(
            event_queue,
            "state",
            {
                "status": "running",
                "phase": "running_tqsdk_native",
                "progress": 5.0,
                "pause_scope": "replay_only",
            },
        )

        from app.tqsdk_native_worker import (
            TqSdkWorkerRequest,
            launch_tqsdk_worker,
        )

        native = launch_tqsdk_worker(
            TqSdkWorkerRequest(
                task_id=task_id,
                task_root=work_dir,
                strategy_path=strategy_path,
                result_path=native_result_path,
                start_date=period.get("start"),
                end_date=period.get("end"),
                initial_balance=float(execution.get("capital") or 1_000_000),
                memory_mb=int(permissions.get("memory_mb") or 4096),
                cpu_cores=int(permissions.get("cpu_cores") or 1),
            ),
            python_executable=python_path,
            project_root=Path(__file__).resolve().parents[1],
            timeout_seconds=int(permissions.get("timeout_seconds") or 3600),
            cancel_check=cancel_requested,
        )
        sandbox = dict(native.get("sandbox") or {})
        if not sandbox.get("submit_ready"):
            raise RuntimeError("天勤网络白名单部署证明缺失，队列拒绝保存执行结果")

        result = _tqsdk_result_v2(task_id=task_id, task=task, native=native)
        outcome = _replay_non_cta_result(
            event_queue,
            command_queue,
            task_id=task_id,
            request=request,
            result=result,
            engine_type="tqsdk_native",
            deferred_commands=deferred_commands,
        )
        _atomic_json_write(final_path, result)
        if not outcome["complete"]:
            _emit(
                event_queue,
                "cancelled",
                {
                    "result_path": str(final_path),
                    "result_available": True,
                    "partial": True,
                    "processed_events": outcome["processed_events"],
                },
                terminal=True,
            )
            return
        _emit(
            event_queue,
            "completed",
            {"result_path": str(final_path), "progress": 100.0},
            terminal=True,
        )
    except Exception as exc:  # noqa: BLE001
        if cancelled or "已取消" in str(exc):
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(
            event_queue,
            "failed",
            {"error": f"天勤原生回测失败: {type(exc).__name__}: {exc}"},
            terminal=True,
        )


def run_microstructure_worker(
    task_id: str,
    request: dict[str, Any],
    pxydata_root: str,
    result_path: str,
    event_queue,
    command_queue,
    daa_root: str | None = None,
) -> None:
    """在隔离 worker 中执行 manifest-bound 真实 Tick 回放。"""
    cancelled = False
    deferred_commands: list[dict[str, Any]] = []
    try:
        def cancel_requested() -> bool:
            nonlocal cancelled
            cancelled = cancelled or _drain_worker_commands(
                command_queue, deferred_commands
            )
            return cancelled

        if cancel_requested():
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        task = request.get("_task_contract")
        manifest = request.get("_snapshot_manifest")
        if not isinstance(task, dict) or not isinstance(manifest, dict):
            raise ValueError("microstructure worker 缺少任务契约或完整快照清单")
        _emit(
            event_queue,
            "state",
            {"status": "running", "phase": "verifying_ticks", "progress": 5.0},
        )
        from app.microstructure import run_microstructure_backtest
        from app.optimization import run_task_optimization

        def evaluate(candidate: dict[str, Any]) -> dict[str, Any]:
            return run_microstructure_backtest(
                task_id=task_id,
                task=candidate,
                manifest=manifest,
                data_root=pxydata_root,
                daa_root=daa_root,
            )

        result = run_task_optimization(
            task,
            evaluate,
            cancel_check=cancel_requested,
        )
        outcome = _replay_non_cta_result(
            event_queue,
            command_queue,
            task_id=task_id,
            request=request,
            result=result,
            engine_type="microstructure",
            deferred_commands=deferred_commands,
        )
        _atomic_json_write(Path(result_path), result)
        if not outcome["complete"]:
            _emit(
                event_queue,
                "cancelled",
                {
                    "result_path": str(result_path),
                    "result_available": True,
                    "partial": True,
                    "processed_events": outcome["processed_events"],
                },
                terminal=True,
            )
            return
        _emit(
            event_queue,
            "completed",
            {"result_path": str(result_path), "progress": 100.0},
            terminal=True,
        )
    except Exception as exc:
        if cancelled:
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(
            event_queue,
            "failed",
            {"error": f"microstructure 回测失败: {type(exc).__name__}: {exc}"},
            terminal=True,
        )


def run_learning_worker(
    task_id: str,
    request: dict[str, Any],
    pxydata_root: str,
    result_path: str,
    event_queue,
    command_queue,
) -> None:
    """在隔离进程中执行快照绑定的 ML/深度学习研究回测。"""
    cancelled = False
    deferred_commands: list[dict[str, Any]] = []
    try:
        def cancel_requested() -> bool:
            nonlocal cancelled
            cancelled = cancelled or _drain_worker_commands(
                command_queue, deferred_commands
            )
            return cancelled

        if cancel_requested():
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        task = request.get("_task_contract")
        manifest = request.get("_snapshot_manifest")
        if not isinstance(task, dict) or not isinstance(manifest, dict):
            raise ValueError("学习 worker 缺少任务契约或完整快照清单")
        _emit(
            event_queue,
            "state",
            {"status": "running", "phase": "training_model", "progress": 5.0},
        )
        from app.learning import run_learning_backtest

        if cancel_requested():
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        result = run_learning_backtest(
            task_id=task_id,
            task=task,
            manifest=manifest,
            data_root=pxydata_root,
        )
        outcome = _replay_non_cta_result(
            event_queue,
            command_queue,
            task_id=task_id,
            request=request,
            result=result,
            engine_type=str(task.get("engine_type") or "ml_factor"),
            deferred_commands=deferred_commands,
        )
        _atomic_json_write(Path(result_path), result)
        if not outcome["complete"]:
            _emit(
                event_queue,
                "cancelled",
                {
                    "result_path": str(result_path),
                    "result_available": True,
                    "partial": True,
                    "processed_events": outcome["processed_events"],
                },
                terminal=True,
            )
            return
        _emit(
            event_queue,
            "completed",
            {"result_path": str(result_path), "progress": 100.0},
            terminal=True,
        )
    except Exception as exc:
        if cancelled:
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(
            event_queue,
            "failed",
            {"error": f"学习回测失败: {type(exc).__name__}: {exc}"},
            terminal=True,
        )


def run_lighter_worker(
    task_id: str,
    request: dict[str, Any],
    pxydata_root: str,
    result_path: str,
    event_queue,
    command_queue,
) -> None:
    """执行 manifest-bound Lighter 资金费/盘口/主动成交回放。"""
    cancelled = False
    deferred_commands: list[dict[str, Any]] = []
    try:
        task = request.get("_task_contract")
        manifest = request.get("_snapshot_manifest")
        if not isinstance(task, dict) or not isinstance(manifest, dict):
            raise ValueError("Lighter worker 缺少任务契约或完整快照清单")
        def cancel_requested() -> bool:
            nonlocal cancelled
            cancelled = cancelled or _drain_worker_commands(
                command_queue, deferred_commands
            )
            return cancelled

        cancel_requested()
        if cancelled:
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(event_queue, "state", {"status": "running", "phase": "rebuilding_lighter_book", "progress": 5.0})
        from app.lighter_microstructure import run_lighter_backtest
        from app.optimization import run_task_optimization

        def evaluate(candidate: dict[str, Any]) -> dict[str, Any]:
            if cancel_requested():
                raise InterruptedError("Lighter 回测任务已取消")
            return run_lighter_backtest(
                task_id=task_id,
                task=candidate,
                manifest=manifest,
                data_root=pxydata_root,
            )

        logger.info(
            "Lighter 回测开始: task_id=%s snapshot=%s datasets=%s optimization=%s",
            task_id,
            (task.get("data") or {}).get("snapshot_id")
            or ((task.get("data") or {}).get("snapshot") or {}).get("snapshot_id"),
            [
                item.get("name")
                for item in manifest.get("datasets", [])
                if isinstance(item, dict)
            ],
            bool(task.get("optimization")),
        )
        result = run_task_optimization(
            task,
            evaluate,
            cancel_check=cancel_requested,
        )
        cancel_requested()
        outcome = _replay_non_cta_result(
            event_queue,
            command_queue,
            task_id=task_id,
            request=request,
            result=result,
            engine_type="lighter_microstructure",
            deferred_commands=deferred_commands,
        )
        _atomic_json_write(Path(result_path), result)
        if not outcome["complete"]:
            _emit(
                event_queue,
                "cancelled",
                {
                    "result_path": str(result_path),
                    "result_available": True,
                    "partial": True,
                    "processed_events": outcome["processed_events"],
                },
                terminal=True,
            )
            return
        logger.info(
            "Lighter 回测完成: task_id=%s n_trades=%s total_return=%s optimized=%s",
            task_id,
            (result.get("metrics") or {}).get("n_trades"),
            (result.get("metrics") or {}).get("total_return"),
            bool(result.get("optimization")),
        )
        _emit(event_queue, "completed", {"result_path": str(result_path), "progress": 100.0}, terminal=True)
    except Exception as exc:
        if cancelled:
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(event_queue, "failed", {"error": f"Lighter 回测失败: {type(exc).__name__}: {exc}"}, terminal=True)


def _emit(
    event_queue,
    event_type: str,
    data: dict[str, Any],
    *,
    terminal: bool = False,
    reliable: bool = False,
) -> bool:
    """向管理器发送事件。

    可靠事件不能在队列满时静默丢失：在有限时间内无法投递时直接让
    worker 失败，由任务层记录失败，而不是继续生成不可审计的结果。
    普通 UI/回放事件仍允许降采样，但会留下明确的日志证据。
    """
    event = {"type": event_type, "data": data}
    try:
        if terminal or reliable:
            event_queue.put(event, timeout=RELIABLE_EVENT_TIMEOUT_SECONDS)
        else:
            event_queue.put_nowait(event)
        return True
    except queue.Full:
        logger = logging.getLogger("backtest_service")
        if terminal or reliable:
            raise RuntimeError(
                f"可靠回测事件投递失败：事件队列已满，event_type={event_type}"
            )
        logger.warning(
            "非可靠回测事件因队列已满被降采样：event_type=%s",
            event_type,
        )
        return False


def _emit_result_execution_snapshot(
    event_queue,
    *,
    task_id: str,
    request: dict[str, Any],
    result: dict[str, Any],
    engine_type: str,
) -> None:
    """把非 CTA 子进程的最终结果投影为统一执行快照。"""

    task = request.get("_task_contract") or {}
    snapshot_ref = ((task.get("data") or {}).get("snapshot") or {})
    period = task.get("period") or {}
    diagnostics = result.get("diagnostics") or {}
    market = result.get("market") or {}
    curves = result.get("curves") or {}

    def _symbol_map(value: Any) -> dict[str, Any]:
        """把列表形式的跨引擎行情/盘口/持仓规整为 symbol map。"""
        if isinstance(value, dict):
            return dict(value)
        if not isinstance(value, list):
            return {}
        mapped: dict[str, Any] = {}
        for index, item in enumerate(value):
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or item.get("vt_symbol") or index)
            mapped[symbol] = item
        return mapped

    bars = _symbol_map(
        result.get("bars")
        or market.get("bars")
        or result.get("market_bars")
    )
    order_books = _symbol_map(
        result.get("order_books")
        or market.get("order_books")
        or result.get("books")
    )
    positions = _symbol_map(result.get("positions"))
    fills = result.get("deals") or result.get("trades") or []
    if not isinstance(fills, list):
        fills = []
    if engine_type in {"microstructure", "lighter_microstructure"}:
        replay_mode = "real_tick"
        replay_source = "PXYDATA"
    elif engine_type in {"factor_matrix", "event_sentiment", "ml_factor", "deep_learning"}:
        replay_mode = "bar"
        replay_source = "PXYDATA"
    else:
        replay_mode = "bar"
        replay_source = "PXYDATA"
    snapshot = {
        "contract_version": "pxybacktest.replay.v1",
        "run_id": task_id,
        "snapshot_id": str(snapshot_ref.get("snapshot_id") or task_id),
        "event_seq": int(len(result.get("deals") or result.get("trades") or [])),
        "simulated_at": str(period.get("end") or ""),
        "bars": bars,
        "trades": result.get("trade_prints") or result.get("prints") or [],
        "order_books": order_books,
        "news": result.get("news") or result.get("events") or {},
        "sentiment": result.get("sentiment") or {},
        "fundamentals": result.get("fundamentals") or result.get("financials") or {},
        "factors": result.get("factors") or {},
        "signals": result.get("signals") or result.get("strategy_signals") or [],
        "orders": result.get("orders") or [],
        "fills": fills,
        "positions": positions,
        "account": {
            "metrics": result.get("metrics") or {},
            "equity_curve": curves.get("equity") or curves.get("daily") or [],
        },
        "queue_lag": 0,
        "engine_type": engine_type,
        "replay": {
            "contract": "pxybacktest.replay.v1",
            "mode": replay_mode,
            "source": replay_source,
            "execution_stream": "final_result_projection",
            "availability_time_enforced": True,
        },
        "adapter": diagnostics.get("adapter"),
        "audit": result.get("replay_audit"),
    }
    _emit(event_queue, "execution_snapshot", {"snapshot": snapshot})


def _replay_non_cta_result(
    event_queue,
    command_queue,
    *,
    task_id: str,
    request: dict[str, Any],
    result: dict[str, Any],
    engine_type: str,
    deferred_commands: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """用统一 EventCursor/ReplayClock 回放适配器事件，并写回可持久化快照。"""

    from app.replay import ResultReplayController

    replay_events = result.pop("_replay_events", None)
    if not isinstance(replay_events, list) or not replay_events:
        _emit_result_execution_snapshot(
            event_queue,
            task_id=task_id,
            request=request,
            result=result,
            engine_type=engine_type,
        )
        result["complete"] = True
        result["termination_reason"] = "completed"
        return {
            "complete": True,
            "termination_reason": "completed",
            "processed_events": 0,
            "remaining_events": 0,
            "total_events": 0,
            "execution_snapshot": {},
            "replay_audit": result.get("replay_audit") or {},
        }

    task = dict(request.get("_task_contract") or {})
    execution = dict(task.get("execution") or {})
    snapshot_ref = dict((task.get("data") or {}).get("snapshot") or {})
    speed = float(execution.get("speed") or request.get("speed") or 50)
    mode = str(execution.get("execution_mode") or "visual")
    pending = deferred_commands if deferred_commands is not None else []
    original_audit = result.get("replay_audit") or {}
    complete_stream = int(original_audit.get("event_count") or 0) == len(replay_events)
    replay_source = (
        "DAA"
        if engine_type in DAA_ENGINE_TYPES
        else "tqsdk-native"
        if engine_type in TQSDK_ENGINE_TYPES
        else "PXYDATA"
    )

    def read_commands() -> list[dict[str, Any]]:
        commands = list(pending)
        pending.clear()
        try:
            while True:
                command = command_queue.get_nowait()
                if isinstance(command, dict):
                    commands.append(command)
        except queue.Empty:
            pass
        return commands

    def emit_state(state: dict[str, Any]) -> None:
        payload = {"phase": "replaying_events", **state}
        _emit(
            event_queue,
            "state",
            payload,
            reliable=payload.get("status") in {"paused", "running"},
        )

    def emit_snapshot(snapshot: dict[str, Any]) -> None:
        replay = dict(snapshot.get("replay") or {})
        processed = int(replay.get("processed_events") or 0)
        total = max(1, len(replay_events))
        replay.update(
            {
                "contract": "pxybacktest.replay.v1",
                "source": replay_source,
                "execution_stream": (
                    "complete_ordered_audited"
                    if complete_stream
                    else "ordered_audited_projection"
                ),
                "availability_time_enforced": True,
            }
        )
        snapshot["replay"] = replay
        snapshot["engine_type"] = engine_type
        _emit(event_queue, "execution_snapshot", {"snapshot": snapshot})
        _emit(
            event_queue,
            "state",
            {
                "status": snapshot.get("status") or "running",
                "phase": "replaying_events",
                "progress": min(99.0, 10.0 + processed / total * 89.0),
                "processed_events": processed,
                "total_events": len(replay_events),
                "current_datetime": snapshot.get("simulated_at"),
                "speed": replay.get("speed"),
                "replay": replay,
            },
        )

    controller = ResultReplayController(
        run_id=task_id,
        snapshot_id=str(snapshot_ref.get("snapshot_id") or task_id),
        events=replay_events,
        mode=mode,
        speed=speed,
    )
    outcome = controller.run(
        read_commands=read_commands,
        on_snapshot=emit_snapshot,
        on_state=emit_state,
    )
    snapshot = dict(outcome["execution_snapshot"])
    snapshot["engine_type"] = engine_type
    replay = dict(snapshot.get("replay") or {})
    replay.update(
        {
            "contract": "pxybacktest.replay.v1",
            "source": replay_source,
            "execution_stream": (
                "complete_ordered_audited"
                if complete_stream
                else "ordered_audited_projection"
            ),
            "availability_time_enforced": True,
        }
    )
    snapshot["replay"] = replay
    result["complete"] = bool(outcome["complete"])
    result["termination_reason"] = str(outcome["termination_reason"])
    result["execution_snapshot"] = snapshot
    result["replay_audit"] = outcome["replay_audit"]
    diagnostics = dict(result.get("diagnostics") or {})
    diagnostics["replay"] = {
        "processed_events": outcome["processed_events"],
        "remaining_events": outcome["remaining_events"],
        "total_events": outcome["total_events"],
        "execution_stream": replay["execution_stream"],
    }
    result["diagnostics"] = diagnostics

    if not outcome["complete"]:
        _apply_partial_execution_result(result, task=task, snapshot=snapshot)

    reproducibility = dict(result.get("reproducibility") or {})
    reproducibility.pop("result_sha256", None)
    reproducibility["event_log_sha256"] = outcome["replay_audit"].get(
        "chain_sha256"
    )
    reproducibility["event_count"] = int(
        outcome["replay_audit"].get("event_count") or 0
    )
    result["reproducibility"] = reproducibility
    reproducibility["result_sha256"] = _stable_hash(result)
    return outcome


def _apply_partial_execution_result(
    result: dict[str, Any],
    *,
    task: dict[str, Any],
    snapshot: dict[str, Any],
) -> None:
    """取消时移除尚未回放的最终结果，只保留已执行区间。"""

    fills = list(snapshot.get("fills") or [])
    orders = list(snapshot.get("orders") or [])
    positions = list(dict(snapshot.get("positions") or {}).values())
    account_curve = list(snapshot.get("account_curve") or [])
    bar_history = list(snapshot.get("bar_history") or [])
    execution = dict(task.get("execution") or {})
    initial_capital = float(execution.get("capital") or 1_000_000)
    latest_account = dict(snapshot.get("account") or {})
    final_equity = latest_account.get("value")
    if final_equity is None:
        final_equity = latest_account.get("equity")
    if final_equity is None:
        realized = sum(
            float(
                item.get("pnl_amount")
                or item.get("net_pnl")
                or item.get("pnl")
                or 0.0
            )
            for item in fills
            if isinstance(item, dict)
        )
        final_equity = initial_capital + realized
    final_equity = float(final_equity)
    result["orders"] = orders
    result["deals"] = fills
    result["positions"] = positions
    result["curves"] = {"equity": account_curve}
    result["market"] = {"bars": bar_history}
    result["order_books"] = snapshot.get("order_books") or {}
    result["sentiment"] = snapshot.get("sentiment") or {}
    result["news"] = snapshot.get("news") or {}
    result["factors"] = snapshot.get("factors") or {}
    result["signals"] = snapshot.get("signals") or []
    result["metrics"] = {
        "initial_capital": initial_capital,
        "final_equity": final_equity,
        "net_profit": final_equity - initial_capital,
        "total_return": final_equity / initial_capital - 1.0,
        "n_trades": len(fills),
        "partial": True,
    }


def run_backtest_worker(
    task_id: str,
    request: dict[str, Any],
    pxylh_root: str,
    result_path: str,
    render_interval_ms: int,
    event_queue,
    command_queue,
    daa_root: str | None = None,
) -> None:
    startup_started_at = time.perf_counter()
    data_provenance = _configure_pxylh_cta_worker_environment()
    _emit(event_queue, "state", {"status": "running", "phase": "loading_runtime"})
    backend_root = Path(pxylh_root).resolve() / "backend"
    if not backend_root.is_dir():
        _emit(
            event_queue,
            "failed",
            {"error": f"PXYLH backend directory does not exist: {backend_root}"},
            terminal=True,
        )
        return
    if str(backend_root) not in sys.path:
        sys.path.insert(0, str(backend_root))

    try:
        from services.backtest_service.engine_runner import run_backtest_sync
        from services.backtest_service.kline_loader import ensure_backtest_kline_data
        from services.backtest_service.models import BacktestTask, convert_numpy_types
        from services.backtest_service.replay_runtime_utils import (
            build_replay_state,
            extract_live_orders,
            extract_live_positions,
            extract_live_trades,
        )
        from services.backtest_service.strategy_line_utils import extract_strategy_lines
        from services.backtest_service.time_utils import (
            parse_backtest_end_to_beijing_naive,
            parse_to_beijing_naive,
        )
        from datetime import datetime, timezone
    except Exception as exc:
        _emit(
            event_queue,
            "failed",
            {
                "error": f"unable to load PXYLH backtest runtime: {type(exc).__name__}: {exc}"
            },
            terminal=True,
        )
        return

    task = BacktestTask(
        task_id=task_id,
        user_id="workstation",
        strategy_class=str(request["strategy_class"]),
        vt_symbol=str(request["vt_symbol"]),
        interval=str(request.get("interval") or "1h"),
        start_time=parse_to_beijing_naive(str(request["start_time"])),
        end_time=parse_backtest_end_to_beijing_naive(str(request["end_time"])),
        parameters=dict(request.get("parameters") or {}),
        capital=float(request.get("capital") or 1_000_000),
        rate=_parse_request_rate(request),
        slippage=float(request.get("slippage") or 0),
        speed=float(request.get("speed") or 50),
        mode=str(request.get("mode") or "BAR").upper(),
        execution_mode=str(request.get("execution_mode") or "visual").lower(),
    )
    # PXYLH 任务内部统一使用北京时间 naive，但 MT5 Tester 的 FromDate /
    # ToDate 必须保留请求原始时区。将明确带偏移的值传给 MT5 快照加载器，
    # naive 旧请求按 UTC 解释，禁止在下游再次猜测时区。
    def _mt5_request_bound(raw: str) -> datetime:
        value = datetime.fromisoformat(str(raw).strip().replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    task._mt5_tester_start = _mt5_request_bound(str(request["start_time"]))
    task._mt5_tester_end = _mt5_request_bound(str(request["end_time"]))
    task_contract = request.get("_task_contract") or {}
    # CTA AI bridge 只从 DAA 版本化目录读取策略；把身份和根目录绑定到
    # 当前 task，供 PXYLH engine_runner 的动态加载门禁使用。
    task._task_contract = task_contract
    if daa_root:
        import os
        os.environ["PXYBACKTEST_DAA_ROOT"] = str(daa_root)
    snapshot_ref = ((task_contract.get("data") or {}).get("snapshot") or {})
    execution_snapshot_id = str(snapshot_ref.get("snapshot_id") or task_id)
    from app.replay import ReplayAudit

    replay_audit = ReplayAudit(run_id=task_id, snapshot_id=execution_snapshot_id)

    holder: dict[str, Any] = {}
    cancelled = False
    worker_status = "running"
    requested_paused = False
    requested_speed = float(task.speed)
    runtime_loaded_ms = round((time.perf_counter() - startup_started_at) * 1000)

    # 策略线程完整处理每个 Tick；这里只对发往浏览器的 bar 投影限帧。
    # 这样不会改变成交和结果，只会减少伪 Tick 造成的 UI 事件洪峰。
    visual_projection_gate = None

    def visual_interval_ms(speed: float) -> int:
        """按速度收紧显示帧门控，避免高倍速被固定 33ms 封顶。

        引擎仍完整处理每个 Tick；这里仅控制浏览器投影频率。低于约
        30x 时保留部署配置，高于该速度时按 1000/speed 收紧，最低 8ms
        以避免无界 UI 事件洪峰。
        """
        configured = max(16, int(render_interval_ms or 33))
        requested = max(1.0, min(100.0, float(speed or 1.0)))
        return max(8, min(configured, int(round(1000.0 / requested))))

    if str(getattr(task, "execution_mode", "visual")).lower() == "visual":
        from app.replay import VisualProjectionGate

        visual_projection_gate = VisualProjectionGate(
            interval_ms=visual_interval_ms(task.speed)
        )

    def emit_runtime_event(event_type: str, payload: dict[str, Any]) -> bool:
        replay_audit.record(event_type, payload)
        if visual_projection_gate is None or event_type != "bar":
            return _emit(event_queue, event_type, payload, reliable=True)
        # 普通 Tick 可以按显示帧合并，但每根 K 线的最终状态必须保留。
        # 否则下一分钟的开盘 Tick 会覆盖上一分钟尚未发出的收盘帧，
        # 页面会同时出现分钟缺口和大量 OHLC 相等的“横线”。
        pending = visual_projection_gate.offer(
            (event_type, dict(payload)),
            force=_is_completed_visual_bar(task),
        )
        if pending is None:
            return True
        projected_type, projected_payload = pending
        return _emit(
            event_queue,
            str(projected_type),
            {
                **dict(projected_payload),
                # 前端只显示投影帧，不应把省略的伪 Tick 误判为回放断裂。
                "coalesced": True,
                "projection": "visual_frame",
            },
            reliable=True,
        )

    def execute() -> None:
        try:
            _emit(
                event_queue,
                "state",
                {"phase": "loading_data", "runtime_loaded_ms": runtime_loaded_ms},
            )
            asyncio.run(ensure_backtest_kline_data(task))
            _emit(
                event_queue,
                "state",
                {
                    "phase": "starting_engine",
                    "startup_elapsed_ms": round(
                        (time.perf_counter() - startup_started_at) * 1000
                    ),
                },
            )
            raw_result = run_backtest_sync(
                task,
                None,
                event_sink=emit_runtime_event,
            )
            if isinstance(raw_result, dict):
                raw_result["data_provenance"] = dict(data_provenance)
            holder["result"] = raw_result
        except BaseException as exc:
            holder["error"] = f"{type(exc).__name__}: {exc}"
            holder["traceback"] = traceback.format_exc()

    thread = threading.Thread(
        target=execute, name=f"backtest-{task_id[:8]}", daemon=True
    )
    thread.start()

    _emit(event_queue, "state", {"status": "running", "progress": 0.0})
    last_replay_seq = -1
    snapshot_hashes: dict[str, str] = {}
    emitted_trade_count = 0
    applied_speed: float | None = None
    render_interval = max(0.033, render_interval_ms / 1000)
    last_snapshot_at = 0.0
    last_replay_snapshot_at = 0.0

    while thread.is_alive():
        try:
            while True:
                command = command_queue.get_nowait()
                action = str(command.get("action") or "")
                if action == "pause":
                    requested_paused = True
                elif action == "resume":
                    requested_paused = False
                elif action == "speed":
                    requested_speed = max(
                        1.0, min(100.0, float(command.get("speed") or 1))
                    )
                    task.speed = requested_speed
                    applied_speed = None
                    if visual_projection_gate is not None:
                        visual_projection_gate.set_interval_ms(
                            visual_interval_ms(requested_speed)
                        )
                elif action == "step":
                    requested_paused = True
                    engine = task.engine
                    if engine and hasattr(engine, "step"):
                        engine.step()
                elif action == "cancel":
                    cancelled = True
                    requested_paused = False
                    engine = task.engine
                    if engine and hasattr(engine, "cancel"):
                        engine.cancel()
        except queue.Empty:
            pass

        engine = task.engine
        if engine:
            if (
                cancelled
                and not bool(getattr(engine, "is_cancelled", False))
                and hasattr(engine, "cancel")
            ):
                engine.cancel()
            if applied_speed != requested_speed and hasattr(engine, "set_speed"):
                engine.set_speed(requested_speed)
                applied_speed = requested_speed
                _emit(event_queue, "state", {"speed": requested_speed})

            engine_paused = bool(getattr(engine, "is_paused", False))
            if requested_paused and not engine_paused and hasattr(engine, "pause"):
                engine.pause()
                worker_status = "paused"
                _emit(event_queue, "state", {"status": "paused"}, reliable=True)
            elif not requested_paused and engine_paused and hasattr(engine, "resume"):
                engine.resume()
                worker_status = "running"
                _emit(event_queue, "state", {"status": "running"}, reliable=True)

        replay_seq = int(getattr(task, "replay_seq", 0) or 0)
        if replay_seq != last_replay_seq:
            last_replay_seq = replay_seq
            replay = build_replay_state(task)
            _emit(
                event_queue,
                "state",
                {
                    "status": worker_status,
                    "progress": float(task.progress or 0),
                    "current_datetime": str(task.current_datetime or ""),
                    "processed_bars": int(task.processed_bars or 0),
                    "total_bars": int(task.total_bars or 0),
                    "speed": float(task.speed or 1),
                    "phase": "replaying",
                    "replay": replay,
                },
            )

        visual_trades = getattr(engine, "_visual_trades", None) if engine else None
        current_trades = (
            [dict(item) for item in visual_trades]
            if visual_trades is not None
            else extract_live_trades(task)
        )
        if len(current_trades) < emitted_trade_count:
            emitted_trade_count = 0
        for trade in current_trades[emitted_trade_count:]:
            _emit(event_queue, "trade", {"trade": trade}, reliable=True)
        emitted_trade_count = len(current_trades)

        now = time.monotonic()
        if now - last_snapshot_at >= 0.25:
            last_snapshot_at = now
            snapshots = {
                "trades": extract_live_trades(task),
                "orders": extract_live_orders(task),
                "positions": extract_live_positions(task),
                "strategy_lines": extract_strategy_lines(task),
            }
            for name, items in snapshots.items():
                digest = _stable_hash(items)
                if digest != snapshot_hashes.get(name):
                    snapshot_hashes[name] = digest
                    _emit(event_queue, f"{name}_snapshot", {"items": items})

            execution_snapshot = {
                "contract_version": "pxybacktest.replay.v1",
                "run_id": task.task_id,
                "snapshot_id": execution_snapshot_id,
                "event_seq": int(getattr(task, "replay_seq", 0) or 0),
                "simulated_at": str(task.current_datetime or ""),
                "bars": (
                    {str(task.vt_symbol): dict(task.current_bar)}
                    if isinstance(getattr(task, "current_bar", None), dict)
                    else {}
                ),
                "orders": snapshots["orders"],
                "fills": snapshots["trades"],
                "positions": snapshots["positions"],
                "strategy_lines": snapshots["strategy_lines"],
                "queue_lag": 0,
                "replay": replay,
                "audit": replay_audit.to_dict(),
            }
            execution_digest = _stable_hash(execution_snapshot)
            if execution_digest != snapshot_hashes.get("execution"):
                snapshot_hashes["execution"] = execution_digest
                _emit(
                    event_queue,
                    "execution_snapshot",
                    {"snapshot": execution_snapshot},
                )

        if now - last_replay_snapshot_at >= 1.0:
            last_replay_snapshot_at = now
            replay_events = build_replay_event_snapshot(task.replay_events)
            digest = _stable_hash(replay_events)
            if digest != snapshot_hashes.get("replay_events"):
                snapshot_hashes["replay_events"] = digest
                _emit(
                    event_queue,
                    "replay_events_snapshot",
                    {"items": replay_events},
                )

        time.sleep(render_interval)

    thread.join(timeout=1.0)
    if visual_projection_gate is not None:
        pending = visual_projection_gate.flush()
        if pending is not None:
            projected_type, projected_payload = pending
            _emit(
                event_queue,
                str(projected_type),
                {
                    **dict(projected_payload),
                    "coalesced": True,
                    "projection": "visual_frame",
                },
                reliable=True,
            )
    if "error" in holder:
        partial_result = {
            "complete": False,
            "termination_reason": "failed",
            "error": holder["error"],
            "trades": extract_live_trades(task),
            "orders": extract_live_orders(task),
            "positions": extract_live_positions(task),
            "strategy_lines": extract_strategy_lines(task),
            "replay_audit": replay_audit.to_dict(),
            "progress": float(task.progress or 0.0),
            "processed_bars": int(task.processed_bars or 0),
            "total_bars": int(task.total_bars or 0),
            "current_datetime": str(task.current_datetime or ""),
            "data_provenance": dict(data_provenance),
        }
        if request.get("_task_contract"):
            from app.result_contract import build_result_v2

            partial_result = build_result_v2(
                task_id=task_id,
                request=request,
                raw_result=partial_result,
            )
        final_path = Path(result_path)
        _atomic_json_write(final_path, convert_numpy_types(partial_result))
        _emit(
            event_queue,
            "failed",
            {
                "error": holder["error"],
                "result_path": str(final_path),
                "result_available": True,
                "progress": float(task.progress or 0.0),
            },
            terminal=True,
        )
        return

    result = convert_numpy_types(holder.get("result") or {})
    result.update(
        {
            "replay_audit": replay_audit.to_dict(),
            "complete": not cancelled,
            "termination_reason": "cancelled" if cancelled else "completed",
            "progress": float(task.progress or 0.0),
            "processed_bars": int(task.processed_bars or 0),
            "total_bars": int(task.total_bars or 0),
            "current_datetime": str(task.current_datetime or ""),
        }
    )
    if request.get("_task_contract"):
        from app.result_contract import build_result_v2

        result = build_result_v2(task_id=task_id, request=request, raw_result=result)
    final_path = Path(result_path)
    _atomic_json_write(final_path, result)
    if cancelled:
        _emit(
            event_queue,
            "cancelled",
            {
                "result_path": str(final_path),
                "result_available": True,
                "progress": float(task.progress or 0.0),
            },
            terminal=True,
        )
        return
    _emit(
        event_queue,
        "completed",
        {"result_path": str(final_path), "progress": 100.0},
        terminal=True,
    )
