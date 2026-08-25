from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
RELIABLE_EVENT_TIMEOUT_SECONDS = 5.0


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
            try:
                while True:
                    command_message = command_queue.get_nowait()
                    if str(command_message.get("action") or "") == "cancel":
                        cancelled = True
            except queue.Empty:
                pass
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
        _atomic_json_write(final_path, result)
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
    run_backtest_worker(
        task_id,
        request,
        pxylh_root,
        result_path,
        render_interval_ms,
        event_queue,
        command_queue,
    )


def run_microstructure_worker(
    task_id: str,
    request: dict[str, Any],
    pxydata_root: str,
    result_path: str,
    event_queue,
    command_queue,
) -> None:
    """在隔离 worker 中执行 manifest-bound 真实 Tick 回放。"""
    cancelled = False
    try:
        def cancel_requested() -> bool:
            nonlocal cancelled
            try:
                while True:
                    command = command_queue.get_nowait()
                    if str(command.get("action") or "") == "cancel":
                        cancelled = True
            except queue.Empty:
                pass
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
            )

        result = run_task_optimization(
            task,
            evaluate,
            cancel_check=cancel_requested,
        )
        _atomic_json_write(Path(result_path), result)
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
    try:
        def cancel_requested() -> bool:
            nonlocal cancelled
            try:
                while True:
                    command = command_queue.get_nowait()
                    if str(command.get("action") or "") == "cancel":
                        cancelled = True
            except queue.Empty:
                pass
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
        _atomic_json_write(Path(result_path), result)
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
    try:
        task = request.get("_task_contract")
        manifest = request.get("_snapshot_manifest")
        if not isinstance(task, dict) or not isinstance(manifest, dict):
            raise ValueError("Lighter worker 缺少任务契约或完整快照清单")
        try:
            while True:
                command = command_queue.get_nowait()
                if str(command.get("action") or "") == "cancel":
                    cancelled = True
        except queue.Empty:
            pass
        if cancelled:
            _emit(event_queue, "cancelled", {}, terminal=True)
            return
        _emit(event_queue, "state", {"status": "running", "phase": "rebuilding_lighter_book", "progress": 5.0})
        from app.lighter_microstructure import run_lighter_backtest
        result = run_lighter_backtest(task_id=task_id, task=task, manifest=manifest, data_root=pxydata_root)
        _atomic_json_write(Path(result_path), result)
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


def run_backtest_worker(
    task_id: str,
    request: dict[str, Any],
    pxylh_root: str,
    result_path: str,
    render_interval_ms: int,
    event_queue,
    command_queue,
) -> None:
    startup_started_at = time.perf_counter()
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
        rate=float(request.get("rate") or 0.0004),
        slippage=float(request.get("slippage") or 0),
        speed=float(request.get("speed") or 50),
        mode=str(request.get("mode") or "BAR").upper(),
        execution_mode=str(request.get("execution_mode") or "visual").lower(),
    )

    holder: dict[str, Any] = {}
    cancelled = False
    worker_status = "running"
    requested_paused = False
    requested_speed = float(task.speed)
    runtime_loaded_ms = round((time.perf_counter() - startup_started_at) * 1000)

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
            holder["result"] = run_backtest_sync(
                task,
                None,
                event_sink=lambda event_type, payload: _emit(
                    event_queue, event_type, payload, reliable=True
                ),
            )
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
    if "error" in holder:
        _emit(
            event_queue,
            "failed",
            {"error": holder["error"]},
            terminal=True,
        )
        return

    result = convert_numpy_types(holder.get("result") or {})
    result.update(
        {
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
