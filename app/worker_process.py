from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import queue
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any


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
    return list(events[-max(1, limit):])


def preload_pxylh_runtime(pxylh_root: str) -> None:
    backend_root = Path(pxylh_root).resolve() / "backend"
    if not backend_root.is_dir():
        raise FileNotFoundError(f"PXYLH backend directory does not exist: {backend_root}")
    backend_path = str(backend_root)
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)
    from services.backtest_service.engine_runner import run_backtest_sync  # noqa: F401
    from services.backtest_service.kline_loader import ensure_backtest_kline_data  # noqa: F401
    from services.backtest_service.models import BacktestTask, convert_numpy_types  # noqa: F401
    from services.backtest_service.replay_runtime_utils import (  # noqa: F401
        build_replay_state,
        extract_live_orders,
        extract_live_positions,
        extract_live_trades,
    )
    from services.backtest_service.strategy_line_utils import extract_strategy_lines  # noqa: F401


def run_preloaded_worker(
    pxylh_root: str,
    render_interval_ms: int,
    event_queue,
    command_queue,
    job_queue,
    ready_queue,
) -> None:
    _configure_backtest_worker_logging()
    try:
        preload_pxylh_runtime(pxylh_root)
        ready_queue.put({"ready": True})
    except BaseException as exc:
        ready_queue.put({"ready": False, "error": f"{type(exc).__name__}: {exc}"})
        return

    job = job_queue.get()
    if job is None:
        return
    task_id, request, result_path = job
    run_backtest_worker(
        task_id,
        request,
        pxylh_root,
        result_path,
        render_interval_ms,
        event_queue,
        command_queue,
    )


def _emit(
    event_queue,
    event_type: str,
    data: dict[str, Any],
    *,
    terminal: bool = False,
    reliable: bool = False,
) -> None:
    event = {"type": event_type, "data": data}
    try:
        if terminal or reliable:
            event_queue.put(event)
        else:
            event_queue.put_nowait(event)
    except queue.Full:
        if terminal:
            event_queue.put(event, timeout=5.0)


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
            {"error": f"unable to load PXYLH backtest runtime: {type(exc).__name__}: {exc}"},
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

    thread = threading.Thread(target=execute, name=f"backtest-{task_id[:8]}", daemon=True)
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
                    requested_speed = max(1.0, min(100.0, float(command.get("speed") or 1)))
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
    if cancelled:
        _emit(event_queue, "cancelled", {}, terminal=True)
        return
    if "error" in holder:
        _emit(
            event_queue,
            "failed",
            {"error": holder["error"]},
            terminal=True,
        )
        return

    result = convert_numpy_types(holder.get("result") or {})
    final_path = Path(result_path)
    _atomic_json_write(final_path, result)
    _emit(
        event_queue,
        "completed",
        {"result_path": str(final_path), "progress": 100.0},
        terminal=True,
    )
