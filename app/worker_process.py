from __future__ import annotations

import asyncio
import hashlib
import json
import queue
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any


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


def _emit(event_queue, event_type: str, data: dict[str, Any], *, terminal: bool = False) -> None:
    event = {"type": event_type, "data": data}
    try:
        if terminal:
            event_queue.put(event, timeout=2.0)
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
    backend_root = Path(pxylh_root).resolve() / "backend"
    if not backend_root.is_dir():
        _emit(
            event_queue,
            "failed",
            {"error": f"PXYLH backend directory does not exist: {backend_root}"},
            terminal=True,
        )
        return
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
        from services.backtest_service.time_utils import parse_to_beijing_naive
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
        end_time=parse_to_beijing_naive(str(request["end_time"])),
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

    def execute() -> None:
        try:
            asyncio.run(ensure_backtest_kline_data(task))
            holder["result"] = run_backtest_sync(task, None)
        except BaseException as exc:
            holder["error"] = f"{type(exc).__name__}: {exc}"
            holder["traceback"] = traceback.format_exc()

    thread = threading.Thread(target=execute, name=f"backtest-{task_id[:8]}", daemon=True)
    thread.start()

    _emit(event_queue, "state", {"status": "running", "progress": 0.0})
    last_replay_seq = -1
    last_bar_signature = ""
    snapshot_hashes: dict[str, str] = {}
    render_interval = max(0.033, render_interval_ms / 1000)
    last_snapshot_at = 0.0

    while thread.is_alive():
        try:
            while True:
                command = command_queue.get_nowait()
                action = str(command.get("action") or "")
                engine = task.engine
                if action == "pause" and engine and hasattr(engine, "pause"):
                    engine.pause()
                    worker_status = "paused"
                    _emit(event_queue, "state", {"status": "paused"})
                elif action == "resume" and engine and hasattr(engine, "resume"):
                    engine.resume()
                    worker_status = "running"
                    _emit(event_queue, "state", {"status": "running"})
                elif action == "speed" and engine and hasattr(engine, "set_speed"):
                    speed = max(1.0, min(100.0, float(command.get("speed") or 1)))
                    engine.set_speed(speed)
                    task.speed = speed
                    _emit(event_queue, "state", {"speed": speed})
                elif action == "cancel":
                    cancelled = True
                    if engine and hasattr(engine, "cancel"):
                        engine.cancel()
        except queue.Empty:
            pass

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
                    "replay": replay,
                },
            )
            bar = dict(task.current_bar or {})
            signature = _stable_hash(bar) if bar else ""
            if bar and signature != last_bar_signature:
                last_bar_signature = signature
                _emit(event_queue, "bar", {"bar": bar, "replay_seq": replay_seq})

        now = time.monotonic()
        if now - last_snapshot_at >= 0.25:
            last_snapshot_at = now
            snapshots = {
                "trades": extract_live_trades(task),
                "orders": extract_live_orders(task),
                "positions": extract_live_positions(task),
                "strategy_lines": extract_strategy_lines(task),
                "replay_events": list(task.replay_events[-120:]),
            }
            for name, items in snapshots.items():
                digest = _stable_hash(items)
                if digest != snapshot_hashes.get(name):
                    snapshot_hashes[name] = digest
                    _emit(event_queue, f"{name}_snapshot", {"items": items})

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
