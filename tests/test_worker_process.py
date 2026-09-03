import json
import logging
import queue
from pathlib import Path

import pytest

from app import worker_process
from app.worker_process import (
    _configure_backtest_worker_logging,
    _emit,
    _emit_result_execution_snapshot,
    _is_completed_visual_bar,
    _parse_request_rate,
    _replay_non_cta_result,
    build_replay_event_snapshot,
    run_a_share_worker,
    run_lighter_worker,
    run_microstructure_worker,
)


def test_completed_visual_bar_detection_only_accepts_final_frame() -> None:
    class Task:
        replay_bar_progress = 0.998

    task = Task()
    assert _is_completed_visual_bar(task) is False

    task.replay_bar_progress = 0.999
    assert _is_completed_visual_bar(task) is True

    task.replay_bar_progress = 1.0
    assert _is_completed_visual_bar(task) is True


def test_parse_request_rate_preserves_explicit_zero() -> None:
    assert _parse_request_rate({"rate": 0}) == 0.0
    assert _parse_request_rate({"rate": 0.0}) == 0.0
    assert _parse_request_rate({}) == 0.0004
    assert _parse_request_rate({"rate": None}) == 0.0004


def test_result_execution_snapshot_projects_non_cta_result() -> None:
    events: list[dict] = []

    class EventQueue:
        def put_nowait(self, item: dict) -> None:
            events.append(item)

    request = {
        "_task_contract": {
            "engine_type": "factor_matrix",
            "period": {"end": "2026-08-01T00:00:00Z"},
            "data": {
                "snapshot": {
                    "snapshot_id": "btsnap_v1_" + "a" * 32,
                }
            },
        }
    }
    _emit_result_execution_snapshot(
        EventQueue(),
        task_id="task-1",
        request=request,
        result={
            "metrics": {"total_return": 0.1},
            "curves": {"equity": [{"date": "2026-08-01", "equity": 1001}]},
            "deals": [{"trade_id": "d1"}],
            "factors": {"factor-v1": {"value": 1.2}},
            "bars": [{"symbol": "600000.SH", "close": 10.2}],
            "order_books": [{"symbol": "600000.SH", "bid_price1": 10.1}],
            "positions": [{"symbol": "600000.SH", "quantity": 100}],
            "fundamentals": {"report-1": {"eps": 1.2}},
            "signals": [{"symbol": "600000.SH", "side": "buy"}],
        },
        engine_type="factor_matrix",
    )
    assert events[0]["type"] == "execution_snapshot"
    snapshot = events[0]["data"]["snapshot"]
    assert snapshot["engine_type"] == "factor_matrix"
    assert snapshot["factors"]["factor-v1"]["value"] == 1.2
    assert snapshot["fills"] == [{"trade_id": "d1"}]
    assert snapshot["bars"]["600000.SH"]["close"] == 10.2
    assert snapshot["order_books"]["600000.SH"]["bid_price1"] == 10.1
    assert snapshot["positions"]["600000.SH"]["quantity"] == 100
    assert snapshot["fundamentals"]["report-1"]["eps"] == 1.2
    assert snapshot["signals"][0]["side"] == "buy"
    assert snapshot["replay"]["mode"] == "bar"
    assert snapshot["replay"]["availability_time_enforced"] is True


def test_non_cta_replay_uses_event_cursor_and_removes_private_event_tape() -> None:
    event_queue: queue.Queue = queue.Queue()
    command_queue: queue.Queue = queue.Queue()
    snapshot_id = "btsnap_v1_" + "b" * 32
    result = {
        "metrics": {"total_return": 0.1},
        "_replay_events": [
            {
                "event_type": "market_bar",
                "event_time": "2026-08-01",
                "symbol": "600000.SH",
                "payload": {
                    "symbol": "600000.SH",
                    "datetime": "2026-08-01",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                },
            },
            {
                "event_type": "account",
                "event_time": "2026-08-01",
                "payload": {"date": "2026-08-01", "value": 1_010_000},
            },
        ],
        "replay_audit": {"event_count": 2},
    }
    request = {
        "_task_contract": {
            "engine_type": "factor_matrix",
            "execution": {"capital": 1_000_000, "speed": 100, "execution_mode": "fast"},
            "data": {"snapshot": {"snapshot_id": snapshot_id}},
        }
    }

    outcome = _replay_non_cta_result(
        event_queue,
        command_queue,
        task_id="task-replay",
        request=request,
        result=result,
        engine_type="factor_matrix",
    )

    assert outcome["complete"] is True
    assert "_replay_events" not in result
    assert result["execution_snapshot"]["bar_history_count"] == 1
    assert result["execution_snapshot"]["account_curve_count"] == 1
    assert result["replay_audit"]["event_count"] == 2


def test_non_cta_cancel_saves_only_processed_execution_snapshot() -> None:
    event_queue: queue.Queue = queue.Queue()

    class DelayedCancelQueue:
        calls = 0

        def get_nowait(self) -> dict:
            self.calls += 1
            if self.calls == 2:
                return {"action": "cancel"}
            raise queue.Empty

    result = {
        "metrics": {"total_return": 0.99},
        "curves": {"equity": [{"date": "future", "value": 1_990_000}]},
        "_replay_events": [
            {
                "event_type": "account",
                "event_time": f"2026-08-0{day}",
                "payload": {"date": f"2026-08-0{day}", "value": 1_000_000 + day * 1_000},
            }
            for day in range(1, 4)
        ],
        "replay_audit": {"event_count": 3},
    }
    request = {
        "_task_contract": {
            "engine_type": "event_sentiment",
            "execution": {"capital": 1_000_000, "speed": 100, "execution_mode": "fast"},
            "data": {"snapshot": {"snapshot_id": "btsnap_v1_" + "c" * 32}},
        }
    }

    outcome = _replay_non_cta_result(
        event_queue,
        DelayedCancelQueue(),
        task_id="task-partial",
        request=request,
        result=result,
        engine_type="event_sentiment",
    )

    assert outcome["complete"] is False
    assert outcome["processed_events"] == 1
    assert result["complete"] is False
    assert result["metrics"]["partial"] is True
    assert result["metrics"]["final_equity"] == 1_001_000
    assert result["curves"]["equity"] == [
        {"date": "2026-08-01", "value": 1_001_000}
    ]


def test_emit_reliable_event_fails_closed_when_queue_is_full() -> None:
    events: queue.Queue = queue.Queue(maxsize=1)
    events.put({"type": "existing", "data": {}})

    with pytest.raises(RuntimeError, match="可靠回测事件投递失败"):
        _emit(events, "trade", {"trade": {"id": "t1"}}, reliable=True)


def test_emit_lossy_event_records_queue_drop(caplog: pytest.LogCaptureFixture) -> None:
    events: queue.Queue = queue.Queue(maxsize=1)
    events.put({"type": "existing", "data": {}})

    with caplog.at_level(logging.WARNING, logger="backtest_service"):
        assert _emit(events, "state", {"progress": 1.0}) is False

    assert "非可靠回测事件因队列已满被降采样" in caplog.text


def test_replay_event_snapshot_is_bounded_to_twenty_items() -> None:
    events = [{"seq": index} for index in range(100)]

    snapshot = build_replay_event_snapshot(events)

    assert len(snapshot) == 20
    assert snapshot[0]["seq"] == 80
    assert snapshot[-1]["seq"] == 99


def test_backtest_worker_logging_emits_info_once(capsys) -> None:
    logger = logging.getLogger("backtest_service")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    try:
        logger.handlers.clear()
        _configure_backtest_worker_logging()
        _configure_backtest_worker_logging()

        logger.info("回测优先加载 PXYDATA 1m K线")

        assert len(logger.handlers) == 1
        assert "回测优先加载 PXYDATA 1m K线" in capsys.readouterr().err
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate


def _a_share_worker_request() -> dict:
    return {
        "_task_contract": {
            "schema_version": 2,
            "engine_type": "a_share_portfolio",
            "strategy": {
                "id": "boll_breakout",
                "version": "builtin",
                "source_hash": "a" * 64,
                "entrypoint": "boll_breakout",
            },
            "universe": {"symbols": ["600000.SH"]},
            "period": {"start": "2026-01-01", "end": "2026-02-01"},
            "data": {
                "snapshot": {
                    "snapshot_id": "btsnap_v1_" + "a" * 32,
                    "manifest_sha256": "b" * 64,
                    "warnings": [],
                    "quality_accepted": True,
                }
            },
            "execution": {"capital": 100000},
            "parameters": {},
        },
        "_snapshot_manifest": {
            "snapshot_id": "btsnap_v1_" + "a" * 32,
            "manifest_sha256": "b" * 64,
        },
    }


def _adapter_runtime(tmp_path: Path) -> tuple[Path, Path]:
    daa_root = tmp_path / "DAA"
    python = daa_root / "backend" / ".venv" / "Scripts" / "python.exe"
    adapter = daa_root / "backend" / "app" / "backtest" / "pxy_adapter.py"
    python.parent.mkdir(parents=True)
    adapter.parent.mkdir(parents=True)
    python.write_bytes(b"test")
    adapter.write_text("# test", encoding="utf-8")
    data_root = tmp_path / "PXYDATA" / "data"
    data_root.mkdir(parents=True)
    return daa_root, data_root


def test_a_share_worker_maps_successful_subprocess_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    daa_root, data_root = _adapter_runtime(tmp_path)

    class CompletedProcess:
        def poll(self) -> int:
            return 0

    def fake_popen(command: list[str], **_kwargs) -> CompletedProcess:
        result_path = Path(command[command.index("--result") + 1])
        result_path.write_text(
            json.dumps(
                {
                    "success": True,
                    "result": {
                        "stats": {
                            "total_return": 0.1,
                            "adapter": {
                                "contract_version": "pxybacktest.engine-adapter.a-share.v1",
                                "snapshot_enforcement": "manifest_bound",
                                "price_adjustment": "none",
                                "corporate_actions_applied": False,
                                "worker_version": "daa.a-share-adapter.v1",
                                "loaded_rows": 10,
                            },
                        },
                        "trades": [],
                    },
                }
            ),
            encoding="utf-8",
        )
        return CompletedProcess()

    monkeypatch.setattr(worker_process.subprocess, "Popen", fake_popen)
    event_queue: queue.Queue = queue.Queue()
    result_path = tmp_path / "result" / "result.json"
    run_a_share_worker(
        "task-a",
        _a_share_worker_request(),
        str(daa_root),
        str(data_root),
        str(result_path),
        str(tmp_path / "jobs" / "task-a"),
        event_queue,
        queue.Queue(),
    )

    result = json.loads(result_path.read_text(encoding="utf-8"))
    events = list(event_queue.queue)
    assert result["metrics"]["total_return"] == 0.1
    assert result["diagnostics"]["snapshot_enforcement"] == "manifest_bound"
    assert events[-1]["type"] == "completed"


def test_a_share_worker_terminates_subprocess_on_cancel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    daa_root, data_root = _adapter_runtime(tmp_path)

    class RunningProcess:
        def __init__(self) -> None:
            self.return_code: int | None = None
            self.terminated = False

        def poll(self) -> int | None:
            return self.return_code

        def terminate(self) -> None:
            self.terminated = True
            self.return_code = 0

        def wait(self, timeout: float) -> int:
            del timeout
            return int(self.return_code or 0)

        def kill(self) -> None:
            self.return_code = -9

    process = RunningProcess()
    monkeypatch.setattr(
        worker_process.subprocess,
        "Popen",
        lambda *_args, **_kwargs: process,
    )
    commands: queue.Queue = queue.Queue()
    commands.put({"action": "cancel"})
    events: queue.Queue = queue.Queue()

    run_a_share_worker(
        "task-a",
        _a_share_worker_request(),
        str(daa_root),
        str(data_root),
        str(tmp_path / "result.json"),
        str(tmp_path / "jobs" / "task-a"),
        events,
        commands,
    )

    assert process.terminated is True
    assert list(events.queue)[-1]["type"] == "cancelled"


def _install_recording_adapter(
    monkeypatch: pytest.MonkeyPatch,
    recorded_payloads: list[dict],
) -> None:
    class CompletedProcess:
        def poll(self) -> int:
            return 0

    def fake_popen(command: list[str], **_kwargs) -> CompletedProcess:
        request_path = Path(command[command.index("--request") + 1])
        result_path = Path(command[command.index("--result") + 1])
        payload = json.loads(request_path.read_text(encoding="utf-8"))
        recorded_payloads.append(payload)
        parameters = payload["task"]["parameters"]
        threshold = float(parameters.get("entry_threshold", 0.0))
        result_path.write_text(
            json.dumps(
                {
                    "success": True,
                    "result": {
                        "stats": {
                            "total_return": threshold,
                            "max_drawdown": -threshold / 10,
                            "adapter": {
                                "contract_version": "pxybacktest.engine-adapter.a-share.v1",
                                "snapshot_enforcement": "manifest_bound",
                                "price_adjustment": "none",
                                "corporate_actions_applied": False,
                                "worker_version": "daa.a-share-adapter.v1",
                                "loaded_rows": 10,
                            },
                        },
                        "equity_curve": [
                            {
                                "date": payload["task"]["period"]["end"][:10],
                                "value": 1 + threshold,
                            }
                        ],
                        "trades": [],
                    },
                }
            ),
            encoding="utf-8",
        )
        return CompletedProcess()

    monkeypatch.setattr(worker_process.subprocess, "Popen", fake_popen)


def test_a_share_worker_optuna_keeps_every_trial_on_same_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("optuna")
    daa_root, data_root = _adapter_runtime(tmp_path)
    request = _a_share_worker_request()
    request["_task_contract"]["optimization"] = {
        "method": "optuna",
        "search_space": {
            "entry_threshold": {"type": "float", "low": 0.1, "high": 0.3}
        },
        "objectives": [
            {"metric": "total_return", "direction": "maximize"},
            {"metric": "max_drawdown", "direction": "maximize"},
        ],
        "n_trials": 3,
        "sampler_seed": 7,
        "train_days": 20,
        "test_days": 5,
        "step_days": 5,
    }
    recorded: list[dict] = []
    _install_recording_adapter(monkeypatch, recorded)
    result_path = tmp_path / "result.json"

    run_a_share_worker(
        "task-optuna",
        request,
        str(daa_root),
        str(data_root),
        str(result_path),
        str(tmp_path / "jobs" / "task-optuna"),
        queue.Queue(),
        queue.Queue(),
    )

    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["optimization"]["method"] == "optuna"
    assert result["optimization"]["n_completed"] == 3
    assert result["optimization"]["pareto_front"]
    assert len(recorded) == 4  # 三次 trial 加一次代表性最优参数复跑
    assert all(item["manifest"] == request["_snapshot_manifest"] for item in recorded)
    assert all(
        item["task"]["data"] == request["_task_contract"]["data"]
        for item in recorded
    )
    assert all(item["task"]["optimization"] is None for item in recorded)


def test_a_share_worker_walk_forward_keeps_snapshot_and_oos_disjoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("optuna")
    daa_root, data_root = _adapter_runtime(tmp_path)
    request = _a_share_worker_request()
    request["_task_contract"]["period"] = {
        "start": "2026-01-01T00:00:00+08:00",
        "end": "2026-01-10T23:59:59+08:00",
    }
    request["_task_contract"]["optimization"] = {
        "method": "walk_forward",
        "search_space": {
            "entry_threshold": {"type": "float", "low": 0.1, "high": 0.3}
        },
        "objectives": [{"metric": "total_return", "direction": "maximize"}],
        "n_trials": 1,
        "sampler_seed": 7,
        "train_days": 4,
        "test_days": 3,
        "step_days": 3,
    }
    recorded: list[dict] = []
    _install_recording_adapter(monkeypatch, recorded)
    result_path = tmp_path / "result.json"

    run_a_share_worker(
        "task-walk-forward",
        request,
        str(daa_root),
        str(data_root),
        str(result_path),
        str(tmp_path / "jobs" / "task-walk-forward"),
        queue.Queue(),
        queue.Queue(),
    )

    result = json.loads(result_path.read_text(encoding="utf-8"))
    folds = result["optimization"]["folds"]
    assert result["optimization"]["method"] == "walk_forward"
    assert len(folds) == 2
    assert all(fold["train_end"] < fold["test_start"] for fold in folds)
    assert all(item["manifest"] == request["_snapshot_manifest"] for item in recorded)
    assert all(
        item["task"]["data"] == request["_task_contract"]["data"]
        for item in recorded
    )


def test_microstructure_worker_honors_cancel_before_optimization(tmp_path: Path) -> None:
    commands: queue.Queue = queue.Queue()
    commands.put({"action": "cancel"})
    events: queue.Queue = queue.Queue()

    run_microstructure_worker(
        "task-tick",
        {},
        str(tmp_path),
        str(tmp_path / "result.json"),
        events,
        commands,
    )

    assert list(events.queue)[-1]["type"] == "cancelled"
    assert not (tmp_path / "result.json").exists()


def test_lighter_worker_runs_the_common_optimizer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = {
        "engine_type": "lighter_microstructure",
        "data": {"snapshot": {"snapshot_id": "snap-lighter"}},
        "optimization": {"method": "optuna"},
    }
    request = {"_task_contract": task, "_snapshot_manifest": {"datasets": []}}
    calls: list[dict] = []

    def fake_backtest(*, task_id: str, task: dict, manifest: dict, data_root: str) -> dict:
        calls.append({"task_id": task_id, "task": task, "manifest": manifest, "data_root": data_root})
        return {"metrics": {"n_trades": 1, "total_return": 0.01}}

    def fake_optimizer(task: dict, evaluator, *, cancel_check=None) -> dict:
        assert task["optimization"]["method"] == "optuna"
        assert cancel_check is not None
        return {**evaluator(task), "optimization": {"method": "optuna"}}

    monkeypatch.setattr("app.lighter_microstructure.run_lighter_backtest", fake_backtest)
    monkeypatch.setattr("app.optimization.run_task_optimization", fake_optimizer)
    events: queue.Queue = queue.Queue()
    commands: queue.Queue = queue.Queue()
    result_path = tmp_path / "result.json"

    run_lighter_worker("task-lighter", request, str(tmp_path), str(result_path), events, commands)

    assert json.loads(result_path.read_text(encoding="utf-8"))["optimization"]["method"] == "optuna"
    assert len(calls) == 1
    assert list(events.queue)[-1]["type"] == "completed"
