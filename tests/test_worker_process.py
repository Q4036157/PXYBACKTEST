import json
import logging
import queue
from pathlib import Path

import pytest

from app import worker_process
from app.worker_process import (
    _configure_backtest_worker_logging,
    build_replay_event_snapshot,
    run_a_share_worker,
    run_lighter_worker,
    run_microstructure_worker,
)


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
