from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from app import tqsdk_native_worker
from app.tqsdk_native_worker import (
    TqSdkWorkerError,
    TqSdkWorkerRequest,
    _isolated_python_path,
    launch_tqsdk_worker,
)


def _fake_tqsdk(root: Path) -> Path:
    package = root / "tqsdk"
    package.mkdir(parents=True)
    (package / "exceptions.py").write_text(
        "class BacktestFinished(Exception):\n    pass\n",
        encoding="utf-8",
    )
    (package / "__init__.py").write_text(
        '''from .exceptions import BacktestFinished
__version__ = "3.10.1-test"

class TqAuth:
    def __init__(self, username, password):
        self.username = username

class TqBacktest:
    def __init__(self, start_dt, end_dt):
        self.start_dt = start_dt
        self.end_dt = end_dt

class TqSim:
    def __init__(self, init_balance, account_id=None):
        self.init_balance = init_balance
        self.tqsdk_stat = {}
        self.trade_log = {
            "20260901": {
                "trades": [{
                    "trade_date_time": 1788231600000000000,
                    "trade_id": "T1",
                    "order_id": "O1",
                    "exchange_id": "SHFE",
                    "instrument_id": "au2612",
                    "direction": "BUY",
                    "offset": "OPEN",
                    "volume": 1,
                    "price": 500.2,
                    "commission": 10.0
                }],
                "account": {
                    "balance": 100090.0,
                    "available": 90000.0,
                    "margin": 10000.0,
                    "commission": 10.0
                }
            }
        }

class FakeFrame:
    def to_dict(self, orient):
        assert orient == "records"
        return [
            {"datetime": 1788231540000000000, "open": 499.8, "high": 500.1, "low": 499.7, "close": 500.0, "volume": 12},
            {"datetime": 1788231600000000000, "open": 500.0, "high": 500.4, "low": 499.9, "close": 500.2, "volume": 15},
        ]

class TqApi:
    def __init__(self, account=None, backtest=None, auth=None, **kwargs):
        self.account = account
        self.backtest = backtest
        self.auth = auth

    def wait_update(self, deadline=None):
        raise BacktestFinished()

    def get_kline_serial(self, symbol, duration_seconds, data_length):
        return FakeFrame()

    def get_order(self):
        return {"O1": {"status": "FINISHED"}}

    def get_position(self):
        return {"SHFE.au2612": {"pos_long": 1}}

    def get_account(self):
        return {"balance": 100090.0, "available": 90000.0}

    def close(self):
        self.account.tqsdk_stat = {"ror": 0.0009}
''',
        encoding="utf-8",
    )
    return root


def _request(task_root: Path) -> TqSdkWorkerRequest:
    strategy = task_root / "strategy.py"
    strategy.write_text(
        "import sys\n"
        "assert 'app.windows_sandbox' not in sys.modules\n"
        "from tqsdk import TqApi\n"
        "api = TqApi()\n"
        "bars = api.get_kline_serial('SHFE.au2612', 60, 2)\n"
        "while True:\n    api.wait_update()\n",
        encoding="utf-8",
    )
    return TqSdkWorkerRequest(
        task_id="tq-vector-1",
        task_root=task_root,
        strategy_path=strategy,
        result_path=task_root / "result.json",
        start_date="2026-09-01",
        end_date="2026-09-02",
        initial_balance=100_000,
    )


def test_isolated_python_path_excludes_parent_development_tools(tmp_path: Path) -> None:
    base_root = tmp_path / "Python312"
    (base_root / "DLLs").mkdir(parents=True)
    system_root = tmp_path / "Windows"
    (system_root / "System32").mkdir(parents=True)
    venv_root = tmp_path / "venv"
    scripts = venv_root / "Scripts"
    scripts.mkdir(parents=True)
    python_path = scripts / "python.exe"
    python_path.write_bytes(b"launcher")
    (venv_root / "pyvenv.cfg").write_text(
        f"home = {base_root}\nversion = 3.12.10\n", encoding="utf-8"
    )

    isolated = _isolated_python_path(python_path, system_root).split(os.pathsep)

    assert isolated == [
        str(scripts.resolve()),
        str(base_root.resolve()),
        str((base_root / "DLLs").resolve()),
        str((system_root / "System32").resolve()),
        str(system_root.resolve()),
    ]
    assert "Python311" not in os.pathsep.join(isolated)
    assert "Codex" not in os.pathsep.join(isolated)


def test_tqsdk_worker_runs_original_script_in_dedicated_process(
    tmp_path: Path, monkeypatch
) -> None:
    task_root = tmp_path / "task"
    task_root.mkdir()
    runtime = _fake_tqsdk(tmp_path / "fake-runtime")
    monkeypatch.setenv("PXYBACKTEST_TQSDK_USERNAME", "test-user")
    monkeypatch.setenv("PXYBACKTEST_TQSDK_PASSWORD", "test-password")

    result = launch_tqsdk_worker(
        _request(task_root),
        python_executable=Path(sys.executable),
        project_root=Path(__file__).parents[1],
        timeout_seconds=5,
        runtime_pythonpaths=[runtime],
    )

    assert result["runtime_identity"] == "tqsdk-3.10.1-test"
    assert result["deals"][0]["symbol"] == "SHFE.au2612"
    assert result["deals"][0]["direction"] == "buy"
    assert result["account_curve"][0]["balance"] == 100090.0
    assert result["native_metrics"] == {"ror": 0.0009}
    assert result["sandbox"]["strength"] == "windows_partial"
    assert result["sandbox"]["restricted_token"] is True
    assert result["sandbox"]["job_object"] is True
    assert result["sandbox"]["network_allowlist_enforced"] is False
    assert result["sandbox"]["submit_ready"] is False
    assert result["visual"]["available"] is True
    assert result["visual"]["bar_history_count"] == 2
    assert result["execution_snapshot"]["bar_history_count"] == 2
    assert len(result["replay_events"]) == 6


def test_tqsdk_worker_requires_controlled_credentials(
    tmp_path: Path, monkeypatch
) -> None:
    task_root = tmp_path / "task"
    task_root.mkdir()
    runtime = _fake_tqsdk(tmp_path / "fake-runtime")
    monkeypatch.delenv("PXYBACKTEST_TQSDK_USERNAME", raising=False)
    monkeypatch.delenv("PXYBACKTEST_TQSDK_PASSWORD", raising=False)

    with pytest.raises(TqSdkWorkerError, match="凭据"):
        launch_tqsdk_worker(
            _request(task_root),
            python_executable=Path(sys.executable),
            project_root=Path(__file__).parents[1],
            timeout_seconds=5,
            runtime_pythonpaths=[runtime],
        )


def test_tqsdk_worker_structures_strategy_system_exit(
    tmp_path: Path, monkeypatch
) -> None:
    task_root = tmp_path / "task"
    task_root.mkdir()
    runtime = _fake_tqsdk(tmp_path / "fake-runtime")
    request = _request(task_root)
    request.strategy_path.write_text(
        "from tqsdk import TqApi\napi = TqApi()\nraise SystemExit('planned-exit')\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PXYBACKTEST_TQSDK_USERNAME", "test-user")
    monkeypatch.setenv("PXYBACKTEST_TQSDK_PASSWORD", "test-password")

    with pytest.raises(TqSdkWorkerError, match="SystemExit: planned-exit"):
        launch_tqsdk_worker(
            request,
            python_executable=Path(sys.executable),
            project_root=Path(__file__).parents[1],
            timeout_seconds=5,
            runtime_pythonpaths=[runtime],
        )


def test_worker_main_preserves_primary_error_when_result_write_fails(
    tmp_path: Path, monkeypatch
) -> None:
    task_root = tmp_path / "task"
    task_root.mkdir()
    request = _request(task_root)
    request_path = task_root / "request.json"
    request_path.write_text(request.model_dump_json(), encoding="utf-8")
    diagnostic_path = task_root / "bootstrap-error.log"

    def fail_strategy(_request: TqSdkWorkerRequest) -> dict[str, object]:
        raise RuntimeError("primary-worker-failure")

    def fail_result_write(_path: Path, _payload: object) -> None:
        raise OSError("result-write-failure")

    monkeypatch.setenv(
        "PXYBACKTEST_TQSDK_BOOTSTRAP_ERROR", str(diagnostic_path)
    )
    monkeypatch.setattr(tqsdk_native_worker, "run_tqsdk_strategy", fail_strategy)
    monkeypatch.setattr(tqsdk_native_worker, "_write_result", fail_result_write)

    assert tqsdk_native_worker.main(["--request", str(request_path)]) == 1
    diagnostic = diagnostic_path.read_text(encoding="utf-8")
    assert "RuntimeError: primary-worker-failure" in diagnostic
    assert "SystemExit: 1" not in diagnostic


def test_worker_reports_last_imports_after_native_crash(
    tmp_path: Path, monkeypatch
) -> None:
    task_root = tmp_path / "task"
    task_root.mkdir()
    request = _request(task_root)
    monkeypatch.setenv("PXYBACKTEST_TQSDK_USERNAME", "test-user")
    monkeypatch.setenv("PXYBACKTEST_TQSDK_PASSWORD", "test-password")

    class NativeCrash:
        exit_code = 0xC06D007E

    captured_command: list[str] = []

    def crash(*args: object, **kwargs: object) -> NativeCrash:
        command = args[0]
        assert isinstance(command, list)
        captured_command.extend(str(item) for item in command)
        environment = kwargs["environment"]
        assert isinstance(environment, dict)
        trace_path = Path(str(environment["PXYBACKTEST_TQSDK_IMPORT_TRACE"]))
        trace_path.write_text("numpy\nnumpy._core._multiarray_umath\n", encoding="utf-8")
        return NativeCrash()

    monkeypatch.setattr(
        "app.windows_sandbox.launch_sandboxed_process", crash
    )

    with pytest.raises(
        TqSdkWorkerError, match="numpy._core._multiarray_umath"
    ):
        launch_tqsdk_worker(
            request,
            python_executable=Path(sys.executable),
            project_root=Path(__file__).parents[1],
            timeout_seconds=5,
        )

    assert "app.tqsdk_worker_bootstrap" in captured_command
    assert len(subprocess.list2cmdline(captured_command)) < 1024


def test_tqsdk_worker_redacts_credentials_from_structured_error(
    tmp_path: Path, monkeypatch
) -> None:
    task_root = tmp_path / "task"
    task_root.mkdir()
    runtime = _fake_tqsdk(tmp_path / "fake-runtime")
    request = _request(task_root)
    request.strategy_path.write_text(
        "import os\nfrom tqsdk import TqApi\napi = TqApi()\n"
        "raise SystemExit(os.environ['PXYBACKTEST_TQSDK_PASSWORD'])\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PXYBACKTEST_TQSDK_USERNAME", "test-user")
    monkeypatch.setenv("PXYBACKTEST_TQSDK_PASSWORD", "test-password")

    with pytest.raises(TqSdkWorkerError) as raised:
        launch_tqsdk_worker(
            request,
            python_executable=Path(sys.executable),
            project_root=Path(__file__).parents[1],
            timeout_seconds=5,
            runtime_pythonpaths=[runtime],
        )

    assert "test-password" not in str(raised.value)
    assert "<redacted>" in str(raised.value)


def test_tqsdk_worker_rejects_strategy_outside_task_root(tmp_path: Path) -> None:
    outside = tmp_path / "outside.py"
    outside.write_text("pass", encoding="utf-8")
    task_root = tmp_path / "task"
    task_root.mkdir()

    with pytest.raises(ValidationError, match="任务目录内"):
        TqSdkWorkerRequest(
            task_id="escape",
            task_root=task_root,
            strategy_path=outside,
            result_path=task_root / "result.json",
            start_date="2026-09-01",
            end_date="2026-09-02",
            initial_balance=100_000,
        )
