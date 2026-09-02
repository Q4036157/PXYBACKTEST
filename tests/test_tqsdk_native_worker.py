from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.tqsdk_native_worker import (
    TqSdkWorkerError,
    TqSdkWorkerRequest,
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
        "from tqsdk import TqApi\napi = TqApi()\nbars = api.get_kline_serial('SHFE.au2612', 60, 2)\nwhile True:\n    api.wait_update()\n",
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
