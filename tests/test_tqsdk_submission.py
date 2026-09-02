from __future__ import annotations

import hashlib
import json
import queue
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.replay import ReplayEvent
from app.tqsdk_submission import TqSdkTaskSubmission
from app.worker_process import run_tqsdk_queue_worker


SOURCE = (
    "from tqsdk import TqApi\n"
    "api = TqApi()\n"
    "bars = api.get_kline_serial('SHFE.au2612', 60, 20)\n"
    "while True:\n"
    "    api.wait_update()\n"
)


def package_payload(source: str = SOURCE) -> dict:
    encoded = source.encode("utf-8")
    return {
        "strategy_id": "tq-ma-vector",
        "version": "1",
        "source": {
            "platform": "tqsdk",
            "language": "python",
            "entrypoint": "strategy.py",
            "artifacts": [
                {
                    "artifact_id": "source",
                    "file_name": "strategy.py",
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                    "media_type": "text/x-python",
                    "role": "source",
                    "size_bytes": len(encoded),
                }
            ],
            "license_policy": "user_supplied",
        },
        "runner": {
            "mode": "native_sandbox",
            "adapter_id": "tqsdk-native",
            "adapter_version": "windows-restricted-v1",
            "runtime_identity": "tqsdk-3.10.2",
        },
        "subscriptions": [
            {
                "kind": "bar",
                "symbols": ["SHFE.au2612"],
                "interval": "1m",
            }
        ],
        "execution": {
            "semantics": "tqsdk",
            "initial_cash": 1_000_000,
            "leverage": 1,
            "matching_model": "native",
            "position_mode": "netting",
        },
        "permissions": {
            "network": "allowlisted",
            "filesystem": "task_readwrite",
            "timeout_seconds": 60,
            "memory_mb": 512,
            "cpu_cores": 1,
        },
    }


def submission_payload(source: str = SOURCE) -> dict:
    return {
        "package": package_payload(source),
        "source_code": source,
        "start_date": "2026-08-18",
        "end_date": "2026-08-20",
        "execution_mode": "fast",
        "speed": 100,
    }


def test_tqsdk_submission_materializes_existing_queue_contract() -> None:
    submission = TqSdkTaskSubmission.model_validate(submission_payload())

    request = submission.to_worker_request()

    assert request["_task_contract"]["engine_type"] == "tqsdk_native"
    assert request["_task_contract"]["universe"]["symbols"] == ["SHFE.au2612"]
    assert request["_task_contract"]["data"]["native_provider"]["provider"] == "tqsdk"
    assert request["_tqsdk_source_code"] == SOURCE


def test_tqsdk_submission_rejects_source_hash_mismatch() -> None:
    payload = submission_payload()
    payload["source_code"] = payload["source_code"].replace("while", "WHILE")

    with pytest.raises(ValidationError, match="SHA256"):
        TqSdkTaskSubmission.model_validate(payload)


def test_tqsdk_queue_worker_persists_result_and_replays_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submission = TqSdkTaskSubmission.model_validate(submission_payload())
    request = submission.to_worker_request()
    replay_event = ReplayEvent(
        event_type="market_bar",
        event_time="2026-08-18T01:00:00Z",
        snapshot_id="a" * 64,
        source="tqsdk-native",
        symbol="SHFE.au2612",
        payload={
            "symbol": "SHFE.au2612",
            "datetime_ns": 1787014800000000000,
            "open": 500,
            "high": 501,
            "low": 499,
            "close": 500.5,
        },
    )

    def fake_launch(*_args, **_kwargs) -> dict:
        return {
            "runtime_identity": "tqsdk-3.10.2",
            "sandbox": {
                "strength": "windows_restricted",
                "restricted_token": True,
                "job_object": True,
                "dedicated_identity": True,
                "task_directory_acl": True,
                "filesystem_isolated": True,
                "network_allowlist_enforced": True,
                "submit_ready": True,
            },
            "data_manifest_sha256": "a" * 64,
            "native_trade_log_sha256": "b" * 64,
            "native_metrics": {"ror": 0.01},
            "deals": [],
            "orders": {},
            "positions": {},
            "account_curve": [{"trading_day": "20260818", "balance": 1_010_000}],
            "final_account": {"balance": 1_010_000},
            "replay_events": [replay_event.to_dict()],
            "replay_audit": {"event_count": 1, "chain_sha256": "c" * 64},
        }

    import app.tqsdk_native_worker as native_worker

    monkeypatch.setattr(native_worker, "launch_tqsdk_worker", fake_launch)
    monkeypatch.setenv("PXYBACKTEST_TQSDK_PYTHON", sys.executable)
    events: queue.Queue = queue.Queue()
    result_path = tmp_path / "results" / "result.json"
    run_tqsdk_queue_worker(
        "task-tq",
        request,
        str(result_path),
        str(tmp_path / "jobs" / "task-tq"),
        events,
        queue.Queue(),
    )

    result = json.loads(result_path.read_text(encoding="utf-8"))
    emitted = list(events.queue)
    assert emitted[-1]["type"] == "completed"
    assert result["engine_type"] == "tqsdk_native"
    assert result["metrics"]["final_equity"] == 1_010_000
    assert result["execution_snapshot"]["bar_history_count"] == 1
    assert result["execution_snapshot"]["replay"]["source"] == "tqsdk-native"
    assert result["diagnostics"]["pause_scope"] == "replay_only"
    assert "_replay_events" not in result
