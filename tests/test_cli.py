from __future__ import annotations

import io
import json

from app import cli


def test_version_and_schema_are_machine_readable_json():
    version_output = io.StringIO()
    schema_output = io.StringIO()

    assert cli.main(["--version"], output=version_output) == 0
    assert cli.main(["--schema"], output=schema_output) == 0

    version = json.loads(version_output.getvalue())
    schema = json.loads(schema_output.getvalue())
    assert version == {
        "program": "pxybacktest",
        "version": "0.2.0",
        "contract_version": "pxybacktest.agent-cli.v1",
    }
    assert schema["output"]["stdout"] == "json"
    assert schema["commands"]["status"]["job_id_option"] == "--job-id"


def test_status_and_result_accept_job_id_option(monkeypatch):
    calls = []

    def fake_request(self, method, path, payload=None, *, auth=True):
        calls.append((method, path))
        return {"task_id": "task-42", "status": "completed", "result": {"ok": True}}

    monkeypatch.setattr(cli.BacktestApiClient, "request", fake_request)
    status_output = io.StringIO()
    result_output = io.StringIO()

    assert cli.main(["--token", "x", "--json", "status", "--job-id", "task-42"], output=status_output) == 0
    assert cli.main(["--token", "x", "result", "--job-id", "task-42"], output=result_output) == 0

    assert calls == [
        ("GET", "/api/v1/tasks/task-42"),
        ("GET", "/api/v1/tasks/task-42"),
    ]
    assert json.loads(result_output.getvalue())["result"]["ok"] is True


def test_conflicting_task_and_job_ids_fail_without_http_call(monkeypatch, capsys):
    def unexpected_request(*args, **kwargs):
        raise AssertionError("HTTP must not be called")

    monkeypatch.setattr(cli.BacktestApiClient, "request", unexpected_request)
    output = io.StringIO()

    assert cli.main(
        ["--token", "x", "status", "task-1", "--job-id", "task-2"],
        output=output,
    ) == 1
    assert output.getvalue() == ""
    assert "不一致" in capsys.readouterr().err


def test_submit_chooses_v2_and_reads_request_file(tmp_path, monkeypatch):
    request_file = tmp_path / "request.json"
    request_file.write_text(json.dumps({"schema_version": 2, "engine_type": "vnpy_cta"}), encoding="utf-8")
    calls = []

    def fake_request(self, method, path, payload=None, *, auth=True):
        calls.append((method, path, payload, auth))
        return {"success": True, "task_id": "task-1"}

    monkeypatch.setattr(cli.BacktestApiClient, "request", fake_request)
    output = io.StringIO()
    assert cli.main(["--token", "x", "submit", "--request-file", str(request_file)], output=output) == 0
    assert calls[0][0:2] == ("POST", "/api/v2/tasks")
    assert json.loads(output.getvalue())["task_id"] == "task-1"


def test_health_does_not_require_token(monkeypatch):
    calls = []

    def fake_request(self, method, path, payload=None, *, auth=True):
        calls.append((method, path, auth))
        return {"ok": True}

    monkeypatch.setattr(cli.BacktestApiClient, "request", fake_request)
    output = io.StringIO()
    assert cli.main(["health"], output=output) == 0
    assert calls == [("GET", "/health", False)]


def test_accept_result_runs_offline_without_service_token(tmp_path):
    actual_file = tmp_path / "actual.json"
    vector_file = tmp_path / "vector.json"
    actual_file.write_text(
        json.dumps(
            {
                "strategy": {"source_hash": "a" * 64},
                "data_snapshot": {"manifest_sha256": "b" * 64},
                "diagnostics": {"runtime_identity": "runtime"},
                "deals": [],
                "account": [],
                "visual": {},
            }
        ),
        encoding="utf-8",
    )
    vector_file.write_text(
        json.dumps(
            {
                "vector_id": "offline-vector",
                "platform": "vnpy",
                "strategy_source_sha256": "a" * 64,
                "data_manifest_sha256": "b" * 64,
                "runtime_identity": "runtime",
                "identity_checks": [
                    {"path": "strategy.source_hash", "expected": "a" * 64},
                    {
                        "path": "data_snapshot.manifest_sha256",
                        "expected": "b" * 64,
                    },
                    {
                        "path": "diagnostics.runtime_identity",
                        "expected": "runtime",
                    },
                ],
                "trades": {"checks": [{"path": "deals", "expected": []}]},
                "account": {"checks": [{"path": "account", "expected": []}]},
                "visual": {"checks": [{"path": "visual", "expected": {}}]},
            }
        ),
        encoding="utf-8",
    )
    output = io.StringIO()

    code = cli.main(
        [
            "accept-result",
            "--vector",
            str(vector_file),
            "--actual",
            str(actual_file),
        ],
        output=output,
    )

    assert code == 0
    assert json.loads(output.getvalue())["all_passed"] is True


def test_run_saves_terminal_snapshot(tmp_path, monkeypatch):
    request_file = tmp_path / "request.json"
    save_file = tmp_path / "result.json"
    request_file.write_text(json.dumps({"schema_version": 2}), encoding="utf-8")
    calls = []

    def fake_request(self, method, path, payload=None, *, auth=True):
        calls.append((method, path))
        if method == "POST":
            return {"task_id": "task-2"}
        return {"task_id": "task-2", "status": "completed", "result_available": True}

    monkeypatch.setattr(cli.BacktestApiClient, "request", fake_request)
    output = io.StringIO()
    assert cli.main(
        ["--token", "x", "run", "--request-file", str(request_file), "--save", str(save_file)],
        output=output,
    ) == 0
    assert json.loads(save_file.read_text(encoding="utf-8"))["status"] == "completed"
    assert ("GET", "/api/v1/tasks/task-2") in calls


def test_trusted_tqsdk_cli_writes_report_without_opening_gate(
    tmp_path, monkeypatch
):
    report_file = tmp_path / "trusted-report.json"

    def trusted_report():
        return {
            "all_passed": True,
            "submit_ready": False,
            "execution_lane": "trusted_fixed_vector",
        }

    monkeypatch.setattr(
        "app.tqsdk_acceptance.run_tqsdk_trusted_acceptance", trusted_report
    )
    output = io.StringIO()

    code = cli.main(
        [
            "verify-tqsdk-trusted",
            "--report-output",
            str(report_file),
        ],
        output=output,
    )

    assert code == 0
    assert json.loads(report_file.read_text(encoding="utf-8"))["submit_ready"] is False
    assert json.loads(output.getvalue())["submit_ready"] is False
