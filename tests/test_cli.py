from __future__ import annotations

import io
import json

from app import cli


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
