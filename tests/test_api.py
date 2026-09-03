import hashlib
import io
import json
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.manager import PlaybackControlResult
from app.models import SubmitBacktestRequest
from app.store import TaskStore
from pydantic import ValidationError
import pytest


class StoreOnlyManager:
    def __init__(self, store: TaskStore):
        self.store = store

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def submit(self, *, user_id: str, source_node: str, request: dict) -> str:
        return self.store.create_task(
            user_id=user_id, source_node=source_node, request=request
        )

    def result(self, user_id: str, task_id: str) -> dict:
        return self.store.get_task(user_id, task_id)

    async def pause(self, user_id: str, task_id: str) -> PlaybackControlResult:
        task = self.store.get_task(user_id, task_id)
        return PlaybackControlResult(False, False, str(task["status"]))

    async def resume(self, user_id: str, task_id: str) -> PlaybackControlResult:
        task = self.store.get_task(user_id, task_id)
        return PlaybackControlResult(False, False, str(task["status"]))

    async def set_speed(self, user_id: str, task_id: str, speed: float) -> bool:
        self.store.get_task(user_id, task_id)
        return False

    async def step(self, user_id: str, task_id: str) -> PlaybackControlResult:
        task = self.store.get_task(user_id, task_id)
        return PlaybackControlResult(False, False, str(task["status"]))

    async def cancel(self, user_id: str, task_id: str) -> bool:
        self.store.get_task(user_id, task_id)
        return False


def settings(tmp_path: Path) -> Settings:
    return Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-service-token",
    )


def test_submit_requires_trusted_service_identity(tmp_path: Path) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    app = create_app(configured, manager)  # type: ignore[arg-type]
    payload = {
        "strategy_class": "ExampleStrategy",
        "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
        "interval": "1m",
        "start_time": "2026-08-01 00:00:00",
        "end_time": "2026-08-02 00:00:00",
        "parameters": {},
        "capital": 100000,
        "rate": 0.0004,
        "speed": 50,
        "mode": "TICK",
    }

    with TestClient(app) as client:
        unauthorized = client.post("/api/v1/tasks", json=payload)
        assert unauthorized.status_code == 401

        accepted = client.post(
            "/api/v1/tasks",
            json=payload,
            headers={
                "X-PXY-Service-Token": "test-service-token",
                "X-PXY-User-Id": "user-a",
                "X-PXY-Source-Node": "204",
            },
        )

    assert accepted.status_code == 202
    assert accepted.json()["execution_backend"] == "workstation"


def test_health_reports_workstation_only_compute(tmp_path: Path) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    app = create_app(configured, manager)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["computeLocation"] == "workstation-only"


def test_task_routes_enforce_user_ownership(tmp_path: Path) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    task_id = manager.store.create_task(
        user_id="user-a",
        source_node="204",
        request={
            "strategy_class": "ExampleStrategy",
            "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
            "interval": "1m",
            "start_time": "2026-08-01 00:00:00",
            "end_time": "2026-08-02 00:00:00",
        },
    )
    app = create_app(configured, manager)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.get(
            f"/api/v1/tasks/{task_id}",
            headers={
                "X-PXY-Service-Token": "test-service-token",
                "X-PXY-User-Id": "user-b",
            },
        )

    assert response.status_code == 404


def test_task_zip_export_contains_complete_events_and_checksums(tmp_path: Path) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    request = {
        "strategy_class": "ExampleStrategy",
        "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
        "interval": "1m",
        "start_time": "2026-08-01 00:00:00",
        "end_time": "2026-08-02 00:00:00",
    }
    task_id = manager.store.create_task(
        user_id="user-a", source_node="204", request=request
    )
    manager.store.append_events(
        task_id,
        [("state", {"progress": index}) for index in range(1, 4)],
    )
    app = create_app(configured, manager)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.get(
            f"/api/v1/tasks/{task_id}/export.zip",
            headers={
                "X-PXY-Service-Token": "test-service-token",
                "X-PXY-User-Id": "user-a",
            },
        )

    assert response.status_code == 200
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert set(archive.namelist()) == {
            "events.ndjson",
            "manifest.json",
            "request.json",
            "task.json",
        }
        events = [
            json.loads(line)
            for line in archive.read("events.ndjson").decode("utf-8").splitlines()
        ]
        assert [event["seq"] for event in events] == [1, 2, 3]
        manifest = json.loads(archive.read("manifest.json"))
        for entry in manifest["files"]:
            content = archive.read(entry["path"])
            assert entry["size"] == len(content)
            assert entry["sha256"] == hashlib.sha256(content).hexdigest()


def test_pause_response_reports_acceptance_confirmation_and_status(
    tmp_path: Path,
) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    task_id = manager.store.create_task(
        user_id="user-a",
        source_node="204",
        request={
            "strategy_class": "ExampleStrategy",
            "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
            "interval": "1m",
            "start_time": "2026-08-01 00:00:00",
            "end_time": "2026-08-02 00:00:00",
        },
    )
    app = create_app(configured, manager)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.post(
            f"/api/v1/tasks/{task_id}/pause",
            headers={
                "X-PXY-Service-Token": "test-service-token",
                "X-PXY-User-Id": "user-a",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "success": False,
        "confirmed": False,
        "status": "pending",
        "message": "任务当前状态为 pending，无法暂停",
    }


def test_events_report_queue_position_and_active_progress(tmp_path: Path) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    running_id = manager.store.create_task(
        user_id="user-a", source_node="204", request={
            "strategy_class": "ExampleStrategy",
            "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
            "interval": "1m",
            "start_time": "2026-08-01 00:00:00",
            "end_time": "2026-08-02 00:00:00",
        },
    )
    queued_id = manager.store.create_task(
        user_id="user-a", source_node="204", request={
            "strategy_class": "ExampleStrategy",
            "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
            "interval": "1m",
            "start_time": "2026-08-01 00:00:00",
            "end_time": "2026-08-02 00:00:00",
        },
    )
    manager.store.mark_running(running_id)
    manager.store.append_event(
        running_id,
        "state",
        {"progress": 40, "processed_bars": 40, "total_bars": 100},
    )
    app = create_app(configured, manager)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.get(
            f"/api/v1/tasks/{queued_id}/events",
            headers={
                "X-PXY-Service-Token": "test-service-token",
                "X-PXY-User-Id": "user-a",
            },
        )

    assert response.status_code == 200
    assert response.json()["queue_ahead"] == 1
    assert response.json()["active_task"]["progress"] == 40


def test_events_contract_reports_window_without_false_truncation(
    tmp_path: Path,
) -> None:
    configured = settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(configured.database_path))
    task_id = manager.store.create_task(
        user_id="user-a",
        source_node="204",
        request={
            "strategy_class": "ExampleStrategy",
            "vt_symbol": "BTCUSDT_SWAP_OKX.GLOBAL",
            "interval": "1m",
            "start_time": "2026-08-01 00:00:00",
            "end_time": "2026-08-02 00:00:00",
        },
    )
    seq = manager.store.append_event(
        task_id,
        "bar",
        {
            "bar": {
                "datetime": "2026-08-01 00:00:00",
                "open": 1,
                "close": 2,
            },
            "replay_seq": 1,
        },
    )
    app = create_app(configured, manager)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.get(
            f"/api/v1/tasks/{task_id}/events",
            headers={
                "X-PXY-Service-Token": "test-service-token",
                "X-PXY-User-Id": "user-a",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["earliest_seq"] == seq
    assert payload["latest_seq"] == seq
    assert payload["next_seq"] == seq
    assert payload["history_truncated"] is False
    assert payload["resync"] is None


@pytest.mark.parametrize(
    "vt_symbol",
    [
        "LITUSDT_SWAP_LIGHTER.GLOBAL",
        "BTCUSDT_SWAP_OKX.GLOBAL",
        "BTCUSDT_SWAP_BINANCE.GLOBAL",
        "ETHUSDT_SWAP_BITMART.GLOBAL",
        "XAUUSDc_SWAP_MT4.GLOBAL",
        "XAUUSDc_SWAP_MT5.GLOBAL",
    ],
)
def test_submit_model_accepts_supported_platforms(vt_symbol: str) -> None:
    payload = {
        "strategy_class": "ExampleStrategy",
        "vt_symbol": vt_symbol,
        "interval": "1m",
        "start_time": "2026-08-01 00:00:00",
        "end_time": "2026-08-02 00:00:00",
    }
    assert SubmitBacktestRequest.model_validate(payload).vt_symbol == vt_symbol


def test_submit_model_rejects_unsupported_platform() -> None:
    with pytest.raises(ValidationError):
        SubmitBacktestRequest.model_validate(
            {
                "strategy_class": "ExampleStrategy",
                "vt_symbol": "BTCUSDT_SWAP_UNKNOWN.GLOBAL",
                "interval": "1m",
                "start_time": "2026-08-01 00:00:00",
                "end_time": "2026-08-02 00:00:00",
            }
        )
