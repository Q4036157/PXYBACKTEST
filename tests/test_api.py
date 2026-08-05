from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
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

    async def pause(self, user_id: str, task_id: str) -> bool:
        self.store.get_task(user_id, task_id)
        return False

    async def resume(self, user_id: str, task_id: str) -> bool:
        self.store.get_task(user_id, task_id)
        return False

    async def set_speed(self, user_id: str, task_id: str, speed: float) -> bool:
        self.store.get_task(user_id, task_id)
        return False

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
