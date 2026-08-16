from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.config import Settings
from app.daa_client import (
    DaaAdapterClient,
    DaaCapabilitiesError,
    _validate_capabilities,
)
from app.main import _bind_factor_set_to_manifest, create_app
from app.models import (
    DataSnapshotRefV2,
    DataSnapshotSelectionV2,
    SubmitBacktestRequestV2,
)
from app.pxydata_client import PxyDataSnapshotClient, SnapshotProviderError
from app.result_contract import build_a_share_result_v2, build_result_v2
from app.store import TaskStore


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


class FakeSnapshotClient:
    def __init__(
        self,
        snapshot: DataSnapshotRefV2,
        create_error: SnapshotProviderError | None = None,
    ):
        self.snapshot = snapshot
        self.create_error = create_error
        self.created: list[dict] = []
        self.factor_bundles: list[dict] = []
        self.verified: list[str] = []
        self.resolved: list[str] = []

    @property
    def configured(self) -> bool:
        return True

    async def create_snapshot(
        self,
        *,
        selection: DataSnapshotSelectionV2,
        start_date: str,
        end_date: str,
        symbols: list[str],
    ) -> DataSnapshotRefV2:
        if self.create_error is not None:
            raise self.create_error
        self.created.append(
            {
                "selection": selection.model_dump(),
                "start_date": start_date,
                "end_date": end_date,
                "symbols": symbols,
            }
        )
        return self.snapshot

    async def create_factor_bundle(
        self,
        *,
        selection: DataSnapshotSelectionV2,
        start_date: str,
        end_date: str,
        symbols: list[str],
        factor_set_id: str,
    ) -> tuple[DataSnapshotRefV2, DataSnapshotRefV2]:
        if self.create_error is not None:
            raise self.create_error
        self.factor_bundles.append(
            {
                "selection": selection.model_dump(),
                "start_date": start_date,
                "end_date": end_date,
                "symbols": symbols,
                "factor_set_id": factor_set_id,
            }
        )
        input_payload = _a_share_snapshot().model_dump(mode="json")
        input_payload["snapshot_id"] = "btsnap_v1_" + "c" * 32
        input_payload["manifest_sha256"] = "e" * 64
        return DataSnapshotRefV2.model_validate(input_payload), self.snapshot

    async def verify_snapshot(self, snapshot: DataSnapshotRefV2) -> DataSnapshotRefV2:
        self.verified.append(snapshot.snapshot_id)
        if snapshot.manifest_sha256 != self.snapshot.manifest_sha256:
            raise SnapshotProviderError("快照清单哈希不一致", status_code=409)
        return snapshot

    async def resolve_snapshot(
        self, snapshot: DataSnapshotRefV2
    ) -> tuple[DataSnapshotRefV2, dict]:
        self.resolved.append(snapshot.snapshot_id)
        verified = await self.verify_snapshot(snapshot)
        return verified, {
            "contract_version": verified.contract_version,
            "snapshot_id": verified.snapshot_id,
            "manifest_sha256": verified.manifest_sha256,
            "selection": {
                "start_date": "2026-01-01",
                "end_date": "2026-12-31",
                "symbols": ["600000.SH"],
            },
            "quality": {"policy": "require_pass", "accepted": True},
            "datasets": [
                {
                    "name": item.name,
                    "files": [
                        {
                            "path": "normalized/kline_daily/date=2026-08-01/part.parquet",
                            "sha256": "d" * 64,
                            "size_bytes": 100,
                        }
                    ],
                }
                for item in verified.datasets
            ],
        }


class FakeDaaClient:
    def __init__(self, available: bool = True, factor_available: bool = False):
        self.available = available
        self.factor_available = factor_available

    @property
    def configured(self) -> bool:
        return self.available

    async def get_capabilities(self) -> dict:
        if not self.available:
            raise DaaCapabilitiesError("not available")
        strategies = [
            {
                "id": "boll_breakout",
                "name": "布林突破",
                "version": "builtin",
                "source_hash": "a" * 64,
                "entrypoint": "boll_breakout",
                "parameters": [],
                "engine_types": ["a_share_portfolio"],
            }
        ]
        if self.factor_available:
            strategies.extend(
                [
                    {
                        "id": "multi_factor_rank",
                        "name": "多因子横截面排序",
                        "version": "builtin-v1",
                        "source_hash": "f" * 64,
                        "entrypoint": "multi_factor_rank",
                        "parameters": [],
                        "engine_types": ["factor_matrix"],
                    },
                    {
                        "id": "event_sentiment_rank",
                        "name": "事件舆情排序",
                        "version": "builtin-v1",
                        "source_hash": "f" * 64,
                        "entrypoint": "event_sentiment_rank",
                        "parameters": [],
                        "engine_types": ["event_sentiment"],
                    },
                ]
            )
        return {
            "contract_version": "pxybacktest.engine-adapter.a-share.v1",
            "worker_version": "daa.a-share-adapter.v1",
            "strategies": strategies,
        }


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-service-token",
        daa_root=tmp_path / "DAA",
        pxydata_data_root=tmp_path / "PXYDATA" / "data",
    )


def _snapshot() -> DataSnapshotRefV2:
    return DataSnapshotRefV2.model_validate(
        {
            "contract_version": "pxydata.backtest-data-snapshot.v1",
            "snapshot_id": "btsnap_v1_" + "a" * 32,
            "manifest_sha256": "b" * 64,
            "created_at": "2026-08-15T00:00:00+00:00",
            "quality_policy": "allow_unverified",
            "quality_accepted": False,
            "quality_report_id": None,
            "datasets": [
                {
                    "name": "kline_1m",
                    "contract_id": "pxydata.kline_1m.v1",
                    "schema_version": 1,
                    "file_count": 1,
                    "row_count": 100,
                    "size_bytes": 1024,
                    "content_digest": "c" * 64,
                }
            ],
            "warnings": ["研究模式已显式放行未认证数据"],
        }
    )


def _payload() -> dict:
    return {
        "schema_version": 2,
        "engine_type": "vnpy_cta",
        "strategy": {
            "id": "example",
            "version": "1.0.0",
            "source_hash": "12345678abcdef",
            "entrypoint": "ExampleStrategy",
        },
        "universe": {"symbols": ["BTCUSDT_SWAP_OKX.GLOBAL"]},
        "period": {
            "start": "2026-08-01T00:00:00+08:00",
            "end": "2026-08-02T00:00:00+08:00",
            "interval": "1m",
            "timezone": "Asia/Shanghai",
        },
        "data": {
            "selection": {
                "datasets": ["kline_1m"],
                "decision_time": "2026-08-02T00:00:00+08:00",
                "quality_policy": "allow_unverified",
            }
        },
        "execution": {"capital": 100000, "mode": "BAR", "speed": 50},
        "parameters": {"window": 20},
        "random_seed": 7,
    }


def _a_share_snapshot() -> DataSnapshotRefV2:
    payload = _snapshot().model_dump(mode="json")
    payload["datasets"] = [
        {
            "name": "kline_daily",
            "contract_id": "pxydata.kline_daily.v1",
            "schema_version": 1,
            "file_count": 1,
            "row_count": 100,
            "size_bytes": 1024,
            "content_digest": "c" * 64,
        }
    ]
    return DataSnapshotRefV2.model_validate(payload)


def _a_share_payload() -> dict:
    payload = _payload()
    payload.update(
        {
            "engine_type": "a_share_portfolio",
            "strategy": {
                "id": "boll_breakout",
                "version": "builtin",
                "source_hash": "a" * 64,
                "entrypoint": "boll_breakout",
            },
            "universe": {"symbols": ["600000.SH", "000001.SZ"]},
            "period": {
                "start": "2026-08-01T00:00:00+08:00",
                "end": "2026-08-02T23:59:59+08:00",
                "interval": "1d",
                "timezone": "Asia/Shanghai",
            },
            "data": {
                "selection": {
                    "datasets": ["kline_daily"],
                    "decision_time": "2026-08-15T00:00:00+08:00",
                    "quality_policy": "require_pass",
                }
            },
            "execution": {"capital": 100000, "mode": "BAR", "speed": 50},
            "parameters": {
                "overrides": {"basic_filter": {"enabled": False}},
                "mode": "full",
                "holding_days": 5,
            },
        }
    )
    return payload


def _factor_snapshot() -> DataSnapshotRefV2:
    payload = _a_share_snapshot().model_dump(mode="json")
    payload["datasets"].append(
        {
            "name": "factor_matrix_daily",
            "contract_id": "pxydata.factor_matrix_daily.v1",
            "schema_version": 1,
            "file_count": 1,
            "row_count": 100,
            "size_bytes": 2048,
            "content_digest": "d" * 64,
        }
    )
    return DataSnapshotRefV2.model_validate(payload)


def _factor_payload(engine_type: str = "factor_matrix") -> dict:
    payload = _a_share_payload()
    strategy_id = (
        "multi_factor_rank"
        if engine_type == "factor_matrix"
        else "event_sentiment_rank"
    )
    payload.update(
        {
            "engine_type": engine_type,
            "strategy": {
                "id": strategy_id,
                "version": "builtin-v1",
                "source_hash": "f" * 64,
                "entrypoint": strategy_id,
            },
            "data": {
                "selection": {
                    "datasets": ["kline_daily", "financial_statements", "events"],
                    "decision_time": "2026-08-15T00:00:00+08:00",
                    "quality_policy": "require_pass",
                }
            },
            "parameters": {
                "factor_set_id": "a_share_research_daily_v1",
                "factor_weights": {"event_sentiment_5d": 1.0},
                "holding_days": 5,
                "rebalance_days": 5,
                "max_positions": 10,
            },
        }
    )
    return payload


def _enable_a_share_adapter(settings: Settings) -> None:
    python = settings.daa_root / "backend" / ".venv" / "Scripts" / "python.exe"
    adapter = settings.daa_root / "backend" / "app" / "backtest" / "pxy_adapter.py"
    python.parent.mkdir(parents=True)
    adapter.parent.mkdir(parents=True)
    settings.pxydata_data_root.mkdir(parents=True)
    python.write_bytes(b"test")
    adapter.write_text("# test", encoding="utf-8")


def _headers() -> dict[str, str]:
    return {
        "X-PXY-Service-Token": "test-service-token",
        "X-PXY-User-Id": "user-a",
        "X-PXY-Source-Node": "204",
    }


def test_v2_submission_resolves_snapshot_before_queueing(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    snapshots = FakeSnapshotClient(_snapshot())
    app = create_app(
        settings,
        manager,
        snapshots,
        FakeDaaClient(),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.post("/api/v2/tasks", json=_payload(), headers=_headers())

    assert response.status_code == 202
    task_id = response.json()["task_id"]
    request = manager.store.get_request(task_id)
    assert request["strategy_class"] == "ExampleStrategy"
    assert request["_task_contract"]["schema_version"] == 2
    assert request["_task_contract"]["data"]["snapshot"]["snapshot_id"] == (
        _snapshot().snapshot_id
    )
    assert snapshots.created[0]["symbols"] == ["BTCUSDT_SWAP_OKX.GLOBAL"]
    task = manager.store.get_task("user-a", task_id)
    assert task["schema_version"] == 2
    assert task["data_snapshot"]["manifest_sha256"] == "b" * 64
    assert manager.store.list_tasks("user-a")[0]["data_snapshot"]["snapshot_id"] == (
        _snapshot().snapshot_id
    )


def test_v2_unavailable_engine_fails_without_creating_snapshot(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    snapshots = FakeSnapshotClient(_factor_snapshot())
    app = create_app(
        settings,
        manager,
        snapshots,
        FakeDaaClient(),  # type: ignore[arg-type]
    )
    payload = _factor_payload()

    with TestClient(app) as client:
        response = client.post("/api/v2/tasks", json=payload, headers=_headers())

    assert response.status_code == 501
    assert snapshots.created == []


def test_v2_factor_submission_binds_manifest_and_expands_exit_tail(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    snapshots = FakeSnapshotClient(_factor_snapshot())
    app = create_app(
        settings,
        manager,
        snapshots,
        FakeDaaClient(factor_available=True),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/v2/tasks",
            json=_factor_payload(),
            headers=_headers(),
        )

    assert response.status_code == 202
    assert snapshots.factor_bundles[0]["start_date"] == "2026-08-01"
    assert snapshots.factor_bundles[0]["end_date"] == "2026-08-22"
    assert snapshots.factor_bundles[0]["factor_set_id"] == "a_share_research_daily_v1"
    assert snapshots.resolved == [_factor_snapshot().snapshot_id]
    worker = manager.store.get_request(response.json()["task_id"])
    assert worker["_task_contract"]["engine_type"] == "factor_matrix"
    assert worker["_task_contract"]["parameters"]["factor_input_snapshot_id"] == (
        "btsnap_v1_" + "c" * 32
    )
    assert worker["_snapshot_manifest"]["datasets"]


def test_v2_a_share_submission_expands_and_binds_snapshot(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _enable_a_share_adapter(settings)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    snapshots = FakeSnapshotClient(_a_share_snapshot())
    app = create_app(
        settings,
        manager,
        snapshots,
        FakeDaaClient(),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/v2/tasks", json=_a_share_payload(), headers=_headers()
        )

    assert response.status_code == 202
    assert snapshots.created[0]["start_date"] == "2026-04-03"
    assert snapshots.created[0]["end_date"] == "2026-08-22"
    assert snapshots.resolved == [_a_share_snapshot().snapshot_id]
    task_id = response.json()["task_id"]
    worker_request = manager.store.get_request(task_id)
    assert "strategy_class" not in worker_request
    assert worker_request["_snapshot_manifest"]["datasets"][0]["files"]
    assert "files" not in str(manager.store.get_task("user-a", task_id))
    assert "files" not in str(response.json())


def test_v2_a_share_capability_uses_installed_adapter_state(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _enable_a_share_adapter(settings)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    app = create_app(
        settings,
        manager,
        FakeSnapshotClient(_a_share_snapshot()),
        FakeDaaClient(),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.get("/api/v2/capabilities", headers=_headers())

    a_share = next(
        item for item in response.json()["engines"] if item["id"] == "a_share_portfolio"
    )
    assert a_share["available"] is True
    assert a_share["snapshot_enforcement"] == "manifest_bound"
    assert a_share["strategies"][0]["source_hash"] == "a" * 64


def test_v2_capabilities_exposes_factor_and_sentiment_engines(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    app = create_app(
        settings,
        manager,
        FakeSnapshotClient(_factor_snapshot()),
        FakeDaaClient(factor_available=True),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.get("/api/v2/capabilities", headers=_headers())

    engines = {item["id"]: item for item in response.json()["engines"]}
    assert engines["factor_matrix"]["available"] is True
    assert engines["event_sentiment"]["available"] is True
    assert engines["factor_matrix"]["factor_contract"] == (
        "pxydata.factor_matrix_daily.v1"
    )
    assert engines["factor_matrix"]["strategies"][0]["id"] == "multi_factor_rank"


def test_daa_capabilities_validation_drops_internal_fields() -> None:
    payload = FakeDaaClient().get_capabilities()
    resolved = asyncio.run(payload)
    resolved["strategies"][0]["file_path"] = "D:/private/strategy.py"

    normalized = _validate_capabilities(resolved)

    assert "file_path" not in normalized["strategies"][0]


def test_daa_capabilities_preserves_supported_factor_engine_types() -> None:
    resolved = asyncio.run(FakeDaaClient(factor_available=True).get_capabilities())

    normalized = _validate_capabilities(resolved)
    factor = next(
        item for item in normalized["strategies"] if item["id"] == "multi_factor_rank"
    )

    assert factor["engine_types"] == ["factor_matrix"]


def test_daa_capabilities_timeout_is_a_controlled_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    settings.daa_python.parent.mkdir(parents=True)
    settings.daa_python.touch()
    adapter_path = settings.daa_backend_root / "app" / "backtest" / "pxy_adapter.py"
    adapter_path.parent.mkdir(parents=True)
    adapter_path.touch()
    settings.pxydata_data_root.mkdir(parents=True)
    client = DaaAdapterClient(settings, timeout_seconds=12.0)

    def raise_timeout(*args, **kwargs):
        assert kwargs["timeout"] == 12.0
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", raise_timeout)

    with pytest.raises(DaaCapabilitiesError, match="读取超时"):
        client._load_capabilities()


def test_v2_capabilities_degrades_without_failing_when_daa_times_out(
    tmp_path: Path,
) -> None:
    class TimedOutDaaClient:
        @property
        def configured(self) -> bool:
            return True

        async def get_capabilities(self) -> dict:
            raise DaaCapabilitiesError("DAA 策略目录读取超时")

    settings = _settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    app = create_app(
        settings,
        manager,
        FakeSnapshotClient(_a_share_snapshot()),
        TimedOutDaaClient(),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.get("/api/v2/capabilities", headers=_headers())

    assert response.status_code == 200
    engines = {item["id"]: item for item in response.json()["engines"]}
    assert engines["vnpy_cta"]["available"] is True
    assert engines["a_share_portfolio"]["available"] is False


def test_v2_model_rejects_invalid_data_choice_and_compares_actual_instants() -> None:
    payload = _payload()
    payload["data"] = {}
    with pytest.raises(ValidationError, match="exactly one"):
        SubmitBacktestRequestV2.model_validate(payload)

    payload = _payload()
    payload["period"] = {
        **payload["period"],
        "start": "2026-08-01T23:00:00+08:00",
        "end": "2026-08-01T16:00:00+00:00",
    }
    assert SubmitBacktestRequestV2.model_validate(payload).period.end.endswith("+00:00")


def test_snapshot_client_filters_provider_only_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _snapshot().model_dump(mode="json")
    response["created"] = True
    response["manifest_endpoint"] = "/api/v1/backtest/data-snapshots/example"
    response["datasets"][0]["pit_grade"] = "point_in_time"

    def fake_request(*_args, **_kwargs) -> dict:
        return response

    monkeypatch.setattr(PxyDataSnapshotClient, "_request", fake_request)
    client = PxyDataSnapshotClient(base_url="http://pxydata", api_key="test")
    resolved = asyncio.run(
        client.create_snapshot(
            selection=DataSnapshotSelectionV2.model_validate(
                _payload()["data"]["selection"]
            ),
            start_date="2026-08-01",
            end_date="2026-08-02",
            symbols=["BTCUSDT_SWAP_OKX.GLOBAL"],
        )
    )

    assert resolved.snapshot_id == _snapshot().snapshot_id
    assert resolved.datasets[0].pit_grade == "point_in_time"
    assert "manifest_endpoint" not in resolved.model_dump()


def test_snapshot_client_canonicalizes_verified_full_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested = _snapshot()
    dataset = requested.datasets[0].model_dump(mode="json")
    dataset["files"] = [{"path": "normalized/private-file.parquet"}]
    response = {
        "contract_version": requested.contract_version,
        "snapshot_id": requested.snapshot_id,
        "manifest_sha256": requested.manifest_sha256,
        "created_at": requested.created_at,
        "quality": {
            "policy": "require_pass",
            "accepted": True,
            "report_id": "quality-provider",
            "warnings": [],
        },
        "datasets": [dataset],
    }

    def fake_request(*_args, **_kwargs) -> dict:
        return response

    monkeypatch.setattr(PxyDataSnapshotClient, "_request", fake_request)
    client = PxyDataSnapshotClient(base_url="http://pxydata", api_key="test")
    resolved = asyncio.run(client.verify_snapshot(requested))

    assert resolved.quality_policy == "require_pass"
    assert resolved.quality_accepted is True
    assert resolved.quality_report_id == "quality-provider"
    assert "files" not in resolved.datasets[0].model_dump()


def test_snapshot_client_loads_registered_factor_set_and_binds_derivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factor_set = {
        "factor_set_id": "daily_value",
        "version": 3,
        "factor_set_hash": "a" * 64,
        "feature_code_hash": "b" * 64,
        "status": "active",
    }

    def fake_request(_self, method: str, path: str, payload: dict | None) -> dict:
        assert (method, path, payload) == (
            "GET",
            "/api/v1/backtest/factor-sets/daily_value?version=3",
            None,
        )
        return factor_set

    monkeypatch.setattr(PxyDataSnapshotClient, "_request", fake_request)
    client = PxyDataSnapshotClient(base_url="http://pxydata", api_key="test")
    loaded = asyncio.run(client.get_factor_set("daily_value", 3))
    updates = _bind_factor_set_to_manifest(
        _factor_payload(),
        {
            "derivation": {
                "factor_set_id": "daily_value",
                "factor_set_hash": "a" * 64,
                "feature_code_hash": "b" * 64,
            }
        },
        loaded,
    )

    assert updates == {
        "factor_set_id": "daily_value",
        "factor_set_version": 3,
        "factor_set_hash": "a" * 64,
        "feature_code_hash": "b" * 64,
    }


def test_factor_set_binding_rejects_execution_snapshot_hash_mismatch() -> None:
    with pytest.raises(SnapshotProviderError, match="factor_set_hash"):
        _bind_factor_set_to_manifest(
            _factor_payload(),
            {
                "derivation": {
                    "factor_set_id": "daily_value",
                    "factor_set_hash": "c" * 64,
                    "feature_code_hash": "b" * 64,
                }
            },
            {
                "factor_set_id": "daily_value",
                "version": 3,
                "factor_set_hash": "a" * 64,
                "feature_code_hash": "b" * 64,
                "status": "active",
            },
        )


def test_v2_preserves_snapshot_provider_unavailable_status(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manager = StoreOnlyManager(TaskStore(settings.database_path))
    snapshots = FakeSnapshotClient(
        _snapshot(),
        create_error=SnapshotProviderError("PXYDATA 未配置", status_code=503),
    )
    app = create_app(settings, manager, snapshots)  # type: ignore[arg-type]

    with TestClient(app) as client:
        response = client.post("/api/v2/tasks", json=_payload(), headers=_headers())

    assert response.status_code == 503
    assert manager.store.list_tasks("user-a") == []


def test_result_v2_maps_legacy_vnpy_result_without_inventing_orders() -> None:
    snapshot = _snapshot()
    task = _payload()
    task["data"] = {"snapshot": snapshot.model_dump(mode="json")}
    result = build_result_v2(
        task_id="task-1",
        request={"_task_contract": task},
        raw_result={
            "statistics": {"total_return": 0.1},
            "daily_results": [{"date": "2026-08-01", "balance": 101000}],
            "trades": [{"trade_id": "trade-1", "price": 100}],
            "bars": [{"datetime": "2026-08-01T00:00:00", "close": 100}],
            "data_count": 100,
            "trades_count": 1,
        },
    )

    assert result["contract_version"] == "pxybacktest.task-result.v2"
    assert result["metrics"]["total_return"] == 0.1
    assert result["deals"][0]["trade_id"] == "trade-1"
    assert result["orders"] == []
    assert result["diagnostics"]["quality_accepted"] is False
    assert result["diagnostics"]["random_seed"] == 7
    assert result["diagnostics"]["degraded_capabilities"] == ["orders", "positions"]
    assert result["diagnostics"]["snapshot_enforcement"] == "provenance_only"
    assert result["diagnostics"]["strictly_reproducible"] is False
    assert result["run"]["random_seed"] == 7


def test_result_v2_marks_cancelled_execution_as_partial() -> None:
    task = _payload()
    task["data"] = {"snapshot": _snapshot().model_dump(mode="json")}

    result = build_result_v2(
        task_id="task-partial",
        request={"_task_contract": task},
        raw_result={
            "complete": False,
            "termination_reason": "cancelled",
            "progress": 3.91,
            "processed_bars": 226,
            "total_bars": 5760,
            "current_datetime": "2026-08-01 03:45:00",
            "trades": [{"trade_id": "trade-1", "pnl": 12.5}],
            "data_count": 226,
        },
    )

    assert result["complete"] is False
    assert result["termination_reason"] == "cancelled"
    assert result["diagnostics"]["progress"] == 3.91
    assert result["diagnostics"]["processed_bars"] == 226
    assert result["diagnostics"]["total_bars"] == 5760


def test_result_v2_maps_daa_result_and_discloses_price_degradation() -> None:
    task = _a_share_payload()
    task["data"] = {"snapshot": _a_share_snapshot().model_dump(mode="json")}
    result = build_a_share_result_v2(
        task_id="task-a-share",
        request={"_task_contract": task},
        raw_result={
            "stats": {
                "total_return": 0.12,
                "adapter": {
                    "contract_version": "pxybacktest.engine-adapter.a-share.v1",
                    "strategy_source_sha256": "a" * 64,
                    "snapshot_id": _a_share_snapshot().snapshot_id,
                    "manifest_sha256": "b" * 64,
                    "snapshot_enforcement": "manifest_bound",
                    "price_adjustment": "none",
                    "corporate_actions_applied": False,
                    "worker_version": "daa.a-share-adapter.v1",
                    "verified_file_count": 2,
                    "verified_size_bytes": 2048,
                    "loaded_rows": 100,
                    "loaded_symbols": 2,
                },
            },
            "equity_curve": [{"date": "2026-08-01", "value": 100000}],
            "drawdown_curve": [{"date": "2026-08-01", "value": 0}],
            "benchmark_curve": [],
            "trades": [{"symbol": "600000.SH", "pnl_pct": 0.12}],
            "per_symbol_stats": [{"symbol": "600000.SH", "trades": 1}],
            "strategy_info": {"name": "布林突破"},
            "elapsed_ms": 12.5,
        },
    )

    assert result["metrics"]["total_return"] == 0.12
    assert result["deals"][0]["symbol"] == "600000.SH"
    assert result["orders"] == []
    assert result["diagnostics"]["snapshot_enforcement"] == "manifest_bound"
    assert result["diagnostics"]["price_adjustment"] == "none"
    assert result["diagnostics"]["corporate_actions_applied"] is False
    assert "未复权" in "".join(result["diagnostics"]["warnings"])


def test_result_v2_maps_factor_versions_and_research_metrics() -> None:
    task = _factor_payload()
    task["data"] = {"snapshot": _factor_snapshot().model_dump(mode="json")}
    result = build_a_share_result_v2(
        task_id="task-factor",
        request={"_task_contract": task},
        raw_result={
            "stats": {
                "total_return": 0.08,
                "ic": 0.06,
                "icir": 1.2,
                "long_short_return": 0.01,
                "turnover_rate": 0.4,
                "adapter": {
                    "contract_version": "pxybacktest.engine-adapter.a-share.v1",
                    "snapshot_enforcement": "manifest_bound",
                    "worker_version": "daa.a-share-adapter.v1",
                    "loaded_rows": 100,
                    "factor_contract": "pxydata.factor_matrix_daily.v1",
                    "factor_set_id": "a_share_research_daily_v1",
                    "factor_set_hash": "d" * 64,
                    "factor_input_snapshot_id": "btsnap_v1_" + "c" * 32,
                    "factor_input_manifest_sha256": "e" * 64,
                    "feature_code_hash": "f" * 64,
                    "factor_weights": {"event_sentiment_5d": 1.0},
                },
            },
            "equity_curve": [],
            "drawdown_curve": [],
            "trades": [],
        },
    )

    assert result["engine_type"] == "factor_matrix"
    assert result["metrics"]["icir"] == 1.2
    assert result["diagnostics"]["factor_contract"] == (
        "pxydata.factor_matrix_daily.v1"
    )
    assert result["diagnostics"]["factor_set_hash"] == "d" * 64
    assert result["diagnostics"]["feature_code_hash"] == "f" * 64
    assert result["diagnostics"]["adapter"] == "daa.factor_matrix.v1"
    assert result["input_snapshot"] == {
        "snapshot_id": "btsnap_v1_" + "c" * 32,
        "manifest_sha256": "e" * 64,
        "role": "factor_materialization_input",
    }
