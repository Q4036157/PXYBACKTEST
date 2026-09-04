from __future__ import annotations

import asyncio
from pathlib import Path

from app.config import Settings
from app.main import _data_requirement_payload
from app.manager import TaskManager
from app.models import DataSnapshotRefV2, SubmitBacktestRequestV2
from app.pxydata_client import DataRequirementManifestV1, SnapshotProviderError
from app.store import TaskStore

from test_contract_v2 import _payload, _snapshot


class RequirementClient:
    def __init__(self, snapshot: DataSnapshotRefV2):
        self.snapshot = snapshot
        self.status = "pending"
        self.failure_reason = ""
        self.create_calls = 0
        self.get_calls = 0

    async def create_data_requirement(
        self, payload: dict
    ) -> DataRequirementManifestV1:
        self.create_calls += 1
        return self._manifest(payload)

    async def get_data_requirement(
        self, requirement_id: str
    ) -> DataRequirementManifestV1:
        self.get_calls += 1
        task = self._task
        payload = dict(task["state"]["data_requirement"]["request"])
        manifest = self._manifest(payload)
        assert manifest.requirement_id == requirement_id
        return manifest

    def bind(self, store: TaskStore, task_id: str) -> None:
        self._task = next(
            task for task in store.waiting_tasks() if task["task_id"] == task_id
        )

    def _manifest(self, payload: dict) -> DataRequirementManifestV1:
        import hashlib

        identity = (
            f"{payload['consumer_task_id']}\0{payload['request_fingerprint']}".encode()
        )
        return DataRequirementManifestV1.model_validate(
            {
                "requirement_id": "datareq_v1_"
                + hashlib.sha256(identity).hexdigest()[:32],
                "contract_version": "pxydata.data-requirement.v1",
                **payload,
                "status": self.status,
                "failure_reason": self.failure_reason,
                "snapshot": (
                    self.snapshot.model_dump(mode="json")
                    if self.status == "ready"
                    else None
                ),
                "created_at": "2026-09-04T08:00:00+00:00",
                "updated_at": "2026-09-04T08:01:00+00:00",
            }
        )


class UnavailableRequirementClient(RequirementClient):
    async def create_data_requirement(self, payload: dict) -> DataRequirementManifestV1:
        raise SnapshotProviderError("暂时不可用", status_code=502)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-token",
    )


def _body() -> SubmitBacktestRequestV2:
    return SubmitBacktestRequestV2.model_validate(_payload())


def test_waiting_task_resumes_same_identity_after_ready_snapshot(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = TaskStore(settings.database_path)
    manager = TaskManager(settings, store)
    body = _body()
    original = body.model_dump(mode="json")
    receipt = asyncio.run(
        manager.submit(
            user_id="user-a",
            source_node="204",
            request={"speed": 50, "_task_contract": original},
            idempotency_key="wait-1",
            initial_status="waiting_for_data",
            idempotency_payload=original,
        )
    )
    client = RequirementClient(_snapshot())

    async def resolve(request: dict, requirement: DataRequirementManifestV1):
        assert requirement.snapshot is not None
        resolved = body.with_snapshot(requirement.snapshot)
        return resolved.to_worker_request(), requirement.snapshot.model_dump(mode="json")

    manager.configure_data_waiting(client, resolve)
    requirement = asyncio.run(
        manager.register_data_requirement(
            receipt.task_id, _data_requirement_payload(body, receipt.task_id)
        )
    )
    client.bind(store, receipt.task_id)
    assert requirement["registration_confirmed"] is True
    assert store.get_task("user-a", receipt.task_id)["status"] == "waiting_for_data"

    replay = asyncio.run(
        manager.submit(
            user_id="user-a",
            source_node="204",
            request={"speed": 50, "_task_contract": original},
            idempotency_key="wait-1",
            initial_status="waiting_for_data",
            idempotency_payload=original,
        )
    )
    assert replay.task_id == receipt.task_id
    assert replay.idempotent_replay is True

    client.status = "ready"
    manager._last_data_requirement_scan = 0
    asyncio.run(manager._poll_data_requirements())

    task = store.get_task("user-a", receipt.task_id)
    request = store.get_request(receipt.task_id)
    assert task["status"] == "pending"
    assert task["data_snapshot"]["snapshot_id"] == _snapshot().snapshot_id
    assert task["data_checkpoint"] == {
        "phase": "worker_not_started",
        "snapshot_id": _snapshot().snapshot_id,
        "manifest_sha256": _snapshot().manifest_sha256,
    }
    assert request["_task_contract"]["data"]["snapshot"]["snapshot_id"] == (
        _snapshot().snapshot_id
    )
    assert client.create_calls == 1
    assert client.get_calls == 1


def test_failed_requirement_terminates_waiting_task(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = TaskStore(settings.database_path)
    manager = TaskManager(settings, store)
    body = _body()
    receipt = asyncio.run(
        manager.submit(
            user_id="user-a",
            source_node="204",
            request={"_task_contract": body.model_dump(mode="json")},
            initial_status="waiting_for_data",
        )
    )
    client = RequirementClient(_snapshot())
    manager.configure_data_waiting(client, lambda *_: None)  # type: ignore[arg-type]
    asyncio.run(
        manager.register_data_requirement(
            receipt.task_id, _data_requirement_payload(body, receipt.task_id)
        )
    )
    client.bind(store, receipt.task_id)
    client.status = "failed"
    client.failure_reason = "质量检查未通过"
    manager._last_data_requirement_scan = 0

    asyncio.run(manager._poll_data_requirements())

    task = store.get_task("user-a", receipt.task_id)
    assert task["status"] == "failed"
    assert task["error"] == "数据补齐失败: 质量检查未通过"


def test_registration_outage_keeps_retryable_waiting_task(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = TaskStore(settings.database_path)
    manager = TaskManager(settings, store)
    body = _body()
    receipt = asyncio.run(
        manager.submit(
            user_id="user-a",
            source_node="204",
            request={"_task_contract": body.model_dump(mode="json")},
            initial_status="waiting_for_data",
        )
    )
    client = UnavailableRequirementClient(_snapshot())
    manager.configure_data_waiting(client, lambda *_: None)  # type: ignore[arg-type]

    requirement = asyncio.run(
        manager.register_data_requirement(
            receipt.task_id, _data_requirement_payload(body, receipt.task_id)
        )
    )

    task = store.get_task("user-a", receipt.task_id)
    assert task["status"] == "waiting_for_data"
    assert task["data_requirement"]["registration_confirmed"] is False
    assert task["data_requirement"]["last_error"] == "暂时不可用"
    assert requirement["status"] == "pending"
