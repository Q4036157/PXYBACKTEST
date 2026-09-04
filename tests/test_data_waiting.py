from __future__ import annotations

import asyncio
from pathlib import Path

from app.config import Settings
from app.main import _data_requirement_payload
from app.manager import (
    MAX_CONCURRENT_DATA_REQUIREMENTS,
    MAX_DATA_REQUIREMENTS_PER_CYCLE,
    TaskManager,
)
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
        self, requirement_id: str, *, expected: dict | None = None
    ) -> DataRequirementManifestV1:
        self.get_calls += 1
        task = self._task
        payload = dict(task["state"]["data_requirement"]["request"])
        manifest = self._manifest(payload)
        assert manifest.requirement_id == requirement_id
        assert expected == payload
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
        self.create_calls += 1
        raise SnapshotProviderError("暂时不可用", status_code=502)


class MissingOnceRequirementClient(RequirementClient):
    def __init__(self, snapshot: DataSnapshotRefV2):
        super().__init__(snapshot)
        self.missing = True

    async def get_data_requirement(
        self, requirement_id: str, *, expected: dict | None = None
    ) -> DataRequirementManifestV1:
        self.get_calls += 1
        if self.missing:
            self.missing = False
            raise SnapshotProviderError("需求不存在", status_code=404)
        return await super().get_data_requirement(
            requirement_id, expected=expected
        )


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

    manager.configure_data_waiting(
        client,
        resolve,
        lambda request, task_id: _data_requirement_payload(
            SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
            task_id,
        ),
    )
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
    manager.configure_data_waiting(
        client,
        lambda *_: None,  # type: ignore[arg-type]
        lambda request, task_id: _data_requirement_payload(
            SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
            task_id,
        ),
    )
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
    manager.configure_data_waiting(
        client,
        lambda *_: None,  # type: ignore[arg-type]
        lambda request, task_id: _data_requirement_payload(
            SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
            task_id,
        ),
    )

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
    manager._last_data_requirement_scan = 0
    asyncio.run(manager._poll_data_requirements())
    assert client.create_calls == 1


def test_waiting_orphan_rebuilds_requirement_from_persisted_contract(
    tmp_path: Path,
) -> None:
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
    manager.configure_data_waiting(
        client,
        lambda *_: None,  # type: ignore[arg-type]
        lambda request, task_id: _data_requirement_payload(
            SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
            task_id,
        ),
    )

    manager._last_data_requirement_scan = 0
    asyncio.run(manager._poll_data_requirements())

    task = store.get_task("user-a", receipt.task_id)
    assert task["status"] == "waiting_for_data"
    assert task["data_requirement"]["consumer_task_id"] == receipt.task_id
    assert task["data_requirement"]["registration_confirmed"] is True
    assert client.create_calls == 1


def test_missing_remote_requirement_is_registered_again(tmp_path: Path) -> None:
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
    client = MissingOnceRequirementClient(_snapshot())
    manager.configure_data_waiting(
        client,
        lambda *_: None,  # type: ignore[arg-type]
        lambda request, task_id: _data_requirement_payload(
            SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
            task_id,
        ),
    )
    asyncio.run(
        manager.register_data_requirement(
            receipt.task_id, _data_requirement_payload(body, receipt.task_id)
        )
    )
    client.bind(store, receipt.task_id)

    manager._last_data_requirement_scan = 0
    asyncio.run(manager._poll_data_requirements())
    missing = store.get_task("user-a", receipt.task_id)["data_requirement"]
    assert missing["registration_confirmed"] is False

    manager._last_data_requirement_scan = 0
    asyncio.run(manager._poll_data_requirements())
    restored = store.get_task("user-a", receipt.task_id)["data_requirement"]
    assert restored["registration_confirmed"] is True
    assert client.create_calls == 2


def test_cancel_during_ready_poll_cannot_reactivate_task(tmp_path: Path) -> None:
    async def scenario() -> tuple[dict, int]:
        settings = _settings(tmp_path)
        store = TaskStore(settings.database_path)
        manager = TaskManager(settings, store)
        body = _body()
        receipt = await manager.submit(
            user_id="user-a",
            source_node="204",
            request={"_task_contract": body.model_dump(mode="json")},
            initial_status="waiting_for_data",
        )
        client = RequirementClient(_snapshot())
        resolver_calls = 0

        async def resolve(*_):
            nonlocal resolver_calls
            resolver_calls += 1
            return {}, {}

        manager.configure_data_waiting(
            client,
            resolve,
            lambda request, task_id: _data_requirement_payload(
                SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
                task_id,
            ),
        )
        await manager.register_data_requirement(
            receipt.task_id, _data_requirement_payload(body, receipt.task_id)
        )
        client.bind(store, receipt.task_id)
        client.status = "ready"

        original_get = client.get_data_requirement
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked_get(requirement_id: str, *, expected: dict | None = None):
            entered.set()
            await release.wait()
            return await original_get(requirement_id, expected=expected)

        client.get_data_requirement = blocked_get  # type: ignore[method-assign]
        manager._last_data_requirement_scan = 0
        polling = asyncio.create_task(manager._poll_data_requirements())
        await entered.wait()
        assert await manager.cancel("user-a", receipt.task_id) is True
        release.set()
        await polling
        return store.get_task("user-a", receipt.task_id), resolver_calls

    task, resolver_calls = asyncio.run(scenario())
    assert task["status"] == "cancelled"
    assert resolver_calls == 0


def test_data_polling_has_per_cycle_and_concurrency_bounds(tmp_path: Path) -> None:
    class CountingClient(RequirementClient):
        def __init__(self, snapshot: DataSnapshotRefV2):
            super().__init__(snapshot)
            self.active = 0
            self.max_active = 0

        async def create_data_requirement(
            self, payload: dict
        ) -> DataRequirementManifestV1:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await asyncio.sleep(0.01)
                return await super().create_data_requirement(payload)
            finally:
                self.active -= 1

    async def scenario() -> CountingClient:
        settings = _settings(tmp_path)
        store = TaskStore(settings.database_path)
        manager = TaskManager(settings, store)
        body = _body()
        for index in range(MAX_DATA_REQUIREMENTS_PER_CYCLE + 3):
            await manager.submit(
                user_id=f"user-{index}",
                source_node="204",
                request={"_task_contract": body.model_dump(mode="json")},
                initial_status="waiting_for_data",
            )
        client = CountingClient(_snapshot())
        manager.configure_data_waiting(
            client,
            lambda *_: None,  # type: ignore[arg-type]
            lambda request, task_id: _data_requirement_payload(
                SubmitBacktestRequestV2.model_validate(request["_task_contract"]),
                task_id,
            ),
        )
        manager._last_data_requirement_scan = 0
        await manager._poll_data_requirements()
        return client

    client = asyncio.run(scenario())
    assert client.create_calls == MAX_DATA_REQUIREMENTS_PER_CYCLE
    assert client.max_active <= MAX_CONCURRENT_DATA_REQUIREMENTS
