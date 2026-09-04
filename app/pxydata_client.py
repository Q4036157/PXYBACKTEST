from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .config import Settings
from .models import (
    DataSnapshotDatasetV2,
    DataSnapshotRefV2,
    DataSnapshotSelectionV2,
)


class SnapshotProviderError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 502):
        super().__init__(message)
        self.status_code = status_code


class DataRequirementManifestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirement_id: str = Field(pattern=r"^datareq_v1_[0-9a-f]{32}$")
    contract_version: Literal["pxydata.data-requirement.v1"]
    consumer_task_id: str
    request_fingerprint: str
    datasets: list[dict[str, Any]] = Field(min_length=1)
    quality_policy: Literal["require_pass", "allow_warn", "allow_unverified"]
    status: Literal[
        "pending", "backfilling", "quality_checking", "ready", "failed"
    ]
    failure_reason: str = ""
    snapshot: DataSnapshotRefV2 | None = None
    created_at: str
    updated_at: str

    @field_validator("created_at", "updated_at")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("数据需求时间不能为空")
        return value


class SnapshotClient(Protocol):
    @property
    def configured(self) -> bool: ...

    async def create_snapshot(
        self,
        *,
        selection: DataSnapshotSelectionV2,
        start_date: str,
        end_date: str,
        symbols: list[str],
    ) -> DataSnapshotRefV2: ...

    async def create_factor_bundle(
        self,
        *,
        selection: DataSnapshotSelectionV2,
        start_date: str,
        end_date: str,
        symbols: list[str],
        factor_set_id: str,
    ) -> tuple[DataSnapshotRefV2, DataSnapshotRefV2]: ...

    async def verify_snapshot(
        self, snapshot: DataSnapshotRefV2
    ) -> DataSnapshotRefV2: ...

    async def resolve_snapshot(
        self, snapshot: DataSnapshotRefV2
    ) -> tuple[DataSnapshotRefV2, dict[str, Any]]: ...

    async def get_factor_set(
        self, factor_set_id: str, version: int | None = None
    ) -> dict[str, Any]: ...

    async def get_data_quality(
        self, required_datasets: list[str]
    ) -> dict[str, Any]: ...

    async def create_data_requirement(
        self, payload: dict[str, Any]
    ) -> DataRequirementManifestV1: ...

    async def get_data_requirement(
        self, requirement_id: str
    ) -> DataRequirementManifestV1: ...


@dataclass(frozen=True)
class PxyDataSnapshotClient:
    base_url: str
    api_key: str
    timeout_seconds: float = 30.0

    @classmethod
    def from_settings(cls, settings: Settings) -> "PxyDataSnapshotClient":
        return cls(
            base_url=settings.pxydata_base_url.rstrip("/"),
            api_key=settings.pxydata_api_key,
        )

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.api_key)

    async def create_snapshot(
        self,
        *,
        selection: DataSnapshotSelectionV2,
        start_date: str,
        end_date: str,
        symbols: list[str],
    ) -> DataSnapshotRefV2:
        payload = {
            "datasets": selection.datasets,
            "start_date": start_date,
            "end_date": end_date,
            "symbols": symbols,
            "decision_time": selection.decision_time,
            "quality_policy": selection.quality_policy,
        }
        response = await asyncio.to_thread(
            self._request,
            "POST",
            "/api/v1/backtest/data-snapshots",
            payload,
        )
        return _snapshot_ref_from_summary(response)

    async def create_factor_bundle(
        self,
        *,
        selection: DataSnapshotSelectionV2,
        start_date: str,
        end_date: str,
        symbols: list[str],
        factor_set_id: str,
    ) -> tuple[DataSnapshotRefV2, DataSnapshotRefV2]:
        payload = {
            "datasets": selection.datasets,
            "start_date": start_date,
            "end_date": end_date,
            "symbols": symbols,
            "decision_time": selection.decision_time,
            "quality_policy": selection.quality_policy,
            "factor_set_id": factor_set_id,
        }
        response = await asyncio.to_thread(
            self._request,
            "POST",
            "/api/v1/backtest/factor-bundles",
            payload,
        )
        input_snapshot = response.get("input_snapshot")
        execution_snapshot = response.get("execution_snapshot")
        if not isinstance(input_snapshot, dict) or not isinstance(execution_snapshot, dict):
            raise SnapshotProviderError("PXYDATA 因子编排响应缺少快照", status_code=502)
        return (
            _snapshot_ref_from_summary(input_snapshot),
            _snapshot_ref_from_summary(execution_snapshot),
        )

    async def verify_snapshot(self, snapshot: DataSnapshotRefV2) -> DataSnapshotRefV2:
        provider_snapshot, _ = await self.resolve_snapshot(snapshot)
        return provider_snapshot

    async def resolve_snapshot(
        self, snapshot: DataSnapshotRefV2
    ) -> tuple[DataSnapshotRefV2, dict[str, Any]]:
        response = await asyncio.to_thread(
            self._request,
            "GET",
            f"/api/v1/backtest/data-snapshots/{snapshot.snapshot_id}",
            None,
        )
        provider_snapshot = _snapshot_ref_from_manifest(response)
        if provider_snapshot.snapshot_id != snapshot.snapshot_id:
            raise SnapshotProviderError("PXYDATA 快照 ID 不一致", status_code=409)
        if provider_snapshot.manifest_sha256 != snapshot.manifest_sha256:
            raise SnapshotProviderError("PXYDATA 快照清单哈希不一致", status_code=409)
        return provider_snapshot, response

    async def get_factor_set(
        self, factor_set_id: str, version: int | None = None
    ) -> dict[str, Any]:
        """读取并校验 PXYDATA 已注册的版本化因子集合。"""
        identifier = str(factor_set_id).strip()
        if not identifier:
            raise SnapshotProviderError("factor_set_id 不能为空", status_code=422)
        path = f"/api/v1/backtest/factor-sets/{identifier}"
        if version is not None:
            path += f"?version={int(version)}"
        response = await asyncio.to_thread(self._request, "GET", path, None)
        required = (
            "factor_set_id",
            "version",
            "factor_set_hash",
            "feature_code_hash",
            "status",
        )
        if any(not str(response.get(field) or "").strip() for field in required):
            raise SnapshotProviderError("PXYDATA 返回了无效的 factor_set 契约", status_code=502)
        if str(response.get("factor_set_id")) != identifier:
            raise SnapshotProviderError("PXYDATA factor_set_id 不一致", status_code=409)
        if str(response.get("status")) != "active":
            raise SnapshotProviderError("factor_set 未处于 active 状态", status_code=409)
        try:
            resolved_version = int(response["version"])
        except (TypeError, ValueError) as exc:
            raise SnapshotProviderError("PXYDATA factor_set 版本无效", status_code=502) from exc
        if resolved_version <= 0:
            raise SnapshotProviderError("PXYDATA factor_set 版本无效", status_code=502)
        return response

    async def get_data_quality(
        self, required_datasets: list[str]
    ) -> dict[str, Any]:
        """读取最近一次全量质量认证，供能力接口执行提交门禁。"""
        required = ",".join(
            dict.fromkeys(
                str(item).strip() for item in required_datasets if str(item).strip()
            )
        )
        query = urllib.parse.urlencode(
            {"required": required, "live": "false", "max_age_seconds": 86400}
        )
        response = await asyncio.to_thread(
            self._request,
            "GET",
            f"/api/pxydaa/data/admin/data-quality?{query}",
            None,
        )
        report = response.get("report")
        if not isinstance(report, dict):
            raise SnapshotProviderError("PXYDATA 质量接口缺少认证报告", status_code=502)
        return response

    async def create_data_requirement(
        self, payload: dict[str, Any]
    ) -> DataRequirementManifestV1:
        response = await asyncio.to_thread(
            self._request,
            "POST",
            "/api/v1/backtest/data-requirements",
            payload,
        )
        return _validate_data_requirement(response, expected=payload)

    async def get_data_requirement(
        self, requirement_id: str
    ) -> DataRequirementManifestV1:
        identifier = str(requirement_id).strip()
        response = await asyncio.to_thread(
            self._request,
            "GET",
            f"/api/v1/backtest/data-requirements/{identifier}",
            None,
        )
        manifest = _validate_data_requirement(response)
        if manifest.requirement_id != identifier:
            raise SnapshotProviderError("PXYDATA 数据需求 ID 不一致", status_code=409)
        return manifest

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None
    ) -> dict[str, Any]:
        if not self.configured:
            raise SnapshotProviderError("PXYDATA 快照服务凭据未配置", status_code=503)
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout_seconds
            ) as response:
                decoded = json.loads(response.read().decode("utf-8"))
                if not isinstance(decoded, dict):
                    raise SnapshotProviderError(
                        "PXYDATA 返回了非对象响应", status_code=502
                    )
                return decoded
        except urllib.error.HTTPError as exc:
            detail = _http_error_detail(exc)
            raise SnapshotProviderError(
                f"PXYDATA 快照请求失败: {detail}", status_code=exc.code
            ) from exc
        except (OSError, TimeoutError, json.JSONDecodeError) as exc:
            raise SnapshotProviderError(
                f"PXYDATA 快照服务不可用: {type(exc).__name__}", status_code=502
            ) from exc


def _http_error_detail(exc: urllib.error.HTTPError) -> str:
    try:
        payload = json.loads(exc.read().decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return f"HTTP {exc.code}"
    return str(payload.get("detail") or f"HTTP {exc.code}")[:500]


def _snapshot_ref_from_summary(payload: dict[str, Any]) -> DataSnapshotRefV2:
    return _validate_snapshot_ref(payload)


def _snapshot_ref_from_manifest(payload: dict[str, Any]) -> DataSnapshotRefV2:
    quality = payload.get("quality")
    if not isinstance(quality, dict):
        raise SnapshotProviderError("PXYDATA 快照清单缺少质量摘要", status_code=502)
    return _validate_snapshot_ref(
        {
            "contract_version": payload.get("contract_version"),
            "snapshot_id": payload.get("snapshot_id"),
            "manifest_sha256": payload.get("manifest_sha256"),
            "created_at": payload.get("created_at"),
            "quality_policy": quality.get("policy"),
            "quality_accepted": quality.get("accepted"),
            "quality_report_id": quality.get("report_id"),
            "datasets": payload.get("datasets"),
            "warnings": quality.get("warnings") or [],
        }
    )


def _validate_snapshot_ref(payload: dict[str, Any]) -> DataSnapshotRefV2:
    dataset_fields = DataSnapshotDatasetV2.model_fields
    datasets = [
        {key: value for key, value in item.items() if key in dataset_fields}
        for item in payload.get("datasets") or []
        if isinstance(item, dict)
    ]
    fields = DataSnapshotRefV2.model_fields
    normalized = {key: value for key, value in payload.items() if key in fields}
    normalized["datasets"] = datasets
    try:
        return DataSnapshotRefV2.model_validate(normalized)
    except ValidationError as exc:
        raise SnapshotProviderError(
            "PXYDATA 返回了无效的快照契约", status_code=502
        ) from exc


def _validate_data_requirement(
    payload: dict[str, Any], *, expected: dict[str, Any] | None = None
) -> DataRequirementManifestV1:
    try:
        manifest = DataRequirementManifestV1.model_validate(payload)
    except ValidationError as exc:
        raise SnapshotProviderError(
            "PXYDATA 返回了无效的数据需求契约", status_code=502
        ) from exc
    if expected is not None:
        if manifest.consumer_task_id != str(expected.get("consumer_task_id") or ""):
            raise SnapshotProviderError("PXYDATA 数据需求任务身份不一致", status_code=409)
        if manifest.request_fingerprint != str(
            expected.get("request_fingerprint") or ""
        ):
            raise SnapshotProviderError("PXYDATA 数据需求指纹不一致", status_code=409)
    if manifest.status == "ready" and manifest.snapshot is None:
        raise SnapshotProviderError("已就绪的数据需求缺少快照", status_code=502)
    if manifest.status != "ready" and manifest.snapshot is not None:
        raise SnapshotProviderError("未就绪的数据需求不应包含快照", status_code=502)
    return manifest
