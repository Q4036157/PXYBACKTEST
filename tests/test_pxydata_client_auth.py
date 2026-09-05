from __future__ import annotations

import asyncio
import hashlib
import json
import urllib.request
from copy import deepcopy
from io import BytesIO
from typing import Self

import pytest

from app.config import Settings
from app.pxydata_client import PxyDataSnapshotClient, SnapshotProviderError


class _JsonResponse:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps({"ok": True}).encode("utf-8")


def test_settings_loads_pxydata_service_secret_file(tmp_path, monkeypatch) -> None:
    api_key_file = tmp_path / "pxydata-api-key"
    service_secret_file = tmp_path / "pxydata-service-secret"
    api_key_file.write_text("api-key\n", encoding="utf-8")
    service_secret_file.write_text("service-secret\n", encoding="utf-8")
    monkeypatch.setenv("PXYBACKTEST_PXYDATA_API_KEY_FILE", str(api_key_file))
    monkeypatch.setenv(
        "PXYBACKTEST_PXYDATA_SERVICE_SECRET_FILE", str(service_secret_file)
    )
    monkeypatch.delenv("PXYBACKTEST_PXYDATA_API_KEY", raising=False)
    monkeypatch.delenv("PXYBACKTEST_PXYDATA_SERVICE_SECRET", raising=False)

    settings = Settings.from_env()

    assert settings.pxydata_api_key == "api-key"
    assert settings.pxydata_service_secret == "service-secret"


def test_snapshot_client_sends_pxydata_service_secret(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_urlopen(request: urllib.request.Request, *, timeout: float) -> _JsonResponse:
        captured.update({name.lower(): value for name, value in request.header_items()})
        assert timeout == 30.0
        return _JsonResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = PxyDataSnapshotClient(
        base_url="http://127.0.0.1:3020",
        api_key="api-key",
        service_secret="service-secret",
    )

    assert client._request("GET", "/health-probe", None) == {"ok": True}
    assert captured["authorization"] == "Bearer api-key"
    assert captured["x-pxydaa-service-secret"] == "service-secret"


def test_snapshot_client_keeps_service_secret_optional(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_urlopen(request: urllib.request.Request, *, timeout: float) -> _JsonResponse:
        captured.update({name.lower(): value for name, value in request.header_items()})
        return _JsonResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = PxyDataSnapshotClient(base_url="http://pxydata", api_key="api-key")

    assert client._request("GET", "/health-probe", None) == {"ok": True}
    assert "x-pxydaa-service-secret" not in captured


def _requirement_payload() -> dict:
    consumer_task_id = "etf-wire-contract"
    fingerprint = "a" * 64
    identifier = hashlib.sha256(f"{consumer_task_id}\0{fingerprint}".encode()).hexdigest()[:32]
    return {
        "requirement_id": f"datareq_v1_{identifier}",
        "contract_version": "pxydata.data-requirement.v1",
        "consumer_task_id": consumer_task_id,
        "request_fingerprint": fingerprint,
        "datasets": [
            {"name": "kline_etf_daily", "fields": [], "symbols": ["159819.SZ"], "start": "2025-09-06", "end": "2026-09-04", "frequency": "1d", "market": "cn_equity", "pit_required": False},
            {"name": "market_emotion_daily", "fields": [], "symbols": [], "start": "2025-09-06", "end": "2026-09-04", "frequency": "1d", "market": "cn_equity", "pit_required": True},
        ],
        "quality_policy": "allow_warn",
        "snapshot_kind": "snapshot",
        "factor_set_id": None,
    }


def _requirement_response(payload: dict) -> dict:
    return {
        **deepcopy(payload), "status": "pending", "failure_reason": "", "snapshot": None,
        "created_at": "2026-09-05T00:00:00+00:00", "updated_at": "2026-09-05T00:00:00+00:00",
    }


def test_create_requirement_omits_only_local_id_from_wire(monkeypatch) -> None:
    payload = _requirement_payload()
    original = deepcopy(payload)
    captured = []

    def fake_urlopen(request, *, timeout):
        assert request.method == "POST"
        assert request.full_url == "http://pxydata/api/v1/backtest/data-requirements"
        wire = json.loads(request.data)
        captured.append(wire)
        assert set(wire) == {
            "contract_version", "consumer_task_id", "request_fingerprint", "datasets",
            "quality_policy", "snapshot_kind", "factor_set_id",
        }
        assert wire == {key: value for key, value in original.items() if key != "requirement_id"}
        return BytesIO(json.dumps(_requirement_response(original)).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = PxyDataSnapshotClient(base_url="http://pxydata", api_key="test-key")
    for _ in range(2):
        result = asyncio.run(client.create_data_requirement(payload))
        assert result.requirement_id == original["requirement_id"]
        assert payload == original
    assert captured[0] == captured[1]


@pytest.mark.parametrize("field, value", [
    ("requirement_id", "datareq_v1_" + "b" * 32),
    ("consumer_task_id", "another-task"),
    ("request_fingerprint", "b" * 64),
    ("quality_policy", "allow_unverified"),
    ("datasets", []),
])
def test_create_requirement_preserves_local_response_identity_checks(monkeypatch, field, value) -> None:
    payload = _requirement_payload()
    original = deepcopy(payload)
    response = _requirement_response(payload)
    response[field] = value
    if field == "datasets":
        response[field] = deepcopy(payload[field])
        response[field][0]["symbols"] = ["510300.SH"]
    monkeypatch.setattr(urllib.request, "urlopen", lambda *_args, **_kwargs: BytesIO(json.dumps(response).encode()))
    client = PxyDataSnapshotClient(base_url="http://pxydata", api_key="test-key")
    with pytest.raises(SnapshotProviderError) as caught:
        asyncio.run(client.create_data_requirement(payload))
    assert caught.value.status_code == 409
    if field == "requirement_id":
        assert "ID 不一致" in str(caught.value)
    assert payload == original
