from __future__ import annotations

import json
import urllib.request
from typing import Self

from app.config import Settings
from app.pxydata_client import PxyDataSnapshotClient


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
