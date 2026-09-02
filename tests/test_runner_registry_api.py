from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


class IdleManager:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


class UnconfiguredSnapshots:
    @property
    def configured(self) -> bool:
        return False


class UnconfiguredDaa:
    @property
    def configured(self) -> bool:
        return False


def _headers() -> dict[str, str]:
    return {
        "X-PXY-Service-Token": "test-service-token",
        "X-PXY-User-Id": "user-a",
        "X-PXY-Source-Node": "204",
    }


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        runtime_root=tmp_path / "runtime",
        pxylh_root=tmp_path / "PXYLH",
        service_token="test-service-token",
        daa_root=tmp_path / "DAA",
        pxydata_data_root=tmp_path / "PXYDATA" / "data",
    )


def _mt5_package() -> dict:
    return {
        "strategy_id": "123-knight",
        "version": "sha256:test",
        "source": {
            "platform": "mt5",
            "language": "mql5",
            "entrypoint": "123骑士.ex5",
            "artifacts": [
                {
                    "artifact_id": "source",
                    "file_name": "123骑士.mq5",
                    "sha256": "a" * 64,
                    "media_type": "text/plain",
                    "role": "source",
                    "size_bytes": 10,
                },
                {
                    "artifact_id": "binary",
                    "file_name": "123骑士.ex5",
                    "sha256": "b" * 64,
                    "media_type": "application/octet-stream",
                    "role": "binary",
                    "size_bytes": 20,
                },
            ],
            "license_policy": "user_supplied",
        },
        "runner": {
            "mode": "native_oracle",
            "adapter_id": "mt5-native",
            "adapter_version": "1",
            "runtime_identity": "mt5-requested-build",
        },
        "subscriptions": [{"kind": "tick", "symbols": ["XAUUSDm"]}],
        "execution": {
            "semantics": "mt5_hedging",
            "initial_cash": 100_000,
            "leverage": 100,
            "matching_model": "native",
            "position_mode": "hedging",
        },
    }


def test_capabilities_publish_contract_and_honest_runner_states(
    tmp_path: Path, monkeypatch
) -> None:
    mt5_terminal = tmp_path / "mt5" / "terminal64.exe"
    mt5_terminal.parent.mkdir(parents=True)
    mt5_terminal.write_bytes(b"terminal")
    monkeypatch.setenv("PXYBACKTEST_MT5_TERMINAL", str(mt5_terminal))
    monkeypatch.setenv(
        "PXYBACKTEST_MT4_TERMINAL", str(tmp_path / "missing-mt4.exe")
    )
    app = create_app(
        _settings(tmp_path),
        IdleManager(),  # type: ignore[arg-type]
        UnconfiguredSnapshots(),  # type: ignore[arg-type]
        UnconfiguredDaa(),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.get("/api/v2/capabilities", headers=_headers())

    assert response.status_code == 200
    payload = response.json()
    contracts = payload["strategy_runtime_contracts"]
    assert contracts["strategy_package"] == "pxybacktest.strategy-package.v1"
    assert contracts["event_envelope"] == "pxybacktest.event-envelope.v1"
    assert contracts["parity_gate"]["required_dimensions"] == [
        "trades",
        "account",
        "visual",
    ]
    mt5 = next(item for item in payload["runners"] if item["runner_id"] == "mt5_native")
    assert mt5["runtime_detected"] is True
    assert mt5["adapter_ready"] is False
    assert mt5["submit_ready"] is False


def test_validate_strategy_package_resolves_without_submitting(
    tmp_path: Path, monkeypatch
) -> None:
    mt5_terminal = tmp_path / "mt5" / "terminal64.exe"
    mt5_terminal.parent.mkdir(parents=True)
    mt5_terminal.write_bytes(b"terminal")
    monkeypatch.setenv("PXYBACKTEST_MT5_TERMINAL", str(mt5_terminal))
    app = create_app(
        _settings(tmp_path),
        IdleManager(),  # type: ignore[arg-type]
        UnconfiguredSnapshots(),  # type: ignore[arg-type]
        UnconfiguredDaa(),  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/v2/strategy-packages/validate",
            json=_mt5_package(),
            headers=_headers(),
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract_valid"] is True
    assert payload["resolved"] is True
    assert payload["submit_ready"] is False
    assert payload["runner"]["runtime_detected"] is True


def test_validate_strategy_package_rejects_mt5_without_binary(
    tmp_path: Path,
) -> None:
    app = create_app(
        _settings(tmp_path),
        IdleManager(),  # type: ignore[arg-type]
        UnconfiguredSnapshots(),  # type: ignore[arg-type]
        UnconfiguredDaa(),  # type: ignore[arg-type]
    )
    package = _mt5_package()
    package["source"]["artifacts"] = package["source"]["artifacts"][:1]

    with TestClient(app) as client:
        response = client.post(
            "/api/v2/strategy-packages/validate",
            json=package,
            headers=_headers(),
        )

    assert response.status_code == 422
    assert "binary artifact" in response.text
