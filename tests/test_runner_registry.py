from __future__ import annotations

from pathlib import Path

from app.runner_registry import RunnerProbeConfig, build_runner_registry
from app.strategy_package import StrategyPackage


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"test")


def _package(
    *,
    platform: str,
    adapter_id: str,
    mode: str,
    semantics: str,
    language: str,
    binary: bool = False,
) -> StrategyPackage:
    artifacts = [
        {
            "artifact_id": "source",
            "file_name": f"strategy.{language}",
            "sha256": "a" * 64,
            "media_type": "text/plain",
            "role": "source",
            "size_bytes": 10,
        }
    ]
    if binary:
        artifacts.append(
            {
                "artifact_id": "binary",
                "file_name": "strategy.ex5",
                "sha256": "b" * 64,
                "media_type": "application/octet-stream",
                "role": "binary",
                "size_bytes": 20,
            }
        )
    return StrategyPackage.model_validate(
        {
            "strategy_id": "strategy-1",
            "version": "v1",
            "source": {
                "platform": platform,
                "language": language,
                "entrypoint": "strategy",
                "artifacts": artifacts,
                "license_policy": "user_supplied",
            },
            "runner": {
                "mode": mode,
                "adapter_id": adapter_id,
                "adapter_version": "1",
                "runtime_identity": "requested-runtime",
            },
            "subscriptions": [
                {"kind": "tick", "symbols": ["XAUUSDm"]}
            ],
            "execution": {
                "semantics": semantics,
                "initial_cash": 100_000,
                "leverage": 100,
                "matching_model": "native" if binary else "tick_bid_ask",
                "position_mode": "hedging",
            },
        }
    )


def test_vnpy_requires_runtime_and_worker_adapter_before_submission(
    tmp_path: Path,
) -> None:
    pxylh = tmp_path / "PXYLH"
    project = tmp_path / "PXYBACKTEST"
    _touch(pxylh / "venv312" / "Scripts" / "python.exe")
    (pxylh / "vnpy").mkdir(parents=True)
    _touch(pxylh / "backend" / "services" / "backtest_service" / "engine_runner.py")
    _touch(pxylh / "backend" / "services" / "backtest_service" / "models.py")
    _touch(project / "app" / "worker_process.py")

    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=project,
            pxylh_root=pxylh,
            tqsdk_python=None,
            mt4_terminal=tmp_path / "missing-mt4.exe",
            mt5_terminal=tmp_path / "missing-mt5.exe",
        )
    )

    vnpy = next(item for item in registry.capabilities if item.runner_id == "vnpy_cta")
    assert vnpy.runtime_detected is True
    assert vnpy.adapter_ready is True
    assert vnpy.submit_ready is True

    result = registry.resolve(
        _package(
            platform="vnpy",
            adapter_id="vnpy-cta",
            mode="native_sandbox",
            semantics="vnpy_cta",
            language="python",
        )
    )
    assert result["resolved"] is True
    assert result["submit_ready"] is True


def test_detected_native_platform_is_not_reported_as_submit_ready(
    tmp_path: Path,
) -> None:
    tq_python = tmp_path / "tq" / "python.exe"
    mt4 = tmp_path / "mt4" / "terminal.exe"
    mt5 = tmp_path / "mt5" / "terminal64.exe"
    for path in (tq_python, mt4, mt5):
        _touch(path)

    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=tmp_path / "PXYBACKTEST",
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=tq_python,
            mt4_terminal=mt4,
            mt5_terminal=mt5,
        )
    )

    for runner_id in ("tqsdk_native", "mt4_native", "mt5_native"):
        capability = next(
            item for item in registry.capabilities if item.runner_id == runner_id
        )
        assert capability.runtime_detected is True
        assert capability.adapter_ready is False
        assert capability.submit_ready is False
        assert set(capability.parity_dimensions.values()) == {"not_verified"}

    result = registry.resolve(
        _package(
            platform="mt5",
            adapter_id="mt5-native",
            mode="native_oracle",
            semantics="mt5_hedging",
            language="mql5",
            binary=True,
        )
    )
    assert result["resolved"] is True
    assert result["submit_ready"] is False
    assert "worker 尚未接入" in result["reason"]


def test_runner_resolution_rejects_wrong_execution_semantics(tmp_path: Path) -> None:
    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=tmp_path / "PXYBACKTEST",
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=None,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
        )
    )
    package = _package(
        platform="vnpy",
        adapter_id="vnpy-cta",
        mode="compat",
        semantics="custom",
        language="python",
    )

    result = registry.resolve(package)

    assert result["resolved"] is False
    assert result["submit_ready"] is False
    assert "不支持执行语义" in result["reason"]


def test_unimplemented_later_phase_platform_does_not_resolve(tmp_path: Path) -> None:
    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=tmp_path / "PXYBACKTEST",
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=None,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
        )
    )
    package = _package(
        platform="tradingview",
        adapter_id="pine-ir",
        mode="compat",
        semantics="tradingview_bar",
        language="pine",
    )

    result = registry.resolve(package)

    assert result["resolved"] is False
    assert result["runner"] is None
    assert "尚未接入" in result["reason"]
