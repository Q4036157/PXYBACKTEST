from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path

import pytest

from app.runner_registry import RunnerProbeConfig, build_runner_registry
from app.strategy_package import StrategyPackage


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"test")


def _install_fake_module(
    python: Path, module: str, *, version: str = "3.10.2"
) -> None:
    _touch(python)
    site_packages = python.parent.parent / "Lib" / "site-packages"
    package = site_packages / module
    package.mkdir(parents=True)
    _touch(package / "__init__.py")
    metadata = site_packages / f"{module}-{version}.dist-info" / "METADATA"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(
        f"Name: {module}\nVersion: {version}\n", encoding="utf-8"
    )


def _configure_network_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tq_python: Path,
    policy_sha: str,
) -> None:
    effective_python = tmp_path / "base-python" / "python.exe"
    _touch(effective_python)
    python_sha = hashlib.sha256(tq_python.read_bytes()).hexdigest()
    effective_sha = hashlib.sha256(effective_python.read_bytes()).hexdigest()
    policy_path = tmp_path / "tqsdk-network-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "contract_version": "pxyops.tqsdk-network-policy.v1",
                "policy_id": "tqsdk-egress-v1",
                "enforced": True,
                "remote_port": 443,
                "rule_scope": "sandbox_account_all_programs",
                "python_path": str(tq_python),
                "python_sha256": python_sha,
                "effective_python_path": str(effective_python),
                "effective_python_sha256": effective_sha,
                "programs": [
                    {"path": str(tq_python), "sha256": python_sha},
                    {"path": str(effective_python), "sha256": effective_sha},
                ],
                "policy_sha256": policy_sha,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PXYBACKTEST_TQSDK_PYTHON", str(tq_python))
    monkeypatch.setenv("PXYBACKTEST_TQSDK_NETWORK_POLICY_FILE", str(policy_path))


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
    _install_fake_module(tq_python, "tqsdk")
    for path in (mt4, mt5):
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


def test_tqsdk_process_worker_is_adapter_ready_but_not_safe_to_submit(
    tmp_path: Path,
) -> None:
    tq_python = tmp_path / "tq" / "python.exe"
    _install_fake_module(tq_python, "tqsdk")
    for name in (
        "tqsdk_native_worker.py",
        "windows_sandbox.py",
        "tqsdk_replay.py",
        "tqsdk_submission.py",
        "tqsdk_acceptance.py",
        "worker_process.py",
    ):
        _touch(tmp_path / "PXYBACKTEST" / "app" / name)
    _touch(
        tmp_path
        / "PXYBACKTEST"
        / "app"
        / "acceptance_strategies"
        / "tqsdk_native_v1.py"
    )

    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=tmp_path / "PXYBACKTEST",
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=tq_python,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
        )
    )

    tqsdk = next(
        item for item in registry.capabilities if item.runner_id == "tqsdk_native"
    )
    assert tqsdk.runtime_detected is True
    assert tqsdk.adapter_ready is True
    assert tqsdk.submit_ready is False
    assert "受限令牌" in str(tqsdk.reason)


@pytest.mark.skipif(os.name != "nt", reason="天勤安全门禁只在 Windows 开放")
def test_tqsdk_submit_opens_only_with_network_and_three_dimension_gate(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "PXYBACKTEST"
    app_root = project_root / "app"
    app_root.mkdir(parents=True)
    for name in (
        "tqsdk_native_worker.py",
        "windows_sandbox.py",
        "tqsdk_replay.py",
        "tqsdk_submission.py",
        "tqsdk_acceptance.py",
        "worker_process.py",
    ):
        (app_root / name).write_text("# adapter", encoding="utf-8")
    acceptance_strategy = app_root / "acceptance_strategies" / "tqsdk_native_v1.py"
    acceptance_strategy.parent.mkdir(parents=True)
    acceptance_strategy.write_text("# fixed vector", encoding="utf-8")
    strategy_sha = hashlib.sha256(acceptance_strategy.read_bytes()).hexdigest()
    tq_python = tmp_path / "tq" / "Scripts" / "python.exe"
    _install_fake_module(tq_python, "tqsdk")
    policy_sha = "c" * 64
    _configure_network_policy(
        tmp_path, monkeypatch, tq_python=tq_python, policy_sha=policy_sha
    )
    gate_path = tmp_path / "acceptance.json"
    gate_path.write_text(
        json.dumps(
            {
                "contract_version": "pxybacktest.tqsdk-acceptance-gate.v1",
                "vector_id": "tqsdk-native-au2612-1m-v1",
                "accepted_at": "2026-09-03T00:00:00Z",
                "runtime_identity": "tqsdk-3.10.2",
                "strategy_source_sha256": strategy_sha,
                "data_manifest_sha256": "b" * 64,
                "network_policy_sha256": policy_sha,
                "sandbox": {
                    "restricted_token": True,
                    "job_object": True,
                    "dedicated_identity": True,
                    "task_directory_acl": True,
                    "filesystem_isolated": True,
                    "network_allowlist_enforced": True,
                    "submit_ready": True,
                },
                "evidence": {
                    name: {"status": "passed", "evidence_sha256": char * 64}
                    for name, char in (("trades", "d"), ("account", "e"), ("visual", "f"))
                },
            }
        ),
        encoding="utf-8",
    )
    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=project_root,
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=tq_python,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
            tqsdk_acceptance_path=gate_path,
        )
    )

    tqsdk = next(
        item for item in registry.capabilities if item.runner_id == "tqsdk_native"
    )
    assert tqsdk.submit_ready is True
    assert tqsdk.verification_level.value == "parity_verified"
    assert set(tqsdk.parity_dimensions.values()) == {"passed"}


@pytest.mark.skipif(os.name != "nt", reason="天勤安全门禁只在 Windows 开放")
def test_tqsdk_submit_rejects_stale_runtime_or_strategy_gate(
    tmp_path: Path, monkeypatch
) -> None:
    project_root = tmp_path / "PXYBACKTEST"
    app_root = project_root / "app"
    for name in (
        "tqsdk_native_worker.py",
        "windows_sandbox.py",
        "tqsdk_replay.py",
        "tqsdk_submission.py",
        "tqsdk_acceptance.py",
        "worker_process.py",
    ):
        _touch(app_root / name)
    acceptance_strategy = app_root / "acceptance_strategies" / "tqsdk_native_v1.py"
    _touch(acceptance_strategy)
    tq_python = tmp_path / "tq" / "Scripts" / "python.exe"
    _install_fake_module(tq_python, "tqsdk", version="3.10.2")
    policy_sha = "c" * 64
    _configure_network_policy(
        tmp_path, monkeypatch, tq_python=tq_python, policy_sha=policy_sha
    )
    gate_path = tmp_path / "acceptance.json"
    gate_path.write_text(
        json.dumps(
            {
                "contract_version": "pxybacktest.tqsdk-acceptance-gate.v1",
                "vector_id": "tqsdk-native-au2612-1m-v1",
                "accepted_at": "2026-09-03T00:00:00Z",
                "runtime_identity": "tqsdk-3.10.1",
                "strategy_source_sha256": "a" * 64,
                "data_manifest_sha256": "b" * 64,
                "network_policy_sha256": policy_sha,
                "sandbox": {
                    "restricted_token": True,
                    "job_object": True,
                    "dedicated_identity": True,
                    "task_directory_acl": True,
                    "filesystem_isolated": True,
                    "network_allowlist_enforced": True,
                    "submit_ready": True,
                },
                "evidence": {
                    name: {"status": "passed", "evidence_sha256": char * 64}
                    for name, char in (("trades", "d"), ("account", "e"), ("visual", "f"))
                },
            }
        ),
        encoding="utf-8",
    )
    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=project_root,
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=tq_python,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
            tqsdk_acceptance_path=gate_path,
        )
    )

    tqsdk = next(
        item for item in registry.capabilities if item.runner_id == "tqsdk_native"
    )
    assert tqsdk.runtime_detected is True
    assert tqsdk.adapter_ready is True
    assert tqsdk.submit_ready is False
    assert set(tqsdk.parity_dimensions.values()) == {"not_verified"}


def test_tqsdk_python_without_installed_package_is_not_runtime_detected(
    tmp_path: Path,
) -> None:
    tq_python = tmp_path / "tq" / "Scripts" / "python.exe"
    _touch(tq_python)
    registry = build_runner_registry(
        RunnerProbeConfig(
            project_root=Path(__file__).parents[1],
            pxylh_root=tmp_path / "PXYLH",
            tqsdk_python=tq_python,
            mt4_terminal=tmp_path / "mt4.exe",
            mt5_terminal=tmp_path / "mt5.exe",
        )
    )

    tqsdk = next(
        item for item in registry.capabilities if item.runner_id == "tqsdk_native"
    )
    assert tqsdk.runtime_detected is False
    assert tqsdk.adapter_ready is True
    assert tqsdk.submit_ready is False
    assert "尚未安装 tqsdk" in str(tqsdk.reason)


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
