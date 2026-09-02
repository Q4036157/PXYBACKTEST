"""跨平台策略运行器注册表。

注册表只报告可由文件与适配器事实证明的能力。检测到第三方终端不等于已经
接通回测 worker，更不等于已经通过逐笔成交、账户和可视化一致性验收。
"""

from __future__ import annotations

import os
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field

from .strategy_package import (
    EVENT_ENVELOPE_CONTRACT,
    STRATEGY_PACKAGE_CONTRACT,
    EventKind,
    RunnerMode,
    SourcePlatform,
    StrategyPackage,
    VerificationLevel,
)
from .parity_acceptance import PARITY_ACCEPTANCE_CONTRACT
from .tqsdk_acceptance import load_tqsdk_acceptance_gate
from .windows_sandbox import network_policy_attestation


RUNNER_REGISTRY_CONTRACT = "pxybacktest.runner-registry.v1"
PARITY_DIMENSIONS = ("trades", "account", "visual")


class RunnerCapability(BaseModel):
    """一个运行器在当前工作站上的可证明状态。"""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.runner-registry.v1"] = (
        RUNNER_REGISTRY_CONTRACT
    )
    runner_id: str = Field(min_length=1, max_length=100)
    platform: SourcePlatform
    integration_phase: int = Field(ge=1)
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    aliases: list[str] = Field(default_factory=list)
    modes: list[RunnerMode] = Field(min_length=1)
    execution_semantics: list[str] = Field(min_length=1)
    event_kinds: list[EventKind] = Field(default_factory=list)
    runtime_detected: bool
    adapter_ready: bool
    submit_ready: bool
    runtime_identity: str | None = None
    verification_level: VerificationLevel = VerificationLevel.IMPORTED
    parity_dimensions: dict[
        Literal["trades", "account", "visual"],
        Literal["not_verified", "passed", "failed"],
    ] = Field(
        default_factory=lambda: {
            "trades": "not_verified",
            "account": "not_verified",
            "visual": "not_verified",
        }
    )
    reason: str | None = None


@dataclass(frozen=True)
class RunnerProbeConfig:
    """可注入的运行时探测位置，避免测试依赖工作站固定目录。"""

    project_root: Path
    pxylh_root: Path
    tqsdk_python: Path | None
    mt4_terminal: Path
    mt5_terminal: Path
    tqsdk_acceptance_path: Path | None = None

    @classmethod
    def from_environment(
        cls,
        *,
        pxylh_root: Path,
        environ: Mapping[str, str] | None = None,
        project_root: Path | None = None,
    ) -> "RunnerProbeConfig":
        values = environ if environ is not None else os.environ
        tq_raw = values.get("PXYBACKTEST_TQSDK_PYTHON", "").strip()
        tq_acceptance_raw = values.get(
            "PXYBACKTEST_TQSDK_ACCEPTANCE_FILE", ""
        ).strip()
        return cls(
            project_root=(project_root or Path(__file__).resolve().parents[1]),
            pxylh_root=pxylh_root,
            tqsdk_python=Path(tq_raw) if tq_raw else None,
            mt4_terminal=Path(
                values.get(
                    "PXYBACKTEST_MT4_TERMINAL",
                    r"D:\x1\x2\MetaTrader 4 EXNESS\terminal.exe",
                )
            ),
            mt5_terminal=Path(
                values.get(
                    "PXYBACKTEST_MT5_TERMINAL",
                    r"D:\x1\x2\MetaTrader 5 EXNESS\terminal64.exe",
                )
            ),
            tqsdk_acceptance_path=(
                Path(tq_acceptance_raw) if tq_acceptance_raw else None
            ),
        )


class RunnerRegistry:
    """解析策略包应该交给哪个已登记运行器。"""

    def __init__(self, capabilities: list[RunnerCapability]):
        self._capabilities = tuple(capabilities)

    @property
    def capabilities(self) -> tuple[RunnerCapability, ...]:
        return self._capabilities

    def catalog(self) -> list[dict]:
        return [item.model_dump(mode="json") for item in self._capabilities]

    def resolve(self, package: StrategyPackage) -> dict:
        platform_candidates = [
            item
            for item in self._capabilities
            if item.platform == package.source.platform
        ]
        requested_adapter = package.runner.adapter_id.strip().lower()
        capability = next(
            (
                item
                for item in platform_candidates
                if requested_adapter
                in {
                    item.runner_id.lower(),
                    item.adapter_id.lower(),
                    *(alias.lower() for alias in item.aliases),
                }
            ),
            None,
        )
        if capability is None:
            if platform_candidates:
                reason = (
                    f"平台 {package.source.platform.value} 未登记适配器 "
                    f"{package.runner.adapter_id}"
                )
            else:
                reason = f"平台 {package.source.platform.value} 尚未接入运行器注册表"
            return self._resolution(package, None, reason=reason)

        if package.runner.mode not in capability.modes:
            return self._resolution(
                package,
                capability,
                reason=(
                    f"运行器 {capability.runner_id} 不支持模式 "
                    f"{package.runner.mode.value}"
                ),
                resolved=False,
            )
        if package.execution.semantics not in capability.execution_semantics:
            return self._resolution(
                package,
                capability,
                reason=(
                    f"运行器 {capability.runner_id} 不支持执行语义 "
                    f"{package.execution.semantics}"
                ),
                resolved=False,
            )
        return self._resolution(
            package,
            capability,
            reason=capability.reason,
            resolved=True,
        )

    @staticmethod
    def _resolution(
        package: StrategyPackage,
        capability: RunnerCapability | None,
        *,
        reason: str,
        resolved: bool = False,
    ) -> dict:
        submit_ready = bool(resolved and capability and capability.submit_ready)
        return {
            "contract_valid": True,
            "resolved": resolved,
            "submit_ready": submit_ready,
            "reason": None if submit_ready else reason,
            "requested": {
                "platform": package.source.platform.value,
                "mode": package.runner.mode.value,
                "adapter_id": package.runner.adapter_id,
                "runtime_identity": package.runner.runtime_identity,
            },
            "runner": (
                capability.model_dump(mode="json") if capability is not None else None
            ),
        }


def _venv_module_installed(python: Path | None, module: str) -> bool:
    if python is None or not python.is_file():
        return False
    venv_root = python.parent.parent
    candidates = [
        venv_root / "Lib" / "site-packages" / module,
        venv_root / "lib" / "site-packages" / module,
    ]
    lib_root = venv_root / "lib"
    if lib_root.is_dir():
        candidates.extend(lib_root.glob(f"python*/site-packages/{module}"))
    return any(path.is_dir() or path.with_suffix(".py").is_file() for path in candidates)


def _venv_distribution_version(python: Path | None, distribution: str) -> str | None:
    """只读取 dist-info 元数据，不在 API 进程导入第三方运行时。"""

    if python is None or not python.is_file():
        return None
    venv_root = python.parent.parent
    site_roots = [
        venv_root / "Lib" / "site-packages",
        venv_root / "lib" / "site-packages",
    ]
    lib_root = venv_root / "lib"
    if lib_root.is_dir():
        site_roots.extend(lib_root.glob("python*/site-packages"))
    normalized = distribution.replace("-", "_").lower()
    for site_root in site_roots:
        if not site_root.is_dir():
            continue
        for metadata in sorted(site_root.glob("*.dist-info/METADATA")):
            stem = metadata.parent.name.removesuffix(".dist-info")
            if not stem.lower().startswith(f"{normalized}-"):
                continue
            try:
                for line in metadata.read_text(encoding="utf-8").splitlines():
                    if line.startswith("Version:"):
                        return line.partition(":")[2].strip() or None
            except OSError:
                continue
    return None


def build_runner_registry(config: RunnerProbeConfig) -> RunnerRegistry:
    """从当前源码与显式运行时路径构建注册表，不执行第三方代码。"""

    pxylh_backend = config.pxylh_root / "backend"
    vnpy_adapter_files = (
        pxylh_backend / "services" / "backtest_service" / "engine_runner.py",
        pxylh_backend / "services" / "backtest_service" / "models.py",
        config.project_root / "app" / "worker_process.py",
    )
    vnpy_runtime_files = (
        config.pxylh_root / "venv312" / "Scripts" / "python.exe",
        config.pxylh_root / "vnpy",
    )
    vnpy_runtime_detected = all(path.exists() for path in vnpy_runtime_files)
    vnpy_adapter_ready = all(path.is_file() for path in vnpy_adapter_files)
    vnpy_submit_ready = vnpy_runtime_detected and vnpy_adapter_ready

    tq_python_configured = bool(
        config.tqsdk_python is not None and config.tqsdk_python.is_file()
    )
    tq_version = _venv_distribution_version(config.tqsdk_python, "tqsdk")
    tq_runtime_detected = bool(
        _venv_module_installed(config.tqsdk_python, "tqsdk")
        and tq_version
        and tq_version.startswith("3.10.")
    )
    tq_strategy_path = (
        config.project_root
        / "app"
        / "acceptance_strategies"
        / "tqsdk_native_v1.py"
    )
    tq_strategy_sha256 = (
        hashlib.sha256(tq_strategy_path.read_bytes()).hexdigest()
        if tq_strategy_path.is_file()
        else None
    )
    tq_adapter_ready = (
        config.project_root / "app" / "tqsdk_native_worker.py"
    ).is_file() and all(
        (config.project_root / "app" / name).is_file()
        for name in (
            "windows_sandbox.py",
            "tqsdk_replay.py",
            "tqsdk_submission.py",
            "tqsdk_acceptance.py",
            "worker_process.py",
        )
    ) and tq_strategy_sha256 is not None
    network_enforced, _network_policy_id, network_policy_sha256 = (
        network_policy_attestation()
    )
    tq_gate = load_tqsdk_acceptance_gate(
        config.tqsdk_acceptance_path,
        network_policy_sha256=network_policy_sha256,
        runtime_identity=f"tqsdk-{tq_version}" if tq_version else None,
        strategy_source_sha256=tq_strategy_sha256,
    )
    tq_parity = {
        name: (
            str(tq_gate.evidence[name].get("status") or "not_verified")
            if tq_gate is not None
            else "not_verified"
        )
        for name in PARITY_DIMENSIONS
    }
    tq_submit_ready = bool(
        tq_runtime_detected
        and tq_adapter_ready
        and os.name == "nt"
        and network_enforced
        and tq_gate is not None
        and tq_gate.all_passed
    )
    if tq_submit_ready:
        tq_reason = None
    elif not tq_runtime_detected:
        tq_reason = (
            "专用 Python 尚未安装 tqsdk 3.10.x"
            if tq_python_configured
            else "未配置专用 TqSdk Python（PXYBACKTEST_TQSDK_PYTHON）"
        )
    elif os.name != "nt":
        tq_reason = "天勤原生安全沙箱仅在 Windows 工作站开放"
    elif not network_enforced:
        tq_reason = "受限令牌和 Job Object 已实现，但 PXYOPS 网络白名单部署证明缺失"
    elif tq_gate is None:
        tq_reason = "天勤真实固定向量的逐笔成交、账户和可视化三维门禁尚未通过"
    else:
        tq_reason = "天勤安全或三维验收门禁不完整"
    mt4_runtime_detected = config.mt4_terminal.is_file()
    mt5_runtime_detected = config.mt5_terminal.is_file()

    market_events = [
        EventKind.BAR,
        EventKind.TICK,
        EventKind.QUOTE,
        EventKind.TRADE,
    ]
    return RunnerRegistry(
        [
            RunnerCapability(
                runner_id="vnpy_cta",
                platform=SourcePlatform.VNPY,
                integration_phase=1,
                adapter_id="vnpy-cta",
                adapter_version="legacy-v1",
                aliases=["vnpy_cta"],
                modes=[RunnerMode.NATIVE_SANDBOX, RunnerMode.COMPAT],
                execution_semantics=["vnpy_cta"],
                event_kinds=market_events,
                runtime_detected=vnpy_runtime_detected,
                adapter_ready=vnpy_adapter_ready,
                submit_ready=vnpy_submit_ready,
                runtime_identity=("pxylh-vnpy-cta" if vnpy_runtime_detected else None),
                reason=(
                    None
                    if vnpy_submit_ready
                    else "PXYLH vn.py 运行时或 CTA worker 适配器不完整"
                ),
            ),
            RunnerCapability(
                runner_id="tqsdk_native",
                platform=SourcePlatform.TQSDK,
                integration_phase=1,
                adapter_id="tqsdk-native",
                adapter_version="windows-restricted-v1",
                aliases=["tqsdk_native"],
                modes=[RunnerMode.NATIVE_SANDBOX, RunnerMode.COMPAT],
                execution_semantics=["tqsdk"],
                event_kinds=market_events,
                runtime_detected=tq_runtime_detected,
                adapter_ready=tq_adapter_ready,
                submit_ready=tq_submit_ready,
                runtime_identity=(
                    tq_gate.runtime_identity
                    if tq_gate is not None
                    else f"tqsdk-{tq_version}"
                    if tq_runtime_detected
                    else None
                ),
                verification_level=(
                    VerificationLevel.PARITY_VERIFIED
                    if tq_submit_ready
                    else VerificationLevel.IMPORTED
                ),
                parity_dimensions=tq_parity,  # type: ignore[arg-type]
                reason=tq_reason,
            ),
            RunnerCapability(
                runner_id="mt4_native",
                platform=SourcePlatform.MT4,
                integration_phase=2,
                adapter_id="mt4-native",
                adapter_version="planned-v1",
                aliases=["mt4_native"],
                modes=[RunnerMode.NATIVE_ORACLE],
                execution_semantics=["mt4"],
                event_kinds=market_events,
                runtime_detected=mt4_runtime_detected,
                adapter_ready=False,
                submit_ready=False,
                runtime_identity=("mt4-terminal" if mt4_runtime_detected else None),
                reason=(
                    "检测到 MT4 终端，但自动 Strategy Tester worker 尚未接入"
                    if mt4_runtime_detected
                    else "未检测到 MT4 终端"
                ),
            ),
            RunnerCapability(
                runner_id="mt5_native",
                platform=SourcePlatform.MT5,
                integration_phase=2,
                adapter_id="mt5-native",
                adapter_version="oracle-report-v1",
                aliases=["mt5_native"],
                modes=[RunnerMode.NATIVE_ORACLE],
                execution_semantics=["mt5_hedging", "mt5_netting"],
                event_kinds=market_events,
                runtime_detected=mt5_runtime_detected,
                adapter_ready=False,
                submit_ready=False,
                runtime_identity=("mt5-terminal64" if mt5_runtime_detected else None),
                reason=(
                    "检测到 MT5 终端且已有报告比较器，但自动 Strategy Tester worker 尚未接入"
                    if mt5_runtime_detected
                    else "未检测到 MT5 终端"
                ),
            ),
        ]
    )


def runner_contract_capabilities() -> dict:
    """与运行状态无关的稳定契约声明。"""

    return {
        "strategy_package": STRATEGY_PACKAGE_CONTRACT,
        "event_envelope": EVENT_ENVELOPE_CONTRACT,
        "runner_registry": RUNNER_REGISTRY_CONTRACT,
        "parity_acceptance": PARITY_ACCEPTANCE_CONTRACT,
        "parity_gate": {
            "required_dimensions": list(PARITY_DIMENSIONS),
            "rule": "all_dimensions_must_pass",
        },
    }


__all__ = [
    "PARITY_DIMENSIONS",
    "RUNNER_REGISTRY_CONTRACT",
    "RunnerCapability",
    "RunnerProbeConfig",
    "RunnerRegistry",
    "build_runner_registry",
    "runner_contract_capabilities",
]
