"""真实天勤固定向量的记录、复验和提交门禁。"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field

from .kernel import stable_hash
from .parity_acceptance import (
    AcceptanceVector,
    AcceptanceResult,
    compare_acceptance_vector,
)
from .tqsdk_native_worker import (
    TqSdkWorkerRequest,
    launch_tqsdk_worker,
)


TQSDK_ACCEPTANCE_VECTOR_ID = "tqsdk-native-au2612-1m-v1"
TQSDK_ACCEPTANCE_GATE_CONTRACT = "pxybacktest.tqsdk-acceptance-gate.v1"
TQSDK_TRUSTED_ACCEPTANCE_REPORT_CONTRACT = (
    "pxybacktest.tqsdk-trusted-acceptance-report.v1"
)
DEFAULT_SYMBOL = "SHFE.au2612"
DEFAULT_START_DATE = date(2026, 8, 18)
DEFAULT_END_DATE = date(2026, 8, 20)
DEFAULT_INITIAL_BALANCE = 1_000_000.0


class TqSdkAcceptanceGate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: str = TQSDK_ACCEPTANCE_GATE_CONTRACT
    vector_id: str
    accepted_at: str
    runtime_identity: str
    strategy_source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    data_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    network_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    sandbox: dict[str, Any]
    evidence: dict[str, Any]

    @property
    def all_passed(self) -> bool:
        dimensions = ("trades", "account", "visual")
        return bool(
            self.contract_version == TQSDK_ACCEPTANCE_GATE_CONTRACT
            and self.vector_id == TQSDK_ACCEPTANCE_VECTOR_ID
            and all(
                isinstance(self.evidence.get(name), Mapping)
                and self.evidence[name].get("status") == "passed"
                and bool(self.evidence[name].get("evidence_sha256"))
                for name in dimensions
            )
            and self.sandbox.get("restricted_token") is True
            and self.sandbox.get("job_object") is True
            and self.sandbox.get("dedicated_identity") is True
            and self.sandbox.get("task_directory_acl") is True
            and self.sandbox.get("filesystem_isolated") is True
            and self.sandbox.get("network_allowlist_enforced") is True
            and self.sandbox.get("submit_ready") is True
        )


def acceptance_strategy_path() -> Path:
    return Path(__file__).parent / "acceptance_strategies" / "tqsdk_native_v1.py"


def _python_path() -> Path:
    configured = os.getenv("PXYBACKTEST_TQSDK_PYTHON", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).parents[1] / ".venv" / "Scripts" / "python.exe"


def run_tqsdk_acceptance_candidate(
    *,
    execution_lane: Literal[
        "dedicated_sandbox", "trusted_fixed_vector"
    ] = "dedicated_sandbox",
) -> dict[str, Any]:
    """在真实 TqSdk/TqBacktest 上执行固定黄金向量。"""

    strategy = acceptance_strategy_path()
    source_hash = hashlib.sha256(strategy.read_bytes()).hexdigest()
    runtime_root = Path(
        os.getenv("PXYBACKTEST_RUNTIME_ROOT", r"E:\pxy-runtime\PXYBACKTEST")
    ).resolve()
    work_root = runtime_root / "acceptance" / "work"
    work_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="pxy-tqsdk-accept-", dir=work_root
    ) as temporary:
        task_root = Path(temporary).resolve()
        copied_strategy = task_root / strategy.name
        copied_strategy.write_bytes(strategy.read_bytes())
        native = launch_tqsdk_worker(
            TqSdkWorkerRequest(
                task_id=TQSDK_ACCEPTANCE_VECTOR_ID,
                task_root=task_root,
                strategy_path=copied_strategy,
                result_path=task_root / "result.json",
                start_date=DEFAULT_START_DATE,
                end_date=DEFAULT_END_DATE,
                initial_balance=DEFAULT_INITIAL_BALANCE,
                memory_mb=2048,
                cpu_cores=1,
            ),
            python_executable=_python_path(),
            project_root=Path(__file__).parents[1],
            timeout_seconds=600,
            identity_mode=(
                "current_process"
                if execution_lane == "trusted_fixed_vector"
                else "configured"
            ),
        )
    visual = dict(native.get("visual") or {})
    if not visual.get("available") or int(visual.get("bar_history_count") or 0) < 1:
        raise RuntimeError("真实天勤固定向量没有捕获到完整 K 线可视化历史")
    return {
        "strategy": {"source_hash": source_hash},
        "data_snapshot": {
            "manifest_sha256": native["data_manifest_sha256"],
            "provider": "tqsdk",
            "symbol": DEFAULT_SYMBOL,
        },
        "diagnostics": {
            "runtime_identity": native["runtime_identity"],
            "sandbox": {
                **native["sandbox"],
                "execution_lane": execution_lane,
                "trusted_code_only": execution_lane == "trusted_fixed_vector",
                # 可信固定向量只能证明功能一致性，不能开放用户策略提交。
                "submit_ready": False
                if execution_lane == "trusted_fixed_vector"
                else bool(native["sandbox"].get("submit_ready")),
            },
        },
        "deals": native.get("deals") or [],
        "account_curve": native.get("account_curve") or [],
        "final_account": native.get("final_account") or {},
        "execution_snapshot": native.get("execution_snapshot") or {},
        "replay_audit": native.get("replay_audit") or {},
    }


def run_tqsdk_trusted_acceptance() -> dict[str, Any]:
    """连续运行两次内置可信向量，只生成三维功能验收报告。"""

    first = run_tqsdk_acceptance_candidate(
        execution_lane="trusted_fixed_vector"
    )
    vector = build_tqsdk_acceptance_vector(first)
    second = run_tqsdk_acceptance_candidate(
        execution_lane="trusted_fixed_vector"
    )
    result = compare_acceptance_vector(vector, second)
    return {
        "contract_version": TQSDK_TRUSTED_ACCEPTANCE_REPORT_CONTRACT,
        "vector_id": vector.vector_id,
        "execution_lane": "trusted_fixed_vector",
        "trusted_code_only": True,
        "all_passed": result.all_passed,
        "submit_ready": False,
        "vector": vector.model_dump(mode="json"),
        "first_actual": first,
        "second_actual": second,
        "acceptance": result.model_dump(mode="json"),
    }


def build_tqsdk_acceptance_vector(actual: Mapping[str, Any]) -> AcceptanceVector:
    """从首次真实原生执行记录 Oracle；必须再独立执行一次才能放行。"""

    strategy_hash = str(actual["strategy"]["source_hash"])
    data_hash = str(actual["data_snapshot"]["manifest_sha256"])
    runtime_identity = str(actual["diagnostics"]["runtime_identity"])
    payload = {
        "vector_id": TQSDK_ACCEPTANCE_VECTOR_ID,
        "platform": "tqsdk",
        "strategy_source_sha256": strategy_hash,
        "data_manifest_sha256": data_hash,
        "runtime_identity": runtime_identity,
        "identity_checks": [
            {"path": "strategy.source_hash", "expected": strategy_hash},
            {"path": "data_snapshot.manifest_sha256", "expected": data_hash},
            {"path": "diagnostics.runtime_identity", "expected": runtime_identity},
        ],
        "trades": {
            "checks": [
                {"path": "deals", "expected_sha256": stable_hash(actual["deals"])}
            ]
        },
        "account": {
            "checks": [
                {
                    "path": "account_curve",
                    "expected_sha256": stable_hash(actual["account_curve"]),
                },
                {
                    "path": "final_account",
                    "expected_sha256": stable_hash(actual["final_account"]),
                },
            ]
        },
        "visual": {
            "checks": [
                {
                    "path": "execution_snapshot.bar_history",
                    "expected_sha256": stable_hash(
                        actual["execution_snapshot"]["bar_history"]
                    ),
                },
                {
                    "path": "replay_audit",
                    "expected_sha256": stable_hash(actual["replay_audit"]),
                },
            ]
        },
        "metadata": {
            "symbol": DEFAULT_SYMBOL,
            "interval": "1m",
            "start_date": DEFAULT_START_DATE.isoformat(),
            "end_date": DEFAULT_END_DATE.isoformat(),
            "initial_balance": DEFAULT_INITIAL_BALANCE,
            "recording_rule": "首次执行记录 Oracle，第二次独立执行复验后才能生成门禁",
        },
    }
    return AcceptanceVector.model_validate(payload)


def build_tqsdk_acceptance_gate(
    *,
    vector: AcceptanceVector,
    actual: Mapping[str, Any],
    result: AcceptanceResult,
) -> TqSdkAcceptanceGate:
    if not result.all_passed:
        raise ValueError("天勤三维验收未全部通过，不能生成提交门禁")
    sandbox = dict((actual.get("diagnostics") or {}).get("sandbox") or {})
    if not sandbox.get("submit_ready"):
        raise ValueError("天勤 Windows 沙箱或网络白名单未完整实施")
    policy_hash = str(sandbox.get("network_policy_sha256") or "").lower()
    return TqSdkAcceptanceGate(
        vector_id=vector.vector_id,
        accepted_at=datetime.now(timezone.utc).isoformat(),
        runtime_identity=vector.runtime_identity,
        strategy_source_sha256=vector.strategy_source_sha256,
        data_manifest_sha256=vector.data_manifest_sha256,
        network_policy_sha256=policy_hash,
        sandbox=sandbox,
        evidence={
            name: getattr(result, name).model_dump(mode="json")
            for name in ("trades", "account", "visual")
        },
    )


def load_tqsdk_acceptance_gate(
    path: Path | None,
    *,
    network_policy_sha256: str | None,
    runtime_identity: str | None = None,
    strategy_source_sha256: str | None = None,
) -> TqSdkAcceptanceGate | None:
    if path is None or not path.is_file() or not network_policy_sha256:
        return None
    try:
        gate = TqSdkAcceptanceGate.model_validate_json(
            path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        return None
    if not gate.all_passed:
        return None
    if gate.network_policy_sha256 != network_policy_sha256.lower():
        return None
    if runtime_identity and gate.runtime_identity != runtime_identity:
        return None
    if (
        strategy_source_sha256
        and gate.strategy_source_sha256 != strategy_source_sha256.lower()
    ):
        return None
    return gate


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


__all__ = [
    "TQSDK_ACCEPTANCE_GATE_CONTRACT",
    "TQSDK_TRUSTED_ACCEPTANCE_REPORT_CONTRACT",
    "TQSDK_ACCEPTANCE_VECTOR_ID",
    "TqSdkAcceptanceGate",
    "build_tqsdk_acceptance_gate",
    "build_tqsdk_acceptance_vector",
    "load_tqsdk_acceptance_gate",
    "run_tqsdk_acceptance_candidate",
    "run_tqsdk_trusted_acceptance",
    "write_json",
]
