"""生成 GOLD-001 快慢双均线固定向量的完整验收证据包。"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .kernel import stable_hash
from .parity_acceptance import AcceptanceVector, compare_acceptance_vector
from .vnpy_acceptance import run_vnpy_acceptance_vector

EVIDENCE_CONTRACT_VERSION = "pxybacktest.gold-evidence.v1"
ACCEPTANCE_STANDARD_VERSION = "backtest-platform.acceptance.v1"
TASK_CONTRACT_VERSION = "pxybacktest.task.v2"
RESULT_CONTRACT_VERSION = "pxybacktest.task-result.v2"


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_output(repository: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
    )
    return completed.stdout


def _repository_matrix(workspace_root: Path) -> dict[str, dict[str, Any]]:
    matrix: dict[str, dict[str, Any]] = {}
    for name in ("PXYBACKTEST", "PXYLH", "PXYDATA", "DAA", "PXYOPS"):
        repository = workspace_root / name
        if not (repository / ".git").exists():
            continue
        diff = _git_output(repository, "diff", "--binary", "HEAD", "--")
        matrix[name] = {
            "repository": str(repository.resolve()),
            "commit": _git_output(repository, "rev-parse", "HEAD")
            .decode("ascii")
            .strip(),
            "tracked_worktree_dirty": bool(diff),
            "tracked_worktree_diff_sha256": hashlib.sha256(diff).hexdigest(),
        }
    return matrix


def generate_vnpy_gold_evidence(
    *,
    output_dir: Path,
    reviewer: str,
    vector_path: Path,
    workspace_root: Path | None = None,
    actual_factory: Callable[[], dict[str, Any]] = run_vnpy_acceptance_vector,
    repositories: Mapping[str, Mapping[str, Any]] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """运行固定 Oracle，并生成不可覆盖的 GOLD-001 证据目录。"""

    reviewer_name = reviewer.strip()
    if not reviewer_name:
        raise ValueError("复核人（reviewer）不能为空")
    repository_matrix = (
        {name: dict(value) for name, value in repositories.items()}
        if repositories is not None
        else _repository_matrix(
            workspace_root.resolve()
            if workspace_root is not None
            else Path(__file__).parents[2]
        )
    )
    if not repository_matrix:
        raise ValueError("GOLD-001 仓库矩阵为空，请显式指定工作区根目录")
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=False)

    started_at = now().astimezone(UTC)
    vector_payload = json.loads(vector_path.read_text(encoding="utf-8"))
    vector = AcceptanceVector.model_validate(vector_payload)
    actual = actual_factory()
    acceptance = compare_acceptance_vector(vector, actual)
    ended_at = now().astimezone(UTC)
    if not acceptance.all_passed:
        raise ValueError("GOLD-001 三维验收失败，拒绝生成通过证据包")

    task_id = str(uuid.uuid4())
    snapshot_id = f"btsnap_gold001_{vector.data_manifest_sha256[:32]}"
    request = {
        "schema_version": 2,
        "contract_version": TASK_CONTRACT_VERSION,
        "engine_type": "vnpy_cta",
        "strategy": {
            "id": "gold-001-dual-moving-average",
            "version": "1.0.0",
            "source_hash": vector.strategy_source_sha256,
            "entrypoint": "VnpyCtaAcceptanceV1",
        },
        "universe": {"symbols": ["TEST.LOCAL"]},
        "period": {
            "start": "2026-01-05T09:00:00+08:00",
            "end": "2026-01-05T09:07:00+08:00",
            "interval": "1m",
            "timezone": "Asia/Shanghai",
        },
        "data": {
            "snapshot": {
                "contract_version": "pxydata.backtest-data-snapshot.v1",
                "snapshot_id": snapshot_id,
                "manifest_sha256": vector.data_manifest_sha256,
                "data_type": "deterministic_synthetic_bars",
                "bar_count": 8,
            }
        },
        "execution": {
            "capital": 100_000,
            "rate": 0.0001,
            "slippage": 0.2,
            "contract_size": 10,
            "price_tick": 0.2,
            "mode": "BAR",
            "replay_mode": "fast",
        },
        "parameters": {"fast_window": 2, "slow_window": 3},
        "random_seed": 0,
    }
    task_result = {
        "schema_version": 2,
        "contract_version": RESULT_CONTRACT_VERSION,
        "task_id": task_id,
        "complete": True,
        "termination_reason": "completed",
        "engine_type": "vnpy_cta",
        "strategy": request["strategy"],
        "data_snapshot": request["data"]["snapshot"],
        "run": {
            "universe": request["universe"],
            "period": request["period"],
            "execution": request["execution"],
            "parameters": request["parameters"],
            "random_seed": request["random_seed"],
        },
        "orders": actual.get("orders", []),
        "deals": actual.get("deals", []),
        "execution_snapshot": actual.get("execution_snapshot", {}),
        "replay_audit": actual.get("replay_audit", {}),
        "diagnostics": actual.get("diagnostics", {}),
        "reproducibility": {
            "input_contract_sha256": stable_hash(request),
            "strategy_source_sha256": vector.strategy_source_sha256,
            "data_manifest_sha256": vector.data_manifest_sha256,
            "event_log_sha256": (actual.get("replay_audit") or {}).get("chain_sha256"),
        },
    }
    task_result["reproducibility"]["result_sha256"] = stable_hash(task_result)

    artifacts = {
        "request.json": request,
        "vector.json": vector_payload,
        "oracle-actual.json": actual,
        "acceptance-result.json": acceptance.model_dump(mode="json"),
        "task-result.json": task_result,
    }
    for filename, payload in artifacts.items():
        _write_json(output_dir / filename, payload)

    artifact_sha256 = {
        filename: _file_sha256(output_dir / filename) for filename in artifacts
    }
    manifest = {
        "contract_version": EVIDENCE_CONTRACT_VERSION,
        "acceptance_standard_version": ACCEPTANCE_STANDARD_VERSION,
        "golden_case_id": "GOLD-001",
        "golden_case": "CTA 快慢双均线固定合约/快照",
        "task_id": task_id,
        "task_contract_version": TASK_CONTRACT_VERSION,
        "result_contract_version": RESULT_CONTRACT_VERSION,
        "parity_contract_version": vector.contract_version,
        "vector_id": vector.vector_id,
        "status": "passed",
        "repository_matrix": repository_matrix,
        "data_snapshot": request["data"]["snapshot"],
        "execution_profile": request["execution"],
        "test": {
            "command": (
                "python -m app.cli evidence-vnpy "
                "--output-dir <dir> --reviewer <reviewer>"
            ),
            "exit_code": 0,
            "all_passed": True,
            "dimensions": {
                name: getattr(acceptance, name).status
                for name in ("trades", "account", "visual")
            },
        },
        "artifact_sha256": artifact_sha256,
        "run_node": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "python_executable": sys.executable,
            "python_version": platform.python_version(),
            "runtime_identity": vector.runtime_identity,
        },
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "reviewer": reviewer_name,
    }
    _write_json(output_dir / "manifest.json", manifest)
    checksummed_files = [*artifacts, "manifest.json"]
    checksum_lines = [
        f"{_file_sha256(output_dir / filename)}  {filename}"
        for filename in checksummed_files
    ]
    (output_dir / "SHA256SUMS.txt").write_text(
        "\n".join(checksum_lines) + "\n", encoding="ascii"
    )
    return {
        "all_passed": True,
        "golden_case_id": "GOLD-001",
        "task_id": task_id,
        "evidence_dir": str(output_dir),
        "manifest_sha256": _file_sha256(output_dir / "manifest.json"),
        "checksums_sha256": _file_sha256(output_dir / "SHA256SUMS.txt"),
    }


__all__ = ["generate_vnpy_gold_evidence"]
