"""生成 GOLD-001 快慢双均线固定向量的完整验收证据包。"""

from __future__ import annotations

import hashlib
import json
import platform
import re
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
DEPLOYED_PROJECTS = ("PXYBACKTEST", "PXYLH")
GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


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


def _deployment_matrix(deploy_root: Path) -> dict[str, dict[str, Any]]:
    matrix: dict[str, dict[str, Any]] = {}
    for name in DEPLOYED_PROJECTS:
        project_root = (deploy_root / name).resolve()
        current_path = project_root / "current"
        if not current_path.exists():
            raise ValueError(f"{name} 当前部署入口不存在：{current_path}")
        release_path = current_path.resolve(strict=True)
        releases_root = (project_root / "releases").resolve()
        try:
            release_path.relative_to(releases_root)
        except ValueError as exc:
            raise ValueError(
                f"{name} current 未指向受管 releases 目录：{release_path}"
            ) from exc

        manifest_path = release_path / ".pxy-release.json"
        if not manifest_path.is_file():
            raise ValueError(f"{name} 发布清单不存在：{manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        if not isinstance(manifest, dict):
            raise ValueError(f"{name} 发布清单必须是 JSON 对象")
        if manifest.get("project") != name:
            raise ValueError(f"{name} 发布清单项目身份不匹配")
        commit = str(manifest.get("commit", "")).lower()
        if not GIT_COMMIT_PATTERN.fullmatch(commit):
            raise ValueError(f"{name} 发布清单 commit 不是完整 Git SHA")
        if manifest.get("source") != "git_archive":
            raise ValueError(f"{name} 发布来源不是固定 Git commit")
        if manifest.get("working_tree_dirty") is not False:
            raise ValueError(f"{name} 发布清单包含可变工作区")
        snapshot_sha256 = str(manifest.get("snapshot_sha256", "")).lower()
        if not SHA256_PATTERN.fullmatch(snapshot_sha256):
            raise ValueError(f"{name} 发布快照 SHA-256 无效")

        matrix[name] = {
            "current_path": str(current_path),
            "release_path": str(release_path),
            "release_manifest_path": str(manifest_path),
            "release_manifest_sha256": _file_sha256(manifest_path),
            "commit": commit,
            "source": manifest["source"],
            "working_tree_dirty": manifest["working_tree_dirty"],
            "snapshot_sha256": snapshot_sha256,
            "created_at": manifest.get("created_at"),
        }
    return matrix


def generate_vnpy_gold_evidence(
    *,
    output_dir: Path,
    reviewer: str,
    vector_path: Path,
    workspace_root: Path | None = None,
    deploy_root: Path | None = None,
    actual_factory: Callable[[], dict[str, Any]] = run_vnpy_acceptance_vector,
    repositories: Mapping[str, Mapping[str, Any]] | None = None,
    deployments: Mapping[str, Mapping[str, Any]] | None = None,
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
    deployment_matrix = (
        {name: dict(value) for name, value in deployments.items()}
        if deployments is not None
        else _deployment_matrix(deploy_root.resolve())
        if deploy_root is not None
        else {}
    )
    if not deployment_matrix:
        raise ValueError("GOLD-001 部署矩阵为空，请显式指定部署根目录")
    missing_deployments = set(DEPLOYED_PROJECTS) - deployment_matrix.keys()
    if missing_deployments:
        raise ValueError(
            "GOLD-001 部署矩阵缺少项目：" + ", ".join(sorted(missing_deployments))
        )
    repository_commit = str(
        repository_matrix.get("PXYBACKTEST", {}).get("commit", "")
    ).lower()
    deployed_commit = str(deployment_matrix["PXYBACKTEST"].get("commit", "")).lower()
    if repository_commit != deployed_commit:
        raise ValueError(
            "PXYBACKTEST 源码提交与 E 盘运行提交不一致："
            f"{repository_commit or '<missing>'} != {deployed_commit or '<missing>'}"
        )
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
        "deployment_matrix": deployment_matrix,
        "data_snapshot": request["data"]["snapshot"],
        "execution_profile": request["execution"],
        "test": {
            "command": (
                "python -m app.cli evidence-vnpy "
                "--output-dir <dir> --reviewer <reviewer> "
                "--workspace-root <workspace> --deploy-root <deploy>"
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
