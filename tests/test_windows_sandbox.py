from __future__ import annotations

import os
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from app.windows_sandbox import (
    NETWORK_POLICY_ID_ENV,
    NETWORK_POLICY_FILE_ENV,
    NETWORK_POLICY_SHA256_ENV,
    SandboxCancelledError,
    SandboxIdentity,
    SandboxLimits,
    _set_task_directory_access,
    launch_sandboxed_process,
    network_policy_attestation,
)


def _environment() -> dict[str, str]:
    return {
        name: os.environ[name]
        for name in ("SYSTEMROOT", "WINDIR", "PATH", "TEMP", "TMP")
        if os.environ.get(name)
    }


def test_task_acl_is_applied_and_checked_per_existing_path(
    tmp_path: Path, monkeypatch
) -> None:
    strategy = tmp_path / "strategy.py"
    request = tmp_path / "request.json"
    strategy.write_text("pass\n", encoding="utf-8")
    request.write_text("{}", encoding="utf-8")
    calls: list[list[str]] = []

    def record(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", record)
    identity = SandboxIdentity(username="PXYTqSandbox", password="unused")

    _set_task_directory_access(tmp_path, identity, grant=True)

    assert {Path(command[1]) for command in calls} == {tmp_path, strategy, request}
    assert all("/T" not in command and "/C" not in command for command in calls)
    file_calls = [command for command in calls if Path(command[1]).is_file()]
    assert all("PXYTqSandbox:M" in command for command in file_calls)


@pytest.mark.skipif(os.name != "nt", reason="仅验证 Windows 强制边界")
def test_windows_sandbox_uses_restricted_token_and_job_object(tmp_path: Path) -> None:
    output = tmp_path / "child.txt"
    code = (
        "from pathlib import Path; "
        f"Path(r'{output}').write_text('ok', encoding='utf-8')"
    )
    result = launch_sandboxed_process(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        environment=_environment(),
        limits=SandboxLimits(timeout_seconds=5, memory_mb=512),
    )

    assert result.exit_code == 0
    assert result.restricted_token is True
    assert result.job_object is True
    assert result.process_creation_api in {
        "CreateProcessAsUserW",
        "CreateProcessWithTokenW",
    }
    assert result.security_state()["process_creation_api"] == result.process_creation_api
    assert output.read_text(encoding="utf-8") == "ok"


@pytest.mark.skipif(os.name != "nt", reason="仅验证 Windows 强制边界")
def test_windows_sandbox_cancellation_terminates_process_tree(tmp_path: Path) -> None:
    checks = 0

    def cancel() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 2

    with pytest.raises(SandboxCancelledError, match="已取消"):
        launch_sandboxed_process(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            cwd=tmp_path,
            environment=_environment(),
            limits=SandboxLimits(timeout_seconds=5, memory_mb=512),
            cancel_check=cancel,
        )


def test_network_policy_requires_id_and_sha256() -> None:
    assert network_policy_attestation({}) == (False, None, None)
    assert network_policy_attestation(
        {
            NETWORK_POLICY_ID_ENV: "tqsdk-egress-v1",
            NETWORK_POLICY_SHA256_ENV: "a" * 64,
        }
    ) == (False, None, None)


def test_network_policy_file_covers_venv_and_effective_python(
    tmp_path: Path,
) -> None:
    configured_python = tmp_path / "venv-python.exe"
    effective_python = tmp_path / "base-python.exe"
    configured_python.write_bytes(b"venv-launcher")
    effective_python.write_bytes(b"base-interpreter")

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    policy = {
        "contract_version": "pxyops.tqsdk-network-policy.v1",
        "policy_id": "tqsdk-egress-v1",
        "enforced": True,
        "remote_port": 443,
        "rule_scope": "sandbox_account_all_programs",
        "python_path": str(configured_python),
        "python_sha256": digest(configured_python),
        "effective_python_path": str(effective_python),
        "effective_python_sha256": digest(effective_python),
        "programs": [
            {"path": str(configured_python), "sha256": digest(configured_python)},
            {"path": str(effective_python), "sha256": digest(effective_python)},
        ],
        "policy_sha256": "b" * 64,
    }
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    environ = {
        NETWORK_POLICY_FILE_ENV: str(policy_path),
        "PXYBACKTEST_TQSDK_PYTHON": str(configured_python),
    }

    assert network_policy_attestation(environ) == (
        True,
        "tqsdk-egress-v1",
        "b" * 64,
    )

    effective_python.write_bytes(b"changed-base-interpreter")
    assert network_policy_attestation(environ) == (False, None, None)


def test_network_policy_file_rejects_changed_venv_launcher(tmp_path: Path) -> None:
    configured_python = tmp_path / "venv-python.exe"
    effective_python = tmp_path / "base-python.exe"
    configured_python.write_bytes(b"venv-launcher")
    effective_python.write_bytes(b"base-interpreter")
    configured_hash = hashlib.sha256(configured_python.read_bytes()).hexdigest()
    effective_hash = hashlib.sha256(effective_python.read_bytes()).hexdigest()
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "contract_version": "pxyops.tqsdk-network-policy.v1",
                "policy_id": "tqsdk-egress-v1",
                "enforced": True,
                "remote_port": 443,
                "rule_scope": "sandbox_account_all_programs",
                "python_path": str(configured_python),
                "python_sha256": configured_hash,
                "effective_python_path": str(effective_python),
                "effective_python_sha256": effective_hash,
                "programs": [
                    {"path": str(configured_python), "sha256": configured_hash},
                    {"path": str(effective_python), "sha256": effective_hash},
                ],
                "policy_sha256": "c" * 64,
            }
        ),
        encoding="utf-8",
    )
    configured_python.write_bytes(b"changed-venv-launcher")

    assert network_policy_attestation(
        {
            NETWORK_POLICY_FILE_ENV: str(policy_path),
            "PXYBACKTEST_TQSDK_PYTHON": str(configured_python),
        }
    ) == (False, None, None)
