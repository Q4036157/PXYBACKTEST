from __future__ import annotations

import sys
from pathlib import Path

from app import tqsdk_sandbox_probe


class _Completed:
    def __init__(self, exit_code: int) -> None:
        self.exit_code = exit_code
        self.process_creation_api = "CreateProcessWithTokenW"


def test_probe_stops_at_first_failure_and_keeps_profile_inside_task(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[tuple[list[str], dict[str, str], Path]] = []
    exit_codes = iter((0, 0, 0xC06D007E))

    def launch(command: list[str], **kwargs: object) -> _Completed:
        environment = kwargs["environment"]
        cwd = kwargs["cwd"]
        assert isinstance(environment, dict)
        assert isinstance(cwd, Path)
        calls.append((command, environment, cwd))
        return _Completed(next(exit_codes))

    monkeypatch.setattr(tqsdk_sandbox_probe, "launch_sandboxed_process", launch)
    results = tqsdk_sandbox_probe.run_sandbox_import_probes(
        python_path=Path(sys.executable),
        runtime_root=tmp_path,
        sandbox_user="PXYTqSandbox",
        sandbox_password="do-not-print-this",
    )

    assert [result["probe"] for result in results] == [
        "python",
        "encoding_gbk",
        "codecs_cn",
    ]
    assert results[-1]["exit_code_hex"] == "0xC06D007E"
    assert "do-not-print-this" not in str(results)
    for command, environment, cwd in calls:
        assert len(command) == 5
        profile = cwd / ".sandbox-profile"
        assert environment["HOME"] == str(profile)
        assert environment["USERPROFILE"] == str(profile)
        assert environment["TEMP"] == str(profile / "Temp")
        assert environment["TMP"] == str(profile / "Temp")
