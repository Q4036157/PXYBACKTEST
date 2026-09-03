from __future__ import annotations

import json
import os
from pathlib import Path

from app import tqsdk_service_identity_probe


def test_service_identity_python_probe_uses_task_local_profile(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for name in (
        "PATH",
        "PYTHONUTF8",
        "PYTHONIOENCODING",
        "PYTHONNOUSERSITE",
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "TEMP",
        "TMP",
    ):
        monkeypatch.setitem(os.environ, name, os.environ.get(name, ""))
    profile = tmp_path / "profile"
    output = tmp_path / "python.json"

    code = tqsdk_service_identity_probe.main(
        [
            "--probe",
            "python",
            "--profile-root",
            str(profile),
            "--output",
            str(output),
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "passed"
    assert payload["profile_root"] == str(profile.resolve())
    assert (profile / "Temp").is_dir()
    assert (profile / "AppData" / "Roaming").is_dir()
    assert (profile / "AppData" / "Local").is_dir()
