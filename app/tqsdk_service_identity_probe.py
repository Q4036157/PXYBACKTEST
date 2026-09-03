"""在常驻 Worker 候选身份中执行单个无网络导入探针。"""

from __future__ import annotations

import argparse
import getpass
import importlib
import json
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from .tqsdk_native_worker import _isolated_python_path


_PROBE_MODULES = {
    "python": None,
    "encoding_gbk": "encodings.gbk",
    "codecs_cn": "_codecs_cn",
    "ssl": "_ssl",
    "aiohttp": "aiohttp.log",
    "charset_normalizer": "charset_normalizer",
    "tqsdk": "tqsdk",
}


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="验证天勤常驻 Worker 账户兼容性")
    parser.add_argument("--probe", required=True, choices=tuple(_PROBE_MODULES))
    parser.add_argument("--profile-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)

    profile = args.profile_root.resolve()
    output = args.output.resolve()
    if not output.is_relative_to(profile.parent):
        raise ValueError("探针输出必须位于任务根目录")
    temp = profile / "Temp"
    appdata = profile / "AppData" / "Roaming"
    local_appdata = profile / "AppData" / "Local"
    for directory in (profile, temp, appdata, local_appdata):
        directory.mkdir(parents=True, exist_ok=True)
    os.environ.update(
        {
            "PATH": _isolated_python_path(
                Path(os.sys.executable).resolve(),
                Path(os.environ.get("SYSTEMROOT") or r"C:\Windows"),
            ),
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONNOUSERSITE": "1",
            "HOME": str(profile),
            "USERPROFILE": str(profile),
            "APPDATA": str(appdata),
            "LOCALAPPDATA": str(local_appdata),
            "TEMP": str(temp),
            "TMP": str(temp),
        }
    )
    base = {
        "probe": args.probe,
        "username": getpass.getuser(),
        "python": str(Path(os.sys.executable).resolve()),
        "profile_root": str(profile),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(output, {**base, "status": "started"})
    try:
        module = _PROBE_MODULES[args.probe]
        if module:
            importlib.import_module(module)
    except BaseException as exc:  # noqa: BLE001 - SystemExit 也必须落盘
        _write_json(
            output,
            {
                **base,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc()[-4000:],
            },
        )
        return 1
    _write_json(
        output,
        {
            **base,
            "status": "passed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
