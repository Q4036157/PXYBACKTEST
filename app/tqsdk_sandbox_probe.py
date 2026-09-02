"""管理员验收使用的天勤专用账户导入探针。

探针不连接天勤网络，也不读取天勤账号。它只使用专用沙箱账户启动与正式
worker 相同的受限进程，定位第一个无法导入的 Python/原生模块。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence

from .tqsdk_native_worker import _isolated_python_path
from .windows_sandbox import (
    SandboxIdentity,
    SandboxLimits,
    launch_sandboxed_process,
)


_PROBES: tuple[tuple[str, str], ...] = (
    ("python", "pass"),
    ("encoding_gbk", "import encodings.gbk"),
    ("codecs_cn", "import _codecs_cn, _multibytecodec"),
    ("ssl", "import _ssl"),
    ("aiohttp", "import aiohttp.log"),
    ("charset_normalizer", "import charset_normalizer"),
    ("tqsdk", "import tqsdk"),
)


def _child_environment(python_path: Path, task_root: Path) -> dict[str, str]:
    system_root = Path(os.environ.get("SYSTEMROOT") or r"C:\Windows")
    profile = task_root / ".sandbox-profile"
    temp = profile / "Temp"
    appdata = profile / "AppData" / "Roaming"
    local_appdata = profile / "AppData" / "Local"
    for directory in (profile, temp, appdata, local_appdata):
        directory.mkdir(parents=True, exist_ok=True)
    environment = {
        name: os.environ[name]
        for name in ("SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT")
        if os.environ.get(name)
    }
    environment.update(
        {
            "PATH": _isolated_python_path(python_path, system_root),
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
    return environment


def run_sandbox_import_probes(
    *,
    python_path: Path,
    runtime_root: Path,
    sandbox_user: str,
    sandbox_password: str,
) -> list[dict[str, Any]]:
    """逐项运行无网络导入探针，遇到第一个失败立即停止。"""

    python_path = python_path.resolve()
    if not python_path.is_file():
        raise FileNotFoundError(f"天勤 Python 不存在: {python_path}")
    probe_root = runtime_root.resolve() / "acceptance" / "probe"
    probe_root.mkdir(parents=True, exist_ok=True)
    identity = SandboxIdentity(username=sandbox_user, password=sandbox_password)
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(
        prefix="pxy-tqsdk-import-probe-", dir=probe_root
    ) as temporary:
        task_root = Path(temporary).resolve()
        environment = _child_environment(python_path, task_root)
        error_path = task_root / "probe-error.log"
        for name, statement in _PROBES:
            error_path.unlink(missing_ok=True)
            child_code = (
                "import traceback\n"
                "try:\n"
                f" {statement}\n"
                "except BaseException:\n"
                " open('probe-error.log','w',encoding='utf-8').write("
                "traceback.format_exc())\n"
                " raise\n"
            )
            completed = launch_sandboxed_process(
                [
                    str(python_path),
                    "-X",
                    "utf8",
                    "-c",
                    child_code,
                ],
                cwd=task_root,
                environment=environment,
                limits=SandboxLimits(timeout_seconds=30, memory_mb=1024),
                identity=identity,
            )
            diagnostic = ""
            if error_path.is_file():
                diagnostic = error_path.read_text(
                    encoding="utf-8", errors="replace"
                )
                diagnostic = diagnostic.replace(sandbox_password, "<redacted>")
            result = {
                "probe": name,
                "exit_code": completed.exit_code,
                "exit_code_hex": f"0x{completed.exit_code:08X}",
                "process_creation_api": completed.process_creation_api,
                "diagnostic": " ".join(diagnostic.split())[-2000:],
            }
            results.append(result)
            if completed.exit_code != 0:
                break
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="诊断天勤 Windows 专用账户导入")
    parser.add_argument("--python", required=True, type=Path)
    parser.add_argument("--runtime-root", required=True, type=Path)
    parser.add_argument("--sandbox-user", required=True)
    parser.add_argument("--sandbox-password-file", required=True, type=Path)
    args = parser.parse_args(argv)

    password = args.sandbox_password_file.read_text(encoding="utf-8").strip()
    if not password:
        raise RuntimeError("天勤沙箱账户密码文件为空")
    try:
        results = run_sandbox_import_probes(
            python_path=args.python,
            runtime_root=args.runtime_root,
            sandbox_user=args.sandbox_user,
            sandbox_password=password,
        )
    finally:
        password = ""
    for result in results:
        status = "OK" if result["exit_code"] == 0 else "FAIL"
        print(
            f"[{status}] {result['probe']}: exit={result['exit_code']} "
            f"({result['exit_code_hex']}), API={result['process_creation_api']}"
        )
        if result["diagnostic"]:
            print(f"       Python diagnostic: {result['diagnostic']}")
    print(json.dumps({"probes": results}, ensure_ascii=False))
    return 0 if results and results[-1]["exit_code"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

