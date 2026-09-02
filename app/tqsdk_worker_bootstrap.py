"""天勤受限子进程的短命令行入口。

``CreateProcessWithTokenW`` 的命令行上限为 1024 字符，因此这里
使用 ``python -m`` 加载诊断逻辑，不将大段 bootstrap 源码放入 ``-c``。
"""

from __future__ import annotations

import argparse
import os
import runpy
import sys
import traceback
from pathlib import Path
from typing import Sequence


_BOOTSTRAP_ERROR_ENV = "PXYBACKTEST_TQSDK_BOOTSTRAP_ERROR"
_IMPORT_TRACE_ENV = "PXYBACKTEST_TQSDK_IMPORT_TRACE"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动天勤受限策略进程")
    parser.add_argument("--request", required=True)
    args = parser.parse_args(argv)

    error_path = Path(os.environ[_BOOTSTRAP_ERROR_ENV])
    trace_path = Path(os.environ[_IMPORT_TRACE_ENV])
    trace_handle = trace_path.open("a", encoding="utf-8", buffering=1)

    def record_import(event: str, audit_args: tuple[object, ...]) -> None:
        if event == "import" and audit_args:
            trace_handle.write(str(audit_args[0]) + "\n")
            trace_handle.flush()

    sys.addaudithook(record_import)
    trace_handle.write("<bootstrap:run_module>\n")
    original_argv = sys.argv[:]
    sys.argv = ["app.tqsdk_native_worker", "--request", str(args.request)]
    try:
        runpy.run_module(
            "app.tqsdk_native_worker", run_name="__main__", alter_sys=True
        )
    except SystemExit as exc:
        if exc.code not in (None, 0) and not error_path.exists():
            error_path.write_text(traceback.format_exc(), encoding="utf-8")
        raise
    except BaseException:
        error_path.write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        sys.argv = original_argv
        trace_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())

