from __future__ import annotations

import os
import shutil
import tempfile


# 模块导入会创建默认 FastAPI 应用；测试必须与 E 盘生产运行库隔离。
# 只有本次测试自己创建的运行目录才在会话结束时删除；外部指定的目录不动。
_CREATED_RUNTIME_ROOT: str | None = None
if "PXYBACKTEST_RUNTIME_ROOT" not in os.environ:
    _CREATED_RUNTIME_ROOT = tempfile.mkdtemp(prefix="pxybacktest-tests-")
    os.environ["PXYBACKTEST_RUNTIME_ROOT"] = _CREATED_RUNTIME_ROOT


def pytest_sessionfinish(session, exitstatus) -> None:
    # 被占用而删不掉的残留交给 PXYOPS 每日临时目录清理任务，不让测试因清理失败。
    if _CREATED_RUNTIME_ROOT:
        shutil.rmtree(_CREATED_RUNTIME_ROOT, ignore_errors=True)
