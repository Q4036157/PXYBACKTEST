from __future__ import annotations

import os
import tempfile


# 模块导入会创建默认 FastAPI 应用；测试必须与 E 盘生产运行库隔离。
os.environ.setdefault(
    "PXYBACKTEST_RUNTIME_ROOT",
    tempfile.mkdtemp(prefix="pxybacktest-tests-"),
)
