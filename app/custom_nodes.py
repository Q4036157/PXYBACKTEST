"""受控自定义数据处理节点。

节点只能从工作站配置的 ``custom_nodes_root`` 加载，必须通过 SHA256 校验；
context 刻意不暴露 exchange、订单、账户和网络对象。该机制面向受信任的本地
研究代码，不把任意 Python 解释器暴露到公网 API。
"""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CustomNodeError(ValueError):
    """自定义节点不满足受控加载约束。"""


def validate_custom_data_node(spec: CustomDataNodeSpec, *, root: str | Path) -> dict[str, Any]:
    base = Path(root).resolve()
    path = (base / spec.module).resolve()
    try:
        path.relative_to(base)
    except ValueError as exc:
        raise CustomNodeError("自定义节点路径越出受控目录") from exc
    if not path.is_file():
        raise CustomNodeError("自定义节点文件不存在")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != spec.source_hash.lower():
        raise CustomNodeError("自定义节点 SHA256 不一致")
    return {"valid": True, "module": spec.module, "source_hash": digest, "entrypoint": spec.entrypoint}


class CustomDataNodeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    module: str = Field(min_length=1, max_length=240, pattern=r"^[A-Za-z0-9_./-]+\.py$")
    source_hash: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    entrypoint: str = Field(default="main", pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
    max_rows: int = Field(default=200_000, ge=1, le=1_000_000)


class CustomDataNodeRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec: CustomDataNodeSpec
    datas: list[Any] = Field(default_factory=list, max_length=200_000)
    context: dict[str, Any] = Field(default_factory=dict, max_length=64)


def run_custom_data_node(
    spec: CustomDataNodeSpec,
    *,
    root: str | Path,
    datas: list[Any],
    context: dict[str, Any] | None = None,
) -> Any:
    base = Path(root).resolve()
    path = (base / spec.module).resolve()
    try:
        path.relative_to(base)
    except ValueError as exc:
        raise CustomNodeError("自定义节点路径越出受控目录") from exc
    validate_custom_data_node(spec, root=base)
    module_spec = importlib.util.spec_from_file_location("pxy_custom_node", path)
    if module_spec is None or module_spec.loader is None:
        raise CustomNodeError("无法加载自定义节点")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    entrypoint = getattr(module, spec.entrypoint, None)
    if not callable(entrypoint):
        raise CustomNodeError(f"自定义节点缺少 {spec.entrypoint}(ctx, datas)")
    safe_context = {
        "log": lambda message: str(message)[:1000],
        "mode": "research",
        **dict(context or {}),
    }
    safe_context.pop("exchange", None)
    safe_context.pop("signal", None)
    safe_context.pop("orders", None)
    result = entrypoint(safe_context, datas)
    if isinstance(result, list) and len(result) > spec.max_rows:
        raise CustomNodeError("自定义节点输出超过 max_rows")
    return result
