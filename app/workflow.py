"""BeeQuant 类节点工作流的服务端契约。

工作流只描述研究图，不直接触发订单。``live_signal`` 节点的输出仍需经过
PXYLH 的预览、风控和人工确认；PXYBACKTEST 不持有账户状态。
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


WorkflowNodeType = Literal[
    "data_source",
    "feature_engineering",
    "model_training",
    "model_ensemble",
    "custom_data",
    "portfolio",
    "risk",
    "backtest",
    "report",
    "live_signal",
    "llm_signal",
]


class WorkflowNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$")
    type: WorkflowNodeType
    depends_on: list[str] = Field(default_factory=list, max_length=16)
    config: dict[str, Any] = Field(default_factory=dict)


class WorkflowSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    workflow_id: str = Field(min_length=1, max_length=120)
    name: str = Field(min_length=1, max_length=200)
    nodes: list[WorkflowNode] = Field(min_length=2, max_length=64)
    mode: Literal["research", "paper", "live_signal"] = "research"

    @model_validator(mode="after")
    def validate_graph(self) -> "WorkflowSpec":
        validate_workflow(self.model_dump(mode="python"))
        return self


def validate_workflow(payload: dict[str, Any]) -> dict[str, Any]:
    """验证节点唯一、依赖存在且无环，返回拓扑顺序和执行边界。"""
    raw_nodes = payload.get("nodes") or []
    if not isinstance(raw_nodes, list) or len(raw_nodes) < 2:
        raise ValueError("workflow 至少需要 2 个节点")
    nodes: dict[str, dict[str, Any]] = {}
    for raw in raw_nodes:
        if not isinstance(raw, dict):
            raise ValueError("workflow 节点格式无效")
        node_id = str(raw.get("id") or "")
        if not node_id or node_id in nodes:
            raise ValueError(f"workflow 节点 id 重复或为空: {node_id}")
        nodes[node_id] = raw
    indegree = {node_id: 0 for node_id in nodes}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for node_id, node in nodes.items():
        dependencies = list(dict.fromkeys(str(item) for item in node.get("depends_on") or []))
        for dependency in dependencies:
            if dependency not in nodes:
                raise ValueError(f"workflow 依赖不存在: {node_id} -> {dependency}")
            if dependency == node_id:
                raise ValueError(f"workflow 不允许自依赖: {node_id}")
            outgoing[dependency].append(node_id)
            indegree[node_id] += 1
    queue = deque(sorted(node_id for node_id, degree in indegree.items() if degree == 0))
    order: list[str] = []
    while queue:
        node_id = queue.popleft()
        order.append(node_id)
        for child in sorted(outgoing[node_id]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if len(order) != len(nodes):
        raise ValueError("workflow 存在循环依赖")

    types = [str(node.get("type") or "") for node in nodes.values()]
    if types.count("data_source") != 1:
        raise ValueError("workflow 必须且只能有一个 data_source 节点")
    if types.count("backtest") != 1:
        raise ValueError("workflow 必须且只能有一个 backtest 节点")
    backtest_id = next(node_id for node_id, node in nodes.items() if node.get("type") == "backtest")
    source_id = next(node_id for node_id, node in nodes.items() if node.get("type") == "data_source")
    if source_id not in _ancestors(backtest_id, nodes):
        raise ValueError("backtest 必须依赖 data_source（可经由特征/模型/组合/风控节点）")
    mode = str(payload.get("mode") or "research")
    live_ids = [node_id for node_id, node in nodes.items() if node.get("type") == "live_signal"]
    if mode == "live_signal" and not live_ids:
        raise ValueError("live_signal 模式必须包含 live_signal 节点")
    for node_id in live_ids:
        if backtest_id not in _ancestors(node_id, nodes):
            raise ValueError("live_signal 必须依赖 backtest，且只产生信号")
    llm_ids = [node_id for node_id, node in nodes.items() if node.get("type") == "llm_signal"]
    if mode == "research" and llm_ids:
        raise ValueError("llm_signal 只能用于 paper 或 live_signal 模式")
    for node_id in llm_ids:
        if backtest_id not in _ancestors(node_id, nodes):
            raise ValueError("llm_signal 必须依赖 backtest，且只产生信号")
    custom_ids = [node_id for node_id, node in nodes.items() if node.get("type") == "custom_data"]
    for node_id in custom_ids:
        if source_id not in _ancestors(node_id, nodes) and node_id != source_id:
            raise ValueError("custom_data 必须位于 data_source 之后")
    ensemble_ids = [node_id for node_id, node in nodes.items() if node.get("type") == "model_ensemble"]
    for node_id in ensemble_ids:
        ancestors = _ancestors(node_id, nodes)
        if sum(nodes[item].get("type") == "model_training" for item in ancestors) < 2:
            raise ValueError("model_ensemble 至少需要两个上游 model_training 节点")
    return {
        "valid": True,
        "topological_order": order,
        "execution_boundary": "signal_only_no_order_submission",
        "research_backends": {"qlib": "optional_provider", "rd_agent": "optional_orchestrator"},
    }


def _ancestors(node_id: str, nodes: dict[str, dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    stack = list(nodes[node_id].get("depends_on") or [])
    while stack:
        current = str(stack.pop())
        if current in result:
            continue
        result.add(current)
        stack.extend(nodes[current].get("depends_on") or [])
    return result
