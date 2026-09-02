"""天勤原生策略提交契约。

该契约只负责把不可变 StrategyPackage 和源码物化为现有用户隔离任务队列请求；
API 仍由 runner 的安全/三维验收门禁决定是否允许提交。
"""

from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .strategy_package import (
    EventKind,
    RunnerMode,
    SourcePlatform,
    StrategyPackage,
)


TQSDK_SUBMISSION_CONTRACT = "pxybacktest.tqsdk-task-submission.v1"


def _tq_symbol(value: str) -> str:
    """恢复天勤的“交易所大写、品种小写”合约格式。"""

    exchange, separator, instrument = value.strip().partition(".")
    if not separator:
        return value.strip()
    matched = re.fullmatch(r"([A-Za-z]+)(.*)", instrument)
    normalized_instrument = (
        f"{matched.group(1).lower()}{matched.group(2)}"
        if matched
        else instrument
    )
    return f"{exchange.upper()}.{normalized_instrument}"


class TqSdkTaskSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.tqsdk-task-submission.v1"] = (
        TQSDK_SUBMISSION_CONTRACT
    )
    package: StrategyPackage
    source_code: str = Field(min_length=1, max_length=2_000_000)
    start_date: date
    end_date: date
    execution_mode: Literal["visual", "fast"] = "visual"
    speed: float = Field(default=50, ge=1, le=100)

    @model_validator(mode="after")
    def validate_native_submission(self) -> "TqSdkTaskSubmission":
        package = self.package
        if self.end_date < self.start_date:
            raise ValueError("end_date 不能早于 start_date")
        if package.source.platform != SourcePlatform.TQSDK:
            raise ValueError("天勤任务必须使用 platform=tqsdk")
        if package.runner.mode != RunnerMode.NATIVE_SANDBOX:
            raise ValueError("天勤任务必须使用 native_sandbox")
        if package.execution.semantics != "tqsdk":
            raise ValueError("天勤任务必须使用 tqsdk 执行语义")
        if package.permissions.network != "allowlisted":
            raise ValueError("天勤原生运行时只允许声明 allowlisted 网络")
        source_artifacts = [
            item for item in package.source.artifacts if item.role == "source"
        ]
        if len(source_artifacts) != 1:
            raise ValueError("天勤任务必须且只能包含一个源码 artifact")
        artifact = source_artifacts[0]
        source_bytes = self.source_code.encode("utf-8")
        if len(source_bytes) != artifact.size_bytes:
            raise ValueError("天勤源码大小与 artifact 不一致")
        if hashlib.sha256(source_bytes).hexdigest() != artifact.sha256.lower():
            raise ValueError("天勤源码 SHA256 与 artifact 不一致")
        if artifact.file_name != package.source.entrypoint:
            raise ValueError("天勤入口文件必须与源码 artifact 文件名一致")
        if not artifact.file_name.lower().endswith(".py"):
            raise ValueError("天勤入口文件必须是 Python 源码")
        market_kinds = {EventKind.BAR, EventKind.TICK, EventKind.QUOTE}
        subscriptions = [
            item for item in package.subscriptions if item.kind in market_kinds
        ]
        if not subscriptions:
            raise ValueError("天勤任务至少需要一个 K线、Tick 或 Quote 订阅")
        symbols = {
            symbol for item in subscriptions for symbol in item.symbols
        }
        if not symbols:
            raise ValueError("天勤任务必须显式声明合约代码")
        if len(symbols) > 16:
            raise ValueError("天勤首期单任务最多允许 16 个合约")
        return self

    def to_worker_request(self) -> dict:
        source_artifact = next(
            item for item in self.package.source.artifacts if item.role == "source"
        )
        subscriptions = []
        for item in self.package.subscriptions:
            payload = item.model_dump(mode="json")
            payload["symbols"] = [_tq_symbol(symbol) for symbol in item.symbols]
            subscriptions.append(payload)
        symbols = list(
            dict.fromkeys(
                _tq_symbol(symbol)
                for item in self.package.subscriptions
                for symbol in item.symbols
            )
        )
        intervals = [
            str(item.interval)
            for item in self.package.subscriptions
            if item.interval
        ]
        task_contract = {
            "schema_version": 2,
            "engine_type": "tqsdk_native",
            "strategy": {
                "id": self.package.strategy_id,
                "version": self.package.version,
                "source_hash": source_artifact.sha256.lower(),
                "entrypoint": self.package.source.entrypoint,
            },
            "universe": {"symbols": symbols},
            "period": {
                "start": self.start_date.isoformat(),
                "end": self.end_date.isoformat(),
                "interval": intervals[0] if intervals else "tick",
                "timezone": "Asia/Shanghai",
            },
            "data": {
                "native_provider": {
                    "provider": "tqsdk",
                    "capture_policy": "materialize_subscribed_market_events",
                    "manifest_status": "bound_after_native_execution",
                    "subscriptions": subscriptions,
                }
            },
            "execution": {
                "capital": self.package.execution.initial_cash,
                "speed": self.speed,
                "mode": (
                    "TICK"
                    if any(
                        item.kind in {EventKind.TICK, EventKind.QUOTE}
                        for item in self.package.subscriptions
                    )
                    else "BAR"
                ),
                "execution_mode": self.execution_mode,
                "semantics": "tqsdk",
            },
            "parameters": {},
            "strategy_package": self.package.model_dump(mode="json"),
        }
        return {
            "speed": self.speed,
            "execution_mode": self.execution_mode,
            "_task_contract": task_contract,
            "_tqsdk_source_code": self.source_code,
            "_tqsdk_permissions": self.package.permissions.model_dump(mode="json"),
        }


__all__ = ["TQSDK_SUBMISSION_CONTRACT", "TqSdkTaskSubmission"]
