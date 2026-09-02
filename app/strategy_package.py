"""跨平台策略包与统一回测事件契约。

契约统一策略身份、输入能力和执行语义，但不假装一个运行时可以直接解释所有
第三方策略语言。每个平台仍由声明的 native/compat/portable runner 执行。
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


STRATEGY_PACKAGE_CONTRACT = "pxybacktest.strategy-package.v1"
EVENT_ENVELOPE_CONTRACT = "pxybacktest.event-envelope.v1"


class SourcePlatform(StrEnum):
    MT4 = "mt4"
    MT5 = "mt5"
    TRADEBLAZER = "tradeblazer"
    TQSDK = "tqsdk"
    VNPY = "vnpy"
    TRADINGVIEW = "tradingview"
    JOINQUANT = "joinquant"
    PYTHON = "python"
    CUSTOM = "custom"


class RunnerMode(StrEnum):
    NATIVE_ORACLE = "native_oracle"
    NATIVE_SANDBOX = "native_sandbox"
    COMPAT = "compat"
    PORTABLE_IR = "portable_ir"


class VerificationLevel(StrEnum):
    IMPORTED = "imported"
    COMPILED = "compiled"
    NATIVE_VERIFIED = "native_verified"
    PARITY_VERIFIED = "parity_verified"
    OPTIMIZED = "optimized"


class EventKind(StrEnum):
    TICK = "tick"
    QUOTE = "quote"
    TRADE = "trade"
    BAR = "bar"
    BOOK_SNAPSHOT = "book_snapshot"
    BOOK_DELTA = "book_delta"
    FUNDAMENTAL_PIT = "fundamental_pit"
    CORPORATE_ACTION = "corporate_action"
    NEWS = "news"
    SENTIMENT = "sentiment"
    MARKET_EVENT = "market_event"
    FACTOR = "factor"
    CALENDAR = "calendar"
    FUNDING = "funding"
    BORROW = "borrow"


class ArtifactIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact_id: str = Field(min_length=1, max_length=200)
    file_name: str = Field(min_length=1, max_length=300)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    media_type: str = Field(min_length=1, max_length=100)
    role: Literal["source", "binary", "parameter_set", "dependency_lock", "ir"]
    size_bytes: int = Field(ge=0)


class StrategySource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: SourcePlatform
    language: str = Field(min_length=1, max_length=50)
    entrypoint: str = Field(min_length=1, max_length=300)
    artifacts: list[ArtifactIdentity] = Field(min_length=1, max_length=64)
    license_policy: Literal[
        "user_supplied", "internal", "open_source", "proprietary_runtime"
    ]

    @model_validator(mode="after")
    def validate_platform_language(self) -> "StrategySource":
        language = self.language.strip().lower()
        required = {
            SourcePlatform.MT4: {"mql4"},
            SourcePlatform.MT5: {"mql5"},
            SourcePlatform.TRADINGVIEW: {"pine", "pinescript"},
            SourcePlatform.TQSDK: {"python"},
            SourcePlatform.VNPY: {"python"},
            SourcePlatform.JOINQUANT: {"python"},
        }
        accepted = required.get(self.platform)
        if accepted is not None and language not in accepted:
            expected = ", ".join(sorted(accepted))
            raise ValueError(f"{self.platform.value} 策略语言必须是: {expected}")
        self.language = language
        return self


class DataSubscription(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    symbols: list[str] = Field(default_factory=list, max_length=6000)
    universe_ref: str | None = Field(default=None, max_length=200)
    interval: str | None = Field(default=None, max_length=30)
    fields: list[str] = Field(default_factory=list, max_length=300)
    required: bool = True
    point_in_time: bool = True

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, values: list[str]) -> list[str]:
        return list(
            dict.fromkeys(
                value.strip().upper() for value in values if value.strip()
            )
        )

    @model_validator(mode="after")
    def validate_scope(self) -> "DataSubscription":
        if not self.symbols and not self.universe_ref:
            raise ValueError("数据订阅必须声明 symbols 或 universe_ref")
        if self.kind == EventKind.BAR and not self.interval:
            raise ValueError("K 线订阅必须声明 interval")
        if self.kind in {
            EventKind.FUNDAMENTAL_PIT,
            EventKind.NEWS,
            EventKind.SENTIMENT,
            EventKind.MARKET_EVENT,
            EventKind.FACTOR,
        } and not self.point_in_time:
            raise ValueError(f"{self.kind.value} 必须使用 PIT/可得时间语义")
        return self


class ExecutionProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    semantics: Literal[
        "mt4",
        "mt5_hedging",
        "mt5_netting",
        "tradeblazer",
        "tqsdk",
        "vnpy_cta",
        "tradingview_bar",
        "joinquant_a_share",
        "portable",
        "custom",
    ]
    base_currency: str = Field(default="USD", min_length=3, max_length=12)
    initial_cash: float = Field(gt=0)
    leverage: float = Field(default=1, gt=0)
    matching_model: Literal[
        "native",
        "bar_ohlc_conservative",
        "bar_close",
        "tick_bid_ask",
        "tick_price_time",
        "external_adapter",
    ]
    position_mode: Literal["netting", "hedging", "portfolio"]
    price_adjustment: Literal["none", "forward", "backward", "provider"] = "none"


class SandboxPermissions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    network: Literal["denied", "allowlisted"] = "denied"
    filesystem: Literal["task_readonly", "task_readwrite"] = "task_readonly"
    environment_allowlist: list[str] = Field(default_factory=list, max_length=100)
    timeout_seconds: int = Field(default=3600, ge=1, le=604800)
    memory_mb: int = Field(default=4096, ge=128, le=262144)
    cpu_cores: int = Field(default=1, ge=1, le=128)


class RunnerDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: RunnerMode
    adapter_id: str = Field(min_length=1, max_length=200)
    adapter_version: str = Field(min_length=1, max_length=100)
    runtime_identity: str = Field(min_length=1, max_length=300)
    acceptance_vector_ids: list[str] = Field(default_factory=list, max_length=500)


class ParityDimensionEvidence(BaseModel):
    """单个一致性维度的不可变验收证据。"""

    model_config = ConfigDict(extra="forbid")

    status: Literal["passed", "failed", "not_verified"]
    evidence_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-fA-F]{64}$",
    )
    summary: str = Field(default="", max_length=500)

    @model_validator(mode="after")
    def require_passed_evidence(self) -> "ParityDimensionEvidence":
        if self.status == "passed" and not self.evidence_sha256:
            raise ValueError("passed 一致性维度必须包含 evidence_sha256")
        return self


class ParityAcceptanceEvidence(BaseModel):
    """一个固定测试向量的成交、账户和可视化联合验收。"""

    model_config = ConfigDict(extra="forbid")

    vector_id: str = Field(min_length=1, max_length=200)
    trades: ParityDimensionEvidence
    account: ParityDimensionEvidence
    visual: ParityDimensionEvidence

    @property
    def all_passed(self) -> bool:
        return all(
            item.status == "passed"
            for item in (self.trades, self.account, self.visual)
        )


class StrategyPackage(BaseModel):
    """可提交给统一调度器的不可变策略包清单。"""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.strategy-package.v1"] = (
        STRATEGY_PACKAGE_CONTRACT
    )
    strategy_id: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    source: StrategySource
    runner: RunnerDescriptor
    parameters_schema: dict[str, Any] = Field(default_factory=dict)
    subscriptions: list[DataSubscription] = Field(min_length=1, max_length=300)
    execution: ExecutionProfile
    permissions: SandboxPermissions = Field(default_factory=SandboxPermissions)
    verification_level: VerificationLevel = VerificationLevel.IMPORTED
    parity_evidence: list[ParityAcceptanceEvidence] = Field(
        default_factory=list,
        max_length=500,
    )
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_runner_contract(self) -> "StrategyPackage":
        roles = {artifact.role for artifact in self.source.artifacts}
        if self.runner.mode == RunnerMode.PORTABLE_IR and "ir" not in roles:
            raise ValueError("portable_ir runner 必须包含 role=ir 的 artifact")
        if (
            self.source.platform in {SourcePlatform.MT4, SourcePlatform.MT5}
            and self.runner.mode == RunnerMode.NATIVE_ORACLE
            and "binary" not in roles
        ):
            raise ValueError("MT4/MT5 native_oracle 必须包含已编译 binary artifact")
        if self.verification_level in {
            VerificationLevel.NATIVE_VERIFIED,
            VerificationLevel.PARITY_VERIFIED,
            VerificationLevel.OPTIMIZED,
        } and not self.runner.acceptance_vector_ids:
            raise ValueError("已验证策略必须绑定 acceptance_vector_ids")
        if self.verification_level in {
            VerificationLevel.PARITY_VERIFIED,
            VerificationLevel.OPTIMIZED,
        }:
            evidence_by_id = {item.vector_id: item for item in self.parity_evidence}
            missing = [
                vector_id
                for vector_id in self.runner.acceptance_vector_ids
                if vector_id not in evidence_by_id
            ]
            incomplete = [
                vector_id
                for vector_id in self.runner.acceptance_vector_ids
                if vector_id in evidence_by_id
                and not evidence_by_id[vector_id].all_passed
            ]
            if missing:
                raise ValueError(
                    "parity_verified 缺少验收证据: " + ", ".join(missing)
                )
            if incomplete:
                raise ValueError(
                    "逐笔成交、账户和可视化必须全部通过: "
                    + ", ".join(incomplete)
                )
        if (
            self.runner.mode == RunnerMode.NATIVE_ORACLE
            and self.execution.matching_model != "native"
        ):
            raise ValueError("native_oracle 必须使用 native 撮合语义")
        return self


class EventEnvelope(BaseModel):
    """K 线、Tick、财务、舆情和因子的统一回放信封。"""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxybacktest.event-envelope.v1"] = (
        EVENT_ENVELOPE_CONTRACT
    )
    seq: int = Field(ge=0)
    event_id: str = Field(min_length=1, max_length=200)
    kind: EventKind
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source: str = Field(min_length=1, max_length=200)
    snapshot_id: str = Field(min_length=1, max_length=200)
    revision_id: str = Field(min_length=1, max_length=200)
    symbol: str | None = Field(default=None, max_length=100)
    payload: dict[str, Any]

    @field_validator("event_time", "available_at", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("事件时间必须携带时区")
        return value

    @model_validator(mode="after")
    def validate_availability(self) -> "EventEnvelope":
        if self.ingested_at < self.available_at:
            raise ValueError("ingested_at 不能早于 available_at")
        return self


__all__ = [
    "EVENT_ENVELOPE_CONTRACT",
    "STRATEGY_PACKAGE_CONTRACT",
    "ArtifactIdentity",
    "DataSubscription",
    "EventEnvelope",
    "EventKind",
    "ExecutionProfile",
    "ParityAcceptanceEvidence",
    "ParityDimensionEvidence",
    "RunnerDescriptor",
    "RunnerMode",
    "SandboxPermissions",
    "SourcePlatform",
    "StrategyPackage",
    "StrategySource",
    "VerificationLevel",
]
