from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


SUPPORTED_PLATFORMS = {"LIGHTER", "OKX", "BINANCE", "BITMART", "MT4", "MT5"}


def _extract_platform(vt_symbol: str) -> str:
    normalized = vt_symbol.upper()
    if normalized.endswith(".GLOBAL"):
        normalized = normalized[:-7]
    if "." in normalized:
        exchange = normalized.rsplit(".", 1)[-1]
        if exchange in SUPPORTED_PLATFORMS:
            return exchange
    if "_" in normalized:
        return normalized.rsplit("_", 1)[-1]
    return ""


class SubmitBacktestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_class: str = Field(min_length=1, max_length=200)
    vt_symbol: str = Field(min_length=1, max_length=200)
    interval: str = Field(default="1h", min_length=1, max_length=20)
    start_time: str = Field(min_length=1, max_length=64)
    end_time: str = Field(min_length=1, max_length=64)
    parameters: dict[str, Any] = Field(default_factory=dict)
    capital: float = Field(default=1_000_000, gt=0)
    rate: float = Field(default=0.0004, ge=0, le=1)
    slippage: float = Field(default=0, ge=0)
    speed: float = Field(default=50, ge=1, le=100)
    mode: Literal["BAR", "TICK"] = "BAR"

    @field_validator("strategy_class", "vt_symbol", "interval", "start_time", "end_time")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @field_validator("vt_symbol")
    @classmethod
    def validate_supported_platform(cls, value: str) -> str:
        platform = _extract_platform(value)
        if platform not in SUPPORTED_PLATFORMS:
            supported = ", ".join(sorted(SUPPORTED_PLATFORMS))
            raise ValueError(f"vt_symbol 交易平台不受支持，当前仅支持: {supported}")
        return value


class SetSpeedRequest(BaseModel):
    speed: float = Field(ge=1, le=100)


class TaskEvent(BaseModel):
    seq: int
    type: str
    data: dict[str, Any]
    created_at: float
