"""Validated MT5 one-minute OHLC replay options shared by API and worker."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


MT5_OPTION_NAMES = (
    "tick_model", "tick_spread", "contract_size_override",
    "pricetick_override", "min_volume_override",
)


class Mt5OhlcOptions(BaseModel):
    tick_model: Literal["interpolated", "mt5_ohlc_1m"] = "interpolated"
    tick_spread: float = Field(default=0, ge=0)
    contract_size_override: float | None = Field(default=None, gt=0)
    pricetick_override: float | None = Field(default=None, gt=0)
    min_volume_override: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_legacy_replay(self) -> "Mt5OhlcOptions":
        if hasattr(self, "interval"):
            self.validate_replay("vnpy_cta", self.mode, self.interval)
        return self

    def validate_replay(self, engine_type: str, mode: str, interval: str) -> None:
        if self.tick_model == "mt5_ohlc_1m" and (
            engine_type != "vnpy_cta" or mode != "TICK" or interval != "1m"
        ):
            raise ValueError("MT5 1 Minute OHLC 仅支持 vnpy_cta 的 TICK / 1m 回放")

    def mt5_worker_fields(self) -> dict[str, Any]:
        return self.model_dump(include=set(MT5_OPTION_NAMES))


def mt5_task_options(request: dict[str, Any]) -> dict[str, Any]:
    return {"tick_model": str(request.get("tick_model") or "interpolated"),
            "tick_spread": float(request.get("tick_spread") or 0),
            **{key: request.get(key) for key in MT5_OPTION_NAMES[2:]}}
