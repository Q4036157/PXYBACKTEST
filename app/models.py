from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SUPPORTED_PLATFORMS = {"LIGHTER", "OKX", "BINANCE", "BITMART", "MT4", "MT5"}
DAA_ENGINE_TYPES = {"a_share_portfolio", "factor_matrix", "event_sentiment"}
ML_ENGINE_TYPES = {"ml_factor", "deep_learning"}


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
    execution_mode: Literal["visual", "fast"] = "visual"

    @field_validator(
        "strategy_class", "vt_symbol", "interval", "start_time", "end_time"
    )
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


EngineType = Literal[
    "vnpy_cta",
    "a_share_portfolio",
    "factor_matrix",
    "event_sentiment",
    "microstructure",
    "ml_factor",
    "deep_learning",
    "mt5_native",
]


class StrategyIdentityV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    source_hash: str = Field(min_length=8, max_length=128)
    entrypoint: str = Field(min_length=1, max_length=200)


class UniverseV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbols: list[str] = Field(min_length=1, max_length=6000)

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, values: list[str]) -> list[str]:
        normalized = list(
            dict.fromkeys(
                str(value).strip().upper() for value in values if str(value).strip()
            )
        )
        if not normalized:
            raise ValueError("at least one symbol is required")
        return normalized


class BacktestPeriodV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str = Field(min_length=1, max_length=64)
    end: str = Field(min_length=1, max_length=64)
    interval: str = Field(default="1h", min_length=1, max_length=20)
    timezone: str = Field(default="Asia/Shanghai", min_length=1, max_length=64)

    @field_validator("start", "end")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        text = value.strip()
        try:
            _parse_iso_datetime(text)
        except ValueError as exc:
            raise ValueError("must be an ISO-8601 datetime") from exc
        return text

    @model_validator(mode="after")
    def validate_range(self) -> "BacktestPeriodV2":
        start = _parse_iso_datetime(self.start)
        end = _parse_iso_datetime(self.end)
        start_aware = start.utcoffset() is not None
        end_aware = end.utcoffset() is not None
        if start_aware != end_aware:
            raise ValueError(
                "period.start and period.end must use the same timezone form"
            )
        if start > end:
            raise ValueError("period.start must be earlier than or equal to period.end")
        return self


class DataSnapshotSelectionV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    datasets: list[str] = Field(min_length=1, max_length=16)
    decision_time: str = Field(min_length=1, max_length=64)
    quality_policy: Literal["require_pass", "allow_warn", "allow_unverified"] = (
        "require_pass"
    )

    @field_validator("datasets")
    @classmethod
    def normalize_datasets(cls, values: list[str]) -> list[str]:
        normalized = sorted(
            set(str(value).strip() for value in values if str(value).strip())
        )
        if not normalized:
            raise ValueError("at least one dataset is required")
        return normalized

    @field_validator("decision_time")
    @classmethod
    def validate_decision_time(cls, value: str) -> str:
        text = value.strip()
        try:
            parsed = _parse_iso_datetime(text)
        except ValueError as exc:
            raise ValueError("decision_time must be an ISO-8601 datetime") from exc
        if parsed.utcoffset() is None:
            raise ValueError("decision_time must include timezone")
        return text


class DataSnapshotDatasetV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    contract_id: str
    schema_version: int
    file_count: int = Field(ge=1)
    row_count: int = Field(ge=0)
    size_bytes: int = Field(ge=0)
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    pit_grade: str | None = None


class DataSnapshotRefV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["pxydata.backtest-data-snapshot.v1"]
    snapshot_id: str = Field(pattern=r"^btsnap_v1_[0-9a-f]{32}$")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    quality_policy: Literal["require_pass", "allow_warn", "allow_unverified"]
    quality_accepted: bool
    quality_report_id: str | None = None
    datasets: list[DataSnapshotDatasetV2] = Field(min_length=1)
    warnings: list[str] = Field(default_factory=list)

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        text = value.strip()
        try:
            parsed = _parse_iso_datetime(text)
        except ValueError as exc:
            raise ValueError("created_at must be an ISO-8601 datetime") from exc
        if parsed.utcoffset() is None:
            raise ValueError("created_at must include timezone")
        return text


class TaskDataV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    selection: DataSnapshotSelectionV2 | None = None
    snapshot: DataSnapshotRefV2 | None = None

    @model_validator(mode="after")
    def validate_data_choice(self) -> "TaskDataV2":
        self.validate_choice()
        return self

    def validate_choice(self) -> None:
        if (self.selection is None) == (self.snapshot is None):
            raise ValueError("data must contain exactly one of selection or snapshot")


class ExecutionModelV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capital: float = Field(default=1_000_000, gt=0)
    rate: float = Field(default=0.0004, ge=0, le=1)
    slippage: float = Field(default=0, ge=0)
    speed: float = Field(default=50, ge=1, le=100)
    mode: Literal["BAR", "TICK"] = "BAR"
    execution_mode: Literal["visual", "fast"] = "visual"
    leverage: float | None = Field(default=None, gt=0)
    commission: float | None = Field(default=None, ge=0)
    stamp_tax: float | None = Field(default=None, ge=0)


class SearchDimensionV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["float", "int", "categorical"]
    low: float | int | None = None
    high: float | int | None = None
    step: float | int | None = None
    log: bool = False
    choices: list[Any] | None = None

    @model_validator(mode="after")
    def validate_dimension(self) -> "SearchDimensionV2":
        if self.type == "categorical":
            if not self.choices or len(self.choices) > 100:
                raise ValueError("categorical 搜索维度必须包含 1 到 100 个 choices")
            if self.low is not None or self.high is not None or self.step is not None:
                raise ValueError("categorical 搜索维度不得设置 low/high/step")
            return self
        if self.low is None or self.high is None or float(self.low) >= float(self.high):
            raise ValueError("数值搜索维度必须满足 low < high")
        if self.step is not None and float(self.step) <= 0:
            raise ValueError("搜索维度 step 必须大于 0")
        if self.log and self.step is not None:
            raise ValueError("Optuna log 搜索不得同时设置 step")
        if self.log and float(self.low) <= 0:
            raise ValueError("Optuna log 搜索的 low 必须大于 0")
        return self


class OptimizationObjectiveV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: str = Field(min_length=1, max_length=100)
    direction: Literal["maximize", "minimize"] = "maximize"


class OptimizationConfigV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: Literal["optuna", "walk_forward"]
    search_space: dict[str, SearchDimensionV2] = Field(min_length=1, max_length=16)
    objectives: list[OptimizationObjectiveV2] = Field(min_length=1, max_length=3)
    n_trials: int = Field(default=30, ge=1, le=500)
    sampler_seed: int = 42
    train_days: int = Field(default=252, ge=2, le=3650)
    test_days: int = Field(default=63, ge=1, le=730)
    step_days: int = Field(default=63, ge=1, le=730)

    @field_validator("search_space")
    @classmethod
    def validate_search_names(
        cls, values: dict[str, SearchDimensionV2]
    ) -> dict[str, SearchDimensionV2]:
        for name in values:
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name) is None:
                raise ValueError(f"无效搜索参数名: {name}")
        return values

    @field_validator("objectives")
    @classmethod
    def unique_objectives(
        cls, values: list[OptimizationObjectiveV2]
    ) -> list[OptimizationObjectiveV2]:
        metrics = [item.metric for item in values]
        if len(metrics) != len(set(metrics)):
            raise ValueError("optimization objectives 不得重复")
        return values


class SubmitBacktestRequestV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2] = 2
    engine_type: EngineType
    strategy: StrategyIdentityV2
    universe: UniverseV2
    period: BacktestPeriodV2
    data: TaskDataV2
    execution: ExecutionModelV2 = Field(default_factory=ExecutionModelV2)
    parameters: dict[str, Any] = Field(default_factory=dict)
    optimization: OptimizationConfigV2 | None = None
    random_seed: int | None = None

    @model_validator(mode="after")
    def validate_model_contract(self) -> "SubmitBacktestRequestV2":
        self.validate_contract()
        return self

    def validate_contract(self) -> None:
        self.data.validate_choice()
        if self.engine_type == "vnpy_cta" and len(self.universe.symbols) != 1:
            raise ValueError("vnpy_cta currently requires exactly one symbol")
        if self.engine_type == "a_share_portfolio":
            self._validate_a_share_contract()
        elif self.engine_type in {"factor_matrix", "event_sentiment"}:
            self._validate_factor_contract()
        elif self.engine_type == "microstructure":
            self._validate_microstructure_contract()
        elif self.engine_type in ML_ENGINE_TYPES:
            self._validate_learning_contract()
        if self.optimization is not None and self.engine_type not in {
            "a_share_portfolio",
            "factor_matrix",
            "event_sentiment",
            "microstructure",
        }:
            raise ValueError(f"{self.engine_type} 尚不支持统一优化任务")

    def _validate_learning_contract(self) -> None:
        if self.period.interval != "1d":
            raise ValueError("学习回测首期只支持 1d")
        if self.execution.mode != "BAR":
            raise ValueError("学习回测首期只支持 BAR 模式")
        if self.execution.leverage not in (None, 1.0):
            raise ValueError("学习回测不支持杠杆")
        if self.strategy.id != self.strategy.entrypoint:
            raise ValueError("学习策略要求 strategy.id 与 entrypoint 一致")
        if re.fullmatch(r"[0-9a-fA-F]{64}", self.strategy.source_hash) is None:
            raise ValueError("学习策略要求完整的 strategy.source_hash SHA256")
        datasets = (
            self.data.selection.datasets
            if self.data.selection is not None
            else [item.name for item in self.data.snapshot.datasets]  # type: ignore[union-attr]
        )
        if not ({"ml_features_daily", "factor_matrix_daily", "lighter_microstructure_factors"} & set(datasets)):
            raise ValueError(
                "学习回测数据快照必须包含 ml_features_daily、factor_matrix_daily 或 lighter_microstructure_factors"
            )
        parameters = self.parameters
        feature_columns = parameters.get("feature_columns")
        if not isinstance(feature_columns, list) or not feature_columns:
            raise ValueError("学习回测必须指定 parameters.feature_columns")
        if len(feature_columns) > 128 or any(not str(item).strip() for item in feature_columns):
            raise ValueError("parameters.feature_columns 必须包含 1 到 128 个非空字段")
        label_column = str(parameters.get("label_column") or "label").strip()
        if not label_column:
            raise ValueError("parameters.label_column 不能为空")
        if label_column.startswith("forward_return_") and "kline_daily" not in set(datasets):
            raise ValueError("forward_return 标签需要同一快照包含 kline_daily")
        model_type = str(parameters.get("model_type") or "linear_regression").lower()
        allowed = {"linear_regression", "linear_logit", "lightgbm", "transformer"}
        if model_type not in allowed:
            raise ValueError(f"学习模型不受支持: {model_type}")
        task_type = str(parameters.get("task_type") or "regression").lower()
        if task_type not in {"binary", "ranking", "regression"}:
            raise ValueError(f"学习 task_type 不受支持: {task_type}")
        seq_len = int(parameters.get("seq_len") or 1)
        if seq_len < 1 or seq_len > 4096:
            raise ValueError("学习回测 parameters.seq_len 必须在 1 到 4096 之间")
        if self.engine_type == "deep_learning" and model_type != "transformer":
            raise ValueError("deep_learning 引擎必须使用 model_type=transformer")
        for name in ("train_days", "test_days", "step_days", "purge_days", "embargo_days", "top_k"):
            value = parameters.get(name)
            if value is not None and int(value) < 0:
                raise ValueError(f"学习回测 parameters.{name} 不能为负数")

    def _validate_a_share_contract(self) -> None:
        if self.period.interval != "1d":
            raise ValueError("a_share_portfolio 首期只支持 1d")
        if self.execution.mode != "BAR":
            raise ValueError("a_share_portfolio 首期只支持 BAR 模式")
        if self.execution.leverage not in (None, 1.0):
            raise ValueError("a_share_portfolio 首期不支持杠杆")
        if self.strategy.id != self.strategy.entrypoint:
            raise ValueError("A 股内置策略要求 strategy.id 与 entrypoint 一致")
        if re.fullmatch(r"[0-9a-fA-F]{64}", self.strategy.source_hash) is None:
            raise ValueError("A 股内置策略要求完整的 strategy.source_hash SHA256")
        if self.data.selection is not None:
            datasets = self.data.selection.datasets
        else:
            assert self.data.snapshot is not None
            datasets = [item.name for item in self.data.snapshot.datasets]
        if "kline_daily" not in datasets:
            raise ValueError("a_share_portfolio 数据快照必须包含 kline_daily")
        overrides = self.parameters.get("overrides")
        basic_filter = (
            overrides.get("basic_filter") if isinstance(overrides, dict) else None
        )
        if (
            not isinstance(basic_filter, dict)
            or basic_filter.get("enabled") is not False
        ):
            raise ValueError(
                "A 股首期必须显式设置 parameters.overrides.basic_filter.enabled=false"
            )
        mode = str(self.parameters.get("mode") or "position")
        if mode not in {"position", "full"}:
            raise ValueError("A 股回测 parameters.mode 只支持 position 或 full")
        for field_name in ("holding_days", "max_positions"):
            value = self.parameters.get(field_name)
            if value is not None and int(value) < 1:
                raise ValueError(f"A 股回测 parameters.{field_name} 必须大于 0")

    def _validate_factor_contract(self) -> None:
        if self.period.interval != "1d":
            raise ValueError("A 股因子回测只支持 1d")
        if self.execution.mode != "BAR":
            raise ValueError("A 股因子回测只支持 BAR 模式")
        if self.execution.leverage not in (None, 1.0):
            raise ValueError("A 股因子回测不支持杠杆")
        if self.strategy.id != self.strategy.entrypoint:
            raise ValueError("A 股因子策略要求 strategy.id 与 entrypoint 一致")
        if re.fullmatch(r"[0-9a-fA-F]{64}", self.strategy.source_hash) is None:
            raise ValueError("A 股因子策略要求完整的 strategy.source_hash SHA256")
        if self.data.selection is not None:
            datasets = self.data.selection.datasets
            missing = {"kline_daily"} - set(datasets)
        else:
            assert self.data.snapshot is not None
            datasets = [item.name for item in self.data.snapshot.datasets]
            missing = {"kline_daily", "factor_matrix_daily"} - set(datasets)
        if missing:
            raise ValueError(
                f"A 股因子回测数据快照缺少: {', '.join(sorted(missing))}"
            )
        factor_set_id = str(self.parameters.get("factor_set_id") or "").strip()
        factor_snapshot_id = str(
            self.parameters.get("factor_input_snapshot_id") or ""
        ).strip()
        if not factor_set_id:
            raise ValueError("A 股因子回测必须指定 parameters.factor_set_id")
        if (
            self.data.snapshot is not None
            and re.fullmatch(r"btsnap_v1_[0-9a-f]{32}", factor_snapshot_id) is None
        ):
            raise ValueError(
                "A 股因子回测必须指定有效的 parameters.factor_input_snapshot_id"
            )
        weights = self.parameters.get("factor_weights")
        if self.engine_type == "factor_matrix" and (
            not isinstance(weights, dict) or not weights
        ):
            raise ValueError("factor_matrix 必须指定 parameters.factor_weights")
        if weights is not None:
            if not isinstance(weights, dict) or not weights or len(weights) > 16:
                raise ValueError("parameters.factor_weights 必须包含 1 到 16 个因子")
            for field, value in weights.items():
                if not str(field).strip():
                    raise ValueError("factor_weights 因子名不能为空")
                try:
                    number = float(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"factor_weights 权重无效: {field}") from exc
                if not math.isfinite(number) or number == 0:
                    raise ValueError(f"factor_weights 权重无效: {field}")
        for field_name in ("holding_days", "max_positions", "rebalance_days"):
            value = self.parameters.get(field_name)
            if value is not None and int(value) < 1:
                raise ValueError(f"A 股因子回测 parameters.{field_name} 必须大于 0")

    def _validate_microstructure_contract(self) -> None:
        if len(self.universe.symbols) != 1:
            raise ValueError("microstructure 当前要求恰好一个交易品种")
        if self.period.interval != "tick" or self.execution.mode != "TICK":
            raise ValueError("microstructure 必须使用 tick 周期和 TICK 模式")
        if self.strategy.id != self.strategy.entrypoint:
            raise ValueError("microstructure 要求 strategy.id 与 entrypoint 一致")
        if re.fullmatch(r"[0-9a-fA-F]{64}", self.strategy.source_hash) is None:
            raise ValueError("microstructure 要求完整的 strategy.source_hash SHA256")
        if self.data.selection is not None:
            datasets = self.data.selection.datasets
        else:
            assert self.data.snapshot is not None
            datasets = [item.name for item in self.data.snapshot.datasets]
        if "market_ticks" not in datasets:
            raise ValueError("microstructure 数据快照必须包含 market_ticks")
        for field_name in ("latency_ticks", "max_hold_ticks"):
            value = int(self.parameters.get(field_name, 1))
            if value < 1:
                raise ValueError(f"microstructure parameters.{field_name} 必须大于 0")
        threshold = float(self.parameters.get("entry_threshold", 0.2))
        exit_threshold = float(self.parameters.get("exit_threshold", 0.0))
        if not 0 < threshold < 1 or not 0 <= exit_threshold < threshold:
            raise ValueError("microstructure 盘口不平衡阈值无效")

    def with_snapshot(
        self,
        snapshot: DataSnapshotRefV2,
        *,
        parameter_updates: dict[str, Any] | None = None,
    ) -> "SubmitBacktestRequestV2":
        payload = self.model_dump(mode="json")
        payload["data"] = {"snapshot": snapshot.model_dump(mode="json")}
        payload["parameters"] = {**payload["parameters"], **(parameter_updates or {})}
        return SubmitBacktestRequestV2.model_validate(payload)

    def to_worker_request(
        self, *, snapshot_manifest: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        self.validate_contract()
        if self.engine_type in DAA_ENGINE_TYPES:
            if snapshot_manifest is None:
                raise ValueError(f"{self.engine_type} 缺少内部快照清单")
            return {
                "speed": self.execution.speed,
                "_task_contract": self.model_dump(mode="json"),
                "_snapshot_manifest": snapshot_manifest,
            }
        if self.engine_type == "microstructure":
            if snapshot_manifest is None:
                raise ValueError("microstructure 缺少内部快照清单")
            return {
                "speed": self.execution.speed,
                "_task_contract": self.model_dump(mode="json"),
                "_snapshot_manifest": snapshot_manifest,
            }
        if self.engine_type in ML_ENGINE_TYPES:
            if snapshot_manifest is None:
                raise ValueError(f"{self.engine_type} 缺少内部快照清单")
            return {
                "speed": self.execution.speed,
                "_task_contract": self.model_dump(mode="json"),
                "_snapshot_manifest": snapshot_manifest,
            }
        if self.engine_type != "vnpy_cta":
            raise ValueError(f"不支持的 worker 引擎: {self.engine_type}")
        symbol = self.universe.symbols[0]
        legacy = SubmitBacktestRequest.model_validate(
            {
                "strategy_class": self.strategy.entrypoint,
                "vt_symbol": symbol,
                "interval": self.period.interval,
                "start_time": self.period.start,
                "end_time": self.period.end,
                "parameters": self.parameters,
                "capital": self.execution.capital,
                "rate": self.execution.rate,
                "slippage": self.execution.slippage,
                "speed": self.execution.speed,
                "mode": self.execution.mode,
                "execution_mode": self.execution.execution_mode,
            }
        ).model_dump()
        legacy["_task_contract"] = self.model_dump(mode="json")
        return legacy


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
