from __future__ import annotations

from copy import deepcopy
from typing import Any


DEFAULT_PROFILE_CONTRACT = "pxybacktest.default-profile.v1"

_ENGINE_METADATA: dict[str, dict[str, Any]] = {
    "vnpy_cta": {
        "label": "CTA 可视化回放",
        "category": "cta",
        "maturity": "result_ui_verified",
        "frontend_ready": True,
        "result_panels": ["summary", "replay", "orders", "diagnostics"],
    },
    "a_share_portfolio": {
        "label": "A 股组合",
        "category": "portfolio",
        "maturity": "run_verified",
        "frontend_ready": True,
        "result_panels": ["summary", "portfolio", "replay", "diagnostics"],
    },
    "a_share_emotion_etf": {
        "label": "ETF 情绪极值",
        "category": "portfolio",
        "maturity": "run_verified",
        "frontend_ready": True,
        "result_panels": ["summary", "portfolio", "replay", "orders", "diagnostics"],
    },
    "factor_matrix": {
        "label": "A 股多因子",
        "category": "factor",
        "maturity": "run_verified",
        "frontend_ready": True,
        "result_panels": ["summary", "factors", "portfolio", "replay"],
    },
    "event_sentiment": {
        "label": "A 股事件舆情",
        "category": "event",
        "maturity": "run_verified",
        "frontend_ready": True,
        "result_panels": ["summary", "events", "portfolio", "replay"],
    },
    "microstructure": {
        "label": "Tick 微观结构",
        "category": "microstructure",
        "maturity": "run_verified",
        "frontend_ready": True,
        "result_panels": ["summary", "microstructure", "replay", "orders"],
    },
    "ml_factor": {
        "label": "机器学习因子",
        "category": "learning",
        "maturity": "run_verified",
        "frontend_ready": False,
        "result_panels": ["summary", "training", "prediction", "portfolio"],
    },
    "deep_learning": {
        "label": "深度学习",
        "category": "learning",
        "maturity": "run_verified",
        "frontend_ready": False,
        "result_panels": ["summary", "training", "prediction", "portfolio", "replay"],
    },
    "lighter_microstructure": {
        "label": "Lighter 微观结构",
        "category": "microstructure",
        "maturity": "run_verified",
        "frontend_ready": False,
        "result_panels": ["summary", "microstructure", "replay", "orders"],
    },
    "mt5_native": {
        "label": "MT5 原生",
        "category": "external_platform",
        "maturity": "declared",
        "frontend_ready": False,
        "result_panels": ["summary", "replay", "orders", "parity"],
    },
}

_DEFAULT_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "profile_id": "futures-minute-recommended",
        "profile_version": "1.0.0",
        "market": "futures",
        "timeframe": "minute",
        "engine_ids": ["vnpy_cta"],
        "effective_from": "2026-09-03T00:00:00+08:00",
        "recommended": True,
        "defaults": {
            "period": {"lookback_days": 153, "interval": "1m"},
            "execution": {"speed": 50},
            "optimization": {"n_trials": 20},
        },
    },
    {
        "profile_id": "a-share-daily-recommended",
        "profile_version": "1.0.0",
        "market": "a_share",
        "timeframe": "daily",
        "engine_ids": [
            "a_share_portfolio",
            "a_share_emotion_etf",
            "factor_matrix",
            "event_sentiment",
            "ml_factor",
            "deep_learning",
        ],
        "effective_from": "2026-09-03T00:00:00+08:00",
        "recommended": True,
        "defaults": {
            "period": {"lookback_days": 365, "interval": "1d"},
            "execution": {"speed": 50},
            "optimization": {"n_trials": 20},
            "learning": {"max_epochs": 20},
        },
    },
    {
        "profile_id": "tick-recommended",
        "profile_version": "1.0.0",
        "market": "multi_market",
        "timeframe": "tick",
        "engine_ids": ["microstructure", "lighter_microstructure"],
        "effective_from": "2026-09-03T00:00:00+08:00",
        "recommended": True,
        "defaults": {
            "period": {"lookback_days": 6, "interval": "tick"},
            "execution": {"speed": 50},
            "optimization": {"n_trials": 20},
        },
    },
)


def default_profile_catalog() -> list[dict[str, Any]]:
    """返回可安全交给 API 调用方修改的默认档案副本。"""

    return deepcopy(list(_DEFAULT_PROFILES))


def profile_ids_for_engine(engine_id: str) -> list[str]:
    return [
        str(profile["profile_id"])
        for profile in _DEFAULT_PROFILES
        if engine_id in profile["engine_ids"]
    ]


def engine_catalog_metadata(engine_id: str) -> dict[str, Any]:
    metadata = deepcopy(_ENGINE_METADATA.get(engine_id, {}))
    metadata.setdefault("label", engine_id)
    metadata.setdefault("category", "extension")
    metadata.setdefault("maturity", "declared")
    metadata.setdefault("frontend_ready", False)
    metadata.setdefault("result_panels", ["summary", "diagnostics"])
    metadata["config_schema_version"] = "pxybacktest.task-result.v2"
    return metadata


__all__ = [
    "DEFAULT_PROFILE_CONTRACT",
    "default_profile_catalog",
    "engine_catalog_metadata",
    "profile_ids_for_engine",
]
