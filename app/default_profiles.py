from __future__ import annotations

from copy import deepcopy
from typing import Any


DEFAULT_PROFILE_CONTRACT = "pxybacktest.default-profile.v1"

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


__all__ = [
    "DEFAULT_PROFILE_CONTRACT",
    "default_profile_catalog",
    "profile_ids_for_engine",
]
