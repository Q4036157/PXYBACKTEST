"""Result v2 的只读报告投影。

该模块吸收桌面结果窗口的 KPI、净值/回撤、基准、月度和滚动风险展示能力，
但输出 JSON，不依赖 PyQt、matplotlib 或任何行情/交易接口。
"""
from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from .metrics import enrich_metrics, equity_returns, sharpe_ratio


def _value(point: Any) -> float | None:
    if isinstance(point, Mapping):
        for key in ("value", "equity", "balance", "total_asset", "net_value", "close"):
            try:
                value = float(point.get(key))
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                return value
        return None
    try:
        value = float(point)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def monthly_returns(equity_curve: Sequence[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for point in equity_curve:
        if not isinstance(point, Mapping):
            continue
        date = str(point.get("date") or point.get("datetime") or "")
        value = _value(point)
        if len(date) >= 7 and value is not None and value > 0:
            grouped[date[:7]].append(value)
    return [
        {"month": month, "return": round(values[-1] / values[0] - 1.0, 12)}
        for month, values in sorted(grouped.items())
        if len(values) >= 2 and values[0] > 0
    ]


def rolling_sharpe(
    equity_curve: Sequence[Any], *, window: int = 20, periods_per_year: float = 252.0
) -> list[dict[str, Any]]:
    if window < 2:
        raise ValueError("rolling Sharpe window 至少为 2")
    returns = equity_returns(equity_curve)
    result: list[dict[str, Any]] = []
    for index in range(window, len(returns) + 1):
        point = equity_curve[index] if index < len(equity_curve) else equity_curve[-1]
        date = point.get("date") if isinstance(point, Mapping) else None
        result.append(
            {
                "date": date,
                "window": window,
                "sharpe": sharpe_ratio(returns[index - window : index], periods_per_year=periods_per_year),
            }
        )
    return result


def distribution(values: Sequence[Any], *, bins: int = 10) -> list[dict[str, float | int]]:
    numbers = [value for item in values if (value := _value(item)) is not None]
    if bins < 1:
        raise ValueError("bins 必须大于 0")
    if not numbers:
        return []
    low, high = min(numbers), max(numbers)
    if low == high:
        return [{"lower": low, "upper": high, "count": len(numbers)}]
    width = (high - low) / bins
    counts = [0] * bins
    for value in numbers:
        index = min(bins - 1, int((value - low) / width))
        counts[index] += 1
    return [
        {"lower": low + index * width, "upper": low + (index + 1) * width, "count": count}
        for index, count in enumerate(counts)
    ]


def build_report_projection(result: Mapping[str, Any]) -> dict[str, Any]:
    """将统一结果转换为前端/静态报告可直接消费的投影。"""
    curves = dict(result.get("curves") or {})
    equity = list(curves.get("equity") or curves.get("daily") or [])
    benchmark = list(curves.get("benchmark") or [])
    deals = [item for item in (result.get("deals") or []) if isinstance(item, Mapping)]
    metrics = enrich_metrics(dict(result.get("metrics") or {}), equity, deals=deals, benchmark=benchmark)
    returns = equity_returns(equity)
    return {
        "contract_version": "pxybacktest.report.v1",
        "kpis": metrics,
        "curves": {
            "equity": equity,
            "drawdown": list(curves.get("drawdown") or []),
            "benchmark": benchmark,
        },
        "tables": {
            "deals": deals,
            "monthly_returns": monthly_returns(equity),
            "rolling_sharpe_20": rolling_sharpe(equity, window=20) if len(returns) >= 20 else [],
            "return_distribution": distribution(returns),
        },
        "conventions": {
            "return_unit": "decimal",
            "drawdown_unit": "decimal_negative",
            "annualization": 252,
            "source": "result_v2_only",
        },
    }


__all__ = ["build_report_projection", "distribution", "monthly_returns", "rolling_sharpe"]
