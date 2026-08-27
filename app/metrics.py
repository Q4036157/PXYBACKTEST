"""纯函数回测绩效指标。

指标只消费已经由回测引擎产生的曲线和成交记录，不读取行情源、GUI 或交易接口。
所有收益率使用小数表示，回撤为负数（例如 ``-0.12`` 表示最大回撤 12%）。
"""
from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _series(
    values: Iterable[Any],
    *,
    keys: tuple[str, ...] = ("value", "equity", "balance", "total_asset", "net_value", "close"),
) -> list[float]:
    output: list[float] = []
    for value in values:
        if isinstance(value, Mapping):
            number = next((_number(value.get(key)) for key in keys if _number(value.get(key)) is not None), None)
        else:
            number = _number(value)
        if number is not None and number > 0:
            output.append(number)
    return output


def equity_returns(equity: Sequence[Any]) -> list[float]:
    values = _series(equity)
    return [current / previous - 1.0 for previous, current in zip(values, values[1:])]


def drawdown_curve(equity: Sequence[Any]) -> list[float]:
    values = _series(equity)
    peak = 0.0
    result: list[float] = []
    for value in values:
        peak = max(peak, value)
        result.append(value / peak - 1.0 if peak > 0 else 0.0)
    return result


def max_drawdown(equity: Sequence[Any]) -> float:
    return min(drawdown_curve(equity), default=0.0)


def annualized_return(equity: Sequence[Any], *, periods_per_year: float = 252.0) -> float:
    values = _series(equity)
    if len(values) < 2 or values[0] <= 0 or periods_per_year <= 0:
        return 0.0
    periods = len(values) - 1
    return (values[-1] / values[0]) ** (periods_per_year / periods) - 1.0


def annualized_volatility(returns: Sequence[Any], *, periods_per_year: float = 252.0) -> float:
    values = [value for item in returns if (value := _number(item)) is not None]
    if len(values) < 2 or periods_per_year <= 0:
        return 0.0
    return statistics.pstdev(values) * math.sqrt(periods_per_year)


def sharpe_ratio(
    returns: Sequence[Any], *, risk_free_rate: float = 0.0, periods_per_year: float = 252.0
) -> float:
    values = [value for item in returns if (value := _number(item)) is not None]
    if len(values) < 2 or periods_per_year <= 0:
        return 0.0
    rf = (1.0 + risk_free_rate) ** (1.0 / periods_per_year) - 1.0
    excess = [value - rf for value in values]
    deviation = statistics.pstdev(excess)
    return statistics.fmean(excess) / deviation * math.sqrt(periods_per_year) if deviation > 0 else 0.0


def sortino_ratio(
    returns: Sequence[Any], *, risk_free_rate: float = 0.0, periods_per_year: float = 252.0
) -> float:
    values = [value for item in returns if (value := _number(item)) is not None]
    if len(values) < 2 or periods_per_year <= 0:
        return 0.0
    rf = (1.0 + risk_free_rate) ** (1.0 / periods_per_year) - 1.0
    excess = [value - rf for value in values]
    downside = [min(value, 0.0) ** 2 for value in excess]
    downside_deviation = math.sqrt(statistics.fmean(downside))
    return statistics.fmean(excess) / downside_deviation * math.sqrt(periods_per_year) if downside_deviation > 0 else 0.0


def calmar_ratio(equity: Sequence[Any], *, periods_per_year: float = 252.0) -> float:
    drawdown = abs(max_drawdown(equity))
    return annualized_return(equity, periods_per_year=periods_per_year) / drawdown if drawdown > 0 else 0.0


def trade_metrics(deals: Iterable[Mapping[str, Any]]) -> dict[str, float | int]:
    pnls: list[float] = []
    for deal in deals:
        value = next((_number(deal.get(key)) for key in ("pnl", "net_pnl", "pnl_amount", "profit") if _number(deal.get(key)) is not None), None)
        if value is not None:
            pnls.append(value)
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_loss = abs(sum(losses))
    streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    for value in pnls:
        if value > 0:
            streak = max(1, streak + 1)
            max_win_streak = max(max_win_streak, streak)
        elif value < 0:
            streak = min(-1, streak - 1)
            max_loss_streak = max(max_loss_streak, abs(streak))
        else:
            streak = 0
    return {
        "n_trades": len(pnls),
        "win_rate": len(wins) / len(pnls) if pnls else 0.0,
        "profit_factor": sum(wins) / gross_loss if gross_loss > 0 else 0.0,
        "average_trade_pnl": statistics.fmean(pnls) if pnls else 0.0,
        "max_trade_pnl": max(pnls, default=0.0),
        "min_trade_pnl": min(pnls, default=0.0),
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
    }


def benchmark_metrics(equity: Sequence[Any], benchmark: Sequence[Any]) -> dict[str, float]:
    strategy = _series(equity)
    market = _series(benchmark, keys=("value", "equity", "close", "balance"))
    if len(strategy) < 2 or len(market) < 2 or strategy[0] <= 0 or market[0] <= 0:
        return {"benchmark_return": 0.0, "alpha": 0.0, "beta": 0.0}
    returns = equity_returns(strategy)
    benchmark_returns = equity_returns(market)
    n = min(len(returns), len(benchmark_returns))
    returns = returns[-n:]
    benchmark_returns = benchmark_returns[-n:]
    variance = statistics.pvariance(benchmark_returns) if n > 1 else 0.0
    covariance = statistics.pvariance(benchmark_returns) if n > 1 else 0.0
    if n > 1:
        mean_s = statistics.fmean(returns)
        mean_b = statistics.fmean(benchmark_returns)
        covariance = statistics.fmean((s - mean_s) * (b - mean_b) for s, b in zip(returns, benchmark_returns))
    beta = covariance / variance if variance > 0 else 0.0
    return {
        "benchmark_return": market[-1] / market[0] - 1.0,
        "alpha": statistics.fmean(returns) - beta * statistics.fmean(benchmark_returns) if n else 0.0,
        "beta": beta,
    }


def compute_metrics(
    equity: Sequence[Any],
    *,
    deals: Iterable[Mapping[str, Any]] = (),
    benchmark: Sequence[Any] = (),
    risk_free_rate: float = 0.0,
    periods_per_year: float = 252.0,
) -> dict[str, float | int]:
    returns = equity_returns(equity)
    result: dict[str, float | int] = {
        "total_return": (_series(equity)[-1] / _series(equity)[0] - 1.0) if len(_series(equity)) >= 2 else 0.0,
        "annualized_return": annualized_return(equity, periods_per_year=periods_per_year),
        "max_drawdown": max_drawdown(equity),
        "annualized_volatility": annualized_volatility(returns, periods_per_year=periods_per_year),
        "sharpe": sharpe_ratio(returns, risk_free_rate=risk_free_rate, periods_per_year=periods_per_year),
        "sortino": sortino_ratio(returns, risk_free_rate=risk_free_rate, periods_per_year=periods_per_year),
        "calmar": calmar_ratio(equity, periods_per_year=periods_per_year),
        "n_periods": len(_series(equity)),
    }
    result.update(trade_metrics(deals))
    if benchmark:
        result.update(benchmark_metrics(equity, benchmark))
    return result


def enrich_metrics(
    existing: Mapping[str, Any],
    equity: Sequence[Any],
    *,
    deals: Iterable[Mapping[str, Any]] = (),
    benchmark: Sequence[Any] = (),
    risk_free_rate: float = 0.0,
    periods_per_year: float = 252.0,
) -> dict[str, Any]:
    """只补齐缺失指标，保留引擎已经提供的原始口径。"""
    computed = compute_metrics(
        equity,
        deals=deals,
        benchmark=benchmark,
        risk_free_rate=risk_free_rate,
        periods_per_year=periods_per_year,
    )
    result = dict(existing)
    for key, value in computed.items():
        result.setdefault(key, value)
    return result


__all__ = [
    "annualized_return",
    "annualized_volatility",
    "benchmark_metrics",
    "calmar_ratio",
    "compute_metrics",
    "drawdown_curve",
    "enrich_metrics",
    "equity_returns",
    "max_drawdown",
    "sharpe_ratio",
    "sortino_ratio",
    "trade_metrics",
]
