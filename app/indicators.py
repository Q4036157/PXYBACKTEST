"""轻量、无交易接口依赖的序列指标。

这些函数采用通用 Python 序列并返回等长 ``list[float]``；尚未形成窗口的值为
``NaN``。它们可用于回测策略特征或在 PXYDATA 快照适配器中生成派生列，不负责下单。
"""
from __future__ import annotations

import math
from collections.abc import Sequence


def _values(values: Sequence[float]) -> list[float]:
    return [float(value) if value is not None else math.nan for value in values]


def _window_mean(values: list[float], end: int, period: int) -> float:
    window = values[end - period + 1 : end + 1]
    return sum(window) / period if all(math.isfinite(value) for value in window) else math.nan


def ma(values: Sequence[float], period: int) -> list[float]:
    """简单移动平均。"""
    if period < 1:
        raise ValueError("period 必须大于 0")
    data = _values(values)
    return [math.nan if index + 1 < period else _window_mean(data, index, period) for index in range(len(data))]


def ema(values: Sequence[float], period: int) -> list[float]:
    """以首个完整窗口均值为种子的指数移动平均。"""
    if period < 1:
        raise ValueError("period 必须大于 0")
    data = _values(values)
    result = [math.nan] * len(data)
    if len(data) < period:
        return result
    seed = _window_mean(data, period - 1, period)
    if not math.isfinite(seed):
        return result
    result[period - 1] = seed
    alpha = 2.0 / (period + 1.0)
    for index in range(period, len(data)):
        result[index] = data[index] * alpha + result[index - 1] * (1.0 - alpha) if math.isfinite(data[index]) else math.nan
    return result


def rsi(values: Sequence[float], period: int = 14) -> list[float]:
    """Wilder RSI，返回 0 到 100 的序列。"""
    if period < 1:
        raise ValueError("period 必须大于 0")
    data = _values(values)
    result = [math.nan] * len(data)
    if len(data) <= period:
        return result
    gains: list[float] = []
    losses: list[float] = []
    for previous, current in zip(data, data[1:]):
        if not math.isfinite(previous) or not math.isfinite(current):
            gains.append(math.nan)
            losses.append(math.nan)
        else:
            change = current - previous
            gains.append(max(change, 0.0))
            losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period if all(math.isfinite(value) for value in gains[:period]) else math.nan
    avg_loss = sum(losses[:period]) / period if all(math.isfinite(value) for value in losses[:period]) else math.nan

    def value(gain: float, loss: float) -> float:
        if not math.isfinite(gain) or not math.isfinite(loss):
            return math.nan
        if loss == 0:
            return 100.0 if gain > 0 else 50.0
        if gain == 0:
            return 0.0
        return 100.0 - 100.0 / (1.0 + gain / loss)

    result[period] = value(avg_gain, avg_loss)
    for index in range(period + 1, len(data)):
        gain = gains[index - 1]
        loss = losses[index - 1]
        if math.isfinite(gain) and math.isfinite(loss) and math.isfinite(avg_gain) and math.isfinite(avg_loss):
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
        else:
            avg_gain = avg_loss = math.nan
        result[index] = value(avg_gain, avg_loss)
    return result


def macd(
    values: Sequence[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9
) -> tuple[list[float], list[float], list[float]]:
    """返回 ``(DIF, DEA, HIST)``。"""
    if not (1 <= fast_period <= slow_period) or signal_period < 1:
        raise ValueError("MACD 周期参数无效")
    fast = ema(values, fast_period)
    slow = ema(values, slow_period)
    dif = [a - b if math.isfinite(a) and math.isfinite(b) else math.nan for a, b in zip(fast, slow)]
    dea = ema(dif, signal_period)
    hist = [a - b if math.isfinite(a) and math.isfinite(b) else math.nan for a, b in zip(dif, dea)]
    return dif, dea, hist


def bollinger(values: Sequence[float], period: int = 20, deviations: float = 2.0) -> tuple[list[float], list[float], list[float]]:
    """返回 ``(upper, middle, lower)``，标准差使用总体标准差。"""
    if period < 1 or deviations < 0:
        raise ValueError("BOLL 参数无效")
    data = _values(values)
    upper: list[float] = []
    middle: list[float] = []
    lower: list[float] = []
    for index in range(len(data)):
        mean = math.nan if index + 1 < period else _window_mean(data, index, period)
        if not math.isfinite(mean):
            upper.append(math.nan)
            middle.append(math.nan)
            lower.append(math.nan)
            continue
        window = data[index - period + 1 : index + 1]
        std = math.sqrt(sum((value - mean) ** 2 for value in window) / period)
        middle.append(mean)
        upper.append(mean + deviations * std)
        lower.append(mean - deviations * std)
    return upper, middle, lower


def true_range(high: Sequence[float], low: Sequence[float], close: Sequence[float]) -> list[float]:
    """计算真实波幅；首根没有前收盘时使用 high-low。"""
    if not (len(high) == len(low) == len(close)):
        raise ValueError("high、low、close 长度必须一致")
    highs, lows, closes = _values(high), _values(low), _values(close)
    result: list[float] = []
    for index, (hi, lo) in enumerate(zip(highs, lows)):
        previous = closes[index - 1] if index else math.nan
        if not all(math.isfinite(value) for value in (hi, lo)):
            result.append(math.nan)
        elif not math.isfinite(previous):
            result.append(hi - lo)
        else:
            result.append(max(hi - lo, abs(hi - previous), abs(lo - previous)))
    return result


def atr(high: Sequence[float], low: Sequence[float], close: Sequence[float], period: int = 14) -> list[float]:
    """真实波幅的简单移动平均。"""
    return ma(true_range(high, low, close), period)


def cross(previous: Sequence[float], current: Sequence[float]) -> list[bool]:
    """检测当前序列从下向上穿越前一序列。"""
    if len(previous) != len(current):
        raise ValueError("两条序列长度必须一致")
    left, right = _values(previous), _values(current)
    result = [False] * len(left)
    for index in range(1, len(left)):
        if all(math.isfinite(value) for value in (left[index - 1], right[index - 1], left[index], right[index])):
            result[index] = left[index - 1] <= right[index - 1] and left[index] > right[index]
    return result


__all__ = ["atr", "bollinger", "cross", "ema", "ma", "macd", "rsi", "true_range"]
