from __future__ import annotations

import math

import pytest

from app.indicators import atr, bollinger, cross, ema, ma, macd, rsi, true_range


def test_ma_ema_and_rsi_have_stable_warmup_semantics() -> None:
    values = [1, 2, 3, 4, 5]
    assert math.isnan(ma(values, 3)[0])
    assert ma(values, 3)[-1] == 4.0
    assert ema(values, 3)[2] == 2.0
    assert rsi(values, 3)[3] == 100.0


def test_macd_bollinger_and_atr() -> None:
    values = [1, 2, 3, 4, 5, 6]
    dif, dea, hist = macd(values, 2, 3, 2)
    assert len(dif) == len(dea) == len(hist) == len(values)
    upper, middle, lower = bollinger(values, 3, 2)
    assert middle[-1] == 5.0
    assert upper[-1] > middle[-1] > lower[-1]
    ranges = true_range([2, 3, 4], [0, 1, 2], [1, 2, 3])
    assert ranges == [2.0, 2.0, 2.0]
    assert atr([2, 3, 4], [0, 1, 2], [1, 2, 3], 2)[-1] == 2.0


def test_cross_requires_equal_lengths_and_detects_upward_cross() -> None:
    assert cross([1, 2, 4], [2, 2, 3]) == [False, False, True]
    with pytest.raises(ValueError):
        cross([1], [1, 2])

