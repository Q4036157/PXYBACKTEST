# -*- coding: utf-8 -*-
"""
策略基类与数据上下文定义

所有策略都继承 BaseStrategy，只需要实现：
  - parameters: 可调参数
  - select_symbols(): 每天开盘选股
  - should_exit(): 判断是否卖出
  - get_exit_price(): 卖出价格

用户不需要关心数据加载、费用计算、绩效统计。
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
import pandas as pd


@dataclass
class BarData:
    """单根1分钟K线"""
    symbol: str
    datetime: str
    time: str      # "HH:MM:SS"
    date: str      # "YYYY-MM-DD"
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class Trade:
    """一笔完整交易"""
    date: str
    symbol: str
    entry_price: float
    entry_time: str
    exit_price: float
    exit_time: str
    exit_reason: str
    return_pct: float      # 扣费后净收益
    first_gain: float = 0.0  # 首分钟涨幅（策略自定义）


class BaseStrategy:
    """
    横截面策略基类。

    子类只需要实现以下方法：
      - parameters: 策略参数字典
      - filter_universe(): 过滤不可交易的股票
      - score(): 对每只股票打分，返回分数(越高越优先)
      - select_top_n(): 选前N只
      - get_exit_rule(): 返回卖出规则
    """

    # 策略名称（自动用文件名）
    name: str = "base"

    # 可调参数（子类覆盖）
    parameters: dict[str, Any] = {
        "top_n": 10,
        "min_price": 2.0,
        "min_first_gain": 0.005,
    }

    # 数据要求
    required_interval: str = "1m"  # 1m / 5m / 1d
    market: str = "cn_equity"

    def filter_universe(self, bar_df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤不可交易的股票。
        bar_df: 当日09:31的所有股票K线，列: symbol, open, close, high, low, volume
        返回过滤后的DataFrame
        """
        return bar_df

    def score(self, bar_df: pd.DataFrame) -> pd.Series:
        """
        对每只股票打分，分数越高越优先买入。
        返回Series, index=symbol, values=分数
        """
        raise NotImplementedError("子类必须实现 score()")

    def select_top_n(self, scored: pd.DataFrame) -> list[str]:
        """从打分结果中选出前N只"""
        n = self.parameters.get("top_n", 10)
        return scored.nlargest(n, "score")["symbol"].tolist()

    def get_exit_rule(self) -> dict:
        """
        返回卖出规则:
        {
            "type": "time" | "stop_profit_loss",
            "time": "10:30" | "close",  # type=time时
            "take_profit": 0.03,         # type=stop_profit_loss时
            "stop_loss": 0.03,
            "force_exit_time": "10:30" | "close",
        }
        """
        return {"type": "time", "time": "close"}

    def on_before_backtest(self, all_data: dict[str, pd.DataFrame]):
        """回测开始前调用，可做预处理"""
        pass
