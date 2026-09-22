# -*- coding: utf-8 -*-
"""
策略: 开盘首分钟涨幅Top10

逻辑:
  1. 每天09:31(第一根1分钟K线收盘)，计算全市场首分钟涨幅 = 09:31收盘/09:30开盘 - 1
  2. 排除: ST股、股价<2元、涨幅<0.5%、开盘即涨停(>9.5%)、上市不足60天
  3. 选涨幅最大的前10只，在09:31收盘价买入
  4. 卖出规则: 次日10:30 / 次日收盘 / 3%止盈止损 / 10%止盈止损

修改方式:
  - 改 parameters 里的参数
  - 改 score() 里的打分逻辑
  - 改 get_exit_rule() 里的卖出规则
"""

from __future__ import annotations
import pandas as pd
from .strategy_base import BaseStrategy


class FirstMinuteTop10(BaseStrategy):
    name = "first_minute_top10"

    parameters = {
        "top_n": 10,              # 选前N只
        "min_price": 2.0,         # 最低股价(排除仙股/ST)
        "min_first_gain": 0.005,  # 首分钟至少涨0.5%
        "max_gain_limit": 0.095,  # 开盘涨幅超过9.5%买不进(涨停)
        "max_price": 500.0,       # 最高价过滤(可选)
    }

    def filter_universe(self, bar_df: pd.DataFrame) -> pd.DataFrame:
        p = self.parameters
        df = bar_df.copy()
        # 排除低价股
        df = df[df["close"] >= p["min_price"]]
        # 排除超高价股(可选)
        df = df[df["close"] <= p["max_price"]]
        # 排除首分钟涨幅为负
        df = df[df["first_gain"] >= p["min_first_gain"]]
        # 排除开盘涨停买不进
        df = df[df["first_gain"] < p["max_gain_limit"]]
        # 排除异常价格
        df = df[(df["open"] > 0) & (df["close"] > 0)]
        return df

    def score(self, bar_df: pd.DataFrame) -> pd.Series:
        # 首分钟涨幅越大分越高
        return bar_df["first_gain"]

    def get_exit_rule(self) -> dict:
        """
        这里可以切换不同卖出规则测试:
        - {"type": "time", "time": "10:30"}        # 次日10:30卖
        - {"type": "time", "time": "close"}         # 次日收盘卖
        - {"type": "stop_profit_loss", "take_profit": 0.03, "stop_loss": 0.03, "force_exit": "close"}
        - {"type": "stop_profit_loss", "take_profit": 0.10, "stop_loss": 0.10, "force_exit": "10:30"}
        """
        # 默认用次日收盘卖
        return {"type": "time", "time": "close"}
