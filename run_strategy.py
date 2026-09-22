# -*- coding: utf-8 -*-
"""
策略回测运行器

用法:
  cd D:\\x1\\x2\\PXYBACKTEST
  python run_strategy.py strategies.first_minute_top10 --start 2026-07-01 --end 2026-09-22
  python run_strategy.py strategies.first_minute_top10 --exit 1030
  python run_strategy.py strategies.first_minute_top10 --exit stop3
"""

from __future__ import annotations
import argparse
import importlib
import os
import sys
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pyarrow.parquet as pq

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from strategies.strategy_base import BaseStrategy, Trade

# 数据湖路径
LAKE_ROOT = r"E:\pxy-runtime\PXYDATA\data\normalized\kline_1m"

# 交易成本
COMMISSION_RATE = 0.0001      # 佣金万1
COMMISSION_MIN = 1.0           # 最低佣金1元
STAMP_TAX = 0.0005             # 印花税0.05%(卖出)
SLIPPAGE = 0.0                 # 滑点


def load_data(start_date: str, end_date: str) -> pd.DataFrame:
    """从数据湖加载1分钟K线"""
    dates = sorted([d for d in os.listdir(LAKE_ROOT) if d.startswith("date=")])
    dates = [d for d in dates if start_date <= d[5:] <= end_date]
    if not dates:
        raise ValueError(f"No data found between {start_date} and {end_date}")

    print(f"Loading {len(dates)} trading days...")
    frames = []
    for d in dates:
        f = os.path.join(LAKE_ROOT, d, "part.parquet")
        df = pq.read_table(f).to_pandas()
        frames.append(df)
    all_df = pd.concat(frames, ignore_index=True)
    all_df["datetime"] = pd.to_datetime(all_df["datetime"])
    all_df["time"] = all_df["datetime"].dt.strftime("%H:%M:%S")
    all_df["date"] = all_df["datetime"].dt.strftime("%Y-%m-%d")
    print(f"Loaded {len(all_df):,} rows, {all_df['symbol'].nunique()} symbols")
    return all_df


def calc_fee(trade_value: float, is_buy: bool) -> float:
    """计算交易费用"""
    commission = max(trade_value * COMMISSION_RATE, COMMISSION_MIN)
    stamp = trade_value * STAMP_TAX if not is_buy else 0
    return commission + stamp


def run_backtest(strategy: BaseStrategy, all_df: pd.DataFrame, exit_override: str | None = None):
    """
    运行回测
    exit_override: 覆盖策略的退出规则, 可选:
      "1030" / "close" / "stop3_1030" / "stop3_close" / "stop10_1030" / "stop10_close"
    """
    # 应用退出规则覆盖
    if exit_override:
        rule_map = {
            "1030": {"type": "time", "time": "10:30"},
            "close": {"type": "time", "time": "close"},
            "stop3_1030": {"type": "stop_profit_loss", "take_profit": 0.03, "stop_loss": 0.03, "force_exit": "10:30"},
            "stop3_close": {"type": "stop_profit_loss", "take_profit": 0.03, "stop_loss": 0.03, "force_exit": "close"},
            "stop10_1030": {"type": "stop_profit_loss", "take_profit": 0.10, "stop_loss": 0.10, "force_exit": "10:30"},
            "stop10_close": {"type": "stop_profit_loss", "take_profit": 0.10, "stop_loss": 0.10, "force_exit": "close"},
        }
        exit_rule = rule_map.get(exit_override)
        if exit_rule is None:
            print(f"Unknown exit rule: {exit_override}, using strategy default")
            exit_rule = strategy.get_exit_rule()
    else:
        exit_rule = strategy.get_exit_rule()

    dates = sorted(all_df["date"].unique())
    trades = []

    # 预处理: 每只股票的全部数据按日期索引
    print(f"Running backtest: {strategy.name}")
    print(f"  Exit rule: {exit_rule}")
    print(f"  Period: {dates[0]} ~ {dates[-1]} ({len(dates)} days)")

    for di, date in enumerate(dates[:-1]):
        # 当日09:31的bar
        day_df = all_df[all_df["date"] == date]
        bar_0931 = day_df[day_df["time"] == "09:31:00"].copy()
        if len(bar_0931) == 0:
            continue

        # 计算首分钟涨幅
        bar_0931["first_gain"] = bar_0931["close"] / bar_0931["open"] - 1

        # 策略过滤+打分
        filtered = strategy.filter_universe(bar_0931)
        if len(filtered) == 0:
            continue
        filtered["score"] = strategy.score(filtered)
        picks = strategy.select_top_n(filtered)

        # 次日数据
        next_date = dates[di + 1]
        next_df = all_df[all_df["date"] == next_date]

        for sym in picks:
            row = bar_0931[bar_0931["symbol"] == sym].iloc[0]
            entry_price = float(row["close"])
            first_gain = float(row["first_gain"])

            # 找卖出价格
            sym_next = next_df[next_df["symbol"] == sym].sort_values("datetime")
            if len(sym_next) == 0:
                continue

            exit_price, exit_time, exit_reason = _find_exit(
                sym_next, entry_price, exit_rule
            )
            if exit_price is None:
                continue

            # 计算净收益(扣费用)
            buy_value = entry_price * 100
            sell_value = exit_price * 100
            fee = calc_fee(buy_value, is_buy=True) + calc_fee(sell_value, is_buy=False)
            net_ret = (sell_value - buy_value - fee) / buy_value

            trades.append(Trade(
                date=date,
                symbol=sym,
                entry_price=entry_price,
                entry_time="09:31:00",
                exit_price=exit_price,
                exit_time=exit_time,
                exit_reason=exit_reason,
                return_pct=net_ret,
                first_gain=first_gain,
            ))

        if (di + 1) % 10 == 0:
            print(f"  {date} ({di+1}/{len(dates)})")

    return trades, dates


def _find_exit(day_df: pd.DataFrame, entry_price: float, rule: dict):
    """根据规则找卖出点"""
    if rule["type"] == "time":
        target_time = "10:30:00" if rule["time"] == "10:30" else "15:00:00"
        row = day_df[day_df["time"] == target_time]
        if len(row) == 0:
            row = day_df.tail(1)
        if len(row) == 0:
            return None, None, "no_data"
        r = row.iloc[0]
        return float(r["close"]), r["time"], f"time_{rule['time']}"

    elif rule["type"] == "stop_profit_loss":
        tp = entry_price * (1 + rule["take_profit"])
        sl = entry_price * (1 - rule["stop_loss"])
        force_time = "10:30:00" if rule["force_exit"] == "10:30" else "15:00:00"

        for _, r in day_df.iterrows():
            if float(r["high"]) >= tp:
                return tp, r["time"], "take_profit"
            if float(r["low"]) <= sl:
                return sl, r["time"], "stop_loss"
            if r["time"] >= force_time:
                return float(r["close"]), r["time"], f"force_{rule['force_exit']}"
        last = day_df.iloc[-1]
        return float(last["close"]), last["time"], "end_of_day"

    return None, None, "unknown"


def report(trades: list[Trade], dates: list[str]):
    """输出绩效报告"""
    if not trades:
        print("No trades!")
        return

    df = pd.DataFrame([{
        "date": t.date, "symbol": t.symbol,
        "entry": t.entry_price, "exit": t.exit_price,
        "return": t.return_pct, "first_gain": t.first_gain,
        "exit_reason": t.exit_reason,
    } for t in trades])

    print("\n" + "=" * 60)
    print(f"总交易数: {len(df)}")
    print(f"交易天数: {df['date'].nunique()}")
    print(f"日均交易: {len(df)/df['date'].nunique():.1f}")
    print(f"胜率: {(df['return']>0).mean()*100:.1f}%")
    print(f"平均每笔: {df['return'].mean()*100:.2f}%")
    print(f"中位数: {df['return'].median()*100:.2f}%")
    print(f"最好: {df['return'].max()*100:.2f}%")
    print(f"最差: {df['return'].min()*100:.2f}%")

    # 累计收益
    daily = df.groupby("date")["return"].mean()
    cum = (1 + daily).cumprod()
    total = (cum.iloc[-1] - 1) * 100
    sharpe = daily.mean() / daily.std() * (252**0.5) if daily.std() > 0 else 0
    maxdd = ((cum / cum.cummax() - 1).min()) * 100
    print(f"\n累计收益: {total:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {maxdd:.2f}%")
    print("=" * 60)

    return df


def main():
    parser = argparse.ArgumentParser(description="PXY策略回测")
    parser.add_argument("strategy", help="策略模块路径, 如 strategies.first_minute_top10")
    parser.add_argument("--start", default="2026-07-01", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", default="2026-09-22", help="结束日期 YYYY-MM-DD")
    parser.add_argument("--exit", default=None, help="退出规则覆盖")
    args = parser.parse_args()

    # 加载策略
    mod = importlib.import_module(args.strategy)
    # 找到策略类(继承BaseStrategy)
    strategy_cls = None
    for name in dir(mod):
        obj = getattr(mod, name)
        if isinstance(obj, type) and issubclass(obj, BaseStrategy) and obj is not BaseStrategy:
            strategy_cls = obj
            break
    if strategy_cls is None:
        print(f"Error: No BaseStrategy subclass found in {args.strategy}")
        sys.exit(1)

    strategy = strategy_cls()
    print(f"Strategy: {strategy.name}")
    print(f"Parameters: {strategy.parameters}")

    # 加载数据
    all_df = load_data(args.start, args.end)

    # 运行回测
    trades, dates = run_backtest(strategy, all_df, args.exit)

    # 输出报告
    df = report(trades, dates)
    if df is not None:
        out = os.path.join(PROJECT_ROOT, "last_backtest_trades.csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n交易明细已保存: {out}")


if __name__ == "__main__":
    main()
