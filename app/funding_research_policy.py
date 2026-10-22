"""资金费研究的纯计算规则；不读取账户，不产生真实订单。"""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import UTC, datetime
from functools import lru_cache
import math
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

MINUTE = 60_000
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def iso(stamp: int) -> str:
    return datetime.fromtimestamp(stamp / 1000, UTC).isoformat(timespec="seconds")


def number(value: Any, *, positive: bool = False) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError("输入缺少有限数值")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        raise ValueError("输入数值无效")
    return result


def confirmed_ms(value: str) -> int:
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return int(stamp.timestamp() * 1000)


def event_minute(stamp: int) -> int:
    """旧 Gate 记录有秒级延迟，仅允许五秒内归到结算整分钟。"""
    rounded = round(stamp / MINUTE) * MINUTE
    if abs(stamp - rounded) > 5000:
        raise ValueError("结算事件偏离整分钟超过五秒")
    return rounded


class FundingHistory:
    """预测按真实观察时刻可见，最终结算与历史训练可见时间分开。"""

    def __init__(self, node: dict, final_labels: dict | None = None,
                 prices: dict | None = None):
        self.has_prices = prices is not None
        self.prices: dict[tuple[str, str], dict[int, list]] = {}
        self.price_times: dict[tuple[str, str], list[int]] = {}
        for dataset in (prices or {}).get("datasets", []):
            key = dataset["symbol"], dataset["kind"]
            if key in self.prices or key[1] not in {"trade", "mark"}:
                raise ValueError("分钟价格项重复或类型无效")
            bars = {}
            for bar in dataset["bars"]:
                stamp = int(bar[0])
                if stamp % MINUTE or len(bar) < 5:
                    raise ValueError("分钟价格时间戳或列数无效")
                values = [number(value, positive=True) for value in bar[1:5]]
                if values[2] > min(values[0], values[3]) or values[1] < max(values[0], values[3]):
                    raise ValueError("分钟价格 OHLC 关系无效")
                if stamp in bars and bars[stamp] != values:
                    raise ValueError("同一价格时刻存在冲突数据")
                bars[stamp] = values
            self.prices[key] = bars
            self.price_times[key] = sorted(bars)
        quotes: dict[str, dict[int, dict]] = defaultdict(dict)
        for row in node["predictions"]:
            item = dict(row)
            stamp = int(item["observed_at_ms"])
            item["rate"] = number(item["rate"])
            item["mark_price"] = number(item["mark_price"], positive=True)
            item["interval_hours"] = number(item["interval_hours"], positive=True)
            if int(item["next_funding_ts"]) <= stamp:
                raise ValueError("预测观测的下一结算已经过期")
            symbol = item["symbol"]
            if stamp in quotes[symbol] and quotes[symbol][stamp] != item:
                raise ValueError("同一品种时刻存在冲突预测")
            quotes[symbol][stamp] = item
        self.quotes = {key: [items[t] for t in sorted(items)] for key, items in quotes.items()}
        self.times = {key: [r["observed_at_ms"] for r in rows] for key, rows in self.quotes.items()}
        self.training: dict[str, list[dict]] = defaultdict(list)
        finals: dict[tuple[str, int], dict] = {}
        for row in node["settlements"]:
            item = {"symbol": row["symbol"], "ts": event_minute(int(row["funding_time"])),
                    "rate": number(row["rate"]),
                    "available_ms": confirmed_ms(row["confirmed_at"]), "source": row["source"]}
            if (item["symbol"], item["ts"]) in finals:
                raise ValueError("训练结算事件重复")
            self.training[item["symbol"]].append(item)
            finals[item["symbol"], item["ts"]] = item
        for row in (final_labels or {}).get("data", []):
            item = {"symbol": row["symbol"],
                    "ts": event_minute(int(row["settlement_time_ms"])),
                    "rate": number(row["rate_decimal"]), "source": row["source"]}
            # 新拉取的最终值只用作事后收益核算，不能进入历史决策训练。
            finals[item["symbol"], item["ts"]] = item
        self.finals = finals
        self.final_times: dict[str, list[int]] = defaultdict(list)
        for symbol, stamp in finals:
            self.final_times[symbol].append(stamp)
        for stamps in self.final_times.values():
            stamps.sort()
        self.first_ms = min((v[0] for v in self.times.values()), default=None)
        self.last_ms = max((v[-1] for v in self.times.values()), default=None)

    def quote(self, symbol: str, now: int, *, max_age_ms: int = 6 * MINUTE) -> dict | None:
        stamps = self.times.get(symbol, [])
        index = bisect_right(stamps, now) - 1
        if index < 0 or now - stamps[index] > max_age_ms:
            return None
        return self.quotes[symbol][index]

    def market_price(self, symbol: str, now: int, *, kind: str = "trade") -> float | None:
        if not self.has_prices:
            quote = self.quote(symbol, now)
            return quote["mark_price"] if quote is not None else None
        key = symbol, kind
        stamps = self.price_times.get(key, [])
        index = bisect_right(stamps, now - MINUTE) - 1
        if index < 0 or now - (stamps[index] + MINUTE) > MINUTE:
            return None
        return self.prices[key][stamps[index]][3]

    def execution(self, short: str, long: str, signal_ms: int) -> tuple[int, float, float] | None:
        if not self.has_prices:
            a, b = self.market_price(short, signal_ms), self.market_price(long, signal_ms)
            return (signal_ms, a, b) if a is not None and b is not None else None
        stamp = (signal_ms // MINUTE + 1) * MINUTE
        a = self.prices.get((short, "trade"), {}).get(stamp)
        b = self.prices.get((long, "trade"), {}).get(stamp)
        return (stamp, a[0], b[0]) if a is not None and b is not None else None

    @lru_cache(maxsize=250_000)
    def decay(self, symbol: str, now: int) -> tuple[float, int]:
        rows = sorted((r for r in self.training[symbol]
                       if now - 7 * DAY <= r["ts"] < now and r["available_ms"] <= now),
                      key=lambda r: r["ts"])
        if len(rows) < 20:
            return .7, len(rows)
        previous, current = [r["rate"] for r in rows[:-1]], [r["rate"] for r in rows[1:]]
        denominator = sum(x * x for x in previous)
        if denominator <= 1e-20:
            return .7, len(rows)
        phi = sum(x * y for x, y in zip(previous, current)) / denominator
        return max(0., min(1., phi)), len(rows)

    def value(self, short: str, long: str, now: int, *, notional: float,
              horizon_ms: int = DAY, notionals: tuple[float, float] | None = None) -> dict | None:
        legs = [self.quote(short, now), self.quote(long, now)]
        if any(leg is None or int(leg["next_funding_ts"]) <= now for leg in legs):
            return None
        prices = self.market_price(short, now), self.market_price(long, now)
        if any(price is None for price in prices):
            return None
        earliest = min(int(leg["next_funding_ts"]) for leg in legs)
        gross = next_net = 0.
        fits = []
        for symbol, leg, sign, leg_notional in zip((short, long), legs, (1, -1),
                                                  notionals or (notional, notional)):
            phi, count = self.decay(symbol, now)
            fits.append({"symbol": symbol, "phi": phi, "history_count": count})
            stamp = int(leg["next_funding_ts"])
            interval = int(leg["interval_hours"] * HOUR)
            if interval <= 0:
                raise ValueError("资金费周期必须为正")
            rate, index = leg["rate"], 0
            while stamp <= now + horizon_ms:
                amount = sign * leg_notional * rate * phi ** index
                gross += amount
                if stamp == earliest:
                    next_net += amount
                stamp += interval
                index += 1
        return {"gross": gross, "next_net": next_net, "next_ms": earliest,
                "short_mark": prices[0], "long_mark": prices[1],
                "fits": fits}

    def favorable_basis(self, short: str, long: str, now: int) -> bool:
        current = [self.market_price(short, now), self.market_price(long, now)]
        if any(row is None for row in current):
            return False
        ratios = []
        for stamp in range(now - HOUR, now, 5 * MINUTE):
            a, b = self.market_price(short, stamp), self.market_price(long, stamp)
            if a is not None and b is not None:
                ratios.append(a / b)
        return len(ratios) >= 6 and current[0] / current[1] >= median(ratios)

    def payment(self, symbol: str, start: int, end: int, quantity: float,
                sign: int) -> tuple[float, list[dict], list[dict]]:
        """检查预测曾公告的结算事件，缺失最终值不能当成零收益。"""
        expected = {event_minute(int(r["next_funding_ts"]))
                    for r in self.quotes.get(symbol, [])
                    if r["observed_at_ms"] <= end and start < int(r["next_funding_ts"]) <= end}
        actual = {t for t in self.final_times.get(symbol, []) if start < t <= end}
        missing = [{"symbol": symbol, "event_ms": t, "reason": "missing_final_rate"}
                   for t in sorted(expected - actual)]
        total, events = 0., []
        for stamp in sorted(actual):
            price = self.market_price(symbol, stamp, kind="mark")
            if price is None:
                missing.append({"symbol": symbol, "event_ms": stamp, "reason": "missing_settlement_mark"})
                continue
            rate = self.finals[symbol, stamp]["rate"]
            amount = sign * quantity * price * rate
            total += amount
            events.append({"symbol": symbol, "event_ms": stamp, "rate": rate,
                           "mark": price, "payment": amount})
        return total, events, missing
