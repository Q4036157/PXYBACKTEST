"""公共标记价快照研究回放；结果不冒充可成交盘口回测。"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from itertools import permutations
import math
from typing import Any

from .costs import calculate_fill_cost
from .funding_research_policy import DAY, HOUR, MINUTE, FundingHistory, event_minute, iso
from .kernel import stable_hash
from .replay import ReplayClock


@dataclass(frozen=True)
class ResearchScenario:
    name: str
    lead_minutes: int = 60
    adaptive: bool = True
    rollover: bool = True
    favorable_half: bool = True
    horizon_hours: int = 24
    min_ev_fraction: float = .0005
    fee_bps: float = 2.
    slippage_bps: float = 0.
    notional: float = 10.
    max_hold_hours: int = 168
    basis_stop_fraction: float = .01

    def __post_init__(self):
        for name in ("notional", "basis_stop_fraction", "max_hold_hours", "horizon_hours", "lead_minutes"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError("名义金额、周期、提前时间和止损阈值必须为有限正数")
        for name in ("fee_bps", "slippage_bps", "min_ev_fraction"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError("成本和预期值门槛必须为有限非负数")
        if not isinstance(self.lead_minutes, int):
            raise ValueError("提前分钟必须是整数")


def pairs(history: FundingHistory) -> list[tuple[str, str]]:
    groups: dict[str, list[str]] = defaultdict(list)
    for symbol in history.quotes:
        groups[symbol.split("_SWAP_")[0]].append(symbol)
    return [(a, b) for group in groups.values() for a, b in permutations(sorted(group), 2)]


def candidate(history: FundingHistory, pair: tuple[str, str], now: int,
              scenario: ResearchScenario) -> dict | None:
    short, long = pair
    quotes = [history.quote(symbol, now) for symbol in pair]
    if any(row is None for row in quotes):
        return None
    until = min(int(row["next_funding_ts"]) for row in quotes) - now
    if not 0 < until <= scenario.lead_minutes * MINUTE:
        return None
    if not scenario.adaptive and until <= (scenario.lead_minutes - 1) * MINUTE:
        return None
    value = history.value(short, long, now, notional=scenario.notional,
                          horizon_ms=scenario.horizon_hours * HOUR)
    if value is None:
        return None
    until = value["next_ms"] - now
    if not 0 < until <= scenario.lead_minutes * MINUTE:
        return None
    if not scenario.adaptive and until <= (scenario.lead_minutes - 1) * MINUTE:
        return None
    if value["next_net"] <= 0:
        return None
    if scenario.favorable_half and not history.favorable_basis(short, long, now):
        return None
    roundtrip = 4 * scenario.notional * (scenario.fee_bps + scenario.slippage_bps) / 10_000
    net = value["gross"] - roundtrip
    if net <= 0 or net < scenario.notional * scenario.min_ev_fraction:
        return None
    return {"short": short, "long": long, "value": value, "entry_ev": net}


def close_trade(history: FundingHistory, position: dict, now: int,
                reason: str, scenario: ResearchScenario) -> dict:
    short, long = position["short"], position["long"]
    execution = history.execution(short, long, now)
    if execution is None:
        raise ValueError("平仓时缺少新鲜标记价，不能伪造平仓")
    now, mark_a, mark_b = execution
    quantity_a, quantity_b = position["quantity_a"], position["quantity_b"]
    entry_a, entry_b = position["entry_a"], position["entry_b"]
    basis = quantity_a * (entry_a - mark_a) + quantity_b * (mark_b - entry_b)
    costs = []
    for symbol, price, quantity, side in (
        (short, entry_a, quantity_a, "sell"), (long, entry_b, quantity_b, "buy"),
        (short, mark_a, quantity_a, "buy"), (long, mark_b, quantity_b, "sell"),
    ):
        costs.append(calculate_fill_cost(
            price=price, quantity=quantity, side=side, symbol=symbol,
            commission_rate=scenario.fee_bps / 10_000, min_commission=0,
            stamp_tax_rate=0, transfer_fee_rate=0,
            slippage_ratio=scenario.slippage_bps / 10_000,
        ))
    funding, events, missing = 0., [], list(position.get("missing", []))
    for symbol, quantity, sign in ((short, quantity_a, 1), (long, quantity_b, -1)):
        amount, payments, gaps = history.payment(symbol, position["entry_ms"], now, quantity, sign)
        funding += amount
        events.extend(payments)
        missing.extend(gaps)
    fee = sum(item.total_fee for item in costs)
    slippage = sum(item.slippage_cost for item in costs)
    net = basis + funding - fee - slippage if not missing else None
    return {**position, "exit_ms": now, "entry_time": iso(position["entry_ms"]),
            "exit_time": iso(now), "exit_reason": reason, "exit_a": mark_a, "exit_b": mark_b,
            "hold_hours": (now - position["entry_ms"]) / HOUR,
            "basis_pnl": basis, "funding_pnl": funding if not missing else None,
            "known_funding_subtotal": funding, "commission": fee, "slippage": slippage,
            "net_pnl": net, "funding_events": sorted(events, key=lambda row: row["event_ms"]),
            "missing": missing, "valid": not missing, "censored": reason == "window_end"}


def summarize(trades: list[dict], scenario: ResearchScenario) -> dict:
    valid = [row for row in trades if row["valid"]]
    invalid = len(trades) - len(valid)
    subtotal = sum(row["net_pnl"] for row in valid)
    daily: dict[str, float] = defaultdict(float)
    bases: dict[str, float] = defaultdict(float)
    for row in valid:
        daily[row["exit_time"][:10]] += row["net_pnl"]
        bases[row["short"].split("USDT")[0]] += row["net_pnl"]
    realized, high, drawdown = 0., 0., 0.
    for row in valid:
        realized += row["net_pnl"]
        high = max(high, realized)
        drawdown = min(drawdown, realized - high)
    return {"scenario": asdict(scenario), "n_trades": len(trades), "valid_trades": len(valid),
            "invalid_trades": invalid, "net_pnl": subtotal if invalid == 0 else None,
            "verified_subtotal": subtotal, "basis_pnl": sum(r["basis_pnl"] for r in valid),
            "funding_pnl": sum(r["funding_pnl"] for r in valid),
            "commission": sum(r["commission"] for r in valid),
            "slippage": sum(r["slippage"] for r in valid),
            "win_rate": sum(r["net_pnl"] > 0 for r in valid) / len(valid) if valid else None,
            "average_hold_hours": sum(r["hold_hours"] for r in valid) / len(valid) if valid else None,
            "average_funding_events": sum(len(r["funding_events"]) for r in valid) / len(valid) if valid else None,
            "closed_trade_drawdown_usdt": drawdown if invalid == 0 else None,
            "basis_stop_count": sum(r["exit_reason"] == "basis_stop" for r in trades),
            "censored_count": sum(r["censored"] for r in trades),
            "exit_reasons": dict(Counter(r["exit_reason"] for r in trades)),
            "daily_realized_pnl": dict(daily), "base_realized_pnl": dict(bases)}


def run_proxy_replay(history: FundingHistory, *, start_ms: int, end_ms: int,
                     scenario: ResearchScenario) -> dict:
    if history.first_ms is None or end_ms <= start_ms:
        raise ValueError("研究区间或预测输入为空")
    pair_list = pairs(history)
    clock = ReplayClock(start_time=iso(start_ms))
    position, trades, timeline = None, [], []
    now = ((start_ms + MINUTE - 1) // MINUTE) * MINUTE + 30_000
    for now in range(now, end_ms + 1, MINUTE):
        clock.advance_to(iso(now))
        if position is not None:
            short, long = position["short"], position["long"]
            a, b = history.market_price(short, now), history.market_price(long, now)
            if a is None or b is None:
                if not position["missing"] or position["missing"][-1]["reason"] != "stale_mark_while_held":
                    position["missing"].append({"event_ms": now, "reason": "stale_mark_while_held"})
                continue
            basis = position["quantity_a"] * (position["entry_a"] - a) + (
                position["quantity_b"] * (b - position["entry_b"]))
            value = history.value(short, long, now, notional=scenario.notional,
                                  horizon_ms=scenario.horizon_hours * HOUR,
                                  notionals=(position["quantity_a"] * a,
                                             position["quantity_b"] * b))
            reason = None
            if basis <= -scenario.notional * scenario.basis_stop_fraction:
                reason = "basis_stop"
            elif now - position["entry_ms"] >= scenario.max_hold_hours * HOUR:
                reason = "max_hold"
            elif not scenario.rollover and now >= position["event_ms"] + 5 * MINUTE:
                reason = "event_plus_5"
            elif scenario.rollover and value is not None:
                if value["next_net"] <= 0 and value["next_ms"] - now <= 5 * MINUTE:
                    reason = "next_funding_nonpositive"
                elif value["gross"] <= 0:
                    reason = "hold_ev_nonpositive"
            if reason:
                execution = history.execution(short, long, now)
                if execution is None or execution[0] > end_ms:
                    continue
                trade = close_trade(history, position, now, reason, scenario)
                trades.append(trade)
                timeline.append({"event_time": iso(now), "type": "close", "reason": reason,
                                 "short": short, "long": long})
                position = None
            # 同一决策时刻不直接重开刚平的仓，避免模拟连续原价成交。
            continue
        choices = [row for pair in pair_list if (row := candidate(history, pair, now, scenario))]
        if not choices:
            continue
        choice = sorted(choices, key=lambda r: (-r["entry_ev"], r["short"], r["long"]))[0]
        value = choice["value"]
        execution = history.execution(choice["short"], choice["long"], now)
        if execution is None or execution[0] >= min(end_ms, value["next_ms"]):
            continue
        entry_ms, entry_a, entry_b = execution
        position = {"short": choice["short"], "long": choice["long"], "entry_ms": now,
                    "event_ms": value["next_ms"], "entry_a": entry_a,
                    "entry_b": entry_b,
                    "quantity_a": scenario.notional / value["short_mark"],
                    "quantity_b": scenario.notional / value["long_mark"],
                    "entry_ev": choice["entry_ev"], "entry_next_net": value["next_net"],
                    "entry_fits": value["fits"], "missing": []}
        position["signal_ms"] = now
        position["entry_ms"] = entry_ms
        timeline.append({"event_time": iso(now), "type": "open", "short": choice["short"],
                         "long": choice["long"], "entry_ev": choice["entry_ev"]})
    unclosed = None
    if position is not None:
        signal_ms = end_ms - 1 if history.has_prices else end_ms
        if history.execution(position["short"], position["long"], signal_ms) is not None:
            trades.append(close_trade(history, position, signal_ms, "window_end", scenario))
        else:
            unclosed = position
    summary = summarize(trades, scenario)
    if unclosed is not None:
        summary.update(net_pnl=None, unclosed_positions=1)
    else:
        summary["unclosed_positions"] = 0
    return {**summary, "start": iso(start_ms), "end": iso(end_ms), "slots": 1,
            "trades": trades, "unclosed": unclosed, "timeline_sha256": stable_hash(timeline),
            "limitations": ["ohlcv_not_executable_quotes" if history.has_prices else "mark_prices_not_executable_quotes",
                            "one_minute_decisions_five_minute_rate_observations",
                            "no_partial_fills_or_minimum_sizes", "one_minute_mark_for_funding_not_actual_invoice",
                            "swap_policy_not_replayed", "not_a_live_strategy_parity_test"]}


def funding_reversal_study(history: FundingHistory, *, start_ms: int, end_ms: int,
                           leads: tuple[int, ...] = (60, 45, 30, 15, 5)) -> dict:
    """对同一结算的两腿预测比较最终值，拒绝缺少一腿的结算样本。"""
    results = []
    for lead in leads:
        rows, missing = [], 0
        for short, long in pairs(history):
            announced = {event_minute(int(r["next_funding_ts"]))
                         for symbol in (short, long) for r in history.quotes[symbol]
                         if start_ms <= r["observed_at_ms"] < end_ms}
            for event in sorted(announced):
                now = event - lead * MINUTE
                if not start_ms <= now < event <= end_ms:
                    continue
                a, b = history.quote(short, now), history.quote(long, now)
                if a is None or b is None:
                    continue
                value = history.value(short, long, now, notional=10, horizon_ms=HOUR)
                if value is None or event_minute(value["next_ms"]) != event or value["next_net"] <= 0:
                    continue
                expected, realized, valid = 0., 0., True
                for symbol, quote, sign in ((short, a, 1), (long, b, -1)):
                    # 仅纳入当前时刻已公告在该时刻结算的腿，异步周期不强行凑成双腿。
                    if event_minute(int(quote["next_funding_ts"])) == event:
                        expected += sign * quote["rate"]
                        final = history.finals.get((symbol, event))
                        if final is None:
                            valid = False
                        else:
                            realized += sign * final["rate"]
                if not valid:
                    missing += 1
                    continue
                if expected <= 0:
                    continue
                rows.append({"short": short, "long": long, "event_time": iso(event),
                             "decision_time": iso(now), "predicted_rate": expected,
                             "realized_rate": realized, "flipped": realized < 0})
        results.append({"lead_minutes": lead, "samples": len(rows), "missing_labels": missing,
                        "flips": sum(r["flipped"] for r in rows),
                        "flip_rate": sum(r["flipped"] for r in rows) / len(rows) if rows else None,
                        "mean_predicted_bps": sum(r["predicted_rate"] for r in rows) * 10000 / len(rows) if rows else None,
                        "mean_realized_bps": sum(r["realized_rate"] for r in rows) * 10000 / len(rows) if rows else None,
                        "rows": rows})
    return {"scope": "overlapping_counterfactual_pairs_not_a_portfolio", "results": results}


def timing_diagnostics(history: FundingHistory, study: dict, *, end_ms: int,
                       fee_bps: float = 2.) -> dict:
    """固定在结算后五分钟观察标记价，计算费率为正是否仍会亏损。"""
    reports = []
    for section in study["results"]:
        trades = []
        for row in section["rows"]:
            from datetime import datetime
            entry = int(datetime.fromisoformat(row["decision_time"]).timestamp() * 1000)
            event = int(datetime.fromisoformat(row["event_time"]).timestamp() * 1000)
            exit_ms = event + 5 * MINUTE
            if exit_ms > end_ms:
                continue
            a, b = history.market_price(row["short"], entry), history.market_price(row["long"], entry)
            opening = history.execution(row["short"], row["long"], entry - 1 if history.has_prices else entry)
            closing_signal = exit_ms - 1 if history.has_prices else exit_ms
            closing = history.execution(row["short"], row["long"], closing_signal)
            if a is None or b is None or opening is None or closing is None:
                continue
            position = {"short": row["short"], "long": row["long"], "entry_ms": entry,
                        "entry_a": opening[1], "entry_b": opening[2],
                        "quantity_a": 10 / a, "quantity_b": 10 / b,
                        "event_ms": event}
            trade = close_trade(history, position, closing_signal, "diagnostic_event_plus_5",
                                ResearchScenario(name="diagnostic", fee_bps=fee_bps))
            if trade["valid"]:
                trades.append(trade)
        reports.append({"lead_minutes": section["lead_minutes"], "samples": len(trades),
                        "mean_basis_bps": sum(r["basis_pnl"] for r in trades) * 1000 / len(trades) if trades else None,
                        "mean_funding_bps": sum(r["funding_pnl"] for r in trades) * 1000 / len(trades) if trades else None,
                        "mean_net_bps": sum(r["net_pnl"] for r in trades) * 1000 / len(trades) if trades else None,
                        "net_loss_rate": sum(r["net_pnl"] < 0 for r in trades) / len(trades) if trades else None,
                        "positive_funding_but_net_loss": sum(r["funding_pnl"] > 0 and r["net_pnl"] < 0 for r in trades),
                        "trades": trades})
    keys = lambda rows: {(r["short"], r["long"], r["event_ms"]) for r in rows}
    common = set.intersection(*(keys(report["trades"]) for report in reports)) if reports else set()
    matched = []
    for report in reports:
        rows = [r for r in report["trades"] if (r["short"], r["long"], r["event_ms"]) in common]
        matched.append({"lead_minutes": report["lead_minutes"], "samples": len(rows),
                        "mean_net_bps": sum(r["net_pnl"] for r in rows) * 1000 / len(rows) if rows else None})
    return {"scope": "overlapping_counterfactuals_not_portfolio_returns", "single_leg_notional": 10,
            "fee_bps_per_leg_fill": fee_bps, "slippage_bps_per_leg_fill": 0,
            "basis_price": "one_minute_trade_ohlcv" if history.has_prices else "mark_not_executable", "results": reports,
            "matched_positive_at_all_leads_cohort": matched}
