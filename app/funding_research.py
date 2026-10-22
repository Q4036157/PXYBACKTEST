"""资金费策略研究 CLI：冻结输入、时点可见性、单名额回放与准入报告。"""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

from .funding_research_policy import DAY, HOUR, MINUTE, FundingHistory, iso
from .funding_research_replay import (
    ResearchScenario, funding_reversal_study, run_proxy_replay, timing_diagnostics,
)
from .kernel import stable_hash


def read_input(path: Path, contract: str) -> tuple[dict, dict]:
    raw = path.read_bytes()
    data = json.loads(raw)
    if data.get("contract_version") != contract:
        raise ValueError(f"输入契约不匹配：{path.name}")
    return data, {"path": str(path.resolve()), "sha256": hashlib.sha256(raw).hexdigest()}


def window_admission(history: FundingHistory, end_ms: int, days: int) -> dict:
    start = end_ms - days * DAY
    reasons = []
    if history.first_ms is None or history.first_ms > start:
        reasons.append("prediction_history_starts_after_requested_window")
    if history.last_ms is None or history.last_ms < end_ms - 6 * MINUTE:
        reasons.append("prediction_history_ends_before_requested_window")
    reasons.extend(("executable_bid_ask_and_depth_history_missing",
                    "actual_account_fees_and_execution_latency_not_verified"))
    return {"days": days, "start": iso(start), "end": iso(end_ms),
            "status": "not_verified", "net_pnl": None, "reasons": reasons}


def generate_report(node: dict, labels: dict, *, prices: dict | None, end_ms: int,
                    provenance: list[dict], fee_bps: float = 2.,
                    slippages: tuple[float, ...] = (0., 2., 5.)) -> dict:
    history = FundingHistory(node, labels, prices)
    if end_ms > node["end_ms"]:
        raise ValueError("研究截止时间超过冻结预测输入窗口")
    if history.first_ms is None or history.first_ms + HOUR >= end_ms:
        raise ValueError("没有足够的真实预测观测运行研究")
    start = history.first_ms + HOUR
    base = ResearchScenario(name="adaptive60_rollover", fee_bps=fee_bps)
    variants = [
        ResearchScenario(name="t5_tplus5_reference", lead_minutes=5, adaptive=False,
                         rollover=False, favorable_half=False, horizon_hours=1,
                         min_ev_fraction=0., fee_bps=fee_bps),
        *[replace(base, name=f"fixed{lead}_rollover", lead_minutes=lead, adaptive=False)
          for lead in (5, 15, 30, 45, 60)],
        base,
        replace(base, name="adaptive60_single_settlement", rollover=False),
    ]
    replays = []
    for slippage in slippages:
        for scenario in variants:
            current = replace(scenario, slippage_bps=slippage)
            replay = run_proxy_replay(history, start_ms=start, end_ms=end_ms, scenario=current)
            replays.append(replay)
    reversal = funding_reversal_study(history, start_ms=start, end_ms=end_ms)
    timing = timing_diagnostics(history, reversal, end_ms=end_ms, fee_bps=fee_bps)
    coverage = []
    for symbol, stamps in history.times.items():
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        coverage.append({"symbol": symbol, "rows": len(stamps), "first": iso(stamps[0]),
                         "last": iso(stamps[-1]), "max_gap_minutes": max(gaps, default=0) / MINUTE,
                         "gaps_over_six_minutes": sum(gap > 6 * MINUTE for gap in gaps)})
    price_coverage = []
    for (symbol, kind), stamps in history.price_times.items():
        price_coverage.append({"symbol": symbol, "kind": kind, "rows": len(stamps),
                               "first": iso(stamps[0]) if stamps else None,
                               "last": iso(stamps[-1]) if stamps else None,
                               "missing_minutes_inside_span": (stamps[-1] - stamps[0]) // MINUTE + 1 - len(stamps)
                               if stamps else None})
    code_paths = [Path(__file__).with_name(name) for name in (
        "funding_research.py", "funding_research_policy.py", "funding_research_replay.py",
        "costs.py", "kernel.py", "replay.py")]
    code_identity = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    report = {
        "contract_version": "pxybacktest.funding-research-report.v1",
        "admission": "not_ready_for_live_change", "requested_windows": [
            window_admission(history, end_ms, days) for days in (7, 30)],
        "actual_research_start": iso(start), "actual_research_end": iso(end_ms),
        "timezone": "Asia/Tokyo", "input_provenance": provenance,
        "python_version": sys.version,
        "code_sha256": code_identity, "prediction_coverage": coverage,
        "price_coverage": price_coverage, "official_label_errors": labels.get("errors", []),
        "source_observation_rows": sum(len(rows) for rows in history.quotes.values()),
        "official_label_rows": len(labels.get("data", [])),
        "cost_assumptions": {"single_leg_notional_usdt": 10, "total_legs_notional_usdt": 20,
                             "fee_bps_per_leg_fill": fee_bps, "slippage_bps_per_leg_fill": list(slippages)},
        "methodology": {
            "training": "past_seven_days_confirmed_before_decision_ar1_through_origin_min20_fallback0.7_phi_clipped0to1",
            "first_event": "contemporaneous_predicted_rate_only",
            "settlement_labels": "official_final_rates_for_payoff_only",
            "horizon": "each_leg_own_current_interval_expanded_over_24h",
            "entry_basis": "short_to_long_price_ratio_at_or_above_past60min_median",
            "holding": "gross_remaining_funding_no_repeated_exit_cost",
            "execution": "signal_at_minute_plus30s_fill_both_legs_next_minute_open" if prices else "instant_mark_price_proxy",
            "risk": "one_slot_basis_loss_1pct_max168h_next_negative_Tminus5_or_EVnonpositive",
            "basis_stop": "one_minute_close_observation_can_gap_beyond1pct",
            "end_boundary": "funding_at_exit_timestamp_included_before_forced_close",
            "unverified": ["seven_day_max_hold_not_observed", "no_swap_confirmation_test",
                           "one_minute_marks_are_not_exact_funding_settlement_marks",
                           "minimum_order_sizes_partial_fills_liquidations_not_modelled",
                           "eight_selected_coins_not_entire_historical_tradable_universe",
                           "timing_comparisons_are_exploratory_not_independent_out_of_sample_proof"]},
        "reversal_study": reversal, "timing_diagnostics": timing, "replays": replays,
    }
    report["report_sha256"] = stable_hash(report)
    return report


def _fmt(value: Any, digits: int = 4) -> str:
    return "未核实" if value is None else f"{value:.{digits}f}"


def _local(value: str) -> str:
    return datetime.fromisoformat(value).astimezone(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")


def markdown_report(report: dict) -> str:
    costs = report["cost_assumptions"]
    fee = costs["fee_bps_per_leg_fill"]
    slips = "/".join(f"{item:g}" for item in costs["slippage_bps_per_leg_fill"])
    lines = ["# 资金费策略回测研究", "", "**准入结论：暂不修改实盘系统。**", "",
             "7 天和 30 天完整回测均未通过数据准入，净收益保持未知，不填写零收益。",
             f"实际研究区间：{_local(report['actual_research_start'])} 至 {_local(report['actual_research_end'])}（日本时间，UTC+9）。",
             f"已冻结 {report['source_observation_rows']} 条预测观测、{report['official_label_rows']} 条官方结算标签。",
             "", "## 成本与口径", "",
             "每腿名义 10 USDT，两腿合计 20 USDT；资金费名额固定一个。",
             f"基础手续费每腿每次成交 {fee:g} bp，开平两腿合计约 {4 * 10 * fee / 10_000:g} USDT；"
             f"另测每次成交 {slips} bp 额外滑点。",
             "1 bp=0.01%。以下诊断 bp 以单腿 10 USDT 名义为分母，不是账户资金回报率。",
             "若使用分钟 OHLC，决策只见已完成 K 线，订单在下一分钟开盘价加不利滑点撮合。",
             "期末强平与结算同刻时，按先结算再平仓核算；这是边界假设，不能替代交易所实际账单。",
             "盘口、深度、实际费率、成交延迟、最小数量、强平和换仓确认尚未完整验证。",
             "", "## 预测转负", "",
             "只统计入场时预估下一结算净收入为正的方向；不同提前时间的样本池并不完全相同。",
             "这些是可能重叠的交易对结算事件，不能相加成组合收益，也不是独立统计试验。", "",
             "| 提前分钟 | 样本 | 结算转负 | 比例 | 预估净费率 bp | 实际净费率 bp |",
             "|---:|---:|---:|---:|---:|---:|"]
    for row in report["reversal_study"]["results"]:
        lines.append(f"| {row['lead_minutes']} | {row['samples']} | {row['flips']} | "
                     f"{_fmt(row['flip_rate'] * 100 if row['flip_rate'] is not None else None, 2)}% | "
                     f"{_fmt(row['mean_predicted_bps'])} | {_fmt(row['mean_realized_bps'])} |")
    lines += ["", "## 结算后五分钟平仓诊断", "",
              "下表只保留入场价、退出价、结算费率和结算标记价都可核算的样本；",
              "不完整样本被排除，存在覆盖选择偏差，不能据此直接确定最佳开仓时间。",
              "这是不带止损的固定时间对照，基差止损和跨结算持有见后面的单名额组合。", "",
              "| 提前分钟 | 可核算样本 | 平均基差 bp | 平均资金费 bp | 扣费平均净值 bp | 资金费正但净亏次数 |",
              "|---:|---:|---:|---:|---:|---:|"]
    for row in report["timing_diagnostics"]["results"]:
        lines.append(f"| {row['lead_minutes']} | {row['samples']} | {_fmt(row['mean_basis_bps'])} | "
                     f"{_fmt(row['mean_funding_bps'])} | {_fmt(row['mean_net_bps'])} | "
                     f"{row['positive_funding_but_net_loss']} |")
    lines += ["", "同一批所有提前时间均为正预测的交集样本（同样存在条件选择偏差）：", "",
              "| 提前分钟 | 交集样本 | 平均净值 bp |", "|---:|---:|---:|"]
    for row in report["timing_diagnostics"]["matched_positive_at_all_leads_cohort"]:
        lines.append(f"| {row['lead_minutes']} | {row['samples']} | {_fmt(row['mean_net_bps'])} |")
    lines += ["", "## 单名额策略对照", "",
              "固定提前进场与 60 分钟内择时均使用同一套预期值、基差分位和退出规则。",
              "参考 T-5/T+5 只作时间结构对照，不声称与当前实盘逐笔一致。",
              "任何必需收益数据缺失，该场景总净收益均为未知；可核算子集不代替总收益。",
              "期末强平单标为截尾，不能据此验证最长持有七天。", "",
              "| 场景 | 额外滑点 bp/次 | 轮次 | 未核实轮次 | 净收益 U | 可核算子集 U | 基差止损次数 | 期末截尾 |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in report["replays"]:
        scenario = row["scenario"]
        name = {"t5_tplus5_reference": "提前5分钟/结算后5分钟参考",
                "adaptive60_rollover": "60分钟内有利基差/跨结算持有",
                "adaptive60_single_settlement": "60分钟内有利基差/单次结算"}.get(
                    scenario["name"], f"固定提前{scenario['lead_minutes']}分钟/跨结算持有")
        lines.append(f"| {name} | {scenario['slippage_bps']:g} | {row['n_trades']} | "
                     f"{row['invalid_trades']} | {_fmt(row['net_pnl'])} | {_fmt(row['verified_subtotal'])} | "
                     f"{row['basis_stop_count']} | {row['censored_count']} |")
    lines += ["", "## 单名额成本分解", "",
              "下表汇总所有轮次的价格损益、资金费、手续费和滑点；仅全部轮次可核算时展示总量。", "",
              "| 场景 | 额外滑点 bp/次 | 基差 U | 资金费 U | 手续费 U | 滑点 U |",
              "|---|---:|---:|---:|---:|---:|"]
    for row in report["replays"]:
        scenario = row["scenario"]
        if scenario["name"] not in {"adaptive60_rollover", "t5_tplus5_reference"}:
            continue
        values = [row[key] if row["net_pnl"] is not None else None
                  for key in ("basis_pnl", "funding_pnl", "commission", "slippage")]
        name = "60分钟内择时持有" if scenario["name"] == "adaptive60_rollover" else "提前5分钟参考"
        lines.append(f"| {name} | {scenario['slippage_bps']:g} | "
                     + " | ".join(_fmt(value) for value in values) + " |")
    lines += ["", "## 规则与数据限制", "",
              "- 衰减只使用决策前已确认的近七天官方结算，至少二十笔；否则固定 0.7。新补的最终费率仅作收益标签。",
              "- 两腿按各自周期展开未来二十四小时，第一笔取当时公布的预测费率。持有预期值不重复扣平仓成本。",
              "- 价差损失达到单腿名义的 1%、预期值不正、下一笔转不利或满 168 小时触发退出。分钟采样可能穿过止损线。",
              "- 缺少盘口时不能验证有利基差是否可成交；不能用分钟收盘证明每十秒策略、三十秒换仓或实盘盈利。",
              "- 真实预测观测起点见输入覆盖；早期观测无法通过今天下载最终结算值重建。",
              "- 当前结果仅覆盖选定八个币；不代表当时全市场候选，未做独立样本外验证。",
              "", "## 可复现输入", ""]
    for item in report["input_provenance"]:
        lines.append(f"- `{item['path']}`：`{item['sha256']}`")
    lines += ["", f"报告哈希：`{report['report_sha256']}`", "",
              "逐笔成交、资金费事件、缺口与预测拟合记录均保存在同目录 report.json。",
              "本研究不下真实订单，不改 PXYLH，不部署 109/204。"]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-input", type=Path, required=True)
    parser.add_argument("--final-labels", type=Path, required=True)
    parser.add_argument("--prices", type=Path)
    parser.add_argument("--end", required=True, help="带时区的研究截止时间")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fee-bps", type=float, default=2.)
    args = parser.parse_args(argv)
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
    if end.tzinfo is None:
        parser.error("截止时间必须包含时区")
    if (args.output_dir / "report.json").exists() or (args.output_dir / "report.md").exists():
        parser.error("报告已存在，禁止覆盖已完成研究")
    node, node_identity = read_input(args.node_input, "pxybacktest.funding-node-input.v1")
    labels, label_identity = read_input(args.final_labels, "pxybacktest.funding-final-labels.v1")
    prices, provenance = None, [node_identity, label_identity]
    if args.prices:
        prices, identity = read_input(args.prices, "pxybacktest.funding-price-input.v1")
        provenance.append(identity)
    report = generate_report(node, labels, prices=prices, end_ms=int(end.timestamp() * 1000),
                             provenance=provenance, fee_bps=args.fee_bps)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    (args.output_dir / "report.md").write_text(markdown_report(report), encoding="utf-8")
    print(json.dumps({"admission": report["admission"], "report": str((args.output_dir / "report.md").resolve()),
                      "sha256": report["report_sha256"], "scenarios": len(report["replays"]),
                      "requested_windows": report["requested_windows"]}, ensure_ascii=False))
    # 研究运行成功与实盘准入成功是两个状态；命令完成不表示允许改实盘。
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
