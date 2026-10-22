"""资金费离线研究的时点、成本、风险与缺口回归。"""

from copy import deepcopy
from datetime import datetime
import importlib.util
from pathlib import Path

import pytest

from app.funding_research import generate_report, main, markdown_report, read_input, window_admission
from app.funding_research_policy import DAY, HOUR, MINUTE, FundingHistory, event_minute, iso
from app.funding_research_replay import ResearchScenario, close_trade, run_proxy_replay

BASE = int(datetime.fromisoformat("2026-10-01T00:00:00+00:00").timestamp() * 1000)
SHORT, LONG = "BTCUSDT_SWAP_BINANCE", "BTCUSDT_SWAP_OKX"


def quote(symbol=SHORT, minute=0, rate=.001, event=60, interval=1, mark=100):
    return {"symbol": symbol, "observed_at_ms": BASE + minute * MINUTE,
            "rate": rate, "next_funding_ts": BASE + event * MINUTE,
            "interval_hours": interval, "mark_price": mark}


def history(*, settlements=None, labels=None, prices=None):
    return FundingHistory({"predictions": [quote(), quote(LONG, rate=.0002, interval=8)],
                           "settlements": settlements or []}, labels, prices)


def price_input(end=130, short_price=None):
    return {"datasets": [
        {"symbol": symbol, "kind": kind, "bars": [
            [BASE + minute * MINUTE, price, price, price, price, 1]
            for minute in range(-65, end + 1)
            for price in [short_price(minute) if short_price and symbol == SHORT else 100]]}
        for symbol in (SHORT, LONG) for kind in ("trade", "mark")]}


def label(symbol=SHORT, minute=60, rate=.001):
    return {"symbol": symbol, "settlement_time_ms": BASE + minute * MINUTE,
            "rate_decimal": rate, "source": "official_test"}


def test_quotes_never_use_future_and_stale_quotes_expire():
    data = {"predictions": [quote(minute=5, rate=-.02), quote()], "settlements": []}
    h = FundingHistory(data)
    assert h.quote(SHORT, BASE - 1) is None
    assert h.quote(SHORT, BASE + 4 * MINUTE)["rate"] == .001
    assert h.quote(SHORT, BASE + 5 * MINUTE)["rate"] == -.02
    assert h.quote(SHORT, BASE + 12 * MINUTE) is None


def test_final_labels_are_payoff_only_and_cannot_train_decay():
    labels = {"data": [label(minute=-i * 60, rate=.99) for i in range(1, 25)]}
    h = history(labels=labels)
    assert h.decay(SHORT, BASE) == (.7, 0)
    assert len(h.finals) == 24


def test_decay_uses_only_past_confirmed_seven_days_and_minimum_twenty():
    rows = [{"symbol": SHORT, "funding_time": BASE - (21 - i) * HOUR,
             "rate": .01 * .8 ** i, "confirmed_at": iso(BASE - HOUR), "source": "test"}
            for i in range(21)]
    rows[-1]["confirmed_at"] = iso(BASE + MINUTE)
    h = history(settlements=rows)
    assert h.decay(SHORT, BASE) == pytest.approx((.8, 20))
    assert h.decay(SHORT, BASE - 2 * HOUR) == (.7, 0)
    assert history(settlements=rows[:19]).decay(SHORT, BASE) == (.7, 19)
    assert h.decay(SHORT, BASE + 8 * DAY) == (.7, 0)


def test_each_leg_uses_own_interval_first_live_rate_and_signed_decimal():
    value = history().value(SHORT, LONG, BASE, notional=10, horizon_ms=9 * HOUR)
    expected_short = 10 * .001 * sum(.7 ** i for i in range(9))
    expected_long = 10 * .0002 * (1 + .7)
    assert value["gross"] == pytest.approx(expected_short - expected_long)
    assert value["next_net"] == pytest.approx(.008)
    held = history().value(SHORT, LONG, BASE, notional=10, horizon_ms=HOUR,
                           notionals=(20, 10))
    assert held["gross"] == pytest.approx(.018)


def test_decision_sees_completed_close_execution_is_next_open():
    prices = price_input(short_price=lambda m: 100 if m < 1 else 120)
    h = history(prices=prices)
    assert h.market_price(SHORT, BASE + MINUTE + 30_000) == 100
    assert h.execution(SHORT, LONG, BASE + MINUTE + 30_000) == (BASE + 2 * MINUTE, 120, 100)
    assert h.market_price(SHORT, BASE + 140 * MINUTE) is None


def test_favorable_basis_uses_past_median_not_future():
    prices = price_input(short_price=lambda m: 100 if m < 0 else 200)
    h = history(prices=prices)
    assert h.favorable_basis(SHORT, LONG, BASE)
    mutated = deepcopy(prices)
    for dataset in mutated["datasets"]:
        for bar in dataset["bars"]:
            if bar[0] >= BASE:
                bar[1:5] = [1] * 4
    assert history(prices=mutated).favorable_basis(SHORT, LONG, BASE)


def test_missing_final_rate_is_unknown_and_funding_sign_matches_legs():
    prices = price_input()
    h = history(labels={"data": [label()]}, prices=prices)
    amount, _, gaps = h.payment(SHORT, BASE, BASE + HOUR, .1, 1)
    assert amount == pytest.approx(.01)
    assert not gaps
    assert h.payment(SHORT, BASE, BASE + HOUR, .1, -1)[0] == pytest.approx(-.01)
    assert h.payment(LONG, BASE, BASE + HOUR, .1, -1)[2][0]["reason"] == "missing_final_rate"
    position = {"short": SHORT, "long": LONG, "entry_ms": BASE,
                "quantity_a": .1, "quantity_b": .1, "entry_a": 100, "entry_b": 100}
    trade = close_trade(h, position, BASE + HOUR, "test", ResearchScenario(name="test"))
    assert trade["net_pnl"] is None
    assert not trade["valid"]


def test_missing_settlement_mark_is_not_zero_payment():
    prices = price_input()
    prices["datasets"] = [r for r in prices["datasets"] if (r["symbol"], r["kind"]) != (SHORT, "mark")]
    h = history(labels={"data": [label()]}, prices=prices)
    assert h.payment(SHORT, BASE, BASE + HOUR, .1, 1)[2][0]["reason"] == "missing_settlement_mark"


def test_four_fill_costs_and_adverse_slippage_are_deducted_once():
    h = history(labels={"data": [label(), label(LONG, rate=.0002)]}, prices=price_input())
    position = {"short": SHORT, "long": LONG, "entry_ms": BASE,
                "quantity_a": .1, "quantity_b": .1, "entry_a": 100, "entry_b": 100}
    trade = close_trade(h, position, BASE + HOUR, "test",
                        ResearchScenario(name="test", slippage_bps=2))
    assert trade["funding_pnl"] == pytest.approx(.008)
    assert trade["commission"] == pytest.approx(.008)
    assert trade["slippage"] == pytest.approx(.008)
    assert trade["net_pnl"] == pytest.approx(-.008)


def test_exit_timestamp_includes_funding_but_entry_timestamp_does_not():
    h = history(labels={"data": [label()]}, prices=price_input())
    assert h.payment(SHORT, BASE, BASE + HOUR, .1, 1)[0] == pytest.approx(.01)
    assert h.payment(SHORT, BASE + HOUR, BASE + 2 * HOUR, .1, 1)[0] == 0


class ScriptedHistory:
    """固定决策向量隔离组合状态机，真实价格/费率规则另行验证。"""

    first_ms = BASE
    has_prices = True
    quotes = {SHORT: [], LONG: []}

    def __init__(self, mode="hold"):
        self.mode = mode

    def quote(self, symbol, now):
        return {"next_funding_ts": BASE + HOUR if now < BASE + HOUR else now + HOUR}

    def value(self, short, long, now, **kwargs):
        negative = self.mode == "negative" and now >= BASE + 55 * MINUTE
        gross = -.1 if self.mode == "ev" and now >= BASE + 10 * MINUTE else .1
        return {"next_ms": self.quote(short, now)["next_funding_ts"],
                "next_net": -.1 if negative else .1, "gross": gross,
                "short_mark": 100, "long_mark": 100, "fits": []}

    def favorable_basis(self, short, long, now):
        return True

    def market_price(self, symbol, now):
        return 102 if self.mode == "stop" and symbol == SHORT and now >= BASE + 10 * MINUTE else 100

    def execution(self, short, long, now):
        stamp = (now // MINUTE + 1) * MINUTE
        return stamp, self.market_price(short, stamp), self.market_price(long, stamp)

    def payment(self, *args):
        return 0., [], []


@pytest.mark.parametrize(("mode", "reason"), [
    ("stop", "basis_stop"), ("negative", "next_funding_nonpositive"),
    ("ev", "hold_ev_nonpositive"), ("hold", "window_end")])
def test_risk_exits_and_one_slot(mode, reason):
    replay = run_proxy_replay(ScriptedHistory(mode), start_ms=BASE,
                              end_ms=BASE + 2 * HOUR, scenario=ResearchScenario(name="test"))
    assert replay["trades"][0]["exit_reason"] == reason
    assert replay["net_pnl"] is not None
    assert replay["slots"] == 1
    assert all(a["exit_ms"] < b["entry_ms"] for a, b in zip(replay["trades"], replay["trades"][1:]))
    assert replay["trades"][0]["censored"] == (reason == "window_end")


def test_max_hold_is_168_hours_and_quantity_uses_decision_price():
    h = ScriptedHistory()
    replay = run_proxy_replay(h, start_ms=BASE, end_ms=BASE + 169 * HOUR,
                              scenario=ResearchScenario(name="test"))
    first = replay["trades"][0]
    assert first["exit_reason"] == "max_hold"
    assert 168 <= first["hold_hours"] <= 168 + 2 / 60
    h.execution = lambda short, long, now: ((now // MINUTE + 1) * MINUTE, 110, 100)
    first = run_proxy_replay(h, start_ms=BASE, end_ms=BASE + HOUR,
                             scenario=ResearchScenario(name="test"))["trades"][0]
    assert first["quantity_a"] == .1
    assert first["entry_a"] == 110


def test_entry_cannot_fill_at_or_after_target_settlement():
    h = ScriptedHistory()
    result = run_proxy_replay(h, start_ms=BASE + 59 * MINUTE,
                              end_ms=BASE + HOUR, scenario=ResearchScenario(name="test"))
    assert result["n_trades"] == 0


def test_holding_does_not_repeat_exit_cost_and_end_price_gaps_remain_unknown():
    h = ScriptedHistory()
    base_value = h.value
    h.value = lambda a, b, now, **kwargs: {**base_value(a, b, now, **kwargs),
                                           "gross": .001 if now > BASE + MINUTE else .1}
    scenario = ResearchScenario(name="test", min_ev_fraction=0)
    result = run_proxy_replay(h, start_ms=BASE, end_ms=BASE + 2 * HOUR, scenario=scenario)
    assert result["trades"][0]["exit_reason"] == "window_end"
    h = ScriptedHistory()
    execute = h.execution
    h.execution = lambda a, b, now: None if now >= BASE + MINUTE else execute(a, b, now)
    result = run_proxy_replay(h, start_ms=BASE, end_ms=BASE + HOUR, scenario=scenario)
    assert result["net_pnl"] is None
    assert result["unclosed_positions"] == 1


@pytest.mark.parametrize("days", [7, 30])
def test_short_history_never_reports_requested_window_zero_profit(days):
    result = window_admission(history(), BASE + DAY, days)
    assert result["status"] == "not_verified"
    assert result["net_pnl"] is None
    assert "prediction_history_starts_after_requested_window" in result["reasons"]


@pytest.mark.parametrize("kwargs", [{"notional": 0}, {"fee_bps": -1},
                                     {"fee_bps": float("nan")}, {"lead_minutes": 1.5}])
def test_invalid_scenario_is_rejected(kwargs):
    with pytest.raises(ValueError):
        ResearchScenario(name="bad", **kwargs)


def test_frozen_inputs_contract_and_report_overwrite_protection(tmp_path):
    source = tmp_path / "input.json"
    source.write_text('{"contract_version":"wrong"}', encoding="utf-8")
    with pytest.raises(ValueError):
        read_input(source, "expected")
    (tmp_path / "report.json").write_text("frozen", encoding="utf-8")
    with pytest.raises(SystemExit):
        main(["--node-input", str(source), "--final-labels", str(source),
              "--end", "2026-10-23T00:00:00Z", "--output-dir", str(tmp_path)])
    assert (tmp_path / "report.json").read_text() == "frozen"


def test_gate_seconds_normalization_is_bounded():
    assert event_minute(BASE + 4000) == BASE
    with pytest.raises(ValueError):
        event_minute(BASE + 6000)


def test_generated_report_preserves_unknown_windows_and_configured_costs():
    node = {"predictions": [quote(), quote(LONG, rate=.0002)], "settlements": [],
            "end_ms": BASE + 2 * HOUR}
    report = generate_report(node, {"data": []}, prices=price_input(),
                             end_ms=node["end_ms"], provenance=[], fee_bps=3., slippages=(0.,))
    assert all(row["net_pnl"] is None for row in report["requested_windows"])
    assert len(report["replays"]) == 8
    assert "每腿每次成交 3 bp" in markdown_report(report)
    assert len(report["code_sha256"]) == 6
    with pytest.raises(ValueError):
        generate_report(node, {"data": []}, prices=None, end_ms=node["end_ms"] + MINUTE,
                        provenance=[])


def test_collector_only_retries_network_errors(monkeypatch):
    path = Path(__file__).parents[1] / "scripts" / "collect_funding_prices_research.py"
    spec = importlib.util.spec_from_file_location("funding_prices_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)
    calls = []

    def intermittent():
        calls.append(1)
        if len(calls) < 3:
            raise TimeoutError()
        return "ok"

    assert module.retry_network(intermittent, TimeoutError) == "ok"
    assert len(calls) == 3
    with pytest.raises(ValueError):
        module.retry_network(lambda: (_ for _ in ()).throw(ValueError()), TimeoutError)


@pytest.mark.parametrize("following,batch", [(0, [1]), (5000, [])])
def test_data_export_rejects_broken_pagination(tmp_path, following, batch):
    path = Path(__file__).parents[1] / "scripts" / "collect_funding_research.py"
    spec = importlib.util.spec_from_file_location("funding_export_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Client:
        def request(self, *args):
            return {"contract_version": "pxydata.agent-query.v1", "data": batch,
                    "next_offset": following}

    with pytest.raises(ValueError):
        module.collect(Client(), dataset="funding_predictions", symbols=[],
                       start="start", end="end", output=tmp_path / "input.json")
    assert not (tmp_path / "input.json").exists()
