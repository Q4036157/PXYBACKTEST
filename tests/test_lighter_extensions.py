from __future__ import annotations

from unittest.mock import patch

import pytest

from app.lighter_microstructure import (
    _footprint_coverage,
    rebuild_order_book,
    run_lighter_backtest,
)


def _run_lighter_rows(
    rows: list[dict[str, object]],
    *,
    task_id: str = "lighter-vector",
    parameters: dict[str, object] | None = None,
) -> dict[str, object]:
    task = {
        "engine_type": "lighter_microstructure",
        "universe": {"symbols": ["LITUSDT_SWAP_LIGHTER"]},
        "period": {
            "start": rows[0]["event_time"],
            "end": rows[-1]["event_time"],
        },
        "execution": {"capital": 1000, "rate": 0, "slippage": 0, "mode": "TICK"},
        "parameters": {
            "entry_threshold": 0.2,
            "exit_threshold": -1,
            "max_hold_ms": 3_600_000,
            "quantity": 1,
            **(parameters or {}),
        },
        "data": {"snapshot": {"snapshot_id": "lighter-vector-snapshot"}},
    }
    with patch(
        "app.lighter_microstructure.load_manifest_rows",
        side_effect=lambda **kwargs: (
            rows if kwargs["dataset_name"] == "lighter_microstructure_factors" else []
        ),
    ):
        return run_lighter_backtest(
            task_id=task_id,
            task=task,
            manifest={"datasets": []},
            data_root=".",
        )


def test_rebuild_order_book_replays_snapshot_updates_and_depth() -> None:
    rows = rebuild_order_book(
        [
            {"symbol": "LITUSDT_SWAP_LIGHTER", "event_time": "2026-01-01T00:00:00Z", "event_type": "snapshot", "nonce": 1, "bids": [[100, 5], [99, 3]], "asks": [[101, 4], [102, 2]]},
            {"symbol": "LITUSDT_SWAP_LIGHTER", "event_time": "2026-01-01T00:00:01Z", "event_type": "update", "nonce": 2, "side": "buy", "price": 100, "size": 7},
        ],
        depth=2,
    )
    assert rows[-1]["bid_volume1"] == 7
    assert rows[-1]["bid_depth"] == 10
    assert rows[-1]["ask_depth"] == 6
    assert rows[-1]["depth_imbalance"] > 0


def test_lighter_backtest_reports_flow_and_funding() -> None:
    task = {
        "engine_type": "lighter_microstructure",
        "universe": {"symbols": ["LITUSDT_SWAP_LIGHTER"]},
        "period": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:00:03Z"},
        "execution": {"capital": 1000, "rate": 0.0001, "slippage": 0.0},
        "parameters": {"entry_threshold": 0.2, "exit_threshold": 0.0, "max_hold_ms": 10000},
        "data": {"snapshot": {"snapshot_id": "snap"}},
    }
    rows = [
        {"symbol": "LITUSDT_SWAP_LIGHTER", "event_time": "2026-01-01T00:00:00Z", "mid_price": 100, "trade_imbalance": 0.8, "funding_rate": 0.001, "buy_qty": 8, "sell_qty": 2},
        {"symbol": "LITUSDT_SWAP_LIGHTER", "event_time": "2026-01-01T00:00:01Z", "mid_price": 101, "trade_imbalance": -0.8, "funding_rate": 0.001, "buy_qty": 1, "sell_qty": 5},
    ]
    with patch("app.lighter_microstructure.load_manifest_rows", side_effect=lambda **kwargs: rows if kwargs["dataset_name"] == "lighter_microstructure_factors" else []):
        result = run_lighter_backtest(task_id="t1", task=task, manifest={"datasets": []}, data_root=".")
    assert result["engine_type"] == "lighter_microstructure"
    assert result["metrics"]["active_buy_qty"] == 9
    assert result["metrics"]["active_sell_qty"] == 7
    assert "funding_pnl" in result["metrics"]
    assert result["replay_audit"]["event_count"] >= len(rows)
    assert len(result["replay_audit"]["chain_sha256"]) == 64


def test_lighter_long_funding_hits_cash_and_open_position_is_marked_each_tick() -> None:
    rows = [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:00Z",
            "mid_price": 100,
            "trade_imbalance": 1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:01Z",
            "mid_price": 101,
            "trade_imbalance": 1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:02Z",
            "mid_price": 110,
            "trade_imbalance": 1,
            "funding_rate": "0.01",
            "_funding_event": True,
        },
    ]

    result = _run_lighter_rows(rows, task_id="long-funding")

    assert result["metrics"]["funding_pnl"] == pytest.approx(-1.1)
    assert result["metrics"]["final_cash"] == pytest.approx(998.9)
    assert result["metrics"]["unrealized_pnl"] == pytest.approx(9)
    assert result["metrics"]["final_equity"] == pytest.approx(1007.9)
    assert [point["value"] for point in result["curves"]["equity"]] == pytest.approx(
        [1000, 1000, 1007.9]
    )
    assert result["fills"][0]["fill_time"] == rows[1]["event_time"]
    assert result["positions"] == [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "side": "long",
            "quantity": 1.0,
            "entry_price": 101.0,
            "mark_price": 110.0,
            "unrealized_pnl": 9.0,
            "funding_pnl": -1.1,
        }
    ]


def test_lighter_short_receives_positive_funding_and_marks_unrealized_profit() -> None:
    rows = [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:00Z",
            "mid_price": 101,
            "trade_imbalance": -1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:01Z",
            "mid_price": 100,
            "trade_imbalance": -1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:02Z",
            "mid_price": 90,
            "trade_imbalance": -1,
            "funding_rate": "0.01",
            "_funding_event": True,
        },
    ]

    result = _run_lighter_rows(rows, task_id="short-funding")

    assert result["metrics"]["funding_pnl"] == pytest.approx(0.9)
    assert result["metrics"]["final_cash"] == pytest.approx(1000.9)
    assert result["metrics"]["unrealized_pnl"] == pytest.approx(10)
    assert result["metrics"]["final_equity"] == pytest.approx(1010.9)
    assert result["positions"][0]["side"] == "short"
    assert result["positions"][0]["quantity"] == -1.0


def test_lighter_round_trip_fills_entry_and_exit_on_following_ticks() -> None:
    rows = [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:00Z",
            "mid_price": 90,
            "trade_imbalance": 1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:01Z",
            "mid_price": 100,
            "trade_imbalance": -1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:02Z",
            "mid_price": 110,
            "trade_imbalance": -1,
        },
    ]

    result = _run_lighter_rows(
        rows,
        task_id="round-trip",
        parameters={"exit_threshold": 0},
    )

    assert result["metrics"]["n_trades"] == 1
    assert result["metrics"]["n_fills"] == 2
    assert result["metrics"]["final_equity"] == pytest.approx(1010)
    assert result["positions"] == []
    assert [fill["position_effect"] for fill in result["fills"]] == ["open", "close"]
    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert [fill["fill_time"] for fill in result["fills"]] == [
        rows[1]["event_time"],
        rows[2]["event_time"],
    ]
    assert result["deals"][0]["entry_price"] == 100.0
    assert result["deals"][0]["exit_price"] == 110.0
    replay_fills = [
        event for event in result["_replay_events"] if event["event_type"] == "fill"
    ]
    assert [event["payload"]["position_effect"] for event in replay_fills] == [
        "open",
        "close",
    ]


def test_lighter_does_not_charge_a_quoted_funding_rate_on_every_tick() -> None:
    rows = [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": f"2026-01-01T00:00:0{index}Z",
            "mid_price": 100,
            "trade_imbalance": 1,
            "funding_rate": "0.01",
        }
        for index in range(3)
    ]

    result = _run_lighter_rows(rows, task_id="quoted-funding")

    assert result["metrics"]["funding_pnl"] == 0
    assert result["metrics"]["final_equity"] == 1000


def test_lighter_uses_official_funding_mark_and_settlement_position_when_late() -> None:
    factor_rows = [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:00Z",
            "mid_price": 100,
            "trade_imbalance": 1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:01Z",
            "mid_price": 101,
            "trade_imbalance": -1,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:02Z",
            "mid_price": 110,
            "trade_imbalance": 0,
        },
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "event_time": "2026-01-01T00:00:03Z",
            "mid_price": 111,
            "trade_imbalance": 0,
        },
    ]
    funding_rows = [
        {
            "symbol": "LITUSDT_SWAP_LIGHTER",
            "settlement_time": "2026-01-01T00:00:01.500000Z",
            "available_at": "2026-01-01T00:00:03Z",
            "funding_rate": "0.01",
            "mark_price": 120,
        }
    ]
    task = {
        "engine_type": "lighter_microstructure",
        "universe": {"symbols": ["LITUSDT_SWAP_LIGHTER"]},
        "period": {
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-01T00:00:03Z",
        },
        "execution": {"capital": 1000, "rate": 0, "slippage": 0},
        "parameters": {
            "entry_threshold": 0.2,
            "exit_threshold": 0,
            "quantity": 1,
        },
        "data": {"snapshot": {"snapshot_id": "funding-late"}},
    }

    with patch(
        "app.lighter_microstructure.load_manifest_rows",
        side_effect=lambda **kwargs: (
            factor_rows
            if kwargs["dataset_name"] == "lighter_microstructure_factors"
            else funding_rows
            if kwargs["dataset_name"] == "lighter_funding_history"
            else []
        ),
    ):
        result = run_lighter_backtest(
            task_id="funding-late",
            task=task,
            manifest={"datasets": []},
            data_root=".",
        )

    assert result["metrics"]["n_trades"] == 1
    assert result["positions"] == []
    assert result["metrics"]["funding_pnl"] == pytest.approx(-1.2)
    assert result["metrics"]["final_cash"] == pytest.approx(1007.8)
    funding_points = [
        point for point in result["curves"]["equity"] if point["funding_pnl"]
    ]
    assert funding_points[0]["date"] == "2026-01-01T00:00:03Z"


def test_footprint_coverage_requires_every_requested_hour() -> None:
    trades = [
        {
            "symbol": "XAU_SWAP_LIGHTER",
            "event_time": "2026-09-04T03:00:01Z",
            "received_at": "2026-09-04T03:00:02Z",
            "price": 4038.9,
            "qty": 0.25,
            "side": "sell",
        },
        {
            "symbol": "XAU_SWAP_LIGHTER",
            "event_time": "2026-09-04T04:59:59Z",
            "received_at": "2026-09-04T05:00:00Z",
            "price": 4039.1,
            "qty": 0.5,
            "side": "buy",
        },
    ]

    coverage, valid = _footprint_coverage(
        trades,
        start="2026-09-04T03:00:00Z",
        end="2026-09-04T04:59:59Z",
    )

    assert coverage["available"] is True
    assert coverage["covered_hours"] == 2
    assert coverage["trade_count"] == 2
    assert valid == trades


def test_footprint_coverage_reports_missing_hour_and_ignores_bad_row() -> None:
    coverage, valid = _footprint_coverage(
        [
            {
                "symbol": "XAU_SWAP_LIGHTER",
                "event_time": "2026-09-04T03:00:01Z",
                "price": 4038.9,
                "qty": 0.25,
                "side": "sell",
            },
            {
                "symbol": "XAU_SWAP_LIGHTER",
                "event_time": "bad-time",
                "price": 4039,
                "qty": 1,
                "side": "buy",
            },
        ],
        start="2026-09-04T03:00:00Z",
        end="2026-09-04T04:59:59Z",
    )

    assert coverage["available"] is False
    assert coverage["invalid_trade_count"] == 1
    assert coverage["missing_hours"] == ["2026-09-04T04:00:00Z"]
    assert len(valid) == 1


def test_footprint_coverage_explains_kline_only_data() -> None:
    coverage, valid = _footprint_coverage(
        [],
        start="2026-09-04T03:00:00Z",
        end="2026-09-04T03:59:59Z",
    )

    assert coverage["available"] is False
    assert coverage["reason"] == "仅有 K 线，无法生成真实足迹"
    assert valid == []
