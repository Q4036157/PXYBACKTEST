from __future__ import annotations

from app.lighter_microstructure import rebuild_order_book, run_lighter_backtest


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
    from unittest.mock import patch

    with patch("app.lighter_microstructure.load_manifest_rows", side_effect=lambda **kwargs: rows if kwargs["dataset_name"] == "lighter_microstructure_factors" else []):
        result = run_lighter_backtest(task_id="t1", task=task, manifest={"datasets": []}, data_root=".")
    assert result["engine_type"] == "lighter_microstructure"
    assert result["metrics"]["active_buy_qty"] == 9
    assert result["metrics"]["active_sell_qty"] == 7
    assert "funding_pnl" in result["metrics"]
    assert result["replay_audit"]["event_count"] >= len(rows)
    assert len(result["replay_audit"]["chain_sha256"]) == 64
