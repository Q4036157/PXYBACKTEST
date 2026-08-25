from decimal import Decimal

import pytest

from app.kernel import EventLog, PortfolioLedger, replay_fills
from app.snapshot_verifier import SnapshotManifestError, validate_snapshot_manifest


def test_ledger_golden_buy_mark_and_sell() -> None:
    ledger = PortfolioLedger(initial_cash=2000, money_precision=2)
    ledger.apply_fill(
        event_seq=1,
        symbol="600000.SH",
        side="buy",
        quantity=10,
        price=100,
        commission=1,
    )
    ledger.mark("600000.SH", 110)
    snapshot = ledger.snapshot()
    assert snapshot.cash == Decimal("999.00")
    assert snapshot.equity == Decimal("2099.00")
    assert snapshot.unrealized_pnl == Decimal("100.00")
    assert snapshot.positions["600000.SH"]["quantity"] == "10.00000000"

    ledger.apply_fill(
        event_seq=2,
        symbol="600000.SH",
        side="sell",
        quantity=5,
        price=110,
        commission=1,
        stamp_tax=1,
    )
    assert ledger.snapshot().realized_pnl == Decimal("48.00")


def test_ledger_rejects_t_plus_one_sell() -> None:
    ledger = PortfolioLedger(initial_cash=2000, t_plus_one=True)
    ledger.apply_fill(
        event_seq=1,
        symbol="600000.SH",
        side="buy",
        quantity=1,
        price=100,
    )
    with pytest.raises(ValueError, match="T\+1"):
        ledger.apply_fill(
            event_seq=2,
            symbol="600000.SH",
            side="sell",
            quantity=1,
            price=101,
        )


def test_event_log_sequence_and_fingerprint_are_deterministic() -> None:
    def build() -> EventLog:
        log = EventLog(run_id="run-1", engine_id="daa.a_share", snapshot_id="snap-1")
        log.append("MarketEvent", event_time="2026-01-01T09:30:00+08:00", symbol="600000.SH", payload={"close": 100})
        log.append("FillEvent", event_time="2026-01-01T09:31:00+08:00", symbol="600000.SH", payload={"side": "buy", "quantity": 1, "price": 100})
        return log

    first, second = build(), build()
    assert [event.seq for event in first.events] == [1, 2]
    assert first.fingerprint() == second.fingerprint()
    assert first.events[0].event_id == second.events[0].event_id


def test_snapshot_manifest_rejects_path_escape_and_dataset_mismatch() -> None:
    base = {
        "snapshot_id": "btsnap_v1_" + "a" * 32,
        "manifest_sha256": "b" * 64,
        "datasets": [
            {
                "name": "kline_daily",
                "files": [{"path": "normalized/kline/part.parquet", "sha256": "c" * 64, "size_bytes": 10}],
            }
        ],
    }
    assert validate_snapshot_manifest(
        base,
        snapshot_id=base["snapshot_id"],
        manifest_sha256=base["manifest_sha256"],
        expected_datasets={"kline_daily"},
    )["datasets"][0]["files"][0]["path"] == "normalized/kline/part.parquet"
    escaped = {**base, "datasets": [{**base["datasets"][0], "files": [{**base["datasets"][0]["files"][0], "path": "../secret"}]}]}
    with pytest.raises(SnapshotManifestError):
        validate_snapshot_manifest(escaped, snapshot_id=base["snapshot_id"], manifest_sha256=base["manifest_sha256"], expected_datasets={"kline_daily"})


def test_replay_fills_is_stable() -> None:
    from app.kernel import EventRecord

    events = [
        EventRecord(run_id="run", seq=1, event_type="FillEvent", event_time="2026-01-01T00:00:00Z", decision_time=None, symbol="BTC", engine_id="test", engine_version="v1", snapshot_id="snap", payload={"side": "buy", "quantity": 1, "price": 100}),
    ]
    assert replay_fills(initial_cash=1000, events=events).cash == Decimal("900.00")
