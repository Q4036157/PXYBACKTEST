from __future__ import annotations

import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.mt5_tick_loader import Mt5TickSnapshotError, iter_mt5_ticks, read_mt5_snapshot


def _snapshot(tmp_path):
    path = tmp_path / "normalized" / "mt5_ticks" / "market=fx_cfd" / "venue=exness" / "ticks.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table({
            "event_id": ["1", "2"],
            "event_type": ["quote", "quote"],
            "symbol": ["XAUUSDm", "XAUUSDm"],
            "market": ["fx_cfd", "fx_cfd"],
            "time_msc": [1000, 1000],
            "bid": [4425.73, 4425.74],
            "ask": [4425.99, 4426.00],
            "flags": [134, 134],
        }),
        path,
    )
    return {
        "contract_version": "pxydata.backtest-data-snapshot.v1",
        "quality": {"accepted": True},
        "selection": {"symbols": ["XAUUSDM"]},
        "datasets": [{"name": "mt5_ticks", "files": [{
            "path": path.relative_to(tmp_path).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }]}],
    }, path


def test_real_tick_reader_keeps_duplicate_millisecond_order_without_hive_collision(tmp_path):
    manifest, _ = _snapshot(tmp_path)

    rows = list(iter_mt5_ticks(
        data_root=tmp_path, manifest=manifest, symbol="XAUUSDm",
        start_ms=1000, end_ms=1000, batch_size=1,
    ))

    assert [row["event_id"] for row in rows] == ["1", "2"]
    assert [row["event_type"] for row in rows] == ["quote", "quote"]
    assert [row["ask"] for row in rows] == [4425.99, 4426.00]


def test_real_tick_reader_rejects_changed_file(tmp_path):
    manifest, path = _snapshot(tmp_path)
    path.write_bytes(path.read_bytes() + b"tampered")

    with pytest.raises(Mt5TickSnapshotError, match="大小不一致"):
        list(iter_mt5_ticks(
            data_root=tmp_path, manifest=manifest, symbol="XAUUSDm",
            start_ms=1000, end_ms=1000,
        ))


def test_snapshot_manifest_identity_is_required(tmp_path):
    path = tmp_path / "snapshot.json"
    path.write_text('{"contract_version":"pxydata.backtest-data-snapshot.v1"}', encoding="utf-8")

    with pytest.raises(Mt5TickSnapshotError, match="manifest_sha256"):
        read_mt5_snapshot(path, expected_sha256="0" * 64)
