"""Verified, streaming reader for immutable PXYDATA MT5 Tick snapshots."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from pyarrow import parquet

SNAPSHOT_CONTRACT = "pxydata.backtest-data-snapshot.v1"
REQUIRED_COLUMNS = {"event_id", "event_type", "symbol", "time_msc", "bid", "ask", "flags"}


class Mt5TickSnapshotError(ValueError):
    pass


def read_mt5_snapshot(path: str | Path, *, expected_sha256: str) -> dict[str, Any]:
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256.lower():
        raise Mt5TickSnapshotError("MT5 Tick 快照 manifest_sha256 不一致")
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or manifest.get("contract_version") != SNAPSHOT_CONTRACT:
        raise Mt5TickSnapshotError("MT5 Tick 快照契约不匹配")
    return manifest


def _verified_file(root: Path, record: Mapping[str, Any]) -> Path:
    path = (root / str(record.get("path") or "")).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise Mt5TickSnapshotError("Tick 文件越出数据根目录") from exc
    if not path.is_file():
        raise Mt5TickSnapshotError(f"Tick 文件不存在: {record.get('path')}")
    if path.stat().st_size != int(record.get("size_bytes") or -1):
        raise Mt5TickSnapshotError(f"Tick 文件大小不一致: {record.get('path')}")
    expected = str(record.get("sha256") or "")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise Mt5TickSnapshotError(f"Tick 文件 SHA256 不一致: {record.get('path')}")
    return path


def iter_mt5_ticks(
    *,
    data_root: str | Path,
    manifest: Mapping[str, Any],
    symbol: str,
    start_ms: int,
    end_ms: int,
    batch_size: int = 65_536,
) -> Iterator[dict[str, Any]]:
    """Yield every real Tick in stored order; no bar interpolation or UI frame skipping."""
    if manifest.get("contract_version") != SNAPSHOT_CONTRACT:
        raise Mt5TickSnapshotError("MT5 Tick 快照契约不匹配")
    if not bool((manifest.get("quality") or {}).get("accepted")):
        raise Mt5TickSnapshotError("MT5 Tick 快照未通过质量认证")
    if start_ms > end_ms or batch_size < 1:
        raise Mt5TickSnapshotError("Tick 时间范围或批大小无效")
    dataset = next(
        (item for item in manifest.get("datasets") or [] if item.get("name") == "mt5_ticks"),
        None,
    )
    if not isinstance(dataset, dict) or not dataset.get("files"):
        raise Mt5TickSnapshotError("执行快照缺少 mt5_ticks 文件")
    selected = str(symbol).strip().upper()
    allowed = {str(item).upper() for item in (manifest.get("selection") or {}).get("symbols") or []}
    if allowed and selected not in allowed:
        raise Mt5TickSnapshotError("品种不在 MT5 Tick 快照选择范围内")
    root = Path(data_root).resolve()
    previous_ms = -1
    yielded = 0
    for record in dataset["files"]:
        if not isinstance(record, dict):
            raise Mt5TickSnapshotError("Tick 文件清单格式无效")
        path = _verified_file(root, record)
        source = parquet.ParquetFile(path)
        if REQUIRED_COLUMNS - set(source.schema_arrow.names):
            raise Mt5TickSnapshotError(f"Tick 文件缺少必要字段: {record.get('path')}")
        # ParquetFile avoids Hive directory columns colliding with stored market/venue columns.
        for batch in source.iter_batches(batch_size=batch_size, columns=sorted(REQUIRED_COLUMNS)):
            for row in batch.to_pylist():
                if row["event_type"] not in {"quote", "tick"} or str(row["symbol"]).upper() != selected:
                    continue
                time_ms = int(row["time_msc"])
                if time_ms < previous_ms:
                    raise Mt5TickSnapshotError("Tick 时间顺序倒退")
                previous_ms = time_ms
                if not start_ms <= time_ms <= end_ms:
                    continue
                bid, ask = float(row["bid"]), float(row["ask"])
                if bid <= 0 or ask < bid:
                    raise Mt5TickSnapshotError("Tick Bid/Ask 无效")
                yielded += 1
                yield row
    if yielded == 0:
        raise Mt5TickSnapshotError("快照范围内没有真实 MT5 Tick")
