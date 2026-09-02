"""把天勤原生结果转换为统一 ReplayEvent 和执行快照。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .kernel import stable_hash
from .replay import ReplayEvent, ResultReplayController, canonical_time


_CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _native_time(value: Any, *, fallback: str) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number > 0:
            if number >= 100_000_000_000_000_000:
                number /= 1_000_000_000
            elif number >= 100_000_000_000_000:
                number /= 1_000_000
            elif number >= 100_000_000_000:
                number /= 1_000
            return canonical_time(datetime.fromtimestamp(number, tz=timezone.utc).isoformat())
    text = str(value or "").strip()
    if text:
        if len(text) == 8 and text.isdigit():
            parsed = datetime.combine(
                date(int(text[:4]), int(text[4:6]), int(text[6:8])),
                time(15, 0),
                tzinfo=_CHINA_TZ,
            )
            return canonical_time(parsed.isoformat())
        try:
            return canonical_time(text)
        except ValueError:
            pass
    return canonical_time(fallback)


def build_tqsdk_replay(
    *,
    run_id: str,
    market_events: Sequence[Mapping[str, Any]],
    deals: Sequence[Mapping[str, Any]],
    orders: Any,
    positions: Any,
    account_curve: Sequence[Mapping[str, Any]],
    final_account: Mapping[str, Any] | None,
    fallback_time: str,
) -> dict[str, Any]:
    """保留完整 K 线/Tick 历史，并生成确定性回放审计。"""

    normalized_market = [dict(item) for item in market_events]
    data_manifest_sha256 = stable_hash(normalized_market)
    events: list[ReplayEvent] = []
    source_seq = 0

    for item in normalized_market:
        event_type = str(item.pop("event_type", "market_bar"))
        symbol = str(item.get("symbol") or "") or None
        event_time = _native_time(
            item.get("datetime_ns")
            or item.get("datetime")
            or item.get("timestamp"),
            fallback=fallback_time,
        )
        events.append(
            ReplayEvent(
                event_type=event_type,
                event_time=event_time,
                payload=item,
                snapshot_id=data_manifest_sha256,
                source="tqsdk-native",
                symbol=symbol,
                source_seq=source_seq,
            )
        )
        source_seq += 1

    for deal in deals:
        payload = dict(deal)
        events.append(
            ReplayEvent(
                event_type="fill",
                event_time=_native_time(
                    payload.get("trade_time_ns"), fallback=fallback_time
                ),
                payload=payload,
                snapshot_id=data_manifest_sha256,
                source="tqsdk-native",
                symbol=str(payload.get("symbol") or "") or None,
                source_seq=source_seq,
            )
        )
        source_seq += 1

    order_items = orders.values() if isinstance(orders, Mapping) else orders or []
    for raw_order in order_items:
        if not isinstance(raw_order, Mapping):
            continue
        payload = dict(raw_order)
        exchange = str(payload.get("exchange_id") or "")
        instrument = str(payload.get("instrument_id") or "")
        symbol = str(payload.get("symbol") or f"{exchange}.{instrument}".strip("."))
        payload.setdefault("symbol", symbol)
        events.append(
            ReplayEvent(
                event_type="order",
                event_time=_native_time(
                    payload.get("last_msg_time")
                    or payload.get("insert_date_time")
                    or payload.get("trade_date_time"),
                    fallback=fallback_time,
                ),
                payload=payload,
                snapshot_id=data_manifest_sha256,
                source="tqsdk-native",
                symbol=symbol or None,
                source_seq=source_seq,
            )
        )
        source_seq += 1

    position_items = positions.items() if isinstance(positions, Mapping) else []
    for key, raw_position in position_items:
        if not isinstance(raw_position, Mapping):
            continue
        payload = {"symbol": str(key), **dict(raw_position)}
        events.append(
            ReplayEvent(
                event_type="position",
                event_time=fallback_time,
                payload=payload,
                snapshot_id=data_manifest_sha256,
                source="tqsdk-native",
                symbol=str(key),
                source_seq=source_seq,
            )
        )
        source_seq += 1

    for account in account_curve:
        payload = dict(account)
        events.append(
            ReplayEvent(
                event_type="account",
                event_time=_native_time(
                    payload.get("trading_day"), fallback=fallback_time
                ),
                payload=payload,
                snapshot_id=data_manifest_sha256,
                source="tqsdk-native",
                source_seq=source_seq,
            )
        )
        source_seq += 1
    if not account_curve and final_account:
        events.append(
            ReplayEvent(
                event_type="account",
                event_time=fallback_time,
                payload=dict(final_account),
                snapshot_id=data_manifest_sha256,
                source="tqsdk-native",
                source_seq=source_seq,
            )
        )

    controller = ResultReplayController(
        run_id=run_id,
        snapshot_id=data_manifest_sha256,
        events=events,
        mode="fast",
    )
    replay = controller.run(sleep=lambda _seconds: None)
    return {
        "data_manifest_sha256": data_manifest_sha256,
        "replay_events": [event.to_dict() for event in sorted(events, key=ReplayEvent.sort_key)],
        "execution_snapshot": replay["execution_snapshot"],
        "replay_audit": replay["replay_audit"],
        "visual": {
            "available": bool(normalized_market),
            "market_event_count": len(normalized_market),
            "complete_event_count": len(events),
            "bar_history_count": replay["execution_snapshot"].get(
                "bar_history_count", 0
            ),
            "account_curve_count": replay["execution_snapshot"].get(
                "account_curve_count", 0
            ),
        },
    }


__all__ = ["build_tqsdk_replay"]
