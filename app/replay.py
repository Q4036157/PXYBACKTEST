"""统一回测回放内核。

该模块只负责快照绑定的数据馈送、事件排序、模拟时钟和可视化投影，
不读取运行目录，也不执行策略或真实订单。各引擎适配器把 PXYDATA/DAA
的快照记录转换为 :class:`ReplayEvent`，策略执行层消费完整事件，前端只
消费 :class:`ExecutionSnapshot` 投影。
"""

from __future__ import annotations

import copy
import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Callable, Iterable, Iterator, Mapping

from .kernel import stable_hash


REPLAY_CONTRACT_VERSION = "pxybacktest.replay.v1"
REPLAY_CHECKPOINT_CONTRACT_VERSION = "pxybacktest.replay-checkpoint.v1"
FOOTPRINT_CONTRACT_VERSION = "pxybacktest.footprint.v1"
FOOTPRINT_KLINE_ONLY_REASON = "仅有 K 线，无法生成真实足迹"
DEFAULT_RENDER_INTERVAL_MS = 33


class ReplayError(ValueError):
    """回放输入或状态不满足确定性约束。"""


def _parse_time(value: str | int | float) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ReplayError("事件时间不能为空")
    parsed_text = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(parsed_text)
    except ValueError as exc:
        raise ReplayError(f"事件时间不是 ISO-8601: {text}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayError(f"事件时间必须带时区: {text}")
    return parsed.astimezone(timezone.utc)


def canonical_time(value: str | int | float) -> str:
    """返回稳定的 UTC ISO 时间，避免不同输入格式影响排序哈希。"""

    return _parse_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _coerce_time_value(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        seconds = numeric / 1000.0 if abs(numeric) >= 100_000_000_000 else numeric
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError) as exc:
            raise ReplayError(f"数值事件时间无效: {value}") from exc
    text = str(value or "").strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text}T00:00:00Z"
    return text


_EVENT_PRIORITIES = {
    "footprint_coverage": 9,
    "market_tick": 10,
    "market_bar": 10,
    "order_book": 11,
    "trade_print": 12,
    "news": 20,
    "sentiment": 21,
    "fundamental": 22,
    "factor": 23,
    "calendar": 24,
    "signal": 30,
    "order": 40,
    "fill": 50,
}

_EVENT_TYPE_ALIASES = {
    "trade": "trade_print",
    "tick": "market_tick",
    "quote": "market_tick",
    "bar": "market_bar",
    "kline": "market_bar",
    "global_news": "news",
    "news_event": "news",
    "announcement": "fundamental",
    "signal_event": "signal",
    "order_book_event": "order_book",
}


@dataclass(frozen=True)
class ReplayEvent:
    """绑定快照的输入事件。

    ``available_at`` 是策略可见时间。它可以晚于 ``event_time``，例如新闻
    抓取延迟或财报正式披露时间；排序使用 ``ready_time``，从而禁止未来函数。
    """

    event_type: str
    event_time: str
    payload: dict[str, Any]
    snapshot_id: str
    source: str = "PXYDATA"
    symbol: str | None = None
    available_at: str | None = None
    source_seq: int = 0
    priority: int | None = None
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        event_type = str(self.event_type or "").strip().lower()
        event_type = _EVENT_TYPE_ALIASES.get(event_type, event_type)
        if not event_type:
            raise ReplayError("event_type 不能为空")
        if not str(self.snapshot_id or "").strip():
            raise ReplayError("回放事件必须绑定 snapshot_id")
        event_time = canonical_time(self.event_time)
        available_at = canonical_time(self.available_at or event_time)
        source_seq = int(self.source_seq)
        if source_seq < 0:
            raise ReplayError("source_seq 不能为负数")
        priority = (
            int(self.priority)
            if self.priority is not None
            else _EVENT_PRIORITIES.get(event_type, 100)
        )
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "source_seq", source_seq)
        object.__setattr__(self, "priority", priority)
        object.__setattr__(
            self,
            "event_id",
            stable_hash(
                {
                    "contract_version": REPLAY_CONTRACT_VERSION,
                    "event_type": event_type,
                    "event_time": event_time,
                    "available_at": available_at,
                    "snapshot_id": self.snapshot_id,
                    "source": self.source,
                    "symbol": self.symbol,
                    "source_seq": source_seq,
                    "priority": priority,
                    "payload": self.payload,
                }
            ),
        )

    @property
    def ready_time(self) -> str:
        return max(self.event_time, str(self.available_at))

    def sort_key(self) -> tuple[str, str, int, str, int, str]:
        return (
            self.ready_time,
            self.event_time,
            int(self.priority or 100),
            str(self.source),
            self.source_seq,
            self.event_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": REPLAY_CONTRACT_VERSION,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_time": self.event_time,
            "available_at": self.available_at,
            "ready_time": self.ready_time,
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "symbol": self.symbol,
            "source_seq": self.source_seq,
            "priority": self.priority,
            "payload": self.payload,
        }


def event_from_row(
    row: Mapping[str, Any],
    *,
    snapshot_id: str,
    source: str = "PXYDATA",
    source_seq: int = 0,
    default_event_type: str | None = None,
) -> ReplayEvent:
    """把 PXYDATA/DAA 的行记录转换为统一事件。

    允许 ``event_time``/``timestamp``/``datetime`` 和 ``available_at``/
    ``decision_time`` 两组常见字段，适配器不需要复制业务数据。
    """

    event_type = row.get("event_type") or row.get("type") or default_event_type
    event_time = (
        row.get("event_time")
        or row.get("exchange_ts")
        or row.get("timestamp_utc")
        or row.get("timestamp")
        or row.get("datetime")
        or row.get("exchange_ts_ms")
        or row.get("timestamp_ms")
        or row.get("ts_ms")
    )
    available_at = (
        row.get("available_at")
        or row.get("decision_time")
        or row.get("received_at")
        or row.get("received_ts_ms")
        or row.get("tradable_at")
    )
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {
            str(key): value
            for key, value in row.items()
            if key
            not in {
                "event_type",
                "type",
                "event_time",
                "exchange_ts",
                "timestamp_utc",
                "timestamp",
                "datetime",
                "exchange_ts_ms",
                "timestamp_ms",
                "ts_ms",
                "available_at",
                "decision_time",
                "received_at",
                "received_ts_ms",
                "tradable_at",
                "source",
                "symbol",
                "source_seq",
            }
        }
    return ReplayEvent(
        event_type=str(event_type or ""),
        event_time=_coerce_time_value(event_time),
        available_at=_coerce_time_value(available_at) if available_at else None,
        payload=payload,
        snapshot_id=snapshot_id,
        source=str(row.get("source") or source),
        symbol=str(row.get("symbol")) if row.get("symbol") is not None else None,
        source_seq=int(row.get("source_seq") or source_seq),
        priority=int(row["priority"]) if row.get("priority") is not None else None,
    )


class SnapshotDataFeed:
    """快照绑定的、不可变的多数据集事件馈送。

    构造时一次性规范化和排序，之后只读迭代；同一个 snapshot_id 下的
    行记录必然产生相同顺序和 fingerprint，适合执行快照复现。
    """

    def __init__(
        self,
        *,
        snapshot_id: str,
        datasets: Mapping[str, Iterable[ReplayEvent | Mapping[str, Any]]],
        source: str = "PXYDATA",
    ) -> None:
        if not snapshot_id:
            raise ReplayError("SnapshotDataFeed 缺少 snapshot_id")
        normalized: list[ReplayEvent] = []
        for dataset_name, rows in datasets.items():
            name = str(dataset_name or "").strip()
            if not name:
                raise ReplayError("数据集名称不能为空")
            lowered_name = name.lower()
            if "tick" in lowered_name:
                default_event_type = "market_tick"
            elif "trade" in lowered_name:
                default_event_type = "trade_print"
            elif "book" in lowered_name or "depth" in lowered_name:
                default_event_type = "order_book"
            elif "news" in lowered_name:
                default_event_type = "news"
            elif "sentiment" in lowered_name:
                default_event_type = "sentiment"
            elif "factor" in lowered_name:
                default_event_type = "factor"
            elif "fundamental" in lowered_name or "financial" in lowered_name:
                default_event_type = "fundamental"
            elif "signal" in lowered_name:
                default_event_type = "signal"
            else:
                default_event_type = "market_bar"
            for index, row in enumerate(rows, start=1):
                event = row if isinstance(row, ReplayEvent) else event_from_row(
                    row,
                    snapshot_id=snapshot_id,
                    source=source,
                    source_seq=index,
                    default_event_type=default_event_type,
                )
                if event.snapshot_id != snapshot_id:
                    raise ReplayError("事件 snapshot_id 与数据馈送不一致")
                normalized.append(event)
        self.snapshot_id = snapshot_id
        self._events = tuple(sorted(normalized, key=ReplayEvent.sort_key))
        self._fingerprint = stable_hash([event.to_dict() for event in self._events])

    @property
    def events(self) -> tuple[ReplayEvent, ...]:
        return self._events

    @property
    def fingerprint(self) -> str:
        return self._fingerprint

    def __iter__(self) -> Iterator[ReplayEvent]:
        return iter(self._events)

    def __len__(self) -> int:
        return len(self._events)


class ReplayClock:
    """单调模拟时钟。

    ``speed`` 是模拟时间相对墙上时间的倍数；引擎可以完全不 sleep，
    可视化调度器再使用 :meth:`wall_delay` 控制显示节奏。
    """

    def __init__(self, *, start_time: str, speed: float = 1.0) -> None:
        self._current = _parse_time(start_time)
        self._speed = self._validate_speed(speed)
        self._paused = False
        self._cancelled = False
        self._lock = RLock()

    @staticmethod
    def _validate_speed(value: float) -> float:
        speed = float(value)
        if speed <= 0:
            raise ReplayError("ReplayClock speed 必须大于 0")
        return speed

    @property
    def current_time(self) -> str:
        with self._lock:
            return self._current.isoformat(timespec="microseconds").replace("+00:00", "Z")

    @property
    def speed(self) -> float:
        with self._lock:
            return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def set_speed(self, speed: float) -> None:
        with self._lock:
            self._speed = self._validate_speed(speed)

    def pause(self) -> None:
        with self._lock:
            self._paused = True

    def resume(self) -> None:
        with self._lock:
            if self._cancelled:
                raise ReplayError("已取消的 ReplayClock 不能恢复")
            self._paused = False

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            self._paused = True

    def advance_to(self, event_time: str) -> str:
        target = _parse_time(event_time)
        with self._lock:
            if self._cancelled:
                raise ReplayError("ReplayClock 已取消")
            if target < self._current:
                raise ReplayError("ReplayClock 不允许时间倒退")
            self._current = target
            return self.current_time

    def wall_delay(self, next_event_time: str) -> float:
        target = _parse_time(next_event_time)
        with self._lock:
            delta = max(0.0, (target - self._current).total_seconds())
            return delta / self._speed


@dataclass(frozen=True)
class ReplayCheckpoint:
    """可序列化的回放检查点，只覆盖已经生成的 replay feed。"""

    contract_version: str
    run_id: str
    snapshot_id: str
    feed_fingerprint: str
    processed_events: int
    clock: str
    speed: float
    paused: bool
    last_event_id: str | None
    projection_sha256: str
    audit_chain_sha256: str
    audit_event_count: int
    checkpoint_sha256: str

    @staticmethod
    def _field_names() -> set[str]:
        return {
            "contract_version",
            "run_id",
            "snapshot_id",
            "feed_fingerprint",
            "processed_events",
            "clock",
            "speed",
            "paused",
            "last_event_id",
            "projection_sha256",
            "audit_chain_sha256",
            "audit_event_count",
            "checkpoint_sha256",
        }

    @staticmethod
    def _validate_sha256(name: str, value: Any) -> str:
        if not isinstance(value, str) or len(value) != 64 or any(
            char not in "0123456789abcdef" for char in value
        ):
            raise ReplayError(f"检查点 {name} 不是有效 SHA-256")
        return value

    def _unsigned_payload(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "feed_fingerprint": self.feed_fingerprint,
            "processed_events": self.processed_events,
            "clock": self.clock,
            "speed": self.speed,
            "paused": self.paused,
            "last_event_id": self.last_event_id,
            "projection_sha256": self.projection_sha256,
            "audit_chain_sha256": self.audit_chain_sha256,
            "audit_event_count": self.audit_event_count,
        }

    def validate(self) -> None:
        if self.contract_version != REPLAY_CHECKPOINT_CONTRACT_VERSION:
            raise ReplayError("不支持的回放检查点契约版本")
        if (
            not isinstance(self.run_id, str)
            or not self.run_id
            or not isinstance(self.snapshot_id, str)
            or not self.snapshot_id
        ):
            raise ReplayError("回放检查点必须绑定 run_id 和 snapshot_id")
        self._validate_sha256("feed_fingerprint", self.feed_fingerprint)
        self._validate_sha256("projection_sha256", self.projection_sha256)
        self._validate_sha256("audit_chain_sha256", self.audit_chain_sha256)
        self._validate_sha256("checkpoint_sha256", self.checkpoint_sha256)
        if (
            isinstance(self.processed_events, bool)
            or not isinstance(self.processed_events, int)
            or self.processed_events < 0
        ):
            raise ReplayError("检查点 processed_events 必须是非负整数")
        if (
            isinstance(self.audit_event_count, bool)
            or not isinstance(self.audit_event_count, int)
            or self.audit_event_count < 0
        ):
            raise ReplayError("检查点 audit_event_count 必须是非负整数")
        if self.audit_event_count != self.processed_events:
            raise ReplayError("检查点事件数与审计事件数不一致")
        if not isinstance(self.paused, bool):
            raise ReplayError("检查点 paused 必须是布尔值")
        if (
            isinstance(self.speed, bool)
            or not isinstance(self.speed, (int, float))
            or not math.isfinite(float(self.speed))
        ):
            raise ReplayError("检查点 speed 必须是有限数值")
        ReplayClock._validate_speed(self.speed)
        if not isinstance(self.clock, str):
            raise ReplayError("检查点 clock 类型无效")
        if canonical_time(self.clock) != self.clock:
            raise ReplayError("检查点 clock 必须是标准 UTC 时间")
        if self.processed_events == 0 and self.last_event_id is not None:
            raise ReplayError("未处理事件的检查点不能包含 last_event_id")
        if self.processed_events > 0:
            self._validate_sha256("last_event_id", self.last_event_id)
        if stable_hash(self._unsigned_payload()) != self.checkpoint_sha256:
            raise ReplayError("回放检查点校验失败，内容可能已被篡改")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {**self._unsigned_payload(), "checkpoint_sha256": self.checkpoint_sha256}

    @classmethod
    def create(cls, **values: Any) -> "ReplayCheckpoint":
        payload = {
            "contract_version": REPLAY_CHECKPOINT_CONTRACT_VERSION,
            **values,
        }
        checkpoint = cls(
            **payload,
            checkpoint_sha256=stable_hash(payload),
        )
        checkpoint.validate()
        return checkpoint

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ReplayCheckpoint":
        if set(payload) != cls._field_names():
            raise ReplayError("回放检查点字段不完整或包含未知字段")
        processed_events = payload["processed_events"]
        audit_event_count = payload["audit_event_count"]
        speed = payload["speed"]
        if isinstance(processed_events, bool) or not isinstance(
            processed_events, int
        ):
            raise ReplayError("检查点 processed_events 必须是非负整数")
        if isinstance(audit_event_count, bool) or not isinstance(
            audit_event_count, int
        ):
            raise ReplayError("检查点 audit_event_count 必须是非负整数")
        if isinstance(speed, bool) or not isinstance(speed, (int, float)):
            raise ReplayError("检查点 speed 必须是有限数值")
        if not isinstance(payload["paused"], bool):
            raise ReplayError("检查点 paused 必须是布尔值")
        if payload["last_event_id"] is not None and not isinstance(
            payload["last_event_id"], str
        ):
            raise ReplayError("检查点 last_event_id 类型无效")
        string_fields = (
            "contract_version",
            "run_id",
            "snapshot_id",
            "feed_fingerprint",
            "clock",
            "projection_sha256",
            "audit_chain_sha256",
            "checkpoint_sha256",
        )
        if any(not isinstance(payload[name], str) for name in string_fields):
            raise ReplayError("回放检查点字符串字段类型无效")
        checkpoint = cls(
            contract_version=payload["contract_version"],
            run_id=payload["run_id"],
            snapshot_id=payload["snapshot_id"],
            feed_fingerprint=payload["feed_fingerprint"],
            processed_events=processed_events,
            clock=payload["clock"],
            speed=float(speed),
            paused=payload["paused"],
            last_event_id=payload["last_event_id"],
            projection_sha256=payload["projection_sha256"],
            audit_chain_sha256=payload["audit_chain_sha256"],
            audit_event_count=audit_event_count,
            checkpoint_sha256=payload["checkpoint_sha256"],
        )
        checkpoint.validate()
        return checkpoint


class EventCursor:
    """按可得时间输出事件，保证策略不会看到未来数据。"""

    def __init__(self, feed: SnapshotDataFeed) -> None:
        self.feed = feed
        self._index = 0
        self._last_ready_time: str | None = None

    @property
    def exhausted(self) -> bool:
        return self._index >= len(self.feed)

    @property
    def remaining(self) -> int:
        return max(0, len(self.feed) - self._index)

    def peek(self) -> ReplayEvent | None:
        return None if self.exhausted else self.feed.events[self._index]

    def pop_ready(self, clock_time: str, *, limit: int | None = None) -> list[ReplayEvent]:
        current = _parse_time(clock_time)
        events: list[ReplayEvent] = []
        while not self.exhausted:
            event = self.feed.events[self._index]
            if _parse_time(event.ready_time) > current:
                break
            if self._last_ready_time and event.ready_time < self._last_ready_time:
                raise ReplayError("事件可得时间不是单调顺序")
            events.append(event)
            self._index += 1
            self._last_ready_time = event.ready_time
            if limit is not None and len(events) >= limit:
                break
        return events

    def pop_next(self, clock: ReplayClock) -> ReplayEvent | None:
        event = self.peek()
        if event is None:
            return None
        clock.advance_to(event.ready_time)
        return self.pop_ready(clock.current_time, limit=1)[0]

    @staticmethod
    def _normalized_event_type(event_type: str) -> str:
        normalized = str(event_type or "").strip().lower()
        if not normalized:
            raise ReplayError("导航事件类型不能为空")
        return _EVENT_TYPE_ALIASES.get(normalized, normalized)

    def find_previous(self, event_type: str) -> ReplayEvent | None:
        """从当前游标之前只读查找最近的指定类型事件。"""

        target = self._normalized_event_type(event_type)
        for index in range(self._index - 1, -1, -1):
            event = self.feed.events[index]
            if event.event_type == target:
                return event
        return None

    def find_next(self, event_type: str) -> ReplayEvent | None:
        """从当前游标开始只读查找下一个指定类型事件。"""

        target = self._normalized_event_type(event_type)
        for index in range(self._index, len(self.feed)):
            event = self.feed.events[index]
            if event.event_type == target:
                return event
        return None


class ReplaySession:
    """把 Feed、Clock、Cursor、审计和执行投影串成一个可复用会话。"""

    def __init__(
        self,
        *,
        run_id: str,
        feed: SnapshotDataFeed,
        start_time: str | None = None,
        speed: float = 1.0,
    ) -> None:
        if len(feed) == 0 and not start_time:
            raise ReplayError("空数据馈送必须显式指定 start_time")
        initial_time = start_time or feed.events[0].ready_time
        self.run_id = run_id
        self.feed = feed
        self.clock = ReplayClock(start_time=initial_time, speed=speed)
        self.cursor = EventCursor(feed)
        self.snapshot = ExecutionSnapshot(run_id=run_id, snapshot_id=feed.snapshot_id)
        self.audit = ReplayAudit(run_id=run_id, snapshot_id=feed.snapshot_id)
        self.processed_events = 0
        self.last_event: ReplayEvent | None = None

    def step(
        self, handler: Any | None = None, *, allow_paused: bool = False
    ) -> ReplayEvent | None:
        if (self.clock.paused and not allow_paused) or self.clock.cancelled:
            return None
        event = self.cursor.pop_next(self.clock)
        if event is None:
            return None
        self.processed_events += 1
        self.audit.record(event.event_type, event.payload)
        self.snapshot.apply(event, event_seq=self.processed_events)
        self.last_event = event
        if handler is not None:
            handler(event, self.snapshot)
        return event

    def run(self, handler: Any | None = None, *, max_events: int | None = None) -> int:
        processed = 0
        while not self.cursor.exhausted:
            if max_events is not None and processed >= max_events:
                break
            if self.step(handler) is None:
                break
            processed += 1
        return processed

    def execution_snapshot(self, *, include_history: bool = False) -> dict[str, Any]:
        payload = self.snapshot.to_dict(include_history=include_history)
        payload["audit"] = self.audit.to_dict()
        payload["replay"] = {
            "clock": self.clock.current_time,
            "speed": self.clock.speed,
            "remaining_events": self.cursor.remaining,
            "processed_events": self.processed_events,
        }
        return payload

    def find_previous(self, event_type: str) -> ReplayEvent | None:
        return self.cursor.find_previous(event_type)

    def find_next(self, event_type: str) -> ReplayEvent | None:
        return self.cursor.find_next(event_type)

    def checkpoint(self) -> ReplayCheckpoint:
        """保存 feed 内执行状态，不包含外部策略或优化器状态。"""

        if self.clock.cancelled:
            raise ReplayError("已取消的回放会话不能创建检查点")
        return ReplayCheckpoint.create(
            run_id=self.run_id,
            snapshot_id=self.feed.snapshot_id,
            feed_fingerprint=self.feed.fingerprint,
            processed_events=self.processed_events,
            clock=self.clock.current_time,
            speed=self.clock.speed,
            paused=self.clock.paused,
            last_event_id=(self.last_event.event_id if self.last_event else None),
            projection_sha256=stable_hash(
                self.snapshot.to_dict(include_history=True)
            ),
            audit_chain_sha256=self.audit.chain_sha256,
            audit_event_count=self.audit.event_count,
        )

    @classmethod
    def from_checkpoint(
        cls,
        *,
        feed: SnapshotDataFeed,
        checkpoint: ReplayCheckpoint | Mapping[str, Any],
        run_id: str | None = None,
    ) -> "ReplaySession":
        """用相同 feed 前缀重建投影；不恢复外部策略或优化器状态。"""

        restored = (
            checkpoint
            if isinstance(checkpoint, ReplayCheckpoint)
            else ReplayCheckpoint.from_dict(checkpoint)
        )
        restored.validate()
        target_run_id = restored.run_id if run_id is None else run_id
        if restored.run_id != target_run_id:
            raise ReplayError("检查点 run_id 与恢复目标不一致")
        if restored.snapshot_id != feed.snapshot_id:
            raise ReplayError("检查点 snapshot_id 与数据馈送不一致")
        if restored.feed_fingerprint != feed.fingerprint:
            raise ReplayError("检查点 feed fingerprint 与数据馈送不一致")
        if restored.processed_events > len(feed):
            raise ReplayError("检查点 processed_events 超出数据馈送范围")

        initial_time = (
            feed.events[0].ready_time
            if restored.processed_events > 0
            else restored.clock
        )
        session = cls(run_id=target_run_id, feed=feed, start_time=initial_time)
        for _ in range(restored.processed_events):
            if session.step() is None:
                raise ReplayError("无法从数据馈送重建回放检查点")

        projection_sha256 = stable_hash(
            session.snapshot.to_dict(include_history=True)
        )
        last_event_id = session.last_event.event_id if session.last_event else None
        mismatches = {
            "clock": session.clock.current_time != restored.clock,
            "last_event_id": last_event_id != restored.last_event_id,
            "projection_sha256": projection_sha256 != restored.projection_sha256,
            "audit_event_count": session.audit.event_count
            != restored.audit_event_count,
            "audit_chain_sha256": session.audit.chain_sha256
            != restored.audit_chain_sha256,
        }
        invalid_fields = [name for name, mismatch in mismatches.items() if mismatch]
        if invalid_fields:
            raise ReplayError(
                "回放检查点重建校验失败: " + ", ".join(invalid_fields)
            )
        session.clock.set_speed(restored.speed)
        if restored.paused:
            session.clock.pause()
        return session


@dataclass
class ExecutionSnapshot:
    """执行层向 UI 暴露的最新投影，不包含完整事件日志。"""

    run_id: str
    snapshot_id: str
    event_seq: int = 0
    simulated_at: str | None = None
    bars: dict[str, dict[str, Any]] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)
    order_books: dict[str, dict[str, Any]] = field(default_factory=dict)
    news: dict[str, Any] = field(default_factory=dict)
    sentiment: dict[str, Any] = field(default_factory=dict)
    fundamentals: dict[str, Any] = field(default_factory=dict)
    factors: dict[str, Any] = field(default_factory=dict)
    signals: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)
    fills: list[dict[str, Any]] = field(default_factory=list)
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    account: dict[str, Any] = field(default_factory=dict)
    footprint: dict[str, Any] = field(
        default_factory=lambda: {
            "contract": FOOTPRINT_CONTRACT_VERSION,
            "available": False,
            "complete": False,
            "source": "none",
            "interval": "1m",
            "interval_ms": 60_000,
            "reason": FOOTPRINT_KLINE_ONLY_REASON,
        }
    )
    footprint_bars: dict[str, dict[str, Any]] = field(default_factory=dict)
    footprint_trade_count: int = 0
    footprint_invalid_trade_count: int = 0
    queue_lag: int = 0
    bar_history: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=50_000)
    )
    factor_history: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=50_000)
    )
    sentiment_timeline: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=50_000)
    )
    account_curve: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=50_000)
    )
    bar_history_count: int = 0
    factor_history_count: int = 0
    sentiment_timeline_count: int = 0
    account_curve_count: int = 0

    @staticmethod
    def _set_bounded(
        target: dict[str, Any], key: str, value: Any, *, limit: int = 5000
    ) -> None:
        target[key] = value
        while len(target) > limit:
            target.pop(next(iter(target)))

    @staticmethod
    def _append_bounded(
        target: list[dict[str, Any]], value: dict[str, Any], *, limit: int = 50_000
    ) -> None:
        target.append(value)
        if len(target) > limit:
            del target[: len(target) - limit]

    def apply(self, event: ReplayEvent, *, event_seq: int | None = None) -> None:
        self.event_seq = int(event_seq or self.event_seq + 1)
        self.simulated_at = event.ready_time
        payload = dict(event.payload)
        symbol = event.symbol or str(payload.get("symbol") or "")
        if event.event_type == "footprint_coverage":
            complete = bool(payload.get("complete"))
            source = str(payload.get("source") or "none")
            available = bool(payload.get("available")) and complete and source not in {
                "none",
                "bar_pseudo_tick",
                "pseudo_tick",
            }
            interval_ms = max(1_000, int(payload.get("interval_ms") or 60_000))
            self.footprint = {
                **payload,
                "contract": FOOTPRINT_CONTRACT_VERSION,
                "available": available,
                "complete": complete,
                "source": source,
                "interval": str(payload.get("interval") or "1m"),
                "interval_ms": interval_ms,
                "reason": (
                    ""
                    if available
                    else str(payload.get("reason") or FOOTPRINT_KLINE_ONLY_REASON)
                ),
            }
        elif event.event_type in {"market_tick", "market_bar"} and symbol:
            self.bars[symbol] = payload
            if event.event_type == "market_bar":
                self.bar_history.append(payload)
                self.bar_history_count += 1
        elif event.event_type == "trade_print":
            # 逐笔成交不是 K 线：保留原始成交顺序，避免把微观结构数据
            # 错误地覆盖主图 OHLC。前端可以按 symbol/price/volume 绘制成交层。
            self._append_bounded(self.trades, payload)
            self._apply_footprint_trade(event, payload, symbol)
        elif event.event_type == "order_book" and symbol:
            self.order_books[symbol] = payload
        elif event.event_type == "news":
            key = str(payload.get("event_id") or event.event_id)
            self._set_bounded(self.news, key, payload)
            # 兼容旧版前端：带情绪评分的新闻仍可从 sentiment 读取；
            # news 字典保留完整新闻域，二者不是互相覆盖。
            if any(field in payload for field in ("score", "sentiment", "sentiment_score")):
                self._set_bounded(self.sentiment, key, payload)
            self.sentiment_timeline.append(payload)
            self.sentiment_timeline_count += 1
        elif event.event_type == "sentiment":
            key = str(payload.get("event_id") or event.event_id)
            self._set_bounded(self.sentiment, key, payload)
            self.sentiment_timeline.append(payload)
            self.sentiment_timeline_count += 1
        elif event.event_type == "fundamental":
            key = str(payload.get("event_id") or payload.get("report_id") or event.event_id)
            self._set_bounded(self.fundamentals, key, payload)
        elif event.event_type == "factor":
            factor_name = str(
                payload.get("factor_set_id") or payload.get("name") or event.event_id
            )
            key = f"{symbol}:{factor_name}" if symbol else factor_name
            self.factors[key] = payload
            self.factor_history.append(payload)
            self.factor_history_count += 1
        elif event.event_type == "signal":
            self._append_bounded(self.signals, payload)
        elif event.event_type == "order":
            self._append_bounded(self.orders, payload)
        elif event.event_type == "fill":
            self._append_bounded(self.fills, payload)
        elif event.event_type == "position" and symbol:
            self.positions[symbol] = payload
        elif event.event_type == "account":
            self.account = payload
            self.account_curve.append(payload)
            self.account_curve_count += 1

    def _apply_footprint_trade(
        self,
        event: ReplayEvent,
        payload: dict[str, Any],
        symbol: str,
    ) -> None:
        try:
            price = float(payload.get("price"))
            qty = float(
                payload.get("qty")
                if payload.get("qty") is not None
                else payload.get("volume")
            )
        except (TypeError, ValueError):
            self.footprint_invalid_trade_count += 1
            return
        if (
            not symbol
            or price <= 0
            or qty <= 0
            or not math.isfinite(price)
            or not math.isfinite(qty)
        ):
            self.footprint_invalid_trade_count += 1
            return
        side = str(payload.get("side") or "unknown").strip().lower()
        if side not in {"buy", "sell", "unknown"}:
            self.footprint_invalid_trade_count += 1
            return
        interval_ms = max(1_000, int(self.footprint.get("interval_ms") or 60_000))
        event_ms = int(_parse_time(event.event_time).timestamp() * 1000)
        start_ts = event_ms // interval_ms * interval_ms
        interval = str(self.footprint.get("interval") or "1m")
        key = f"{symbol}|{interval}|{start_ts}"
        frame = self.footprint_bars.get(key)
        if frame is None:
            frame = {
                "type": "L1",
                "event": "footprint",
                "symbol": symbol,
                "platform": "backtest",
                "data": {
                    "interval": interval,
                    "startTs": start_ts,
                    "endTs": start_ts + interval_ms,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "buyVol": 0.0,
                    "sellVol": 0.0,
                    "unknownVol": 0.0,
                    "delta": 0.0,
                    "tradeCount": 0,
                    "bins_by_price": {},
                },
            }
            self._set_bounded(self.footprint_bars, key, frame)
        data = frame["data"]
        data["high"] = max(float(data["high"]), price)
        data["low"] = min(float(data["low"]), price)
        data["close"] = price
        volume_key = {
            "buy": "buyVol",
            "sell": "sellVol",
            "unknown": "unknownVol",
        }[side]
        data[volume_key] = float(data[volume_key]) + qty
        data["delta"] = float(data["buyVol"]) - float(data["sellVol"])
        data["tradeCount"] = int(data["tradeCount"]) + 1
        bins = data["bins_by_price"]
        price_key = format(price, ".15g")
        bin_row = bins.setdefault(
            price_key,
            {
                "price": price,
                "buyVol": 0.0,
                "sellVol": 0.0,
                "unknownVol": 0.0,
                "tradeCount": 0,
                "maxTradeQty": 0.0,
                "delta": 0.0,
            },
        )
        bin_row[volume_key] = float(bin_row[volume_key]) + qty
        bin_row["tradeCount"] = int(bin_row["tradeCount"]) + 1
        bin_row["maxTradeQty"] = max(float(bin_row["maxTradeQty"]), qty)
        bin_row["delta"] = float(bin_row["buyVol"]) - float(bin_row["sellVol"])
        self.footprint_trade_count += 1

    def footprint_frame_for(self, event: ReplayEvent) -> dict[str, Any] | None:
        """返回当前逐笔所在足迹桶的公开帧。"""

        if event.event_type != "trade_print" or not self.footprint.get("available"):
            return None
        symbol = event.symbol or str(event.payload.get("symbol") or "")
        interval_ms = max(1_000, int(self.footprint.get("interval_ms") or 60_000))
        event_ms = int(_parse_time(event.event_time).timestamp() * 1000)
        start_ts = event_ms // interval_ms * interval_ms
        key = f"{symbol}|{self.footprint.get('interval') or '1m'}|{start_ts}"
        frame = self.footprint_bars.get(key)
        return self._public_footprint_frame(frame) if frame else None

    @staticmethod
    def _public_footprint_frame(frame: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(frame)
        data = result["data"]
        bins = data.pop("bins_by_price", {})
        data["bins"] = sorted(bins.values(), key=lambda item: float(item["price"]))
        return result

    def to_dict(self, *, include_history: bool = False) -> dict[str, Any]:
        result = {
            "contract_version": REPLAY_CONTRACT_VERSION,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "event_seq": self.event_seq,
            "simulated_at": self.simulated_at,
            "bars": self.bars,
            "trades": self.trades[-5000:],
            "order_books": self.order_books,
            "news": self.news,
            "sentiment": self.sentiment,
            "fundamentals": self.fundamentals,
            "factors": self.factors,
            "signals": self.signals[-5000:],
            "orders": self.orders[-5000:],
            "fills": self.fills[-5000:],
            "positions": self.positions,
            "account": self.account,
            "footprint": {
                **self.footprint,
                "trade_count": self.footprint_trade_count,
                "invalid_trade_count": self.footprint_invalid_trade_count,
            },
            "queue_lag": self.queue_lag,
            "bar_history_count": self.bar_history_count,
            "factor_history_count": self.factor_history_count,
            "sentiment_timeline_count": self.sentiment_timeline_count,
            "account_curve_count": self.account_curve_count,
            "history_truncated": {
                "bars": self.bar_history_count > len(self.bar_history),
                "factors": self.factor_history_count > len(self.factor_history),
                "sentiment": self.sentiment_timeline_count
                > len(self.sentiment_timeline),
                "account": self.account_curve_count > len(self.account_curve),
            },
        }
        if include_history:
            result.update(
                {
                    "bar_history": list(self.bar_history),
                    "factor_history": list(self.factor_history),
                    "sentiment_timeline": list(self.sentiment_timeline),
                    "account_curve": list(self.account_curve),
                    "footprint_bars": (
                        [
                            self._public_footprint_frame(frame)
                            for frame in self.footprint_bars.values()
                        ]
                        if self.footprint.get("available")
                        else []
                    ),
                }
            )
        return result


class VisualProjectionGate:
    """把完整执行事件降采样为固定显示帧，绝不影响执行层事件。"""

    def __init__(self, *, interval_ms: int = DEFAULT_RENDER_INTERVAL_MS) -> None:
        self._lock = RLock()
        self.interval_seconds = 0.0
        self.set_interval_ms(interval_ms)
        self._last_emit: float | None = None
        self._pending: Any | None = None

    def set_interval_ms(self, interval_ms: int) -> None:
        """调整显示帧间隔，不触碰执行事件顺序。"""
        value = int(interval_ms)
        if value < 1:
            raise ReplayError("可视化帧间隔必须大于 0ms")
        with self._lock:
            self.interval_seconds = value / 1000.0

    def offer(self, event: Any, *, now: float | None = None, force: bool = False) -> Any | None:
        current = time.monotonic() if now is None else float(now)
        with self._lock:
            self._pending = event
            if (
                force
                or self._last_emit is None
                or current - self._last_emit >= self.interval_seconds
            ):
                self._last_emit = current
                emitted = self._pending
                self._pending = None
                return emitted
            return None

    def flush(self) -> Any | None:
        with self._lock:
            emitted = self._pending
            self._pending = None
            if emitted is not None:
                self._last_emit = time.monotonic()
            return emitted


class ResultReplayController:
    """统一驱动非 CTA 结果事件的 EventCursor、ReplayClock 和 UI 限帧。

    引擎产生的每个事件都会进入 :class:`ReplaySession` 和审计链；
    :class:`VisualProjectionGate` 只限制执行快照的发送频率，不参与策略执行。
    """

    def __init__(
        self,
        *,
        run_id: str,
        snapshot_id: str,
        events: Iterable[ReplayEvent | Mapping[str, Any]],
        mode: str = "visual",
        speed: float = 1.0,
        render_interval_ms: int = DEFAULT_RENDER_INTERVAL_MS,
        checkpoint: ReplayCheckpoint | Mapping[str, Any] | None = None,
    ) -> None:
        normalized_mode = str(mode or "visual").strip().lower()
        if normalized_mode not in {"visual", "fast"}:
            raise ReplayError(f"未知回放模式: {mode}")
        self.mode = normalized_mode
        self.feed = SnapshotDataFeed(
            snapshot_id=snapshot_id,
            datasets={"result_events": events},
            source="adapter",
        )
        start_time = (
            self.feed.events[0].ready_time if len(self.feed) else "1970-01-01T00:00:00Z"
        )
        self.session = (
            ReplaySession.from_checkpoint(
                run_id=run_id,
                feed=self.feed,
                checkpoint=checkpoint,
            )
            if checkpoint is not None
            else ReplaySession(
                run_id=run_id,
                feed=self.feed,
                start_time=start_time,
                speed=speed,
            )
        )
        if checkpoint is not None and self.session.clock.paused:
            # 服务恢复会把中断任务重新放回 pending；沿用速度，但不保留旧进程暂停锁。
            self.session.clock.resume()
        self.gate = VisualProjectionGate(interval_ms=render_interval_ms)
        self._pending_updates: dict[str, list[dict[str, Any]]] = {
            "bar_updates": [],
            "footprint_updates": [],
            "order_book_updates": [],
            "sentiment_updates": [],
            "factor_updates": [],
            "account_updates": [],
        }
        self._last_projected_count = -1
        self._step_budget = 0

    def _apply_command(
        self,
        command: Mapping[str, Any],
        on_state: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        action = str(command.get("action") or "").strip().lower()
        if action == "pause" and not self.session.clock.cancelled:
            self.session.clock.pause()
            if on_state is not None:
                on_state({"status": "paused", "speed": self.session.clock.speed})
        elif action == "resume" and not self.session.clock.cancelled:
            self.session.clock.resume()
            if on_state is not None:
                on_state({"status": "running", "speed": self.session.clock.speed})
        elif action == "speed" and not self.session.clock.cancelled:
            self.session.clock.set_speed(float(command.get("speed") or 1.0))
            if on_state is not None:
                on_state({"speed": self.session.clock.speed})
        elif action == "step" and not self.session.clock.cancelled:
            if not self.session.clock.paused:
                self.session.clock.pause()
            self._step_budget += 1
            if on_state is not None:
                on_state(
                    {
                        "status": "paused",
                        "speed": self.session.clock.speed,
                        "step_pending": self._step_budget,
                    }
                )
        elif action == "cancel":
            self.session.clock.cancel()

    def _read_commands(
        self,
        reader: Callable[[], Iterable[Mapping[str, Any]] | Mapping[str, Any] | None]
        | None,
        on_state: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        if reader is None:
            return
        commands = reader()
        if isinstance(commands, Mapping):
            commands = [commands]
        for command in commands or []:
            if isinstance(command, Mapping):
                self._apply_command(command, on_state)

    def _record_frame_update(self, event: ReplayEvent) -> None:
        payload = dict(event.payload)
        if event.symbol and not payload.get("symbol"):
            payload["symbol"] = event.symbol
        if event.event_type == "market_bar":
            self._pending_updates["bar_updates"].append(payload)
        elif event.event_type == "trade_print":
            frame = self.session.snapshot.footprint_frame_for(event)
            if frame is not None:
                self._replace_latest_footprint_update(frame)
        elif event.event_type == "order_book":
            self._replace_latest_update("order_book_updates", event, payload)
        elif event.event_type in {"news", "sentiment"}:
            self._pending_updates["sentiment_updates"].append(payload)
        elif event.event_type == "factor":
            self._replace_latest_update("factor_updates", event, payload)
        elif event.event_type == "account":
            self._pending_updates["account_updates"].append(payload)

    def _replace_latest_update(
        self,
        bucket: str,
        event: ReplayEvent,
        payload: dict[str, Any],
    ) -> None:
        """高频状态域在单帧内按品种/名称合并，避免 UI 帧无限膨胀。"""

        key = (
            event.symbol or str(payload.get("symbol") or ""),
            str(payload.get("factor_set_id") or payload.get("name") or ""),
        )
        values = self._pending_updates[bucket]
        for index in range(len(values) - 1, -1, -1):
            item = values[index]
            item_key = (
                str(item.get("symbol") or ""),
                str(item.get("factor_set_id") or item.get("name") or ""),
            )
            if item_key == key:
                values[index] = payload
                return
        values.append(payload)

    def _replace_latest_footprint_update(self, frame: dict[str, Any]) -> None:
        data = dict(frame.get("data") or {})
        key = (str(frame.get("symbol") or ""), int(data.get("startTs") or 0))
        values = self._pending_updates["footprint_updates"]
        for index in range(len(values) - 1, -1, -1):
            current = values[index]
            current_data = dict(current.get("data") or {})
            current_key = (
                str(current.get("symbol") or ""),
                int(current_data.get("startTs") or 0),
            )
            if current_key == key:
                values[index] = frame
                return
        values.append(frame)

    def _snapshot_payload(self, *, include_history: bool = False) -> dict[str, Any]:
        snapshot = self.session.execution_snapshot(include_history=include_history)
        snapshot["status"] = (
            "cancelled"
            if self.session.clock.cancelled
            else "paused"
            if self.session.clock.paused
            else "completed"
            if self.session.cursor.exhausted
            else "running"
        )
        snapshot["mode"] = self.mode
        if not self.session.clock.cancelled:
            snapshot["replay_checkpoint"] = self.session.checkpoint().to_dict()
        return copy.deepcopy(snapshot)

    def _project(
        self,
        on_snapshot: Callable[[dict[str, Any]], None] | None,
        *,
        force: bool = False,
    ) -> None:
        marker = self.gate.offer(self.session.processed_events, force=force)
        if marker is None and not force:
            return
        if on_snapshot is not None:
            snapshot = self._snapshot_payload()
            for key, values in self._pending_updates.items():
                snapshot[key] = copy.deepcopy(values)
            on_snapshot(snapshot)
        for values in self._pending_updates.values():
            values.clear()
        self._last_projected_count = self.session.processed_events

    def _visual_delay(self, next_event: ReplayEvent) -> float:
        if self.mode != "visual":
            return 0.0
        # 日线不能按真实自然日等待；每个不同时间片最多等待 1/speed 秒。
        return min(
            self.session.clock.wall_delay(next_event.ready_time),
            1.0 / self.session.clock.speed,
        )

    def run(
        self,
        *,
        handler: Callable[[ReplayEvent, ExecutionSnapshot], None] | None = None,
        read_commands: Callable[
            [], Iterable[Mapping[str, Any]] | Mapping[str, Any] | None
        ]
        | None = None,
        on_snapshot: Callable[[dict[str, Any]], None] | None = None,
        on_state: Callable[[dict[str, Any]], None] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, Any]:
        while not self.session.cursor.exhausted:
            self._read_commands(read_commands, on_state)
            if self.session.clock.cancelled:
                break
            stepping = self.session.clock.paused and self._step_budget > 0
            if self.session.clock.paused and not stepping:
                sleep(0.01)
                continue

            next_event = self.session.cursor.peek()
            if next_event is None:
                break
            remaining_delay = 0.0 if stepping else self._visual_delay(next_event)
            while remaining_delay > 0:
                interval = min(0.02, remaining_delay)
                sleep(interval)
                remaining_delay -= interval
                self._read_commands(read_commands, on_state)
                if self.session.clock.cancelled or self.session.clock.paused:
                    break
            if self.session.clock.cancelled or (
                self.session.clock.paused and not stepping
            ):
                continue

            event = self.session.step(handler, allow_paused=stepping)
            if event is None:
                continue
            self._record_frame_update(event)
            self._project(on_snapshot)
            if stepping:
                self._step_budget = max(0, self._step_budget - 1)
                if on_state is not None:
                    on_state(
                        {
                            "status": "paused",
                            "speed": self.session.clock.speed,
                            "step_pending": self._step_budget,
                        }
                    )

        if self._last_projected_count != self.session.processed_events:
            self._project(on_snapshot, force=True)
        complete = self.session.cursor.exhausted and not self.session.clock.cancelled
        execution_snapshot = self._snapshot_payload(include_history=True)
        return {
            "complete": complete,
            "termination_reason": "completed" if complete else "cancelled",
            "processed_events": self.session.processed_events,
            "remaining_events": self.session.cursor.remaining,
            "total_events": len(self.feed),
            "execution_snapshot": execution_snapshot,
            "replay_audit": self.session.audit.to_dict(),
        }


class ReplayAudit:
    """完整执行事件的轻量哈希链审计，不把高频 Tick 写入 UI/SQLite。"""

    def __init__(self, *, run_id: str, snapshot_id: str) -> None:
        if not run_id or not snapshot_id:
            raise ReplayError("ReplayAudit 必须绑定 run_id 和 snapshot_id")
        self.run_id = run_id
        self.snapshot_id = snapshot_id
        self.event_count = 0
        self._chain = stable_hash(
            {
                "contract_version": REPLAY_CONTRACT_VERSION,
                "run_id": run_id,
                "snapshot_id": snapshot_id,
            }
        )
        self._lock = RLock()

    def record(self, event_type: str, payload: Mapping[str, Any]) -> int:
        with self._lock:
            self.event_count += 1
            self._chain = stable_hash(
                {
                    "previous": self._chain,
                    "seq": self.event_count,
                    "event_type": str(event_type),
                    "payload_hash": stable_hash(dict(payload)),
                }
            )
            return self.event_count

    @property
    def chain_sha256(self) -> str:
        with self._lock:
            return self._chain

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": REPLAY_CONTRACT_VERSION,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "event_count": self.event_count,
            "chain_sha256": self.chain_sha256,
        }


def build_replay_audit(
    *,
    run_id: str,
    snapshot_id: str,
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """对适配器产生的完整事件序列生成统一审计链。

    ``events`` 的每一项至少包含 ``event_type`` 和 ``payload``，可选
    ``event_time``/``available_at``/``source_seq``。排序使用策略可见的
    ``max(event_time, available_at)``，因此真实 Tick、因子和账户事件可以在
    不保留完整事件日志的情况下共享同一确定性校验规则。
    """
    normalized: list[tuple[ReplayEvent, int]] = []
    for index, item in enumerate(events):
        event_type = str(item.get("event_type") or item.get("type") or "")
        payload = item.get("payload")
        if not isinstance(payload, dict):
            payload = {
                str(key): value
                for key, value in item.items()
                if key not in {"event_type", "type", "event_time", "available_at", "source_seq", "payload"}
            }
        event_time = item.get("event_time") or item.get("timestamp") or item.get("datetime")
        if not event_time:
            # 结果曲线通常只有 date；按 UTC 日界转换，仍然保证稳定顺序。
            event_time = f"{item.get('date') or '1970-01-01'}T00:00:00Z"
        event_value = _audit_time_value(event_time)
        available_value = (
            _audit_time_value(item["available_at"])
            if item.get("available_at")
            else None
        )
        event = ReplayEvent(
            event_type=event_type,
            event_time=_coerce_time_value(event_value),
            available_at=(_coerce_time_value(available_value) if available_value else None),
            payload=payload,
            snapshot_id=snapshot_id,
            source=str(item.get("source") or "adapter"),
            symbol=str(item.get("symbol")) if item.get("symbol") is not None else None,
            source_seq=int(item.get("source_seq") or index),
        )
        normalized.append((event, index))
    normalized.sort(key=lambda pair: (pair[0].sort_key(), pair[1]))
    audit = ReplayAudit(run_id=run_id, snapshot_id=snapshot_id)
    for event, _ in normalized:
        audit.record(event.event_type, event.payload)
    return audit.to_dict()


def _audit_time_value(value: Any) -> Any:
    """把适配器常见的无时区日期补成 UTC 日界。"""
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            return f"{text}T00:00:00Z"
    return value


__all__ = [
    "DEFAULT_RENDER_INTERVAL_MS",
    "EventCursor",
    "ExecutionSnapshot",
    "REPLAY_CHECKPOINT_CONTRACT_VERSION",
    "REPLAY_CONTRACT_VERSION",
    "ReplayClock",
    "ReplayCheckpoint",
    "ReplayError",
    "ReplayEvent",
    "ResultReplayController",
    "ReplaySession",
    "ReplayAudit",
    "build_replay_audit",
    "SnapshotDataFeed",
    "VisualProjectionGate",
    "canonical_time",
    "event_from_row",
]
