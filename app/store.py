from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .kernel import stable_hash


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
EVENT_RETENTION = 5000
EVENT_PRUNE_INTERVAL = 500


class TaskNotFoundError(LookupError):
    pass


class TaskStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._last_pruned_seq: dict[str, int] = {}
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout = 30000")
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    source_node TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    result_path TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_tasks_user_created
                    ON tasks(user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS ix_tasks_status_created
                    ON tasks(status, created_at ASC);
                CREATE TABLE IF NOT EXISTS task_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );
                CREATE INDEX IF NOT EXISTS ix_task_events_task_seq
                    ON task_events(task_id, seq);
                """
            )

    def create_task(
        self,
        *,
        user_id: str,
        source_node: str,
        request: dict[str, Any],
    ) -> str:
        task_id = str(uuid.uuid4())
        now = time.time()
        task_contract = request.get("_task_contract") or {}
        execution = task_contract.get("execution") or {}
        state = {
            "task_id": task_id,
            "schema_version": int(task_contract.get("schema_version") or 1),
            "engine_type": task_contract.get("engine_type", "vnpy_cta"),
            "data_snapshot": task_contract.get("data", {}).get("snapshot"),
            "status": "pending",
            "progress": 0.0,
            "processed_bars": 0,
            "total_bars": 0,
            "speed": request.get("speed", execution.get("speed", 50)),
            "execution_mode": request.get(
                "execution_mode", execution.get("execution_mode", "visual")
            ),
            "result_available": False,
            "replay": {},
            "live_bars": [],
            "live_trades": [],
            "live_orders": [],
            "live_positions": [],
            "strategy_lines": [],
            "recent_replay_events": [],
            "event_seq": 0,
            "last_bar_replay_seq": 0,
            "events_pruned_through": 0,
        }
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO tasks(
                    task_id, user_id, source_node, status, request_json,
                    state_json, created_at, updated_at
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (
                    task_id,
                    user_id,
                    source_node,
                    json.dumps(request, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                    now,
                    now,
                ),
            )
        return task_id

    def count_queued_for_user(self, user_id: str) -> int:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM tasks WHERE user_id = ? AND status = 'pending'",
                (user_id,),
            ).fetchone()
        return int(row["count"] if row else 0)

    def recover_interrupted_tasks(self) -> int:
        now = time.time()
        with self._lock, self._connection() as connection:
            changed = connection.execute(
                """
                UPDATE tasks
                SET status = 'pending', error = '', updated_at = ?
                WHERE status IN ('running', 'paused')
                """,
                (now,),
            )
        return int(changed.rowcount)

    def pending_tasks(self) -> list[dict[str, Any]]:
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT task_id, user_id, request_json FROM tasks WHERE status = 'pending' ORDER BY created_at ASC"
            ).fetchall()
        return [
            {
                "task_id": str(row["task_id"]),
                "user_id": str(row["user_id"]),
                "request": json.loads(row["request_json"]),
            }
            for row in rows
        ]

    def mark_running(self, task_id: str) -> bool:
        now = time.time()
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT state_json FROM tasks WHERE task_id = ? AND status = 'pending'",
                (task_id,),
            ).fetchone()
            if row is None:
                return False
            state = json.loads(row["state_json"])
            state["status"] = "running"
            changed = connection.execute(
                """
                UPDATE tasks SET status = 'running', state_json = ?, updated_at = ?
                WHERE task_id = ? AND status = 'pending'
                """,
                (
                    json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                    now,
                    task_id,
                ),
            )
        return changed.rowcount == 1

    def set_status(self, task_id: str, status: str, error: str = "") -> None:
        now = time.time()
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT state_json FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(task_id)
            state = json.loads(row["state_json"])
            state["status"] = status
            if error:
                state["error"] = error
            connection.execute(
                """
                UPDATE tasks SET status = ?, error = ?, state_json = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (
                    status,
                    error,
                    json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                    now,
                    task_id,
                ),
            )

    def append_event(
        self, task_id: str, event_type: str, payload: dict[str, Any]
    ) -> int:
        return self.append_events(task_id, [(event_type, payload)])[0]

    def append_events(
        self,
        task_id: str,
        events: list[tuple[str, dict[str, Any]]],
    ) -> list[int]:
        if not events:
            return []

        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT user_id, state_json FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(task_id)
            user_id = str(row["user_id"])
            state = json.loads(row["state_json"])
            result_path = ""
            error = ""
            sequences: list[int] = []

            for event_type, payload in events:
                cursor = connection.execute(
                    """
                    INSERT INTO task_events(task_id, user_id, event_type, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        user_id,
                        event_type,
                        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                        time.time(),
                    ),
                )
                seq = int(cursor.lastrowid)
                sequences.append(seq)
                state = self._apply_event(state, event_type, payload)
                state["event_seq"] = seq
                if event_type in {"completed", "cancelled"}:
                    result_path = str(payload.get("result_path") or "")
                elif event_type == "failed":
                    error = str(payload.get("error") or "")

            last_seq = sequences[-1]
            last_pruned_seq = self._last_pruned_seq.get(task_id, 0)
            if last_seq - last_pruned_seq >= EVENT_PRUNE_INTERVAL:
                cutoff = connection.execute(
                    """
                    SELECT seq FROM task_events
                    WHERE task_id = ?
                    ORDER BY seq DESC LIMIT 1 OFFSET ?
                    """,
                    (task_id, EVENT_RETENTION - 1),
                ).fetchone()
                if cutoff is not None:
                    cutoff_seq = int(cutoff["seq"])
                    deleted = connection.execute(
                        """
                        SELECT MAX(seq) AS seq FROM task_events
                        WHERE task_id = ? AND seq < ?
                        """,
                        (task_id, cutoff_seq),
                    ).fetchone()
                    deleted_through = int(deleted["seq"] or 0) if deleted else 0
                    if deleted_through:
                        connection.execute(
                            "DELETE FROM task_events WHERE task_id = ? AND seq < ?",
                            (task_id, cutoff_seq),
                        )
                        state["events_pruned_through"] = max(
                            int(state.get("events_pruned_through") or 0),
                            deleted_through,
                        )
                self._last_pruned_seq[task_id] = last_seq

            status = str(state.get("status") or "running")
            now = time.time()
            connection.execute(
                """
                UPDATE tasks
                SET status = ?, state_json = ?,
                    result_path = CASE WHEN ? <> '' THEN ? ELSE result_path END,
                    error = CASE WHEN ? <> '' THEN ? ELSE error END,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    status,
                    json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                    result_path,
                    result_path,
                    error,
                    error,
                    now,
                    task_id,
                ),
            )
        return sequences

    @staticmethod
    def _trade_key(trade: dict[str, Any]) -> str:
        trade_id = str(trade.get("trade_id") or "").strip()
        if trade_id:
            return trade_id
        return "|".join(
            str(trade.get(key) or "")
            for key in ("datetime", "direction", "offset", "price", "volume")
        )

    @classmethod
    def _merge_trades(
        cls, existing: list[dict[str, Any]], incoming: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        merged = list(existing)
        indexes = {cls._trade_key(item): index for index, item in enumerate(merged)}
        for trade in incoming:
            if not isinstance(trade, dict):
                continue
            key = cls._trade_key(trade)
            if key in indexes:
                merged[indexes[key]] = trade
            else:
                indexes[key] = len(merged)
                merged.append(trade)
        return merged[-5000:]

    @staticmethod
    def _apply_event(
        state: dict[str, Any], event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if event_type == "state":
            state.update(payload)
            state["status"] = payload.get("status", state.get("status", "running"))
        elif event_type == "bar":
            bar = payload.get("bar")
            if isinstance(bar, dict) and bar.get("datetime"):
                bars = list(state.get("live_bars") or [])
                if bars and bars[-1].get("datetime") == bar.get("datetime"):
                    bars[-1] = bar
                else:
                    bars.append(bar)
                # 执行快照是事件裁剪后的恢复权威，必须保留回测起点以来的完整 K 线。
                state["live_bars"] = bars
                state["current_bar"] = bar
                state["last_bar_replay_seq"] = max(
                    int(state.get("last_bar_replay_seq") or 0),
                    int(payload.get("replay_seq") or 0),
                )
        elif event_type == "trade":
            trade = payload.get("trade")
            if isinstance(trade, dict):
                state["live_trades"] = TaskStore._merge_trades(
                    list(state.get("live_trades") or []), [trade]
                )
        elif event_type.endswith("_snapshot"):
            key = event_type.removesuffix("_snapshot")
            mapping = {
                "trades": "live_trades",
                "orders": "live_orders",
                "positions": "live_positions",
                "strategy_lines": "strategy_lines",
                "replay_events": "recent_replay_events",
            }
            if key in mapping:
                items = payload.get("items", [])
                if key == "trades":
                    state["live_trades"] = TaskStore._merge_trades(
                        list(state.get("live_trades") or []),
                        list(items) if isinstance(items, list) else [],
                    )
                else:
                    state[mapping[key]] = items
        elif event_type == "completed":
            state["status"] = "completed"
            state["progress"] = 100.0
            state["result_available"] = bool(payload.get("result_path"))
        elif event_type == "failed":
            state["status"] = "failed"
            state["error"] = str(payload.get("error") or "backtest worker failed")
        elif event_type == "cancelled":
            state["status"] = "cancelled"
            state["result_available"] = bool(
                payload.get("result_available") or payload.get("result_path")
            )
            if payload.get("progress") is not None:
                state["progress"] = float(payload["progress"])
        return state

    def get_task(self, user_id: str, task_id: str) -> dict[str, Any]:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ? AND user_id = ?",
                (task_id, user_id),
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        state = json.loads(row["state_json"])
        state.update(
            {
                "task_id": task_id,
                "status": str(row["status"]),
                "created_at": float(row["created_at"]),
                "updated_at": float(row["updated_at"]),
            }
        )
        return state

    def get_task_status(self, user_id: str, task_id: str) -> dict[str, str]:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT status, error FROM tasks WHERE task_id = ? AND user_id = ?",
                (task_id, user_id),
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return {"status": str(row["status"]), "error": str(row["error"] or "")}

    def get_result_path(self, task_id: str) -> Path:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT result_path FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return Path(str(row["result_path"] or ""))

    def get_request(self, task_id: str) -> dict[str, Any]:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT request_json FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return json.loads(row["request_json"])

    def list_tasks(self, user_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                """
                SELECT task_id, status, request_json, state_json, created_at, updated_at
                FROM tasks WHERE user_id = ? ORDER BY created_at DESC LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        tasks: list[dict[str, Any]] = []
        for row in rows:
            request = json.loads(row["request_json"])
            state = json.loads(row["state_json"])
            task_contract = request.get("_task_contract") or {}
            strategy = task_contract.get("strategy") or {}
            universe = task_contract.get("universe") or {}
            period = task_contract.get("period") or {}
            tasks.append(
                {
                    "task_id": str(row["task_id"]),
                    "schema_version": int(task_contract.get("schema_version") or 1),
                    "engine_type": task_contract.get("engine_type", "vnpy_cta"),
                    "data_snapshot": task_contract.get("data", {}).get("snapshot"),
                    "strategy_class": request.get("strategy_class")
                    or strategy.get("entrypoint", ""),
                    "vt_symbol": request.get("vt_symbol")
                    or ",".join(universe.get("symbols") or []),
                    "interval": request.get("interval") or period.get("interval", ""),
                    "start_time": request.get("start_time") or period.get("start", ""),
                    "end_time": request.get("end_time") or period.get("end", ""),
                    "status": str(row["status"]),
                    "progress": float(state.get("progress") or 0),
                    "result_available": bool(state.get("result_available")),
                    "execution_mode": str(state.get("execution_mode") or "visual"),
                    "created_at": float(row["created_at"]),
                    "updated_at": float(row["updated_at"]),
                }
            )
        return tasks

    def queue_context(self, user_id: str, task_id: str) -> dict[str, Any]:
        with self._lock, self._connection() as connection:
            task = connection.execute(
                "SELECT status, created_at FROM tasks WHERE task_id = ? AND user_id = ?",
                (task_id, user_id),
            ).fetchone()
            if task is None:
                raise TaskNotFoundError(task_id)

            if str(task["status"]) != "pending":
                return {"queue_ahead": 0, "queue_position": 0, "active_task": None}

            queue_ahead_row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM tasks
                WHERE status IN ('running', 'paused')
                   OR (status = 'pending' AND created_at < ?)
                """,
                (float(task["created_at"]),),
            ).fetchone()
            active = connection.execute(
                """
                SELECT user_id, status, state_json
                FROM tasks
                WHERE status IN ('running', 'paused')
                ORDER BY created_at ASC LIMIT 1
                """
            ).fetchone()

        active_task = None
        if active is not None:
            state = json.loads(active["state_json"])
            active_task = {
                "status": str(active["status"]),
                "progress": float(state.get("progress") or 0),
                "processed_bars": int(state.get("processed_bars") or 0),
                "total_bars": int(state.get("total_bars") or 0),
                "owned_by_current_user": str(active["user_id"]) == user_id,
            }

        queue_ahead = int(queue_ahead_row["count"] if queue_ahead_row else 0)
        return {
            "queue_ahead": queue_ahead,
            "queue_position": queue_ahead + 1,
            "active_task": active_task,
        }

    @staticmethod
    def _serialize_event_rows(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for row in rows:
            event_type = str(row["event_type"])
            payload = json.loads(row["payload_json"])
            if event_type in {"completed", "cancelled"}:
                payload.pop("result_path", None)
            if event_type == "failed":
                payload.pop("traceback", None)
            events.append(
                {
                    "seq": int(row["seq"]),
                    "type": event_type,
                    "data": payload,
                    "created_at": float(row["created_at"]),
                }
            )
        return events

    @staticmethod
    def _replay_resync_snapshot(state: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "event_seq",
            "last_bar_replay_seq",
            "status",
            "progress",
            "current_datetime",
            "processed_bars",
            "total_bars",
            "speed",
            "replay",
            "live_bars",
            "live_trades",
            "live_orders",
            "live_positions",
            "strategy_lines",
            "recent_replay_events",
            "current_bar",
        )
        return {key: state.get(key) for key in keys}

    def event_page(
        self, user_id: str, task_id: str, after_seq: int, limit: int = 200
    ) -> dict[str, Any]:
        """原子读取事件窗口；历史已裁剪时返回当前执行快照。"""
        with self._lock, self._connection() as connection:
            owned = connection.execute(
                """
                SELECT status, error, state_json FROM tasks
                WHERE task_id = ? AND user_id = ?
                """,
                (task_id, user_id),
            ).fetchone()
            if owned is None:
                raise TaskNotFoundError(task_id)
            state = json.loads(owned["state_json"])
            bounds = connection.execute(
                """
                SELECT MIN(seq) AS earliest_seq, MAX(seq) AS latest_seq
                FROM task_events WHERE task_id = ? AND user_id = ?
                """,
                (task_id, user_id),
            ).fetchone()
            earliest_seq = int(bounds["earliest_seq"] or 0) if bounds else 0
            latest_seq = int(bounds["latest_seq"] or 0) if bounds else 0
            pruned_through = int(state.get("events_pruned_through") or 0)
            history_truncated = pruned_through > after_seq
            rows = connection.execute(
                """
                SELECT seq, event_type, payload_json, created_at
                FROM task_events
                WHERE task_id = ? AND user_id = ? AND seq > ?
                ORDER BY seq ASC LIMIT ?
                """,
                (task_id, user_id, after_seq, limit),
            ).fetchall()

        events = self._serialize_event_rows(rows)
        next_seq = int(events[-1]["seq"]) if events else after_seq
        return {
            "status": str(owned["status"]),
            "error": str(owned["error"] or ""),
            "events": events,
            "next_seq": next_seq,
            "earliest_seq": earliest_seq,
            "latest_seq": latest_seq,
            "history_truncated": history_truncated,
            "resync": self._replay_resync_snapshot(state)
            if history_truncated
            else None,
        }

    def events_after(
        self, user_id: str, task_id: str, after_seq: int, limit: int = 200
    ) -> list[dict[str, Any]]:
        with self._lock, self._connection() as connection:
            owned = connection.execute(
                "SELECT 1 FROM tasks WHERE task_id = ? AND user_id = ?",
                (task_id, user_id),
            ).fetchone()
            if owned is None:
                raise TaskNotFoundError(task_id)
            rows = connection.execute(
                """
                SELECT seq, event_type, payload_json, created_at
                FROM task_events
                WHERE task_id = ? AND user_id = ? AND seq > ?
                ORDER BY seq ASC LIMIT ?
                """,
                (task_id, user_id, after_seq, limit),
            ).fetchall()
        return self._serialize_event_rows(rows)
