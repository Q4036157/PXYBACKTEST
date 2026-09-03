from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .kernel import stable_hash


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
RESULT_RETENTION_SECONDS = 7 * 24 * 60 * 60


class TaskNotFoundError(LookupError):
    pass


class QueueLimitReachedError(ValueError):
    """用户的待执行任务达到上限。"""


class IdempotencyConflictError(ValueError):
    """同一用户重复使用幂等键提交了不同请求。"""


@dataclass(frozen=True)
class TaskCreationReceipt:
    task_id: str
    idempotent_replay: bool
    idempotency_key: str | None


class TaskStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
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
                    task_seq INTEGER,
                    task_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );
                CREATE TABLE IF NOT EXISTS user_drafts (
                    user_id TEXT NOT NULL,
                    draft_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(user_id, draft_id)
                );
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(tasks)").fetchall()
            }
            if "idempotency_key" not in columns:
                connection.execute("ALTER TABLE tasks ADD COLUMN idempotency_key TEXT")
            if "request_sha256" not in columns:
                connection.execute("ALTER TABLE tasks ADD COLUMN request_sha256 TEXT")
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_user_idempotency
                ON tasks(user_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL
                """
            )
            event_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(task_events)"
                ).fetchall()
            }
            if "task_seq" not in event_columns:
                connection.execute(
                    "ALTER TABLE task_events ADD COLUMN task_seq INTEGER"
                )
            self._backfill_task_event_sequences(connection)
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_task_events_task_sequence
                ON task_events(task_id, task_seq)
                """
            )

    @staticmethod
    def _backfill_task_event_sequences(connection: sqlite3.Connection) -> None:
        """为旧库补齐每任务连续游标。"""

        pending = connection.execute(
            "SELECT 1 FROM task_events WHERE task_seq IS NULL LIMIT 1"
        ).fetchone()
        if pending is None:
            return
        connection.execute(
            """
            WITH ranked AS (
                SELECT seq,
                       ROW_NUMBER() OVER (
                           PARTITION BY task_id ORDER BY seq ASC
                       ) AS task_seq
                FROM task_events
            )
            UPDATE task_events
            SET task_seq = (
                SELECT ranked.task_seq FROM ranked
                WHERE ranked.seq = task_events.seq
            )
            WHERE task_seq IS NULL
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
        with self._lock, self._connection() as connection:
            self._insert_task(
                connection,
                task_id=task_id,
                user_id=user_id,
                source_node=source_node,
                request=request,
                now=now,
            )
        return task_id

    def create_task_if_queue_available(
        self,
        *,
        user_id: str,
        source_node: str,
        request: dict[str, Any],
        max_queued: int,
        idempotency_key: str | None = None,
    ) -> TaskCreationReceipt:
        """在同一 SQLite 事务中检查队列并创建任务。

        先 ``COUNT`` 再 ``INSERT`` 会让并发提交绕过每用户队列上限。
        ``BEGIN IMMEDIATE`` 让检查和插入对其他提交者保持原子性。
        """
        if max_queued < 1:
            raise ValueError("max_queued 必须大于 0")
        normalized_key = str(idempotency_key or "").strip() or None
        if normalized_key is not None and len(normalized_key) > 200:
            raise ValueError("幂等键长度不得超过 200 个字符")
        request_sha256 = stable_hash(request)
        task_id = str(uuid.uuid4())
        now = time.time()
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if normalized_key is not None:
                existing = connection.execute(
                    """
                    SELECT task_id, request_sha256 FROM tasks
                    WHERE user_id = ? AND idempotency_key = ?
                    """,
                    (user_id, normalized_key),
                ).fetchone()
                if existing is not None:
                    if str(existing["request_sha256"] or "") != request_sha256:
                        raise IdempotencyConflictError(
                            "同一幂等键已用于不同的回测请求"
                        )
                    return TaskCreationReceipt(
                        task_id=str(existing["task_id"]),
                        idempotent_replay=True,
                        idempotency_key=normalized_key,
                    )
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM tasks WHERE user_id = ? AND status = 'pending'",
                (user_id,),
            ).fetchone()
            if int(row["count"] if row else 0) >= max_queued:
                raise QueueLimitReachedError("当前用户的回测排队任务已达到上限")
            self._insert_task(
                connection,
                task_id=task_id,
                user_id=user_id,
                source_node=source_node,
                request=request,
                now=now,
                idempotency_key=normalized_key,
                request_sha256=request_sha256,
            )
        return TaskCreationReceipt(
            task_id=task_id,
            idempotent_replay=False,
            idempotency_key=normalized_key,
        )

    @staticmethod
    def _insert_task(
        connection: sqlite3.Connection,
        *,
        task_id: str,
        user_id: str,
        source_node: str,
        request: dict[str, Any],
        now: float,
        idempotency_key: str | None = None,
        request_sha256: str | None = None,
    ) -> None:
        task_contract = request.get("_task_contract") or {}
        execution = task_contract.get("execution") or {}
        state = {
            "task_id": task_id,
            "schema_version": int(task_contract.get("schema_version") or 1),
            "engine_type": task_contract.get("engine_type", "vnpy_cta"),
            "default_profile": task_contract.get("default_profile"),
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
            "completed_at": None,
            "expires_at": None,
            "replay": {},
            "live_bars": [],
            "live_trades": [],
            "live_orders": [],
            "live_positions": [],
            "strategy_lines": [],
            "recent_replay_events": [],
            "execution_snapshot": {},
            "event_seq": 0,
            "last_bar_replay_seq": 0,
            "events_pruned_through": 0,
        }
        connection.execute(
            """
            INSERT INTO tasks(
                task_id, user_id, source_node, status, request_json,
                state_json, created_at, updated_at, idempotency_key, request_sha256
            ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                user_id,
                source_node,
                json.dumps(request, ensure_ascii=False, separators=(",", ":")),
                json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                now,
                now,
                idempotency_key,
                request_sha256 or stable_hash(request),
            ),
        )

    def count_queued_for_user(self, user_id: str) -> int:
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM tasks WHERE user_id = ? AND status = 'pending'",
                (user_id,),
            ).fetchone()
        return int(row["count"] if row else 0)

    def save_draft(
        self, user_id: str, draft_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        now = time.time()
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT version FROM user_drafts
                WHERE user_id = ? AND draft_id = ?
                """,
                (user_id, draft_id),
            ).fetchone()
            version = int(row["version"] if row else 0) + 1
            connection.execute(
                """
                INSERT INTO user_drafts(
                    user_id, draft_id, version, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, draft_id) DO UPDATE SET
                    version = excluded.version,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    user_id,
                    draft_id,
                    version,
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    now,
                ),
            )
        return {
            "draft_id": draft_id,
            "version": version,
            "payload": payload,
            "updated_at": now,
        }

    def get_draft(self, user_id: str, draft_id: str) -> dict[str, Any]:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                """
                SELECT version, payload_json, updated_at FROM user_drafts
                WHERE user_id = ? AND draft_id = ?
                """,
                (user_id, draft_id),
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(draft_id)
        return {
            "draft_id": draft_id,
            "version": int(row["version"]),
            "payload": json.loads(row["payload_json"]),
            "updated_at": float(row["updated_at"]),
        }

    def delete_draft(self, user_id: str, draft_id: str) -> bool:
        with self._lock, self._connection() as connection:
            changed = connection.execute(
                "DELETE FROM user_drafts WHERE user_id = ? AND draft_id = ?",
                (user_id, draft_id),
            )
        return changed.rowcount == 1

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
            if status in TERMINAL_STATUSES and not state.get("expires_at"):
                state["completed_at"] = now
                state["expires_at"] = now + RESULT_RETENTION_SECONDS
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
            latest = connection.execute(
                """
                SELECT COALESCE(MAX(task_seq), 0) AS latest
                FROM task_events WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
            next_sequence = int(latest["latest"] or 0)

            for event_type, payload in events:
                next_sequence += 1
                connection.execute(
                    """
                    INSERT INTO task_events(
                        task_seq, task_id, user_id, event_type, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        next_sequence,
                        task_id,
                        user_id,
                        event_type,
                        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                        time.time(),
                    ),
                )
                seq = next_sequence
                sequences.append(seq)
                state = self._apply_event(state, event_type, payload)
                state["event_seq"] = seq
                if event_type in {"completed", "cancelled", "failed"}:
                    result_path = str(payload.get("result_path") or "")
                if event_type == "failed":
                    error = str(payload.get("error") or "")

            status = str(state.get("status") or "running")
            now = time.time()
            if status in TERMINAL_STATUSES and not state.get("expires_at"):
                state["completed_at"] = now
                state["expires_at"] = now + RESULT_RETENTION_SECONDS
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
            if key == "execution":
                snapshot = payload.get("snapshot")
                if isinstance(snapshot, dict):
                    state["execution_snapshot"] = snapshot
                return state
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
            state["result_available"] = bool(
                payload.get("result_available") or payload.get("result_path")
            )
            if payload.get("progress") is not None:
                state["progress"] = float(payload["progress"])
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

    def expire_due_results(self, now: float | None = None) -> list[str]:
        checked_at = time.time() if now is None else now
        expired: list[str] = []
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                """
                SELECT task_id, state_json FROM tasks
                WHERE status IN ('completed', 'failed', 'cancelled')
                """
            ).fetchall()
            for row in rows:
                state = json.loads(row["state_json"])
                expires_at = float(state.get("expires_at") or 0)
                if expires_at <= 0 or expires_at > checked_at or state.get("result_expired"):
                    continue
                task_id = str(row["task_id"])
                state["result_available"] = False
                state["result_expired"] = True
                state["expired_at"] = checked_at
                connection.execute(
                    """
                    UPDATE tasks SET state_json = ?, result_path = '', updated_at = ?
                    WHERE task_id = ?
                    """,
                    (
                        json.dumps(state, ensure_ascii=False, separators=(",", ":")),
                        checked_at,
                        task_id,
                    ),
                )
                expired.append(task_id)
        return expired

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
                    "default_profile": task_contract.get("default_profile"),
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
                    "completed_at": state.get("completed_at"),
                    "expires_at": state.get("expires_at"),
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
                    "seq": int(row["task_seq"]),
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
            "execution_snapshot",
            "current_bar",
        )
        return {key: state.get(key) for key in keys}

    def event_page(
        self, user_id: str, task_id: str, after_seq: int, limit: int = 200
    ) -> dict[str, Any]:
        """原子读取某个任务的完整追加式事件窗口。"""
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
                SELECT MIN(task_seq) AS earliest_seq, MAX(task_seq) AS latest_seq
                FROM task_events WHERE task_id = ? AND user_id = ?
                """,
                (task_id, user_id),
            ).fetchone()
            earliest_seq = int(bounds["earliest_seq"] or 0) if bounds else 0
            latest_seq = int(bounds["latest_seq"] or 0) if bounds else 0
            rows = connection.execute(
                """
                SELECT task_seq, event_type, payload_json, created_at
                FROM task_events
                WHERE task_id = ? AND user_id = ? AND task_seq > ?
                ORDER BY task_seq ASC LIMIT ?
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
            "history_truncated": False,
            "resync": None,
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
                SELECT task_seq, event_type, payload_json, created_at
                FROM task_events
                WHERE task_id = ? AND user_id = ? AND task_seq > ?
                ORDER BY task_seq ASC LIMIT ?
                """,
                (task_id, user_id, after_seq, limit),
            ).fetchall()
        return self._serialize_event_rows(rows)
