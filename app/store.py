"""事件存取层：幂等写入、时间游标分页、确认。"""
from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .db import Database

STATUS_PENDING = "pending"
STATUS_ACKED = "acked"


class EventConflictError(Exception):
    """event_id 已存在，但提交的内容与首个版本不同。"""

    def __init__(self, event_id: str) -> None:
        super().__init__(event_id)
        self.event_id = event_id


class BatchInconsistentError(Exception):
    """同一批次内相同 event_id 携带了不同内容（对应 HTTP 422）。

    conflicts 为 [{"event_id": ..., "indices": [批内下标, ...]}, ...]。
    """

    def __init__(self, conflicts: list[dict]) -> None:
        super().__init__("inconsistent batch")
        self.conflicts = conflicts


class BatchConflictError(Exception):
    """批次内事件与库中已存的首个版本内容不同（对应 HTTP 409）。

    conflicts 为 [{"event_id": ..., "indices": [批内下标, ...]}, ...]。
    """

    def __init__(self, conflicts: list[dict]) -> None:
        super().__init__("batch conflicts with stored events")
        self.conflicts = conflicts


class EventNotFoundError(Exception):
    pass


class InvalidCursorError(Exception):
    pass


def utcnow_iso() -> str:
    """固定 6 位小数的 UTC ISO-8601 字符串，保证字典序 == 时间序。"""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"


def canonical_json(value: Any) -> str:
    """规范化 JSON：键排序、无空白，使语义相同的 payload 得到相同文本。"""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(type_: str, payload: Any) -> str:
    return hashlib.sha256(f"{type_}\n{canonical_json(payload)}".encode("utf-8")).hexdigest()


def _row_to_event(row) -> dict:
    return {
        "event_id": row["event_id"],
        "type": row["type"],
        "payload": json.loads(row["payload"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "acked_at": row["acked_at"],
    }


def encode_cursor(created_at: str, event_id: str) -> str:
    raw = json.dumps({"created_at": created_at, "event_id": event_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return str(data["created_at"]), str(data["event_id"])
    except Exception as exc:
        raise InvalidCursorError(cursor) from exc


class EventStore:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create_event(self, event_id: str, type_: str, payload: Any) -> tuple[dict, bool]:
        """幂等写入。

        返回 (事件记录, 是否新建)。并发下由 INSERT OR IGNORE 保证只有首个
        版本落库；重复提交返回库中已存的首个版本；内容不同则抛冲突。
        """
        digest = content_hash(type_, payload)
        created_at = utcnow_iso()
        with self.db.lock:
            cur = self.db.conn.execute(
                """
                INSERT OR IGNORE INTO events
                    (event_id, type, payload, content_hash, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, type_, canonical_json(payload), digest, STATUS_PENDING, created_at),
            )
            created = cur.rowcount == 1
            row = self.db.conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        if not created and row["content_hash"] != digest:
            raise EventConflictError(event_id)
        return _row_to_event(row), created

    def create_events_batch(
        self, events: list[tuple[str, str, Any]]
    ) -> list[tuple[dict, bool]]:
        """原子批量入箱。

        1. 批内按 event_id 归并：同 ID 内容语义相同视为重复，映射到同一结果；
           内容不同抛 BatchInconsistentError（不触碰数据库）。
        2. 与库中记录比对：任一内容冲突抛 BatchConflictError，整批零写入。
        3. 无冲突时在单一事务（BEGIN IMMEDIATE）内写入所有新事件。

        返回与入参等长、同序的 [(记录, 是否新建), ...]。并发批次由写锁与
        单事务保证不产生部分提交，也不会覆盖库中首个版本。
        """
        if not events:
            return []

        # event_id -> type、规范化 payload、内容指纹；indices 记录批内全部下标
        types: dict[str, str] = {}
        payloads: dict[str, str] = {}
        digests: dict[str, str] = {}
        indices: dict[str, list[int]] = {}
        inconsistent_ids: set[str] = set()

        for index, (event_id, type_, payload) in enumerate(events):
            indices.setdefault(event_id, []).append(index)
            if event_id not in digests:
                types[event_id] = type_
                payloads[event_id] = canonical_json(payload)
                digests[event_id] = content_hash(type_, payload)
            elif digests[event_id] != content_hash(type_, payload):
                inconsistent_ids.add(event_id)

        if inconsistent_ids:
            # 批内冲突优先，无需写库；下标按首次出现顺序给出
            inconsistent = [
                {"event_id": event_id, "indices": indices[event_id]}
                for event_id in indices
                if event_id in inconsistent_ids
            ]
            raise BatchInconsistentError(inconsistent)

        created_at = utcnow_iso()
        with self.db.lock:
            # IMMEDIATE 立即取得保留锁，使并发批次的“检查 + 写入”串行化
            self.db.conn.execute("BEGIN IMMEDIATE")
            committed = False
            try:
                ids = list(digests)
                placeholders = ",".join("?" for _ in ids)
                rows = self.db.conn.execute(
                    f"SELECT * FROM events WHERE event_id IN ({placeholders})", ids
                ).fetchall()
                stored = {row["event_id"]: row for row in rows}

                conflicts = [
                    {"event_id": event_id, "indices": indices[event_id]}
                    for event_id in ids
                    if event_id in stored
                    and stored[event_id]["content_hash"] != digests[event_id]
                ]
                if conflicts:
                    raise BatchConflictError(conflicts)

                new_ids = [event_id for event_id in ids if event_id not in stored]
                self.db.conn.executemany(
                    """
                    INSERT OR IGNORE INTO events
                        (event_id, type, payload, content_hash, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            event_id,
                            types[event_id],
                            payloads[event_id],
                            digests[event_id],
                            STATUS_PENDING,
                            created_at,
                        )
                        for event_id in new_ids
                    ],
                )
                self.db.conn.execute("COMMIT")
                committed = True
            except Exception:
                if not committed:
                    # 整批回滚：冲突或任何中途失败都零写入
                    self.db.conn.execute("ROLLBACK")
                raise

            # 事务提交后再回读，确保返回的全部是已提交数据
            rows = self.db.conn.execute(
                f"SELECT * FROM events WHERE event_id IN ({placeholders})", ids
            ).fetchall()
            stored = {row["event_id"]: row for row in rows}

        created_ids = set(new_ids)
        results_by_id = {
            event_id: (_row_to_event(stored[event_id]), event_id in created_ids)
            for event_id in ids
        }
        return [results_by_id[event_id] for event_id, _, _ in events]

    def get_event(self, event_id: str) -> dict:
        row = self.db.conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise EventNotFoundError(event_id)
        return _row_to_event(row)

    def list_events(
        self, limit: int, cursor: str | None = None, status: str | None = None
    ) -> dict:
        """按 (created_at, event_id) 升序的键集分页（时间游标）。"""
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if cursor is not None:
            c_created, c_id = decode_cursor(cursor)
            clauses.append("(created_at > ? OR (created_at = ? AND event_id > ?))")
            params.extend([c_created, c_created, c_id])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.conn.execute(
            f"SELECT * FROM events {where} ORDER BY created_at, event_id LIMIT ?",
            (*params, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        items = [_row_to_event(r) for r in rows[:limit]]
        next_cursor = (
            encode_cursor(items[-1]["created_at"], items[-1]["event_id"]) if has_more else None
        )
        return {"items": items, "next_cursor": next_cursor, "has_more": has_more}

    def ack_event(self, event_id: str) -> dict:
        """确认单条事件。幂等：重复确认返回当前状态。"""
        with self.db.lock:
            self.db.conn.execute(
                "UPDATE events SET status = ?, acked_at = ? WHERE event_id = ? AND status = ?",
                (STATUS_ACKED, utcnow_iso(), event_id, STATUS_PENDING),
            )
            row = self.db.conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
        if row is None:
            raise EventNotFoundError(event_id)
        return _row_to_event(row)
