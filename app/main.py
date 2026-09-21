"""Webhook 收件箱 API。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from .db import Database
from .store import (
    BatchConflictError,
    BatchInconsistentError,
    EventConflictError,
    EventNotFoundError,
    EventStore,
    InvalidCursorError,
)


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    type: str = Field(min_length=1, max_length=128)
    payload: Any


class EventOut(BaseModel):
    event_id: str
    type: str
    payload: Any
    status: Literal["pending", "acked"]
    created_at: str
    acked_at: str | None = None


class EventBatchItem(EventOut):
    created: bool


class EventPage(BaseModel):
    items: list[EventOut]
    next_cursor: str | None
    has_more: bool


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时自动建表（CREATE TABLE IF NOT EXISTS）
    db = Database(os.environ.get("DATABASE_PATH", "events.db"))
    app.state.store = EventStore(db)
    yield
    db.close()


app = FastAPI(title="Webhook Inbox", version="1.1.0", lifespan=lifespan)


def get_store(request: Request) -> EventStore:
    return request.app.state.store


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/events", response_model=EventOut, status_code=201)
def create_event(event: EventIn, response: Response, store: EventStore = Depends(get_store)):
    """提交事件。重复提交（内容相同）返回 200 与首个版本；内容不同返回 409。"""
    try:
        record, created = store.create_event(event.event_id, event.type, event.payload)
    except EventConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"event_id '{exc.event_id}' already exists with different content",
        )
    response.status_code = 201 if created else 200
    return record


@app.post("/events/batch", response_model=list[EventBatchItem], status_code=201)
def create_events_batch(
    response: Response,
    events: list[EventIn] = Body(min_length=1, max_length=100),
    store: EventStore = Depends(get_store),
):
    """原子批量提交 1～100 个事件（结构与 POST /events 一致）。

    - 批内同 event_id 内容语义相同：归并为重复，映射到同一结果；
      内容不同返回 422 并标出冲突下标，整批不写库。
    - 与库中已存事件内容冲突：返回 409 及对应下标，整批零写入。
    - 无冲突时单事务写入，响应按原输入顺序返回记录及 created 标记；
      全部命中已存记录时返回 200，否则 201。
    """
    try:
        results = store.create_events_batch(
            [(event.event_id, event.type, event.payload) for event in events]
        )
    except BatchInconsistentError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "same event_id appears with different content within the batch",
                "conflicts": exc.conflicts,
            },
        )
    except BatchConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "event_id already exists with different content",
                "conflicts": exc.conflicts,
            },
        )
    response.status_code = 201 if any(created for _, created in results) else 200
    return [{**record, "created": created} for record, created in results]



@app.get("/events", response_model=EventPage)
def list_events(
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
    status: Literal["pending", "acked"] | None = Query(default=None),
    store: EventStore = Depends(get_store),
):
    """按时间游标分页查询事件（升序）。"""
    try:
        return store.list_events(limit=limit, cursor=cursor, status=status)
    except InvalidCursorError:
        raise HTTPException(status_code=400, detail="invalid cursor")


@app.get("/events/{event_id}", response_model=EventOut)
def get_event(event_id: str, store: EventStore = Depends(get_store)):
    try:
        return store.get_event(event_id)
    except EventNotFoundError:
        raise HTTPException(status_code=404, detail="event not found")


@app.post("/events/{event_id}/ack", response_model=EventOut)
def ack_event(event_id: str, store: EventStore = Depends(get_store)):
    """确认单条事件（幂等）。"""
    try:
        return store.ack_event(event_id)
    except EventNotFoundError:
        raise HTTPException(status_code=404, detail="event not found")
