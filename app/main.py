"""Webhook 收件箱 API。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from .db import Database
from .store import (
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


app = FastAPI(title="Webhook Inbox", version="1.0.0", lifespan=lifespan)


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
