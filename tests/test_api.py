import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.main import app
from app.store import (
    BatchConflictError,
    BatchInconsistentError,
    EventConflictError,
    EventNotFoundError,
    EventStore,
)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def store(tmp_path):
    s = EventStore(Database(str(tmp_path / "store.db")))
    yield s
    s.db.close()


def make_event(i: int) -> dict:
    return {"event_id": f"evt-{i:04d}", "type": "user.created", "payload": {"n": i}}


# ---------- API 层 ----------


def test_create_event_returns_201(client):
    r = client.post("/events", json=make_event(1))
    assert r.status_code == 201
    body = r.json()
    assert body["event_id"] == "evt-0001"
    assert body["type"] == "user.created"
    assert body["payload"] == {"n": 1}
    assert body["status"] == "pending"
    assert body["acked_at"] is None
    assert body["created_at"]


def test_duplicate_same_content_returns_consistent_result(client):
    first = client.post("/events", json=make_event(1))
    dup = client.post("/events", json=make_event(1))
    assert first.status_code == 201
    assert dup.status_code == 200
    assert dup.json() == first.json()  # 返回与首个版本一致的结果


def test_duplicate_is_semantic_not_textual(client):
    client.post("/events", json={"event_id": "e1", "type": "t", "payload": {"a": 1, "b": 2}})
    r = client.post("/events", json={"event_id": "e1", "type": "t", "payload": {"b": 2, "a": 1}})
    assert r.status_code == 200  # 键序不同但语义相同，不算冲突


def test_same_id_different_content_returns_409(client):
    client.post("/events", json=make_event(1))
    r = client.post(
        "/events", json={"event_id": "evt-0001", "type": "user.created", "payload": {"n": 999}}
    )
    assert r.status_code == 409
    r2 = client.post(
        "/events", json={"event_id": "evt-0001", "type": "other.type", "payload": {"n": 1}}
    )
    assert r2.status_code == 409
    # 冲突写入不会覆盖首个版本
    assert client.get("/events/evt-0001").json()["payload"] == {"n": 1}


def test_validation_error(client):
    assert client.post("/events", json={"event_id": "x"}).status_code == 422
    assert client.post("/events", json={"event_id": "", "type": "t", "payload": {}}).status_code == 422


def test_get_single_event(client):
    client.post("/events", json=make_event(1))
    r = client.get("/events/evt-0001")
    assert r.status_code == 200
    assert r.json()["event_id"] == "evt-0001"
    assert client.get("/events/nope").status_code == 404


def test_pagination_walks_all_events_in_order(client):
    for i in range(25):
        client.post("/events", json=make_event(i))

    seen, cursor, pages = [], None, 0
    while True:
        params = {"limit": 10}
        if cursor:
            params["cursor"] = cursor
        r = client.get("/events", params=params)
        assert r.status_code == 200
        body = r.json()
        seen.extend(item["event_id"] for item in body["items"])
        cursor = body["next_cursor"]
        pages += 1
        if not body["has_more"]:
            break

    assert pages == 3
    assert len(seen) == 25
    assert len(set(seen)) == 25  # 无重复
    assert seen == sorted(seen)  # 按 event_id 字典序（此处与时间序一致）
    assert cursor is None


def test_pagination_invalid_cursor(client):
    assert client.get("/events", params={"cursor": "!!!bad!!!"}).status_code == 400


def test_status_filter(client):
    for i in range(5):
        client.post("/events", json=make_event(i))
    client.post("/events/evt-0001/ack")
    client.post("/events/evt-0003/ack")

    pending = client.get("/events", params={"status": "pending"}).json()
    acked = client.get("/events", params={"status": "acked"}).json()
    assert len(pending["items"]) == 3
    assert len(acked["items"]) == 2
    assert {i["event_id"] for i in acked["items"]} == {"evt-0001", "evt-0003"}


def test_ack_event(client):
    client.post("/events", json=make_event(1))
    r = client.post("/events/evt-0001/ack")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "acked"
    assert body["acked_at"]

    again = client.post("/events/evt-0001/ack")  # 幂等
    assert again.status_code == 200
    assert again.json()["status"] == "acked"

    assert client.get("/events/evt-0001").json()["status"] == "acked"


def test_ack_not_found(client):
    assert client.post("/events/nope/ack").status_code == 404


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# ---------- 批量入箱：API 层 ----------


def test_batch_creates_all_and_marks_created(client):
    r = client.post(
        "/events/batch",
        json=[make_event(1), make_event(2), make_event(3)],
    )
    assert r.status_code == 201
    items = r.json()
    assert [i["event_id"] for i in items] == ["evt-0001", "evt-0002", "evt-0003"]
    assert all(i["created"] is True for i in items)
    assert all(i["status"] == "pending" for i in items)
    # 同一批次时间戳一致（单事务写入）
    assert len({i["created_at"] for i in items}) == 1
    for i in (1, 2, 3):
        assert client.get(f"/events/evt-000{i}").status_code == 200


def test_batch_response_order_matches_input_with_duplicates(client):
    body = [make_event(2), make_event(1), make_event(2), make_event(3)]
    r = client.post("/events/batch", json=body)
    assert r.status_code == 201
    items = r.json()
    assert [i["event_id"] for i in items] == ["evt-0002", "evt-0001", "evt-0002", "evt-0003"]
    # 批内重复映射到同一结果（含相同 created 标记）
    assert items[0] == items[2]
    assert [i["created"] for i in items] == [True, True, True, True]


def test_batch_semantic_duplicate_within_batch(client):
    r = client.post(
        "/events/batch",
        json=[
            {"event_id": "e1", "type": "t", "payload": {"a": 1, "b": 2}},
            {"event_id": "e1", "type": "t", "payload": {"b": 2, "a": 1}},
        ],
    )
    assert r.status_code == 201
    assert [i["created"] for i in r.json()] == [True, True]
    assert r.json()[0] == r.json()[1]


def test_batch_existing_ids_not_created_and_mixed_status(client):
    client.post("/events", json=make_event(1))
    r = client.post(
        "/events/batch",
        json=[make_event(1), make_event(2)],
    )
    assert r.status_code == 201  # 有新建即 201
    flags = [i["created"] for i in r.json()]
    assert flags == [False, True]
    # 已存在事件返回首个版本
    first = client.get("/events/evt-0001").json()
    assert r.json()[0]["created_at"] == first["created_at"]


def test_batch_all_existing_returns_200(client):
    client.post("/events", json=make_event(1))
    r = client.post("/events/batch", json=[make_event(1), make_event(1)])
    assert r.status_code == 200
    assert all(i["created"] is False for i in r.json())


def test_batch_inconsistent_within_batch_returns_422(client):
    r = client.post(
        "/events/batch",
        json=[
            {"event_id": "e1", "type": "t", "payload": {"v": 1}},
            {"event_id": "e2", "type": "t", "payload": {"v": 2}},
            {"event_id": "e1", "type": "t", "payload": {"v": 3}},
        ],
    )
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["conflicts"] == [{"event_id": "e1", "indices": [0, 2]}]
    # 整批零写入（包括内容正常的 e2）
    assert client.get("/events/e1").status_code == 404
    assert client.get("/events/e2").status_code == 404


def test_batch_type_conflict_within_batch_returns_422(client):
    r = client.post(
        "/events/batch",
        json=[
            {"event_id": "e1", "type": "t.a", "payload": {}},
            {"event_id": "e1", "type": "t.b", "payload": {}},
        ],
    )
    assert r.status_code == 422
    assert r.json()["detail"]["conflicts"][0]["indices"] == [0, 1]


def test_batch_conflict_with_stored_event_returns_409_and_zero_writes(client):
    client.post("/events", json={"event_id": "e1", "type": "t", "payload": {"v": 1}})
    r = client.post(
        "/events/batch",
        json=[
            {"event_id": "e2", "type": "t", "payload": {"v": 2}},
            {"event_id": "e1", "type": "t", "payload": {"v": 999}},
            {"event_id": "e1", "type": "t", "payload": {"v": 999}},
        ],
    )
    assert r.status_code == 409
    assert r.json()["detail"]["conflicts"] == [{"event_id": "e1", "indices": [1, 2]}]
    # 整批零写入：e2 不存在，e1 仍是首个版本
    assert client.get("/events/e2").status_code == 404
    assert client.get("/events/e1").json()["payload"] == {"v": 1}


def test_batch_within_batch_conflict_takes_priority_over_stored_conflict(client):
    client.post("/events", json={"event_id": "e1", "type": "t", "payload": {"v": 1}})
    r = client.post(
        "/events/batch",
        json=[
            {"event_id": "e1", "type": "t", "payload": {"v": 2}},  # 与库冲突
            {"event_id": "e1", "type": "t", "payload": {"v": 3}},  # 批内也冲突
        ],
    )
    assert r.status_code == 422


def test_batch_size_validation(client):
    assert client.post("/events/batch", json=[]).status_code == 422
    too_many = [
        {"event_id": f"e{i}", "type": "t", "payload": {}} for i in range(101)
    ]
    assert client.post("/events/batch", json=too_many).status_code == 422
    assert client.post("/events/batch", json={}).status_code == 422
    # 批内单条结构仍走与单条接口相同的校验
    bad = client.post(
        "/events/batch", json=[{"event_id": "", "type": "t", "payload": {}}]
    )
    assert bad.status_code == 422


def test_batch_boundary_size_accepted(client):
    body = [
        {"event_id": f"e{i:03d}", "type": "t", "payload": {"i": i}}
        for i in range(100)
    ]
    r = client.post("/events/batch", json=body)
    assert r.status_code == 201
    assert len(r.json()) == 100


def test_batch_then_single_interop(client):
    """批量与单条接口共享同一套幂等规则。"""
    r = client.post("/events/batch", json=[make_event(1)])
    assert r.status_code == 201
    single = client.post("/events", json=make_event(1))
    assert single.status_code == 200  # 批量已写入，单条视为重复
    conflict = client.post(
        "/events", json={"event_id": "evt-0001", "type": "t", "payload": {"n": 2}}
    )
    assert conflict.status_code == 409


# ---------- 存储层：批量并发与原子性 ----------


def test_store_batch_inconsistent_raises(store):
    with pytest.raises(BatchInconsistentError) as exc:
        store.create_events_batch(
            [("e1", "t", {"v": 1}), ("e2", "t", {}), ("e1", "t", {"v": 2})]
        )
    assert exc.value.conflicts == [{"event_id": "e1", "indices": [0, 2]}]
    with pytest.raises(EventNotFoundError):
        store.get_event("e2")


def test_store_batch_conflict_rolls_back(store):
    store.create_event("e1", "t", {"v": 1})
    with pytest.raises(BatchConflictError) as exc:
        store.create_events_batch(
            [("e2", "t", {"v": 2}), ("e1", "t", {"v": 9})]
        )
    assert exc.value.conflicts == [{"event_id": "e1", "indices": [1]}]
    with pytest.raises(EventNotFoundError):
        store.get_event("e2")  # 同事务中的新事件也未写入


def test_concurrent_identical_batches_write_once(store):
    def submit(_):
        return store.create_events_batch(
            [("evt-b", "t", {"v": "same"}), ("evt-c", "t", {"v": "same"})]
        )

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(64)))

    # 只有一个批次真正写入；其余批次的两个事件都标记为未新建
    winning = [batch for batch in results if all(created for _, created in batch)]
    losing = [batch for batch in results if not any(created for _, created in batch)]
    assert len(winning) == 1
    assert len(losing) == 63
    # 所有批次返回的记录内容一致（首个版本）
    winner_records = [rec for rec, _ in winning[0]]
    for batch in results:
        assert [rec for rec, _ in batch] == winner_records
    # 全进程总共只写入两个事件
    assert store.db.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2


def test_concurrent_batches_same_id_different_content_one_winner(store):
    def submit(i):
        try:
            _rec, created = store.create_events_batch(
                [("evt-z", "t", {"v": "a" if i % 2 == 0 else "b"})]
            )[0]
            return ("ok", created)
        except BatchConflictError:
            return ("conflict", False)

    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = list(pool.map(submit, range(64)))

    winners = [created for _status, created in outcomes if created]
    assert len(winners) == 1  # 只有首个版本真正写入
    # 同内容者幂等成功，异内容者全部 409
    statuses = [status for status, _ in outcomes]
    assert statuses.count("conflict") == 32
    assert statuses.count("ok") == 32
    stored = store.get_event("evt-z")
    assert stored["payload"] in ({"v": "a"}, {"v": "b"})
    assert store.db.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_concurrent_batch_and_single_share_idempotency(store):
    def submit(i):
        try:
            if i % 2 == 0:
                rec, created = store.create_event("evt-m", "t", {"v": 1})
                return ("single", created)
            batch = store.create_events_batch([("evt-m", "t", {"v": 1})])
            return ("batch", batch[0][1])
        except (EventConflictError, BatchConflictError):
            return ("conflict", None)

    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = list(pool.map(submit, range(64)))

    assert sum(1 for _who, created in outcomes if created) == 1
    assert not any(who == "conflict" for who, _ in outcomes)  # 内容一致，无冲突


# ---------- 存储层：并发与持久化 ----------


def test_concurrent_duplicates_keep_first_version(store):
    def submit(_):
        return store.create_event("evt-x", "t", {"v": "first"})

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(64)))

    assert sum(created for _, created in results) == 1  # 只有一个请求真正写入
    first = results[0][0]
    assert all(rec == first for rec, _ in results)  # 所有结果一致（首个版本）


def test_concurrent_conflicting_content_single_winner(store):
    def submit(i):
        try:
            rec, _ = store.create_event("evt-y", "t", {"v": "a" if i % 2 == 0 else "b"})
            return ("ok", json.dumps(rec["payload"], sort_keys=True))
        except EventConflictError:
            return ("conflict", None)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(64)))

    winners = {payload for status, payload in results if status == "ok"}
    assert len(winners) == 1  # 并发下只接受了一个版本
    assert any(status == "conflict" for status, _ in results)  # 另一方全部 409
    stored = store.get_event("evt-y")
    assert json.dumps(stored["payload"], sort_keys=True) == next(iter(winners))


def test_persistence_across_reopen(tmp_path):
    path = str(tmp_path / "persist.db")
    s1 = EventStore(Database(path))
    s1.create_event("evt-p", "t", {"k": 1})
    s1.ack_event("evt-p")
    s1.db.close()

    s2 = EventStore(Database(path))  # 重新打开同一文件
    rec = s2.get_event("evt-p")
    assert rec["status"] == "acked"
    assert rec["acked_at"]
    assert rec["payload"] == {"k": 1}
    s2.db.close()
