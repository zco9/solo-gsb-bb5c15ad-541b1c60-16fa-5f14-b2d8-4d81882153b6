"""API 行为测试：租约独占/过期、幂等上报、提交与状态查询。"""
import hashlib
import os
import tempfile
import threading
from datetime import datetime, timezone

import pytest

# 必须在导入 app 之前指定测试数据库
os.environ["UPLOAD_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
os.environ["DEFAULT_LEASE_TTL_SECONDS"] = "300"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import get_conn  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)


def make_session(total_chunks=3, file_digest="f" * 64):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease(session_id, index, ttl=None):
    body = {} if ttl is None else {"lease_ttl_seconds": ttl}
    return client.post(f"/sessions/{session_id}/chunks/{index}/lease", json=body)


def complete(session_id, index, token, digest):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/complete",
        json={"lease_token": token, "chunk_digest": digest},
    )


def test_create_session_and_status():
    s = make_session(total_chunks=4)
    assert s["status"] == "active"
    assert s["total_chunks"] == 4

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["completed_chunks"] == 0
    assert st["missing_chunks"] == [0, 1, 2, 3]
    assert [c["status"] for c in st["chunks"]] == ["pending"] * 4


def test_lease_is_exclusive_until_expiry():
    s = make_session()
    r1 = lease(s["id"], 0)
    assert r1.status_code == 200
    token1 = r1.json()["lease_token"]
    assert token1

    # 同一分片在租约有效期内不可再次领取
    r2 = lease(s["id"], 0)
    assert r2.status_code == 409

    # 手动让租约过期后，可被重新领取
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    r3 = lease(s["id"], 0)
    assert r3.status_code == 200
    assert r3.json()["lease_token"] != token1


def test_complete_requires_valid_lease_and_token():
    s = make_session()
    # 未领取租约直接上报
    assert complete(s["id"], 0, "no-such-token", "a" * 64).status_code == 409

    token = lease(s["id"], 0).json()["lease_token"]
    # 令牌不匹配
    assert complete(s["id"], 0, "wrong-token", "a" * 64).status_code == 409
    # 正确令牌
    assert complete(s["id"], 0, token, "a" * 64).status_code == 200


def test_expired_lease_cannot_complete():
    s = make_session()
    token = lease(s["id"], 0).json()["lease_token"]
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    assert complete(s["id"], 0, token, "a" * 64).status_code == 409


def test_complete_is_idempotent():
    s = make_session()
    token = lease(s["id"], 1).json()["lease_token"]
    digest = "b" * 64

    r1 = complete(s["id"], 1, token, digest)
    assert r1.status_code == 200
    assert r1.json()["idempotent_replay"] is False

    # 相同令牌 + 相同摘要：幂等成功，completed_at 不变
    r2 = complete(s["id"], 1, token, digest)
    assert r2.status_code == 200
    assert r2.json()["idempotent_replay"] is True
    assert r2.json()["completed_at"] == r1.json()["completed_at"]

    # 相同令牌但摘要不同：冲突
    assert complete(s["id"], 1, token, "c" * 64).status_code == 409
    # 已完成分片不可再领取租约
    assert lease(s["id"], 1).status_code == 409


def test_finalize_blocked_until_all_chunks_done():
    s = make_session(total_chunks=3)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200

    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["missing_chunks"] == [1, 2]

    for i in (1, 2):
        t = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, t, str(i) * 64).status_code == 200

    ok = client.post(f"/sessions/{s['id']}/finalize")
    assert ok.status_code == 200
    body = ok.json()
    assert body["status"] == "finalized"
    assert body["file_digest"] == "f" * 64
    # 清单按分片序号升序汇总
    assert [m["chunk_index"] for m in body["manifest"]] == [0, 1, 2]
    assert [m["chunk_digest"] for m in body["manifest"]] == [str(i) * 64 for i in range(3)]

    # 重复提交幂等
    again = client.post(f"/sessions/{s['id']}/finalize")
    assert again.status_code == 200
    assert again.json()["manifest"] == body["manifest"]

    # 已提交会话不可再领取租约
    assert lease(s["id"], 0).status_code == 409

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["status"] == "finalized"
    assert st["missing_chunks"] == []
    assert st["completed_chunks"] == 3


def test_unknown_session_and_chunk():
    assert client.get("/sessions/nope").status_code == 404
    s = make_session()
    assert lease(s["id"], 99).status_code == 404
    assert complete(s["id"], 99, "t", "d").status_code == 404


def test_validation():
    assert client.post("/sessions", json={"total_chunks": 0, "file_digest": "x"}).status_code == 422
    assert client.post("/sessions", json={"total_chunks": 1}).status_code == 422


# ---------------------------------------------------------------- 依赖调度

def make_session_with_deps(total_chunks, dependencies):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256",
        "dependencies": {str(k): v for k, v in dependencies.items()},
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def ready(session_id):
    resp = client.get(f"/sessions/{session_id}/ready")
    assert resp.status_code == 200, resp.text
    return resp.json()


def expire_lease(session_id, index):
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = ?",
        (session_id, index),
    )
    get_conn().commit()


def create_session_raw(payload):
    return client.post("/sessions", json={"file_digest": "f" * 64, **payload})


def test_dependencies_rejected_when_invalid():
    # 键越界
    assert create_session_raw({"total_chunks": 3, "dependencies": {"3": [0]}}).status_code == 422
    assert create_session_raw({"total_chunks": 3, "dependencies": {"-1": [0]}}).status_code == 422
    # 值越界
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [3]}}).status_code == 422
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [-2]}}).status_code == 422
    # 自依赖
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [1]}}).status_code == 422
    # 环：二元环与三元环
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"0": [1], "1": [0]}}).status_code == 422
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"0": [1], "1": [2], "2": [0]}}).status_code == 422
    # 非数值键
    assert create_session_raw({"total_chunks": 3, "dependencies": {"x": [0]}}).status_code == 422
    # 合法依赖（含重复值，服务端去重）可创建
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"2": [0, 1, 1]}}).status_code == 201


def test_lease_blocked_until_dependencies_complete():
    s = make_session_with_deps(3, {2: [0, 1]})

    # 前置全部未完成：409 + 按序缺失前置
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [0, 1]

    # 完成一个前置后仍被阻塞，缺失列表随之缩小
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [1]

    # 前置全部完成后无需额外操作即可领取
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200
    assert lease(s["id"], 2).status_code == 200

    # 无依赖的分片不受影响
    s2 = make_session()
    assert lease(s2["id"], 2).status_code == 200


def test_status_exposes_dependencies():
    s = make_session_with_deps(3, {2: [0, 1]})
    st = client.get(f"/sessions/{s['id']}").json()
    assert [c["dependencies"] for c in st["chunks"]] == [[], [], [0, 1]]

    # 未提供依赖的会话：每个分片 dependencies 为空
    st2 = client.get(f"/sessions/{make_session()['id']}").json()
    assert [c["dependencies"] for c in st2["chunks"]] == [[], [], []]


def test_ready_endpoint_lists_ready_and_blocked():
    s = make_session_with_deps(4, {2: [0], 3: [1, 2]})

    # 初始：0、1 可领取；2 等 0，3 等 1 和 2
    r = ready(s["id"])
    assert r["ready_chunks"] == [0, 1]
    assert r["blocked_chunks"] == [
        {"chunk_index": 2, "missing_dependencies": [0]},
        {"chunk_index": 3, "missing_dependencies": [1, 2]},
    ]

    # 持有有效租约的分片不再出现在 ready 中
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert ready(s["id"])["ready_chunks"] == [1]

    # 完成 0 后 2 解锁
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r = ready(s["id"])
    assert r["ready_chunks"] == [1, 2]
    assert r["blocked_chunks"] == [{"chunk_index": 3, "missing_dependencies": [1, 2]}]

    # 链式推进：完成 1、2 后 3 解锁
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200
    t2 = lease(s["id"], 2).json()["lease_token"]
    assert complete(s["id"], 2, t2, "2" * 64).status_code == 200
    r = ready(s["id"])
    assert r["ready_chunks"] == [3]
    assert r["blocked_chunks"] == []


def test_ready_endpoint_treats_expired_lease_as_claimable():
    s = make_session_with_deps(2, {1: [0]})

    lease(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == []

    # 租约过期后恢复为可领取
    expire_lease(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == [0]

    # 无依赖会话：全部 pending 分片均可领取，无阻塞
    s2 = make_session()
    r = ready(s2["id"])
    assert r["ready_chunks"] == [0, 1, 2]
    assert r["blocked_chunks"] == []


def test_ready_unknown_session():
    assert client.get("/sessions/nope/ready").status_code == 404


def test_finalize_with_dependencies():
    s = make_session_with_deps(2, {1: [0]})
    # 被依赖阻塞时无法完成后续分片，提交自然列出缺失
    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["missing_chunks"] == [0, 1]

    for i in (0, 1):
        t = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, t, str(i) * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200


# ---------------------------------------------------------------- 会话级并发上限与批量领取

def make_capped_session(total_chunks=5, max_active_leases=2, dependencies=None):
    payload = {
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256",
        "max_active_leases": max_active_leases,
    }
    if dependencies is not None:
        payload["dependencies"] = {str(k): v for k, v in dependencies.items()}
    resp = client.post("/sessions", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def batch_lease(session_id, count, ttl=None):
    body = {"count": count}
    if ttl is not None:
        body["lease_ttl_seconds"] = ttl
    return client.post(f"/sessions/{session_id}/leases/batch", json=body)


def test_create_session_persists_max_active_leases():
    s = make_capped_session(max_active_leases=3)
    assert s["max_active_leases"] == 3

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["max_active_leases"] == 3
    assert st["active_leases"] == 0

    # 缺省 max_active_leases：上限与槽位均为 null（保持旧行为）
    plain = make_session()
    plain_st = client.get(f"/sessions/{plain['id']}").json()
    assert plain_st["max_active_leases"] is None
    assert plain_st["active_leases"] == 0


def test_invalid_max_active_leases_rejected():
    assert create_session_raw(
        {"total_chunks": 3, "max_active_leases": 0}).status_code == 422
    assert create_session_raw(
        {"total_chunks": 3, "max_active_leases": -1}).status_code == 422


def test_batch_lease_claims_in_index_order():
    s = make_capped_session(total_chunks=5, max_active_leases=10)
    r = batch_lease(s["id"], 3, ttl=120)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["requested"] == 3
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1, 2]
    assert len({g["lease_token"] for g in body["granted"]}) == 3  # 令牌各自不同
    assert body["active"] == 3
    assert body["blocked"] == 0
    assert body["max_active_leases"] == 10
    assert body["remaining_slots"] == 7
    # 同批租约采用统一时长，过期时间一致
    expires = {g["expires_at"] for g in body["granted"]}
    assert len(expires) == 1

    # 第二次批量领取跳过已占用分片，按序号继续向后领取
    r2 = batch_lease(s["id"], 10)
    assert [g["chunk_index"] for g in r2.json()["granted"]] == [3, 4]
    assert r2.json()["active"] == 5
    assert r2.json()["remaining_slots"] == 5

    # 状态查询中的有效租约数同步更新
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["active_leases"] == 5


def test_batch_lease_respects_remaining_slots():
    s = make_capped_session(total_chunks=6, max_active_leases=2)
    # 首批受槽位限制，只发 2 个，尽管请求 5 个、可领取 6 个
    r = batch_lease(s["id"], 5)
    body = r.json()
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1]
    assert body["active"] == 2
    assert body["remaining_slots"] == 0
    assert body["blocked"] == 0
    # 槽位被占满且仍有未完成分片等待：next_expiry 给出最早过期时间
    assert body["next_expiry"] is not None
    expiry = datetime.fromisoformat(body["next_expiry"])
    assert expiry > datetime.now(timezone.utc)

    # 槽位占满：返回空列表与汇总，不新增租约
    r2 = batch_lease(s["id"], 3)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["granted"] == []
    assert body2["active"] == 2
    assert body2["remaining_slots"] == 0
    assert body2["blocked"] == 0
    assert body2["next_expiry"] == body["next_expiry"]

    # 释放一个分片后，批量领取只占用新腾出的 1 个槽位
    t0 = next(g["lease_token"] for g in body["granted"] if g["chunk_index"] == 0)
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r3 = batch_lease(s["id"], 3)
    assert [g["chunk_index"] for g in r3.json()["granted"]] == [2]
    assert r3.json()["active"] == 2
    assert r3.json()["remaining_slots"] == 0


def test_batch_lease_treats_expired_leases_as_free():
    s = make_capped_session(total_chunks=3, max_active_leases=2)
    assert len(batch_lease(s["id"], 2).json()["granted"]) == 2
    # 槽位占满
    assert batch_lease(s["id"], 1).json()["granted"] == []

    # 一个租约过期：同时释放分片资格与并发槽位
    expire_lease(s["id"], 0)
    r = batch_lease(s["id"], 2)
    body = r.json()
    assert [g["chunk_index"] for g in body["granted"]] == [0]
    assert body["active"] == 2  # 过期租约不计入，新发放后重新占满


def test_batch_lease_skips_blocked_dependencies():
    # 0 可领取；1 依赖 0；2 无依赖；3 依赖 1
    s = make_capped_session(total_chunks=4, max_active_leases=10,
                            dependencies={1: [0], 3: [1]})
    r = batch_lease(s["id"], 10)
    body = r.json()
    # 仅前置已完成（此处为无依赖）的 0、2 可领取，按序号升序
    assert [g["chunk_index"] for g in body["granted"]] == [0, 2]
    assert body["blocked"] == 2  # 1、3 被依赖阻塞

    # 完成 0 后，1 解锁可领取；3 仍被 1 阻塞
    t0 = next(g["lease_token"] for g in body["granted"] if g["chunk_index"] == 0)
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r2 = batch_lease(s["id"], 10)
    body2 = r2.json()
    assert [g["chunk_index"] for g in body2["granted"]] == [1]
    assert body2["blocked"] == 1

    # 无依赖图、全部分片已被领取/完成：空列表 + blocked=0、next_expiry=null 时也正常
    s2 = make_capped_session(total_chunks=1, max_active_leases=5)
    assert len(batch_lease(s2["id"], 1).json()["granted"]) == 1
    tail = batch_lease(s2["id"], 1).json()
    assert tail["granted"] == []
    assert tail["blocked"] == 0
    assert tail["active"] == 1
    assert tail["next_expiry"] is not None


def test_batch_lease_without_cap_behaves_like_unlimited():
    # 旧语义会话（max_active_leases 缺省）：批量领取不受并发上限约束
    s = make_session(total_chunks=4)
    r = batch_lease(s["id"], 10)
    body = r.json()
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1, 2, 3]
    assert body["max_active_leases"] is None
    assert body["remaining_slots"] is None
    assert batch_lease(s["id"], 1).json()["granted"] == []


def test_single_lease_enforces_session_cap():
    # 单片接口不得绕过会话并发上限
    s = make_capped_session(total_chunks=4, max_active_leases=2)
    assert lease(s["id"], 0).status_code == 200
    assert lease(s["id"], 1).status_code == 200
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["max_active_leases"] == 2
    assert r.json()["active_leases"] == 2

    # 租约过期后单片领取同样把过期租约视为空闲槽位
    expire_lease(s["id"], 0)
    assert lease(s["id"], 2).status_code == 200


def test_batch_and_single_leases_compete_atomically_under_concurrency():
    s = make_capped_session(total_chunks=10, max_active_leases=4)
    results: list[list[int]] = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(4)

    def worker(kind: str):
        barrier.wait()  # 尽量同时发起，迫使临界区串行化资格计算与写入
        if kind == "batch":
            granted = [g["chunk_index"]
                       for g in batch_lease(s["id"], 3).json()["granted"]]
        else:
            r = lease(s["id"], 0)  # 具体序号无关，只关心是否占用到唯一分片
            granted = []
            if r.status_code == 200:
                # 单片接口只能拿到 0；若已被批量领取则应为 409
                granted = [0]
        with results_lock:
            results.append(granted)

    threads = [
        threading.Thread(target=worker, args=("batch",)),
        threading.Thread(target=worker, args=("batch",)),
        threading.Thread(target=worker, args=("single",)),
        threading.Thread(target=worker, args=("batch",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed = [idx for granted in results for idx in granted]
    # 不重复分配
    assert len(claimed) == len(set(claimed))
    # 不突破并发上限
    assert len(claimed) <= 4
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["active_leases"] == len(claimed)
    assert st["active_leases"] <= 4


def test_batch_lease_request_validation():
    s = make_session()
    assert client.post(
        f"/sessions/{s['id']}/leases/batch", json={"count": 0}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/leases/batch", json={"count": -1}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/leases/batch", json={}).status_code == 422
    assert client.post(
        "/sessions/nope/leases/batch", json={"count": 1}).status_code == 404


def test_batch_lease_blocked_after_finalize():
    s = make_capped_session(total_chunks=1, max_active_leases=2)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
    assert batch_lease(s["id"], 1).status_code == 409


def test_migration_adds_max_active_leases_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy.db"
    conn = sqlite3.connect(legacy)
    # 旧版本 SCHEMA：sessions 无 max_active_leases 列
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        CREATE TABLE chunk_dependencies (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            depends_on INTEGER NOT NULL,
            PRIMARY KEY (session_id, chunk_index, depends_on)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-sid', 2, 'dig', 'sha256', 'active', '2026-01-01T00:00:00+00:00');
        INSERT INTO chunks (session_id, chunk_index) VALUES ('legacy-sid', 0), ('legacy-sid', 1);
        """
    )
    conn.commit()
    conn.close()

    # 通过独立的连接对象复用迁移逻辑（不影响进程级测试连接）
    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    assert "max_active_leases" in cols
    # 旧会话上限为 NULL：迁移幂等且保持“不限并发”的旧行为
    row = migrated.execute(
        "SELECT max_active_leases FROM sessions WHERE id = 'legacy-sid'"
    ).fetchone()
    assert row[0] is None
    db.run_migrations(migrated)  # 再次执行不报错
    migrated.close()


# ---------------------------------------------------------------- 失败上报与退避重试

def fail(session_id, index, token):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/fail",
        json={"lease_token": token},
    )


def set_backoff(session_id, index, when):
    """直接写退避截止时间，避免真实等待。when 为 ISO 字符串或 None。"""
    get_conn().execute(
        "UPDATE chunks SET backoff_until = ? WHERE session_id = ? AND chunk_index = ?",
        (when, session_id, index),
    )
    get_conn().commit()


PAST = "2000-01-01T00:00:00+00:00"
FUTURE = "2999-01-01T00:00:00+00:00"


def make_retry_session(total_chunks=3, base_retry_seconds=5, max_retry_seconds=300,
                       max_active_leases=None):
    payload = {
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "base_retry_seconds": base_retry_seconds,
        "max_retry_seconds": max_retry_seconds,
    }
    if max_active_leases is not None:
        payload["max_active_leases"] = max_active_leases
    resp = client.post("/sessions", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease_fail_retry(session_id, index):
    """领取→失败上报；返回 fail 响应（此时分片处于冷却中）。"""
    token = lease(session_id, index).json()["lease_token"]
    r = fail(session_id, index, token)
    assert r.status_code == 200, r.text
    return r.json()


def test_session_retry_defaults_persisted():
    s = make_session()
    assert s["base_retry_seconds"] == 5
    assert s["max_retry_seconds"] == 300
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["base_retry_seconds"] == 5
    assert st["max_retry_seconds"] == 300
    # 新分片失败计数为 0、无冷却
    assert st["chunks"][0]["failure_count"] == 0
    assert st["chunks"][0]["retry_after"] is None


def test_retry_params_validation():
    ok = {"total_chunks": 2, "file_digest": "f" * 64}
    assert client.post("/sessions", json={**ok, "base_retry_seconds": 0}).status_code == 422
    assert client.post("/sessions", json={**ok, "max_retry_seconds": 0}).status_code == 422
    assert client.post("/sessions", json={**ok, "base_retry_seconds": -1}).status_code == 422
    # max 必须不小于 base
    assert client.post(
        "/sessions", json={**ok, "base_retry_seconds": 10, "max_retry_seconds": 9}
    ).status_code == 422
    # 相等合法
    assert client.post(
        "/sessions", json={**ok, "base_retry_seconds": 10, "max_retry_seconds": 10}
    ).status_code == 201


def test_fail_requires_valid_lease_token():
    s = make_session()
    # 未持有租约
    assert fail(s["id"], 0, "no-such-token").status_code == 409
    # 令牌错误
    token = lease(s["id"], 0).json()["lease_token"]
    assert fail(s["id"], 0, "wrong-token").status_code == 409
    # 租约过期后不能上报失败
    expire_lease(s["id"], 0)
    assert fail(s["id"], 0, token).status_code == 409
    # 未知会话 / 分片
    assert fail("nope", 0, "t").status_code == 404
    assert fail(s["id"], 99, "t").status_code == 404


def test_fail_cannot_report_completed_or_finalized():
    s = make_session()
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    assert fail(s["id"], 0, token).status_code == 409  # 已完成不可失败

    s2 = make_session(total_chunks=1)
    t = lease(s2["id"], 0).json()["lease_token"]
    assert complete(s2["id"], 0, t, "0" * 64).status_code == 200
    assert client.post(f"/sessions/{s2['id']}/finalize").status_code == 200
    assert fail(s2["id"], 0, t).status_code == 409  # 已提交会话只读


def test_fail_clears_lease_and_increments_failures_atomically():
    s = make_retry_session()

    body = lease_fail_retry(s["id"], 0)
    assert body["status"] == "pending"
    assert body["failure_count"] == 1
    assert body["retry_after_seconds"] == 5  # base × 2^0

    # 租约已被原子清除：冷却期间重新领取被拒，旧令牌不能再完成
    assert lease(s["id"], 0).status_code == 409
    assert complete(s["id"], 0, "whatever-token", "0" * 64).status_code == 409
    st = client.get(f"/sessions/{s['id']}").json()
    c0 = st["chunks"][0]
    assert c0["status"] == "pending"
    assert c0["lease_expires_at"] is None
    assert c0["failure_count"] == 1
    assert c0["retry_after"] is not None
    assert st["active_leases"] == 0

    # 第二次失败：计数 2，冷却 10 秒
    set_backoff(s["id"], 0, PAST)
    body2 = lease_fail_retry(s["id"], 0)
    assert body2["failure_count"] == 2
    assert body2["retry_after_seconds"] == 10
    assert datetime.fromisoformat(body2["retry_after"]) > datetime.now(timezone.utc)


def test_exponential_backoff_is_capped_at_max():
    s = make_retry_session(base_retry_seconds=2, max_retry_seconds=10)
    expected = [2, 4, 8, 10, 10]  # 2×2^(n-1)，封顶 10
    for n, delay in enumerate(expected, start=1):
        body = lease_fail_retry(s["id"], 0)
        assert body["failure_count"] == n
        assert body["retry_after_seconds"] == delay
        set_backoff(s["id"], 0, PAST)  # 冷却结束才能再次领取

    # 默认参数下第 7 次起封顶 300
    s2 = make_retry_session()
    delays = []
    for _ in range(7):
        delays.append(lease_fail_retry(s2["id"], 0)["retry_after_seconds"])
        set_backoff(s2["id"], 0, PAST)
    assert delays == [5, 10, 20, 40, 80, 160, 300]


def test_cooling_chunk_not_claimable_single_or_batch():
    s = make_retry_session(total_chunks=3)
    lease_fail_retry(s["id"], 0)  # 分片 0 进入冷却

    # 单片领取：409 且响应体带 failure_count / retry_after
    r = lease(s["id"], 0)
    assert r.status_code == 409
    assert r.json()["failure_count"] == 1
    assert r.json()["retry_after"] is not None

    # /ready：0 既不在 ready 也不在 blocked，而在 cooling_chunks
    rdy = ready(s["id"])
    assert rdy["ready_chunks"] == [1, 2]
    assert rdy["blocked_chunks"] == []
    assert len(rdy["cooling_chunks"]) == 1
    cooling0 = rdy["cooling_chunks"][0]
    assert cooling0["chunk_index"] == 0
    assert cooling0["failure_count"] == 1
    assert datetime.fromisoformat(cooling0["retry_after"]) > datetime.now(timezone.utc)

    # 批量领取跳过冷却分片
    body = batch_lease(s["id"], 10).json()
    assert [g["chunk_index"] for g in body["granted"]] == [1, 2]

    # 会话状态：冷却中的分片 pending + retry_after
    st = client.get(f"/sessions/{s['id']}").json()
    c0 = st["chunks"][0]
    assert c0["status"] == "pending"
    assert c0["failure_count"] == 1
    assert c0["retry_after"] is not None


def test_chunk_claimable_again_after_backoff_expires():
    s = make_retry_session(total_chunks=2)
    lease_fail_retry(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == [1]
    assert [g["chunk_index"] for g in batch_lease(s["id"], 10).json()["granted"]] == [1]

    # 冷却结束：单片、批量均可再次领取，/ready 冷却列表清空
    set_backoff(s["id"], 0, PAST)
    rdy = ready(s["id"])
    assert 0 in rdy["ready_chunks"]
    assert rdy["cooling_chunks"] == []
    assert lease(s["id"], 0).status_code == 200

    # 状态读时口径：过期退避不再报告 retry_after，但失败次数保留
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["chunks"][0]["retry_after"] is None
    assert st["chunks"][0]["failure_count"] == 1


def test_complete_clears_backoff_but_keeps_failure_count():
    s = make_retry_session()
    lease_fail_retry(s["id"], 0)
    set_backoff(s["id"], 0, PAST)

    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200

    st = client.get(f"/sessions/{s['id']}").json()
    c0 = st["chunks"][0]
    assert c0["status"] == "completed"
    assert c0["retry_after"] is None       # 退避状态已清除
    assert c0["failure_count"] == 1        # 失败次数保留


def test_batch_next_expiry_is_min_of_lease_and_backoff():
    # 分片 0 失败进入冷却；分片 1 持有效租约（TTL 300）
    s = make_retry_session(total_chunks=2)
    lease_fail_retry(s["id"], 0)
    token1 = lease(s["id"], 1, ttl=300).json()["lease_token"]

    # 冷却截止推到很远：next_expiry 取租约过期时间
    set_backoff(s["id"], 0, FUTURE)
    body = batch_lease(s["id"], 10).json()
    assert body["granted"] == []
    st = client.get(f"/sessions/{s['id']}").json()
    lease_expiry = st["chunks"][1]["lease_expires_at"]
    assert datetime.fromisoformat(body["next_expiry"]) == datetime.fromisoformat(lease_expiry)

    # 租约过期时间推得比冷却更远：next_expiry 取退避结束时间
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = ? WHERE session_id = ? AND chunk_index = 1",
        (FUTURE, s["id"]),
    )
    get_conn().commit()
    backoff_end = "2027-01-01T00:00:00+00:00"
    set_backoff(s["id"], 0, backoff_end)
    body = batch_lease(s["id"], 10).json()
    assert datetime.fromisoformat(body["next_expiry"]) == datetime.fromisoformat(backoff_end)

    # 分片 1 完成：仅余冷却中的 0、无有效租约，next_expiry 即退避结束时间
    assert complete(s["id"], 1, token1, "1" * 64).status_code == 200
    body = batch_lease(s["id"], 10).json()
    assert body["granted"] == []
    assert datetime.fromisoformat(body["next_expiry"]) == datetime.fromisoformat(backoff_end)

    # 0 冷却结束后领取并完成：无未完成分片，也无任何等待事件，next_expiry 为 null
    set_backoff(s["id"], 0, PAST)
    token0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token0, "0" * 64).status_code == 200
    body = batch_lease(s["id"], 10).json()
    assert body["granted"] == []
    assert body["next_expiry"] is None


def test_failure_releases_concurrency_slot():
    s = make_retry_session(total_chunks=2, max_active_leases=1)
    lease_fail_retry(s["id"], 0)  # 失败清除租约：槽位释放，但 0 冷却中
    # 单片接口受冷却拦截
    assert lease(s["id"], 0).status_code == 409
    # 槽位已空：另一个分片可领取；批量同样只发 1
    assert lease(s["id"], 1).status_code == 200
    assert batch_lease(s["id"], 1).json()["granted"] == []


def test_migration_adds_failure_and_retry_columns_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy2.db"
    conn = sqlite3.connect(legacy)
    # 0001 之后、本次改动之前的旧 SCHEMA：已有 max_active_leases，缺退避相关列
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-sid', 1, 'dig', 'sha256', 'active',
                    '2026-01-01T00:00:00+00:00');
        INSERT INTO chunks (session_id, chunk_index) VALUES ('legacy-sid', 0);
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    session_cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    chunk_cols = {r[1] for r in migrated.execute("PRAGMA table_info(chunks)").fetchall()}
    assert {"base_retry_seconds", "max_retry_seconds"} <= session_cols
    assert {"failure_count", "backoff_until"} <= chunk_cols

    # 旧会话取默认退避参数；旧分片失败计数 0、无冷却
    row = migrated.execute(
        "SELECT base_retry_seconds, max_retry_seconds FROM sessions WHERE id = 'legacy-sid'"
    ).fetchone()
    assert tuple(row) == (5, 300)
    row = migrated.execute(
        "SELECT failure_count, backoff_until FROM chunks"
        " WHERE session_id = 'legacy-sid' AND chunk_index = 0"
    ).fetchone()
    assert tuple(row) == (0, None)

    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()


# ---------------------------------------------------------------- 完成纠错回退（reopen）

def reopen(session_id, index, token, digest):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/reopen",
        json={"lease_token": token, "expected_digest": digest},
    )


def lease_complete(session_id, index, digest=None):
    """领取并立即完成分片；返回完成时保留的租约令牌与摘要。"""
    token = lease(session_id, index).json()["lease_token"]
    digest = digest if digest is not None else str(index) * 64
    assert complete(session_id, index, token, digest).status_code == 200
    return token, digest


def test_reopen_resets_target_and_transitive_downstream_atomically():
    # 依赖图：1←0 不相关；2→1；3→1；4→2；5 无依赖
    s = make_session_with_deps(6, {2: [1], 3: [1], 4: [2]})

    # 分片 0：先失败一次再完成（失败次数应跨完成/回退保留，且 0 不在回退范围内）
    lease_fail_retry(s["id"], 0)
    set_backoff(s["id"], 0, PAST)
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)

    t1, d1 = lease_complete(s["id"], 1, "1" * 64)
    lease_complete(s["id"], 2, "2" * 64)
    t3 = lease(s["id"], 3).json()["lease_token"]      # 下游持有有效租约
    lease_fail_retry(s["id"], 4)                       # 下游处于退避冷却中
    # 分片 5 保持 pending，与回退无关

    st0 = client.get(f"/sessions/{s['id']}").json()
    assert st0["revision"] == 0

    r = reopen(s["id"], 1, t1, d1)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["chunk_index"] == 1
    assert body["revision"] == 1
    # 目标 + 全部传递下游，按（拓扑层级, 分片序号）升序：1→0 层，2/3→1 层，4→2 层
    assert body["affected_chunks"] == [
        {"chunk_index": 1, "level": 0},
        {"chunk_index": 2, "level": 1},
        {"chunk_index": 3, "level": 1},
        {"chunk_index": 4, "level": 2},
    ]

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["revision"] == 1  # 成功回退递增一次
    chunks = {c["chunk_index"]: c for c in st["chunks"]}

    # 未被波及的分片保持原状
    assert chunks[0]["status"] == "completed"
    assert chunks[0]["chunk_digest"] == d0
    assert chunks[0]["failure_count"] == 1
    assert chunks[5]["status"] == "pending"

    # 目标与下游：状态、摘要、完成时间、租约、退避全部清除
    for idx in (1, 2, 3, 4):
        c = chunks[idx]
        assert c["status"] == "pending"
        assert c["chunk_digest"] is None
        assert c["completed_at"] is None
        assert c["lease_expires_at"] is None
        assert c["retry_after"] is None
    # 累计失败次数保留
    assert chunks[4]["failure_count"] == 1
    assert chunks[1]["failure_count"] == 0

    # 旧完成凭据全部失效：完成时保留的令牌、下游在途租约都不能再上报
    assert complete(s["id"], 1, t1, d1).status_code == 409
    assert complete(s["id"], 3, t3, "3" * 64).status_code == 409

    # 回退后可按依赖顺序重新领取、重新完成并最终提交
    assert lease(s["id"], 2).status_code == 409  # 前置 1 已回到 pending
    _, nd1 = lease_complete(s["id"], 1, "a" * 64)
    lease_complete(s["id"], 2, "b" * 64)
    lease_complete(s["id"], 3, "c" * 64)
    lease_complete(s["id"], 4, "d" * 64)
    lease_complete(s["id"], 5, "e" * 64)
    fin = client.post(f"/sessions/{s['id']}/finalize")
    assert fin.status_code == 200
    assert [m["chunk_digest"] for m in fin.json()["manifest"]] == [
        d0, "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64,
    ]


def test_reopen_topological_levels_with_diamond_and_skip_edges():
    # 菱形 + 跨层边：5 直接依赖 0，但经 4 的路径更长，层级取最长路径
    s = make_session_with_deps(7, {1: [0], 2: [0], 3: [1, 2], 4: [3], 5: [0, 4]})
    tokens = {}
    for i in range(6):  # 0..5 全部完成，6 保持 pending
        tokens[i] = lease_complete(s["id"], i)

    r = reopen(s["id"], 0, tokens[0][0], tokens[0][1])
    assert r.status_code == 200, r.text
    assert r.json()["affected_chunks"] == [
        {"chunk_index": 0, "level": 0},
        {"chunk_index": 1, "level": 1},
        {"chunk_index": 2, "level": 1},
        {"chunk_index": 3, "level": 2},
        {"chunk_index": 4, "level": 3},
        {"chunk_index": 5, "level": 4},  # 直接边 0→5 不压扁层级
    ]
    # 无依赖的 6 不受影响
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["chunks"][6]["status"] == "pending"
    assert st["revision"] == 1


def test_reopen_requires_completed_chunk_and_matching_credentials():
    s = make_session_with_deps(3, {1: [0], 2: [1]})
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    t1, d1 = lease_complete(s["id"], 1, "1" * 64)

    # 目标分片未完成：pending / leased 均不可回退
    assert reopen(s["id"], 2, "any", "any").status_code == 409
    lease(s["id"], 2)
    assert reopen(s["id"], 2, "any", "any").status_code == 409

    # 令牌不匹配 / 摘要不匹配
    assert reopen(s["id"], 1, "wrong-token", d1).status_code == 409
    assert reopen(s["id"], 1, t1, "9" * 64).status_code == 409
    # 凭据属于别的分片同样不匹配
    assert reopen(s["id"], 1, t0, d0).status_code == 409

    # 校验失败不得部分回退：状态与修订号保持原样
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["revision"] == 0
    assert [c["status"] for c in st["chunks"]] == ["completed", "completed", "leased"]
    assert st["chunks"][1]["chunk_digest"] == d1

    # 未知会话 / 分片
    assert reopen("nope", 0, "t", "d").status_code == 404
    assert reopen(s["id"], 99, "t", "d").status_code == 404

    # 缺字段 / 空串：请求校验
    assert client.post(
        f"/sessions/{s['id']}/chunks/1/reopen", json={"lease_token": t1}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/chunks/1/reopen",
        json={"lease_token": "", "expected_digest": d1}).status_code == 422


def test_reopen_rejected_after_finalize():
    s = make_session_with_deps(2, {1: [0]})
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    lease_complete(s["id"], 1, "1" * 64)
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200

    assert reopen(s["id"], 0, t0, d0).status_code == 409
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["status"] == "finalized"
    assert st["revision"] == 0
    assert all(c["status"] == "completed" for c in st["chunks"])


def test_revision_increments_once_per_successful_reopen():
    s = make_session_with_deps(3, {1: [0], 2: [0]})
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    lease_complete(s["id"], 1, "1" * 64)
    lease_complete(s["id"], 2, "2" * 64)

    # 失败尝试不递增
    assert reopen(s["id"], 0, "bad", d0).status_code == 409
    assert client.get(f"/sessions/{s['id']}").json()["revision"] == 0

    # 每次成功回退恰好递增 1
    r1 = reopen(s["id"], 0, t0, d0)
    assert r1.status_code == 200 and r1.json()["revision"] == 1
    assert client.get(f"/sessions/{s['id']}").json()["revision"] == 1

    t0b, d0b = lease_complete(s["id"], 0, "f" * 64)
    r2 = reopen(s["id"], 0, t0b, d0b)
    assert r2.status_code == 200 and r2.json()["revision"] == 2
    assert client.get(f"/sessions/{s['id']}").json()["revision"] == 2


def test_reopen_without_dependencies_only_resets_target():
    s = make_session()
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    lease_complete(s["id"], 1, "1" * 64)

    r = reopen(s["id"], 0, t0, d0)
    assert r.status_code == 200
    assert r.json()["affected_chunks"] == [{"chunk_index": 0, "level": 0}]

    st = client.get(f"/sessions/{s['id']}").json()
    assert [c["status"] for c in st["chunks"]] == ["pending", "completed", "pending"]


def test_reopen_and_concurrent_lease_never_observe_intermediate_state():
    s = make_session_with_deps(2, {1: [0]})
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    lease_complete(s["id"], 1, "1" * 64)

    barrier = threading.Barrier(3)
    lease_results: list[tuple[int, int]] = []  # (chunk_index, status_code)
    results_lock = threading.Lock()

    def do_reopen():
        barrier.wait()
        r = reopen(s["id"], 0, t0, d0)
        assert r.status_code == 200

    def do_lease(index):
        barrier.wait()
        r = lease(s["id"], index)
        with results_lock:
            lease_results.append((index, r.status_code))

    threads = [
        threading.Thread(target=do_reopen),
        threading.Thread(target=do_lease, args=(0,)),
        threading.Thread(target=do_lease, args=(1,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 领取要么在回退前（409 已完成）、要么在回退后（200 可领取），无中间态
    assert {idx for idx, _ in lease_results} == {0, 1}
    assert all(code in (200, 409) for _, code in lease_results)
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["revision"] == 1
    for c in st["chunks"]:
        if c["status"] == "leased":
            # 只可能是回退后新领取的：无摘要、无完成时间
            assert c["chunk_digest"] is None
            assert c["completed_at"] is None
        else:
            assert c["status"] == "pending"
            assert c["chunk_digest"] is None


def test_migration_adds_revision_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy3.db"
    conn = sqlite3.connect(legacy)
    # 0003 之后、本次改动之前的旧 SCHEMA：sessions 无 revision 列
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
            base_retry_seconds INTEGER NOT NULL DEFAULT 5,
            max_retry_seconds INTEGER NOT NULL DEFAULT 300,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0, backoff_until TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-sid', 1, 'dig', 'sha256', 'active',
                    '2026-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    assert "revision" in cols
    # 既有会话修订号从 0 开始
    row = migrated.execute(
        "SELECT revision FROM sessions WHERE id = 'legacy-sid'"
    ).fetchone()
    assert row[0] == 0
    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()


# ---------------------------------------------------------------- Merkle 完整性校验（sha256-merkle-v1）

def merkle_root_of(digests):
    """按字节级规则独立实现的参考计算：叶 SHA-256(0x00||raw32)，
    父 SHA-256(0x01||左32||右32)，奇数末节点复制自身，逐层归约。"""
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest() for d in digests]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])
        level = [
            hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest()
            for i in range(0, len(level), 2)
        ]
    return level[0].hex()


def chunk_digests(total):
    """生成确定性的分片摘要：sha256(b"chunk-<i>") 的小写十六进制。"""
    return [hashlib.sha256(f"chunk-{i}".encode()).hexdigest() for i in range(total)]


def make_merkle_session(total_chunks, file_digest):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256-merkle-v1",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def complete_all(session_id, digests):
    """依次领取并完成全部分片；返回 {chunk_index: (lease_token, digest)}。"""
    creds = {}
    for i, digest in enumerate(digests):
        token = lease(session_id, i).json()["lease_token"]
        assert complete(session_id, i, token, digest).status_code == 200
        creds[i] = (token, digest)
    return creds


def test_merkle_session_requires_lowercase_hex_file_digest():
    for bad in ("F" * 64, "f" * 63, "f" * 65, "g" * 64, "0" * 64 + " ", ""):
        resp = client.post("/sessions", json={
            "total_chunks": 2,
            "file_digest": bad,
            "digest_algorithm": "sha256-merkle-v1",
        })
        assert resp.status_code == 422, bad

    ok = client.post("/sessions", json={
        "total_chunks": 2,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256-merkle-v1",
    })
    assert ok.status_code == 201
    assert ok.json()["digest_algorithm"] == "sha256-merkle-v1"

    # 非该算法：file_digest 格式不受限，保持原行为
    plain = client.post("/sessions", json={
        "total_chunks": 2, "file_digest": "anything-goes", "digest_algorithm": "sha256",
    })
    assert plain.status_code == 201


def test_merkle_status_exposes_digest_algorithm():
    s = make_merkle_session(2, "f" * 64)
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["digest_algorithm"] == "sha256-merkle-v1"


def test_merkle_complete_requires_lowercase_hex_chunk_digest():
    s = make_merkle_session(2, "f" * 64)
    token = lease(s["id"], 0).json()["lease_token"]
    for bad in ("A" * 64, "b" * 63, "b" * 65, "zz" * 32, "not-hex"):
        r = complete(s["id"], 0, token, bad)
        assert r.status_code == 422, bad
    # 合法格式正常完成；幂等重放不受影响
    assert complete(s["id"], 0, token, "b" * 64).status_code == 200
    r = complete(s["id"], 0, token, "b" * 64)
    assert r.status_code == 200 and r.json()["idempotent_replay"] is True

    # 非该算法会话：任意摘要格式均可上报（原行为）
    s2 = make_session()
    t2 = lease(s2["id"], 0).json()["lease_token"]
    assert complete(s2["id"], 0, t2, "free-form-digest").status_code == 200


def test_merkle_finalize_verifies_root_and_is_idempotent():
    digests = chunk_digests(4)
    s = make_merkle_session(4, merkle_root_of(digests))
    complete_all(s["id"], digests)

    ok = client.post(f"/sessions/{s['id']}/finalize")
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert body["status"] == "finalized"
    assert body["digest_algorithm"] == "sha256-merkle-v1"
    assert [m["chunk_digest"] for m in body["manifest"]] == digests

    # 重复提交幂等返回同一清单
    again = client.post(f"/sessions/{s['id']}/finalize")
    assert again.status_code == 200
    assert again.json()["manifest"] == body["manifest"]
    assert again.json()["finalized_at"] == body["finalized_at"]


def test_merkle_finalize_known_vectors():
    # 硬编码已知向量（独立计算）：防止奇数复制/域分离字节规则被改动
    vectors = {
        ("00" * 32,): "7f9c9e31ac8256ca2f258583df262dbc7d6f68f2a03043d5c99a4ae5a7396ce9",
        ("00" * 32, "11" * 32, "22" * 32):
            "d09bdfda1282f5b832eb6afd304f68a7249245148a5f8224289701572e241122",
        ("00" * 32, "11" * 32, "22" * 32, "33" * 32):
            "23cddd0a87e9f0e8b557376ed3156858efd19c6666db3f1e176db6b9546e0a5e",
    }
    for digests, root in vectors.items():
        assert merkle_root_of(list(digests)) == root
        s = make_merkle_session(len(digests), root)
        complete_all(s["id"], list(digests))
        assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200


def test_merkle_finalize_odd_chunk_count_duplicates_last():
    # 3、5 个分片：奇数层末节点复制自身后配对（参考实现与已知向量一致）
    for total in (3, 5):
        digests = chunk_digests(total)
        s = make_merkle_session(total, merkle_root_of(digests))
        complete_all(s["id"], digests)
        assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200


def test_merkle_finalize_mismatch_returns_409_and_session_stays_active():
    digests = chunk_digests(3)
    wrong_root = "0" * 64
    assert wrong_root != merkle_root_of(digests)
    s = make_merkle_session(3, wrong_root)
    complete_all(s["id"], digests)

    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    body = r.json()
    assert body["computed_digest"] == merkle_root_of(digests)

    # 会话保持 active：不写入任何提交状态，重复提交得到同样的 409
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["status"] == "active"
    assert st["finalized_at"] is None
    r2 = client.post(f"/sessions/{s['id']}/finalize")
    assert r2.status_code == 409
    assert r2.json()["computed_digest"] == body["computed_digest"]


def test_merkle_finalize_mismatch_then_reopen_and_fix():
    # 分片 1 上报了错误摘要：先 409，reopen 纠错后重新完成即可提交
    digests = chunk_digests(3)
    s = make_merkle_session(3, merkle_root_of(digests))
    bad = digests.copy()
    bad[1] = hashlib.sha256(b"corrupted").hexdigest()
    creds = complete_all(s["id"], bad)

    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["computed_digest"] == merkle_root_of(bad)

    # 凭完成时保留的令牌与摘要回退分片 1，重新上报正确摘要
    token1, digest1 = creds[1]
    assert reopen(s["id"], 1, token1, digest1).status_code == 200
    new_token = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, new_token, digests[1]).status_code == 200

    ok = client.post(f"/sessions/{s['id']}/finalize")
    assert ok.status_code == 200
    assert [m["chunk_digest"] for m in ok.json()["manifest"]] == digests


def test_merkle_finalize_single_chunk():
    digest = chunk_digests(1)
    # 单分片：根即叶节点 SHA-256(0x00 || 摘要原始32字节)
    s = make_merkle_session(1, merkle_root_of(digest))
    complete_all(s["id"], digest)
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200


def test_non_merkle_session_finalize_skips_verification():
    # 非 sha256-merkle-v1：finalize 不校验摘要，保持原行为
    s = make_session(total_chunks=2, file_digest="not-a-merkle-root")
    t0 = lease(s["id"], 0).json()["lease_token"]
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 0, t0, "whatever").status_code == 200
    assert complete(s["id"], 1, t1, "digests").status_code == 200
    ok = client.post(f"/sessions/{s['id']}/finalize")
    assert ok.status_code == 200
    assert ok.json()["file_digest"] == "not-a-merkle-root"


# ---------------------------------------------------------------- 进度分支（fork）

def fork(session_id, expected_revision, request_id, reuse_chunks):
    return client.post(
        f"/sessions/{session_id}/fork",
        json={
            "expected_revision": expected_revision,
            "request_id": request_id,
            "reuse_chunks": reuse_chunks,
        },
    )


def fork_deps_session(total_chunks=4, dependencies=None):
    """默认依赖图：1 依赖 0；2 依赖 0、1；3 无依赖。"""
    if dependencies is None:
        dependencies = {1: [0], 2: [0, 1]}
    return make_session_with_deps(total_chunks, dependencies)


def test_status_exposes_source_session_id_null_for_original():
    s = make_session()
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["source_session_id"] is None


def test_fork_copies_completed_progress_as_completed_rest_pending():
    # 依赖图：1 依赖 0；2 依赖 0、1；3 无依赖
    s = fork_deps_session()
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    t1, d1 = lease_complete(s["id"], 1, "1" * 64)
    # 分片 2 持有有效租约、分片 3 失败后退避；这些运行状态都不应被复制
    lease(s["id"], 2)
    lease_fail_retry(s["id"], 3)

    r = fork(s["id"], 0, "req-1", [0, 1])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "active"
    assert body["revision"] == 0
    assert body["source_session_id"] == s["id"]
    assert body["reused_chunks"] == [0, 1]
    assert body["pending_chunks"] == [2, 3]
    assert body["idempotent_replay"] is False
    new_id = body["id"]
    assert new_id != s["id"]

    st = client.get(f"/sessions/{new_id}").json()
    assert st["source_session_id"] == s["id"]
    assert st["revision"] == 0
    assert st["completed_chunks"] == 2
    assert st["missing_chunks"] == [2, 3]
    chunks = {c["chunk_index"]: c for c in st["chunks"]}

    # 复用分片：摘要与完成时间原样复制为 completed
    assert chunks[0]["status"] == "completed"
    assert chunks[0]["chunk_digest"] == d0
    assert chunks[1]["status"] == "completed"
    assert chunks[1]["chunk_digest"] == d1
    src_chunks01 = {c["chunk_index"]: c for c in
                    client.get(f"/sessions/{s['id']}").json()["chunks"]}
    assert chunks[0]["completed_at"] == src_chunks01[0]["completed_at"]
    assert chunks[1]["completed_at"] == src_chunks01[1]["completed_at"]

    # 其余分片 pending：无租约、无退避、失败次数为 0
    for idx in (2, 3):
        c = chunks[idx]
        assert c["status"] == "pending"
        assert c["lease_expires_at"] is None
        assert c["chunk_digest"] is None
        assert c["completed_at"] is None
        assert c["failure_count"] == 0
        assert c["retry_after"] is None

    # 旧租约令牌在新会话无效：不能凭源会话令牌直接完成
    assert complete(new_id, 2, "any", "x").status_code == 409

    # 依赖图一并复制：分片 2 仍依赖 0、1（已复用为完成），立即可领取
    assert [c["dependencies"] for c in st["chunks"]] == [[], [0], [0, 1], []]
    rdy = client.get(f"/sessions/{new_id}/ready").json()
    # 2 前置已完成、无租约可领取；3 无依赖同样可领取
    assert rdy["ready_chunks"] == [2, 3]
    assert lease(new_id, 2).status_code == 200

    # 源会话保持原状：2 仍持有租约、3 仍在退避
    src = client.get(f"/sessions/{s['id']}").json()
    src_chunks = {c["chunk_index"]: c for c in src["chunks"]}
    assert src_chunks[2]["status"] == "leased"
    assert src_chunks[3]["retry_after"] is not None
    assert src["source_session_id"] is None


def test_fork_copies_session_configuration_and_dependencies():
    resp = client.post("/sessions", json={
        "total_chunks": 3,
        "file_digest": "e" * 64,
        "digest_algorithm": "sha256-merkle-v1",
        "dependencies": {"2": [0, 1]},
        "max_active_leases": 1,
        "base_retry_seconds": 7,
        "max_retry_seconds": 70,
    })
    assert resp.status_code == 201, resp.text
    sid = resp.json()["id"]

    body = fork(sid, 0, "cfg", []).json()
    assert body["file_digest"] == "e" * 64
    assert body["digest_algorithm"] == "sha256-merkle-v1"
    assert body["max_active_leases"] == 1
    assert body["base_retry_seconds"] == 7
    assert body["max_retry_seconds"] == 70
    new_id = body["id"]

    st = client.get(f"/sessions/{new_id}").json()
    assert [c["dependencies"] for c in st["chunks"]] == [[], [], [0, 1]]
    # 复制来的并发上限生效
    assert lease(new_id, 0).status_code == 200
    assert lease(new_id, 1).status_code == 409


def test_fork_rejects_uncompleted_reuse_chunks_without_persisting():
    s = fork_deps_session()
    lease_complete(s["id"], 0, "0" * 64)
    # 1 已领取未完成；2、3 未开始
    lease(s["id"], 1)

    r = fork(s["id"], 0, "bad-1", [0, 1])
    assert r.status_code == 409
    assert r.json()["not_completed_chunks"] == [1]

    r = fork(s["id"], 0, "bad-2", [0, 2, 3])
    assert r.status_code == 409
    assert r.json()["not_completed_chunks"] == [2, 3]

    # 越界（含负数）序号同样按未完成拒绝
    r = fork(s["id"], 0, "bad-3", [0, 4])
    assert r.status_code == 409
    assert r.json()["not_completed_chunks"] == [4]

    # 校验失败不落库：源会话仍可创建其他分支，且不存在新会话记录
    ok = fork(s["id"], 0, "good", [0])
    assert ok.status_code == 201
    rows = get_conn().execute(
        "SELECT COUNT(*) AS n FROM sessions WHERE source_session_id = ?", (s["id"],)
    ).fetchone()
    assert rows["n"] == 1


def test_fork_requires_transitive_prerequisite_closure():
    # 链：3 -> 2 -> 1 -> 0
    s = fork_deps_session(total_chunks=4, dependencies={1: [0], 2: [1], 3: [2]})
    for i in range(4):
        lease_complete(s["id"], i, str(i) * 64)

    # 直接复用 2 但缺少前置 0、1（含传递前置 0）
    r = fork(s["id"], 0, "closure-1", [2])
    assert r.status_code == 409
    assert r.json()["missing_prerequisites"] == [0, 1]

    # 复用 2、1 但仍缺传递前置 0
    r = fork(s["id"], 0, "closure-2", [1, 2])
    assert r.status_code == 409
    assert r.json()["missing_prerequisites"] == [0]

    # 深层：复用 3 必须带上 0、1、2
    r = fork(s["id"], 0, "closure-3", [0, 3])
    assert r.status_code == 409
    assert r.json()["missing_prerequisites"] == [1, 2]

    # 闭包完整：通过
    r = fork(s["id"], 0, "closure-ok", [0, 1, 2, 3])
    assert r.status_code == 201
    assert r.json()["pending_chunks"] == []

    # 闭包完整（前缀）：同样通过
    r = fork(s["id"], 0, "closure-prefix", [0, 1, 2])
    assert r.status_code == 201
    assert r.json()["reused_chunks"] == [0, 1, 2]
    assert r.json()["pending_chunks"] == [3]


def test_fork_checks_expected_revision_and_active_status():
    s = make_session_with_deps(2, {1: [0]})
    t0, d0 = lease_complete(s["id"], 0, "0" * 64)
    lease_complete(s["id"], 1, "1" * 64)

    # 修订号不匹配：409 且给出当前修订号
    r = fork(s["id"], 1, "rev-bad", [0])
    assert r.status_code == 409
    body = r.json()
    assert body["detail"] == "revision mismatch"
    assert body["expected_revision"] == 1
    assert body["current_revision"] == 0

    # reopen 使源会话 revision 变为 1 后，旧修订号请求被拒、新修订号通过
    assert reopen(s["id"], 0, t0, d0).status_code == 200
    assert fork(s["id"], 0, "rev-stale", []).status_code == 409
    assert fork(s["id"], 1, "rev-ok", []).status_code == 201

    # 已提交会话不可再分支：reopen 会把 0 及下游 1 一并重置，需重新完成才能提交
    t0b = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, t0b, "0" * 64).status_code == 200
    t1b = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1b, "1" * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
    assert fork(s["id"], 1, "finalized", []).status_code == 409

    # 未知源会话
    assert fork("nope", 0, "x", []).status_code == 404


def test_fork_request_id_is_idempotent():
    s = fork_deps_session()
    lease_complete(s["id"], 0, "0" * 64)

    r1 = fork(s["id"], 0, "idem", [0])
    assert r1.status_code == 201
    assert r1.json()["idempotent_replay"] is False
    new_id = r1.json()["id"]
    created_at = r1.json()["created_at"]

    # 同参数重放：返回首次新会话（200 + idempotent_replay=true），不重复建会话
    r2 = fork(s["id"], 0, "idem", [0])
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["id"] == new_id
    assert body2["idempotent_replay"] is True
    assert body2["created_at"] == created_at
    assert body2["reused_chunks"] == [0]

    # reuse_chunks 仅顺序不同（去重排序后一致）：仍视为同参数重放
    r3 = fork(s["id"], 0, "idem", [0, 0])
    assert r3.status_code == 200
    assert r3.json()["id"] == new_id

    rows = get_conn().execute(
        "SELECT COUNT(*) AS n FROM sessions WHERE source_session_id = ?", (s["id"],)
    ).fetchone()
    assert rows["n"] == 1

    # 同一 request_id 但 expected_revision 变化：409
    assert fork(s["id"], 2, "idem", [0]).status_code == 409
    # 同一 request_id 但 reuse_chunks 变化：409
    assert fork(s["id"], 0, "idem", []).status_code == 409
    # 参数变化的冲突不产生新会话，重放仍返回首次结果
    assert fork(s["id"], 0, "idem", [0]).json()["id"] == new_id

    # 不同 request_id：创建另一个独立分支
    r4 = fork(s["id"], 0, "idem-2", [0])
    assert r4.status_code == 201
    assert r4.json()["id"] != new_id


def test_fork_with_empty_reuse_starts_all_pending():
    s = fork_deps_session()
    lease_complete(s["id"], 0, "0" * 64)

    r = fork(s["id"], 0, "empty", [])
    assert r.status_code == 201
    body = r.json()
    assert body["reused_chunks"] == []
    assert body["pending_chunks"] == [0, 1, 2, 3]
    st = client.get(f"/sessions/{body['id']}").json()
    assert all(c["status"] == "pending" for c in st["chunks"])
    assert st["completed_chunks"] == 0


def test_forked_session_is_independent_and_finalizable():
    s = fork_deps_session()
    lease_complete(s["id"], 0, "0" * 64)
    lease_complete(s["id"], 1, "1" * 64)

    new_id = fork(s["id"], 0, "indep", [0, 1]).json()["id"]
    # 在新会话完成剩余分片并提交
    lease_complete(new_id, 2, "2" * 64)
    lease_complete(new_id, 3, "3" * 64)
    fin = client.post(f"/sessions/{new_id}/finalize")
    assert fin.status_code == 200
    assert [m["chunk_digest"] for m in fin.json()["manifest"]] == [
        "0" * 64, "1" * 64, "2" * 64, "3" * 64,
    ]

    # 新会话已提交不影响源会话：源会话仍 active
    assert client.get(f"/sessions/{s['id']}").json()["status"] == "active"
    # 已提交的分支自身不可再被 fork
    assert fork(new_id, 0, "from-finalized", []).status_code == 409
    # 源会话仍可继续分支
    assert fork(s["id"], 0, "indep-2", [0, 1]).status_code == 201


def test_fork_from_a_fork_chains_source_session_id():
    s = fork_deps_session()
    lease_complete(s["id"], 0, "0" * 64)
    child = fork(s["id"], 0, "child", [0]).json()
    assert child["source_session_id"] == s["id"]

    grand = fork(child["id"], 0, "grand", []).json()
    assert grand["source_session_id"] == child["id"]
    assert grand["id"] not in (s["id"], child["id"])

    st = client.get(f"/sessions/{grand['id']}").json()
    assert st["source_session_id"] == child["id"]


def test_fork_request_validation():
    s = make_session()
    # 缺字段
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"request_id": "x", "reuse_chunks": []}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"expected_revision": 0, "reuse_chunks": []}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"expected_revision": 0, "request_id": "x"}).status_code == 422
    # 空 request_id / 负修订号 / 负分片号 / 重复分片号
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"expected_revision": 0, "request_id": "",
              "reuse_chunks": []}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"expected_revision": -1, "request_id": "x",
              "reuse_chunks": []}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"expected_revision": 0, "request_id": "x",
              "reuse_chunks": [-1]}).status_code == 422
    # 非数组 reuse_chunks
    assert client.post(
        f"/sessions/{s['id']}/fork",
        json={"expected_revision": 0, "request_id": "x",
              "reuse_chunks": "0"}).status_code == 422
    # 重复序号不报错：服务端去重排序（422 仅针对负数等格式问题）
    s2 = make_session()
    t0 = lease(s2["id"], 0).json()["lease_token"]
    assert complete(s2["id"], 0, t0, "0" * 64).status_code == 200
    r = client.post(
        f"/sessions/{s2['id']}/fork",
        json={"expected_revision": 0, "request_id": "dup-ok",
              "reuse_chunks": [0, 0]})
    assert r.status_code == 201
    assert r.json()["reused_chunks"] == [0]


def test_migration_adds_source_session_id_column_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy4.db"
    conn = sqlite3.connect(legacy)
    # 0004 之后、本次改动之前的旧 SCHEMA：sessions 无 source_session_id 列
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
            base_retry_seconds INTEGER NOT NULL DEFAULT 5,
            max_retry_seconds INTEGER NOT NULL DEFAULT 300,
            revision INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0, backoff_until TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        CREATE TABLE chunk_dependencies (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            depends_on INTEGER NOT NULL,
            PRIMARY KEY (session_id, chunk_index, depends_on)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-sid', 1, 'dig', 'sha256', 'active',
                    '2026-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    assert "source_session_id" in cols
    # 既有原生会话来源为 NULL
    row = migrated.execute(
        "SELECT source_session_id FROM sessions WHERE id = 'legacy-sid'"
    ).fetchone()
    assert row[0] is None
    # fork_requests 为新建表
    tables = {r[0] for r in migrated.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    assert "fork_requests" in tables
    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()


# ---------------------------------------------------------------- 跨会话公平调度（/dispatch/lease）

def dispatch(queue_key, session_ids, ttl=None):
    body = {"queue_key": queue_key, "session_ids": session_ids}
    if ttl is not None:
        body["lease_ttl_seconds"] = ttl
    return client.post("/dispatch/lease", json=body)


def test_dispatch_rotates_sessions_in_creation_order():
    a = make_session(total_chunks=2)["id"]
    b = make_session(total_chunks=2)["id"]
    c = make_session(total_chunks=2)["id"]
    ordered = [a, b, c]

    # 首次无游标：从（创建时间, id）排序首位开始，按序轮转
    granted_sessions = []
    for i in range(6):
        r = dispatch("q-rotate", ordered)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["granted"] is not None
        assert body["unavailable"] == []
        granted_sessions.append(body["granted"]["session_id"])
        # 成功后游标推进到被发放会话
        assert body["cursor_session_id"] == body["granted"]["session_id"]

    assert granted_sessions == [a, b, c, a, b, c]

    # 每个会话各拿到其序号最低的分片：首轮 0、次轮 1
    st = {sid: client.get(f"/sessions/{sid}").json() for sid in ordered}
    assert st[a]["active_leases"] == 2
    assert [ch["chunk_index"] for ch in st[a]["chunks"] if ch["status"] == "leased"] == [0, 1]


def test_dispatch_deduplicates_candidate_ids():
    a = make_session(total_chunks=1)["id"]
    b = make_session(total_chunks=1)["id"]

    # 重复候选等价于只列一次：不会在同一会话上连续发放
    r1 = dispatch("q-dedup", [b, a, b, a, b])
    assert r1.status_code == 200
    assert r1.json()["granted"]["session_id"] == a  # 创建时间更早
    r2 = dispatch("q-dedup", [b, a, b])
    assert r2.json()["granted"]["session_id"] == b  # 游标在 a 之后，从 b 继续
    assert r2.json()["cursor_session_id"] == b


def test_dispatch_grants_lowest_index_ready_chunk():
    s = make_session_with_deps(3, {2: [0]})
    # 先领走 0：0 持有效租约、2 被依赖阻塞，可领取的最低序号为 1
    lease(s["id"], 0)
    r = dispatch("q-lowest", [s["id"]])
    assert r.status_code == 200
    g = r.json()["granted"]
    assert g["session_id"] == s["id"]
    assert g["chunk_index"] == 1
    assert g["lease_token"]


def test_dispatch_skips_unavailable_sessions_and_advances_cursor():
    # busy 先占用唯一分片，idle 空闲
    busy = make_session(total_chunks=1)["id"]
    idle = make_session(total_chunks=1)["id"]
    lease(busy, 0)

    r1 = dispatch("q-skip", [busy, idle])
    assert r1.status_code == 200
    assert r1.json()["granted"]["session_id"] == idle
    assert r1.json()["granted"]["chunk_index"] == 0
    assert r1.json()["cursor_session_id"] == idle

    # idle 已占满且 busy 仍持有租约：无项可发，游标保持 idle 不变
    r2 = dispatch("q-skip", [busy, idle])
    assert r2.status_code == 200
    assert r2.json()["granted"] is None
    assert r2.json()["cursor_session_id"] == idle
    reasons = {u["session_id"]: u["reason"] for u in r2.json()["unavailable"]}
    assert reasons == {busy: "chunks_leased", idle: "chunks_leased"}

    # busy 租约过期后恢复可发放；游标在 idle 之后，循环扫描从 busy 开始
    expire_lease(busy, 0)
    r3 = dispatch("q-skip", [busy, idle])
    assert r3.json()["granted"]["session_id"] == busy
    assert r3.json()["granted"]["chunk_index"] == 0
    assert r3.json()["cursor_session_id"] == busy


def test_dispatch_miss_reasons_and_next_retry_order():
    not_found = "does-not-exist"
    finalized = make_session(total_chunks=1)["id"]
    t = lease(finalized, 0).json()["lease_token"]
    complete(finalized, 0, t, "0" * 64)
    client.post(f"/sessions/{finalized}/finalize")

    done = make_session(total_chunks=1)["id"]
    t = lease(done, 0).json()["lease_token"]
    complete(done, 0, t, "0" * 64)

    leased = make_session(total_chunks=1)["id"]
    lease(leased, 0, ttl=600)

    cooling = make_retry_session(total_chunks=1)["id"]
    lease_fail_retry(cooling, 0)

    full = make_capped_session(total_chunks=2, max_active_leases=1)["id"]
    lease(full, 0)  # 槽位占满，分片 1 具备领取资格

    r = dispatch("q-miss", [not_found, finalized, done, leased, cooling, full])
    assert r.status_code == 200
    body = r.json()
    assert body["granted"] is None
    # 首次无游标：扫描顺序为现存会话按创建时间，未知会话排最后
    reasons = {u["session_id"]: u["reason"] for u in body["unavailable"]}
    assert reasons == {
        not_found: "session_not_found",
        finalized: "session_finalized",
        done: "no_unfinished_chunks",
        leased: "chunks_leased",
        cooling: "chunks_in_backoff",
        full: "session_full",
    }
    assert [u["session_id"] for u in body["unavailable"]] == [
        finalized, done, leased, cooling, full, not_found,
    ]
    # 终态/未知会话不携带 next_retry_at
    by_sid = {u["session_id"]: u for u in body["unavailable"]}
    assert by_sid[not_found]["next_retry_at"] is None
    assert by_sid[finalized]["next_retry_at"] is None
    assert by_sid[done]["next_retry_at"] is None
    assert by_sid[leased]["next_retry_at"] is not None
    assert by_sid[cooling]["next_retry_at"] is not None
    full_entry = by_sid[full]
    assert full_entry["active_leases"] == 1
    assert full_entry["max_active_leases"] == 1
    assert full_entry["next_retry_at"] is not None  # 持有的有效租约有过期时间
    # 全局最早可重试时刻为各会话最早者之一
    candidates = [u["next_retry_at"] for u in body["unavailable"]
                  if u["next_retry_at"] is not None]
    assert body["next_retry_at"] == min(candidates)
    # 无成功发放：游标仍未建立
    assert body["cursor_session_id"] is None



def test_dispatch_backoff_reason_takes_precedence_over_blocked():
    # 链 1 -> 0：0 失败冷却（同时 1 被阻塞）。无就绪分片，但存在冷却等待事件，
    # 原因应为 chunks_in_backoff 而非纯 blocked
    s = make_session_with_deps(2, {1: [0]})
    lease_fail_retry(s["id"], 0)

    r = dispatch("q-blockmix", [s["id"]])
    assert r.status_code == 200
    body = r.json()
    assert body["granted"] is None
    assert len(body["unavailable"]) == 1
    entry = body["unavailable"][0]
    assert entry["session_id"] == s["id"]
    assert entry["reason"] == "chunks_in_backoff"
    assert entry["next_retry_at"] is not None
    assert body["next_retry_at"] == entry["next_retry_at"]
    assert body["cursor_session_id"] is None  # 未成功发放，游标不建立


def test_dispatch_treats_expired_lease_as_claimable():
    s = make_session(total_chunks=1)["id"]
    lease(s, 0)
    assert dispatch("q-expired", [s]).json()["granted"] is None
    expire_lease(s, 0)
    r = dispatch("q-expired", [s])
    body = r.json()
    assert body["granted"] is not None
    assert body["granted"]["session_id"] == s
    assert body["granted"]["chunk_index"] == 0
    assert body["cursor_session_id"] == s


def test_dispatch_resumes_after_cooldown_and_completion():
    # 冷却结束后可调度；完成上报释放分片后会话变为全部完成
    cooling = make_retry_session(total_chunks=1)["id"]
    lease_fail_retry(cooling, 0)
    assert dispatch("q-cool", [cooling]).json()["granted"] is None

    set_backoff(cooling, 0, PAST)
    r = dispatch("q-cool", [cooling])
    assert r.status_code == 200
    g = r.json()["granted"]
    assert g["session_id"] == cooling and g["chunk_index"] == 0

    token = g["lease_token"]
    assert complete(cooling, 0, token, "0" * 64).status_code == 200
    body = dispatch("q-cool", [cooling]).json()
    assert body["granted"] is None
    assert body["unavailable"][0]["reason"] == "no_unfinished_chunks"
    # 游标仍停在上次成功发放的会话
    assert body["cursor_session_id"] == cooling


def test_dispatch_ttl_is_applied_and_capped():
    s = make_session(total_chunks=1)["id"]
    body = dispatch("q-ttl", [s], ttl=120).json()
    expires = datetime.fromisoformat(body["granted"]["expires_at"])
    assert 119 <= (expires - datetime.now(timezone.utc)).total_seconds() <= 120

    # 缺省 TTL 取服务端默认 300
    s2 = make_session(total_chunks=1)["id"]
    body2 = dispatch("q-ttl-default", [s2]).json()
    expires2 = datetime.fromisoformat(body2["granted"]["expires_at"])
    assert 299 <= (expires2 - datetime.now(timezone.utc)).total_seconds() <= 300


def test_dispatch_cursors_are_isolated_per_queue_key():
    a = make_session(total_chunks=2)["id"]
    b = make_session(total_chunks=2)["id"]

    # 两个队列独立轮转
    assert dispatch("q-a", [a, b]).json()["granted"]["session_id"] == a
    assert dispatch("q-b", [a, b]).json()["granted"]["session_id"] == a  # 独立游标，仍从 a
    assert dispatch("q-a", [a, b]).json()["granted"]["session_id"] == b
    assert dispatch("q-b", [a, b]).json()["granted"]["session_id"] == b


def test_dispatch_restarts_from_head_when_cursor_not_in_candidates():
    a = make_session(total_chunks=2)["id"]
    b = make_session(total_chunks=2)["id"]
    c = make_session(total_chunks=2)["id"]

    # q 上游标停在 b
    dispatch("q-move", [a, b, c])  # a
    dispatch("q-move", [a, b, c])  # b
    # 本次候选不含 b（游标所指会话）：从排序首位 a 重新开始
    r = dispatch("q-move", [c, a])
    assert r.json()["granted"]["session_id"] == a


def test_dispatch_request_validation():
    s = make_session()["id"]
    assert client.post("/dispatch/lease", json={
        "queue_key": "", "session_ids": [s]}).status_code == 422
    assert client.post("/dispatch/lease", json={
        "queue_key": "q", "session_ids": []}).status_code == 422
    assert client.post("/dispatch/lease", json={
        "queue_key": "q", "session_ids": [""]}).status_code == 422
    assert client.post("/dispatch/lease", json={
        "queue_key": "q", "session_ids": [s], "lease_ttl_seconds": 0}).status_code == 422
    assert client.post("/dispatch/lease", json={
        "queue_key": "q"}).status_code == 422
    # 未知会话不返回 4xx：作为不可用原因返回
    r = client.post("/dispatch/lease", json={
        "queue_key": "q", "session_ids": ["ghost"]})
    assert r.status_code == 200
    assert r.json()["granted"] is None
    assert r.json()["unavailable"][0]["reason"] == "session_not_found"


def test_dispatch_never_double_grants_under_concurrency():
    # 3 个单分片空闲会话 + 若干无项可发的阻塞会话；6 个并发请求抢 3 个分片
    ready_sessions = [make_session(total_chunks=1)["id"] for _ in range(3)]
    busy_sessions = [make_session(total_chunks=1)["id"] for _ in range(2)]
    for sid in busy_sessions:
        lease(sid, 0)
    candidates = ready_sessions + busy_sessions

    granted: list[tuple[str, int]] = []
    granted_lock = threading.Lock()
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        body = dispatch("q-concurrent", candidates).json()
        if body["granted"] is not None:
            with granted_lock:
                granted.append((body["granted"]["session_id"],
                                body["granted"]["chunk_index"]))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好发放 3 个租约，且不重复
    assert len(granted) == 3
    assert len(set(granted)) == 3
    assert {sid for sid, _ in granted} == set(ready_sessions)
    for sid in ready_sessions:
        st = client.get(f"/sessions/{sid}").json()
        assert st["active_leases"] == 1

    # 之后的请求无项可发，游标停在最后一个被发放会话
    body = dispatch("q-concurrent", candidates).json()
    assert body["granted"] is None
    assert body["cursor_session_id"] in ready_sessions
    # 各会话原因齐全（扫描覆盖全部候选）
    reasons = {u["session_id"]: u["reason"] for u in body["unavailable"]}
    assert set(reasons) == set(candidates)
    assert all(reasons[sid] == "chunks_leased" for sid in candidates)


def test_dispatch_finalized_session_skipped_but_others_served():
    fin = make_session(total_chunks=1)["id"]
    token = lease(fin, 0).json()["lease_token"]
    complete(fin, 0, token, "0" * 64)
    client.post(f"/sessions/{fin}/finalize")

    active = make_session(total_chunks=1)["id"]
    body = dispatch("q-fin", [fin, active]).json()
    assert body["granted"]["session_id"] == active
    assert body["cursor_session_id"] == active

    # 候选全为已提交会话：无项可发且游标不动
    body2 = dispatch("q-fin", [fin])
    assert body2.json()["granted"] is None
    assert body2.json()["unavailable"][0]["reason"] == "session_finalized"
    assert body2.json()["cursor_session_id"] == active


def test_migration_adds_dispatch_cursors_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy5.db"
    conn = sqlite3.connect(legacy)
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
            base_retry_seconds INTEGER NOT NULL DEFAULT 5,
            max_retry_seconds INTEGER NOT NULL DEFAULT 300,
            revision INTEGER NOT NULL DEFAULT 0,
            source_session_id TEXT,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0, backoff_until TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    tables = {r[0] for r in migrated.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    assert "dispatch_cursors" in tables
    cols = {r[1] for r in migrated.execute(
        "PRAGMA table_info(dispatch_cursors)").fetchall()}
    assert cols == {"queue_key", "last_session_id", "updated_at"}
    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()


# ---------------------------------------------------------------- 交付凭证（delivery ticket）

def issue_ticket(session_id, expires_in_seconds=3600):
    return client.post(
        f"/sessions/{session_id}/delivery-tickets",
        json={"expires_in_seconds": expires_in_seconds},
    )


def redeem(ticket_token, request_id):
    return client.post(
        "/delivery-tickets/redeem",
        json={"token": ticket_token, "request_id": request_id},
    )


def revoke(ticket_token):
    return client.post(
        "/delivery-tickets/revoke",
        json={"token": ticket_token},
    )


def finalized_session(total_chunks=3):
    """创建会话并逐分片领取/完成/提交；返回 (session_id, 期望清单)。"""
    s = make_session(total_chunks=total_chunks)
    expected = []
    for i in range(total_chunks):
        token = lease(s["id"], i).json()["lease_token"]
        digest = f"{i:064d}"
        assert complete(s["id"], i, token, digest).status_code == 200
        expected.append({"chunk_index": i, "chunk_digest": digest})
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
    return s["id"], expected


def test_ticket_requires_finalized_session():
    s = make_session(total_chunks=1)
    # active 会话不可签发
    r = issue_ticket(s["id"])
    assert r.status_code == 409
    assert "finalized" in r.json()["detail"]
    # 未知会话 404
    assert issue_ticket("nope").status_code == 404

    sid, _ = finalized_session(1)
    ok = issue_ticket(sid, 600)
    assert ok.status_code == 201, ok.text
    body = ok.json()
    assert body["status"] == "active"
    assert body["session_id"] == sid
    assert len(body["ticket_token"]) >= 40
    expires = datetime.fromisoformat(body["expires_at"])
    created = datetime.fromisoformat(body["created_at"])
    assert 599 <= (expires - created).total_seconds() <= 600
    assert expires > datetime.now(timezone.utc)


def test_ticket_token_is_shown_once_and_only_hash_persisted():
    sid, _ = finalized_session(1)
    body = issue_ticket(sid, 3600).json()
    token = body["ticket_token"]

    # 明文令牌不出现在任何表中；delivery_tickets 仅保存 SHA-256、会话、到期与状态
    rows = get_conn().execute(
        "SELECT * FROM delivery_tickets WHERE session_id = ?", (sid,)
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row.keys() == ["token_hash", "session_id", "expires_at", "status"]
    assert row["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    assert row["status"] == "active"

    found = get_conn().execute(
        "SELECT COUNT(*) AS n FROM delivery_tickets WHERE token_hash = ?",
        (token,),
    ).fetchone()["n"]
    assert found == 0


def test_redeem_returns_ordered_manifest_and_consumes_once():
    sid, expected = finalized_session(3)
    token = issue_ticket(sid, 3600).json()["ticket_token"]
    fin_manifest = client.post(f"/sessions/{sid}/finalize").json()["manifest"]

    r = redeem(token, "req-first")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["session_id"] == sid
    assert body["status"] == "finalized"
    assert body["request_id"] == "req-first"
    assert body["total_chunks"] == 3
    assert [m["chunk_index"] for m in body["manifest"]] == [0, 1, 2]
    assert [{k: m[k] for k in ("chunk_index", "chunk_digest")} for m in body["manifest"]] == expected
    # 兑换清单与提交清单一致（含 completed_at）
    assert body["manifest"] == fin_manifest
    assert datetime.fromisoformat(body["redeemed_at"]) <= datetime.now(timezone.utc)

    # 凭证已消费：不同 request_id 不可再领取
    r2 = redeem(token, "req-second")
    assert r2.status_code == 409
    assert "consumed" in r2.json()["detail"]

    # 库内状态已置 consumed
    row = get_conn().execute(
        "SELECT status FROM delivery_tickets WHERE token_hash = ?",
        (hashlib.sha256(token.encode()).hexdigest(),),
    ).fetchone()
    assert row["status"] == "consumed"


def test_redeem_same_request_id_replays_identical_result():
    sid, _ = finalized_session(2)
    token = issue_ticket(sid, 3600).json()["ticket_token"]

    r1 = redeem(token, "idem-1")
    assert r1.status_code == 200
    r2 = redeem(token, "idem-1")
    assert r2.status_code == 200
    r3 = redeem(token, "idem-1")
    assert r3.status_code == 200
    # 同一 request_id 重放返回首次完全相同的结果
    assert r2.json() == r1.json()
    assert r3.json() == r1.json()
    assert r3.json()["redeemed_at"] == r1.json()["redeemed_at"]

    # 只有一条幂等记录
    n = get_conn().execute(
        "SELECT COUNT(*) AS n FROM delivery_ticket_redemptions WHERE request_id = 'idem-1'"
    ).fetchone()["n"]
    assert n == 1

    # 不同 request_id 仍然失败（一次性）
    assert redeem(token, "other").status_code == 409


def test_redeem_replay_works_even_after_time_passes_and_expiry():
    sid, _ = finalized_session(1)
    token = issue_ticket(sid, 3600).json()["ticket_token"]
    first = redeem(token, "replay-key")
    assert first.status_code == 200

    # 凭证已消费且到期时间被改为过去：同 request_id 重放仍返回首次结果
    get_conn().execute(
        "UPDATE delivery_tickets SET expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE token_hash = ?",
        (hashlib.sha256(token.encode()).hexdigest(),)
    )
    get_conn().commit()
    again = redeem(token, "replay-key")
    assert again.status_code == 200
    assert again.json() == first.json()


def test_concurrent_redeems_with_different_request_ids_only_one_succeeds():
    sid, _ = finalized_session(1)
    token = issue_ticket(sid, 3600).json()["ticket_token"]

    statuses: list[int] = []
    bodies: list[dict] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker(i):
        barrier.wait()
        r = redeem(token, f"race-{i}")
        with lock:
            statuses.append(r.status_code)
            bodies.append(r.json())

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses).count(200) == 1
    assert sorted(statuses).count(409) == 7
    winner = next(b for b in bodies if "manifest" in b)
    assert [m["chunk_index"] for m in winner["manifest"]] == [0]

    # 获胜 request_id 的重放（含并发结束后）仍返回首次结果
    replay = redeem(token, winner["request_id"])
    assert replay.status_code == 200
    assert replay.json() == winner


def test_concurrent_same_request_id_replays_all_get_first_result():
    sid, _ = finalized_session(1)
    token = issue_ticket(sid, 3600).json()["ticket_token"]

    results: list[tuple[int, dict]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        r = redeem(token, "same-key")
        with lock:
            results.append((r.status_code, r.json()))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 同一 request_id：全部成功且结果完全一致（首个提交者的响应）
    assert all(code == 200 for code, _ in results)
    first = results[0][1]
    assert all(body == first for _, body in results)
    n = get_conn().execute(
        "SELECT COUNT(*) AS n FROM delivery_ticket_redemptions"
        " WHERE request_id = 'same-key'"
    ).fetchone()["n"]
    assert n == 1


def test_expired_ticket_cannot_redeem_but_another_ticket_still_works():
    sid, _ = finalized_session(1)
    stale = issue_ticket(sid, 3600).json()["ticket_token"]
    fresh = issue_ticket(sid, 600).json()["ticket_token"]

    # 手动让第一张到期
    get_conn().execute(
        "UPDATE delivery_tickets SET expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE token_hash = ?",
        (hashlib.sha256(stale.encode()).hexdigest(),)
    )
    get_conn().commit()

    r = redeem(stale, "late")
    assert r.status_code == 409
    assert "expired" in r.json()["detail"]
    # 过期但未消费：状态仍是 active（不自动改状态，保持可撤销）
    row = get_conn().execute(
        "SELECT status FROM delivery_tickets WHERE token_hash = ?",
        (hashlib.sha256(stale.encode()).hexdigest(),)
    ).fetchone()
    assert row["status"] == "active"
    # 同一会话的另一张凭证不受影响
    assert redeem(fresh, "on-time").status_code == 200


def test_revoked_ticket_cannot_redeem_and_revoke_is_idempotent():
    sid, _ = finalized_session(1)
    token = issue_ticket(sid, 3600).json()["ticket_token"]

    r1 = revoke(token)
    assert r1.status_code == 200
    assert r1.json()["status"] == "revoked"
    assert r1.json()["already_revoked"] is False

    # 重复撤销幂等
    r2 = revoke(token)
    assert r2.status_code == 200
    assert r2.json()["status"] == "revoked"
    assert r2.json()["already_revoked"] is True

    # 撤销后任何 request_id 都不可领取（包括重放一个从未成功过的键）
    assert redeem(token, "never-used").status_code == 409
    assert redeem(token, "another").status_code == 409


def test_consumed_ticket_cannot_be_revoked():
    sid, _ = finalized_session(1)
    token = issue_ticket(sid, 3600).json()["ticket_token"]
    assert redeem(token, "keeper").status_code == 200

    r = revoke(token)
    assert r.status_code == 409
    assert "consumed" in r.json()["detail"]
    # 状态保持 consumed；同 request_id 重放仍成功
    assert redeem(token, "keeper").status_code == 200


def test_expired_unconsumed_ticket_can_be_revoked():
    sid, _ = finalized_session(1)
    token = issue_ticket(sid, 3600).json()["ticket_token"]
    get_conn().execute(
        "UPDATE delivery_tickets SET expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE token_hash = ?",
        (hashlib.sha256(token.encode()).hexdigest(),)
    )
    get_conn().commit()
    assert redeem(token, "x").status_code == 409  # 已过期
    r = revoke(token)
    assert r.status_code == 200
    assert r.json()["status"] == "revoked"
    assert redeem(token, "x").status_code == 409  # 撤销后依旧不可领取


def test_redeem_and_revoke_unknown_token():
    assert redeem("definitely-not-a-real-token", "r").status_code == 404
    assert revoke("definitely-not-a-real-token").status_code == 404


def test_multiple_tickets_for_one_session_are_independent():
    sid, _ = finalized_session(1)
    t1 = issue_ticket(sid, 3600).json()["ticket_token"]
    t2 = issue_ticket(sid, 3600).json()["ticket_token"]
    assert t1 != t2

    # 撤销第一张不影响第二张
    assert revoke(t1).status_code == 200
    assert redeem(t1, "a").status_code == 409
    r2 = redeem(t2, "b")
    assert r2.status_code == 200
    # 第二张消费后不可再用
    assert redeem(t2, "c").status_code == 409

    # 同一会话仍可继续签发新凭证
    t3 = issue_ticket(sid, 60).json()["ticket_token"]
    assert redeem(t3, "d").status_code == 200


def test_delivery_ticket_request_validation():
    sid, _ = finalized_session(1)
    # expires_in_seconds 必须为正
    assert issue_ticket(sid, 0).status_code == 422
    assert issue_ticket(sid, -5).status_code == 422
    # 缺字段
    assert client.post(
        f"/sessions/{sid}/delivery-tickets", json={}).status_code == 422
    # 兑换：缺字段 / 空串
    assert client.post("/delivery-tickets/redeem", json={
        "request_id": "r"}).status_code == 422
    assert client.post("/delivery-tickets/redeem", json={
        "token": "t"}).status_code == 422
    assert client.post("/delivery-tickets/redeem", json={
        "token": "", "request_id": "r"}).status_code == 422
    assert client.post("/delivery-tickets/redeem", json={
        "token": "t", "request_id": ""}).status_code == 422
    # 撤销：缺字段 / 空串
    assert client.post("/delivery-tickets/revoke", json={}).status_code == 422
    assert client.post("/delivery-tickets/revoke", json={"token": ""}).status_code == 422


def test_migration_adds_delivery_ticket_tables_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy6.db"
    conn = sqlite3.connect(legacy)
    # 交付凭证特性之前的旧 SCHEMA：无 delivery_tickets 相关表
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
            base_retry_seconds INTEGER NOT NULL DEFAULT 5,
            max_retry_seconds INTEGER NOT NULL DEFAULT 300,
            revision INTEGER NOT NULL DEFAULT 0,
            source_session_id TEXT,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0, backoff_until TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    tables = {r[0] for r in migrated.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    assert "delivery_tickets" in tables
    assert "delivery_ticket_redemptions" in tables

    ticket_cols = {r[1] for r in migrated.execute(
        "PRAGMA table_info(delivery_tickets)").fetchall()}
    assert ticket_cols == {"token_hash", "session_id", "expires_at", "status"}
    redemption_cols = {r[1] for r in migrated.execute(
        "PRAGMA table_info(delivery_ticket_redemptions)").fetchall()}
    assert redemption_cols == {"token_hash", "request_id", "response_json", "created_at"}

    # 新表可正常写入与按状态消费
    migrated.execute(
        "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status,"
        " created_at, finalized_at) VALUES"
        " ('fin-sid', 1, 'dig', 'sha256', 'finalized',"
        " '2026-01-01T00:00:00+00:00', '2026-01-02T00:00:00+00:00')"
    )
    migrated.execute(
        "INSERT INTO delivery_tickets (token_hash, session_id, expires_at, status)"
        " VALUES ('abc', 'fin-sid', '2999-01-01T00:00:00+00:00', 'active')"
    )
    migrated.commit()
    cur = migrated.execute(
        "UPDATE delivery_tickets SET status = 'consumed'"
        " WHERE token_hash = 'abc' AND status = 'active'"
    )
    assert cur.rowcount == 1
    migrated.commit()

    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()


# ---------------------------------------------------------------- 分片验收队列

VERIFICATION_PAST = "2000-01-01T00:00:00+00:00"


def make_verification_session(total_chunks=3, dependencies=None, file_digest="f" * 64):
    payload = {
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256",
        "verification_required": True,
    }
    if dependencies is not None:
        payload["dependencies"] = dependencies
    resp = client.post("/sessions", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease_complete_awaiting(session_id, index, digest=None):
    """领取上传租约并上报完成；返回 complete 响应（验收会话中应为 awaiting_verification）。"""
    digest = digest if digest is not None else f"{index:064d}"
    token = lease(session_id, index).json()["lease_token"]
    resp = complete(session_id, index, token, digest)
    assert resp.status_code == 200, resp.text
    return resp


def verification_lease(ttl=None):
    body = {} if ttl is None else {"lease_ttl_seconds": ttl}
    return client.post("/verifications/lease", json=body)


def decide(session_id, index, token, decision):
    return client.post(
        f"/verifications/{session_id}/{index}/decide",
        json={"verification_token": token, "decision": decision},
    )


def expire_verification_lease(session_id, index, when=VERIFICATION_PAST):
    """直接把验收令牌置为过期，避免真实等待。"""
    get_conn().execute(
        "UPDATE verification_leases SET expires_at = ?"
        " WHERE session_id = ? AND chunk_index = ?",
        (when, session_id, index),
    )
    get_conn().commit()


@pytest.fixture(autouse=True)
def _drain_global_verification_queue():
    """验收队列为全局队列，而测试共用一个进程库：每个用例前后把其他用例遗留的
    awaiting_verification 分片全部 approve 清空，避免跨用例串扰。本用例自行创建的
    分片在其执行期间不受影响（清 drain 发生在用例开始之前）。"""
    def _drain():
        conn = get_conn()
        conn.execute("DELETE FROM verification_leases")
        conn.execute("DELETE FROM verification_decisions")
        conn.execute(
            "UPDATE chunks SET status = 'completed' WHERE status = 'awaiting_verification'"
        )
        conn.commit()

    _drain()
    yield
    _drain()


def test_create_session_verification_flag_persisted():
    s = make_verification_session(total_chunks=2)
    assert s["verification_required"] is True
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["verification_required"] is True
    assert st["awaiting_verification_chunks"] == []
    # 未启用（默认）会话不带待验收分片且标记为 false
    plain = make_session(total_chunks=2)
    assert plain["verification_required"] is False
    pst = client.get(f"/sessions/{plain['id']}").json()
    assert pst["verification_required"] is False
    assert pst["awaiting_verification_chunks"] == []


def test_complete_enters_awaiting_verification_when_enabled():
    s = make_verification_session(total_chunks=2)
    r = lease_complete_awaiting(s["id"], 0)
    assert r.json()["status"] == "awaiting_verification"
    assert r.json()["idempotent_replay"] is False
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["awaiting_verification_chunks"] == [0]
    assert st["missing_chunks"] == [0, 1]
    assert st["completed_chunks"] == 0
    chunk0 = st["chunks"][0]
    assert chunk0["status"] == "awaiting_verification"
    assert chunk0["chunk_digest"] == f"{0:064d}"
    # 尚未领取验收令牌：verification_expires_at 为 null
    assert chunk0["verification_expires_at"] is None
    # /ready 单独列出待验收分片，且不计入 ready/blocked/cooling；
    # 同会话未上报的分片 1 仍可领取
    rd = client.get(f"/sessions/{s['id']}/ready").json()
    assert rd["ready_chunks"] == [1]
    assert rd["blocked_chunks"] == []
    assert rd["cooling_chunks"] == []
    assert rd["awaiting_verification_chunks"] == [0]


def test_complete_replay_is_idempotent_while_awaiting():
    s = make_verification_session(total_chunks=1)
    token = lease(s["id"], 0).json()["lease_token"]
    r1 = complete(s["id"], 0, token, "a" * 64)
    assert r1.json()["status"] == "awaiting_verification"
    r2 = complete(s["id"], 0, token, "a" * 64)
    assert r2.status_code == 200
    assert r2.json()["idempotent_replay"] is True
    assert r2.json()["status"] == "awaiting_verification"
    assert r2.json()["completed_at"] == r1.json()["completed_at"]
    # 摘要不同仍为冲突
    r3 = complete(s["id"], 0, token, "b" * 64)
    assert r3.status_code == 409


def test_awaiting_chunk_cannot_be_upload_leased_failed_or_finalized():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    # 不可重复领取上传租约
    assert lease(s["id"], 0).status_code == 409
    # 不可上报失败
    assert fail(s["id"], 0, "whatever").status_code == 409
    # 不可 reopen
    rr = client.post(
        f"/sessions/{s['id']}/chunks/0/reopen",
        json={"lease_token": "x", "expected_digest": "a" * 64},
    )
    assert rr.status_code == 409
    # 存在待验收分片时不可 finalize
    fr = client.post(f"/sessions/{s['id']}/finalize")
    assert fr.status_code == 409
    assert fr.json()["missing_chunks"] == [0]


def test_verification_lease_is_fifo_and_exclusive():
    s1 = make_verification_session(total_chunks=1)
    s2 = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s1["id"], 0, digest="1" * 64)
    lease_complete_awaiting(s2["id"], 0, digest="2" * 64)

    first = verification_lease().json()
    second = verification_lease().json()
    # 按完成时间 FIFO：两个不同分片依次被领取，互不相同
    assert (first["session_id"], first["chunk_index"]) != (
        second["session_id"], second["chunk_index"])
    assert first["chunk_digest"] == "1" * 64
    assert second["chunk_digest"] == "2" * 64
    assert first["expires_at"] > first["issued_at"]
    # 状态中展示未过期的验收令牌到期时间
    st = client.get(f"/sessions/{first['session_id']}").json()
    chunk = st["chunks"][first["chunk_index"]]
    assert chunk["verification_expires_at"] == first["expires_at"]
    # 全部待验收分片均持有有效令牌：409 且不发放
    g3 = verification_lease()
    assert g3.status_code == 409
    assert g3.json()["detail"] == "no awaiting verification chunk available"


def test_verification_lease_ttl_is_applied_and_capped():
    s = make_verification_session(total_chunks=2)
    lease_complete_awaiting(s["id"], 0)
    lease_complete_awaiting(s["id"], 1)
    g = verification_lease(ttl=120).json()
    issued = datetime.fromisoformat(g["issued_at"])
    expires = datetime.fromisoformat(g["expires_at"])
    assert int((expires - issued).total_seconds()) == 120
    # 超上限被截断
    g2 = verification_lease(ttl=10 ** 9).json()
    expires2 = datetime.fromisoformat(g2["expires_at"])
    issued2 = datetime.fromisoformat(g2["issued_at"])
    assert int((expires2 - issued2).total_seconds()) <= 3600


def test_verification_lease_expired_can_be_reclaimed_with_rotated_token():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    g1 = verification_lease().json()
    expire_verification_lease(s["id"], 0)
    g2 = verification_lease().json()
    assert g2["chunk_index"] == 0
    assert g2["verification_token"] != g1["verification_token"]
    # 旧令牌已失效：decide 被拒
    r = decide(s["id"], 0, g1["verification_token"], "approve")
    assert r.status_code == 409
    # 库内每个分片至多一行验收租约
    n = get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    ).fetchone()["n"]
    assert n == 1
    # 令牌过期后读时 verification_expires_at 回到 null，但分片仍待验收
    expire_verification_lease(s["id"], 0)
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["chunks"][0]["status"] == "awaiting_verification"
    assert st["chunks"][0]["verification_expires_at"] is None


def test_verification_lease_never_double_grants_under_concurrency():
    s = make_verification_session(total_chunks=2)
    lease_complete_awaiting(s["id"], 0)
    lease_complete_awaiting(s["id"], 1)

    granted = []
    statuses = []

    def worker():
        r = verification_lease()
        statuses.append(r.status_code)
        if r.status_code == 201:
            body = r.json()
            granted.append((body["session_id"], body["chunk_index"]))

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(granted) == [(s["id"], 0), (s["id"], 1)]
    assert statuses.count(201) == 2
    assert statuses.count(409) == 14


def test_decide_approve_completes_chunk_and_allows_finalize():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    g = verification_lease().json()
    r = decide(s["id"], 0, g["verification_token"], "approve")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "completed"
    assert body["decision"] == "approve"
    assert body["chunk_digest"] == f"{0:064d}"
    assert body["idempotent_replay"] is False
    # 验收租约行被清除、决定落库
    n_lease = get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_leases WHERE session_id = ?",
        (s["id"],),
    ).fetchone()["n"]
    n_dec = get_conn().execute(
        "SELECT COUNT(*) AS n FROM verification_decisions WHERE session_id = ?",
        (s["id"],),
    ).fetchone()["n"]
    assert n_lease == 0 and n_dec == 1
    # approve 后可正常 finalize
    fr = client.post(f"/sessions/{s['id']}/finalize")
    assert fr.status_code == 200
    assert fr.json()["status"] == "finalized"


def test_decide_reject_clears_digest_and_returns_chunk_to_pending():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    g = verification_lease().json()
    r = decide(s["id"], 0, g["verification_token"], "reject")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "pending"
    assert body["decision"] == "reject"
    assert body["chunk_digest"] is None
    row = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    ).fetchone()
    assert row["status"] == "pending"
    assert row["chunk_digest"] is None and row["completed_at"] is None
    assert row["lease_token"] is None and row["lease_expires_at"] is None
    assert row["backoff_until"] is None
    # 可立即重新领取上传租约、重新上报
    token = lease(s["id"], 0).json()["lease_token"]
    rr = complete(s["id"], 0, token, "c" * 64)
    assert rr.json()["status"] == "awaiting_verification"
    # reject 后 finalize 仍被阻止
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 409


def test_decide_same_token_same_decision_replays_idempotently():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    g = verification_lease().json()
    r1 = decide(s["id"], 0, g["verification_token"], "approve")
    r2 = decide(s["id"], 0, g["verification_token"], "approve")
    assert r2.status_code == 200
    b1, b2 = r1.json(), r2.json()
    assert b2["idempotent_replay"] is True
    assert b2["decided_at"] == b1["decided_at"]
    assert b2["status"] == "completed"
    # 并发重放也全部成功且为首次结果
    results = []

    def worker():
        results.append(decide(s["id"], 0, g["verification_token"], "approve").status_code)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(results) == {200}


def test_decide_same_token_conflicting_decision_returns_409():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    g = verification_lease().json()
    assert decide(s["id"], 0, g["verification_token"], "approve").status_code == 200
    conflict_resp = decide(s["id"], 0, g["verification_token"], "reject")
    assert conflict_resp.status_code == 409
    assert conflict_resp.json()["original_decision"] == "approve"
    # 分片维持首次 approve 的结果
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["chunks"][0]["status"] == "completed"

    # reject 之后再 approve 同样冲突，分片维持 pending
    s2 = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s2["id"], 0)
    g2 = verification_lease().json()
    assert decide(s2["id"], 0, g2["verification_token"], "reject").status_code == 200
    conflict2 = decide(s2["id"], 0, g2["verification_token"], "approve")
    assert conflict2.status_code == 409
    assert conflict2.json()["original_decision"] == "reject"


def test_decide_requires_current_token_and_awaiting_chunk():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    g = verification_lease().json()
    # 未知令牌
    assert decide(s["id"], 0, "not-a-real-token", "approve").status_code == 409
    # 令牌过期
    expire_verification_lease(s["id"], 0)
    r = decide(s["id"], 0, g["verification_token"], "approve")
    assert r.status_code == 409 and "expired" in r.json()["detail"]
    # 重新领取后旧令牌仍然无效（已被换发）
    g2 = verification_lease().json()
    assert decide(s["id"], 0, g["verification_token"], "approve").status_code == 409
    # 新令牌决定成功
    assert decide(s["id"], 0, g2["verification_token"], "approve").status_code == 200
    # 未知会话/分片
    assert decide("nope", 0, g2["verification_token"], "approve").status_code == 404
    s3 = make_verification_session(total_chunks=1)
    assert decide(s3["id"], 0, "x", "approve").status_code == 409  # pending，非待验收


def test_decide_token_bound_to_its_own_chunk_and_session():
    # 令牌只对其签发的 (session, chunk) 有效：拿到 chunk0 的令牌后用于 chunk1 一律 409
    s = make_verification_session(total_chunks=2)
    lease_complete_awaiting(s["id"], 0)
    lease_complete_awaiting(s["id"], 1)
    g0 = verification_lease().json()
    assert g0["chunk_index"] == 0
    # chunk1 尚未持有验收令牌：用 chunk0 的令牌决定 chunk1 -> 409
    assert decide(s["id"], 1, g0["verification_token"], "approve").status_code == 409
    # chunk0 approve 后，拿其令牌去其他分片重放仍是 409（而非返回 chunk0 的结果）
    assert decide(s["id"], 0, g0["verification_token"], "approve").status_code == 200
    assert decide(s["id"], 1, g0["verification_token"], "approve").status_code == 409
    # 其他会话同样不可用
    s2 = make_verification_session(total_chunks=1)
    assert decide(s2["id"], 0, g0["verification_token"], "approve").status_code == 409


def test_verification_ignores_non_verification_sessions():
    s = make_session(total_chunks=2)
    token = lease(s["id"], 0).json()["lease_token"]
    r = complete(s["id"], 0, token, "a" * 64)
    assert r.json()["status"] == "completed"
    # 普通会话即使有完成分片，验收队列也无项可领
    assert verification_lease().status_code == 409
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["awaiting_verification_chunks"] == []
    rd = client.get(f"/sessions/{s['id']}/ready").json()
    assert rd["awaiting_verification_chunks"] == []
    assert rd["ready_chunks"] == [1]


def test_verification_interacts_with_dependencies():
    # 分片 2 依赖 0、1；前置处于 awaiting（未完成）时 2 仍被阻塞
    s = make_verification_session(
        total_chunks=3, dependencies={"2": [0, 1]})
    lease_complete_awaiting(s["id"], 0)
    lease_complete_awaiting(s["id"], 1)
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [0, 1]
    # 只 approve 一个仍阻塞
    g0 = verification_lease().json()
    assert g0["chunk_index"] == 0
    decide(s["id"], 0, g0["verification_token"], "approve")
    r = lease(s["id"], 2)
    assert r.status_code == 409 and r.json()["missing_dependencies"] == [1]
    # approve 第二个后解除阻塞
    g1 = verification_lease().json()
    assert g1["chunk_index"] == 1
    decide(s["id"], 1, g1["verification_token"], "approve")
    assert lease(s["id"], 2).status_code == 200


def test_dispatch_skips_and_reports_awaiting_verification_sessions():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    r = client.post("/dispatch/lease", json={
        "queue_key": "verify-q", "session_ids": [s["id"]]})
    assert r.status_code == 200
    body = r.json()
    assert body["granted"] is None
    assert body["unavailable"][0]["reason"] == "chunks_awaiting_verification"
    # approve 后会话无未完成分片：原因变为 no_unfinished_chunks
    g = verification_lease().json()
    decide(s["id"], 0, g["verification_token"], "approve")
    r = client.post("/dispatch/lease", json={
        "queue_key": "verify-q", "session_ids": [s["id"]]})
    assert r.json()["unavailable"][0]["reason"] == "no_unfinished_chunks"


def test_fork_copies_verification_required_and_rejects_awaiting_reuse():
    s = make_verification_session(total_chunks=2)
    lease_complete_awaiting(s["id"], 0)  # 0 待验收
    # 待验收分片不算 completed，不能被 fork 复用
    r = client.post(f"/sessions/{s['id']}/fork", json={
        "expected_revision": 0, "request_id": "fk-1", "reuse_chunks": [0]})
    assert r.status_code == 409
    assert r.json()["not_completed_chunks"] == [0]
    # approve 0 后可以复用；新会话复制验收开关
    g = verification_lease().json()
    decide(s["id"], 0, g["verification_token"], "approve")
    r = client.post(f"/sessions/{s['id']}/fork", json={
        "expected_revision": 0, "request_id": "fk-2", "reuse_chunks": [0]})
    assert r.status_code == 201
    assert r.json()["verification_required"] is True
    assert r.json()["reused_chunks"] == [0]
    assert r.json()["pending_chunks"] == [1]
    # 复用分片在新会话中直接为 completed（不需要再次验收）
    st = client.get(f"/sessions/{r.json()['id']}").json()
    assert st["chunks"][0]["status"] == "completed"
    assert st["awaiting_verification_chunks"] == []


def test_verification_request_validation():
    s = make_verification_session(total_chunks=1)
    lease_complete_awaiting(s["id"], 0)
    # TTL 必须为正
    assert verification_lease(ttl=0).status_code == 422
    g = verification_lease().json()
    # decision 只允许 approve / reject
    r = client.post(
        f"/verifications/{s['id']}/0/decide",
        json={"verification_token": g["verification_token"], "decision": "maybe"},
    )
    assert r.status_code == 422
    # 令牌不可为空
    r = client.post(
        f"/verifications/{s['id']}/0/decide",
        json={"verification_token": "", "decision": "approve"},
    )
    assert r.status_code == 422


def test_migration_adds_verification_columns_and_tables_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy_verify.db"
    conn = sqlite3.connect(legacy)
    # 验收特性之前的旧 SCHEMA：sessions 无 verification_required，无验收相关表
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
            base_retry_seconds INTEGER NOT NULL DEFAULT 5,
            max_retry_seconds INTEGER NOT NULL DEFAULT 300,
            revision INTEGER NOT NULL DEFAULT 0,
            source_session_id TEXT,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0, backoff_until TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-v', 1, 'dig', 'sha256', 'active',
                    '2026-01-01T00:00:00+00:00');
        INSERT INTO chunks (session_id, chunk_index) VALUES ('legacy-v', 0);
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    cols = {r[1] for r in migrated.execute(
        "PRAGMA table_info(sessions)").fetchall()}
    assert "verification_required" in cols
    tables = {r[0] for r in migrated.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()}
    assert "verification_leases" in tables
    assert "verification_decisions" in tables
    # 旧会话默认关闭验收：保持原行为
    row = migrated.execute(
        "SELECT verification_required FROM sessions WHERE id = 'legacy-v'"
    ).fetchone()
    assert row[0] == 0

    # 新表可正常写入与 UPSERT（过期重领）
    migrated.execute(
        "INSERT INTO verification_leases (token_hash, session_id, chunk_index,"
        " issued_at, expires_at) VALUES"
        " ('h1', 'legacy-v', 0, '2026-01-01T00:00:00+00:00',"
        "  '2000-01-01T00:00:00+00:00')"
    )
    migrated.execute(
        "INSERT INTO verification_leases (token_hash, session_id, chunk_index,"
        " issued_at, expires_at) VALUES"
        " ('h2', 'legacy-v', 0, '2026-01-02T00:00:00+00:00',"
        "  '2999-01-01T00:00:00+00:00')"
        " ON CONFLICT(session_id, chunk_index) DO UPDATE SET"
        " token_hash = excluded.token_hash, issued_at = excluded.issued_at,"
        " expires_at = excluded.expires_at"
    )
    migrated.commit()
    row = migrated.execute(
        "SELECT token_hash FROM verification_leases"
        " WHERE session_id = 'legacy-v' AND chunk_index = 0"
    ).fetchone()
    assert row[0] == "h2"
    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()
