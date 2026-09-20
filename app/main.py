"""断点分片上传协调 API。

仅协调元数据：不接收、不保存任何文件内容。
- 创建会话：记录总分片数与期望文件摘要，可选提交分片依赖图（越界/自依赖/成环将被拒绝），
  可选失败退避参数 base_retry_seconds（默认 5）与 max_retry_seconds（默认 300）
- 领取租约：同一分片同一时刻仅一个有效租约，过期后可重新领取；
  声明了依赖的分片须等前置分片全部完成后才可领取；失败退避冷却中的分片不可领取
- 上报完成：需携带匹配租约令牌与分片摘要，重复上报幂等；完成后清除退避冷却、保留累计失败次数
- 上报失败：需携带当前有效租约令牌，成功后原子清除租约并递增失败次数，
  第 n 次失败冷却 min(max, base×2^(n-1)) 秒，冷却期间单片/批量领取均跳过该分片
- 完成纠错回退（reopen）：凭目标分片完成时保留的租约令牌与摘要发起，仅允许未提交会话中
  已完成且凭据匹配的分片；同一事务内将目标及依赖图中的全部传递下游重置为 pending
  （清除摘要、完成时间、有效租约与退避截止时间，保留累计失败次数），会话 revision 递增 1；
  任一校验失败不产生部分回退，并发领取不会看到中间状态
- 提交会话：仅当全部分片完成，返回按序分片清单，否则列出缺失分片；
  会话使用 sha256-merkle-v1 时先按分片序号构建 Merkle 树并与 file_digest
  常量时间比较，不匹配返回 409 及 computed_digest、会话保持 active
- 交付凭证（delivery ticket）：为已 finalized 会话签发一次性清单领取凭证，
  POST /sessions/{id}/delivery-tickets 接受 expires_in_seconds，返回仅展示一次的
  随机 token（库内只存其 SHA-256、会话、到期时间与状态）；POST /delivery-tickets/redeem
  接受 token 与 request_id，在同一写事务内校验未过期、未撤销、未消费后返回按序 manifest，
  并发的不同 request_id 只有一个成功，同一 request_id 重放返回首次相同结果；
  POST /delivery-tickets/revoke 撤销凭证，已消费凭证不可撤销、已撤销/已消费凭证不可领取
- 进度分支（fork）：POST /sessions/{id}/fork 携带 expected_revision、幂等 request_id
  与 reuse_chunks，仅可从 active 会话创建新会话；复用分片必须全部已完成且包含其全部
  传递前置分片，否则返回 409 及缺失项且不落库；新会话复制会话配置与依赖图，复用分片
  复制摘要与完成时间为 completed，其余为 pending，不复制租约、退避与失败次数；
  同一源会话 + request_id 重放返回首次新会话，参数变化返回 409
- 就绪查询：会话与逐分片状态（含各分片依赖、失败次数与冷却截止时间，以及会话 revision
  与 source_session_id）
- 就绪查询：当前可领取的分片、被依赖阻塞的分片、退避冷却中的分片以及待验收分片
- 分片验收队列（可选，创建会话 verification_required=true 启用）：启用后完成上报先进入
  awaiting_verification（不再直接 completed），存在待验收分片时不可 finalize；
  POST /verifications/lease 在同一写锁事务内按完成时间从早到晚领取一个待验收分片，
  签发有 TTL 的验收令牌（仅存 SHA-256），未过期独占、过期可重领、并发请求不会重复领取；
  POST /verifications/{session_id}/{chunk_index}/decide 携带当前令牌 approve 或 reject：
  approve 转 completed，reject 在同一事务内原子清除摘要并回到 pending（可重新上传）；
  同一令牌同一决定重放幂等、冲突决定返回 409。未启用会话保持原行为
- 跨会话公平调度：POST /dispatch/lease 携带 queue_key、候选 session_ids 与 lease_ttl_seconds；
  候选会话去重后按（创建时间, id）稳定排序，服务端按 queue_key 持久化最近一次成功会话，
  在同一写锁事务内从其后循环扫描，从首个存在可领取分片且仍有并发槽位的会话领取序号最低的
  分片；资格判断复用依赖、退避、有效租约与 max_active_leases 规则；并发请求不会重复发放。
  无可发放项时返回各会话的不可用原因及最早 next_retry_at 且不推进游标；成功后才更新游标，
  使连续请求在可用会话间轮转
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .db import DB_LOCK, get_conn, init_db
from .schemas import (
    MERKLE_DIGEST_ALGORITHM,
    BatchLeaseItem,
    BatchLeaseRequest,
    BatchLeaseResponse,
    BlockedChunk,
    CHUNK_STATUS_AWAITING_VERIFICATION,
    ChunkStatus,
    CompleteRequest,
    CompleteResponse,
    CoolingChunk,
    CreateDeliveryTicketRequest,
    CreateSessionRequest,
    DISPATCH_REASON_BACKOFF,
    DISPATCH_REASON_BLOCKED,
    DISPATCH_REASON_DONE,
    DISPATCH_REASON_LEASED,
    DISPATCH_REASON_AWAITING_VERIFICATION,
    DISPATCH_REASON_SESSION_FINALIZED,
    DISPATCH_REASON_SESSION_FULL,
    DISPATCH_REASON_SESSION_NOT_FOUND,
    DeliveryTicketResponse,
    DispatchGrant,
    DispatchLeaseRequest,
    DispatchLeaseResponse,
    DispatchUnavailableSession,
    FailRequest,
    FailResponse,
    FinalizeResponse,
    ForkSessionRequest,
    ForkSessionResponse,
    LeaseRequest,
    LeaseResponse,
    ManifestEntry,
    ReadyResponse,
    RedeemDeliveryTicketRequest,
    RedeemDeliveryTicketResponse,
    ReopenAffectedChunk,
    ReopenRequest,
    ReopenResponse,
    RevokeDeliveryTicketRequest,
    RevokeDeliveryTicketResponse,
    SessionResponse,
    SessionStatusResponse,
    TICKET_STATUS_ACTIVE,
    TICKET_STATUS_CONSUMED,
    TICKET_STATUS_REVOKED,
    VERIFICATION_DECISION_APPROVE,
    VERIFICATION_DECISION_REJECT,
    VerificationDecideRequest,
    VerificationDecideResponse,
    VerificationLeaseRequest,
    VerificationLeaseResponse,
    is_valid_merkle_digest,
)

DEFAULT_LEASE_TTL_SECONDS = int(os.environ.get("DEFAULT_LEASE_TTL_SECONDS", "300"))
MAX_LEASE_TTL_SECONDS = int(os.environ.get("MAX_LEASE_TTL_SECONDS", "3600"))
DEFAULT_VERIFICATION_TTL_SECONDS = int(
    os.environ.get("DEFAULT_VERIFICATION_TTL_SECONDS", str(DEFAULT_LEASE_TTL_SECONDS))
)
MAX_VERIFICATION_TTL_SECONDS = int(
    os.environ.get("MAX_VERIFICATION_TTL_SECONDS", str(MAX_LEASE_TTL_SECONDS))
)
DEFAULT_BASE_RETRY_SECONDS = int(os.environ.get("DEFAULT_BASE_RETRY_SECONDS", "5"))
DEFAULT_MAX_RETRY_SECONDS = int(os.environ.get("DEFAULT_MAX_RETRY_SECONDS", "300"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # 启动时建库建表
    yield


app = FastAPI(
    title="Chunked Upload Coordinator",
    description="断点分片上传协调服务：只管理元数据（会话 / 租约 / 分片摘要），不接收文件内容。",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------- 工具函数

def get_session_or_404(session_id: str):
    row = get_conn().execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="session not found")
    return row


def get_chunk_or_404(session_id: str, chunk_index: int):
    row = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
        (session_id, chunk_index),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="chunk not found")
    return row


def lease_valid(chunk, now: datetime) -> bool:
    return (
        chunk["lease_expires_at"] is not None
        and parse_ts(chunk["lease_expires_at"]) > now
    )


def backoff_active(chunk, now: datetime) -> bool:
    """该分片是否正处于失败退避冷却中（backoff_until 为空或已过期均视为未冷却）。"""
    return (
        chunk["backoff_until"] is not None
        and parse_ts(chunk["backoff_until"]) > now
    )


def backoff_delay(session, failure_count: int) -> int:
    """第 n 次失败的冷却秒数：min(max_retry_seconds, base_retry_seconds × 2^(n-1))。"""
    base = session["base_retry_seconds"]
    cap = session["max_retry_seconds"]
    return min(cap, base * (2 ** (failure_count - 1)))


def merkle_root_sha256(digests: list[str]) -> str:
    """sha256-merkle-v1：按分片序号升序的摘要列表（64 位小写十六进制）计算 Merkle 根。

    字节级规则：
    - 叶节点 = SHA-256(0x00 || 摘要的原始 32 字节)
    - 父节点 = SHA-256(0x01 || 左节点 32 字节 || 右节点 32 字节)
    - 每层节点数为奇数时，末节点复制自身后配对
    - 逐层归约直至单一根，返回其小写十六进制编码
    """
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest() for d in digests]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])  # 奇数末节点复制自身后配对
        level = [
            hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest()
            for i in range(0, len(level), 2)
        ]
    return level[0].hex()


def load_dependencies(session_id: str) -> dict[int, list[int]]:
    """读取会话的依赖图：chunk_index -> 升序前置分片列表。"""
    rows = get_conn().execute(
        "SELECT chunk_index, depends_on FROM chunk_dependencies"
        " WHERE session_id = ? ORDER BY chunk_index, depends_on",
        (session_id,),
    ).fetchall()
    deps: dict[int, list[int]] = {}
    for row in rows:
        deps.setdefault(row["chunk_index"], []).append(row["depends_on"])
    return deps


def missing_transitive_prerequisites(
    deps: dict[int, list[int]], reuse: set[int]
) -> list[int]:
    """临界区内调用：返回 reuse 集合中各分片依赖图上的全部传递前置分片中，
    未被 reuse 集合包含的部分（升序）。空列表表示传递闭包完整。"""
    missing: set[int] = set()
    for chunk_index in reuse:
        stack = list(deps.get(chunk_index, []))
        while stack:
            pred = stack.pop()
            if pred in reuse or pred in missing:
                continue
            missing.add(pred)
            stack.extend(deps.get(pred, []))
    return sorted(missing)


def incomplete_dependencies(session_id: str, chunk_index: int) -> list[int]:
    """返回该分片尚未完成（含待验收）的前置分片（升序）。"""
    rows = get_conn().execute(
        "SELECT d.depends_on FROM chunk_dependencies d"
        " JOIN chunks c ON c.session_id = d.session_id AND c.chunk_index = d.depends_on"
        " WHERE d.session_id = ? AND d.chunk_index = ? AND c.status != 'completed'"
        " ORDER BY d.depends_on",
        (session_id, chunk_index),
    ).fetchall()
    return [row["depends_on"] for row in rows]


def collect_reopen_affected(session_id: str, target: int) -> list[tuple[int, int]]:
    """临界区内调用：沿依赖图找出目标分片的全部传递下游，返回
    （分片序号, 拓扑层级）列表，按（层级, 序号）升序稳定排列。

    受影响集合 = 目标分片 + 全部传递下游；目标层级为 0，其余节点取受影响
    子图中的最长路径层级（Kahn 分层：节点仅在全部受影响前置处理完后进入
    下一层），保证按（层级, 序号）排序后任意受影响前置都排在其下游之前。
    依赖图在创建会话时已校验无环，分层必然收敛。
    """
    deps = load_dependencies(session_id)  # chunk_index -> 升序前置列表
    children: dict[int, list[int]] = {}
    for chunk_index, preds in deps.items():
        for pred in preds:
            children.setdefault(pred, []).append(chunk_index)

    # 沿反向边（前置 -> 依赖它的分片）收集全部传递下游
    affected = {target}
    stack = [target]
    while stack:
        node = stack.pop()
        for child in children.get(node, []):
            if child not in affected:
                affected.add(child)
                stack.append(child)

    # Kahn 分层：仅统计受影响子图内的入度，逐波推进得到最长路径层级
    indegree = {
        n: sum(1 for p in deps.get(n, []) if p in affected) for n in affected
    }
    level: dict[int, int] = {}
    frontier = [n for n in affected if indegree[n] == 0]
    current = 0
    while frontier:
        nxt: list[int] = []
        for node in frontier:
            level[node] = current
            for child in children.get(node, []):
                if child in affected:
                    indegree[child] -= 1
                    if indegree[child] == 0:
                        nxt.append(child)
        frontier = nxt
        current += 1

    return sorted(level.items(), key=lambda item: (item[1], item[0]))


def count_active_leases(session_id: str, now: datetime) -> int:
    """会话内当前有效（未过期）租约数；已完成/待验收分片与过期租约均不计入。"""
    return get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunks"
        " WHERE session_id = ? AND status = 'leased'"
        " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
        (session_id, now.isoformat()),
    ).fetchone()["n"]


def earliest_wakeup(session_id: str, now: datetime) -> Optional[datetime]:
    """会话内最早的可重试时刻：有效租约的最早过期时间与各分片退避冷却的最早结束时间
    中更早者；均不存在（或均已过期）时返回 None。待验收分片不计入（由验收队列处理）。"""
    conn = get_conn()
    lease_row = conn.execute(
        "SELECT MIN(lease_expires_at) AS earliest FROM chunks"
        " WHERE session_id = ? AND status = 'leased'"
        " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
        (session_id, now.isoformat()),
    ).fetchone()
    backoff_row = conn.execute(
        "SELECT MIN(backoff_until) AS earliest FROM chunks"
        " WHERE session_id = ? AND status = 'pending'"
        " AND backoff_until IS NOT NULL AND backoff_until > ?",
        (session_id, now.isoformat()),
    ).fetchone()
    candidates = [
        parse_ts(lease_row["earliest"]) if lease_row["earliest"] else None,
        parse_ts(backoff_row["earliest"]) if backoff_row["earliest"] else None,
    ]
    candidates = [ts for ts in candidates if ts is not None]
    return min(candidates) if candidates else None


def claimable_chunks(session_id: str, now: datetime) -> tuple[list[int], int]:
    """临界区内调用：返回（按序号升序的可领取分片列表, 被依赖阻塞的未完成分片数）。

    可领取 = 未完成且非待验收、无有效租约（过期租约视为空闲）、不在失败退避冷却中且前置分片
    全部完成；被阻塞 = 未完成（非待验收）且至少一个前置分片尚未完成（无论该分片当前是否持有
    有效租约或处于冷却）。待验收分片既不可领取也不计入阻塞，由验收队列单独处理。
    """
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    deps = load_dependencies(session_id)
    ready: list[int] = []
    blocked = 0
    for row in rows:
        if row["status"] in ("completed", CHUNK_STATUS_AWAITING_VERIFICATION):
            continue
        chunk_index = row["chunk_index"]
        if any(d not in completed for d in deps.get(chunk_index, [])):
            blocked += 1
        elif not lease_valid(row, now) and not backoff_active(row, now):
            ready.append(chunk_index)
    return ready, blocked


def conflict(message: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": message, **extra})


def dispatch_session_state(session_id: str, now: datetime):
    """临界区内调用：评估候选会话对跨会话调度的状态，全程复用单片/批量领取的同一套规则
    （有效租约、失败退避、依赖完成情况、max_active_leases 槽位均以同一谓词判定）。

    返回 (session_row_or_None, ready, blocked, backoff_n, leased_n, active_leases, awaiting_n)：
    - session_row_or_None：会话不存在时为 None（此时其余字段无意义）
    - ready：按序号升序、当前可领取（未完成 / 无有效租约 / 不在退避冷却 / 前置全完成）的分片
    - blocked：被未完成前置阻塞的未完成分片数
    - backoff_n：前置已完成但正处于失败退避冷却中的未完成分片数
    - leased_n：前置已完成、未在冷却但持有有效租约的未完成分片数
    - active_leases：会话当前有效（未过期）租约总数
    - awaiting_n：处于 awaiting_verification 的分片数（验收队列独占处理，不可被上传调度领取）
    已完成分片不计入以上任一分桶；各分桶对未完成分片构成完备划分。
    """
    conn = get_conn()
    session = conn.execute(
        "SELECT * FROM sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if session is None:
        return None, [], 0, 0, 0, 0, 0
    rows = conn.execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    deps = load_dependencies(session_id)
    ready: list[int] = []
    blocked = 0
    backoff_n = 0
    leased_n = 0
    awaiting_n = 0
    for row in rows:
        if row["status"] == "completed":
            continue
        if row["status"] == CHUNK_STATUS_AWAITING_VERIFICATION:
            awaiting_n += 1
            continue
        chunk_index = row["chunk_index"]
        if any(d not in completed for d in deps.get(chunk_index, [])):
            blocked += 1
            continue
        if lease_valid(row, now):
            leased_n += 1
        elif backoff_active(row, now):
            backoff_n += 1
        else:
            ready.append(chunk_index)
    active_leases = count_active_leases(session_id, now)
    return session, ready, blocked, backoff_n, leased_n, active_leases, awaiting_n


# ---------------------------------------------------------------- 会话

@app.post(
    "/sessions",
    status_code=201,
    response_model=SessionResponse,
    tags=["sessions"],
    summary="创建上传会话",
)
def create_session(req: CreateSessionRequest):
    now = utcnow()
    session_id = uuid.uuid4().hex
    deps = req.dependencies or {}
    with DB_LOCK:
        conn = get_conn()
        conn.execute(
            "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status,"
            " max_active_leases, base_retry_seconds, max_retry_seconds,"
            " verification_required, created_at)"
            " VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)",
            (session_id, req.total_chunks, req.file_digest, req.digest_algorithm,
             req.max_active_leases, req.base_retry_seconds, req.max_retry_seconds,
             1 if req.verification_required else 0, now.isoformat()),
        )
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index) VALUES (?, ?)",
            [(session_id, i) for i in range(req.total_chunks)],
        )
        conn.executemany(
            "INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on) VALUES (?, ?, ?)",
            [(session_id, idx, dep) for idx, dep_list in deps.items() for dep in dep_list],
        )
        conn.commit()
    return SessionResponse(
        id=session_id,
        total_chunks=req.total_chunks,
        file_digest=req.file_digest,
        digest_algorithm=req.digest_algorithm,
        status="active",
        source_session_id=None,
        max_active_leases=req.max_active_leases,
        base_retry_seconds=req.base_retry_seconds,
        max_retry_seconds=req.max_retry_seconds,
        verification_required=req.verification_required,
        created_at=now,
    )


@app.get(
    "/sessions/{session_id}",
    response_model=SessionStatusResponse,
    tags=["sessions"],
    summary="查询会话状态",
)
def get_session_status(session_id: str):
    session = get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    deps = load_dependencies(session_id)
    now = utcnow()
    # 待验收分片当前持有的验收令牌（含已过期）：(session_id, chunk_index) 至多一行
    verification_rows = get_conn().execute(
        "SELECT chunk_index, expires_at FROM verification_leases WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    verification_expiry = {r["chunk_index"]: r["expires_at"] for r in verification_rows}

    chunks: list[ChunkStatus] = []
    missing: list[int] = []
    awaiting: list[int] = []
    for row in rows:
        status = row["status"]
        lease_expires_at = parse_ts(row["lease_expires_at"]) if row["lease_expires_at"] else None
        if status == "leased" and (lease_expires_at is None or lease_expires_at <= now):
            status = "pending"  # 租约已过期，读时按可领取展示
            lease_expires_at = None
        # 冷却截止时间按读时口径展示：已过期的退避不再报告
        retry_after = parse_ts(row["backoff_until"]) if row["backoff_until"] else None
        if retry_after is not None and retry_after <= now:
            retry_after = None
        # 待验收分片展示验收令牌到期时间：未领取或令牌已过期时为 null（仍可被重领）
        verification_expires_at = None
        if row["status"] == CHUNK_STATUS_AWAITING_VERIFICATION:
            awaiting.append(row["chunk_index"])
            raw_verification_expiry = verification_expiry.get(row["chunk_index"])
            if raw_verification_expiry is not None:
                parsed = parse_ts(raw_verification_expiry)
                if parsed > now:
                    verification_expires_at = parsed
        if row["status"] != "completed":
            missing.append(row["chunk_index"])
        chunks.append(
            ChunkStatus(
                chunk_index=row["chunk_index"],
                status=status,
                lease_expires_at=lease_expires_at,
                chunk_digest=row["chunk_digest"],
                completed_at=parse_ts(row["completed_at"]) if row["completed_at"] else None,
                dependencies=deps.get(row["chunk_index"], []),
                failure_count=row["failure_count"] or 0,
                retry_after=retry_after,
                verification_expires_at=verification_expires_at,
            )
        )

    return SessionStatusResponse(
        id=session["id"],
        status=session["status"],
        total_chunks=session["total_chunks"],
        completed_chunks=session["total_chunks"] - len(missing),
        missing_chunks=missing,
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        source_session_id=session["source_session_id"],
        revision=session["revision"],
        max_active_leases=session["max_active_leases"],
        base_retry_seconds=session["base_retry_seconds"],
        max_retry_seconds=session["max_retry_seconds"],
        verification_required=bool(session["verification_required"]),
        awaiting_verification_chunks=awaiting,
        active_leases=sum(
            1 for c in chunks
            if c.status == "leased" and c.lease_expires_at is not None
        ),
        created_at=parse_ts(session["created_at"]),
        finalized_at=parse_ts(session["finalized_at"]) if session["finalized_at"] else None,
        chunks=chunks,
    )


# ---------------------------------------------------------------- 进度分支

def build_fork_response(
    new_id: str,
    source_id: str,
    request_id: str,
    session,
    reuse: list[int],
    created_at: datetime,
    idempotent_replay: bool,
) -> ForkSessionResponse:
    reuse_set = set(reuse)
    return ForkSessionResponse(
        id=new_id,
        source_session_id=source_id,
        request_id=request_id,
        total_chunks=session["total_chunks"],
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        status="active",
        revision=0,
        reused_chunks=reuse,
        pending_chunks=[i for i in range(session["total_chunks"]) if i not in reuse_set],
        max_active_leases=session["max_active_leases"],
        base_retry_seconds=session["base_retry_seconds"],
        max_retry_seconds=session["max_retry_seconds"],
        verification_required=bool(session["verification_required"]),
        created_at=created_at,
        idempotent_replay=idempotent_replay,
    )


@app.post(
    "/sessions/{session_id}/fork",
    response_model=ForkSessionResponse,
    status_code=201,
    tags=["sessions"],
    summary="从 active 会话分叉出新的上传会话（复用已完成分片进度）",
    responses={
        200: {"description": "同一 request_id 的幂等重放（idempotent_replay=true，返回首次新会话）"},
        409: {"description": "源会话已提交、修订号不匹配、复用分片未完成或缺少传递前置、"
                             "同一 request_id 参数变化"},
    },
)
def fork_session(session_id: str, req: ForkSessionRequest):
    """在同一 DB_LOCK 事务内：校验源会话存在且 active → 校验 expected_revision 与源会话
    revision 一致 → 校验 reuse_chunks 中每项均已完成（越界序号同样视为未完成）→ 校验
    reuse_chunks 包含其全部传递前置分片。任一校验失败返回 409 且不落库。

    通过后复制会话配置（total_chunks / file_digest / digest_algorithm /
    max_active_leases / 退避参数）与完整依赖图；复用分片复制摘要与完成时间为 completed，
    其余分片初始化为 pending——不复制租约、退避截止时间与累计失败次数。
    （源会话, request_id）唯一：参数一致的重放返回首次创建的新会话，参数变化返回 409。
    """
    now = utcnow()
    reuse = sorted(set(req.reuse_chunks))
    reuse_set = set(reuse)
    with DB_LOCK:
        conn = get_conn()
        session = get_session_or_404(session_id)

        # 幂等优先：同一（源会话, request_id）已存在时，按参数决定重放或冲突
        existing = conn.execute(
            "SELECT * FROM fork_requests WHERE source_session_id = ? AND request_id = ?",
            (session_id, req.request_id),
        ).fetchone()
        if existing is not None:
            params_changed = (
                existing["expected_revision"] != req.expected_revision
                or json.loads(existing["reuse_chunks"]) != reuse
            )
            if params_changed:
                raise HTTPException(
                    status_code=409,
                    detail="request_id already used with different parameters",
                )
            new_session = conn.execute(
                "SELECT * FROM sessions WHERE id = ?",
                (existing["new_session_id"],),
            ).fetchone()
            return JSONResponse(
                status_code=200,
                content=build_fork_response(
                    new_id=new_session["id"],
                    source_id=session_id,
                    request_id=req.request_id,
                    session=new_session,
                    reuse=reuse,
                    created_at=parse_ts(existing["created_at"]),
                    idempotent_replay=True,
                ).model_dump(mode="json"),
            )

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if session["revision"] != req.expected_revision:
            return conflict(
                "revision mismatch",
                expected_revision=req.expected_revision,
                current_revision=session["revision"],
            )

        rows = conn.execute(
            "SELECT chunk_index, status, chunk_digest, completed_at FROM chunks"
            " WHERE session_id = ?",
            (session_id,),
        ).fetchall()
        by_index = {r["chunk_index"]: r for r in rows}
        not_completed = sorted(
            idx for idx in reuse
            if idx not in by_index or by_index[idx]["status"] != "completed"
        )
        if not_completed:
            return conflict(
                "reuse_chunks must reference completed chunks only",
                not_completed_chunks=not_completed,
            )

        deps = load_dependencies(session_id)
        missing_prerequisites = missing_transitive_prerequisites(deps, reuse_set)
        if missing_prerequisites:
            return conflict(
                "reuse_chunks must include all transitive prerequisites",
                missing_prerequisites=missing_prerequisites,
            )

        # 全部校验先于写入：以下任一失败都不会发生，整个复制在同一事务/临界区内完成
        new_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status,"
            " max_active_leases, base_retry_seconds, max_retry_seconds, revision,"
            " source_session_id, verification_required, created_at)"
            " VALUES (?, ?, ?, ?, 'active', ?, ?, ?, 0, ?, ?, ?)",
            (new_id, session["total_chunks"], session["file_digest"],
             session["digest_algorithm"], session["max_active_leases"],
             session["base_retry_seconds"], session["max_retry_seconds"],
             session_id, session["verification_required"], now.isoformat()),
        )
        # 复用分片：复制摘要与完成时间为 completed；其余默认 pending。
        # 不复制 lease_token / lease_expires_at / backoff_until / failure_count
        reused_rows = [
            (new_id, idx, by_index[idx]["chunk_digest"], by_index[idx]["completed_at"])
            for idx in reuse
        ]
        pending_rows = [
            (new_id, idx)
            for idx in range(session["total_chunks"])
            if idx not in reuse_set
        ]
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index, status, chunk_digest,"
            " completed_at) VALUES (?, ?, 'completed', ?, ?)",
            reused_rows,
        )
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index) VALUES (?, ?)",
            pending_rows,
        )
        # 完整复制依赖图：新会话的调度约束与源会话一致
        conn.execute(
            "INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on)"
            " SELECT ?, chunk_index, depends_on FROM chunk_dependencies WHERE session_id = ?",
            (new_id, session_id),
        )
        conn.execute(
            "INSERT INTO fork_requests (source_session_id, request_id, new_session_id,"
            " expected_revision, reuse_chunks, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (session_id, req.request_id, new_id, req.expected_revision,
             json.dumps(reuse), now.isoformat()),
        )
        conn.commit()

    return build_fork_response(
        new_id=new_id,
        source_id=session_id,
        request_id=req.request_id,
        session=session,
        reuse=reuse,
        created_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 就绪查询

@app.get(
    "/sessions/{session_id}/ready",
    response_model=ReadyResponse,
    tags=["sessions"],
    summary="查询当前可领取的分片与被依赖阻塞的分片",
)
def get_ready_chunks(session_id: str):
    get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    deps = load_dependencies(session_id)
    now = utcnow()

    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    ready: list[int] = []
    blocked: list[BlockedChunk] = []
    cooling: list[CoolingChunk] = []
    awaiting: list[int] = []
    for row in rows:
        chunk_index = row["chunk_index"]
        if row["status"] == "completed":
            continue
        if row["status"] == CHUNK_STATUS_AWAITING_VERIFICATION:
            # 已完成上报、等待验收：既不可领取也不属于阻塞/冷却，验收后才离开该状态
            awaiting.append(chunk_index)
            continue
        missing_deps = [d for d in deps.get(chunk_index, []) if d not in completed]
        if missing_deps:
            blocked.append(
                BlockedChunk(chunk_index=chunk_index, missing_dependencies=missing_deps)
            )
        elif backoff_active(row, now):
            # 失败退避冷却中：不可被单片或批量领取，冷却结束后自动恢复
            cooling.append(
                CoolingChunk(
                    chunk_index=chunk_index,
                    failure_count=row["failure_count"] or 0,
                    retry_after=parse_ts(row["backoff_until"]),
                )
            )
        elif not lease_valid(row, now):
            # 无租约或租约已过期，且前置已全部完成：立即可领取
            ready.append(chunk_index)

    return ReadyResponse(
        session_id=session_id,
        ready_chunks=ready,
        blocked_chunks=blocked,
        cooling_chunks=cooling,
        awaiting_verification_chunks=awaiting,
    )


# ---------------------------------------------------------------- 租约

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/lease",
    response_model=LeaseResponse,
    tags=["leases"],
    summary="领取分片租约（限时，独占；前置分片未完成时返回 409）",
    responses={409: {"description": "租约冲突；前置未完成时响应体包含 missing_dependencies"}},
)
def claim_lease(
    session_id: str,
    chunk_index: int,
    req: Optional[LeaseRequest] = Body(default=None),
):
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req is not None and req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            raise HTTPException(status_code=409, detail="chunk already completed")
        if chunk["status"] == CHUNK_STATUS_AWAITING_VERIFICATION:
            raise HTTPException(
                status_code=409,
                detail="chunk is awaiting verification and cannot be leased again",
            )
        if lease_valid(chunk, now):
            return conflict(
                "chunk lease already held",
                lease_expires_at=chunk["lease_expires_at"],
            )
        if backoff_active(chunk, now):
            # 失败退避冷却中：单片领取同样拒绝，直至 retry_after
            return conflict(
                "chunk in failure backoff",
                failure_count=chunk["failure_count"] or 0,
                retry_after=chunk["backoff_until"],
            )
        missing_deps = incomplete_dependencies(session_id, chunk_index)
        if missing_deps:
            return conflict(
                "chunk dependencies not completed",
                missing_dependencies=missing_deps,
            )
        # 会话级并发上限：已占满槽位（过期租约不计入）时拒绝，避免单片接口绕过批量上限
        max_active = session["max_active_leases"]
        active = count_active_leases(session_id, now)
        if max_active is not None and active >= max_active:
            return conflict(
                "session max active leases reached",
                max_active_leases=max_active,
                active_leases=active,
            )

        token = secrets.token_urlsafe(32)
        expires_at = now + timedelta(seconds=ttl)
        get_conn().execute(
            "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (token, expires_at.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return LeaseResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        lease_token=token,
        expires_at=expires_at,
    )


@app.post(
    "/sessions/{session_id}/leases/batch",
    response_model=BatchLeaseResponse,
    tags=["leases"],
    summary="会话级原子批量领取租约（按分片序号，受并发上限约束）",
)
def claim_leases_batch(session_id: str, req: BatchLeaseRequest):
    """一次领取至多 count 个租约：资格计算与写入在同一临界区，保证并发请求不重复分配、
    不突破 max_active_leases；数量不超过请求值与剩余并发槽位；无可领取项时返回空列表。"""
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")

        # 资格计算与写入均在 DB_LOCK 临界区内：其它领取请求无法穿插看到中间状态
        ready, blocked = claimable_chunks(session_id, now)
        max_active = session["max_active_leases"]
        active = count_active_leases(session_id, now)
        remaining = None if max_active is None else max(0, max_active - active)
        quota = req.count if remaining is None else min(req.count, remaining)

        granted: list[BatchLeaseItem] = []
        if quota > 0:
            expires_at = now + timedelta(seconds=ttl)
            conn = get_conn()
            for chunk_index in ready[:quota]:
                token = secrets.token_urlsafe(32)
                conn.execute(
                    "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
                    " WHERE session_id = ? AND chunk_index = ?",
                    (token, expires_at.isoformat(), session_id, chunk_index),
                )
                granted.append(
                    BatchLeaseItem(
                        chunk_index=chunk_index,
                        lease_token=token,
                        expires_at=expires_at,
                    )
                )
            conn.commit()

        active_after = active + len(granted)
        # 无可领取项时，调用方可据 next_expiry 等待：有效租约过期或退避冷却结束，取更早者
        next_expiry = earliest_wakeup(session_id, now)

    return BatchLeaseResponse(
        session_id=session_id,
        requested=req.count,
        granted=granted,
        active=active_after,
        blocked=blocked,
        next_expiry=next_expiry,
        max_active_leases=max_active,
        remaining_slots=(
            None if max_active is None else max(0, max_active - active_after)
        ),
    )


# ---------------------------------------------------------------- 跨会话公平调度

@app.post(
    "/dispatch/lease",
    response_model=DispatchLeaseResponse,
    tags=["dispatch"],
    summary="跨会话公平调度：在候选会话间轮转发一片分片租约",
)
def dispatch_lease(req: DispatchLeaseRequest):
    """候选会话去重后按（创建时间, id）稳定排序；读取 queue_key 的持久化游标（最近一次
    成功发放的会话），在同一 DB_LOCK 临界区内从游标之后的候选会话开始循环扫描，向首个
    “存在可领取分片且仍有并发槽位”的会话发放其序号最低分片的租约。资格判定复用单片/批量
    领取的同一规则：未完成、无有效租约（过期视为空闲）、不在退避冷却中、前置全部完成、
    且会话未提交、未达 max_active_leases。并发请求经同一写锁串行化，不会重复发放。

    无可发放项时不做任何写入（游标保持不变），返回各候选会话的不可用原因（按扫描顺序）
    与其中最早的 next_retry_at；成功发放后才在同一事务内把游标推进到被发放的会话，
    使连续请求在可用会话之间轮转。
    """
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    candidate_ids = list(dict.fromkeys(req.session_ids))  # 去重并保留首次出现顺序

    with DB_LOCK:
        conn = get_conn()

        # 候选会话按（创建时间, id）稳定排序；不存在的会话排最后并按 id 排序，顺序确定
        placeholders = ",".join("?" for _ in candidate_ids)
        existing_rows = conn.execute(
            f"SELECT * FROM sessions WHERE id IN ({placeholders})",
            candidate_ids,
        ).fetchall()
        existing = {row["id"]: row for row in existing_rows}
        missing_ids = sorted(sid for sid in candidate_ids if sid not in existing)
        ordered_existing_ids = [
            row["id"]
            for row in sorted(existing.values(), key=lambda r: (r["created_at"], r["id"]))
        ]
        ordered_ids = ordered_existing_ids + missing_ids

        # 队列游标：最近一次成功发放的会话；不存在或不在本次候选中时从排序首位开始
        cursor_row = conn.execute(
            "SELECT last_session_id FROM dispatch_cursors WHERE queue_key = ?",
            (req.queue_key,),
        ).fetchone()
        cursor_session_id = cursor_row["last_session_id"] if cursor_row else None
        if cursor_session_id in ordered_ids:
            start_index = (ordered_ids.index(cursor_session_id) + 1) % len(ordered_ids)
        else:
            start_index = 0
        scan_order = [
            ordered_ids[(start_index + i) % len(ordered_ids)]
            for i in range(len(ordered_ids))
        ]

        unavailable: list[DispatchUnavailableSession] = []
        retry_times: list[datetime] = []

        for sid in scan_order:
            session = existing.get(sid)
            if session is None:
                unavailable.append(DispatchUnavailableSession(
                    session_id=sid,
                    reason=DISPATCH_REASON_SESSION_NOT_FOUND,
                    detail="session not found",
                ))
                continue

            if session["status"] == "finalized":
                unavailable.append(DispatchUnavailableSession(
                    session_id=sid,
                    reason=DISPATCH_REASON_SESSION_FINALIZED,
                    detail="session already finalized",
                ))
                continue

            _, ready, blocked, backoff_n, leased_n, active, awaiting_n = (
                dispatch_session_state(sid, now)
            )

            if ready:
                max_active = session["max_active_leases"]
                if max_active is not None and active >= max_active:
                    # 存在可领取资格的分片，但会话级并发槽位已占满
                    wakeup = earliest_wakeup(sid, now)
                    unavailable.append(DispatchUnavailableSession(
                        session_id=sid,
                        reason=DISPATCH_REASON_SESSION_FULL,
                        detail="session max active leases reached",
                        next_retry_at=wakeup,
                        active_leases=active,
                        max_active_leases=max_active,
                    ))
                    if wakeup is not None:
                        retry_times.append(wakeup)
                    continue

                # 命中：领取该会话内序号最低的可领取分片；资格计算与写入在同一临界区
                chunk_index = ready[0]
                token = secrets.token_urlsafe(32)
                expires_at = now + timedelta(seconds=ttl)
                conn.execute(
                    "UPDATE chunks SET status = 'leased', lease_token = ?,"
                    " lease_expires_at = ? WHERE session_id = ? AND chunk_index = ?",
                    (token, expires_at.isoformat(), sid, chunk_index),
                )
                # 成功后才推进游标（UPSERT）：无可发放项的请求不会执行到这里
                conn.execute(
                    "INSERT INTO dispatch_cursors (queue_key, last_session_id, updated_at)"
                    " VALUES (?, ?, ?)"
                    " ON CONFLICT(queue_key) DO UPDATE SET"
                    " last_session_id = excluded.last_session_id,"
                    " updated_at = excluded.updated_at",
                    (req.queue_key, sid, now.isoformat()),
                )
                conn.commit()

                return DispatchLeaseResponse(
                    queue_key=req.queue_key,
                    granted=DispatchGrant(
                        session_id=sid,
                        chunk_index=chunk_index,
                        lease_token=token,
                        expires_at=expires_at,
                    ),
                    cursor_session_id=sid,
                    unavailable=[],
                    next_retry_at=None,
                )

            # 无就绪分片：按未完成分片的分桶给出确定的不可用原因。
            # 若同时存在被阻塞分片与冷却/在租分片，优先报告后者（带可等待时刻）。
            wakeup = earliest_wakeup(sid, now)
            if wakeup is not None:
                retry_times.append(wakeup)

            if backoff_n and not leased_n:
                reason = DISPATCH_REASON_BACKOFF
                detail = "all eligible chunks are in failure backoff"
            elif leased_n and not backoff_n:
                reason = DISPATCH_REASON_LEASED
                detail = "all eligible chunks hold an active lease"
            elif backoff_n and leased_n:
                # 两类等待事件同时存在：以更早的整体唤醒时刻所属的类型为主因
                lease_earliest = conn.execute(
                    "SELECT MIN(lease_expires_at) AS earliest FROM chunks"
                    " WHERE session_id = ? AND status = 'leased'"
                    " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
                    (sid, now.isoformat()),
                ).fetchone()["earliest"]
                backoff_earliest = conn.execute(
                    "SELECT MIN(backoff_until) AS earliest FROM chunks"
                    " WHERE session_id = ? AND status = 'pending'"
                    " AND backoff_until IS NOT NULL AND backoff_until > ?",
                    (sid, now.isoformat()),
                ).fetchone()["earliest"]
                lease_ts = parse_ts(lease_earliest) if lease_earliest else None
                backoff_ts = parse_ts(backoff_earliest) if backoff_earliest else None
                if backoff_ts is not None and (lease_ts is None or backoff_ts <= lease_ts):
                    reason = DISPATCH_REASON_BACKOFF
                    detail = "eligible chunks are in failure backoff or actively leased"
                else:
                    reason = DISPATCH_REASON_LEASED
                    detail = "eligible chunks are actively leased or in failure backoff"
            elif blocked:
                reason = DISPATCH_REASON_BLOCKED
                detail = "all unfinished chunks blocked by unfinished dependencies"
            elif awaiting_n:
                # 剩余未完成分片全部待验收：上传调度无可发放项，须由验收队列 approve/reject
                reason = DISPATCH_REASON_AWAITING_VERIFICATION
                detail = "all unfinished chunks are awaiting verification"
            else:
                reason = DISPATCH_REASON_DONE
                detail = "all chunks already completed"

            entry = DispatchUnavailableSession(
                session_id=sid,
                reason=reason,
                detail=detail,
                next_retry_at=wakeup,
            )
            if reason == DISPATCH_REASON_BLOCKED:
                entry.blocked = blocked
            unavailable.append(entry)

        # 全部候选均不可发放：本路径无任何写入，游标保持不变

    return DispatchLeaseResponse(
        queue_key=req.queue_key,
        granted=None,
        cursor_session_id=cursor_session_id,
        unavailable=unavailable,
        next_retry_at=min(retry_times) if retry_times else None,
    )


# ---------------------------------------------------------------- 完成上报

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/complete",
    response_model=CompleteResponse,
    tags=["chunks"],
    summary="上报分片完成（幂等）",
)
def complete_chunk(session_id: str, chunk_index: int, req: CompleteRequest):
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if session["digest_algorithm"] == MERKLE_DIGEST_ALGORITHM and not is_valid_merkle_digest(
            req.chunk_digest
        ):
            raise HTTPException(
                status_code=422,
                detail="chunk_digest must be 64 lowercase hex characters "
                       "for sha256-merkle-v1 sessions",
            )

        if chunk["status"] in ("completed", CHUNK_STATUS_AWAITING_VERIFICATION):
            # 幂等重放：同一租约令牌 + 同一摘要 => 返回首次完成结果（状态为当前所处状态）
            if chunk["lease_token"] == req.lease_token and chunk["chunk_digest"] == req.chunk_digest:
                return CompleteResponse(
                    session_id=session_id,
                    chunk_index=chunk_index,
                    status=chunk["status"],
                    chunk_digest=chunk["chunk_digest"],
                    completed_at=parse_ts(chunk["completed_at"]),
                    idempotent_replay=True,
                )
            raise HTTPException(
                status_code=409,
                detail="chunk already completed with different lease token or digest",
            )

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if not lease_valid(chunk, now):
            raise HTTPException(status_code=409, detail="lease expired")

        # 启用验收队列：完成上报先进入 awaiting_verification，验收 approve 后才转 completed；
        # 未启用会话保持原行为，直接 completed。两种情况下退避冷却都随完成上报清除。
        new_status = (
            CHUNK_STATUS_AWAITING_VERIFICATION
            if session["verification_required"]
            else "completed"
        )
        get_conn().execute(
            "UPDATE chunks SET status = ?, chunk_digest = ?, completed_at = ?,"
            " backoff_until = NULL"
            " WHERE session_id = ? AND chunk_index = ?",
            (new_status, req.chunk_digest, now.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return CompleteResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        status=new_status,
        chunk_digest=req.chunk_digest,
        completed_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 失败上报

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/fail",
    response_model=FailResponse,
    tags=["chunks"],
    summary="上报分片失败（校验当前有效租约，清除租约并进入指数退避）",
    responses={409: {"description": "分片已完成、会话已提交、租约令牌不匹配或租约已过期"}},
)
def fail_chunk(session_id: str, chunk_index: int, req: FailRequest):
    """成功后在同一事务内原子地：校验令牌 → 清除租约（状态回到 pending、清空令牌与过期时间）
    → failure_count + 1 → 计算 backoff_until = now + min(max, base×2^(n-1))。"""
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            raise HTTPException(status_code=409, detail="chunk already completed")
        if chunk["status"] == CHUNK_STATUS_AWAITING_VERIFICATION:
            raise HTTPException(
                status_code=409,
                detail="chunk is awaiting verification and cannot report failure",
            )
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if not lease_valid(chunk, now):
            raise HTTPException(status_code=409, detail="lease expired")

        failure_count = (chunk["failure_count"] or 0) + 1
        delay = backoff_delay(session, failure_count)
        retry_after = now + timedelta(seconds=delay)
        # 原子清除租约 + 递增失败次数 + 设置冷却截止；failure_count 永不因本接口清零
        get_conn().execute(
            "UPDATE chunks SET status = 'pending', lease_token = NULL,"
            " lease_expires_at = NULL, failure_count = ?, backoff_until = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (failure_count, retry_after.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return FailResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        status="pending",
        failure_count=failure_count,
        retry_after_seconds=delay,
        retry_after=retry_after,
    )


# ---------------------------------------------------------------- 完成纠错回退

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/reopen",
    response_model=ReopenResponse,
    tags=["chunks"],
    summary="纠错回退：将已完成分片及其全部传递下游原子重置为 pending",
    responses={409: {"description": "会话已提交、分片未完成或完成凭据（令牌/摘要）不匹配"}},
)
def reopen_chunk(session_id: str, chunk_index: int, req: ReopenRequest):
    """凭目标分片完成时保留的租约令牌与摘要发起回退：全部校验先于任何写入，
    任一校验失败不产生部分回退；校验通过后在同一事务内将目标分片与依赖图中的
    全部传递下游重置为 pending（清除摘要、完成时间、有效租约与退避截止时间，
    保留累计失败次数），并将会话 revision 递增 1。资格计算与写入均在 DB_LOCK
    临界区内，并发领取不会看到中间状态。"""
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["status"] == CHUNK_STATUS_AWAITING_VERIFICATION:
            raise HTTPException(
                status_code=409,
                detail="chunk is awaiting verification; reject it instead of reopening",
            )
        if chunk["status"] != "completed":
            raise HTTPException(status_code=409, detail="chunk is not completed")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if chunk["chunk_digest"] != req.expected_digest:
            raise HTTPException(status_code=409, detail="digest mismatch")

        affected = collect_reopen_affected(session_id, chunk_index)
        conn = get_conn()
        # failure_count 不在重置之列：累计失败次数跨回退保留
        conn.executemany(
            "UPDATE chunks SET status = 'pending', lease_token = NULL,"
            " lease_expires_at = NULL, chunk_digest = NULL, completed_at = NULL,"
            " backoff_until = NULL"
            " WHERE session_id = ? AND chunk_index = ?",
            [(session_id, idx) for idx, _ in affected],
        )
        # 受影响分片中若存在残留的待验收租约行（理论上仅可能是历史脏数据），一并清除
        conn.executemany(
            "DELETE FROM verification_leases WHERE session_id = ? AND chunk_index = ?",
            [(session_id, idx) for idx, _ in affected],
        )
        conn.execute(
            "UPDATE sessions SET revision = revision + 1 WHERE id = ?",
            (session_id,),
        )
        conn.commit()

    return ReopenResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        revision=session["revision"] + 1,
        affected_chunks=[
            ReopenAffectedChunk(chunk_index=idx, level=lvl) for idx, lvl in affected
        ],
    )


# ---------------------------------------------------------------- 提交

@app.post(
    "/sessions/{session_id}/finalize",
    response_model=FinalizeResponse,
    tags=["sessions"],
    summary="提交会话（全部分片完成后；sha256-merkle-v1 会话先校验 Merkle 根）",
    responses={409: {"description": "存在缺失分片（missing_chunks），"
                                    "或 Merkle 根与 file_digest 不匹配（computed_digest）"}},
)
def finalize_session(session_id: str):
    with DB_LOCK:
        session = get_session_or_404(session_id)
        rows = get_conn().execute(
            "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

        missing = [r["chunk_index"] for r in rows if r["status"] != "completed"]
        if missing:
            return conflict("upload incomplete", missing_chunks=missing)

        if session["digest_algorithm"] == MERKLE_DIGEST_ALGORITHM:
            # 按分片序号升序读取摘要构建 Merkle 树，与 file_digest 常量时间比较；
            # 不匹配时不做任何写入，会话保持 active，可纠错后重试
            computed = merkle_root_sha256([r["chunk_digest"] for r in rows])
            if not hmac.compare_digest(computed, session["file_digest"]):
                return conflict("file digest mismatch", computed_digest=computed)

        now = utcnow()
        finalized_at = parse_ts(session["finalized_at"]) if session["finalized_at"] else None
        if session["status"] != "finalized":
            finalized_at = now
            get_conn().execute(
                "UPDATE sessions SET status = 'finalized', finalized_at = ? WHERE id = ?",
                (finalized_at.isoformat(), session_id),
            )
            get_conn().commit()
        # 已 finalized 时重复提交：幂等返回同一份清单

    return FinalizeResponse(
        session_id=session_id,
        status="finalized",
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        total_chunks=session["total_chunks"],
        manifest=[
            ManifestEntry(
                chunk_index=r["chunk_index"],
                chunk_digest=r["chunk_digest"],
                completed_at=parse_ts(r["completed_at"]),
            )
            for r in rows
        ],
        finalized_at=finalized_at,
    )


# ---------------------------------------------------------------- 交付凭证

def hash_ticket_token(token: str) -> str:
    """凭证令牌不落库：仅保存其 SHA-256 十六进制摘要。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def build_session_manifest(session, rows) -> tuple[list[ManifestEntry], datetime]:
    """临界区内调用：由会话行与按序分片行组装（按序清单, 提交时间）。"""
    manifest = [
        ManifestEntry(
            chunk_index=r["chunk_index"],
            chunk_digest=r["chunk_digest"],
            completed_at=parse_ts(r["completed_at"]),
        )
        for r in rows
    ]
    return manifest, parse_ts(session["finalized_at"])


@app.post(
    "/sessions/{session_id}/delivery-tickets",
    response_model=DeliveryTicketResponse,
    status_code=201,
    tags=["delivery-tickets"],
    summary="为已提交会话签发一次性清单领取凭证",
    responses={
        404: {"description": "会话不存在"},
        409: {"description": "会话尚未 finalized，不能签发交付凭证"},
    },
)
def create_delivery_ticket(session_id: str, req: CreateDeliveryTicketRequest):
    """生成高强度随机令牌（secrets.token_urlsafe(32)），明文仅在本响应返回一次；
    库内只插入令牌的 SHA-256、所属会话、到期时间与 active 状态。仅 finalized 会话可签发；
    同一会话可签发多张相互独立的凭证（分别一次性兑换、可分别撤销）。"""
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] != "finalized":
            raise HTTPException(
                status_code=409,
                detail="delivery tickets can only be issued for finalized sessions",
            )
        token = secrets.token_urlsafe(32)
        token_hash = hash_ticket_token(token)
        expires_at = now + timedelta(seconds=req.expires_in_seconds)
        get_conn().execute(
            "INSERT INTO delivery_tickets (token_hash, session_id, expires_at, status)"
            " VALUES (?, ?, ?, 'active')",
            (token_hash, session_id, expires_at.isoformat()),
        )
        get_conn().commit()

    return DeliveryTicketResponse(
        ticket_token=token,
        session_id=session_id,
        status=TICKET_STATUS_ACTIVE,
        expires_at=expires_at,
        created_at=now,
    )


@app.post(
    "/delivery-tickets/redeem",
    response_model=RedeemDeliveryTicketResponse,
    tags=["delivery-tickets"],
    summary="凭一次性凭证与幂等 request_id 兑换已提交会话的按序清单",
    responses={
        200: {"description": "首次兑换成功；同一 request_id 重放也返回 200 与首次相同结果"},
        404: {"description": "令牌不存在（未知凭证）"},
        409: {"description": "凭证已消费（不同 request_id）、已撤销或已过期"},
    },
)
def redeem_delivery_ticket(req: RedeemDeliveryTicketRequest):
    """在同一 DB_LOCK 写事务内：定位凭证 → 优先处理 (凭证, request_id) 幂等重放，
    原样返回首次结果 → 校验状态（未消费、未撤销、未过期）→ 条件 UPDATE 将 active 凭证
    原子置为 consumed（仅影响一行才算成功）→ 插入幂等记录保存首次完整响应 JSON。

    并发的不同 request_id 经同一写锁串行化：恰好一个条件 UPDATE 命中，其余得到 409；
    同一 request_id 的并发/重试请求均读到首次幂等记录并返回相同内容。
    校验顺序：已消费 → 已撤销 → 已过期（过期不自动改状态，保持可撤销）。
    """
    now = utcnow()
    token_hash = hash_ticket_token(req.token)
    with DB_LOCK:
        conn = get_conn()
        ticket = conn.execute(
            "SELECT * FROM delivery_tickets WHERE token_hash = ?", (token_hash,)
        ).fetchone()
        if ticket is None:
            raise HTTPException(status_code=404, detail="delivery ticket not found")

        # 幂等优先：同一（凭证, request_id）的重放原样返回首次结果，
        # 即使凭证此后已被其它 request_id 消费、已撤销或已过期
        existing = conn.execute(
            "SELECT response_json FROM delivery_ticket_redemptions"
            " WHERE token_hash = ? AND request_id = ?",
            (token_hash, req.request_id),
        ).fetchone()
        if existing is not None:
            return JSONResponse(status_code=200, content=json.loads(existing["response_json"]))

        if ticket["status"] == TICKET_STATUS_CONSUMED:
            raise HTTPException(
                status_code=409, detail="delivery ticket already consumed")
        if ticket["status"] == TICKET_STATUS_REVOKED:
            raise HTTPException(status_code=409, detail="delivery ticket revoked")
        if parse_ts(ticket["expires_at"]) <= now:
            raise HTTPException(status_code=409, detail="delivery ticket expired")

        session = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (ticket["session_id"],)
        ).fetchone()
        rows = conn.execute(
            "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
            (ticket["session_id"],),
        ).fetchall()

        manifest, finalized_at = build_session_manifest(session, rows)
        result = RedeemDeliveryTicketResponse(
            session_id=session["id"],
            status="finalized",
            file_digest=session["file_digest"],
            digest_algorithm=session["digest_algorithm"],
            total_chunks=session["total_chunks"],
            manifest=manifest,
            finalized_at=finalized_at,
            redeemed_at=now,
            request_id=req.request_id,
        )
        payload = result.model_dump(mode="json")

        # 原子消费：仅 active 凭证可被置为 consumed；并发不同 request_id 至多一个命中
        cur = conn.execute(
            "UPDATE delivery_tickets SET status = 'consumed'"
            " WHERE token_hash = ? AND status = 'active'",
            (token_hash,),
        )
        if cur.rowcount != 1:
            # 临界区内被并发请求抢先消费（理论上写锁串行化不会发生，防御性处理）
            raise HTTPException(
                status_code=409, detail="delivery ticket already consumed")
        conn.execute(
            "INSERT INTO delivery_ticket_redemptions"
            " (token_hash, request_id, response_json, created_at)"
            " VALUES (?, ?, ?, ?)",
            (token_hash, req.request_id, json.dumps(payload), now.isoformat()),
        )
        conn.commit()

    return result


@app.post(
    "/delivery-tickets/revoke",
    response_model=RevokeDeliveryTicketResponse,
    tags=["delivery-tickets"],
    summary="撤销未消费的交付凭证（已消费凭证不可撤销）",
    responses={
        404: {"description": "令牌不存在（未知凭证）"},
        409: {"description": "凭证已消费，不可撤销"},
    },
)
def revoke_delivery_ticket(req: RevokeDeliveryTicketRequest):
    """在同一写事务内将未消费凭证置为 revoked：active 未消费凭证（含已过期但尚未消费的）
    可撤销；已撤销凭证重复撤销幂等返回；已消费凭证不可撤销（409）。
    撤销后任何兑换请求（含从未用过的 request_id）都得到 409。"""
    now = utcnow()
    with DB_LOCK:
        conn = get_conn()
        ticket = conn.execute(
            "SELECT * FROM delivery_tickets WHERE token_hash = ?",
            (hash_ticket_token(req.token),),
        ).fetchone()
        if ticket is None:
            raise HTTPException(status_code=404, detail="delivery ticket not found")
        if ticket["status"] == TICKET_STATUS_CONSUMED:
            raise HTTPException(
                status_code=409, detail="consumed delivery ticket cannot be revoked")

        already_revoked = ticket["status"] == TICKET_STATUS_REVOKED
        if not already_revoked:
            conn.execute(
                "UPDATE delivery_tickets SET status = 'revoked' WHERE token_hash = ?",
                (ticket["token_hash"],),
            )
            conn.commit()

    return RevokeDeliveryTicketResponse(
        session_id=ticket["session_id"],
        status=TICKET_STATUS_REVOKED,
        revoked_at=now,
        already_revoked=already_revoked,
    )


# ---------------------------------------------------------------- 分片验收队列

def hash_verification_token(token: str) -> str:
    """验收令牌不落库：仅保存其 SHA-256 十六进制摘要。"""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@app.post(
    "/verifications/lease",
    response_model=VerificationLeaseResponse,
    status_code=201,
    tags=["verifications"],
    summary="按完成时间领取一个待验收分片，签发有 TTL 的验收令牌",
    responses={
        409: {"description": "当前没有任何可领取（未持有有效验收令牌）的待验收分片"},
    },
)
def lease_verification(req: Optional[VerificationLeaseRequest] = Body(default=None)):
    """在同一 DB_LOCK 写事务内：跨全部启用验收队列的会话，按完成时间（completed_at,
    session_id, chunk_index）从早到晚找到首个 awaiting_verification 分片：

    - 无验收租约或验收租约已过期 => 领取成功，签发新的随机验收令牌（库内仅存 SHA-256 与
      到期时间），同一分片的旧租约行被原子替换；未过期令牌独占，重复/并发请求得到 409；
    - 所有待验收分片均持有未过期验收令牌（或根本不存在待验收分片）=> 409，不做任何写入。

    领取后验收方可凭令牌在 TTL 内 decide；令牌过期不改变分片状态（仍为 awaiting_verification），
    可被任意请求重新领取并换发新令牌，旧令牌随即失效。
    """
    ttl = DEFAULT_VERIFICATION_TTL_SECONDS
    if req is not None and req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_VERIFICATION_TTL_SECONDS)

    now = utcnow()
    now_iso = now.isoformat()
    with DB_LOCK:
        conn = get_conn()
        # 完成时间 FIFO：同刻以 (session_id, chunk_index) 破序，结果确定。
        # LEFT JOIN：从未被领取过的待验收分片（无 verification_leases 行）同样可领取；
        # 已领取但令牌过期（expires_at <= now）也可重领；未过期的行被 WHERE 过滤。
        row = conn.execute(
            "SELECT c.session_id AS session_id, c.chunk_index AS chunk_index,"
            " c.chunk_digest AS chunk_digest, c.completed_at AS completed_at"
            " FROM chunks c"
            " JOIN sessions s ON s.id = c.session_id"
            " LEFT JOIN verification_leases v"
            "   ON v.session_id = c.session_id AND v.chunk_index = c.chunk_index"
            " WHERE c.status = ? AND s.verification_required = 1"
            " AND (v.expires_at IS NULL OR v.expires_at <= ?)"
            " ORDER BY c.completed_at, c.session_id, c.chunk_index"
            " LIMIT 1",
            (CHUNK_STATUS_AWAITING_VERIFICATION, now_iso),
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=409,
                detail="no awaiting verification chunk available",
            )

        token = secrets.token_urlsafe(32)
        token_hash = hash_verification_token(token)
        expires_at = now + timedelta(seconds=ttl)
        # UPSERT：同一分片至多一行当前验收租约；过期重领以新令牌/新到期时间原子换发。
        # 资格检查与写入在同一临界区，并发请求串行化后只有一个能领取任一分片。
        conn.execute(
            "INSERT INTO verification_leases"
            " (token_hash, session_id, chunk_index, issued_at, expires_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(session_id, chunk_index) DO UPDATE SET"
            " token_hash = excluded.token_hash, issued_at = excluded.issued_at,"
            " expires_at = excluded.expires_at",
            (token_hash, row["session_id"], row["chunk_index"],
             now_iso, expires_at.isoformat()),
        )
        conn.commit()

    return VerificationLeaseResponse(
        session_id=row["session_id"],
        chunk_index=row["chunk_index"],
        verification_token=token,
        chunk_digest=row["chunk_digest"],
        completed_at=parse_ts(row["completed_at"]),
        issued_at=now,
        expires_at=expires_at,
    )


@app.post(
    "/verifications/{session_id}/{chunk_index}/decide",
    response_model=VerificationDecideResponse,
    tags=["verifications"],
    summary="凭当前验收令牌对一个待验收分片作出 approve / reject 决定",
    responses={
        404: {"description": "会话或分片不存在"},
        409: {"description": "分片不在待验收状态、验收令牌不匹配/已过期，"
                             "或同一令牌携带与首次不同的冲突决定"},
    },
)
def decide_verification(session_id: str, chunk_index: int, req: VerificationDecideRequest):
    """在同一 DB_LOCK 写事务内校验并落决定：

    - 幂等优先：同一验收令牌 + 同一决定重放（含并发重试）原样返回首次结果；
      同一令牌携带冲突决定返回 409（一枚令牌只能作一次决定）；
    - 分片必须仍为 awaiting_verification；令牌须为该分片当前（未过期）验收令牌，
      否则返回 409（令牌不匹配/已过期/已被新令牌换发）；
    - approve：分片转 completed（摘要与完成时间沿用完成上报值）；
    - reject：原子清除摘要与完成时间、分片回到 pending（同时清除上传租约与退避冷却，
      保留累计失败次数），可立即重新领取上传租约重传。
    成功后删除该分片的当前验收租约行，并写入按令牌哈希唯一的决定记录。
    """
    now = utcnow()
    token_hash = hash_verification_token(req.verification_token)
    with DB_LOCK:
        conn = get_conn()
        get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        decision_row = conn.execute(
            "SELECT * FROM verification_decisions WHERE token_hash = ?",
            (token_hash,),
        ).fetchone()
        if decision_row is not None:
            # 该令牌已被使用：URL 必须指向令牌当初作决定的分片，否则按令牌不匹配拒绝
            if (decision_row["session_id"] != session_id
                    or decision_row["chunk_index"] != chunk_index):
                raise HTTPException(
                    status_code=409, detail="verification token mismatch")
            # 同枚令牌已有决定：同决定幂等返回首次结果，不同决定冲突
            if decision_row["decision"] != req.decision:
                return conflict(
                    "verification token already used with a conflicting decision",
                    original_decision=decision_row["decision"],
                )
            current = conn.execute(
                "SELECT status, chunk_digest FROM chunks"
                " WHERE session_id = ? AND chunk_index = ?",
                (session_id, chunk_index),
            ).fetchone()
            return VerificationDecideResponse(
                session_id=session_id,
                chunk_index=chunk_index,
                decision=req.decision,
                status=current["status"],
                chunk_digest=current["chunk_digest"],
                decided_at=parse_ts(decision_row["decided_at"]),
                idempotent_replay=True,
            )

        if chunk["status"] != CHUNK_STATUS_AWAITING_VERIFICATION:
            raise HTTPException(
                status_code=409,
                detail="chunk is not awaiting verification",
            )
        lease_row = conn.execute(
            "SELECT * FROM verification_leases"
            " WHERE session_id = ? AND chunk_index = ?",
            (session_id, chunk_index),
        ).fetchone()
        if lease_row is None or lease_row["token_hash"] != token_hash:
            # 令牌不属于该分片当前验收租约（从未领取、过期后已被换发或用于其他分片）
            raise HTTPException(status_code=409, detail="verification token mismatch")
        if parse_ts(lease_row["expires_at"]) <= now:
            raise HTTPException(status_code=409, detail="verification token expired")

        if req.decision == VERIFICATION_DECISION_APPROVE:
            new_status = "completed"
            conn.execute(
                "UPDATE chunks SET status = 'completed'"
                " WHERE session_id = ? AND chunk_index = ?",
                (session_id, chunk_index),
            )
            result_digest = chunk["chunk_digest"]
        else:
            # reject：原子清除摘要/完成时间并回到 pending；同时清除上传租约与退避冷却，
            # failure_count 保留，分片可立即被重新领取重传
            new_status = "pending"
            conn.execute(
                "UPDATE chunks SET status = 'pending', chunk_digest = NULL,"
                " completed_at = NULL, lease_token = NULL, lease_expires_at = NULL,"
                " backoff_until = NULL"
                " WHERE session_id = ? AND chunk_index = ?",
                (session_id, chunk_index),
            )
            result_digest = None

        # 决定落库（token_hash 主键保证一枚令牌至多一个决定），随后删除当前验收租约
        conn.execute(
            "INSERT INTO verification_decisions"
            " (token_hash, session_id, chunk_index, decision, decided_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (token_hash, session_id, chunk_index, req.decision, now.isoformat()),
        )
        conn.execute(
            "DELETE FROM verification_leases WHERE session_id = ? AND chunk_index = ?",
            (session_id, chunk_index),
        )
        conn.commit()

    return VerificationDecideResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        decision=req.decision,
        status=new_status,
        chunk_digest=result_digest,
        decided_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 其他

@app.get("/healthz", tags=["meta"], summary="健康检查")
def healthz():
    get_conn().execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", tags=["meta"], summary="服务信息")
def root():
    return {
        "service": "chunked-upload-coordinator",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "default_lease_ttl_seconds": DEFAULT_LEASE_TTL_SECONDS,
        "max_lease_ttl_seconds": MAX_LEASE_TTL_SECONDS,
        "default_verification_ttl_seconds": DEFAULT_VERIFICATION_TTL_SECONDS,
        "max_verification_ttl_seconds": MAX_VERIFICATION_TTL_SECONDS,
        "default_base_retry_seconds": DEFAULT_BASE_RETRY_SECONDS,
        "default_max_retry_seconds": DEFAULT_MAX_RETRY_SECONDS,
    }
