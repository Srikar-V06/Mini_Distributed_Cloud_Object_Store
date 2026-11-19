import json, logging
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

from db import get_conn, transaction
from errors import ApiError
from upload import now
from validation import is_valid_id, parse_patch, parse_nonneg_int
from worker_client import delete_chunk

log = logging.getLogger("objects")
router = APIRouter()
_cleanup_pool = ThreadPoolExecutor(max_workers=8)

SYSTEM_FIELDS = ("object_id", "name", "size", "content_type", "chunk_size",
                 "created_at", "modified_at", "checksum", "num_chunks")


# ---------- GET metadata ----------
@router.get("/api/v1/objects/{object_id}/metadata")
def get_metadata(object_id: str):
    if not is_valid_id(object_id):
        raise ApiError(404, "not_found", "object not found")
    conn = get_conn()
    try:
        conn.execute("BEGIN")        # one read transaction = one consistent snapshot (WAL)
        obj = conn.execute("SELECT * FROM objects WHERE object_id=? AND status='complete'",
                           (object_id,)).fetchone()
        if obj is None:
            raise ApiError(404, "not_found", "object not found")
        custom_rows = conn.execute("SELECT key, value_json FROM object_metadata WHERE object_id=?",
                                   (object_id,)).fetchall()
        chunk_rows = conn.execute("""
            SELECT c.idx, c.chunk_id, c.size, c.checksum, cr.worker_id, cr.state
            FROM chunks c LEFT JOIN chunk_replicas cr ON cr.chunk_id = c.chunk_id
            WHERE c.object_id = ? ORDER BY c.idx, cr.worker_id""", (object_id,)).fetchall()
    finally:
        conn.close()

    chunks: dict[int, dict] = {}
    for r in chunk_rows:
        ch = chunks.setdefault(r["idx"], {"index": r["idx"], "chunk_id": r["chunk_id"],
                                          "size": r["size"], "checksum": r["checksum"],
                                          "replicas": []})
        if r["worker_id"] is not None:
            ch["replicas"].append({"worker": r["worker_id"], "state": r["state"]})

    return {"system": {k: obj[k] for k in SYSTEM_FIELDS},
            "custom": {r["key"]: json.loads(r["value_json"]) for r in custom_rows},
            "chunks": [chunks[i] for i in sorted(chunks)]}


# ---------- PATCH metadata ----------
def apply_patch(object_id: str, raw: bytes):                                  
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")   # take the write lock now: concurrent PATCHes serialize here
        obj = conn.execute("SELECT 1 FROM objects WHERE object_id=? AND status='complete'",
                           (object_id,)).fetchone()
        if obj is None:
            raise ApiError(404, "not_found", "object not found")
        patch = parse_patch(raw)

        existing = {r["key"]: r["value_json"] for r in conn.execute(
            "SELECT key, value_json FROM object_metadata WHERE object_id=?", (object_id,))}

        after = dict(existing)                                # compute the result first
        for k, v in patch.items():
            if v is None:
                after.pop(k, None)
            else:
                after[k] = json.dumps(v)
        if len(after) > 50:
            raise ApiError(400, "invalid_metadata", "object would exceed 50 custom keys")

        changed = after != existing                           # only real changes touch modified_at
        for k in set(existing) - set(after):
            conn.execute("DELETE FROM object_metadata WHERE object_id=? AND key=?", (object_id, k))
        for k, vj in after.items():
            if existing.get(k) != vj:
                conn.execute("""INSERT INTO object_metadata(object_id, key, value_json) VALUES (?,?,?)
                                ON CONFLICT(object_id, key) DO UPDATE SET value_json=excluded.value_json""",
                             (object_id, k, vj))
        if changed:
            conn.execute("UPDATE objects SET modified_at=? WHERE object_id=?", (now(), object_id))
        modified_at = conn.execute("SELECT modified_at FROM objects WHERE object_id=?",
                                   (object_id,)).fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"object_id": object_id, "modified_at": modified_at,
            "custom": {k: json.loads(v) for k, v in after.items()}}


@router.patch("/api/v1/objects/{object_id}/metadata")
async def patch_metadata(object_id: str, request: Request):
    if not is_valid_id(object_id):
        raise ApiError(404, "not_found", "object not found")
    raw = await request.body()           # reading the body is async in Starlette...
    return await run_in_threadpool(apply_patch, object_id, raw)   # ...DB work stays off the event loop


# ---------- LIST ----------
@router.get("/api/v1/objects")
def list_objects(limit: str = "100", offset: str = "0"):
    lim = parse_nonneg_int("limit", limit, 1, 1000)
    off = parse_nonneg_int("offset", offset, 0, 10**9)
    conn = get_conn()
    try:
        conn.execute("BEGIN")
        total = conn.execute("SELECT COUNT(*) FROM objects WHERE status='complete'").fetchone()[0]
        rows = conn.execute("""SELECT object_id, name, size, content_type, num_chunks, created_at
                               FROM objects WHERE status='complete'
                               ORDER BY created_at, object_id LIMIT ? OFFSET ?""",
                            (lim, off)).fetchall()
    finally:
        conn.close()
    return {"items": [dict(r) for r in rows], "total": total, "limit": lim, "offset": off}


# ---------- DELETE ----------
def purge_from_workers(replicas):
    """Best effort. Dead workers are not even attempted (saves a 2s connect timeout each)."""
    def one(r):
        if r["wstatus"] != "alive" or not r["address"]:
            return None
        if delete_chunk(r["address"], r["chunk_id"]):
            return r["chunk_id"], r["worker_id"]

    deleted = [key for key in _cleanup_pool.map(one, replicas) if key is not None]
    if deleted:
        with transaction() as conn:
            conn.executemany("DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?", deleted)
    if len(deleted) < len(replicas):
        log.warning("%d chunk deletes deferred to pending_deletes", len(replicas) - len(deleted))


@router.delete("/api/v1/objects/{object_id}", status_code=204)
def delete_object(object_id: str):
    if not is_valid_id(object_id):
        raise ApiError(404, "not_found", "object not found")
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")   # a second concurrent DELETE waits, then sees 'not found'
        obj = conn.execute("SELECT status FROM objects WHERE object_id=?", (object_id,)).fetchone()
        if obj is None:
            raise ApiError(404, "not_found", "object not found")
        if obj["status"] != "complete":
            raise ApiError(409, "upload_in_progress", "object is still being uploaded")
        replicas = [dict(r) for r in conn.execute("""
            SELECT cr.chunk_id, cr.worker_id, w.address, w.status AS wstatus
            FROM chunks c
            JOIN chunk_replicas cr ON cr.chunk_id = c.chunk_id
            LEFT JOIN workers w ON w.worker_id = cr.worker_id
            WHERE c.object_id = ?""", (object_id,))]      # capture BEFORE the cascade erases them
        conn.executemany("INSERT OR IGNORE INTO pending_deletes VALUES (?,?,?)",
                         [(r["chunk_id"], r["worker_id"], now()) for r in replicas])
        conn.execute("DELETE FROM objects WHERE object_id=?", (object_id,))   # cascades
        conn.commit()                     # <- the object is now gone for every client
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    purge_from_workers(replicas)          # slow part happens after the commit
    return Response(status_code=204)
