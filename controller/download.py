import logging, random, re
from itertools import chain
from urllib.parse import quote
from fastapi import APIRouter
from fastapi.responses import Response, StreamingResponse

from db import get_conn
from errors import ApiError
from repair import read_verified
from signals import repair_wakeup
from upload import now
from validation import is_valid_id

log = logging.getLogger("download")
router = APIRouter()


class StreamAborted(Exception):
    """Raised inside the body generator after headers are sent. Aborts the connection."""


def get_object_or_404(object_id: str):
    if not is_valid_id(object_id):
        raise ApiError(404, "not_found", "object not found")
    conn = get_conn()
    try:
        obj = conn.execute("SELECT * FROM objects WHERE object_id=? AND status='complete'",
                           (object_id,)).fetchone()
    finally:
        conn.close()
    if obj is None:
        raise ApiError(404, "not_found", "object not found")
    return obj


def preflight(object_id: str, num_chunks: int):
    """Decide 200 / 404 / 500 / 503 BEFORE any header is sent.
    Returns the ordered chunk list [(chunk_id, size, checksum), ...]."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT c.idx, c.chunk_id, c.size, c.checksum, cr.worker_id, cr.state, w.status AS wstatus
            FROM chunks c
            LEFT JOIN chunk_replicas cr ON cr.chunk_id = c.chunk_id
            LEFT JOIN workers w ON w.worker_id = cr.worker_id
            WHERE c.object_id = ?
            ORDER BY c.idx""", (object_id,)).fetchall()
    finally:
        conn.close()

    chunks, by_chunk = {}, {}
    for r in rows:
        chunks[r["idx"]] = (r["chunk_id"], r["size"], r["checksum"])
        by_chunk.setdefault(r["idx"], []).append(r)

    if len(chunks) != num_chunks:                 # rows vanished: concurrent delete
        raise ApiError(404, "not_found", "object not found")

    unavailable, unrecoverable = [], []
    for idx in sorted(chunks):
        reps = by_chunk[idx]
        if any(r["state"] == "ok" and r["wstatus"] == "alive" for r in reps):
            continue                              # at least one usable candidate
        if any(r["state"] == "ok" for r in reps):
            unavailable.append(idx)               # copies exist, but on dead workers
        else:
            unrecoverable.append(idx)             # every copy corrupt/lost: permanent

    if unrecoverable:
        raise ApiError(500, "data_unrecoverable",
                       f"all copies of chunk(s) {unrecoverable[:5]} are corrupt or lost")
    if unavailable:
        raise ApiError(503, "data_unavailable",
                       f"chunk(s) {unavailable[:5]} are only on unreachable workers")

    return [chunks[i] for i in sorted(chunks)]


def ordered_candidates(chunk_id: str, skip: set):
    """Fresh DB read per chunk (states change). Random order spreads load;
    workers that already failed this request are tried last."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT cr.worker_id, cr.verified_at, w.address
            FROM chunk_replicas cr JOIN workers w ON w.worker_id = cr.worker_id
            WHERE cr.chunk_id = ? AND cr.state = 'ok' AND w.status = 'alive'""",
            (chunk_id,)).fetchall()
    finally:
        conn.close()
    cands = [dict(r) for r in rows]
    random.shuffle(cands)
    cands.sort(key=lambda c: c["worker_id"] in skip)     # stable: failed ones go last
    return cands


def mark_replica(chunk_id: str, worker_id: str, state: str, verified_at: str | None):
    conn = get_conn()
    try:
        with conn:
            updated = conn.execute("UPDATE chunk_replicas SET state=?, verified_at=? "
                                   "WHERE chunk_id=? AND worker_id=? AND state='ok' AND verified_at IS ?",
                                   (state, now(), chunk_id, worker_id, verified_at)).rowcount
    finally:
        conn.close()
    if updated:
        log.warning("replica %s on %s marked %s", chunk_id, worker_id, state)
        repair_wakeup.set()


def stream_chunks(chunks):
    skip: set[str] = set()                    # workers that timed out THIS request only
    for chunk_id, size, checksum in chunks:
        data = None
        for cand in ordered_candidates(chunk_id, skip):
            status, payload = read_verified(cand["address"], chunk_id, size, checksum)
            if status == "ok":
                data = payload
                break
            if status == "corrupt":
                mark_replica(chunk_id, cand["worker_id"], "corrupt", cand["verified_at"])
            elif status == "missing":
                mark_replica(chunk_id, cand["worker_id"], "lost", cand["verified_at"])
            else:                             # unreachable: do NOT change DB, heartbeats decide
                skip.add(cand["worker_id"])
        if data is None:
            log.error("download aborted: no readable copy of %s", chunk_id)
            raise StreamAborted(chunk_id)
        yield data


def content_disposition(name: str) -> str:
    fallback = re.sub(r"[^A-Za-z0-9._-]", "_", name) or "download"
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(name, safe='')}"


@router.get("/api/v1/objects/{object_id}")
def download_object(object_id: str):
    obj = get_object_or_404(object_id)
    headers = {
        "Content-Length": str(obj["size"]),
        "Content-Disposition": content_disposition(obj["name"]),
        "ETag": f'"{obj["checksum"]}"',
        "X-Object-Checksum": obj["checksum"],
    }
    if obj["num_chunks"] == 0:
        return Response(content=b"", media_type=obj["content_type"], headers=headers)

    chunks = preflight(object_id, obj["num_chunks"])      # may raise 404/500/503
    stream = stream_chunks(chunks)
    try:
        first = next(stream)
    except StreamAborted:
        # Read failures may have changed replica states since the initial preflight.
        preflight(object_id, obj["num_chunks"])
        raise ApiError(503, "data_unavailable", "no replica responded for the first chunk") from None
    return StreamingResponse(chain((first,), stream),
                             media_type=obj["content_type"], headers=headers)
