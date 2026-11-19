import hashlib, logging, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from fastapi import APIRouter, File, Form, UploadFile

from config import *
from db import get_conn
from errors import ApiError, ReplicationError
from event_log import event
from placement import pick_workers, count_alive
from validation import sanitize_filename, parse_metadata
from worker_client import send_chunk, delete_chunk
import json

log = logging.getLogger("upload")
router = APIRouter()
active_uploads: set[str] = set()      
_pool = ThreadPoolExecutor(max_workers=16)

def now() -> str:
    return datetime.now(timezone.utc).isoformat()

def read_full(f, n: int) -> bytes:
    """Read exactly n bytes unless EOF. A single read(n) may legally return less."""
    buf = bytearray()
    while len(buf) < n:
        piece = f.read(n - len(buf))
        if not piece:          # EOF
            break
        buf.extend(piece)
    return bytes(buf)

def replicate_chunk(chunk_id: str, data: bytes, checksum: str, written: list) -> list[dict]:
    """Get REPLICATION_FACTOR verified copies on distinct workers.
    Every successful PUT is appended to `written` immediately, so abort can clean it."""
    good: list[dict] = []
    tried: set[str] = set()                     # used OR failed -> never reuse for this chunk
    for _ in range(1 + MAX_REPLACEMENT_ROUNDS):
        need = REPLICATION_FACTOR - len(good)
        if need == 0:
            return good
        targets = pick_workers(exclude=tried, k=need)
        if len(targets) < need:
            raise ReplicationError(f"only {len(targets)} eligible workers for {chunk_id}")
        futures = [(w, _pool.submit(send_chunk, w, chunk_id, data, checksum)) for w in targets]
        for w, fut in futures:
            tried.add(w["worker_id"])
            if fut.result():
                good.append(w)
                written.append((chunk_id, w["worker_id"], w["address"]))
            else:
                log.warning("PUT %s to %s failed", chunk_id, w["worker_id"])
    if len(good) < REPLICATION_FACTOR:
        raise ReplicationError(f"could not store {chunk_id} on {REPLICATION_FACTOR} workers")
    return good

def record_chunk(object_id, idx, chunk_id, size, checksum, workers):
    conn = get_conn()
    try:
        with conn:   # one short transaction: chunk row + both replica rows
            conn.execute("INSERT INTO chunks(chunk_id, object_id, idx, size, checksum) VALUES (?,?,?,?,?)",
                         (chunk_id, object_id, idx, size, checksum))
            for w in workers:
                conn.execute("INSERT INTO chunk_replicas(chunk_id, worker_id, state, verified_at) "
                             "VALUES (?,?,?,?)", (chunk_id, w["worker_id"], "ok", now()))
    finally:
        conn.close()

def abort_upload(object_id: str, written: list):
    for chunk_id, worker_id, address in written:
        if not delete_chunk(address, chunk_id):
            c = get_conn()
            try:
                with c:
                    c.execute("INSERT OR IGNORE INTO pending_deletes(chunk_id, worker_id, created_at) VALUES (?,?,?)",
                              (chunk_id, worker_id, now()))
            finally:
                c.close()
    c = get_conn()
    try:
        with c:
            c.execute("DELETE FROM objects WHERE object_id=?", (object_id,))  # cascades
    finally:
        c.close()

@router.post("/api/v1/objects", status_code=201)
def upload_object(file: UploadFile = File(None), metadata: str = Form(None)):
    # 1. pre-check (fail fast, before any DB writes)
    if count_alive() < REPLICATION_FACTOR:
        raise ApiError(503, "insufficient_workers", "fewer than 2 storage workers are alive")

    # 2. validate
    if file is None:
        raise ApiError(400, "missing_file", "multipart field 'file' is required")
    custom_md = parse_metadata(metadata)
    name = sanitize_filename(file.filename)
    content_type = file.content_type or "application/octet-stream"

    # 3. register the object as 'uploading' (invisible to GET/list)
    object_id = str(uuid.uuid4())
    ts = now()
    conn = get_conn()
    try:
        with conn:
            conn.execute("""INSERT INTO objects(object_id,name,size,content_type,chunk_size,
                            created_at,modified_at,checksum,num_chunks,status)
                            VALUES (?,?,?,?,?,?,?,?,?,'uploading')""",
                         (object_id, name, 0, content_type, CHUNK_SIZE, ts, ts, "", 0))
    finally:
        conn.close()
    active_uploads.add(object_id)

    written: list = []   # (chunk_id, worker_id, address) for every successful PUT
    try:
        # 4. read loop
        obj_hash = hashlib.sha256()
        total, idx = 0, 0
        while True:
            data = read_full(file.file, CHUNK_SIZE)
            if not data:                      # EOF -> never emit an empty trailing chunk
                break
            total += len(data)
            if total > MAX_OBJECT_SIZE:
                raise ApiError(400, "too_large", "object exceeds maximum size of 1 GB")
            obj_hash.update(data)
            chunk_hash = hashlib.sha256(data).hexdigest()
            chunk_id = f"{object_id}_{idx:06d}"

            # 5. replicate this chunk, then record it
            workers = replicate_chunk(chunk_id, data, chunk_hash, written)
            record_chunk(object_id, idx, chunk_id, len(data), chunk_hash, workers)
            idx += 1

        # 6. finalize: one transaction flips the object to 'complete'
        checksum = obj_hash.hexdigest()
        final_ts = now()
        conn = get_conn()
        try:
            with conn:
                conn.execute("""UPDATE objects SET size=?, checksum=?, num_chunks=?,
                                status='complete', modified_at=? WHERE object_id=?""",
                             (total, checksum, idx, final_ts, object_id))
                for k, v in custom_md.items():
                    conn.execute("INSERT INTO object_metadata(object_id,key,value_json) VALUES (?,?,?)",
                                 (object_id, k, json.dumps(v)))
        finally:
            conn.close()

        log.info("stored object %s", object_id,
                 extra=event("object_uploaded", object_id=object_id, size=total,
                             num_chunks=idx, checksum=checksum))
        return {"object_id": object_id, "name": name, "size": total,
                "content_type": content_type, "checksum": checksum,
                "num_chunks": idx, "created_at": ts, "metadata": custom_md}

    except Exception as e:
        # 7. abort path
        abort_upload(object_id, written)
        if isinstance(e, ApiError):
            raise
        if isinstance(e, ReplicationError):
            raise ApiError(503, "storage_unavailable", str(e))
        log.exception("unexpected upload failure for %s", object_id)
        raise ApiError(500, "internal_error", "unexpected error during upload")
    finally:
        active_uploads.discard(object_id)
