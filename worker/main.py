import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

import controller_link
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("worker")

WORKER_ID = os.environ["WORKER_ID"]
SCRUB_SLEEP = float(os.getenv("SCRUB_SLEEP", "0.01"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.startup(WORKER_ID)          # raises (and the process exits) on wrong volume
    stop = threading.Event()
    threading.Thread(target=controller_link.run, args=(stop,), daemon=True).start()
    log.info("worker %s ready", WORKER_ID)
    yield
    stop.set()


app = FastAPI(lifespan=lifespan)


def err(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message})


@app.get("/health")
def health():
    return {"status": "ok", "worker_id": WORKER_ID}


@app.put("/internal/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    if not storage.valid_id(chunk_id):
        return err(400, "invalid_chunk_id", "chunk id must match [A-Za-z0-9_-]+")
    expected = request.headers.get("x-chunk-checksum")
    if not expected:
        return err(400, "missing_checksum", "X-Chunk-Checksum header required")

    declared = request.headers.get("content-length")
    if declared is not None:
        if not declared.isdigit():
            return err(400, "bad_content_length", "invalid Content-Length")
        if int(declared) > storage.MAX_CHUNK_BYTES:
            return err(400, "chunk_too_large", "chunk exceeds 1 MB")

    buf = bytearray()
    async for part in request.stream():
        buf.extend(part)
        if len(buf) > storage.MAX_CHUNK_BYTES:
            return err(400, "chunk_too_large", "chunk exceeds 1 MB")
    if not buf:
        return err(400, "empty_chunk", "empty chunks are not allowed")
    if declared is not None and int(declared) != len(buf):
        return err(422, "length_mismatch", "body length differs from Content-Length")

    try:
        digest = await run_in_threadpool(storage.store_chunk, chunk_id, bytes(buf), expected)
    except storage.ChecksumMismatch:
        return err(422, "checksum_mismatch", "body hash differs from X-Chunk-Checksum")
    except storage.DiskFull:
        return err(507, "disk_full", "worker has no space left")
    return JSONResponse(status_code=201, content={
        "chunk_id": chunk_id, "checksum": digest, "size": len(buf)})


@app.get("/internal/chunks/{chunk_id}")
def get_chunk(chunk_id: str):
    if not storage.valid_id(chunk_id):
        return err(400, "invalid_chunk_id", "chunk id must match [A-Za-z0-9_-]+")
    try:
        data, digest = storage.read_verified(chunk_id)
    except storage.ChunkMissing:
        return err(404, "not_found", "chunk not found")
    except storage.ChunkCorrupt as e:
        log.warning("corrupt chunk %s: %s", chunk_id, e)
        return err(409, "corrupt", f"chunk failed verification: {e}")
    return Response(content=data, media_type="application/octet-stream",
                    headers={"X-Chunk-Checksum": digest})


@app.delete("/internal/chunks/{chunk_id}")
def delete_chunk(chunk_id: str):
    if not storage.valid_id(chunk_id):
        return err(400, "invalid_chunk_id", "chunk id must match [A-Za-z0-9_-]+")
    storage.delete_chunk(chunk_id)
    return Response(status_code=204)


@app.get("/internal/workers/{worker_id}/chunks")
def list_chunks(worker_id: str):
    if worker_id != WORKER_ID:
        return err(404, "unknown_worker", "this worker is not " + worker_id)
    return {"worker_id": WORKER_ID, "chunks": storage.list_chunks()}


@app.post("/internal/scrub")
def scrub(expected: dict[str, str] | None = None):
    if expected is not None and any(not storage.valid_id(cid) or not storage.HEX64.fullmatch(digest)
                                    for cid, digest in expected.items()):
        return err(400, "invalid_scrub_request", "expected valid chunk IDs and SHA-256 checksums")
    result = storage.scrub(SCRUB_SLEEP, expected)
    if result["bad"]:
        log.warning("scrub found %d bad chunks: %s", len(result["bad"]), result["bad"])
    return result
