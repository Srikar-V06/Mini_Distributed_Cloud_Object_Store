import logging
import threading
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from fastapi import Request
from fastapi.responses import JSONResponse
from errors import ApiError

import janitor
import reconcile
import registry
import scrubber
import upload
import download
import os
import objects_api
import admin_replication, repair
from db import init_db
from error_handlers import install_error_handling

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("controller")



@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    registry.mark_all_unknown()
    janitor.clean_uploads(startup=True)
    stop = threading.Event()
    threading.Thread(target=registry.liveness_loop, args=(stop,), daemon=True).start()
    threading.Thread(target=janitor.run, args=(stop,), daemon=True, name="janitor").start()
    threading.Thread(target=scrubber.run, args=(stop,), daemon=True, name="scrubber").start()
    log.info("controller ready (startup grace period active)")
    repair.start()
    try:
        yield
    finally:
        stop.set()
        repair.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(upload.router)
app.include_router(download.router)
app.include_router(objects_api.router)
app.include_router(admin_replication.router)
app.include_router(scrubber.router)
install_error_handling(app)

class HeartbeatBody(BaseModel):
    worker_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=64)
    chunk_count: int = Field(0, ge=0)
    bytes_used: int = Field(0, ge=0)


class RegisterBody(HeartbeatBody):
    address: str = Field(pattern=r"^https?://\S+$", max_length=200)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/internal/workers/register")
def register_worker(body: RegisterBody, bg: BackgroundTasks):
    prev = registry.register(body.worker_id, body.address, body.chunk_count, body.bytes_used)
    bg.add_task(reconcile.reconcile_worker, body.worker_id)   # runs after the response is sent
    return {"status": "registered", "worker_id": body.worker_id, "previous_status": prev}


@app.post("/internal/workers/heartbeat")
def worker_heartbeat(body: HeartbeatBody, bg: BackgroundTasks):
    prev = registry.heartbeat(body.worker_id, body.chunk_count, body.bytes_used)
    if prev is None:
        return JSONResponse(status_code=404, content={
            "error": "unknown_worker", "message": "worker not registered, please register"})
    if prev != "alive":                                       # came back (dead or unknown)
        bg.add_task(reconcile.reconcile_worker, body.worker_id)
    return {"status": "ok"}


@app.get("/api/v1/admin/workers")
def admin_workers():
    workers = registry.list_workers()
    return {"workers": workers,
            "alive_count": sum(1 for w in workers if w["status"] == "alive")}

@app.exception_handler(ApiError) #temporary handler
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse(status_code=exc.status,
                        content={"error": exc.code, "message": exc.message})
if os.getenv("DEBUG_ROUTES") == "1":
    import debug_routes
    app.include_router(debug_routes.router)
