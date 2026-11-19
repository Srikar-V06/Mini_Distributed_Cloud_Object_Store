from fastapi import APIRouter

import repair
from errors import ApiError
from validation import is_valid_id

router = APIRouter()

@router.get("/api/v1/admin/replication")
def replication_status():
    return repair.get_report()


@router.get("/api/v1/admin/chunks/{object_id}")
def chunk_locations(object_id: str):
    if not is_valid_id(object_id):
        raise ApiError(404, "not_found", "object not found")
    obj, chunks = repair.load_chunk_view(object_id)
    if obj is None:
        raise ApiError(404, "not_found", "object not found")

    return {**dict(obj), "replication_factor": repair.RF, "chunks": [
        {"chunk_id": ch["chunk_id"], "index": ch["index"], "size": ch["size"],
         "checksum": ch["checksum"], "effective_replicas": repair.effective_replica_count(ch),
         "replicas": [
             {"worker_id": replica["worker_id"], "state": replica["state"],
              "worker_status": replica["wstatus"] or "unknown", "verified_at": replica["verified_at"]}
             for replica in ch["replicas"]
         ]}
        for ch in chunks.values()
    ]}
