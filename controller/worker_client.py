import httpx
from config import WORKER_CONNECT_TIMEOUT, WORKER_READ_TIMEOUT

_timeout = httpx.Timeout(WORKER_READ_TIMEOUT, connect=WORKER_CONNECT_TIMEOUT)

def send_chunk(worker: dict, chunk_id: str, data: bytes, checksum: str) -> bool:
    """True only on 201. Any timeout, 5xx, 507 or 422 counts as failure."""
    try:
        r = httpx.put(f"{worker['address']}/internal/chunks/{chunk_id}",
                      content=data,
                      headers={"X-Chunk-Checksum": checksum},
                      timeout=_timeout)
        return r.status_code == 201
    except httpx.HTTPError:
        return False

def delete_chunk(worker_address: str, chunk_id: str) -> bool:
    try:
        r = httpx.delete(f"{worker_address}/internal/chunks/{chunk_id}", timeout=_timeout)
        return r.status_code in (204, 404)
    except httpx.HTTPError:
        return False
def fetch_chunk(address: str, chunk_id: str):
    """Returns (status, bytes|None). status is one of:
    ok          200, body returned (caller must still verify length and hash)
    corrupt     worker said 409 (its own re-hash failed)
    missing     worker said 404
    unreachable timeout, connection error, 5xx, anything unexpected
    """
    try:
        r = httpx.get(f"{address}/internal/chunks/{chunk_id}", timeout=_timeout)
    except httpx.HTTPError:
        return "unreachable", None
    if r.status_code == 200:
        return "ok", r.content
    if r.status_code == 409:
        return "corrupt", None
    if r.status_code == 404:
        return "missing", None
    return "unreachable", None

