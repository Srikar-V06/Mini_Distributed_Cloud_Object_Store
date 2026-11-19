import math
import re
from urllib.parse import quote

import httpx
from config import SCRUB_READ_TIMEOUT, WORKER_CONNECT_TIMEOUT, WORKER_READ_TIMEOUT

_timeout = httpx.Timeout(WORKER_READ_TIMEOUT, connect=WORKER_CONNECT_TIMEOUT)
_scrub_timeout = httpx.Timeout(SCRUB_READ_TIMEOUT, connect=WORKER_CONNECT_TIMEOUT)

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


def list_worker_chunks(address: str, worker_id: str) -> dict[str, dict] | None:
    """Return a complete inventory, or None if it could not be trusted."""
    try:
        response = httpx.get(f"{address}/internal/workers/{quote(worker_id, safe='')}/chunks",
                             timeout=_timeout)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        return None

    if (not isinstance(payload, dict) or payload.get("worker_id") != worker_id
            or not isinstance(payload.get("chunks"), list)):
        return None

    inventory = {}
    for chunk in payload["chunks"]:
        if not isinstance(chunk, dict):
            return None
        chunk_id = chunk.get("chunk_id")
        if not isinstance(chunk_id, str):
            return None
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", chunk_id):
            continue  # The worker API cannot operate on unrelated files in the chunk directory.
        checksum = chunk.get("checksum")
        size = chunk.get("size")
        age = chunk.get("age_seconds")
        try:
            valid_age = type(age) in (int, float) and math.isfinite(age)
        except OverflowError:
            return None
        if (chunk_id in inventory or not isinstance(checksum, str)
                or not re.fullmatch(r"[0-9a-f]{64}", checksum)
                or type(size) is not int or size < 0 or not valid_age):
            return None
        inventory[chunk_id] = chunk
    return inventory


def scrub_worker(address: str, expected: dict[str, str]) -> list[dict]:
    response = httpx.post(f"{address}/internal/scrub", json=expected, timeout=_scrub_timeout)
    response.raise_for_status()
    payload = response.json()
    if (not isinstance(payload, dict) or payload.get("checked") != len(expected)
            or not isinstance(payload.get("bad"), list)):
        raise ValueError("invalid scrub response")
    bad = {}
    for entry in payload["bad"]:
        if not isinstance(entry, dict):
            raise ValueError("invalid scrub finding")
        cid = entry.get("chunk_id")
        if (not isinstance(cid, str) or cid not in expected or cid in bad
                or entry.get("state") not in ("corrupt", "lost")
                or not isinstance(entry.get("reason"), str)):
            raise ValueError("invalid scrub finding")
        bad[cid] = entry
    return list(bad.values())

