import logging
import threading
from contextlib import closing
from itertools import batched

import httpx
from fastapi import APIRouter

import registry
import repair
from config import SCRUB_INTERVAL
from db import get_conn, transaction, utc_now
from errors import ApiError
from signals import repair_wakeup
from worker_client import scrub_worker

log = logging.getLogger("controller.scrub")
router = APIRouter()
_scan_lock = threading.Lock()


def _replicas(worker_id: str) -> dict:
    with closing(get_conn()) as conn:
        return {row["chunk_id"]: row for row in conn.execute("""
            SELECT cr.chunk_id, cr.verified_at, c.checksum
            FROM chunk_replicas cr JOIN chunks c USING (chunk_id)
            JOIN objects o USING (object_id)
            WHERE cr.worker_id=? AND cr.state='ok' AND o.status='complete'
        """, (worker_id,))}


def _record(worker: dict, finding: dict, verified_at: str | None) -> bool:
    cid, state = finding["chunk_id"], finding["state"]
    with transaction() as conn:
        updated = conn.execute("""
            UPDATE chunk_replicas SET state=?, verified_at=?
            WHERE chunk_id=? AND worker_id=? AND state='ok' AND verified_at IS ?
              AND EXISTS (SELECT 1 FROM workers WHERE worker_id=? AND address=? AND status='alive')
        """, (state, utc_now(), cid, worker["worker_id"], verified_at,
              worker["worker_id"], worker["address"])).rowcount
    if updated:
        log.warning("scrub marked %s on %s %s: %s", cid, worker["worker_id"], state, finding["reason"])
    return bool(updated)


def _scan_worker(worker: dict, stop: threading.Event | None) -> dict:
    result = {"worker_id": worker["worker_id"], "checked": 0, "corrupt": 0, "lost": 0,
              "skipped": 0}
    replicas = _replicas(worker["worker_id"])
    for group in batched(replicas, 256):
        if stop is not None and stop.is_set():
            break
        expected = {cid: replicas[cid]["checksum"] for cid in group if not repair.is_inflight(cid)}
        result["skipped"] += len(group) - len(expected)
        if not expected:
            continue

        try:
            bad = scrub_worker(worker["address"], expected)
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("scrub failed for %s: %s", worker["worker_id"], exc)
            return {**result, "error": "worker_scrub_failed"}

        result["checked"] += len(expected)
        for finding in bad:
            cid = finding["chunk_id"]
            # Ignore findings that raced a repair or a newer verification of the same copy.
            if repair.run_guarded(cid, _record, worker, finding, replicas[cid]["verified_at"]) is True:
                result[finding["state"]] += 1
                repair_wakeup.set()
            else:
                result["skipped"] += 1
    return result


def run_once(stop: threading.Event | None = None) -> dict | None:
    if not _scan_lock.acquire(blocking=False):
        return None
    try:
        workers = []
        for worker in registry.alive_workers():
            if stop is not None and stop.is_set():
                break
            workers.append(_scan_worker(worker, stop))
        counts = {key: sum(row[key] for row in workers) for key in ("checked", "corrupt", "lost", "skipped")}
        log.info("scrub finished: %s; workers with errors=%d", counts, sum("error" in w for w in workers))
        return {**counts, "workers": workers}
    finally:
        _scan_lock.release()


@router.post("/api/v1/admin/scrub")
def admin_scrub():
    report = run_once()
    if report is None:
        raise ApiError(409, "scrub_in_progress", "a scrub is already running")
    return report


def run(stop: threading.Event) -> None:
    while not stop.wait(SCRUB_INTERVAL):
        try:
            run_once(stop)
        except Exception:
            log.exception("periodic scrub failed")
