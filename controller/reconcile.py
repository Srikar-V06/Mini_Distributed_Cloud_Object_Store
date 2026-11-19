import logging
import threading
import time
from contextlib import closing

import config
import repair
from db import get_conn, transaction, utc_now
from event_log import event
from signals import repair_wakeup
from worker_client import delete_chunk, list_worker_chunks

log = logging.getLogger("controller.reconcile")
_lock = threading.Lock()
_running: set[str] = set()
_retry_at: dict[str, float] = {}


def _retry(worker_id: str, delay: float) -> None:
    due = time.monotonic() + delay
    with _lock:
        _retry_at[worker_id] = min(due, _retry_at.get(worker_id, due))


def retry_due_reconciliations() -> None:
    with _lock:
        due = [worker_id for worker_id, at in _retry_at.items() if at <= time.monotonic()]
    for worker_id in due:
        reconcile_worker(worker_id)


def _worker_address(conn, worker_id: str) -> str | None:
    row = conn.execute("SELECT address FROM workers WHERE worker_id=? AND status='alive'",
                       (worker_id,)).fetchone()
    return row["address"] if row else None


def _delete_pending(chunk_id: str, worker_id: str, address: str) -> None:
    key = (chunk_id, worker_id)
    with closing(get_conn()) as conn:
        if _worker_address(conn, worker_id) != address or not conn.execute(
            "SELECT 1 FROM pending_deletes WHERE chunk_id=? AND worker_id=?", key
        ).fetchone():
            return
    if delete_chunk(address, chunk_id):
        with transaction() as conn:
            conn.execute("DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?", key)


def retry_pending_deletes(worker_id: str | None = None) -> None:
    sql = """SELECT p.chunk_id, p.worker_id, w.address FROM pending_deletes p
             JOIN workers w USING (worker_id) WHERE w.status='alive'"""
    params = ()
    if worker_id is not None:
        sql += " AND p.worker_id=?"
        params = (worker_id,)
    with closing(get_conn()) as conn:
        rows = conn.execute(sql, params).fetchall()
    for row in rows:
        cid = row["chunk_id"]
        repair.run_guarded(cid, _delete_pending, cid, row["worker_id"], row["address"])


def _update_expected(worker_id: str, address: str, expected: dict, inventory: dict) -> dict:
    """Apply inventory results only to replica rows unchanged since before the HTTP call."""
    counts = {"ok": 0, "corrupt": 0, "lost": 0}
    changed = []
    verified_at = utc_now()
    with transaction() as conn:
        if _worker_address(conn, worker_id) != address:
            return counts
        for cid, row in expected.items():
            file = inventory.get(cid)
            if file is None:
                if row["state"] != "ok":
                    continue
                state = "lost"
            elif file["checksum"] == row["checksum"] and file["size"] == row["size"]:
                state = "ok"
            else:
                state = "corrupt"
            result = conn.execute("""
                UPDATE chunk_replicas SET state=?, verified_at=?
                WHERE chunk_id=? AND worker_id=? AND state=? AND verified_at IS ?
                  AND NOT EXISTS (SELECT 1 FROM pending_deletes
                                  WHERE chunk_id=? AND worker_id=?)""",
                (state, verified_at, cid, worker_id, row["state"], row["verified_at"], cid, worker_id))
            counts[state] += result.rowcount
            if result.rowcount and state != "ok" and state != row["state"]:
                changed.append((cid, state))
    for cid, state in changed:
        log.warning("rejoin marked %s on %s %s", cid, worker_id, state,
                    extra=event(f"replica_{state}", chunk_id=cid, worker_id=worker_id, detector="rejoin"))
    return counts


def _untracked_file(cid: str, file: dict, worker_id: str, address: str) -> str:
    """Adopt an unrecorded replica or queue an old orphan for deletion."""
    key = (cid, worker_id)
    with transaction() as conn:
        if _worker_address(conn, worker_id) != address:
            return "retry"
        if conn.execute("SELECT 1 FROM chunk_replicas WHERE chunk_id=? AND worker_id=?", key).fetchone():
            return "skip"
        if conn.execute("SELECT 1 FROM pending_deletes WHERE chunk_id=? AND worker_id=?", key).fetchone():
            return "skip"
        chunk = conn.execute("""
            SELECT c.size, c.checksum, o.status FROM chunks c JOIN objects o USING (object_id)
            WHERE c.chunk_id=?""", (cid,)).fetchone()
        if chunk is None:
            # A worker PUT may finish before upload.py records its chunk row.
            object_id = cid.rpartition("_")[0]
            if conn.execute("SELECT 1 FROM objects WHERE object_id=? AND status='uploading'",
                            (object_id,)).fetchone():
                return "deferred"
            if file["age_seconds"] < config.ORPHAN_MIN_AGE:
                return "deferred"
            conn.execute("INSERT OR IGNORE INTO pending_deletes VALUES (?,?,?)", (*key, utc_now()))
        elif chunk["status"] != "complete":
            return "deferred"

    if chunk is None:
        _delete_pending(cid, worker_id, address)
        return "orphan"

    # A trim may have removed this copy since the inventory was fetched.
    state, _ = repair.read_verified(address, cid, chunk["size"], chunk["checksum"])
    if state == "unreachable":
        return "retry"
    if state == "missing":
        return "skip"
    with transaction() as conn:
        if _worker_address(conn, worker_id) != address:
            return "retry"
        inserted = conn.execute("""
            INSERT OR IGNORE INTO chunk_replicas (chunk_id, worker_id, state, verified_at)
            SELECT chunk_id, ?, ?, ? FROM chunks JOIN objects USING (object_id)
            WHERE chunk_id=? AND status='complete'
              AND NOT EXISTS (SELECT 1 FROM pending_deletes WHERE chunk_id=? AND worker_id=?)""",
            (worker_id, state, utc_now(), cid, cid, worker_id)).rowcount
    return state if inserted else "skip"


def _reconcile(worker_id: str) -> None:
    retry_pending_deletes(worker_id)
    with closing(get_conn()) as conn:
        address = _worker_address(conn, worker_id)
        if address is None:
            return
        expected = {row["chunk_id"]: row for row in conn.execute("""
            SELECT cr.chunk_id, cr.state, cr.verified_at, c.size, c.checksum
            FROM chunk_replicas cr JOIN chunks c USING (chunk_id) JOIN objects o USING (object_id)
            WHERE cr.worker_id=? AND o.status='complete'""", (worker_id,))}

    inventory = list_worker_chunks(address, worker_id)
    if inventory is None:
        log.warning("inventory unavailable for %s; reconciliation will retry", worker_id,
                    extra=event("worker_inventory_failed", worker_id=worker_id))
        _retry(worker_id, config.JANITOR_INTERVAL)
        return

    counts = _update_expected(worker_id, address, expected, inventory)
    for cid, file in inventory.items():
        if cid in expected:
            continue
        result = repair.run_guarded(cid, _untracked_file, cid, file, worker_id, address)
        if result in ("busy", "error", "retry"):
            _retry(worker_id, config.JANITOR_INTERVAL)
        elif result == "deferred":
            _retry(worker_id, max(config.JANITOR_INTERVAL, config.ORPHAN_MIN_AGE - file["age_seconds"]))
        elif result in counts:
            counts[result] += 1
        elif result == "orphan":
            counts["orphans_queued"] = counts.get("orphans_queued", 0) + 1

    repair_wakeup.set()
    log.info("reconciled worker %s: %s", worker_id, counts,
             extra=event("worker_reconciled", worker_id=worker_id, counts=counts))


def reconcile_worker(worker_id: str) -> None:
    with _lock:
        if worker_id in _running:
            _retry_at[worker_id] = time.monotonic()
            return
        _running.add(worker_id)
        _retry_at.pop(worker_id, None)
    try:
        _reconcile(worker_id)
    except Exception:
        log.exception("reconciliation failed for %s", worker_id)
        _retry(worker_id, config.JANITOR_INTERVAL)
    finally:
        with _lock:
            _running.discard(worker_id)
