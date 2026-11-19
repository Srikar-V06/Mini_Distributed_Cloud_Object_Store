import hashlib
import logging
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone

import config
from db import get_conn, transaction
from event_log import event
from placement import pick_workers
from signals import repair_wakeup
from upload import now
from worker_client import delete_chunk, fetch_chunk, send_chunk

log = logging.getLogger("repair")

RF = config.REPLICATION_FACTOR
# A worker silent for LESS than this may still come back, so its copies are not repaired yet.
BLIP_WINDOW = config.DEAD_AFTER + config.REPAIR_DELAY

_stop = threading.Event()
_inflight: set[str] = set()
_inflight_lock = threading.Lock()
_report_lock = threading.Lock()
_report: dict = {"state": "starting"}
_totals = {"repairs": 0, "trims": 0}
_last_summary = None


# ---------- small helpers ----------
def _age(ts):
    """Seconds since an ISO timestamp written by the controller; inf if unknown."""
    if not ts:
        return float("inf")
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return float("inf")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds()


def is_inflight(chunk_id: str) -> bool:
    with _inflight_lock:
        return chunk_id in _inflight


def _bump(key: str):
    with _report_lock:
        _totals[key] += 1


def get_report() -> dict:
    with _report_lock:
        monitor = dict(_report)
        totals = dict(_totals)
    failed = monitor.pop("failed_repairs", ())
    _, chunks = load_chunk_view()
    return {**monitor, **describe_replication(chunks, failed), "observed_at": now(),
            "repairs_done": totals["repairs"], "trims_done": totals["trims"]}


def read_verified(address, chunk_id, size, checksum):
    """Read one replica and check it against the DB (the source of truth).
    Returns (ok|corrupt|missing|unreachable, bytes|None)."""
    status, data = fetch_chunk(address, chunk_id)
    if status != "ok":
        return status, None
    if len(data) == size and hashlib.sha256(data).hexdigest() == checksum:
        return "ok", data
    return "corrupt", None


# ---------- DB writes (each one short transaction) ----------
def set_state(chunk_id, worker_id, state):
    conn = get_conn()
    try:
        with conn:
            conn.execute("UPDATE chunk_replicas SET state=?, verified_at=? "
                         "WHERE chunk_id=? AND worker_id=?", (state, now(), chunk_id, worker_id))
    finally:
        conn.close()
    log.warning("replica %s on %s marked %s", chunk_id, worker_id, state,
                extra=event(f"replica_{state}", chunk_id=chunk_id, worker_id=worker_id, detector="repair"))


def queue_pending_delete(chunk_id, worker_id):
    conn = get_conn()
    try:
        with conn:
            conn.execute("INSERT OR IGNORE INTO pending_deletes(chunk_id, worker_id, created_at) VALUES (?,?,?)",
                         (chunk_id, worker_id, now()))
    finally:
        conn.close()


def record_replica(chunk_id, worker_id) -> bool:
    """Upsert an 'ok' row. False if the chunk vanished meanwhile (object deleted mid-repair)."""
    try:
        with transaction() as conn:
            conn.execute("""INSERT INTO chunk_replicas(chunk_id, worker_id, state, verified_at)
                            VALUES (?,?, 'ok', ?)
                            ON CONFLICT(chunk_id, worker_id)
                            DO UPDATE SET state='ok', verified_at=excluded.verified_at""",
                         (chunk_id, worker_id, now()))
            conn.execute("DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?",
                         (chunk_id, worker_id))
        return True
    except sqlite3.IntegrityError:        # foreign key: the chunk row is gone
        return False


# ---------- snapshot and classification ----------
SNAPSHOT_SQL = """
SELECT c.object_id, c.idx, c.chunk_id, c.size, c.checksum, cr.worker_id,
       cr.state, cr.verified_at, w.status AS wstatus, w.address, w.last_heartbeat
FROM chunks c
JOIN objects o ON o.object_id = c.object_id AND o.status = 'complete'   -- skip uploading objects
LEFT JOIN chunk_replicas cr ON cr.chunk_id = c.chunk_id
LEFT JOIN workers w ON w.worker_id = cr.worker_id
"""


def load_snapshot():
    conn = get_conn()
    try:
        conn.execute("BEGIN")                         # one consistent view for the whole cycle
        rows = conn.execute(SNAPSHOT_SQL).fetchall()
        loads = {r["worker_id"]: r["n"] for r in conn.execute(
            "SELECT worker_id, COUNT(*) AS n FROM chunk_replicas GROUP BY worker_id")}
    finally:
        conn.close()
    return _group_chunks(rows), loads


def load_chunk_view(object_id: str | None = None):
    """Return a stable database view, including completed objects with zero chunks."""
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        obj = None
        sql = SNAPSHOT_SQL
        params = ()
        if object_id is not None:
            obj = conn.execute("""
                SELECT object_id, size, checksum, num_chunks, created_at, modified_at FROM objects
                WHERE object_id=? AND status='complete'
            """, (object_id,)).fetchone()
            if obj is None:
                return None, {}
            sql += " WHERE c.object_id=?"
            params = (object_id,)
        rows = conn.execute(sql + " ORDER BY c.object_id, c.idx, cr.worker_id", params).fetchall()
    return obj, _group_chunks(rows)


def _group_chunks(rows) -> dict:
    chunks = {}
    for r in rows:
        ch = chunks.setdefault(r["chunk_id"], {"chunk_id": r["chunk_id"], "object_id": r["object_id"],
                                               "index": r["idx"], "size": r["size"],
                                               "checksum": r["checksum"], "replicas": []})
        if r["worker_id"] is not None:
            ch["replicas"].append({"worker_id": r["worker_id"], "state": r["state"],
                                   "verified_at": r["verified_at"],
                                   "wstatus": r["wstatus"], "address": r["address"],
                                   "age": _age(r["last_heartbeat"])})
    return chunks


def effective_replica_count(chunk) -> int:
    return sum(r["state"] == "ok" and r["wstatus"] == "alive" for r in chunk["replicas"])


def classify(ch) -> str:
    reps = ch["replicas"]
    dead_ok = [r for r in reps if r["state"] == "ok" and r["wstatus"] != "alive"]
    blips = [r for r in dead_ok if r["age"] <= BLIP_WINDOW]     # might come back soon
    n = effective_replica_count(ch)
    if n > RF:
        return "over"
    if n == RF:
        return "healthy"
    if n + len(blips) >= RF:
        return "waiting"
    if n == 0:
        return "unavailable" if dead_ok else "unrecoverable"
    return "repair"


def describe_replication(chunks: dict, failed_repairs) -> dict:
    counts = dict(healthy=0, over_replicated=0, under_replicated=0, waiting=0,
                  repair_pending=0, degraded=0, unavailable=0, unrecoverable=0)
    affected = {"degraded": [], "unavailable": [], "unrecoverable": []}
    details = []
    for cid, ch in chunks.items():
        state = classify(ch)
        if state == "repair":
            state = "degraded" if cid in failed_repairs else "repair_pending"
        elif state == "over":
            state = "over_replicated"
        effective = effective_replica_count(ch)
        counts[state] += 1
        counts["under_replicated"] += effective < RF
        if state in affected:
            affected[state].append(cid)
        details.append({"chunk_id": cid, "object_id": ch["object_id"], "index": ch["index"],
                        "effective_replicas": effective, "status": state})
    return {**counts, "replication_factor": RF, "total_chunks": len(chunks), "chunks": details,
            **{f"{state}_chunks": ids for state, ids in affected.items()},
            "sample": {state: ids[:20] for state, ids in affected.items()}}


# ---------- repair one chunk ----------
def next_target(inplace, tried):
    """Heal in place first (overwrites the bad file), then least-loaded alive worker."""
    for r in inplace:
        if r["worker_id"] not in tried:
            return {"worker_id": r["worker_id"], "address": r["address"]}
    picked = pick_workers(exclude=tried, k=1)
    return picked[0] if picked else None


def repair_chunk(ch) -> str:
    cid, reps = ch["chunk_id"], ch["replicas"]
    alive_ok = [r for r in reps if r["state"] == "ok" and r["wstatus"] == "alive"]
    random.shuffle(alive_ok)

    # 1. find a source that passes verification
    data, src, bad = None, None, set()
    for s in alive_ok:
        status, payload = read_verified(s["address"], cid, ch["size"], ch["checksum"])
        if status == "ok":
            data, src = payload, s
            break
        bad.add(s["worker_id"])
        if status == "corrupt":
            set_state(cid, s["worker_id"], "corrupt")
        elif status == "missing":
            set_state(cid, s["worker_id"], "lost")
        # 'unreachable': leave the row alone, heartbeats decide liveness
    if data is None:
        return "no_source"

    # 2. copy until the chunk is back at RF good copies
    good = len(alive_ok) - len(bad)
    need = RF - good
    tried = {r["worker_id"] for r in reps if r["state"] == "ok"}      # never target a current holder
    inplace = [r for r in reps if r["state"] != "ok" and r["wstatus"] == "alive"]
    repaired = set()
    while len(repaired) < need:
        target = next_target(inplace, tried)
        if target is None:
            break                                                      # no eligible target: degraded
        tried.add(target["worker_id"])
        log.info("chunk %s under-replicated (%d/%d): copying %s -> %s",
                 cid, good + len(repaired), RF, src["worker_id"], target["worker_id"],
                 extra=event("repair_started", chunk_id=cid, source_worker_id=src["worker_id"],
                             target_worker_id=target["worker_id"]))
        if not send_chunk(target, cid, data, ch["checksum"]):          # worker verifies before 201
            log.warning("copy of %s to %s failed, trying another target", cid, target["worker_id"],
                        extra=event("repair_write_failed", chunk_id=cid, target_worker_id=target["worker_id"]))
            continue
        if not record_replica(cid, target["worker_id"]):               # row only AFTER verification
            if not delete_chunk(target["address"], cid):               # object deleted mid-copy
                queue_pending_delete(cid, target["worker_id"])
            return "deleted"
        repaired.add(target["worker_id"])
        _bump("repairs")
        log.info("stored repaired copy of %s on %s", cid, target["worker_id"],
                 extra=event("replica_repaired", chunk_id=cid, source_worker_id=src["worker_id"],
                             target_worker_id=target["worker_id"]))
    if len(repaired) < need:
        return "degraded"
    for replica in reps:
        if replica["worker_id"] not in repaired:
            if remove_replica(cid, replica, bad_only=True):
                log.info("removed bad replica of %s on %s after repair", cid, replica["worker_id"])
    return "repaired"


# ---------- trim one over-replicated chunk ----------
def remove_replica(cid, replica, *, bad_only=False) -> bool:
    key = (cid, replica["worker_id"])
    sql = "DELETE FROM chunk_replicas WHERE chunk_id=? AND worker_id=?"
    if bad_only:
        sql += " AND state!='ok'"
    with transaction() as conn:
        if not conn.execute(sql, key).rowcount:
            return False
        conn.execute("INSERT OR IGNORE INTO pending_deletes VALUES (?,?,?)", (*key, now()))
    if replica["wstatus"] == "alive" and delete_chunk(replica["address"], cid):
        with transaction() as conn:
            conn.execute("DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?", key)
    return True


def trim_chunk(ch, loads) -> str:
    cid = ch["chunk_id"]
    alive_ok = [r for r in ch["replicas"] if r["state"] == "ok" and r["wstatus"] == "alive"]
    ranked = sorted(alive_ok, key=lambda r: loads.get(r["worker_id"], 0))   # least loaded first
    keep, extras = ranked[:RF], ranked[RF:]

    for k in keep:                         # prove the copies we KEEP are good before deleting anything
        status, _ = read_verified(k["address"], cid, ch["size"], ch["checksum"])
        if status != "ok":
            if status == "corrupt":
                set_state(cid, k["worker_id"], "corrupt")
            elif status == "missing":
                set_state(cid, k["worker_id"], "lost")
            return "skipped"

    for e in extras:                       # extras come from the most-loaded workers
        if remove_replica(cid, e):
            log.info("chunk %s over-replicated: removed extra copy from %s", cid, e["worker_id"],
                     extra=event("replica_trimmed", chunk_id=cid, worker_id=e["worker_id"]))
            _bump("trims")
    return "trimmed"


# ---------- one cycle ----------
def run_guarded(cid, fn, *args):
    """Keep repair, trimming and reconciliation cleanup from overlapping on a chunk."""
    with _inflight_lock:
        if cid in _inflight:
            return "busy"
        _inflight.add(cid)
    try:
        return fn(*args)
    except Exception:
        log.exception("repair of %s crashed", cid)
        return "error"
    finally:
        with _inflight_lock:
            _inflight.discard(cid)


def run_cycle(pool):
    t0 = time.monotonic()
    chunks, loads = load_snapshot()

    repairs, trims = {}, []
    for cid, ch in chunks.items():
        cls = classify(ch)
        if cls == "repair":
            repairs[cid] = pool.submit(run_guarded, cid, repair_chunk, ch)
        elif cls == "over":
            trims.append(pool.submit(run_guarded, cid, trim_chunk, ch, loads))
    failed = frozenset(cid for cid, job in repairs.items() if job.result() in ("degraded", "no_source", "error"))
    for job in trims:
        job.result()

    global _report
    with _report_lock:
        _report = {"state": "running", "at": now(), "cycle_ms": round((time.monotonic() - t0) * 1000),
                   "failed_repairs": failed}
    if repairs or trims:
        _, chunks = load_chunk_view()
    _log_summary(describe_replication(chunks, failed))


def _log_summary(c):
    """One line per CHANGE in the degraded picture, never one line per chunk per cycle."""
    global _last_summary
    counts = {state: c[state] for state in (
        "over_replicated", "waiting", "repair_pending", "degraded", "unavailable", "unrecoverable"
    )}
    key = tuple(counts.values())
    if key == _last_summary:
        return
    _last_summary = key
    if any(key):
        log.warning("replication not at target: %s", counts, extra=event("replication_status", **counts))
    else:
        log.info("replication healthy: every chunk has %d effective copies", RF,
                 extra=event("replication_healthy", replication_factor=RF))


# ---------- thread lifecycle ----------
def _run():
    global _report
    with _report_lock:
        _report = {"state": "grace", "grace_seconds": BLIP_WINDOW}
    log.info("repair monitor waiting %.0fs startup grace", BLIP_WINDOW)
    started = time.monotonic()
    while not _stop.is_set() and time.monotonic() - started < BLIP_WINDOW:
        _stop.wait(1)                       # a controller restart must not look like 'all workers died'

    pool = ThreadPoolExecutor(max_workers=config.REPAIR_PARALLELISM, thread_name_prefix="repair")
    log.info("repair monitor started (interval %.0fs, parallelism %d)",
             config.REPAIR_INTERVAL, config.REPAIR_PARALLELISM)
    while not _stop.is_set():
        repair_wakeup.wait(timeout=config.REPAIR_INTERVAL)   # woken early by corruption found on read
        repair_wakeup.clear()
        if _stop.is_set():
            break
        try:
            run_cycle(pool)
        except Exception:
            log.exception("repair cycle failed")
    pool.shutdown(wait=False)


def start():
    _stop.clear()
    threading.Thread(target=_run, name="repair-monitor", daemon=True).start()


def stop():
    _stop.set()
    repair_wakeup.set()
