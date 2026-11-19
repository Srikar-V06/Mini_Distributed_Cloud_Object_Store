import hashlib
import logging
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import config
from db import get_conn, transaction
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


def is_inflight(chunk_id: str) -> bool:        # Step 12's scrubber will call this
    with _inflight_lock:
        return chunk_id in _inflight


def _bump(key: str):
    with _report_lock:
        _totals[key] += 1


def get_report() -> dict:
    with _report_lock:
        return dict(_report)


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
    log.warning("replica %s on %s marked %s", chunk_id, worker_id, state)


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
SELECT c.chunk_id, c.size, c.checksum,
       cr.worker_id, cr.state, w.status AS wstatus, w.address, w.last_heartbeat
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
    chunks: dict[str, dict] = {}
    for r in rows:
        ch = chunks.setdefault(r["chunk_id"], {"chunk_id": r["chunk_id"], "size": r["size"],
                                               "checksum": r["checksum"], "replicas": []})
        if r["worker_id"] is not None:
            ch["replicas"].append({"worker_id": r["worker_id"], "state": r["state"],
                                   "wstatus": r["wstatus"], "address": r["address"],
                                   "age": _age(r["last_heartbeat"])})
    return chunks, loads


def classify(ch) -> str:
    reps = ch["replicas"]
    alive_ok = [r for r in reps if r["state"] == "ok" and r["wstatus"] == "alive"]
    dead_ok = [r for r in reps if r["state"] == "ok" and r["wstatus"] != "alive"]
    blips = [r for r in dead_ok if r["age"] <= BLIP_WINDOW]     # might come back soon
    n = len(alive_ok)
    if n > RF:
        return "over"
    if n == RF:
        return "healthy"
    if n + len(blips) >= RF:
        return "waiting"
    if n == 0:
        return "unavailable" if dead_ok else "unrecoverable"
    return "repair"


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
    made = 0
    while made < need:
        target = next_target(inplace, tried)
        if target is None:
            break                                                      # no eligible target: degraded
        tried.add(target["worker_id"])
        log.info("chunk %s under-replicated (%d/%d): copying %s -> %s",
                 cid, good + made, RF, src["worker_id"], target["worker_id"])
        if not send_chunk(target, cid, data, ch["checksum"]):          # worker verifies before 201
            log.warning("copy of %s to %s failed, trying another target", cid, target["worker_id"])
            continue
        if not record_replica(cid, target["worker_id"]):               # row only AFTER verification
            if not delete_chunk(target["address"], cid):               # object deleted mid-copy
                queue_pending_delete(cid, target["worker_id"])
            return "deleted"
        made += 1
        _bump("repairs")
    return "repaired" if made >= need else "degraded"


# ---------- trim one over-replicated chunk ----------
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
        key = (cid, e["worker_id"])
        with transaction() as conn:
            conn.execute("DELETE FROM chunk_replicas WHERE chunk_id=? AND worker_id=?", key)
            conn.execute("INSERT OR IGNORE INTO pending_deletes VALUES (?,?,?)", (*key, now()))
        if delete_chunk(e["address"], cid):
            with transaction() as conn:
                conn.execute("DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?", key)
        log.info("chunk %s over-replicated: removed extra copy from %s", cid, e["worker_id"])
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

    classes, futures = {}, {}
    for cid, ch in chunks.items():
        cls = classify(ch)
        classes[cid] = cls
        if cls == "repair":
            futures[cid] = pool.submit(run_guarded, cid, repair_chunk, ch)
        elif cls == "over":
            futures[cid] = pool.submit(run_guarded, cid, trim_chunk, ch, loads)
    results = {cid: f.result() for cid, f in futures.items()}

    # counts AFTER the work, so the report shows where the system stands now
    c = dict(healthy=0, over=0, under=0, waiting=0, degraded=0, unavailable=0, unrecoverable=0)
    samples = {"degraded": [], "unavailable": [], "unrecoverable": []}
    for cid, cls in classes.items():
        res = results.get(cid)
        if cls == "healthy":
            c["healthy"] += 1
        elif cls == "waiting":
            c["waiting"] += 1; c["under"] += 1
        elif cls in ("unavailable", "unrecoverable"):
            c[cls] += 1; c["under"] += 1
            if len(samples[cls]) < 20:
                samples[cls].append(cid)
        elif cls == "repair":
            if res in ("repaired", "deleted"):
                c["healthy"] += 1
            else:
                c["degraded"] += 1; c["under"] += 1
                if len(samples["degraded"]) < 20:
                    samples["degraded"].append(cid)
        elif cls == "over":
            if res == "trimmed":
                c["healthy"] += 1
            else:
                c["over"] += 1

    global _report
    with _report_lock:
        _report = {"state": "running", "at": now(), "cycle_ms": round((time.monotonic() - t0) * 1000),
                   "total_chunks": len(chunks), "healthy": c["healthy"],
                   "over_replicated": c["over"], "under_replicated": c["under"],
                   "waiting": c["waiting"], "degraded": c["degraded"],
                   "unavailable": c["unavailable"], "unrecoverable": c["unrecoverable"],
                   "repairs_done": _totals["repairs"], "trims_done": _totals["trims"],
                   "sample": samples}
    _log_summary(c)


def _log_summary(c):
    """One line per CHANGE in the degraded picture, never one line per chunk per cycle."""
    global _last_summary
    key = (c["waiting"], c["degraded"], c["unavailable"], c["unrecoverable"])
    if key == _last_summary:
        return
    _last_summary = key
    if any(key):
        log.warning("replication not at target: waiting=%d degraded(no eligible target)=%d "
                    "unavailable=%d unrecoverable=%d", *key)
    else:
        log.info("replication healthy: every chunk has %d verified copies", RF)


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
