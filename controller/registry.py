import logging
import time
from datetime import datetime, timedelta, timezone
import threading

from config import DEAD_AFTER, REPAIR_DELAY, LIVENESS_INTERVAL
from db import get_conn, transaction, utc_now
from event_log import event

log = logging.getLogger("controller.registry")

# Monotonic start time of this controller process. Drives the startup grace period.
STARTED_AT = time.monotonic()


def _cutoff(seconds: float) -> str:
    dt = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return dt.isoformat(timespec="milliseconds")


def grace_period_over() -> bool:
    """Step 10's repair monitor must not act until this is True."""
    return time.monotonic() - STARTED_AT > DEAD_AFTER + REPAIR_DELAY


def mark_all_unknown() -> None:
    with transaction() as c:
        n = c.execute("UPDATE workers SET status='unknown'").rowcount
    log.info("startup: %d known workers set to 'unknown' until their first heartbeat", n)


def register(worker_id: str, address: str, chunk_count: int, bytes_used: int) -> str:
    """Upsert. Returns the previous status ('new' if first time seen)."""
    now = utc_now()
    with transaction() as c:
        row = c.execute("SELECT status FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        c.execute(
            """INSERT INTO workers(worker_id, address, last_heartbeat, status, chunk_count, bytes_used)
               VALUES (?, ?, ?, 'alive', ?, ?)
               ON CONFLICT(worker_id) DO UPDATE SET
                   address=excluded.address,
                   last_heartbeat=excluded.last_heartbeat,
                   status='alive',
                   chunk_count=excluded.chunk_count,
                   bytes_used=excluded.bytes_used""",
            (worker_id, address, now, chunk_count, bytes_used),
        )
    prev = row["status"] if row else "new"
    log.info("worker %s registered at %s (previous status: %s)", worker_id, address, prev,
             extra=event("worker_registered" if prev == "new" else "worker_rejoined",
                         worker_id=worker_id, status="alive", previous_status=prev, via="registration"))
    return prev


def heartbeat(worker_id: str, chunk_count: int, bytes_used: int) -> str | None:
    """Returns the previous status, or None if the worker is unknown (-> HTTP 404)."""
    now = utc_now()
    with transaction() as c:
        row = c.execute("SELECT status FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        if row is None:
            return None
        c.execute(
            """UPDATE workers SET last_heartbeat=?, status='alive', chunk_count=?, bytes_used=?
               WHERE worker_id=?""",
            (now, chunk_count, bytes_used, worker_id),
        )
    prev = row["status"]
    if prev != "alive":
        log.info("worker %s is ALIVE again (was %s)", worker_id, prev,
                 extra=event("worker_rejoined", worker_id=worker_id, status="alive",
                             previous_status=prev, via="heartbeat"))
    return prev


def sweep() -> None:
    """Mark silent workers dead. Runs every LIVENESS_INTERVAL seconds."""
    stale = _cutoff(DEAD_AFTER)
    past_startup_window = (time.monotonic() - STARTED_AT) > DEAD_AFTER
    with transaction() as c:
        dead = [r["worker_id"] for r in c.execute(
            "SELECT worker_id FROM workers WHERE status='alive' AND last_heartbeat < ?", (stale,))]
        if past_startup_window:
            # still 'unknown' after a full DEAD_AFTER since controller start => never came back
            dead += [r["worker_id"] for r in c.execute(
                "SELECT worker_id FROM workers WHERE status='unknown'")]
        if dead:
            c.executemany("UPDATE workers SET status='dead' WHERE worker_id=?",
                          [(w,) for w in dead])
    for w in dead:
        log.warning("worker %s marked DEAD (no heartbeat for >%ss)", w, DEAD_AFTER,
                    extra=event("worker_dead", worker_id=w, status="dead", timeout_seconds=DEAD_AFTER))


def liveness_loop(stop: threading.Event) -> None:
    while not stop.wait(LIVENESS_INTERVAL):
        try:
            sweep()
        except Exception:
            log.exception("liveness sweep failed")   # the thread must never die


def _rows(sql: str, params=()) -> list[dict]:
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def alive_workers() -> list[dict]:
    """Placement (Step 6) and repair (Step 10) call this. Only 'alive' counts."""
    return _rows("SELECT * FROM workers WHERE status='alive' ORDER BY worker_id")


def list_workers() -> list[dict]:
    now = datetime.now(timezone.utc)
    out = _rows("SELECT * FROM workers ORDER BY worker_id")
    for w in out:
        hb = w["last_heartbeat"]
        w["seconds_since_heartbeat"] = (
            round((now - datetime.fromisoformat(hb)).total_seconds(), 1) if hb else None)
    return out
