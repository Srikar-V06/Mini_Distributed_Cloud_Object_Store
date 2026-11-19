import logging
import threading
from datetime import datetime, timedelta, timezone

import config
import reconcile
from db import backup_if_due, transaction, utc_now
from upload import active_uploads

log = logging.getLogger("controller.janitor")


def clean_uploads(*, startup: bool = False) -> None:
    """Queue known copies for deletion before cascading away their metadata."""
    sql = "SELECT object_id FROM objects WHERE status='uploading'"
    params = ()
    if not startup:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=config.STALE_UPLOAD_AGE)
        sql += " AND created_at < ?"
        params = (cutoff.isoformat(timespec="microseconds"),)
    with transaction() as conn:
        stale = [row["object_id"] for row in conn.execute(sql, params)
                 if startup or row["object_id"] not in active_uploads]
        for object_id in stale:
            conn.execute("""
                INSERT OR IGNORE INTO pending_deletes (chunk_id, worker_id, created_at)
                SELECT cr.chunk_id, cr.worker_id, ? FROM chunk_replicas cr
                JOIN chunks c USING (chunk_id) WHERE c.object_id=?""", (utc_now(), object_id))
            conn.execute("DELETE FROM objects WHERE object_id=?", (object_id,))
    if stale:
        log.info("removed %d abandoned uploads (%s)", len(stale), "startup" if startup else "periodic")


def run(stop: threading.Event) -> None:
    while not stop.wait(config.JANITOR_INTERVAL):
        try:
            clean_uploads()
            reconcile.retry_pending_deletes()
            reconcile.retry_due_reconciliations()
        except Exception:
            log.exception("janitor cycle failed")
        try:
            if backup_if_due(config.DB_BACKUP_INTERVAL):
                log.info("saved controller metadata backup")
        except Exception:
            log.exception("controller metadata backup failed")
