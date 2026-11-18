import random #in this using python sort so tie breaker is choosen randomly while assigning the least loaded worker
from db import get_conn

def pick_workers(exclude: set[str], k: int) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT w.worker_id, w.address, COUNT(cr.chunk_id) AS load
            FROM workers w
            LEFT JOIN chunk_replicas cr ON cr.worker_id = w.worker_id
            WHERE w.status = 'alive'
            GROUP BY w.worker_id
        """).fetchall()
    finally:
        conn.close()
    cands = [dict(r) for r in rows if r["worker_id"] not in exclude]
    random.shuffle(cands)
    cands.sort(key=lambda r: r["load"])
    return cands[:k]

def count_alive() -> int:
    conn = get_conn()
    try:
        return conn.execute("SELECT COUNT(*) FROM workers WHERE status='alive'").fetchone()[0]
    finally:
        conn.close()