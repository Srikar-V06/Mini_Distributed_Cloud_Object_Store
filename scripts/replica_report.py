import sqlite3, sys
from collections import Counter

c = sqlite3.connect("/meta/controller.db")
rows = c.execute("""
    SELECT c.chunk_id,
           COALESCE(SUM(CASE WHEN cr.state='ok' AND w.status='alive' THEN 1 ELSE 0 END), 0) AS eff,
           group_concat(cr.worker_id || ':' || cr.state || ':' || COALESCE(w.status, '?'))
    FROM chunks c
    JOIN objects o ON o.object_id = c.object_id AND o.status = 'complete'
    LEFT JOIN chunk_replicas cr ON cr.chunk_id = c.chunk_id
    LEFT JOIN workers w ON w.worker_id = cr.worker_id
    GROUP BY c.chunk_id""").fetchall()

print("effective replicas -> chunks:", dict(sorted(Counter(r[1] for r in rows).items())))
limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
for r in [r for r in rows if r[1] != 2][:limit]:
    print("  ", r)