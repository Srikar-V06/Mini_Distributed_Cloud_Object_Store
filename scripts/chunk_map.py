import sqlite3, sys

oid = sys.argv[1]
c = sqlite3.connect("/meta/controller.db")
for row in c.execute("""
    SELECT c.idx, c.chunk_id, group_concat(cr.worker_id || ':' || cr.state)
    FROM chunks c JOIN chunk_replicas cr ON cr.chunk_id = c.chunk_id
    WHERE c.object_id LIKE ? GROUP BY c.chunk_id ORDER BY c.idx""", (oid + "%",)):
    print(row)