import sqlite3, sys

oid = sys.argv[1] if len(sys.argv) > 1 else "1080e6b0"
c = sqlite3.connect("/meta/controller.db")

print("chunks with != 2 distinct workers (expect none):")
print(c.execute("""SELECT chunk_id, COUNT(DISTINCT worker_id) n FROM chunk_replicas
                   WHERE chunk_id LIKE ? GROUP BY chunk_id HAVING n <> 2""",
                (oid + "%",)).fetchall())

print("replicas per worker (expect total 400):")
print(c.execute("""SELECT worker_id, COUNT(*) FROM chunk_replicas
                   WHERE chunk_id LIKE ? GROUP BY worker_id""",
                (oid + "%",)).fetchall())

print("object row:")
print(c.execute("SELECT status, num_chunks, size FROM objects WHERE object_id LIKE ?",
                (oid + "%",)).fetchall())