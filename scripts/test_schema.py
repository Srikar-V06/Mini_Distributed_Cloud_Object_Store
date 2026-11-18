import os, sys, sqlite3, tempfile

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "controller"))

from db import init_db, get_conn, transaction, utc_now

init_db()
init_db()  # idempotency check

oid = "11111111-2222-4333-8444-555555555555"
now = utc_now()

with transaction() as c:
    c.execute("INSERT INTO objects VALUES (?,?,?,?,?,?,?,?,?,?)",
              (oid, "test.bin", 2_000_000, "application/octet-stream",
               1_048_576, now, now, "abc123", 2, "complete"))
    c.execute("INSERT INTO object_metadata VALUES (?,?,?)", (oid, "author", '"srikar"'))
    for i in range(2):
        cid = f"{oid}_{i:06d}"
        c.execute("INSERT INTO chunks VALUES (?,?,?,?,?)", (cid, oid, i, 1_000_000, f"sum{i}"))
        for w in ("worker1", "worker2"):
            c.execute("INSERT INTO chunk_replicas VALUES (?,?,?,?)", (cid, w, "ok", now))

c = get_conn()
count = lambda t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
assert (count("objects"), count("object_metadata"), count("chunks"), count("chunk_replicas")) == (1, 1, 2, 4)
print("insert ok")

# CHECK constraint must reject a bad state
try:
    with transaction() as t:
        t.execute("UPDATE chunk_replicas SET state='corupt'")
    raise SystemExit("FAIL: CHECK not enforced")
except sqlite3.IntegrityError:
    print("CHECK constraint ok")

# FK must reject an orphan chunk
try:
    with transaction() as t:
        t.execute("INSERT INTO chunks VALUES ('x_000000','nope',0,1,'s')")
    raise SystemExit("FAIL: FK not enforced")
except sqlite3.IntegrityError:
    print("FK constraint ok")

# Cascade delete
with transaction() as t:
    t.execute("DELETE FROM objects WHERE object_id=?", (oid,))
assert (count("objects"), count("object_metadata"), count("chunks"), count("chunk_replicas")) == (0, 0, 0, 0)
print("cascade ok")

print("WAL mode:", c.execute("PRAGMA journal_mode").fetchone()[0])