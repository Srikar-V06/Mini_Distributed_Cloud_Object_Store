import sqlite3

c = sqlite3.connect("/meta/controller.db")
for row in c.execute("SELECT object_id, name, size, num_chunks, status FROM objects ORDER BY created_at"):
    print(row)
    