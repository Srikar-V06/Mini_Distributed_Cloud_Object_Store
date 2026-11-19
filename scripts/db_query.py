import sqlite3, sys

c = sqlite3.connect("/meta/controller.db")
for row in c.execute(sys.argv[1]):
    print(row)