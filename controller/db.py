import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH = os.getenv("DB_PATH", "/meta/controller.db")


def utc_now() -> str:
    """Controller-clock UTC ISO-8601. Never use worker clocks."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def get_conn() -> sqlite3.Connection:
    """New connection every call. Never share one across threads/requests."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5, isolation_level=None)  # autocommit; we control BEGIN
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def transaction():
    """Usage: with transaction() as conn: conn.execute(...)
    BEGIN IMMEDIATE takes the write lock up front, so two writers queue
    (up to busy_timeout) instead of failing mid-transaction with 'database is locked'."""
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def init_db() -> None:
    """Safe to call on every startup: everything is CREATE IF NOT EXISTS,
    and the DB file persists on the /meta volume."""
    with open(SCHEMA_PATH) as f:
        schema = f.read()
    conn = get_conn()
    try:
        conn.executescript(schema)
    finally:
        conn.close()