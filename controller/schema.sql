CREATE TABLE IF NOT EXISTS objects (
    object_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    size         INTEGER NOT NULL DEFAULT 0 CHECK (size >= 0),
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    chunk_size   INTEGER NOT NULL CHECK (chunk_size > 0),
    created_at   TEXT NOT NULL,
    modified_at  TEXT NOT NULL,
    checksum     TEXT,                       -- NULL until finalized
    num_chunks   INTEGER NOT NULL DEFAULT 0 CHECK (num_chunks >= 0),
    status       TEXT NOT NULL CHECK (status IN ('uploading','complete'))
);

CREATE TABLE IF NOT EXISTS object_metadata (
    object_id  TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    key        TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (object_id, key)
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,              -- {object_id}_{idx:06d}
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    idx       INTEGER NOT NULL CHECK (idx >= 0),
    size      INTEGER NOT NULL CHECK (size >= 0),
    checksum  TEXT NOT NULL,
    UNIQUE (object_id, idx)
);

CREATE TABLE IF NOT EXISTS chunk_replicas (
    chunk_id    TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL,
    state       TEXT NOT NULL CHECK (state IN ('ok','corrupt','lost')),
    verified_at TEXT,
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id      TEXT PRIMARY KEY,
    address        TEXT NOT NULL,
    last_heartbeat TEXT,
    status         TEXT NOT NULL CHECK (status IN ('alive','dead','unknown')),
    chunk_count    INTEGER NOT NULL DEFAULT 0,
    bytes_used     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pending_deletes (
    chunk_id   TEXT NOT NULL,
    worker_id  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE INDEX IF NOT EXISTS idx_replicas_worker ON chunk_replicas(worker_id);
CREATE INDEX IF NOT EXISTS idx_replicas_state  ON chunk_replicas(state);
CREATE INDEX IF NOT EXISTS idx_objects_status_created ON objects(status, created_at);