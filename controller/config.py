import os

DEAD_AFTER = float(os.getenv("DEAD_AFTER", "10"))             # seconds without heartbeat -> dead
REPAIR_DELAY = float(os.getenv("REPAIR_DELAY", "15"))         # used by Step 10
LIVENESS_INTERVAL = float(os.getenv("LIVENESS_INTERVAL", "1"))  # how often the sweep runs
CHUNK_SIZE = 1024 * 1024
MAX_OBJECT_SIZE = int(os.getenv("MAX_OBJECT_SIZE", 1 << 30))
REPLICATION_FACTOR = 2
MAX_REPLACEMENT_ROUNDS = 3
WORKER_CONNECT_TIMEOUT = float(os.getenv("WORKER_CONNECT_TIMEOUT", 2))
WORKER_READ_TIMEOUT = float(os.getenv("WORKER_READ_TIMEOUT", 10))
REPAIR_INTERVAL = float(os.getenv("REPAIR_INTERVAL", "5"))     # seconds between cycles
REPAIR_PARALLELISM = int(os.getenv("REPAIR_PARALLELISM", "4"))  # concurrent chunk repairs
JANITOR_INTERVAL = float(os.getenv("JANITOR_INTERVAL", "30"))
STALE_UPLOAD_AGE = float(os.getenv("STALE_UPLOAD_AGE", "60"))
ORPHAN_MIN_AGE = float(os.getenv("ORPHAN_MIN_AGE", "600"))
SCRUB_INTERVAL = float(os.getenv("SCRUB_INTERVAL", "300"))
SCRUB_READ_TIMEOUT = float(os.getenv("SCRUB_READ_TIMEOUT", "120"))
DB_BACKUP_INTERVAL = float(os.getenv("DB_BACKUP_INTERVAL", "86400"))
