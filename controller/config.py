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