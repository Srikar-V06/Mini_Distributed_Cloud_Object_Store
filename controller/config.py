import os

DEAD_AFTER = float(os.getenv("DEAD_AFTER", "10"))             # seconds without heartbeat -> dead
REPAIR_DELAY = float(os.getenv("REPAIR_DELAY", "15"))         # used by Step 10
LIVENESS_INTERVAL = float(os.getenv("LIVENESS_INTERVAL", "1"))  # how often the sweep runs