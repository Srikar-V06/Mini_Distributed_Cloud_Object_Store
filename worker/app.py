
import os
from fastapi import FastAPI

app = FastAPI()

WORKER_ID = os.environ["WORKER_ID"]


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": WORKER_ID
    }
