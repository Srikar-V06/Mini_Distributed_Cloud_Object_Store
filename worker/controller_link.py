import logging
import os
import threading

import httpx

import storage

log = logging.getLogger("worker.link")

WORKER_ID = os.environ["WORKER_ID"]
CONTROLLER_URL = os.environ["CONTROLLER_URL"].rstrip("/")
ADDRESS = os.getenv("WORKER_ADDRESS", f"http://{WORKER_ID}:8000")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "3"))
TIMEOUT = httpx.Timeout(10.0, connect=2.0)


def _payload() -> dict:
    return {"worker_id": WORKER_ID, "address": ADDRESS, **storage.stats()}


def _register_with_backoff(client: httpx.Client, stop: threading.Event) -> bool:
    delay = 1.0
    while not stop.is_set():
        try:
            r = client.post(f"{CONTROLLER_URL}/internal/workers/register", json=_payload())
            r.raise_for_status()
            log.info("registered with controller as %s", WORKER_ID)
            return True
        except Exception as e:
            log.warning("registration failed (%s), retrying in %.0fs", e, delay)
        stop.wait(delay)
        delay = min(delay * 2, 30)
    return False


def run(stop: threading.Event) -> None:
    failures = 0
    with httpx.Client(timeout=TIMEOUT) as client:
        if not _register_with_backoff(client, stop):
            return
        while not stop.wait(HEARTBEAT_INTERVAL):
            try:
                r = client.post(f"{CONTROLLER_URL}/internal/workers/heartbeat", json=_payload())
                if r.status_code == 404:
                    log.warning("controller doesn't know us, re-registering")
                    if not _register_with_backoff(client, stop):
                        return
                    continue
                r.raise_for_status()
                if failures:
                    log.info("heartbeat recovered after %d failures", failures)
                failures = 0
            except Exception as e:
                failures += 1
                if failures == 1 or failures % 10 == 0:   # avoid log spam
                    log.warning("heartbeat failed x%d: %s", failures, e)