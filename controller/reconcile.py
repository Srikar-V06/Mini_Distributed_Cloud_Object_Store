import logging

log = logging.getLogger("controller.reconcile")


def reconcile_worker(worker_id: str) -> None:
    log.info("reconcile requested for %s (implemented in Step 11)", worker_id)