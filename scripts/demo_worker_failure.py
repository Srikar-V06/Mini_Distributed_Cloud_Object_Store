"""Stop a worker, read through the outage, and verify repair and rejoin."""

from demo_common import MIB, WORKERS, expect, fully_replicated, live_replicas, run_demo, show_map, timestamp


def choose_worker(view):
    holders = set().union(*(live_replicas(chunk) for chunk in view["chunks"]))
    return "worker2" if "worker2" in holders else min(holders)


def placement(view):
    return {c["chunk_id"]: live_replicas(c) for c in view["chunks"]}


def no_orphans(demo, object_id, view):
    """Compare fixture files, including sidecars, against their recorded locations."""
    if not fully_replicated(view) or any(len(c["replicas"]) != 2 for c in view["chunks"]):
        return False
    for worker in WORKERS:
        listing = demo.docker("exec", "-T", worker, "ls", "-1", "/data/chunks")
        actual = {name for name in listing.splitlines() if name.startswith(f"{object_id}_")}
        expected = {name for c in view["chunks"] if worker in live_replicas(c)
                    for name in (c["chunk_id"], f"{c['chunk_id']}.sha256")}
        if actual != expected:
            return False
    return view


def wait_for_rejoin(demo, worker, since):
    result = demo.wait_for(f"{worker} to reconcile its inventory",
                           lambda: demo.find_event(since, "worker_reconciled", worker_id=worker))
    print(f"{result['timestamp']} {result['message']}")
    return result


def brief_restart(demo, object_id, view):
    worker = choose_worker(view)
    original_placement = placement(view)
    since = timestamp()
    print(f"Bonus: brief outage of {worker}, followed by immediate restart after death is detected")
    with demo.stopped_worker(worker):
        demo.wait_for(f"{worker} to be marked dead", lambda: demo.worker_status(worker) == "dead")
        restart_at = timestamp()
    rejoined = wait_for_rejoin(demo, worker, restart_at)
    demo.wait_for("a repair-monitor cycle after the rejoin",
                  lambda: demo.get("replication").get("at", "") >= rejoined["timestamp"])
    after = demo.chunk_map(object_id)
    expect(placement(after) == original_placement, "Placement changed during the brief outage")
    expect(not any(e["event"] == "replica_repaired" and e.get("chunk_id") in original_placement
                   for e in demo.events(since)),
           "The brief outage triggered a repair. Rejoin may have exceeded the configured delay.")
    print("The brief outage did not trigger a repair or change the fixture placement")


def demo_failure(demo, args):
    demo.wait_for("the repair monitor to start", lambda: demo.get("replication")["state"] == "running")
    with demo.fixture("worker-failure", 4 * MIB) as (object_id, original):
        before = demo.chunk_map(object_id)
        expect(fully_replicated(before), "Expected a healthy fixture before stopping a worker")
        worker = choose_worker(before)
        if worker != "worker2":
            print(f"worker2 holds no copy of this fixture. Using {worker}, which holds a replica.")
        affected = {c["chunk_id"] for c in before["chunks"] if worker in live_replicas(c)}
        demo.docker("exec", "-T", worker, "test", "-f", f"/data/chunks/{min(affected)}")
        print("Before the outage")
        show_map(before)
        since = timestamp()
        print(f"Stopping {worker}")
        with demo.stopped_worker(worker):
            demo.wait_for(f"{worker} to be marked dead", lambda: demo.worker_status(worker) == "dead")
            failure = demo.wait_for("the worker failure log",
                                    lambda: demo.find_event(since, "worker_dead", worker_id=worker))
            print(f"{failure['timestamp']} {failure['message']}")
            report = demo.get("replication")
            under = sum(ch["object_id"] == object_id and ch["effective_replicas"] < report["replication_factor"]
                        for ch in report["chunks"])
            expect(under > 0, "No under-replicated fixture chunks observed while the worker was down")
            print(f"Under-replicated chunks: 0 -> {under} for this object ({report['under_replicated']} cluster-wide)")
            demo.download_and_verify(object_id, original)
            demo.wait_for("two live copies per chunk while the worker remains offline",
                          lambda: fully_replicated(demo.chunk_map(object_id)))
            repaired = {e["chunk_id"] for e in demo.events(since) if e["event"] == "replica_repaired"
                        and e.get("target_worker_id") != worker}
            expect(affected <= repaired, "Missing repair events for one or more affected chunks")
            print(f"All {len(affected)} affected chunks repaired on the remaining workers")
            show_map(demo.chunk_map(object_id))
            restart_at = timestamp()
            print(f"Starting {worker}")

        wait_for_rejoin(demo, worker, restart_at)
        final = demo.wait_for("two replicas per chunk and no fixture orphans",
                             lambda: no_orphans(demo, object_id, demo.chunk_map(object_id)))
        show_map(final)
        print("All four worker directories match the controller's placement for this fixture")
        if args.brief_restart:
            brief_restart(demo, object_id, final)


if __name__ == "__main__":
    raise SystemExit(run_demo(__doc__, demo_failure, docker=True, brief_restart=True))
