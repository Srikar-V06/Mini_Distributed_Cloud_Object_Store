"""Damage a disposable replica, verify the download, and watch the chunk heal."""

from demo_common import ROOT, cli, expect, fully_replicated, live_replicas, run_demo, show_map, timestamp

MODES = ("flip", "truncate", "append", "delete", "zero", "sidecar")


def scrub(demo):
    response = demo.client.post("/api/v1/admin/scrub")
    if response.status_code == 409:
        return None
    return cli.response_json(response)


def demo_integrity(demo, _args):
    demo.wait_for("the repair monitor to start", lambda: demo.get("replication")["state"] == "running")
    helper = (ROOT / "scripts/corrupt_chunk.py").read_text(encoding="utf-8")
    with demo.fixture("integrity", 32768) as (object_id, original):
        for mode in MODES:
            view = demo.chunk_map(object_id)
            expect(fully_replicated(view), "Expected two healthy copies before injecting corruption")
            chunk = view["chunks"][0]
            chunk_id = chunk["chunk_id"]
            worker = min(live_replicas(chunk))
            since = timestamp()
            print(f"\n{mode}: {chunk_id} on {worker}")
            print(demo.docker("exec", "-T", worker, "python", "-", chunk_id, mode, source=helper).strip())
            demo.wait_for("an on-demand scrub to complete", lambda: scrub(demo))
            event_name = "replica_lost" if mode == "delete" else "replica_corrupt"
            detection = demo.wait_for(f"{mode} to appear in controller logs",
                                      lambda: demo.find_event(since, event_name, chunk_id=chunk_id, worker_id=worker))
            print(f"{detection['timestamp']} {detection['message']}")
            demo.download_and_verify(object_id, original)
            repair = demo.wait_for("a successful repair event",
                                  lambda: demo.find_event(since, "replica_repaired", chunk_id=chunk_id))
            print(f"{repair['timestamp']} {repair['message']}")
            demo.wait_for("two effective replicas", lambda: fully_replicated(demo.chunk_map(object_id)))
            healed = demo.chunk_map(object_id)
            for holder in live_replicas(healed["chunks"][0]):
                digest = demo.docker("exec", "-T", holder, "python", "-", chunk_id, source=(
                    "import sys, storage\n"
                    "print(storage.read_verified(sys.argv[1])[1])\n"
                )).strip()
                expect(digest == chunk["checksum"], f"Repaired copy on {holder} failed SHA-256 verification")
            show_map(healed)


if __name__ == "__main__":
    raise SystemExit(run_demo(__doc__, demo_integrity, docker=True))
