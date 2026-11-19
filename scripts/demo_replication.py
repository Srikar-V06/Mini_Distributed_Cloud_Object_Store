"""Show two verified copies on distinct live workers for every chunk."""

from demo_common import MIB, expect, fully_replicated, run_demo, show_map


def demo_replication(demo, _args):
    with demo.fixture("replication", 3 * MIB) as (object_id, _):
        view = demo.chunk_map(object_id)
        show_map(view)
        expect(view["replication_factor"] == 2 and fully_replicated(view)
               and all(len(c["replicas"]) == 2 for c in view["chunks"]),
               "Each chunk must have exactly two healthy replicas on distinct workers")


if __name__ == "__main__":
    raise SystemExit(run_demo(__doc__, demo_replication))
