"""Show chunk placement for a roughly 10 MiB upload and inspect each worker's files."""

from demo_common import MIB, WORKERS, expect, run_demo, show_map


def demo_chunking(demo, _args):
    with demo.fixture("chunking", 10 * MIB + 123) as (object_id, _):
        view = demo.chunk_map(object_id)
        chunks = view["chunks"]
        expect([c["index"] for c in chunks] == list(range(11)), "Expected 11 ordered chunks")
        expect([c["size"] for c in chunks] == [MIB] * 10 + [123], "Unexpected chunk sizes")
        show_map(view)
        for worker in WORKERS:
            print(f"\n{worker} /data/chunks (demo object only)")
            listing = demo.docker("exec", "-T", worker, "ls", "-lh", "/data/chunks")
            lines = {line.rsplit(maxsplit=1)[-1]: line for line in listing.splitlines() if line.strip()}
            actual = {name for name in lines if name.startswith(f"{object_id}_")}
            expected = {filename for c in chunks for r in c["replicas"] if r["worker_id"] == worker
                        for filename in (c["chunk_id"], c["chunk_id"] + ".sha256")}
            expect(actual == expected, f"{worker} files do not match the controller's chunk map")
            print("\n".join(lines[name] for name in sorted(actual)) or "(no chunks from this object)")


if __name__ == "__main__":
    raise SystemExit(run_demo(__doc__, demo_chunking, docker=True))
