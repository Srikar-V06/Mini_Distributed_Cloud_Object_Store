"""Upload, download, and compare the source and downloaded SHA-256 hashes."""

from demo_common import MIB, run_demo


def demo_storage(demo, _args):
    with demo.fixture("object-storage", MIB + 123) as (object_id, source):
        demo.download_and_verify(object_id, source)


if __name__ == "__main__":
    raise SystemExit(run_demo(__doc__, demo_storage))
