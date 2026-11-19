"""Demonstrate adding, changing, and removing metadata without re-uploading."""

import argparse
import json

from demo_common import cli, expect, run_demo


def demo_metadata(demo, _args):
    initial = {"version": 1, "remove": True}
    with demo.fixture("metadata", 4096, initial) as (object_id, _):
        before = cli.metadata(demo.client, argparse.Namespace(id=object_id))
        changes = {"version": 2, "label": "demo", "remove": None}
        cli.patch_metadata(demo.client, argparse.Namespace(id=object_id, values=changes))
        after = cli.metadata(demo.client, argparse.Namespace(id=object_id))
        print(f"Before: {json.dumps(before['custom'])}")
        print(f"PATCH:  {json.dumps(changes)}")
        print(f"After:  {json.dumps(after['custom'])}")
        expect(before["custom"] == initial and after["custom"] == {"version": 2, "label": "demo"},
               "Custom metadata did not match the expected merge patch")
        for key in ("checksum", "created_at"):
            expect(after["system"][key] == before["system"][key], f"{key} changed unexpectedly")
            print(f"{key} unchanged: {after['system'][key]}")
        old, new = before["system"]["modified_at"], after["system"]["modified_at"]
        expect(new > old, "modified_at did not advance")
        print(f"modified_at advanced: {old} -> {new}")
        expect(after["chunks"] == before["chunks"], "Chunk map changed during the metadata patch")
        print("Chunk map unchanged")


if __name__ == "__main__":
    raise SystemExit(run_demo(__doc__, demo_metadata))
