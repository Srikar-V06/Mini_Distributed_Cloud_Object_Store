"""Shared setup, cleanup, and polling for the Step 15 demos."""

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "client"))
import cli

MIB = 1024 * 1024
WORKERS = ("worker1", "worker2", "worker3", "worker4")
ADMIN = "/api/v1/admin"


class DemoError(Exception):
    pass


def expect(condition, message):
    if not condition:
        raise DemoError(message)


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def live_replicas(chunk):
    return {r["worker_id"] for r in chunk["replicas"]
            if r["state"] == "ok" and r["worker_status"] == "alive"}


def fully_replicated(view):
    target = view["replication_factor"]
    return bool(view["chunks"]) and all(
        c["effective_replicas"] == len(live_replicas(c)) == target for c in view["chunks"]
    )


def show_map(view):
    print(f"{'Index':>5}  {'Bytes':>9}  {'Live':>4}  Workers (replica/worker state)")
    for chunk in view["chunks"]:
        workers = ", ".join(f"{r['worker_id']} ({r['state']}/{r['worker_status']})"
                            for r in chunk["replicas"])
        print(f"{chunk['index']:>5}  {chunk['size']:>9}  {chunk['effective_replicas']:>4}  {workers}")


class Demo:
    def __init__(self, client, timeout, docker_path):
        self.client = client
        self.timeout = timeout
        self.docker_path = docker_path

    def get(self, path):
        return cli.response_json(self.client.get(f"{ADMIN}/{path}", timeout=5))

    def chunk_map(self, object_id):
        return self.get(f"chunks/{object_id}")

    def wait_for(self, label, check):
        print(f"Waiting for {label} ...", flush=True)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                result = check()
                if result:
                    return result
            except httpx.TransportError:
                pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DemoError(f"Timed out waiting for {label}")
            time.sleep(min(0.5, remaining))

    def docker(self, *args, source=None):
        result = subprocess.run([self.docker_path, "compose", *args], cwd=ROOT, input=source,
                                capture_output=True, text=True, timeout=45)
        expect(result.returncode == 0, result.stderr.strip() or result.stdout.strip()
               or f"Docker Compose exited with code {result.returncode}")
        return result.stdout

    def events(self, since):
        for line in self.docker("logs", "--no-color", "--since", since, "controller").splitlines():
            _, _, message = line.partition(" | ")
            if message.startswith("{"):
                yield json.loads(message)

    def find_event(self, since, name, **fields):
        return next((entry for entry in self.events(since)
                     if entry["event"] == name and all(entry.get(k) == v for k, v in fields.items())), None)

    def worker_status(self, worker_id):
        return next((w["status"] for w in self.get("workers")["workers"]
                     if w["worker_id"] == worker_id), None)

    @contextmanager
    def stopped_worker(self, worker_id):
        expect(self.worker_status(worker_id) == "alive", f"{worker_id} must be alive before the demo")
        try:
            self.docker("stop", worker_id)
            yield
        finally:
            self.docker("start", worker_id)
            self.wait_for(f"{worker_id} to register or resume heartbeats",
                          lambda: self.worker_status(worker_id) == "alive")

    def download_and_verify(self, object_id, original):
        target = original.with_name("downloaded.bin")
        result = cli.download(self.client, argparse.Namespace(id=object_id, out=target))
        hashes = cli.verify(original, target)
        expect(result["checksum"] == hashes["original_checksum"], "Downloaded SHA-256 differs from the source")
        print(f"Original and downloaded SHA-256 match: {result['checksum']}")
        return result

    @contextmanager
    def fixture(self, label, size, metadata=None):
        with tempfile.TemporaryDirectory(prefix="objectstore-demo-") as directory:
            source = Path(directory) / f"{label}.bin"
            with source.open("wb") as file:
                remaining = size
                while remaining:
                    data = os.urandom(min(remaining, MIB))
                    file.write(data)
                    remaining -= len(data)
            uploaded = cli.upload(self.client, argparse.Namespace(file=source, meta=metadata or {}))
            object_id = uploaded["object_id"]
            print(f"Uploaded {label}: {size} bytes, {uploaded['num_chunks']} chunks, ID {object_id}", flush=True)
            try:
                yield object_id, source
            finally:
                try:
                    cli.delete(self.client, argparse.Namespace(id=object_id))
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 404:
                        raise
                print(f"Cleaned up demo object {object_id}")


def run_demo(doc, scenario, *, docker=False, brief_restart=False):
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument("--base", default=os.getenv("OBJECT_STORE_URL", "http://localhost:8000"),
                        help="controller URL (defaults to localhost:8000)")
    parser.add_argument("--timeout", type=float, default=120, help="maximum seconds per readiness check")
    if docker:
        parser.add_argument("--docker", help="path to the Docker executable")
    if brief_restart:
        parser.add_argument("--brief-restart", action="store_true",
                            help="also rejoin within the repair delay and check that no repair occurs")
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive number")

    try:
        docker_path = None
        if docker:
            docker_path = args.docker or shutil.which("docker")
            if not docker_path and os.getenv("LOCALAPPDATA"):
                candidate = Path(os.environ["LOCALAPPDATA"]) / "Programs/DockerDesktop/resources/bin/docker.exe"
                if candidate.is_file():
                    docker_path = str(candidate)
            expect(docker_path, "Docker CLI not found. Add it to PATH or pass --docker.")
        with httpx.Client(base_url=args.base.rstrip("/"), timeout=cli.TIMEOUT) as client:
            demo = Demo(client, args.timeout, docker_path)
            if docker:
                running = set(demo.docker("ps", "--status", "running", "--services").splitlines())
                expect({"controller", *WORKERS} <= running, "Start the controller and all four Compose workers")
            demo.wait_for("all four workers to be alive", lambda: demo.get("workers")["alive_count"] == 4)
            scenario(demo, args)
    except httpx.HTTPStatusError as exc:
        error = cli.http_error(exc).body
        print(f"FAIL: {error['error']}: {error['message']}", file=sys.stderr)
        return 1
    except (DemoError, cli.CliError, httpx.HTTPError, httpx.InvalidURL, OSError, subprocess.TimeoutExpired) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("PASS")
    return 0
