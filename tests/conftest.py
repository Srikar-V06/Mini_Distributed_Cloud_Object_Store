"""Create an independent five-container cluster for each pytest session."""

import json
from pathlib import Path
import subprocess
import socket
import time
import uuid

import pytest
import httpx

ROOT = Path(__file__).resolve().parents[1]
WORKERS = tuple(f"worker{i}" for i in range(1, 5))


def pytest_terminal_summary(terminalreporter):
    # Keep the standard case verdict lines intact for the console catalog.
    for reports in terminalreporter.stats.values():
        for report in reports:
            if getattr(report, "when", None) == "call":
                for line in report.capstdout.splitlines():
                    if line.startswith("DRILL TC-"):
                        terminalreporter.write_line(line)


def wait(check, timeout=90):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if check():
                return
        except httpx.TransportError:
            pass
        time.sleep(0.3)
    raise AssertionError(f"Condition still false after {timeout}s")


@pytest.fixture(scope="session")
def cluster(tmp_path_factory):
    name = "storeqa" + uuid.uuid4().hex[:12]
    path = tmp_path_factory.mktemp("compose") / "cluster.json"
    model = json.loads(
        subprocess.check_output(
            [
                "docker",
                "compose",
                "-f",
                "docker-compose.yml",
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
        )
    )
    model["name"] = name
    model["networks"] = {"default": {}}
    model["volumes"] = {key: {} for key in model["volumes"]}
    sockets = []
    try:
        for service in model["services"].values():
            listener = socket.socket()
            sockets.append(listener)
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            service.pop("container_name", None)
            service["ports"] = [f"127.0.0.1:{port}:8000"]
    finally:
        for listener in sockets:
            listener.close()
    path.write_text(json.dumps(model))

    def compose(*args, source=None):
        return subprocess.run(
            ["docker", "compose", "-p", name, "-f", str(path), *args],
            input=source,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=300,
            check=True,
        ).stdout.strip()

    try:
        compose("up", "-d", "--build", "--wait")
        urls = {s: "http://" + compose("port", s, "8000") for s in model["services"]}
        yield compose, urls
    finally:
        compose("down", "-v", "--remove-orphans", "--rmi", "local")
