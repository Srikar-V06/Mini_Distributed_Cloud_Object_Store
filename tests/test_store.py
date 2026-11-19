"""TC numbers follow the project's comprehensive verification specification."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from hashlib import sha256
import json
import math
import os
import subprocess
import tempfile
import zipfile
from pathlib import Path
import threading
import time
import uuid

import httpx
import pytest

from conftest import ROOT, WORKERS, wait

M = 1048576
OBJ = "/api/v1/objects"
ADMIN = "/api/v1/admin"


def patterned(size, seed=0):
    return b"".join(
        bytes([(i // M + seed) % 256]) * min(M, size - i) for i in range(0, size, M)
    )


def error(response, status):
    assert response.status_code == status, response.text[:500]
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["error"] and response.json()["message"]


def parallel(count, fn):
    gate = threading.Barrier(count)

    def client(index):
        gate.wait(timeout=30)
        return fn(index)

    with ThreadPoolExecutor(count) as pool:
        return list(pool.map(client, range(count)))


class Store:
    def __init__(self, cluster, client):
        self.compose, self.urls = cluster
        self.http = client
        self.ids = []

    def post(self, data, name="qa.bin", custom=None, mime="application/octet-stream"):
        return self.http.post(
            OBJ,
            files={"file": (name, data, mime)},
            data={"metadata": json.dumps(custom or {})},
        )

    def upload(self, data, **kw):
        response = self.post(data, **kw)
        assert response.status_code == 201, response.text
        oid = response.json()["object_id"]
        self.ids.append(oid)
        return oid

    def meta(self, oid):
        return self.http.get(f"{OBJ}/{oid}/metadata").raise_for_status().json()

    def chunks(self, oid):
        return (
            self.http.get(f"{ADMIN}/chunks/{oid}").raise_for_status().json()["chunks"]
        )

    def inventory(self, worker):
        url = self.urls[worker] + f"/internal/workers/{worker}/chunks"
        return self.http.get(url).raise_for_status().json()["chunks"]

    def snapshot(self, workers=WORKERS):
        return {
            (w, c["chunk_id"]): c["checksum"]
            for w in workers
            for c in self.inventory(w)
        }

    def verify(self, oid, data):
        r = self.http.get(f"{OBJ}/{oid}")
        assert r.status_code == 200 and r.content == data
        assert r.headers["x-object-checksum"] == sha256(data).hexdigest()
        return r

    def alive(self):
        return (
            self.http.get(ADMIN + "/workers").raise_for_status().json()["alive_count"]
        )

    @contextmanager
    def outage(self, workers):
        try:
            self.compose("stop", *workers)
            yield
        finally:
            self.compose("start", *workers)
            wait(
                lambda: all(
                    self.http.get(self.urls[w] + "/health").is_success for w in workers
                )
                and all(
                    row["status"] == "alive"
                    for row in self.http.get(ADMIN + "/workers").json()["workers"]
                    if row["worker_id"] in workers
                )
            )

    def damage(self, worker, cid, mode):
        return self.compose(
            "exec",
            "-T",
            worker,
            "python",
            "-",
            cid,
            mode,
            source=(ROOT / "scripts/corrupt_chunk.py").read_text(),
        )

    def scrub(self):
        def completed():
            r = self.http.post(ADMIN + "/scrub")
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True

        wait(completed)


def holders(chunk):
    return {
        r["worker_id"]
        for r in chunk["replicas"]
        if r["state"] == "ok" and r["worker_status"] == "alive"
    }


def replicated(rows):
    return bool(rows) and all(
        c["effective_replicas"] == len(holders(c)) == 2 for c in rows
    )


@pytest.fixture
def s(cluster):
    with httpx.Client(
        base_url=cluster[1]["controller"], timeout=120, trust_env=False
    ) as client:
        store = Store(cluster, client)
        wait(lambda: store.alive() == 4)
        try:
            yield store
        finally:
            for oid in store.ids:
                r = client.delete(f"{OBJ}/{oid}")
                assert r.status_code in (204, 404), r.text


@pytest.mark.parametrize("tc", range(1, 71), ids=lambda n: f"TC-{n:02}")
def test_case(s, tc):
    if tc <= 6:
        startup(s, tc)
    elif tc <= 29 or tc in (62, 69):
        objects(s, tc)
    elif tc <= 41:
        validation(s, tc)
    elif tc <= 47:
        internal(s, tc)
    elif tc <= 56 or tc == 70:
        recovery(s, tc)
    elif tc <= 63:
        integrity(s, tc)
    else:
        concurrency(s, tc)


def startup(s, tc):
    if tc == 1:
        assert set(
            s.compose("ps", "--services", "--status", "running").splitlines()
        ) == {"controller", *WORKERS}
        for w in WORKERS:
            assert "/data" in s.compose("exec", "-T", w, "cat", "/proc/mounts")
    elif tc in (2, 3):

        def registry():
            return {
                r["worker_id"]: r
                for r in s.http.get(ADMIN + "/workers").json()["workers"]
            }

        rows = registry()
        assert set(rows) == set(WORKERS)
        assert all(row["status"] == "alive" and row["address"] for row in rows.values())
        if tc == 3:
            wait(
                lambda: all(
                    v["last_heartbeat"] != rows[k]["last_heartbeat"]
                    for k, v in registry().items()
                )
            )
    elif tc == 4:
        s.compose("restart", "controller")
        wait(lambda: s.alive() == 4)
    elif tc == 5:
        data = patterned(2 * M)
        oid = s.upload(data)
        w = sorted(holders(s.chunks(oid)[0]))[0]
        before = s.snapshot([w])
        s.compose("restart", w)
        wait(lambda: s.snapshot([w]) == before)
        s.verify(oid, data)
    else:
        listing = s.http.get(OBJ).raise_for_status().json()
        assert listing["items"] == [] and listing["total"] == 0


def objects(s, tc):
    sizes = {
        17: M // 2,
        18: M,
        19: M + 1,
        20: M - 1,
        21: 5 * M,
        22: 7 * M // 2,
        23: 0,
        24: 12 * M,
        25: 4 * M,
        27: 20 * M,
        28: 3 * M,
        29: 2 * M,
        62: 5 * M,
        69: 50 * M,
    }
    if tc in (28, 29):
        stopped = WORKERS[-(tc - 27) :]
        with s.outage(stopped):
            wait(lambda: s.alive() == 4 - len(stopped))
            oid = s.upload(patterned(sizes[tc]))
            rows = s.chunks(oid)
            assert replicated(rows) and all(
                not holders(c).intersection(stopped) for c in rows
            )
        return
    if tc == 8:
        return media_roundtrip(s)
    data = patterned(sizes.get(tc, 2 * M))
    if tc == 7:
        data = b"Plain text round trip\n"
        assert (
            s.verify(s.upload(data, name="readme.txt", mime="text/plain"), data)
            .headers["content-type"]
            .startswith("text/plain")
        )
        return
    oid = s.upload(data, custom={"author": "alice", "env": "test"})
    meta = s.meta(oid)
    rows = s.chunks(oid)
    assert meta["custom"] == {"author": "alice", "env": "test"}
    assert meta["system"]["size"] == len(data)
    assert meta["system"]["checksum"] == sha256(data).hexdigest()
    assert len(rows) == meta["system"]["num_chunks"] == math.ceil(len(data) / M)
    for i, c in enumerate(rows):
        part = data[i * M : (i + 1) * M]
        assert (
            c["index"] == i
            and c["size"] == len(part)
            and c["checksum"] == sha256(part).hexdigest()
        )
    assert len({c["chunk_id"] for c in rows}) == len(rows)
    if rows:
        assert replicated(rows)
    if tc == 10:
        assert {
            "created_at",
            "modified_at",
            "content_type",
            "name",
            "object_id",
        } <= meta["system"].keys()
        assert all(len(c["replicas"]) == 2 for c in meta["chunks"])
    if tc == 11:
        r = s.http.patch(
            f"{OBJ}/{oid}/metadata", json={"author": "bob", "reviewed": True}
        )
        r.raise_for_status()
        after = s.meta(oid)
        assert after["custom"] == {"author": "bob", "env": "test", "reviewed": True}
        assert after["system"]["modified_at"] > meta["system"]["modified_at"]
        assert after["chunks"] == meta["chunks"]
        assert all(
            after["system"][k] == v
            for k, v in meta["system"].items()
            if k != "modified_at"
        )
    if tc in (12, 13):
        for i in range(2 if tc == 12 else 30):
            s.upload(str(i).encode(), name=f"entry-{i}.txt")
        found = []
        for offset in range(0, len(s.ids), 10):
            items = (
                s.http.get(OBJ, params={"offset": offset, "limit": 10})
                .raise_for_status()
                .json()["items"]
            )
            assert all(
                {"object_id", "name", "size", "content_type"} <= item.keys()
                for item in items
            )
            found.extend(item["object_id"] for item in items)
        assert sorted(found) == sorted(s.ids)
        error(s.http.get(OBJ, params={"limit": 0}), 400)
    if tc == 15:
        second = s.upload(data)
        assert (
            second != oid and s.meta(second)["system"]["name"] == meta["system"]["name"]
        )
        s.verify(second, data)
    if tc == 26:
        for i in range(9):
            assert replicated(s.chunks(s.upload(patterned(2 * M, i + 1))))
    if tc in (17, 18, 24, 25, 27, 69):
        expected = {(w, c["chunk_id"]): c["checksum"] for c in rows for w in holders(c)}
        physical = {
            key: value
            for key, value in s.snapshot().items()
            if key[1].startswith(oid + "_")
        }
        assert expected == physical
        if tc in (24, 69):
            assert {w for w, _ in physical} == set(WORKERS)
    s.verify(oid, data)
    if tc in (14, 69):
        assert s.http.delete(f"{OBJ}/{oid}").status_code == 204
        error(s.http.get(f"{OBJ}/{oid}"), 404)
        error(s.http.get(f"{OBJ}/{oid}/metadata"), 404)
        assert oid not in [r["object_id"] for r in s.http.get(OBJ).json()["items"]]
        assert not any(cid.startswith(oid + "_") for _, cid in s.snapshot())


def media_roundtrip(s):
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(os.environ.get("STORE_MEDIA_DIR", tmp))
        if "STORE_MEDIA_DIR" not in os.environ:
            import imageio_ffmpeg

            ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
            for ext, source in [
                ("png", "color=size=32x32"),
                ("mp4", "testsrc=size=32x32"),
                ("wav", "sine=frequency=440"),
            ]:
                subprocess.run(
                    [
                        ffmpeg,
                        "-loglevel",
                        "error",
                        "-f",
                        "lavfi",
                        "-i",
                        source,
                        "-t",
                        "0.1",
                        *(["-frames:v", "1"] if ext == "png" else []),
                        str(folder / ("sample." + ext)),
                    ],
                    check=True,
                )
            with zipfile.ZipFile(folder / "sample.zip", "w") as archive:
                archive.writestr("hello.txt", "Object store verification")
            (folder / "sample.bin").write_bytes(os.urandom(4096))
        for ext, mime in [
            ("png", "image/png"),
            ("mp4", "video/mp4"),
            ("wav", "audio/wav"),
            ("zip", "application/zip"),
            ("bin", "application/octet-stream"),
        ]:
            path = next(folder.glob("*." + ext), None)
            assert path is not None, f"Missing .{ext} in {folder}"
            data = path.read_bytes()
            assert (
                s.verify(s.upload(data, name=path.name, mime=mime), data)
                .headers["content-type"]
                .startswith(mime)
            )


def validation(s, tc):
    if tc in (30, 31, 32):
        before = s.snapshot()
        if tc == 30:
            r = s.http.post(OBJ, files={"metadata": (None, "{}")})
        elif tc == 31:
            r = s.http.post(OBJ, files={"file": ("test", b"a")}, data={"metadata": "{"})
        else:
            r = s.http.post(OBJ, json={})
        error(r, 400)
        assert before == s.snapshot() and s.http.get(OBJ).json()["total"] == 0
        return
    if tc == 33:
        path = OBJ + "/" + uuid.uuid4().hex
        for method, url in [
            ("GET", path),
            ("GET", path + "/metadata"),
            ("PATCH", path + "/metadata"),
            ("DELETE", path),
        ]:
            error(s.http.request(method, url), 404)
        return
    if tc == 34:
        data = b"validation sentinel"
        existing = s.upload(data)
        before = s.meta(existing)
        # Exercise disallowed punctuation, embedded separators, a control byte,
        # and excessive length through the HTTP routing and validation layers.
        for oid in ("bad.id", "folder%2Fchild", "name%27quoted", "id%00null", "a" * 2000):
            path = f"{OBJ}/{oid}"
            for response in (s.http.get(path), s.http.get(path + "/metadata"),
                             s.http.patch(path + "/metadata", json={"probe": True}),
                             s.http.delete(path)):
                error(response, 404)
        assert s.meta(existing) == before
        s.verify(existing, data)
        return
    oid = s.upload(
        b"validation",
        name="résumé final (2026) — 日本語.pdf",
        custom={"title": '日本語 "quoted"'},
    )
    path = f"{OBJ}/{oid}"
    original = s.meta(oid)
    if tc == 35:
        for body in (b"{", b'["a"]', b""):
            error(s.http.patch(path + "/metadata", content=body), 400)
            assert s.meta(oid) == original
    elif tc == 36:
        s.http.patch(
            path + "/metadata",
            json={
                "object_id": "probe",
                "size": 999,
                "checksum": "probe",
                "chunks": "probe",
            },
        ).raise_for_status()
        after = s.meta(oid)
        assert all(
            after["system"][k] == v
            for k, v in original["system"].items()
            if k != "modified_at"
        )
        assert after["chunks"] == original["chunks"]
    elif tc == 37:
        assert original["system"]["name"] == "résumé final (2026) — 日本語.pdf"
        assert original["custom"] == {"title": '日本語 "quoted"'}
        assert s.http.get(OBJ).json()["items"][0]["name"] == original["system"]["name"]
    elif tc == 38:
        assert s.http.delete(path).status_code == 204
        error(s.http.delete(path), 404)
    elif tc == 39:
        before = s.snapshot()
        for n in (3, 4):
            with s.outage(WORKERS[:n]):
                wait(lambda: s.alive() == 4 - n)
                error(s.post(b"no quorum"), 503)
        assert s.snapshot() == before and s.http.get(OBJ).json()["total"] == 1
    elif tc == 40:
        with s.outage(sorted(holders(s.chunks(oid)[0]))):
            wait(lambda: s.alive() == 2)
            error(s.http.get(path), 503)
    else:
        error(s.http.get("/api/v1/_debug/boom"), 500)
        responses = parallel(8, lambda _: s.http.post(ADMIN + "/scrub"))
        assert any(r.status_code == 200 for r in responses)
        conflicts = [r for r in responses if r.status_code == 409]
        if not conflicts:
            pytest.skip("Scrub calls did not overlap")
        error(conflicts[0], 409)


def internal(s, tc):
    if tc == 46:
        s.upload(patterned(4 * M))
        start = time.monotonic()
        with s.outage(["worker2"]):
            wait(lambda: s.alive() == 3, 25)
            assert time.monotonic() - start < 25
            assert all(
                "worker2" not in holders(c) for c in s.chunks(s.upload(b"placement"))
            )
        return
    if tc == 45:
        before = s.snapshot()
        oid = s.upload(patterned(2 * M))
        assert {k for k in s.snapshot() if k not in before} == {
            (w, c["chunk_id"]) for c in s.chunks(oid) for w in holders(c)
        }
        s.http.delete(f"{OBJ}/{oid}").raise_for_status()
        assert s.snapshot() == before
        return
    url = s.urls["worker1"] + "/internal/chunks/" + uuid.uuid4().hex
    if tc == 43:
        error(s.http.get(url), 404)
        return
    data = patterned(M)
    try:
        for _ in range(2 if tc == 47 else 1):
            assert (
                s.http.put(
                    url,
                    content=data,
                    headers={"X-Chunk-Checksum": sha256(data).hexdigest()},
                ).status_code
                == 201
            )
        assert s.http.get(url).raise_for_status().content == data
        if tc == 44:
            assert s.http.delete(url).status_code == 204
            error(s.http.get(url), 404)
            assert s.http.delete(url).status_code == 204
    finally:
        s.http.delete(url).raise_for_status()


def recovery(s, tc):
    if tc == 55:
        return transfer_fault(s, churn=False)
    data = patterned((5 if tc in (48, 49, 50) else 4) * M)
    oid = s.upload(data)
    rows = s.chunks(oid)
    worker = next(
        (w for w in ["worker2", *WORKERS] if any(w in holders(c) for c in rows))
    )
    if tc == 70:
        second = s.upload(b"restart")
        s.compose("restart", "controller")
        wait(lambda: s.alive() == 4)
        for key, body in ((oid, data), (second, b"restart")):
            assert s.meta(key)["system"]["checksum"] == sha256(body).hexdigest()
            s.verify(key, body)
            assert replicated(s.chunks(key))
        return
    if tc == 56:
        with s.outage(WORKERS):
            wait(lambda: s.alive() == 0)
            error(s.post(data), 503)
            error(s.http.get(f"{OBJ}/{oid}"), 503)
            assert s.meta(oid)["system"]["object_id"] == oid
            assert s.http.get(OBJ).json()["total"] == 1
        s.verify(oid, data)
        return
    if tc == 53:
        lost = sorted(holders(rows[0]))
        for i in range(30):
            survivor = s.upload(bytes([i]))
            if holders(s.chunks(survivor)[0]) - set(lost):
                break
        else:
            pytest.fail("Could not prepare survivor placement")
        with s.outage(lost):
            wait(lambda: s.alive() == 2)
            error(s.http.get(f"{OBJ}/{oid}"), 503)
            s.verify(survivor, bytes([i]))
            wait(lambda: replicated(s.chunks(survivor)))
        return
    with s.outage([worker]):
        if tc == 48:
            s.verify(oid, data)
        wait(lambda: s.alive() == 3)
        if tc == 51:
            for i in range(3):
                body = patterned(2 * M, i)
                key = s.upload(body)
                s.verify(key, body)
                assert replicated(s.chunks(key))
        elif tc == 52:
            assert s.http.delete(f"{OBJ}/{oid}").status_code == 204
            error(s.http.get(f"{OBJ}/{oid}"), 404)
        else:
            s.verify(oid, data)
            if tc in (49, 50, 54):
                if tc == 49:
                    assert any(c["effective_replicas"] < 2 for c in s.chunks(oid))
                wait(lambda: replicated(s.chunks(oid)))
                if tc == 54:
                    second = next(w for w in WORKERS if w != worker)
                    with s.outage([second]):
                        wait(lambda: s.alive() == 2)
                        s.verify(oid, data)
    if tc == 52:
        wait(lambda: not any(cid.startswith(oid + "_") for _, cid in s.snapshot()))
    elif tc == 50:
        wait(
            lambda: replicated(s.chunks(oid))
            and all(len(c["replicas"]) == 2 for c in s.chunks(oid))
        )
        s.verify(oid, data)


def integrity(s, tc):
    data = patterned(11 * M // 2 if tc == 63 else 3 * M)
    oid = s.upload(data)
    initial = s.chunks(oid)
    for index in ([3, 5] if tc == 63 else [0]):
        c = initial[index]
        targets = sorted(holders(c))
        for w in (targets if tc == 61 else targets[:1]):
            evidence = s.damage(w, c["chunk_id"], {59: "truncate", 60: "delete"}.get(tc, "flip"))
            print(f"DRILL TC-{tc} | {w}: {evidence}")
    if tc == 61:
        response = s.http.get(f"{OBJ}/{oid}")
        assert response.status_code in (500, 503)
        error(response, response.status_code)
        print(f"DRILL TC-{tc} | Download rejected: HTTP {response.status_code}, {response.json()['message']}")
        return
    download = s.verify(oid, data)
    print(f"DRILL TC-{tc} | Download HTTP {download.status_code}: all {len(data):,} bytes matched the original.")
    s.scrub()

    def repaired():
        if not replicated(s.chunks(oid)):
            return False
        physical = s.snapshot()
        return all(
            physical.get((w, c["chunk_id"])) == c["checksum"]
            for c in s.chunks(oid)
            for w in holders(c)
        )

    wait(repaired)
    print(f"DRILL TC-{tc} | Repair verified: two effective replicas per chunk, with matching SHA-256 values in worker inventories.")


def concurrency(s, tc):
    if tc == 64:
        bodies = [patterned(2 * M, i) for i in range(8)]
        ids = parallel(8, lambda i: s.upload(bodies[i], name=f"client-{i}.bin"))
        assert len(set(ids)) == 8
        for oid, data in zip(ids, bodies):
            s.verify(oid, data)
            assert replicated(s.chunks(oid))
        return
    if tc == 68:
        return transfer_fault(s, churn=True)
    data = patterned((15 if tc == 67 else 10) * M)
    oid = s.upload(data)
    path = f"{OBJ}/{oid}"
    if tc == 65:
        parallel(10, lambda _: s.verify(oid, data))
    elif tc == 66:
        parallel(
            5,
            lambda i: s.http.patch(
                path + "/metadata", json={f"k{i}": i}
            ).raise_for_status(),
        )
        assert s.meta(oid)["custom"] == {f"k{i}": i for i in range(5)}
    else:

        def race(i):
            if i == 1:
                assert s.http.delete(path).status_code == 204
                return
            r = s.http.get(path)
            if r.status_code == 200:
                assert r.content == data
            else:
                assert r.status_code in (404, 503)

        parallel(2, race)
        error(s.http.get(path), 404)

        # The simultaneous start can let DELETE win. Also exercise an admitted
        # reader with a body larger than the socket buffers and a pending DELETE.
        streaming_path = f"{OBJ}/{s.upload(data)}"
        with ThreadPoolExecutor(1) as pool:
            with s.http.stream("GET", streaming_path) as response:
                assert response.status_code == 200
                chunks = response.iter_bytes(chunk_size=M)
                first = next(chunks)
                deletion = pool.submit(s.http.delete, streaming_path)
                assert first + b"".join(chunks) == data
            assert deletion.result(timeout=30).status_code == 204
        error(s.http.get(streaming_path), 404)


def transfer_fault(s, churn):
    """Observe actual chunk writes before disrupting an in-flight upload."""
    worker = "worker4" if churn else "worker2"
    bodies = [patterned(20 * M, i) for i in range(10 if churn else 1)]
    before = s.snapshot()

    def submit(i):
        r = s.post(bodies[i], name=f"transfer-{i}.bin")
        if r.status_code == 503:
            error(r, 503)
            return None
        assert r.status_code == 201, r.text
        oid = r.json()["object_id"]
        s.ids.append(oid)
        return oid

    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = [pool.submit(submit, i) for i in range(len(bodies))]

        def writing():
            return all(job.done() for job in jobs) or bool(s.inventory(worker))

        wait(writing)
        overlap = any(job.running() for job in jobs)
        try:
            s.compose("stop" if churn else "kill", worker)
        finally:
            s.compose("start", worker)
        results = [job.result() for job in jobs]
    wait(lambda: s.alive() == 4)
    for i, oid in enumerate(results):
        if oid:
            s.verify(oid, bodies[i])
            wait(lambda: replicated(s.chunks(oid)))
    allowed = {c["chunk_id"] for oid in results if oid for c in s.chunks(oid)}
    wait(lambda: all(cid in allowed for _, cid in s.snapshot().keys() - before.keys()))
    if not overlap:
        pytest.skip("Uploads finished before the fault could be triggered")
    if not churn:
        oid = s.upload(bodies[0])
        assert any(worker in holders(c) for c in s.chunks(oid))
        with s.http.stream("GET", f"{OBJ}/{oid}") as r:
            assert r.status_code == 200
            stream = r.iter_bytes(chunk_size=M)
            head = next(stream)
            try:
                s.compose("kill", worker)
                assert head + b"".join(stream) == bodies[0]
            finally:
                s.compose("start", worker)
        wait(lambda: s.alive() == 4)
