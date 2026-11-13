import hashlib, os, subprocess, sys
import httpx

BASE = os.getenv("WORKER_URL", "http://localhost:9001")
CONTAINER = os.getenv("WORKER_CONTAINER", "worker1")
WORKER_ID = CONTAINER
c = httpx.Client(base_url=BASE, timeout=10)

FLIP = "import sys;p='/data/chunks/'+sys.argv[1];b=bytearray(open(p,'rb').read());b[0]^=1;open(p,'wb').write(b)"
TRUNC = "import sys;open('/data/chunks/'+sys.argv[1],'r+b').truncate(100)"
NOSIDE = "import os,sys;os.remove('/data/chunks/'+sys.argv[1]+'.sha256')"


def sha(b): return hashlib.sha256(b).hexdigest()
def put(cid, data, checksum=None):
    return c.put(f"/internal/chunks/{cid}", content=data, headers={"X-Chunk-Checksum": checksum or sha(data)})
def dexec(code, arg):
    subprocess.run(["docker", "exec", CONTAINER, "python", "-c", code, arg], check=True)
def check(name, ok):
    print(("PASS  " if ok else "FAIL  ") + name)
    if not ok: sys.exit(1)


data = os.urandom(1024 * 1024)
cid = "testobj_000000"

r = put(cid, data)
check("PUT 1 MB -> 201 with checksum", r.status_code == 201 and r.json()["checksum"] == sha(data))
r = c.get(f"/internal/chunks/{cid}")
check("GET returns identical bytes + header", r.content == data and r.headers["x-chunk-checksum"] == sha(data))
check("re-PUT is idempotent", put(cid, data).status_code == 201)

check("checksum mismatch -> 422", put("bad_000000", b"hello", sha(b"nope")).status_code == 422)
check("mismatch stored nothing", c.get("/internal/chunks/bad_000000").status_code == 404)
check("invalid id -> 400", c.get("/internal/chunks/bad.id").status_code == 400)
check("chunk > 1 MB -> 400", put("big_000000", os.urandom(1024 * 1024 + 1)).status_code == 400)
check("missing chunk -> 404", c.get("/internal/chunks/nothere_000000").status_code == 404)

lst = c.get(f"/internal/workers/{WORKER_ID}/chunks").json()["chunks"]
check("list shows chunk with right checksum",
      any(x["chunk_id"] == cid and x["checksum"] == sha(data) for x in lst))
check("list for wrong worker id -> 404", c.get("/internal/workers/worker9/chunks").status_code == 404)

for mode, code in {"bit flip": FLIP, "truncate": TRUNC, "missing sidecar": NOSIDE}.items():
    put(cid, data)                      # restore a good copy
    dexec(code, cid)
    r = c.get(f"/internal/chunks/{cid}")
    check(f"{mode} -> 409 corrupt, no bytes", r.status_code == 409 and r.json()["error"] == "corrupt")
    bad = [b["chunk_id"] for b in c.post("/internal/scrub").json()["bad"]]
    check(f"{mode} found by scrub", cid in bad)

put(cid, data)
check("re-PUT heals corruption", c.get(f"/internal/chunks/{cid}").content == data)
check("DELETE -> 204", c.delete(f"/internal/chunks/{cid}").status_code == 204)
check("DELETE again -> 204", c.delete(f"/internal/chunks/{cid}").status_code == 204)
check("GET after delete -> 404", c.get(f"/internal/chunks/{cid}").status_code == 404)
print("\nall worker tests passed")