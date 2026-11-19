import argparse
import sys

import httpx


def main() -> int:
    ap = argparse.ArgumentParser(description="Provoke every failure path; expect JSON errors only.")
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--with-debug", action="store_true", help="also hit the DEBUG_ROUTES endpoints")
    args = ap.parse_args()

    client = httpx.Client(base_url=args.base.rstrip("/"), timeout=30)
    fails = 0

    def report(ok: bool, line: str):
        nonlocal fails
        print(("PASS  " if ok else "FAIL  ") + line)
        if not ok:
            fails += 1

    def check(name: str, expected: int, method: str, path: str, **kw):
        """Passes only if: right status, JSON body with error+message, and an X-Request-ID header."""
        r = client.request(method, path, **kw)
        try:
            body = r.json()
        except ValueError:
            body = None
        ok = (r.status_code == expected
              and isinstance(body, dict) and bool(body.get("error")) and bool(body.get("message"))
              and "x-request-id" in r.headers)
        if ok:
            report(True, f"{name} -> {r.status_code} ({body['error']})")
        else:
            report(False, f"{name} -> {r.status_code} (expected {expected}) body: {r.text[:200]!r}")

    small = ("small.txt", b"hello", "text/plain")
    JSON_CT = {"Content-Type": "application/json"}

    # setup: one real object to run the PATCH / DELETE cases against
    up = client.post("/api/v1/objects", files={"file": small})
    if up.status_code != 201:
        print(f"FAIL  setup upload -> {up.status_code}: {up.text[:200]}")
        return 1
    oid = up.json()["object_id"]

    # routing
    check("unknown route",          404, "GET", "/api/v1/nothing")
    check("wrong method (PUT)",     405, "PUT", "/api/v1/objects")

    # upload validation (files= with a (None, value) tuple forces a multipart body)
    check("upload: missing file",   400, "POST", "/api/v1/objects", files={"metadata": (None, "x")})
    check("upload: metadata junk",  400, "POST", "/api/v1/objects",
          files={"file": small, "metadata": (None, "not json")})
    check("upload: metadata array", 400, "POST", "/api/v1/objects",
          files={"file": small, "metadata": (None, "[1,2]")})

    # unknown / malformed ids
    check("GET unknown id",         404, "GET", "/api/v1/objects/nope")
    check("GET traversal id",       404, "GET", "/api/v1/objects/..%2F..%2Fetc")
    check("metadata unknown id",    404, "GET", "/api/v1/objects/nope/metadata")

    # PATCH validation
    patch = f"/api/v1/objects/{oid}/metadata"
    check("PATCH array body",       400, "PATCH", patch, content=b"[1,2]", headers=JSON_CT)
    check("PATCH nested value",     400, "PATCH", patch, content=b'{"a":{"b":1}}', headers=JSON_CT)
    check("PATCH not json",         400, "PATCH", patch, content=b"not json", headers=JSON_CT)
    check("PATCH unknown id",       404, "PATCH", "/api/v1/objects/nope/metadata",
          content=b"[1,2]", headers=JSON_CT)

    # list parameters
    for q in ("limit=abc", "limit=-1", "limit=0", "limit=1001", "offset=-5", "limit=1.5"):
        check(f"list: {q}",         400, "GET", f"/api/v1/objects?{q}")

    # delete semantics
    check("DELETE unknown id",      404, "DELETE", "/api/v1/objects/nope")
    r = client.delete(f"/api/v1/objects/{oid}")
    report(r.status_code == 204 and r.content == b"", f"DELETE existing -> {r.status_code}")
    check("DELETE twice",           404, "DELETE", f"/api/v1/objects/{oid}")
    check("GET after delete",       404, "GET", f"/api/v1/objects/{oid}")

    if args.with_debug:
        check("validation 422 -> 400", 400, "GET", "/api/v1/_debug/int/abc")
        r = client.get("/api/v1/_debug/boom")
        leaked = any(w in r.text for w in ("Traceback", "RuntimeError", ".py"))
        ok = (r.status_code == 500 and r.json().get("error") == "internal_error"
              and "request_id" in r.json() and not leaked)
        report(ok, f"unhandled -> {r.status_code} (no stack trace leaked: {not leaked})")

    print("ALL PASS" if fails == 0 else f"{fails} FAILED")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())