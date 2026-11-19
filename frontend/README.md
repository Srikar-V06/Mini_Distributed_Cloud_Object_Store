# Object Store Console

From the repository root, start the existing Docker Compose stack and the frontend. The frontend uses Node.js 20 or later and has no package dependencies.

```sh
docker compose up -d --build
node frontend/dev.mjs
```

Open [http://127.0.0.1:5173](http://127.0.0.1:5173). The frontend server binds to loopback and streams requests to the controller at `http://127.0.0.1:8000`. Set `FRONTEND_PORT` to change the listener or `CONTROLLER_ORIGIN` to connect to a different controller. The frontend uses same-origin requests, so the controller needs no CORS configuration.

## Test the store

For the numbered TC-01–TC-70 checks, isolated Docker setup, browser regressions, and short manual sign-off, see the [verification guide](../tests/README.md).

| Scenario | Where to test | Expected result |
| --- | --- | --- |
| Object lifecycle | Upload a file, find it by ID, download and verify it, then delete the test object. | HTTP 201 on upload, matched byte count and SHA-256 on download, HTTP 204 on delete, HTTP 404 on a later lookup. |
| Chunking and placement | Prepare each sample in **Test bench**, then open its chunk placement. | The 0-byte, 1 MiB, 1 MiB + 1 byte, and 5.5 MiB samples show 0, 1, 2, and 6 chunks respectively. A healthy chunk has two effective replicas on distinct workers. |
| Metadata | Upload with JSON such as `{"course":"cloud","obsolete":true}`. Apply `{"course":"storage","obsolete":null,"reviewed":true}`. | Custom values are added, changed, and removed. The object checksum, creation time, and chunk map stay the same. |
| Worker outage and rejoin | Use the worker failure drill in **Test bench** while watching **Cluster**. Run `python scripts/demo_worker_failure.py` for the scripted demonstration. | The heartbeat expires, affected chunks can become under-replicated, and downloads work when every chunk still has a usable copy. Repair or rejoin restores the target when capacity permits. |
| Integrity | Run **Integrity scrub** and **Compare two files** in **Test bench**. Use `python scripts/demo_integrity.py` for fault injection on disposable data. | The scrub returns per-worker counts. A verified download or matching file comparison returns the expected SHA-256. The fault injection demo checks detection and repair. |
| API validation | Run each case under **API error checks**. | Each result shows the expected and actual status, JSON error, and request ID when present. |

The existing Python demos use `python -m pip install -r client/requirements.txt`. Run them against a disposable test cluster or when a brief worker interruption is acceptable. Docker control and deliberate disk corruption are performed by the existing scripts or Docker commands. The browser uses the controller APIs to observe those operations.

Browser SHA-256 verification reads the downloaded file into memory. For very large files or memory-constrained browsers, use the existing CLI download command for streaming verification.

## Deploy the frontend to Vercel

1. Import the repository into Vercel. Set the project root to `frontend` and select the **Other** framework preset. The app serves the `public` directory and has no build step.
2. Run the Docker cluster on a host reachable through an HTTPS gateway. Give the gateway access controls for testers and restrict access to the controller itself. The current controller has unauthenticated object and admin operations. Keep the Vercel preview protected as well.
3. Set `CONTROLLER_ORIGIN` in the Vercel project to the gateway origin, for example `https://storage-gateway.example.com`, and deploy. The frontend configuration uses external rewrites for the supported routes. The gateway must pass through HTTP methods, query strings, multipart uploads, error bodies, and download headers including `Content-Length` and `X-Object-Checksum`.
4. Confirm **Controller online** and run a sample upload and verified download on the preview. With no gateway configured, the deployed static console can still compare two local files.

Vercel hosts the frontend. Docker runs the controller, workers, and their persistent volumes separately. A controller listening only on your computer's localhost is reachable through the local frontend server. Use that setup for large files or long-running tests that could exceed [Vercel’s external proxy timeout](https://vercel.com/docs/limits#proxied-request-timeout).
