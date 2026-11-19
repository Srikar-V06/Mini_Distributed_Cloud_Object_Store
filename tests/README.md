# Verify the object store

Run the Python suite for the 70 numbered specification cases. Run the browser suite for the console interactions. A UI redesign generally needs the browser suite first. Run both before the final demo.

For an interface-only walkthrough, use the [seven manual checks and expected screen outputs](manual-console.md).

## Run the service suite

### From the local frontend

1. Start Docker Desktop. From the repository root, create the Python environment and install the requirements using the first two commands below.
2. In that PowerShell window, set `$env:STORE_TEST_PYTHON = "$env:TEMP\store-test-env\Scripts\python.exe"` and start `node frontend/dev.mjs`. Restart the frontend process if it was already running.
3. Open [Test bench](http://127.0.0.1:5173/#testing). Under **Storage verification suite**, select a case from **TC-01** through **TC-70**, or **All 70 cases**, and click **Run tests**.
4. Read the live pytest output. **Passed** means every selected case passed. **Incomplete** means pytest finished successfully but the pass count fell short, for example due to a skipped case. **Failed** includes assertion failures and environment errors. Inspect the final log lines for the cause.

You can reload the page and reconnect while the Node process is running. Each launch tests a fresh temporary cluster built from this checkout, including when the console is connected to a different controller. Run a single case to demonstrate a specific requirement, or run all 70 together to share cluster setup. The static Vercel deployment has no local Docker runner.

### From PowerShell

Open Docker Desktop, then run these PowerShell commands from the repository root.

```powershell
python -m venv "$env:TEMP\store-test-env"
& "$env:TEMP\store-test-env\Scripts\python.exe" -m pip install -r tests/requirements.txt
& "$env:TEMP\store-test-env\Scripts\python.exe" -m pytest tests/test_store.py -v -rs --tb=short --junitxml=tests/results.xml
```

Each line identifies a PDF case, for example `test_case[TC-19]`. **PASSED** means its automated assertions succeeded. **FAILED** includes the assertion and actual result. **ERROR** means setup or cleanup failed. **SKIPPED** gives the outstanding evidence. The XML file is suitable for a CI test report. To repeat one case, substitute `"tests/test_store.py::test_case[TC-19]"` for the filename.

The session builds a uniquely named Compose project from the repository’s base Compose definition, gives it its own loopback ports, network and named volumes, and removes those temporary resources afterward. Keep Docker running until pytest exits. Several cases wait for genuine heartbeat timeouts, repairs and worker rejoins. Run this suite serially.

| PDF cases | What the Python suite exercises |
| --- | --- |
| 01–06 | Container topology and mounts, worker registration state, heartbeat advancement, controller restart, worker inventory persistence and an empty listing. |
| 07–16 | Text and generated media round trips, metadata, pagination, physical cleanup, duplicate names and controller-only downloads. TC-08 can use your own files if you set `STORE_MEDIA_DIR` to a directory containing PNG, MP4, WAV, ZIP and BIN samples. |
| 17–29 | Exact chunk sizes and ordering, patterned-byte reconstruction, physical inventory hashes, distinct placement, a large replica inventory, and three- and two-worker writes. |
| 30–41 | Invalid requests, missing objects, metadata isolation, Unicode, deletion, insufficient quorum, unavailable reads and structured errors. TC-34 exercises malformed, encoded and oversized identifiers on all four object routes and verifies a stored object's integrity afterward. TC-41 reports skipped if concurrent scrubs miss the conflict window. |
| 42–47 | Worker chunk PUT, GET, DELETE, inventory, heartbeat timeout and duplicate PUT. |
| 48–56 | Read failover, healing, rejoin, writes/deletes during outages, concurrent and cascading failures, mid-transfer disruption and total outage. |
| 57–63 | Single and double corruption, truncation, lost replicas, physical repair hashes, object hash accuracy, and middle/final chunk integrity. |
| 64–70 | Concurrent uploads/downloads/patches, download-delete races, uploads during worker churn, 50 MiB stress and controller metadata persistence. TC-55 and TC-68 require an active upload when the fault is triggered. |

## Run the browser suite

Start your usual Compose cluster and frontend. If the local console is already online with four registered workers, proceed directly to the second block.

```powershell
docker compose up -d --build
node frontend/dev.mjs
```

In another terminal at the repository root, run:

```powershell
npm.cmd install --prefix tests
npm.cmd test --prefix tests
```

The test uses installed Microsoft Edge by default. For Playwright Chromium, run `npx.cmd --prefix tests playwright install chromium`, set `$env:QA_BROWSER = 'chromium'`, and rerun. The browser visits `http://127.0.0.1:5173`, creates one uniquely named test object, and removes it after the checks.

| Browser workflow | What it catches that an API-only script misses |
| --- | --- |
| Upload a 1 MiB + 1 byte file and inspect it (TC-07, 09, 10, 19, 25) | Broken form submission, an incorrect dialog, missing chunk rows or a misplaced final byte. |
| Submit an invalid patch, then a valid one, close, and find by ID (TC-11, 35) | Hidden errors, broken buttons or edits that disappear when the object is reopened. |
| Trigger a download, compare saved bytes, then simulate a damaged download response (TC-16, 62) | A successful-looking UI with a bad file, or failure to show a checksum mismatch. The damaged response exists only in the browser test. |
| Run an error probe and the file comparator (TC-33, 62) | Broken Test bench controls or missing result displays. |
| Navigate Cluster, filter, look up the object and confirm delete (TC-02, 14) | Navigation, filter, lookup or confirmation controls that fail to complete the workflow. |
| Walk all pages at 1440, 1024, 768 and 390 pixels, then simulate loss of controller health | Horizontal page overflow, inaccessible navigation or a stale online indicator. |

## Five-minute manual sign-off

1. At a phone-sized width, use the keyboard to open **Upload object**, choose a local file and close the dialog with Escape. Check focus visibility and text legibility.
2. Drag a file onto the upload area and drop it. Upload, click **Download & verify**, then open the downloaded file in its normal application. Check that long names and Unicode characters remain readable.
3. For a live recovery demonstration, use a disposable cluster. Upload a multi-chunk file, inspect which worker holds a replica, stop that worker, and watch **Cluster**. Download while it is down, wait for two healthy replicas, then restart the worker. The automated suite checks the storage behavior; this walkthrough checks whether a person can follow it on screen.
