const form = document.querySelector('#suite-form');
const select = document.querySelector('#suite-case');
const start = document.querySelector('#suite-start');
const status = document.querySelector('#suite-state');
const log = document.querySelector('#suite-log');
const damageStart = document.querySelector('#damage-start');
const damageCase = document.querySelector('#damage-case');
const damageOutput = document.querySelector('#damage-output');
damageStart.addEventListener('click', () => {
  select.value = damageCase.value;
  showSelection();
  form.requestSubmit();
});

// Entries follow the parameter order in tests/test_store.py::test_case.
const catalog = [
  ['Cluster topology', 'Start the Compose project and inspect worker mounts.', 'One controller and four workers run. Every worker has a /data mount.'],
  ['Worker registration', 'Read the controller registry.', 'Four distinct workers report alive with addresses.'],
  ['Heartbeat updates', 'Compare successive registry snapshots.', 'Every worker advances its heartbeat timestamp.'],
  ['Controller restart', 'Restart the controller and wait for registration.', 'All four workers become visible as alive again.'],
  ['Worker persistence', 'Restart a worker holding an uploaded object.', 'Its inventory survives and the downloaded bytes match.'],
  ['Empty listing', 'List objects on an empty cluster.', 'The items array is empty and total is zero.'],
  ['Plain text round trip', 'Upload and download a text file.', 'HTTP 200, text/plain, original bytes and matching SHA-256.'],
  ['Media round trips', 'Upload PNG, MP4, WAV, ZIP and binary samples.', 'Every download preserves its content type, bytes and checksum.'],
  ['Custom metadata', 'Upload an object with author and environment fields.', 'Custom values, size, checksum and chunk records match the upload.'],
  ['System metadata', 'Inspect identity, timestamps, content type and chunk records.', 'Required fields exist and each chunk lists two replicas.'],
  ['Metadata merge', 'Change the author and add a reviewed flag.', 'The prior environment remains, modification time advances and placement is unchanged.'],
  ['Object listing', 'List three stored objects and try a zero page limit.', 'All three IDs and summary fields appear. Limit zero returns HTTP 400.'],
  ['Pagination', 'Read 31 objects in pages of ten.', 'Every ID appears exactly once. Limit zero returns HTTP 400.'],
  ['Object deletion', 'Delete an object and inspect its listing, metadata and chunks.', 'HTTP 204, then 404 on lookup. No replica remains in worker inventory.'],
  ['Duplicate names', 'Upload two objects with the same filename.', 'Their IDs differ and the second download matches the original data.'],
  ['Controller download', 'Read a multi-chunk object through the controller.', 'HTTP 200 with reconstructed bytes and the expected checksum header.'],
  ['Half-chunk file', 'Upload 512 KiB and inspect physical placement.', 'One 512 KiB chunk has two matching copies.'],
  ['Exact chunk boundary', 'Upload exactly 1 MiB and inspect physical placement.', 'One full chunk has two matching copies.'],
  ['One byte above boundary', 'Upload 1 MiB plus one byte.', 'Two chunks measure 1 MiB and 1 byte. The download matches.'],
  ['One byte below boundary', 'Upload 1 MiB minus one byte.', 'One chunk contains all 1,048,575 bytes.'],
  ['Multiple full chunks', 'Upload exactly 5 MiB.', 'Five ordered 1 MiB chunks reconstruct the original file.'],
  ['Partial final chunk', 'Upload 3.5 MiB.', 'Three full chunks and one 512 KiB chunk reconstruct the file.'],
  ['Empty file', 'Upload and download a zero-byte object.', 'Zero chunks, an empty download and the SHA-256 of empty content.'],
  ['Distribution across workers', 'Inspect physical replicas for a 12 MiB upload.', 'All four workers participate and physical hashes match placement.'],
  ['Replica inventory', 'Compare a 4 MiB object’s placement with worker inventories.', 'Four chunks have the expected physical copies and hashes.'],
  ['Repeated placement', 'Inspect replication across ten two-chunk uploads.', 'Every chunk has two effective replicas on distinct workers.'],
  ['Large replica inventory', 'Upload 20 MiB and compare advertised and physical copies.', 'Twenty chunks have matching inventories and the reconstructed file is intact.'],
  ['Three-worker writes', 'Take one worker offline before uploading.', 'All chunks have two replicas on the three available workers.'],
  ['Two-worker writes', 'Take two workers offline before uploading.', 'Both remaining workers hold a replica of every chunk.'],
  ['Missing upload file', 'Submit multipart metadata without a file.', 'HTTP 400 with a structured error. No object or chunk is created.'],
  ['Invalid upload metadata', 'Upload one byte with malformed metadata JSON.', 'HTTP 400 with a structured error. Object and chunk inventories stay empty.'],
  ['Invalid upload format', 'Send a JSON body to the multipart upload endpoint.', 'HTTP 400 with a structured error and no stored data.'],
  ['Unknown object', 'Read, inspect, patch and delete an unknown ID.', 'All four operations return structured HTTP 404 errors.'],
  ['Invalid object IDs', 'Exercise invalid and oversized IDs across the object routes.', 'Requests return HTTP 404. An existing object retains its metadata and bytes.'],
  ['Invalid metadata patch', 'Submit malformed, array and empty patch bodies.', 'Each returns HTTP 400 and preserves the original metadata.'],
  ['System field isolation', 'Include system field names in a custom patch.', 'System identity, size, checksum and chunk records retain their original values.'],
  ['Unicode and quotes', 'Store an international filename and quoted metadata.', 'Metadata and listing preserve the original text.'],
  ['Repeat object deletion', 'Delete the same object twice.', 'The first response is HTTP 204 and the second is HTTP 404.'],
  ['Insufficient write replicas', 'Attempt uploads with one worker and with zero workers.', 'Both return HTTP 503 and leave existing state intact.'],
  ['No readable replica', 'Stop both holders of one chunk before download.', 'The controller returns a structured HTTP 503.'],
  ['Internal error and scrub conflict', 'Check an internal error and issue overlapping scrubs.', 'Structured HTTP 500, a successful scrub and HTTP 409 for overlap. Missed overlap is skipped.'],
  ['Worker chunk round trip', 'Write a checksummed chunk and read it from a worker.', 'HTTP 201 on write and identical bytes on read.'],
  ['Unknown worker chunk', 'Request a nonexistent chunk from a worker.', 'Structured HTTP 404.'],
  ['Worker chunk deletion', 'Write a chunk, delete it, read it and delete again.', 'Delete returns HTTP 204 both times. The intervening read returns 404.'],
  ['Inventory reconciliation', 'Compare worker inventories before upload and after delete.', 'Added copies match placement exactly. Deletion restores the baseline.'],
  ['Heartbeat timeout', 'Stop a worker and inspect the next upload’s placement.', 'The controller detects the loss within 25 seconds and avoids the dead worker.'],
  ['Duplicate chunk write', 'Write identical bytes to the same worker chunk twice.', 'Both writes return HTTP 201 and the chunk reads back correctly.'],
  ['Read failover', 'Read immediately after stopping a replica holder and again after detection.', 'Both downloads reconstruct the original bytes.'],
  ['Replica healing', 'Observe reduced replication after a worker stops.', 'An under-replicated chunk is observed, then all chunks regain two effective replicas.'],
  ['Worker rejoin', 'Allow repair, then bring the stopped worker back.', 'Each chunk settles at two listed and effective replicas and the object reads correctly.'],
  ['Writes during outage', 'Upload three objects while one worker is down.', 'All three download intact with two effective replicas per chunk.'],
  ['Deletion during outage', 'Delete an object with one replica holder offline, then rejoin it.', 'HTTP 204, then 404. Eventually all workers have removed its chunks.'],
  ['Two simultaneous failures', 'Stop both holders of one chunk and read a separately placed object.', 'The affected download returns 503. The surviving object reads and regains replication.'],
  ['Cascading failure', 'Wait for repair after one loss, then stop a second worker.', 'The download still matches with two workers alive.'],
  ['Failure during transfer', 'Disrupt an active upload and later an active streamed download.', 'Uploads succeed intact or return 503 with no orphan chunks. The streamed read completes intact.'],
  ['Total worker outage', 'Stop every worker and exercise data and metadata operations.', 'Reads and uploads return 503. Listing and metadata work, and the download succeeds after recovery.'],
  ['Single damaged replica', 'Alter bytes in one replica and download, scrub and inspect copies.', 'Healthy bytes are served and all repaired physical hashes match.'],
  ['Repair verification', 'Exercise the same single-replica corruption path as TC-57 and verify repair.', 'Two effective copies return with the recorded SHA-256 values.'],
  ['Truncated replica', 'Truncate one stored chunk copy.', 'The object reads intact and scrubbing restores valid replicas.'],
  ['Missing replica file', 'Remove a stored copy of one chunk.', 'The object reads intact and both physical copies are restored.'],
  ['Both replicas damaged', 'Alter both copies of the first chunk before download.', 'A structured HTTP 500 or 503 is returned.'],
  ['Object checksum', 'Upload 5 MiB and compare metadata and download hashes to the source.', 'Metadata SHA-256, download checksum and original bytes agree.'],
  ['Middle and final chunk damage', 'Damage one copy each of chunks 3 and 5 in a 5.5 MiB object.', 'The download matches and both chunks recover valid physical replicas.'],
  ['Concurrent uploads', 'Start eight uploads with distinct payloads together.', 'Eight unique IDs, correct downloads and replicated chunks.'],
  ['Concurrent downloads', 'Start ten readers on a single 10 MiB object.', 'Every reader receives the complete original bytes and checksum.'],
  ['Concurrent metadata patches', 'Send five patches that update different keys.', 'All five key/value pairs survive.'],
  ['Download versus delete', 'Race a read with deletion, then delete during an admitted stream.', 'An admitted read completes byte-for-byte. Deletion succeeds and later lookup returns 404.'],
  ['Uploads during worker churn', 'Cycle a worker while clients upload multiple objects.', 'Accepted objects verify and regain replication. Failed writes leave no orphan chunks.'],
  ['50 MiB lifecycle', 'Upload, verify, inspect replicas and delete a 50 MiB object.', 'All four workers hold matching copies. Download matches, then deletion clears all copies.'],
  ['Restart persistence', 'Restart the controller after storing two objects.', 'Four workers re-register. Both objects retain their checksums, content and replication.'],
];
const caseRows = new Map();
for (const [index, [name, check, expected]] of catalog.entries()) {
  const id = 'TC-' + String(index + 1).padStart(2, '0');
  select.add(new Option(id + ' · ' + name, id));
  const row = document.createElement('tr');
  for (const value of [id + ' · ' + name, check, expected, 'Not run']) {
    row.insertCell().textContent = value;
  }
  document.querySelector('#case-rows').append(row);
  caseRows.set(id, row);
}

function showSelection() {
  document.querySelector('#suite-node').textContent = select.value === 'all' ? 'tests/test_store.py' :
    'tests/test_store.py::test_case[' + select.value + ']';
  for (const [id, row] of caseRows) row.hidden = select.value !== 'all' && select.value !== id;
  if (select.value !== 'all') document.querySelector('#case-catalog').open = true;
}
select.addEventListener('change', showSelection);

function render(run) {
  const active = run.state === 'Running';
  start.disabled = select.disabled = active;
  damageStart.disabled = damageCase.disabled = active;
  const drill = damageCase.querySelector('option[value="' + run.selection + '"]');
  if (active && drill) damageCase.value = run.selection;
  const evidence = [...run.log.matchAll(/^DRILL (TC-\d+) \| (.+)$/gm)].map(m => m[1] + ' · ' + m[2]);
  damageOutput.hidden = !evidence.length && !drill;
  damageOutput.textContent = evidence.length ? evidence.join('\n\n') : active ? 'Running the drill. Measured checksums appear when the test finishes.' : 'No measurements available. See Full runner output.';
  if (active) select.value = run.selection;
  showSelection();
  const outcomes = new Map();
  for (const match of run.log.matchAll(/test_case\[(TC-\d+)\]\s*(PASSED|FAILED|ERROR|SKIPPED)?/g)) {
    if (match[2] || !outcomes.has(match[1])) outcomes.set(match[1], match[2] || (active ? 'Running' : 'No verdict'));
  }
  for (const [id, row] of caseRows) {
    const cell = row.cells[3];
    cell.textContent = outcomes.get(id) || (active && (run.selection === 'all' || run.selection === id) ? 'Queued' : 'Not run');
    cell.dataset.result = cell.textContent;
  }
  const totals = {};
  for (const value of outcomes.values()) {
    if (['PASSED', 'FAILED', 'SKIPPED', 'ERROR'].includes(value)) totals[value] = (totals[value] || 0) + 1;
  }
  const count = document.querySelector('#suite-count');
  count.hidden = !run.selection;
  count.textContent = Object.entries(totals).map(([verdict, n]) => n + ' ' + verdict.toLowerCase()).join(' · ') ||
    (active ? 'Waiting for the first result…' : 'No case verdicts reported.');
  status.textContent = run.selection ? (run.selection === 'all' ? 'All 70 cases' : run.selection) + ' · ' + run.state : 'Ready';
  const atEnd = log.scrollHeight - log.scrollTop - log.clientHeight < 10;
  log.hidden = !run.log;
  if (log.textContent !== run.log) {
    log.textContent = run.log;
    if (atEnd) log.scrollTop = log.scrollHeight;
  }
  if (active) setTimeout(refresh, 1200);
}

async function refresh() {
  try {
    const response = await fetch('/local/suite', { cache: 'no-store' });
    if (!response.ok) throw new Error();
    const run = await response.json();
    if (typeof run.state !== 'string') throw new Error();
    render(run);
  } catch {
    start.disabled = true;
    status.textContent = 'Runner unavailable. Start the local frontend with node frontend/dev.mjs and reopen Test bench.';
  }
}

form.addEventListener('submit', async event => {
  event.preventDefault();
  start.disabled = select.disabled = true;
  damageStart.disabled = damageCase.disabled = true;
  try {
    const response = await fetch('/local/suite?case=' + encodeURIComponent(select.value),
      { method: 'POST', headers: { 'X-Console-Run': '1' } });
    if (response.status === 409) return refresh();
    const body = await response.json();
    if (!response.ok) throw new Error(body.message);
    render(body);
  } catch (err) {
    status.textContent = 'Could not start test. ' + err.message;
    start.disabled = select.disabled = false;
    damageStart.disabled = damageCase.disabled = false;
  }
});

refresh();

const manualKey = 'object-store.manual-cases.v1';
const picker = document.querySelector('#manual-select');
const note = document.querySelector('#manual-status');
const fields = ['name', 'steps', 'expected', 'observed', 'verdict'];
const field = key => document.querySelector('#manual-' + key);
function readCases() {
  return JSON.parse(localStorage.getItem(manualKey) || '[]');
}
function loadCases(id = '') {
  const cases = readCases();
  picker.replaceChildren(new Option('New case', ''));
  for (const item of cases) picker.add(new Option(item.name + ' · ' + item.verdict, item.id));
  picker.value = id;
  const item = cases.find(item => item.id === id);
  for (const key of fields) field(key).value = item?.[key] ?? (key === 'verdict' ? 'Not run' : '');
  field('observed').required = field('verdict').value !== 'Not run';
}
picker.addEventListener('change', () => { loadCases(picker.value); note.textContent = ''; });
field('verdict').addEventListener('change', () => {
  field('observed').required = field('verdict').value !== 'Not run';
});
document.querySelector('#manual-form').addEventListener('submit', event => {
  event.preventDefault();
  try {
    const cases = readCases();
    const item = { id: picker.value || crypto.randomUUID() };
    for (const key of fields) item[key] = field(key).value.trim();
    if (!item.name || !item.steps || !item.expected || (item.verdict !== 'Not run' && !item.observed)) {
      note.textContent = 'Enter a name, steps and expected output. Include an observation when recording a verdict.';
      return;
    }
    const index = cases.findIndex(entry => entry.id === item.id);
    if (index < 0) cases.push(item);
    else cases[index] = item;
    localStorage.setItem(manualKey, JSON.stringify(cases));
    loadCases(item.id);
    note.textContent = 'Saved in this browser.';
  } catch { note.textContent = 'Could not save this case in browser storage.'; }
});
try { loadCases(); } catch { note.textContent = 'Saved cases are unavailable in this browser.'; }
