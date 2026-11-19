const icons = {
  box: '<path d="m12 3 8 4.5v9L12 21l-8-4.5v-9L12 3Z"/><path d="m4 7.5 8 4.5 8-4.5M12 12v9M8 5.25l8 4.5"/>',
  folder: '<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2Z"/>',
  cluster: '<rect x="8.5" y="3" width="7" height="6" rx="1.3"/><rect x="2" y="15" width="7" height="6" rx="1.3"/><rect x="15" y="15" width="7" height="6" rx="1.3"/><path d="M12 9v3m-6.5 3v-3h13v3"/>',
  flask: '<path d="M9 3h6m-5 0v7l-5 8a2 2 0 0 0 1.7 3h10.6a2 2 0 0 0 1.7-3L14 10V3M8 15h8"/>',
  refresh: '<path d="M20 7v5h-5M4 17v-5h5m-4-4a8 8 0 0 1 13-2l2 3M4 15l2 3a8 8 0 0 0 13-2"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  search: '<circle cx="10.5" cy="10.5" r="5.5"/><path d="m15 15 4 4"/>',
  chevron: '<path d="m9 6 6 6-6 6"/>',
  file: '<path d="M6 3h8l4 4v14H6zM14 3v5h4M9 13h6m-6 4h6"/>',
  shield: '<path d="M12 3c2 2 5 3 8 4v5c0 4-4 7-8 9-4-2-8-5-8-9V7c3-1 6-2 8-4Z"/><path d="m9 12 2 2 4-4"/>',
  compare: '<path d="M5 8h14m-4-4 4 4-4 4M19 16H5m4-4-4 4 4 4"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  upload: '<path d="M12 16V4m-4 4 4-4 4 4M4 15v5h16v-5"/>',
  download: '<path d="M12 4v12m-4-4 4 4 4-4M4 17v3h16v-3"/>',
  server: '<rect x="4" y="5" width="16" height="6" rx="2"/><rect x="4" y="13" width="16" height="6" rx="2"/><path d="M8 8h.01M8 16h.01"/>',
  layers: '<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
};
const $ = (id) => document.getElementById(id);
const objectApi = '/api/v1/objects';
const adminApi = '/api/v1/admin';
const pages = {
  objects: ['Objects', 'Browse files, edit metadata, and inspect how objects are stored.'],
  cluster: ['Cluster', 'Monitor worker heartbeats, replica health, and recovery.'],
  testing: ['Test bench', 'Exercise chunk boundaries, file integrity, and API responses.'],
};
const data = { page: 'objects', offset: 0, objects: null, workers: null, replication: null };
const failures = {};
let refreshVersion = 0;
let refreshing = false;
let detailVersion = 0;
let selectedFile = null;
let selectedObject = null;
let selectedId = null;
let detailBusy = false;
let uploadBusy = false;

function node(tag, className = '', ...children) {
  const element = document.createElement(tag);
  element.className = className;
  element.append(...children);
  return element;
}

function icon(name) {
  const element = document.createElement('i');
  element.dataset.icon = name;
  paintIcon(element);
  return element;
}

function paintIcon(element) {
  element.setAttribute('aria-hidden', 'true');
  element.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round">' + icons[element.dataset.icon] + '</svg>';
}

document.querySelectorAll('[data-icon]').forEach(paintIcon);

function button(label, onClick, className = 'button small') {
  const element = node('button', className, label);
  element.type = 'button';
  element.addEventListener('click', onClick);
  return element;
}

function showMessage(id, message, error = false) {
  const element = $(id);
  element.textContent = message;
  element.classList.toggle('error', error);
  element.hidden = !message;
}

function describeError(status, payload, requestId) {
  const message = payload?.message || payload?.detail || 'The controller did not return a JSON error.';
  return ['HTTP ' + status, payload?.error, typeof message === 'string' ? message : JSON.stringify(message), requestId && 'Request ' + requestId].filter(Boolean).join(' · ');
}

async function request(path, options = {}) {
  let response;
  try {
    response = await fetch(path, { cache: 'no-store', ...options });
  } catch {
    throw new Error('Could not reach the controller. Check the frontend proxy and Docker services.');
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    throw new Error(describeError(response.status, payload, response.headers.get('x-request-id')));
  }
  return response;
}

async function getJson(path, options) {
  const response = await request(path, options);
  try {
    return await response.json();
  } catch {
    throw new Error('The proxy returned an unexpected response for ' + path + '.');
  }
}

const number = new Intl.NumberFormat();
const date = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' });
const shortDate = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', year: 'numeric' });

function dateLabel(value, format = date) {
  return value && Number.isFinite(Date.parse(value)) ? format.format(new Date(value)) : '—';
}

function bytes(size) {
  if (size == null) return '—';
  if (size < 1024) return number.format(size) + ' B';
  const unit = Math.min(3, Math.floor(Math.log(size) / Math.log(1024)));
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(size / 1024 ** unit) + ' ' + ['B', 'KiB', 'MiB', 'GiB'][unit];
}

function badge(status) {
  const good = ['alive', 'healthy', 'ok'];
  const bad = ['dead', 'degraded', 'unavailable', 'unrecoverable', 'corrupt', 'lost', 'missing'];
  const tone = good.includes(status) ? 'good' : bad.includes(status) ? 'bad' : 'warn';
  const label = (status || 'unknown').replaceAll('_', ' ');
  return node('span', 'badge ' + tone, label[0].toUpperCase() + label.slice(1));
}

function tile(name) {
  return node('span', 'tile', icon(name));
}

function metric(label, value, note, glyph) {
  return node('article', 'card metric',
    node('div', 'metric-header', label, tile(glyph)),
    node('strong', 'metric-value', value == null ? '—' : number.format(value)),
    node('span', 'metric-note', note));
}

function cell(value, className = '') {
  return node('td', className, value instanceof Node ? value : String(value ?? '—'));
}

function placeholderRow(body, columns, message) {
  const messageCell = cell(message);
  messageCell.colSpan = columns;
  body.replaceChildren(node('tr', '', messageCell));
}

function renderObjects() {
  const { objects, workers, replication } = data;
  const registry = workers?.workers || [];
  const peak = Math.max(1, ...registry.map(worker => worker.chunk_count));
  const lanes = registry.map(worker => {
    const meter = node('meter');
    meter.min = 0;
    meter.max = peak;
    meter.value = worker.chunk_count;
    meter.setAttribute('aria-label', worker.worker_id + ' replica count');
    return node('div', 'distribution-lane', node('strong', '', worker.worker_id),
      meter, node('span', '', worker.chunk_count + ' replicas · ' + bytes(worker.bytes_used)));
  });
  $('distribution-plot').replaceChildren(...(lanes.length ? lanes :
    [node('p', '', failures.workers || (workers ? 'No workers registered.' : 'Reading worker registry…'))]));
  const list = objects?.items || [];
  $('metrics').replaceChildren(
    metric('Stored objects', objects?.total, 'Completed uploads', 'folder'),
    metric('Total chunks', replication?.total_chunks, 'Across stored objects', 'layers'),
    metric('Active workers', workers?.alive_count, workers ? 'of ' + workers.workers.length + ' registered' : 'Worker registry', 'cluster'),
  );

  $('object-rows').replaceChildren(...list.map((item) => {
    const open = () => openObject(item.object_id);
    const name = button('', open, 'object-name');
    name.replaceChildren(node('span', 'file-tile', icon('file')),
      node('span', 'file-info', item.name, node('small', '', item.content_type || 'application/octet-stream')));
    const inspect = button('', open, 'button icon-button');
    inspect.append(icon('chevron'));
    inspect.setAttribute('aria-label', 'Inspect ' + item.name);
    return node('tr', '', cell(name), cell(bytes(item.size), 'numeric'), cell(item.num_chunks, 'numeric'),
      cell(dateLabel(item.created_at, shortDate), 'numeric'), cell(inspect));
  }));
  const empty = $('object-empty');
  empty.hidden = Boolean(list.length);
  empty.replaceChildren(tile('folder'), node('strong', '', failures.objects ? 'Object list unavailable' : objects ? 'No objects yet' : 'Loading objects'),
    node('p', '', failures.objects || (objects ? 'Upload a file to get started.' : 'Connecting to the controller…')));
  $('page-count').textContent = objects?.total
    ? (objects.offset + 1) + '–' + (objects.offset + list.length) + ' of ' + number.format(objects.total)
    : objects ? '0 objects' : '—';
  $('previous').disabled = !objects || data.offset === 0;
  $('next').disabled = !objects || objects.offset + list.length >= objects.total;
  $('worker-summary').replaceChildren(...(workers?.workers.length ? workers.workers.map((worker) => node('div', 'worker-row',
    node('span', 'worker-name', icon('server'), worker.worker_id), badge(worker.status))) :
    [node('p', 'report-meta', failures.workers || (workers ? 'No registered workers' : 'Loading workers…'))]));
}

function renderCluster() {
  const { workers, replication } = data;
  $('cluster-metrics').replaceChildren(
    metric('Target replicas', replication?.replication_factor, 'Copies per chunk', 'layers'),
    metric('Under-replicated', replication?.under_replicated, 'Chunks below target', 'shield'),
    metric('Repairs completed', replication?.repairs_done, 'Since controller start', 'refresh'),
  );
  const workerRows = $('worker-rows');
  if (workers?.workers.length) {
    workerRows.replaceChildren(...workers.workers.map((worker) => node('tr', '',
      cell(node('span', 'worker-name', icon('server'), worker.worker_id)),
      cell(badge(worker.status)), cell(worker.seconds_since_heartbeat == null ? '—' : worker.seconds_since_heartbeat + ' s'),
      cell(worker.chunk_count), cell(bytes(worker.bytes_used)))));
  } else {
    placeholderRow(workerRows, 5, failures.workers || (workers ? 'No registered workers.' : 'Loading workers…'));
  }
  $('report-meta').textContent = replication ? 'Observed ' + dateLabel(replication.observed_at) : '';
  const raw = $('replication-raw');
  if (raw.parentElement.open) raw.textContent = replication ? JSON.stringify(replication, null, 2) : '';
  renderReplicationRows();
}

function renderReplicationRows() {
  const report = data.replication;
  const filter = $('replication-filter').value;
  const chunks = report?.chunks.filter((chunk) => !filter ||
    (filter === 'under' ? chunk.effective_replicas < report.replication_factor : chunk.status === filter));
  const body = $('replication-rows');
  if (!chunks?.length) {
    placeholderRow(body, 3, failures.replication || (report ? 'No chunks for this view.' : 'Loading replication report…'));
    return;
  }
  body.replaceChildren(...chunks.map((chunk) => node('tr', '',
    cell(button(chunk.chunk_id, () => openObject(chunk.object_id), 'object-name mono')),
    cell(chunk.effective_replicas + ' / ' + report.replication_factor), cell(badge(chunk.status)))));
}

function setConnection(status) {
  const label = status === 'online' ? 'Controller online' : 'Controller unavailable';
  $('connection').className = 'connection ' + status;
  $('connection').replaceChildren(node('span', 'status-dot'), label);
}

async function refresh({ silent = false } = {}) {
  const version = ++refreshVersion;
  refreshing = true;
  const limit = Number($('page-size').value);
  const offset = data.offset;
  const query = new URLSearchParams({ limit, offset });
  const entries = {
    health: '/health',
    objects: objectApi + '?' + query,
    workers: adminApi + '/workers',
    replication: adminApi + '/replication',
  };
  if (!silent) {
    $('refresh').disabled = true;
    $('previous').disabled = true;
    $('next').disabled = true;
  }
  const results = await Promise.allSettled(Object.values(entries).map((path) =>
    getJson(path, { signal: AbortSignal.timeout(15000) })));
  if (version !== refreshVersion) return;
  refreshing = false;

  results.forEach((result, index) => {
    const key = Object.keys(entries)[index];
    data[key] = result.status === 'fulfilled' ? result.value : null;
    failures[key] = result.status === 'rejected' ? result.reason.message : '';
  });
  setConnection(data.health?.status === 'ok' ? 'online' : 'offline');
  if (data.health?.status !== 'ok') {
    showMessage('notice', 'Controller unavailable. For local testing, start Docker Compose and open the console through its local server. On Vercel, check the controller origin and deployment access.', true);
  } else if ($('notice').classList.contains('error')) {
    showMessage('notice', '');
  }

  if (data.objects && offset > 0 && offset >= data.objects.total) {
    data.offset = Math.max(0, Math.floor((data.objects.total - 1) / limit) * limit);
    return refresh({ silent });
  }
  $('refresh').disabled = false;
  renderObjects();
  if (data.page === 'cluster') renderCluster();
}

function showPage() {
  const key = location.hash.slice(1);
  data.page = Object.hasOwn(pages, key) ? key : 'objects';
  const [title, description] = pages[data.page];
  $('breadcrumb').textContent = title;
  $('page-title').textContent = title;
  $('page-description').textContent = description;
  $('upload-open').hidden = data.page !== 'objects';
  document.querySelectorAll('.page').forEach((section) => { section.hidden = section.id !== data.page + '-page'; });
  document.querySelectorAll('[data-page]').forEach((link) => {
    if (link.dataset.page === data.page) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  });
  if (data.page === 'cluster') {
    renderCluster();
    refresh({ silent: true });
  }
}

function objectPath(id) {
  if (!id || !/^[A-Za-z0-9_-]+$/.test(id)) throw new Error('Enter an object ID containing letters, digits, underscores, or hyphens.');
  return objectApi + '/' + id;
}

function setFile(file) {
  selectedFile = file;
  $('file-name').textContent = file ? file.name + ' · ' + bytes(file.size) : 'Choose a file or drop it here';
}

function openUpload(file = null) {
  $('upload-form').reset();
  setFile(file);
  showMessage('upload-result', '');
  $('upload-dialog').showModal();
}

function upload(form, progress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', objectApi);
    xhr.responseType = 'json';
    xhr.upload.addEventListener('progress', (event) => {
      if (event.lengthComputable) {
        progress.max = event.total;
        progress.value = event.loaded;
      }
    });
    xhr.upload.addEventListener('load', () => {
      progress.removeAttribute('value');
      showMessage('upload-result', 'File sent. Waiting for the controller to finish storing replicas…');
    });
    xhr.addEventListener('error', () => reject(new Error('Upload connection failed. Check the controller and refresh the object list before retrying.')));
    xhr.addEventListener('load', () => {
      if (xhr.status === 201 && xhr.response?.object_id) resolve(xhr.response);
      else reject(new Error(describeError(xhr.status, xhr.response, xhr.getResponseHeader('x-request-id'))));
    });
    xhr.send(form);
  });
}

async function submitUpload(event) {
  event.preventDefault();
  if (uploadBusy) return;
  if (!selectedFile) {
    showMessage('upload-result', 'Choose a file first.', true);
    return;
  }
  const form = new FormData();
  const metadata = $('upload-metadata').value.trim();
  form.set('file', selectedFile);
  if (metadata) form.set('metadata', metadata);
  const progress = $('upload-progress');
  const controls = [...event.currentTarget.querySelectorAll('button, input, textarea'), $('upload-dialog').querySelector('.dialog-heading button')];
  uploadBusy = true;
  controls.forEach((control) => { control.disabled = true; });
  progress.removeAttribute('value');
  progress.hidden = false;
  showMessage('upload-result', '');
  try {
    const object = await upload(form, progress);
    $('upload-dialog').close();
    if (data.page !== 'objects') {
      history.replaceState(null, '', '#objects');
      showPage();
      window.scrollTo(0, 0);
    }
    data.offset = 0;
    showMessage('notice', 'Uploaded ' + object.name + ' · ' + object.num_chunks + ' chunk(s) · HTTP 201.');
    refresh({ silent: true });
    openObject(object.object_id);
  } catch (error) {
    showMessage('upload-result', error.message, true);
  } finally {
    uploadBusy = false;
    controls.forEach((control) => { control.disabled = false; });
    progress.hidden = true;
  }
}

function renderMetadata(metadata) {
  const { system, custom, chunks } = metadata;
  selectedObject = system;
  $('detail-title').textContent = system.name;
  $('custom-fields').textContent = JSON.stringify(custom, null, 2);
  const fields = [
    ['Size', bytes(system.size)], ['Content type', system.content_type],
    ['Chunks', system.num_chunks], ['Chunk size', bytes(system.chunk_size)],
    ['Created', dateLabel(system.created_at)], ['Modified', dateLabel(system.modified_at)],
    ['Object SHA-256', system.checksum, true],
  ];
  $('system-fields').replaceChildren(...fields.map(([label, value, full]) => {
    const definition = node('dd', full ? 'mono' : '', String(value ?? '—'));
    if (label === 'Modified') definition.id = 'object-modified';
    return node('div', full ? 'full' : '', node('dt', '', label), definition);
  }));
  $('detail-content').hidden = false;
  renderChunks(chunks);
}

function renderChunks(chunks, target, note) {
  $('chunk-note').textContent = note || (target ? 'Target: ' + target + ' replicas per chunk.' : '');
  const body = $('chunk-rows');
  if (!chunks.length) {
    placeholderRow(body, 3, 'This object has no chunks.');
    return;
  }
  body.replaceChildren(...chunks.map((chunk) => {
    const label = node('div', '', node('strong', '', 'Chunk ' + chunk.index),
      node('div', 'mono muted', chunk.chunk_id), node('div', 'mono muted', chunk.checksum));
    const locations = node('div', '', chunk.effective_replicas == null ? '' : chunk.effective_replicas + ' / ' + target + ' effective',
      node('div', 'replicas', ...chunk.replicas.map((replica) => {
        const element = badge(replica.state);
        element.textContent = (replica.worker_id || replica.worker) + ' · ' + replica.state +
          (replica.worker_status ? ' · ' + replica.worker_status : '');
        if (replica.verified_at) element.title = 'Last verified ' + dateLabel(replica.verified_at);
        return element;
      })));
    return node('tr', '', cell(label), cell(bytes(chunk.size), 'numeric'), cell(locations));
  }));
}

async function loadPlacement(id, version, fallback) {
  try {
    const report = await getJson(adminApi + '/chunks/' + id);
    if (version === detailVersion) renderChunks(report.chunks, report.replication_factor);
  } catch (error) {
    if (version !== detailVersion) return;
    if (fallback) renderChunks(fallback, null, 'Live placement unavailable. Showing replicas from object metadata. ' + error.message);
    else $('chunk-note').textContent = 'Could not refresh placement. ' + error.message;
  }
}

async function openObject(id) {
  const version = ++detailVersion;
  const dialog = $('detail-dialog');
  selectedId = id;
  selectedObject = null;
  $('detail-title').textContent = 'Object details';
  $('detail-id').textContent = id;
  $('detail-content').hidden = true;
  $('patch-form').reset();
  showMessage('detail-result', 'Loading object…');
  if (!dialog.open) dialog.showModal();
  try {
    const metadata = await getJson(objectPath(id) + '/metadata');
    if (version !== detailVersion) return;
    showMessage('detail-result', '');
    renderMetadata(metadata);
    await loadPlacement(id, version, metadata.chunks);
  } catch (error) {
    if (version === detailVersion) showMessage('detail-result', error.message, true);
  }
}

async function withDetail(action) {
  if (detailBusy || !selectedObject) return;
  detailBusy = true;
  const controls = $('detail-content').querySelectorAll('button, textarea');
  controls.forEach((element) => { element.disabled = true; });
  try {
    await action(selectedObject);
  } catch (error) {
    showMessage('detail-result', error.message, true);
  } finally {
    detailBusy = false;
    controls.forEach((element) => { element.disabled = false; });
  }
}

async function sha256(blob) {
  if (!window.crypto.subtle) throw new Error('SHA-256 requires HTTPS or localhost.');
  const digest = await crypto.subtle.digest('SHA-256', await blob.arrayBuffer());
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

async function downloadVerified(object) {
  showMessage('detail-result', 'Downloading and verifying SHA-256…');
  const response = await request(objectPath(object.object_id));
  const headerHash = response.headers.get('x-object-checksum');
  const headerSize = response.headers.get('content-length');
  if (response.status !== 200 || !/^[0-9a-f]{64}$/i.test(headerHash || '')) {
    throw new Error('The download response did not return HTTP 200 with a valid SHA-256.');
  }
  let blob;
  try {
    blob = await response.blob();
  } catch {
    throw new Error('The download stream ended early. No file was saved.');
  }
  const uncompressed = !response.headers.has('content-encoding');
  if (blob.size !== object.size ||
      uncompressed && headerSize !== null && (!/^\d+$/.test(headerSize) || blob.size !== Number(headerSize))) {
    throw new Error('Download size mismatch. No file was saved.');
  }
  const actual = await sha256(blob);
  if (actual !== headerHash.toLowerCase() || actual !== object.checksum.toLowerCase()) {
    throw new Error('SHA-256 mismatch. No file was saved.');
  }
  const url = URL.createObjectURL(blob);
  const anchor = node('a');
  anchor.href = url;
  anchor.download = object.name;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
  showMessage('detail-result', 'Download verified · ' + bytes(blob.size) + ' · SHA-256 matched.');
}

async function runTest(control, resultId, run) {
  const result = $(resultId);
  control.disabled = true;
  result.textContent = 'Running…';
  result.hidden = false;
  try {
    result.textContent = JSON.stringify(await run(), null, 2);
  } catch (error) {
    result.textContent = error.message;
  } finally {
    control.disabled = false;
  }
}

$('refresh').addEventListener('click', () => refresh());
$('page-size').addEventListener('change', () => { data.offset = 0; refresh(); });
$('previous').addEventListener('click', () => { data.offset = Math.max(0, data.offset - Number($('page-size').value)); refresh(); });
$('next').addEventListener('click', () => { data.offset += Number($('page-size').value); refresh(); });
$('replication-filter').addEventListener('change', renderReplicationRows);
$('replication-raw').parentElement.addEventListener('toggle', () => {
  if ($('replication-raw').parentElement.open) renderCluster();
});
$('live').addEventListener('change', () => { if ($('live').checked) refresh({ silent: true }); });
$('upload-open').addEventListener('click', () => openUpload());
$('upload-file').addEventListener('change', (event) => setFile(event.target.files[0] || null));
$('upload-form').addEventListener('submit', submitUpload);
$('upload-dialog').addEventListener('cancel', (event) => { if (uploadBusy) event.preventDefault(); });
$('detail-dialog').addEventListener('cancel', (event) => { if (detailBusy) event.preventDefault(); });
$('detail-dialog').addEventListener('close', () => { if (!$('detail-dialog').open) detailVersion++; });
document.querySelectorAll('[data-close]').forEach((control) => control.addEventListener('click', () => {
  const dialog = control.closest('dialog');
  if (dialog.id !== 'detail-dialog' || !detailBusy) dialog.close();
}));

for (const type of ['dragover', 'dragleave', 'drop']) {
  $('dropzone').addEventListener(type, (event) => {
    event.preventDefault();
    $('dropzone').classList.toggle('dragging', type === 'dragover' && !uploadBusy);
    if (type === 'drop' && !uploadBusy && event.dataTransfer.files.length) {
      $('upload-file').value = '';
      setFile(event.dataTransfer.files[0]);
    }
  });
}

$('lookup-open').addEventListener('click', () => $('lookup-dialog').showModal());
$('lookup-form').addEventListener('submit', (event) => {
  event.preventDefault();
  $('lookup-dialog').close();
  openObject($('lookup-id').value.trim());
});

$('fixture-open').addEventListener('click', () => {
  const size = Number($('fixture').value);
  const content = new Uint8Array(size);
  for (let start = 0; start < size; start += 1024 * 1024) {
    content.fill(65 + start / (1024 * 1024), start, start + 1024 * 1024);
  }
  openUpload(new File([content], 'test-' + size + '-bytes.bin', { type: 'application/octet-stream' }));
});

$('text-fixture').addEventListener('submit', event => {
  event.preventDefault();
  openUpload(new File([$('text-body').value], $('text-name').value, { type: 'text/plain' }));
});

$('patch-form').addEventListener('submit', (event) => {
  event.preventDefault();
  withDetail(async (object) => {
    const path = objectPath(object.object_id) + '/metadata';
    showMessage('detail-result', 'Applying metadata patch…');
    const result = await getJson(path, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: $('metadata-patch').value,
    });
    $('patch-form').reset();
    $('custom-fields').textContent = JSON.stringify(result.custom, null, 2);
    $('object-modified').textContent = dateLabel(result.modified_at);
    showMessage('detail-result', 'Metadata updated · HTTP 200.');
  });
});

$('chunks-refresh').addEventListener('click', () => withDetail(() => loadPlacement(selectedId, ++detailVersion)));
$('download').addEventListener('click', () => withDetail(downloadVerified));
$('delete').addEventListener('click', () => withDetail(async (object) => {
  if (!window.confirm('Delete "' + object.name + '" from the object store?')) return;
  await request(objectPath(object.object_id), { method: 'DELETE' });
  $('detail-dialog').close();
  showMessage('notice', 'Deleted ' + object.name + ' · HTTP 204.');
  refresh({ silent: true });
}));

$('scrub').addEventListener('click', (event) => runTest(event.currentTarget, 'scrub-result', async () => {
  const result = await getJson(adminApi + '/scrub', { method: 'POST' });
  refresh({ silent: true });
  return result;
}));

$('compare-form').addEventListener('submit', (event) => {
  event.preventDefault();
  const original = $('original').files[0];
  const downloaded = $('downloaded').files[0];
  runTest(event.submitter, 'compare-result', async () => {
    const first = await sha256(original);
    const second = await sha256(downloaded);
    return { match: first === second, original_sha256: first, downloaded_sha256: second };
  });
});

$('probe-form').addEventListener('submit', (event) => {
  event.preventDefault();
  const scenario = $('probe').value;
  runTest(event.submitter, 'probe-result', async () => {
    const form = new FormData();
    form.set('metadata', '{');
    if (scenario === 'meta') form.set('file', new File([], 'validation-test.bin'));
    const routes = {
      missing: [objectApi + '/' + crypto.randomUUID() + '/metadata', {}, 404],
      limit: [objectApi + '?limit=0', {}, 400],
      file: [objectApi, { method: 'POST', body: new FormData() }, 400],
      meta: [objectApi, { method: 'POST', body: form }, 400],
    };
    const [path, options, expected] = routes[scenario];
    const response = await fetch(path, { cache: 'no-store', ...options });
    const payload = await response.json().catch(() => null);
    return {
      expected_status: expected, actual_status: response.status,
      passed: response.status === expected && typeof payload?.error === 'string',
      request_id: response.headers.get('x-request-id'), response: payload,
    };
  });
});

window.addEventListener('hashchange', () => {
  showPage();
  window.scrollTo(0, 0);
});
setInterval(() => {
  if (data.page === 'cluster' && $('live').checked && !document.hidden && !refreshing) refresh({ silent: true });
}, 5000);
renderObjects();
showPage();
if (data.page !== 'cluster') refresh();
