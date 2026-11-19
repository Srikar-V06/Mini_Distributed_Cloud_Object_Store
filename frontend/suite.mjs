import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { securityHeaders } from './gateway.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const cases = Array.from({ length: 70 }, (_, i) => 'TC-' + String(i + 1).padStart(2, '0'));
let run = { state: 'Ready', selection: null, log: '' };

function start(selection) {
  const target = selection === 'all' ? 'tests/test_store.py' : 'tests/test_store.py::test_case[' + selection + ']';
  run = { state: 'Running', selection, log: '' };
  const child = spawn(process.env.STORE_TEST_PYTHON || 'python',
    ['-u', '-m', 'pytest', target, '-v', '-rs', '--tb=short', '--color=no'], { cwd: root, windowsHide: true });
  const append = data => { run.log = (run.log + data).slice(-120000); };
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding('utf8');
    stream.on('data', append);
  }
  child.on('error', err => { append('\nCould not start Python: ' + err.message); });
  child.on('close', code => {
    const count = selection === 'all' ? 70 : 1;
    const passed = run.log.match(/(?:^|\s)(\d+) passed\b/g)?.at(-1)?.trim();
    run.state = code !== 0 ? 'Failed' : passed === count + ' passed' ? 'Passed' : 'Incomplete';
  });
}

export function handleSuite(req, res, url, base) {
  const reply = (status, body) => {
    res.writeHead(status, { ...securityHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(body));
  };
  if (req.method === 'GET') return reply(200, run);
  if (req.method !== 'POST') return reply(405, { message: 'Use GET or POST.' });
  if (req.headers.origin !== base || req.headers['x-console-run'] !== '1') {
    return reply(403, { message: 'Start the test from the local console.' });
  }
  const selection = url.searchParams.get('case');
  if (selection !== 'all' && !cases.includes(selection)) return reply(400, { message: 'Choose a listed test.' });
  if (run.state === 'Running') return reply(409, { message: 'A test run is already active.' });
  start(selection);
  reply(202, run);
}
