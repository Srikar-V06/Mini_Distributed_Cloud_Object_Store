import { readFile } from 'node:fs/promises';
import { createServer, request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { controllerOrigin, controllerRoutes, securityHeaders } from './gateway.mjs';
import { handleSuite } from './suite.mjs';

const port = Number(process.env.FRONTEND_PORT || 5173);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('FRONTEND_PORT must be a valid port.');
const origin = controllerOrigin(process.env.CONTROLLER_ORIGIN || 'http://127.0.0.1:8000');
const staticFiles = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/styles.css', ['styles.css', 'text/css; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/suite.js', ['suite.js', 'text/javascript; charset=utf-8']],
]);
const proxyRoutes = controllerRoutes.map(([path, methods]) =>
  [new RegExp('^' + path.replace(':id', '') + '$'), methods]);
const allowedHosts = new Set(['localhost:' + port, '127.0.0.1:' + port]);

function sendError(res, status, error, message) {
  res.writeHead(status, { ...securityHeaders, 'Cache-Control': 'no-store', 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error, message }));
}

function proxy(req, res, url) {
  const upstreamRequest = origin.protocol === 'https:' ? httpsRequest : httpRequest;
  const headers = { 'Accept-Encoding': 'identity' };
  for (const name of ['content-type', 'content-length', 'x-request-id']) {
    if (req.headers[name] !== undefined) headers[name] = req.headers[name];
  }
  const upstream = upstreamRequest(origin, { method: req.method, path: url.pathname + url.search, headers },
    (response) => {
      const outgoing = { ...securityHeaders, 'Cache-Control': 'no-store' };
      for (const name of ['content-type', 'content-length', 'content-disposition', 'content-encoding',
        'x-object-checksum', 'x-request-id', 'etag', 'retry-after']) {
        if (response.headers[name] !== undefined) outgoing[name] = response.headers[name];
      }
      response.on('error', (error) => res.destroy(error));
      res.writeHead(response.statusCode, outgoing);
      response.pipe(res);
    });
  upstream.setTimeout(10 * 60 * 1000, () => upstream.destroy(new Error('Controller request timed out.')));
  upstream.on('error', (error) => {
    if (res.headersSent) res.destroy(error);
    else if (!res.destroyed) sendError(res, 502, 'controller_unreachable', 'Could not reach ' + origin.origin + '. Check that the controller is running.');
  });
  req.on('error', (error) => upstream.destroy(error));
  res.on('close', () => { if (!res.writableFinished) upstream.destroy(); });
  req.pipe(upstream);
}

const server = createServer(async (req, res) => {
  const base = 'http://' + req.headers.host;
  if (!allowedHosts.has(req.headers.host)) {
    sendError(res, 403, 'invalid_host', 'Use the localhost address printed by the frontend server.');
    return;
  }
  if (!req.url.startsWith('/') || req.url.startsWith('//') || req.url.includes('\\')) {
    sendError(res, 400, 'invalid_path', 'Use a relative URL on the frontend server.');
    return;
  }
  const url = new URL(req.url, base);
  if (url.pathname === '/local/suite') return handleSuite(req, res, url, base);
  const file = staticFiles.get(url.pathname);
  if (file && ['GET', 'HEAD'].includes(req.method)) {
    try {
      const body = await readFile(new URL('./public/' + file[0], import.meta.url));
      res.writeHead(200, { ...securityHeaders, 'Content-Type': file[1], 'Cache-Control': 'no-store' });
      res.end(req.method === 'HEAD' ? undefined : body);
    } catch {
      sendError(res, 500, 'frontend_file_unavailable', 'Could not read the frontend asset.');
    }
    return;
  }
  const route = proxyRoutes.find(([pattern]) => pattern.test(url.pathname));
  if (!route) {
    sendError(res, 404, 'not_found', 'The frontend does not serve this route.');
    return;
  }
  if (!route[1].includes(req.method)) {
    sendError(res, 405, 'method_not_allowed', 'The frontend proxy does not forward this method.');
    return;
  }
  if (req.method !== 'GET' && (req.headers.origin && req.headers.origin !== base ||
      req.headers['sec-fetch-site'] && req.headers['sec-fetch-site'] !== 'same-origin')) {
    sendError(res, 403, 'origin_not_allowed', 'Make changes from the local console.');
    return;
  }
  proxy(req, res, url);
});

server.requestTimeout = 15 * 60 * 1000;
server.listen(port, '127.0.0.1', () => {
  console.log('Object Store Console: http://127.0.0.1:' + port);
  console.log('Controller: ' + origin.origin);
});
