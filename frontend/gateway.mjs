export const controllerRoutes = [
  ['/health', ['GET']],
  ['/api/v1/objects', ['GET', 'POST']],
  ['/api/v1/objects/:id([A-Za-z0-9_-]+)', ['GET', 'DELETE']],
  ['/api/v1/objects/:id([A-Za-z0-9_-]+)/metadata', ['GET', 'PATCH']],
  ['/api/v1/admin/workers', ['GET']],
  ['/api/v1/admin/replication', ['GET']],
  ['/api/v1/admin/chunks/:id([A-Za-z0-9_-]+)', ['GET']],
  ['/api/v1/admin/scrub', ['POST']],
];

export const securityHeaders = {
  'Content-Security-Policy': "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; object-src 'none'; form-action 'self'",
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
};

export function controllerOrigin(value, protocols = ['http:', 'https:']) {
  const url = new URL(value);
  if (!protocols.includes(url.protocol) || url.username || url.password ||
      url.pathname !== '/' || url.search || url.hash) {
    throw new Error('CONTROLLER_ORIGIN must be an HTTP(S) origin without credentials, path, query, or fragment.');
  }
  return url;
}
