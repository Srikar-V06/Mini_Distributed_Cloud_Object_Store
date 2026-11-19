import { controllerOrigin, controllerRoutes, securityHeaders } from './gateway.mjs';

const configuredOrigin = process.env.CONTROLLER_ORIGIN;
const origin = configuredOrigin ? controllerOrigin(configuredOrigin, ['https:']).origin : null;

export const config = {
  framework: null,
  buildCommand: null,
  outputDirectory: 'public',
  headers: [
    {
      source: '/(.*)',
      headers: Object.entries(securityHeaders).map(([key, value]) => ({ key, value })),
    },
    ...controllerRoutes.map(([source]) => ({
      source,
      headers: [{ key: 'Cache-Control', value: 'no-store' }],
    })),
  ],
  rewrites: origin ? controllerRoutes.map(([source]) => ({
    source,
    destination: origin + source.replace(':id([A-Za-z0-9_-]+)', ':id'),
  })) : [],
};
