import { readFile } from 'node:fs/promises';
import { buildControlPlaneApp } from './control_plane_app_build.mjs';

export const CONTROL_PLANE_APP_URI = 'ui://aurekai/project-control-plane/v1.html';
export const CONTROL_PLANE_APP_RESOURCE = {
  uri: CONTROL_PLANE_APP_URI,
  name: 'Aurekai Project Control Plane',
  description: 'Capability packs, estate readiness, STOP state and managed-runtime drift for the current Project MCP aperture.',
  mimeType: 'text/html;profile=mcp-app',
  _meta: { ui: { csp: { connectDomains: [], resourceDomains: [], frameDomains: [], baseUriDomains: [] } } },
};

let defaultBundle = null;
async function bundle(file) {
  if (file) return readFile(file, 'utf8');
  if (!defaultBundle) defaultBundle = buildControlPlaneApp({ stdout: true }).then((result) => result.html).catch((error) => {
    defaultBundle = null;
    throw error;
  });
  return defaultBundle;
}

export async function readControlPlaneApp(uri, file = process.env.BONFYRE_PROJECT_CONTROL_PLANE_APP_FILE || null) {
  if (uri !== CONTROL_PLANE_APP_URI) return null;
  const text = await bundle(file);
  if (Buffer.byteLength(text) > 512 * 1024 || !text.startsWith('<!doctype html>')) throw Error('Invalid Project control-plane application bundle.');
  return { contents: [{ ...CONTROL_PLANE_APP_RESOURCE, text }] };
}
