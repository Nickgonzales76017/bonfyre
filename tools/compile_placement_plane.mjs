#!/usr/bin/env bun
// CUE validates the declaration; this compiler lowers it to reviewable plans.
// Applying routes, tunnels or SQL remains an explicit deployment act.
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = { input: '', out: '' };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].slice(2);
  if (!process.argv[i].startsWith('--') || !Object.hasOwn(args, key) || i + 1 >= process.argv.length) throw Error(`placement-plane: unknown or incomplete option ${process.argv[i]}`);
  args[key] = process.argv[++i];
}
const requireLaw = (okay, message) => { if (!okay) throw Error(`placement-plane: ${message}`); };
requireLaw(args.input && args.out, '--input and --out are required');
const declaration = JSON.parse(readFileSync(args.input === '/dev/stdin' ? 0 : args.input, 'utf8'));
requireLaw(declaration.schema_version === 'bonfyre.placement-plane.v1', 'unsupported schema_version');
requireLaw(declaration.law === 'No realization may be the sole residence of truth required to reconstruct itself.', 'reconstruction law missing');
requireLaw(JSON.stringify(declaration.providers) === JSON.stringify(['cloudflare', 'supabase', 'oracle']), 'provider machine must be Cloudflare, Supabase, Oracle');
const services = declaration.services;
requireLaw(Array.isArray(services), 'services must be a list');
requireLaw(new Set(services.map(s => s.id)).size === services.length, 'service ids must be unique');
const service = id => { const value = services.find(s => s.id === id); requireLaw(value, `required service missing: ${id}`); return value; };
const mcp = service('mcp-resource'), auth = service('oauth-authority'), estate = service('public-estate'), continuity = service('continuity-projection'), material = service('immutable-material');
requireLaw(mcp.primary === 'oracle' && auth.primary === 'oracle', 'MCP and OAuth execute on Oracle');
requireLaw(continuity.primary === 'supabase', 'continuity projection must be on Supabase');
requireLaw(estate.primary === 'cloudflare' && estate.public_origin, 'public estate requires a Cloudflare public origin');
requireLaw(material.primary === 'cloudflare' && material.public_origin, 'immutable material requires a direct Cloudflare public origin');
const mcpOrigin = mcp.public_origin, authOrigin = auth.public_origin;
requireLaw(mcpOrigin !== authOrigin, 'MCP resource and authorization server require distinct canonical origins');
const legacy = declaration.legacy_worker_exclusions || [], canonical = declaration.canonical_worker_exclusions || [];
requireLaw(['aurekai.work/oauth*', 'aurekai.work/mcp*', 'aurekai.work/.well-known/oauth-*'].every(x => legacy.includes(x)), 'legacy OAuth/MCP no-worker exclusions missing');
requireLaw(['mcp.aurekai.work/*', 'auth.aurekai.work/*'].every(x => canonical.includes(x)), 'canonical control-host no-worker exclusions missing');
const sorted = value => Array.isArray(value) ? value.map(sorted) : value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(k => [k, sorted(value[k])])) : value;
const digest = createHash('sha256').update(JSON.stringify(sorted(declaration))).digest('hex');
const host = origin => origin.replace(/^https:\/\//, '').replace(/\/+$/, '');
const dump = (relative, value) => { const path = join(args.out, relative); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, JSON.stringify(sorted(value), null, 2) + '\n'); };
const text = (relative, value) => { const path = join(args.out, relative); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, value); };
dump('placement-receipt.json', { schema_version: 'bonfyre.placement-receipt.v1', declaration_digest: `sha256:${digest}`, state: 'planned', law: declaration.law,
  services: services.map(s => ({ id: s.id, semantic_owner: s.semantic_owner, primary: s.primary, projections: s.projections || [] })) });
dump('cloudflare/worker-route-plan.json', { action: 'create-no-worker-routes-before-broad-estate-worker-route', routes: [...canonical, ...legacy].map(pattern => ({ pattern, script: null })),
  invariant: 'Canonical MCP/OAuth control hosts and legacy compatibility paths bypass the static estate Worker.' });
dump('cloudflare/r2-material-domain-plan.json', { bucket: 'aurekai-estate', domain: host(material.public_origin), action: 'attach-custom-domain-and-create-no-worker-exclusion',
  worker_exclusion: `${host(material.public_origin)}/*`, invariant: 'Immutable material delivery bypasses the estate Worker; direct material residency remains distinct from page routing and semantic ownership.' });
text('cloudflared/placement-plane-ingress.yml', `# Generated PlacementPlane ingress fragment.  These exact hosts MUST precede
# *.aurekai.work so machine control is never treated as an estate object.
ingress:
  - hostname: ${host(mcpOrigin)}
    service: http://127.0.0.1:8791
  - hostname: ${host(authOrigin)}
    service: http://127.0.0.1:8791
  - hostname: aurekai.work
    service: http://127.0.0.1:8787
  - hostname: "*.aurekai.work"
    service: http://127.0.0.1:8787
  - service: http_status:404
`);
text('systemd/bonfyre-oauth-mcp-placement.conf', `# Generated PlacementPlane service environment.
[Service]
Environment=BONFYRE_MCP_PUBLIC_ORIGIN=${mcpOrigin}
Environment=BONFYRE_AUTH_PUBLIC_ORIGIN=${authOrigin}
`);
text('supabase/placement-plane.sql', readFileSync(join(root, 'deploy', 'supabase', 'placement-plane.sql'), 'utf8'));
dump('oracle/service-bindings.json', { gateway: 'bonfyre-oauth-mcp', listen: '127.0.0.1:8791', mcp_origin: mcpOrigin, auth_origin: authOrigin,
  durable_projection: 'supabase.oauth_authorization_transaction', invariant: 'Oracle is authoritative execution; Supabase stores only sealed transaction continuity and projection receipts.' });
dump('failure-matrix.json', {
  cloudflare_unavailable: { public_delivery: 'degraded', native_effects: 'unreachable from public edge', truth: 'retained outside delivery' },
  supabase_unavailable: { public_delivery: 'serves last material', oauth_code_exchange: 'refused when durable store is configured', native_effects: 'continue only at owner discretion' },
  oracle_unavailable: { public_delivery: 'serves last proven material', search_projection: 'serves last indexed result', mcp_and_oauth: 'degraded', truth: 'reconstruct from material and continuity projections' },
});
