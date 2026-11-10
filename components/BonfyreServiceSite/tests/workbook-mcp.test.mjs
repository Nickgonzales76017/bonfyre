import { test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync, readFileSync, openSync, closeSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { workspaceFixture, root } from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import { freePort, pause } from '../../../cmd/BonfyreWebAddress/tests/auth_continuation_fixture.mjs';
import { projectFinanceWorkspaceSetup, publishSetups } from '../../../scripts/publish_experience_setups.mjs';
import { createWorkspaceMcp } from '../../../integrations/mcp/workspace_mcp.mjs';
import { workbookPackage } from '../workbook-package.mjs';
import { importWorkbook } from '../workbook-import.mjs';
import { workbookReviewValue, readWorkbookReview } from '../workbook-record.mjs';
import { calculateWorkbook } from '../workbook-calculation.mjs';
import { detectReview, projectReview, markRowsReviewed, withReviewSheet } from '../workbook-review.mjs';
import { exportWorkbookSnapshot } from '../workbook-model.mjs';
import { buildWorkbookMcpApp } from '../workbook-mcp-build.mjs';
import { createWorkbookMcpController } from '../workbook-mcp-controller.mjs';
import { WORKBOOK_APP_URI } from '../../../integrations/mcp/workbook_app_resource.mjs';
import { App } from '@modelcontextprotocol/ext-apps';
import { AppBridge } from '@modelcontextprotocol/ext-apps/app-bridge';

let temp, pack, f, binding, call, gateway, gatewayBase, original;
const key = 'finance-workbook:mcp-review', verified = new Set(), wire = [];
const previous = Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE', 'BONFYRE_WORKSPACE_PACKAGES'].map(k => [k, process.env[k]]));
const session = user => decodeURIComponent(user.cookie.slice(user.cookie.indexOf('=') + 1));
const identity = read => ({ binding: binding.id, key, expected_revision: read.revision, sha256: read.sha256 });
const select = row => ({ row: row.row, fingerprint: row.fingerprint });
const invoke = (name, args, user = f.owner) => call(name, args, session(user));
const current = () => invoke('workbook_review', { binding: binding.id, key });
beforeAll(async () => {
  temp = mkdtempSync(join(tmpdir(), 'workbook-mcp-')); const packages = join(temp, 'packages');
  await buildWorkbookMcpApp(join(temp, 'review.html'));
  pack = await workbookPackage({ packageRoot: packages });
  process.env.BONFYRE_WORKSPACE_RELEASE = pack.id; process.env.BONFYRE_WORKSPACE_PACKAGES = packages;
  f = await workspaceFixture();
  publishSetups({ records: projectFinanceWorkspaceSetup(pack.id), binary: f.cms, db: f.db, schemas: join(f.home, 'schemas'), apply: true });
  binding = await f.install('experience.finance-workbooks.workspace', 'institution:workspace-team');
  const book = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(book, XLSX.utils.aoa_to_sheet([
    ['Description', 'Amount', 'Account', 'Tax'],
    ['Supplies', { t: 'n', v: 20, f: '10*2', z: '#,##0.00' }, 'Office', { t: 'n', v: 2, f: 'B2*0.1' }],
    ['Train', -12.34, 'Travel', 0], ['Missing receipt', '', '', 0],
  ]), 'Transactions');
  original = await importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx' }), { name: 'Source.xlsx' });
  const calculated = (await calculateWorkbook(original.snapshot)).snapshot;
  await f.store(binding).saveLocal(key, workbookReviewValue(calculated, original.source, detectReview(calculated)), 0);
  call = createWorkspaceMcp({ ownerUrl: f.base, publicOrigin: f.base, request: async (url, options) => { wire.push({ method: options.method, operation: url.searchParams.get('operation'), prefix: url.searchParams.get('prefix') }); return fetch(url, options); } });
  const port = await freePort(), tokenfile = join(f.home, 'project.token'); writeFileSync(tokenfile, 'fixture-only');
  const log = openSync(join(f.home, 'gateway.log'), 'w', 0o600);
  gateway = spawn(process.env.WORKBOOK_MCP_GATEWAY_RUNTIME || 'node', [join(root, 'integrations/mcp/bonfyre_oauth_gateway.mjs')], { env: { ...process.env,
    BONFYRE_AUTH_BINARY: f.auth, BONFYRE_AUTH_DB: f.authdb, BONFYRE_CMS_BINARY: f.cms, BONFYRE_CMS_DB: f.db,
    BONFYRE_UPPER90_BINARY: join(root, 'cmd/BonfyreUpper90/bonfyre-upper90'), BONFYRE_UPPER90_DB: join(f.home, 'upper90.db'),
    BONFYRE_PROJECT_MCP_TOKEN_FILE: tokenfile, BONFYRE_PROJECT_MCP_URL: 'http://127.0.0.1:1/mcp', BONFYRE_OAUTH_MCP_PORT: String(port),
    BONFYRE_WORKSPACE_OWNER_URL: f.base, BONFYRE_WORKSPACE_PUBLIC_ORIGIN: f.base, BONFYRE_WORKBOOK_MCP_APP_FILE: join(temp, 'review.html') }, stdio: ['ignore', log, log] }); closeSync(log);
  gatewayBase = `http://127.0.0.1:${port}`;
  for (let n = 0; n < 100; n++) { if (gateway.exitCode !== null) throw Error(readFileSync(join(f.home, 'gateway.log'), 'utf8')); try { if ((await fetch(gatewayBase + '/auth.md')).ok) return; } catch {} await pause(50); }
  throw Error('Workbook MCP gateway did not start.');
}, 60000);
afterAll(async () => {
  if (verified.size === 9 && process.env.WORKBOOK_MCP_PROOF) {
    const out = process.env.WORKBOOK_MCP_PROOF; mkdirSync(out, { recursive: true });
    writeFileSync(join(out, 'mcp.json'), JSON.stringify({ package: pack.id, checks: [...verified], browser_verified: false, live_deployment: false,
      scope: 'isolated real Auth, CMS, WebAddress, account MCP gateway and Univer worker', gateway_runtime: gateway.spawnargs[0],
      binaries: Object.fromEntries(['web', 'cms', 'auth'].map(name => [name, createHash('sha256').update(readFileSync(f[name])).digest('hex')])) }, null, 2) + '\n');
  }
  if (gateway && gateway.exitCode === null) { gateway.kill(); await Promise.race([new Promise(r => gateway.once('exit', r)), pause(2000)]); if (gateway.exitCode === null) gateway.kill('SIGKILL'); }
  await f?.close(); if (temp) rmSync(temp, { recursive: true, force: true });
  for (const [key, value] of Object.entries(previous)) if (value === undefined) delete process.env[key]; else process.env[key] = value;
});
async function rpc(method, params, user = f.owner) {
  const response = await fetch(gatewayBase + '/mcp', { method: 'POST', headers: { Authorization: 'Bearer ' + session(user), 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) });
  expect(response.status).toBe(200); return (await response.json()).result;
}
const mcp = (name, args, user) => rpc('tools/call', { name, arguments: args }, user);

test('official MCP Apps bridge continues native financial review through the authenticated gateway', async () => {
  const tools = (await rpc('tools/list', {})).tools;
  expect(tools.find(tool => tool.name === 'workbook_review')._meta.ui.resourceUri).toBe(WORKBOOK_APP_URI);
  expect(tools.find(tool => tool.name === 'workbook_preview')._meta).toBeUndefined();
  const resources = (await rpc('resources/list', {})).resources;
  expect(resources.find(resource => resource.uri === WORKBOOK_APP_URI).mimeType).toBe('text/html;profile=mcp-app');
  const resource = (await rpc('resources/read', { uri: WORKBOOK_APP_URI })).contents[0];
  expect(resource._meta.ui.csp.connectDomains).toEqual([]);
  expect(resource.text).toBe(readFileSync(join(temp, 'review.html'), 'utf8'));
  expect(resource.text).not.toContain('Source.xlsx');
  const unauthorized = await fetch(gatewayBase + '/mcp', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'resources/read', params: { uri: WORKBOOK_APP_URI } }) });
  expect(unauthorized.status).toBe(401);
  // These linked transports exercise the official protocol, not browser rendering.
  const transport = () => ({ async start() {}, async close() { this.onclose?.(); }, async send(message) { queueMicrotask(() => this.peer.onmessage?.(structuredClone(message))); } });
  const left = transport(), right = transport(); left.peer = right; right.peer = left;
  const bridge = new AppBridge(null, { name: 'Fixture host', version: '1.0.0' }, { serverTools: {}, openLinks: {} });
  const app = new App({ name: 'Financial review fixture', version: '1.0.0' }, {}, { autoResize: false });
  const c = createWorkbookMcpController({ call: request => app.callServerTool(request) });
  bridge.oncalltool = params => mcp(params.name, params.arguments, f.writer);
  let received; const delivered = new Promise(resolve => { received = resolve; });
  app.ontoolresult = result => { c.receive(result); received(); };
  try {
    await bridge.connect(left); await app.connect(right);
    const args = { binding: binding.id, key, release: pack.id };
    await bridge.sendToolInput({ arguments: args }); await bridge.sendToolResult(await mcp('workbook_review', args)); await delivered;
    const source = await c.inspect(1, 1); expect(source.cells[0].imported.value).toBe(20);
    await c.preview({ reviewed: [select(c.state.read.rows[0])], note: 'Reviewed in embedded fixture' });
    expect(c.state.preview.reviewed_after).toBe(1); await c.save();
    const opened = await readWorkbookReview(await f.store(binding).readVersion(key));
    expect((await projectReview(opened.snapshot, opened.review)).reviewed).toBe(1);
    const route = new URL(await c.editor()); expect((await f.raw(route.pathname + route.search)).status).toBe(303);
    await c.preview({ reopen: [1] }); await c.save(); expect(c.state.read.summary.reviewed).toBe(0);
    bridge.oncalltool = params => mcp(params.name, params.arguments, f.reader);
    await c.preview({ reopen: [1] }); await expect(c.save()).rejects.toMatchObject({ status: 403 });
    expect(c.state.uncertain).not.toBeNull(); await c.load();
    verified.add('official MCP Apps SDK handshake, resource discovery, source inspection, exact save/reopen, reader refusal and native editor continuation; rendering unverified');
  } finally { await app.close(); await bridge.close(); }
}, 60000);

test('account MCP exposes bounded review pages and original cell inspection without downloading XLSX', async () => {
  const tools = await rpc('tools/list', {}); expect(tools.tools.filter(t => t.name.startsWith('workbook_')).map(t => t.name).sort()).toEqual(['workbook_preview', 'workbook_review', 'workbook_update']);
  const result = await mcp('workbook_review', { binding: binding.id, key, limit: 1 });
  expect(result.isError).toBe(false); expect(result.structuredContent.summary).toMatchObject({ total: 3, reviewed: 0, missing_amounts: 1 });
  const read = result.structuredContent; expect(read.more).toBe(true); expect(read.rows).toHaveLength(1);
  wire.length = 0;
  const next = await invoke('workbook_review', { ...identity(read), offset: 1, limit: 1, inspect: { sheetId: 'sheet-0', row: 1, column: 1, rows: 1, columns: 1 } });
  expect(next.rows[0].description).toBe('Train'); expect(next.cells[0].imported.value).toBe(20);
  expect(wire.some(r => r.operation === 'blob-get' && r.prefix === original.source.sha256)).toBe(false);
  await expect(invoke('workbook_review', { binding: binding.id, key, offset: 1 })).rejects.toThrow('revision');
  verified.add('gateway tool discovery, bounded revision-bound pages and original-address inspection without original XLSX download');
}, 30000);

test('preview is read-only; exact gateway commit recalculates through Univer and reopens in the editor record', async () => {
  const before = await current(), args = { ...identity(before), edits: [{ row: 1, role: 'amount', value: 35.5 }, { row: 1, role: 'account', value: '=literal account' }] };
  wire.length = 0;
  const preview = await invoke('workbook_preview', args);
  expect(wire.every(r => r.method === 'GET')).toBe(true);
  expect((await current()).sha256).toBe(before.sha256);
  expect(preview.changed_cells.find(c => c.column === 3 && c.row === 1).after.value).toBeCloseTo(3.55, 8);
  const saved = await mcp('workbook_update', { ...args, preview_sha256: preview.preview_sha256, review_time: preview.review_time, request_id: 'workbook-edit' }, f.writer);
  expect(saved.isError).toBe(false); expect(saved.structuredContent.record.revision).toBe(before.revision + 1);
  const opened = await readWorkbookReview(await f.store(binding).readVersion(key));
  const cells = opened.snapshot.sheets['sheet-0'].cellData[1];
  expect(cells[1].v).toBe(35.5); expect(cells[1].f ?? null).toBeNull(); expect(cells[2].v).toBe('=literal account'); expect(cells[2].f ?? null).toBeNull();
  expect(await opened.source.original.arrayBuffer()).toEqual(await original.source.original.arrayBuffer());
  expect(await opened.source.baseline.text()).toBe(await original.source.baseline.text());
  verified.add('read-only preview and exact gateway save share Univer edits, recalculation and immutable sources with the editor');
}, 30000);

test('agent decisions and manual decisions continue each other and survive XLSX export', async () => {
  let read = await current();
  const args = { ...identity(read), reviewed: [select(read.rows[0])], note: 'Source checked through account review' };
  const preview = await invoke('workbook_preview', args);
  const saved = await invoke('workbook_update', { ...args, review_time: preview.review_time, preview_sha256: preview.preview_sha256, request_id: 'agent-decision' });
  expect(saved.reviewed).toBe(1);
  const opened = await readWorkbookReview(await f.store(binding).readVersion(key));
  const projection = await projectReview(opened.snapshot, opened.review);
  const manual = await markRowsReviewed(opened.snapshot, opened.review, [select(projection.rows[1])], 'Reviewed in workbook');
  await f.store(binding).saveLocal(key, workbookReviewValue(opened.snapshot, opened.source, manual), opened.revision);
  read = await current(); expect(read.summary.reviewed).toBe(2);
  const book = XLSX.read(exportWorkbookSnapshot(await withReviewSheet(opened.snapshot, manual)), { type: 'array' });
  expect(book.Sheets['Review decisions'].E2.v).toBe('Reviewed'); expect(book.Sheets['Review decisions'].E3.v).toBe('Reviewed');
  expect(book.Sheets['Review decisions'].G2.v).toBe(args.note);
  const reopenArgs = { ...identity(read), reopen: [1] }, reopen = await invoke('workbook_preview', reopenArgs);
  await invoke('workbook_update', { ...reopenArgs, review_time: reopen.review_time, preview_sha256: reopen.preview_sha256, request_id: 'reopen-decision' });
  expect((await current()).summary.reviewed).toBe(1);
  verified.add('manual and agent review decisions interoperate, reopen and export through the same record');
}, 30000);

test('invalid amounts, unresolved rows, oversized arguments and altered previews cannot save', async () => {
  const read = await current(), args = { ...identity(read), edits: [{ row: 1, role: 'amount', value: 10 }] };
  await expect(invoke('workbook_preview', { ...identity(read), reviewed: [select(read.rows[2])] })).rejects.toThrow('Amount needs review');
  for (const edits of [[{ row: 1, role: 'amount', value: 1.005 }], [{ row: 0, role: 'amount', value: 10 }], Array.from({ length: 33 }, () => ({ row: 1, role: 'amount', value: 10 }))]) await expect(invoke('workbook_preview', { ...identity(read), edits })).rejects.toThrow();
  await expect(invoke('workbook_review', { binding: binding.id, key, actor: 'someone-else' })).rejects.toThrow();
  const preview = await invoke('workbook_preview', args);
  await expect(invoke('workbook_update', { ...args, review_time: preview.review_time, preview_sha256: '0'.repeat(64), request_id: 'altered' })).rejects.toMatchObject({ status: 409 });
  await expect(invoke('workbook_update', { ...args, review_time: '2020-01-01T00:00:00.000Z', preview_sha256: preview.preview_sha256, request_id: 'expired' })).rejects.toMatchObject({ status: 409 });
  expect((await current()).sha256).toBe(read.sha256);
  verified.add('invalid cells, unresolved rows, excess inputs, actor injection, altered confirmation and expired preview refusal');
}, 30000);

test('competing saves, current permissions and inactive installations apply to workbook operations', async () => {
  const read = await current(), args = { ...identity(read), edits: [{ row: 2, role: 'account', value: 'Transport' }] };
  const preview = await invoke('workbook_preview', args, f.reader), update = { ...args, review_time: preview.review_time, preview_sha256: preview.preview_sha256, request_id: 'reader-refused' };
  await expect(invoke('workbook_update', update, f.reader)).rejects.toMatchObject({ status: 403 });
  await expect(invoke('workbook_review', { binding: binding.id, key }, f.other)).rejects.toMatchObject({ status: 403 });
  f.grant(f.writer.id, 'workspace-team', 'read');
  await expect(invoke('workbook_update', { ...update, request_id: 'revoked-writer' }, f.writer)).rejects.toMatchObject({ status: 403 });
  f.grant(f.writer.id, 'workspace-team', 'write');
  const opened = await readWorkbookReview(await f.store(binding).readVersion(key)); opened.snapshot.sheets['sheet-0'].cellData[1][1].v = 40;
  await f.store(binding).saveLocal(key, workbookReviewValue(opened.snapshot, opened.source, opened.review), opened.revision);
  await expect(invoke('workbook_update', { ...update, request_id: 'stale' })).rejects.toMatchObject({ status: 409 });
  await expect(invoke('workbook_review', { ...identity(read), offset: 1 })).rejects.toMatchObject({ status: 409 });
  await f.action('deactivate', 'experience.finance-workbooks.workspace', 'institution:workspace-team');
  await expect(current()).rejects.toMatchObject({ status: 404 });
  await f.action('activate', 'experience.finance-workbooks.workspace', 'institution:workspace-team');
  verified.add('competing edits, stale pages, read-only and revoked writers, unrelated users and inactive installations are refused by native owners');
}, 30000);

test('lost response after native commit retains request identity and never retries the write', async () => {
  const read = await current(), args = { ...identity(read), edits: [{ row: 2, role: 'account', value: 'Transport' }] }, preview = await invoke('workbook_preview', args);
  let posts = 0;
  const interrupted = createWorkspaceMcp({ ownerUrl: f.base, publicOrigin: f.base, request: async (url, options) => {
    const response = await fetch(url, options);
    if (options.method === 'POST' && url.searchParams.get('operation') === 'write') { posts++; await response.arrayBuffer(); throw Error('fixture lost response'); }
    return response;
  } });
  await expect(interrupted('workbook_update', { ...args, review_time: preview.review_time, preview_sha256: preview.preview_sha256, request_id: 'lost-workbook-response' }, session(f.owner))).rejects.toMatchObject({ status: 502, request_id: 'lost-workbook-response' });
  expect(posts).toBe(1); const reopened = await current(); expect(reopened.revision).toBe(read.revision + 1); expect(reopened.rows[1].account).toBe('Transport');
  verified.add('lost response after a real commit never auto-retries and is reconciled by current workbook read');
}, 30000);

test('existing scalar workspace edits and continuation still operate over the same financial record', async () => {
  const read = (await invoke('workspace_records', { binding: binding.id, key })).record;
  const args = { binding: binding.id, key, expected_revision: read.revision, sha256: read.sha256, edits: [{ path: ['title'], value: 'Reviewed workbook' }] };
  const preview = await invoke('workspace_preview', args);
  const saved = await invoke('workspace_update', { ...args, preview_sha256: preview.preview_sha256, request_id: 'generic-field-regression' });
  expect(saved.record.value.title).toBe('Reviewed workbook'); expect(saved.record.files).toEqual(read.files);
  const views = await mcp('workspace_views', { binding: binding.id, key, revision: saved.record.revision, release: pack.id });
  expect(views.isError).toBe(false); expect(views.structuredContent.views).toHaveLength(1);
  const route = new URL(views.structuredContent.views[0].url); expect((await f.raw(route.pathname + route.search)).status).toBe(303);
  verified.add('generic workspace preview/save preserves workbook files and native exact continuation');
}, 30000);

test('workbook concurrency is bounded and releases capacity after completed reads', async () => {
  let release, entered = 0, ready;
  const blocked = new Promise(resolve => { release = resolve; }), bothStarted = new Promise(resolve => { ready = resolve; });
  const limited = createWorkspaceMcp({ ownerUrl: f.base, publicOrigin: f.base, request: async (url, options) => {
    if (url.searchParams.get('operation') === 'get') { if (++entered === 2) ready(); await blocked; }
    return fetch(url, options);
  } });
  const args = { binding: binding.id, key }, pending = [limited('workbook_review', args, session(f.owner)), limited('workbook_review', args, session(f.owner))];
  await bothStarted;
  try { await expect(limited('workbook_review', args, session(f.owner))).rejects.toMatchObject({ status: 503 }); }
  finally { release(); }
  expect(await Promise.all(pending)).toHaveLength(2);
  expect((await limited('workbook_review', args, session(f.owner))).rows).toHaveLength(3);
  verified.add('two-operation workbook concurrency bound and capacity recovery after completion');
}, 30000);

// The page inlines two large minified bundles. Every inline script must parse:
// a string replacement once turned $' $` $& inside AG Grid into page text and
// the host saw a page whose scripts threw before connecting.
test('every inline script in the built financial review page parses', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'workbook-app-parse-'));
  try {
    const { path } = await buildWorkbookMcpApp(join(dir, 'review.html'));
    const html = readFileSync(path, 'utf8');
    const scripts = [...html.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/g)];
    expect(scripts.length).toBeGreaterThanOrEqual(2);
    for (const [index, [, attributes, body]] of scripts.entries()) {
      const file = join(dir, `script-${index}.${/type="module"/.test(attributes) ? 'mjs' : 'js'}`);
      writeFileSync(file, body);
      const checked = Bun.spawnSync(['node', '--check', file], { stderr: 'pipe' });
      expect({ index, error: checked.exitCode === 0 ? null : checked.stderr.toString().split('\n').slice(0, 4).join('\n') }).toEqual({ index, error: null });
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
