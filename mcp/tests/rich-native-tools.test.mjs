import { expect, test } from 'bun:test';
import { mkdtemp, mkdir, writeFile, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { createNativeRichTools } from '../rich-native-tools.mjs';
import { openLedger } from '../../integrations/mcp/recursive_overlay.mjs';

const root = await mkdtemp(join(tmpdir(), 'bonfyre-mcp-rich-native-'));
const path = join(root, 'mesh.db');
const db = new Database(path);
db.exec('CREATE TABLE calls(id TEXT, ts REAL, trace_id TEXT, tool TEXT, args_json TEXT, state TEXT, duration_ms INTEGER, result_json TEXT)');
db.exec('CREATE TABLE deltas(seq INTEGER, ts REAL, source TEXT, head TEXT, status_hash TEXT, payload_json TEXT)');
db.exec('CREATE TABLE events(seq INTEGER, ts REAL, kind TEXT, trace_id TEXT, payload_json TEXT)');
db.exec('CREATE TABLE tasks(id TEXT, ts REAL, kind TEXT, name TEXT, unit_name TEXT, state TEXT, log_path TEXT)');
db.query('INSERT INTO calls VALUES (?,?,?,?,?,?,?,?)').run('a', 1, 'trace', 'probe', '{}', 'passed', 4, '{"ok":true}');
db.query('INSERT INTO deltas VALUES (?,?,?,?,?,?)').run(1, 1, 'source', 'head', 'hash', '{"source":"source"}');
db.close();

test('native rich reads preserve legacy mesh ledger shape', async () => {
  const call = createNativeRichTools({ env: { BONFYRE_MCP_DB: path } });
  expect((await call('mcp_call_history', { tool: 'probe' })).calls[0].result.ok).toBe(true);
  expect((await call('mcp_call_history', { tool: 'other' })).calls).toEqual([]);
  expect((await call('project_recent_deltas')).deltas[0].payload.source).toBe('source');
  await expect(call('mcp_call_history', { limit: 1.5 })).rejects.toThrow('integer');
});

test('native rich Atlas and runtime stay behind exact operation gates', async () => {
  const commands = [], requests = [];
  const call = createNativeRichTools({
    env: { BONFYRE_MCP_DB: path, BONFYRE_ATLAS_BIN: join(process.cwd(), 'cmd/BonfyreAtlas/bonfyre-atlas'), BONFYRE_RUNTIME_URL: 'http://localhost:3045' },
    previewPaths: ['/api/bonfyre-mcp/graphs/preview'],
    run: async argv => { commands.push(argv); return { exit_code: 0, stdout: 'ok', stderr: '', duration_ms: 1 }; },
    request: async (url, options) => { requests.push([url, options]); return new Response('{"ok":true}', { status: 200 }); },
  });
  expect((await call('architecture_atlas', { op: 'cannot_infer', arg: 'machine-membrane' })).stdout).toBe('ok');
  expect(commands[0].slice(-2)).toEqual(['cannot-infer', 'machine-membrane']);
  expect((await call('runtime_get', { path: '/health' })).result.ok).toBe(true);
  expect((await call('runtime_preview', { path: '/api/bonfyre-mcp/graphs/preview', body: { scope: 'public' } })).status).toBe(200);
  expect(requests[1][1].method).toBe('POST');
  await expect(call('runtime_get', { path: '/admin' })).rejects.toThrow('runtime path');
  await expect(call('runtime_preview', { path: '/api/execute' })).rejects.toThrow('not allowlisted');
});

test('native measurement resolves a retired Python declaration to the installed Bun runner', async () => {
  const cms = join(root, 'cms.db'), scripts = join(root, 'scripts');
  await mkdir(scripts);
  await writeFile(join(scripts, 'funding-closure.mjs'), '// native fixture');
  const store = new Database(cms);
  store.exec('CREATE TABLE measurement_surface(measure TEXT, runner TEXT, subject_flag TEXT, subject_kind TEXT, effect_class TEXT, answers TEXT, cannot_infer TEXT, status TEXT)');
  const insert = store.query('INSERT INTO measurement_surface VALUES (?,?,?,?,?,?,?,?)');
  insert.run('funding-closure', 'funding_closure.py', '--institution', 'institution', 'PURE', 'residual', 'not an approval', 'published');
  insert.run('unsafe', '../evil.py', '', '', 'QUERY', '', 'never execute path', 'published');
  insert.run('effectful', 'funding_closure.py', '', '', 'EFFECT', '', 'not a read', 'published');
  store.close();
  const commands = [];
  const call = createNativeRichTools({ env: { BONFYRE_ROOT: root, BONFYRE_CMS_DB: cms, BONFYRE_MCP_DB: path }, run: async (argv, opts) => { commands.push([argv, opts]); return { stdout: '{"requirements":3}', stderr: '', exit_code: 0 }; } });
  const list = await call('institutional_measure', { op: 'list' });
  expect(list.measurements.find(item => item.measure === 'funding-closure').runnable_here).toBe(true);
  expect(list.measurements.find(item => item.measure === 'unsafe').runnable_here).toBe(false);
  expect((await call('institutional_measure', { op: 'run', measure: '../evil' })).error).toBe('unknown_measure');
  expect((await call('institutional_measure', { op: 'run', measure: 'effectful' })).error).toBe('not_readonly');
  expect((await call('institutional_measure', { op: 'run', measure: 'unsafe' })).error).toBe('unsafe_runner');
  expect((await call('institutional_measure', { op: 'run', measure: 'funding-closure', subject: 'x;id' })).error).toBe('unsafe_subject');
  const value = await call('institutional_measure', { op: 'run', measure: 'funding-closure', subject: 'institution:nightgrid' });
  expect(value.receipt.requirements).toBe(3);
  expect(value.cannot_infer).toBe('not an approval');
  expect(commands[0][0]).toEqual(['bun', join(scripts, 'funding-closure.mjs'), '--institution', 'institution:nightgrid', '--json']);
});

test('native carrier and finance reads preserve one source without claiming a sale', async () => {
  const finance = join(root, 'finance.db');
  const store = new Database(finance);
  store.exec('CREATE TABLE services(name TEXT, category TEXT, buy_price REAL, sell_price REAL, source TEXT)');
  store.query('INSERT INTO services VALUES (?,?,?,?,?)').run('school-map', 'public', 1, 3, 'book');
  store.query('INSERT INTO services VALUES (?,?,?,?,?)').run('unpriced', 'public', 1, 0, 'book');
  store.close();
  const urls = [], commands = [];
  const call = createNativeRichTools({
    env: { BONFYRE_FINANCE_DB: finance, BONFYRE_SURFACE: 'https://aurekai.life' },
    protocolVariants: { ogc_items: '/ogc/collections/parcels/items', activitypub_outbox: '/ap/outbox' },
    excludedCarriers: ['mcp'],
    request: async url => { urls.push(url); return new Response('{"capabilities":[]}', { status: 200 }); },
    run: async argv => { commands.push(argv); return { exit_code: 0, stdout: '{}', stderr: '', duration_ms: 1 }; },
  });
  const book = await call('finance_book');
  expect(book.book.rows.length).toBe(2);
  expect(book.book.rows.find(row => row.name === 'unpriced').status).toBe('not_for_sale');
  expect(book.x402_executable_catalogue.stdout).toContain('capabilities');
  expect((await call('protocol_fetch', { carrier: 'MCP' })).error).toBe('unknown carrier');
  expect((await call('protocol_fetch', { carrier: 'ogc_items' })).stdout).toContain('__STATUS__200');
  await call('protocol_fetch', { carrier: 'x402', name: 'school-map' });
  expect(urls.at(-1)).toBe('https://aurekai.life/capability/school-map');
  expect((await call('desk_open_decisions')).stdout).not.toContain('__STATUS__');
  await call('membrane_state');
  expect(commands[0]).toEqual(['/usr/local/bin/bonfyre-membrane', 'summary']);
});

test('Oracle native family enforces authored allowlists and preserves run receipts', async () => {
  const cmdRoot = join(root, 'cmd');
  await mkdir(join(cmdRoot, 'BonfyreProbe'), { recursive: true });
  const binary = join(cmdRoot, 'BonfyreProbe', 'bonfyre-probe');
  await writeFile(binary, 'fixture', { mode: 0o755 });
  const profilePath = join(root, 'output/phase94-hypercycle');
  await mkdir(profilePath, { recursive: true });
  await writeFile(join(profilePath, 'profile-run-receipts.jsonl'), '{"profile":"safe","state":"failed"}\nnot json\n{"profile":"safe","state":"success"}\n');
  await mkdir(join(root, 'tools'), { recursive: true });
  await writeFile(join(root, 'tools', 'flow.ts'), 'fixture');
  const receipts = [];
  const call = createNativeRichTools({
    env: { BONFYRE_ROOT: root, BONFYRE_CMD_ROOT: cmdRoot, BONFYRE_RUNTIME_URL: 'http://127.0.0.1:3045' },
    oraclePolicy: { profiles: ['safe'], flows: { safe_flow: 'flow.ts' }, units: ['bonfyred.service'] },
    run: async (argv, options) => {
      receipts.push({ argv, options });
      const stdout = argv[0] === 'file' ? 'ELF 64-bit LSB executable' : argv.includes('show') ? 'Result=success\nExecMainStatus=0' : '';
      return { exit_code: 0, stdout, stderr: '', duration_ms: 1 };
    },
  });
  await expect(call('oracle_service_status', { unit: 'unknown.service' })).rejects.toThrow('allowlisted');
  expect((await call('oracle_service_status', { unit: 'bonfyred.service', journal_lines: 0 })).journal).toBeNull();
  await expect(call('oracle_profile_run', { profile: 'other' })).rejects.toThrow('allowlisted');
  expect((await call('oracle_profile_run', { profile: 'safe' })).state).toBe('success');
  expect((await call('oracle_profile_receipts', { state: 'success' })).records).toEqual([{ profile: 'safe', state: 'success' }]);
  await expect(call('oracle_command_probe', { family: '../Probe' })).rejects.toThrow('invalid command family');
  expect((await call('oracle_command_probe', { family: 'BonfyreProbe', mode: 'help' })).argv).toEqual([binary, '--help']);
  expect((await call('oracle_command_describe', { family: 'BonfyreProbe' })).binary).toBe(binary);
  await expect(call('oracle_deep_flow', { flow: 'unsafe' })).rejects.toThrow('allowlisted');
  expect((await call('oracle_deep_flow', { flow: 'safe_flow' })).argv).toEqual(['bun', join(root, 'tools', 'flow.ts')]);
  const flowRun = receipts.at(-1);
  expect(flowRun.options.env.BONFYRE_EXTERNAL_WRITES_ENABLED).toBe('0');
  expect(flowRun.options.env.BONFYRE_LIVE_APPLY_ENABLED).toBe('0');
});

test('native artifacts are bounded to output even through symlinks', async () => {
  const output = join(root, 'output');
  await writeFile(join(output, 'receipt.json'), '{"proof":true}');
  const secret = join(root, 'outside.txt');
  await writeFile(secret, 'private');
  await symlink(secret, join(output, 'escape.txt'));
  const call = createNativeRichTools({ env: { BONFYRE_ROOT: root } });
  expect((await call('artifact_search', { name: 'receipt', contains: 'proof' })).matches[0].relative).toBe('receipt.json');
  expect((await call('artifact_read', { path: 'receipt.json', max_bytes: 3 })).text).toBe('{"p');
  await expect(call('artifact_read', { path: '../outside.txt' })).rejects.toThrow('escapes');
  await expect(call('artifact_read', { path: 'escape.txt' })).rejects.toThrow('escapes');
  expect((await call('artifact_search', { name: 'escape' })).matches).toEqual([]);
});

test('foreign and finance effects stay on native owning organs with typed arguments', async () => {
  const invocations = [];
  const call = createNativeRichTools({
    env: { BONFYRE_FINANCE_DB: '/var/lib/bonfyre/state/finance.db' },
    run: async (argv, options) => { invocations.push({ argv, options }); return { exit_code: 0, stdout: 'receipt', stderr: '', duration_ms: 1 }; },
  });
  expect((await call('foreign_discover', { host: 'x;touch /tmp/pwn' })).error).toContain('host must');
  expect((await call('sitemap_delta', { sitemap_url: 'file:///etc/passwd' })).error).toContain('http(s)');
  expect((await call('finance_price_set', { name: 'service', buy_usd: -1, sell_usd: 2 })).error).toContain('negative');
  expect((await call('finance_price_set', { name: 'service', sell_usd: 2 })).error).toContain('numbers');
  await call('foreign_discover', { host: 'example.org' });
  await call('sitemap_delta', { from_robots_host: 'example.org' });
  await call('finance_price_set', { name: 'school-map', buy_usd: 1.3, sell_usd: 2.4 });
  expect(invocations.map(x => x.argv[0])).toEqual(['/usr/local/bin/bonfyre-membrane', '/usr/local/bin/bonfyre-sitemap', '/usr/local/bin/bonfyre-finance']);
  expect(invocations.at(-1).options.env.BONFYRE_FINANCE_DB).toBe('/var/lib/bonfyre/state/finance.db');
  expect(invocations.at(-1).argv).toContain('2.40');
});

test('Studio writes share the entity, Fleming WorkGraph, and economy stores', async () => {
  const calls = [];
  const call = createNativeRichTools({
    env: { BONFYRE_STATE_ROOT: '/var/lib/bonfyre/state' },
    run: async (argv, options) => { calls.push({ argv, options }); return { exit_code: 0, stdout: 'ok', stderr: '', duration_ms: 1 }; },
  });
  expect((await call('studio_client_register', { slug: 'x;id' })).error).toContain('slug');
  expect((await call('studio_fleming_watch', { account: '../a' })).error).toContain('account');
  expect((await call('studio_work_complete', { mission: 'm', node: 'n', worker: 'w', token: 'short' })).error).toContain('64-hex');
  await call('studio_client_register', { slug: 'school.alpha' });
  await call('studio_fleming_watch', { account: '123-45' });
  await call('studio_work_claim', { worker: 'worker.1' });
  await call('studio_work_complete', { mission: 'm', node: 'n', worker: 'w', token: 'a'.repeat(64) });
  await call('studio_fleming_opportunity', { account: '123-45' });
  await call('studio_membrane_budget_set', { recipe: 'r', usd_cap: 1.2 });
  await call('studio_economy_cost_record', { recipe: 'r', stage: 's', model: 'm', usd: 0.123456 });
  expect(calls).toHaveLength(7);
  expect(calls[0].options.env.BONFYRE_ENTITY_DB).toBe('/var/lib/bonfyre/state/entity.db');
  expect(calls[3].argv).toContain('/var/lib/bonfyre/state/fleming/fleming-work.db');
  expect(calls[5].options.env.BONFYRE_ECONOMY_DB).toBe('/var/lib/bonfyre/state/economy.db');
  expect(calls[6].argv).toContain('0.123456');
});

test('Studio read projections preserve hot-over-cold Fleming composition', async () => {
  const calls = [];
  const call = createNativeRichTools({
    env: { BONFYRE_STATE_ROOT: '/var/lib/bonfyre/state' }, flemingProjections: ['estate', 'coverage', 'markets', 'owners'],
    run: async argv => { calls.push(argv); return { exit_code: 0, stdout: 'rows', stderr: '', duration_ms: 1 }; },
  });
  await call('studio_client_list');
  await call('studio_membrane_status');
  await call('studio_fleming_query', { projection: 'estate' });
  expect(calls).toHaveLength(3);
  expect(calls.every(argv => argv[0] === '/usr/local/bin/bonfyre-query')).toBe(true);
  expect(calls[0][3]).toContain('e.entities');
  expect(calls[1][3]).toContain('ec.budgets');
  expect(calls[2][3]).toContain('src_id NOT IN (SELECT DISTINCT src_id FROM hot.observation)');
  expect(calls[2][3]).toContain('parquet_scan');
  expect((await call('studio_fleming_query', { projection: 'arbitrary' })).error).toContain('unknown projection');
});

test('mesh and release attestations gather live owners without promoting a status to proof', async () => {
  const release = join(root, 'release');
  const cmdRoot = join(root, 'attest-cmd');
  await mkdir(join(release, 'cmd'), { recursive: true });
  await writeFile(join(release, 'sha256-manifest.txt'), 'fixture');
  await writeFile(join(release, 'cmd', 'test-elf'), 'fixture', { mode: 0o755 });
  for (const family of ['BonfyreQwenFPQ', 'BonfyreMoE', 'BonfyreEmbed', 'BonfyreTranscribe']) {
    await mkdir(join(cmdRoot, family), { recursive: true });
    await writeFile(join(cmdRoot, family, 'binary'), 'fixture', { mode: 0o755 });
  }
  const commands = [];
  const call = createNativeRichTools({
    env: { BONFYRE_ROOT: root, BONFYRE_CMD_ROOT: cmdRoot, BONFYRE_MCP_DB: path, BONFYRE_RELEASE_CURRENT: release }, catalogueToolCount: 53,
    projectCall: async name => ({ name, current: true }),
    run: async argv => { commands.push(argv); return { exit_code: 0, stdout: argv[0] === 'file' ? 'ELF 64-bit LSB executable' : 'active', stderr: '', duration_ms: 1 }; },
    request: async () => new Response('{"status":"ok"}', { status: 200 }),
  });
  const mesh = await call('mesh_status');
  expect(mesh.project.current).toBe(true);
  expect(mesh.database.calls).toBe(1);
  expect(mesh.tool_count).toBe(53);
  const attest = await call('release_attest');
  expect(attest.command_elfs).toBe(1);
  expect(attest.manifest).toBe(true);
  expect(attest.runtime_health).toEqual([200, { status: 'ok' }]);
  expect(Object.keys(attest.portable_dependencies)).toHaveLength(4);
  expect(commands.some(argv => argv[0] === 'ldd')).toBe(true);
});

test('native task handles persist a systemd receipt and refuse unapproved work', async () => {
  const tasks = join(root, 'native-tasks');
  const ledger = openLedger(join(root, 'native-task-ledger.db'));
  const commands = [];
  const call = createNativeRichTools({
    env: { BONFYRE_ROOT: root, BONFYRE_MCP_TASK_DIR: tasks }, ledger,
    oraclePolicy: { profiles: ['safe'], flows: { safe_flow: 'flow.ts' } },
    run: async argv => { commands.push(argv); return { exit_code: 0, stdout: 'ok', stderr: '', duration_ms: 1 }; },
  });
  await expect(call('task_start', { kind: 'flow', name: 'unknown' })).rejects.toThrow('allowlisted');
  const started = await call('task_start', { kind: 'flow', name: 'safe_flow' }, 'trace-task');
  expect(started.state).toBe('started');
  expect(commands[0]).toContain('BONFYRE_EXTERNAL_WRITES_ENABLED=0');
  expect(commands[0]).toContain('BONFYRE_LIVE_APPLY_ENABLED=0');
  expect((await call('task_status', { task_id: started.task_id })).task.name).toBe('safe_flow');
  await expect(call('task_status', { task_id: '../wrong' })).rejects.toThrow('invalid task id');
  expect(ledger.events().events.some(event => event.kind === 'task.started')).toBe(true);
  ledger.close();
});

test('local model generation never falls through to a remote provider', async () => {
  const requests = [];
  const env = { BONFYRE_ROOT: root, BONFYRE_MODEL_STATE_ROOT: join(root, 'model-state'), BONFYRE_ORGANS_DIR: join(root, 'absent-organs'), BONFYRE_LOCAL_MODEL_URL: 'http://127.0.0.1:8088' };
  const down = createNativeRichTools({ env, run: async () => ({ exit_code: 0, stdout: '', stderr: '', duration_ms: 1 }), request: async url => { requests.push(url); throw new Error('down'); } });
  expect((await down('model_local_generate', { prompt: 'hello' })).error).toBe('local_path_incomplete');
  expect(requests).toEqual(['http://127.0.0.1:8088/health']);
  const up = createNativeRichTools({ env, run: async () => ({ exit_code: 0, stdout: '', stderr: '', duration_ms: 1 }), request: async (url, options) => {
    requests.push(url);
    return url.endsWith('/health') ? new Response('{"status":"ok"}') : new Response('{"choices":[{"message":{"content":"local answer"}}],"usage":{"output_tokens":2}}');
  } });
  const value = await up('model_local_generate', { prompt: 'hello', max_tokens: 20 });
  expect(value.text).toBe('local answer');
  expect(value.served_by).toBe('http://127.0.0.1:8088');
  expect(requests.at(-1)).toBe('http://127.0.0.1:8088/v1/chat/completions');
});
