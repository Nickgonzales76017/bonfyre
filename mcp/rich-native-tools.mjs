#!/usr/bin/env bun
// Cutovers from the rich Python mesh. Exact query/read owners remain unchanged.
import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { join, resolve, relative, isAbsolute } from 'node:path';
import { stat, readdir, readFile, realpath, mkdir } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { nativeRun } from '../integrations/mcp/recursive_overlay_tools.mjs';
import { boundedPipe } from '../integrations/mcp/bounded_pipe.mjs';
import { admitEstateSql } from './estate-sql-guard.mjs';

const limit = (value, fallback, max) => {
  const n = Number(value ?? fallback);
  if (!Number.isInteger(n)) throw new Error('limit must be an integer');
  return Math.max(1, Math.min(n, max));
};
const readJson = async response => {
  const text = await response.text();
  try { return JSON.parse(text); } catch { return { text }; }
};

export function createNativeRichTools({
  env = process.env,
  run = nativeRun,
  request = fetch,
  projectCall = async () => { throw new Error('project owner unavailable'); },
  ledger,
  catalogueToolCount = 0,
  previewPaths = [],
  protocolVariants = {},
  excludedCarriers = ['mcp'],
  oraclePolicy = {},
  flemingProjections = [],
} = {}) {
  const atlas = env.BONFYRE_ATLAS_BIN || join(env.BONFYRE_ROOT || '/opt/bonfyre', 'cmd', 'BonfyreAtlas', 'bonfyre-atlas');
  const runtime = (env.BONFYRE_RUNTIME_URL || 'http://127.0.0.1:3045').replace(/\/$/, '');
  const dbPath = env.BONFYRE_MCP_DB || '/var/lib/bonfyre/mcp/mesh.sqlite3';
  const cmsDb = env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db';
  const scripts = join(env.BONFYRE_ROOT || '/opt/bonfyre', 'scripts');
  const financeDb = env.BONFYRE_FINANCE_DB || '/var/lib/bonfyre/state/finance.db';
  const previews = new Set(previewPaths);
  const carrierRegistry = JSON.parse(readFileSync(new URL('../integrations/mcp/carriers_contract.json', import.meta.url), 'utf8'));
  const carrierPaths = Object.fromEntries(carrierRegistry.carriers.filter(item => !excludedCarriers.includes(item.name.toLowerCase())).map(item => [item.name.toLowerCase(), item.path]));
  Object.assign(carrierPaths, protocolVariants);
  const surface = (env.BONFYRE_SURFACE || 'https://aurekai.life').replace(/\/$/, '');
  const root = env.BONFYRE_ROOT || '/opt/bonfyre';
  const commandRoot = env.BONFYRE_CMD_ROOT || '/opt/cmd';
  const releasePath = env.BONFYRE_RELEASE_CURRENT || '/opt/bonfyre/releases/command-plane-current';
  const taskDir = env.BONFYRE_MCP_TASK_DIR || '/var/lib/bonfyre/mcp/tasks';
  const stateRoot = env.BONFYRE_STATE_ROOT || '/var/lib/bonfyre/state';
  const localModel = (env.BONFYRE_LOCAL_MODEL_URL || 'http://127.0.0.1:8088').replace(/\/$/, '');
  const workDb = join(stateRoot, 'fleming/fleming-work.db');
  const duckdbHome = env.BONFYRE_DUCKDB_HOME || join(stateRoot, 'duckdb');
  const flemingRoot = join(stateRoot, 'fleming');
  const flemingQueries = JSON.parse(readFileSync(new URL('./fleming-projections.json', import.meta.url), 'utf8'));
  const allowedFleming = new Set(flemingProjections);
  const sqlLiteral = path => {
    if (!/^[A-Za-z0-9/_.:-]+$/.test(path)) throw new Error('unsafe native query path');
    return `'${path}'`;
  };
  const sqlBoot = `SET home_directory=${sqlLiteral(duckdbHome)}; INSTALL sqlite; LOAD sqlite; `;
  const estateViewSql = inner => `${sqlBoot}ATTACH ${sqlLiteral(join(flemingRoot, 'estate-lite.db'))} AS hot (TYPE sqlite, READ_ONLY); CREATE OR REPLACE TEMP VIEW observation AS SELECT * FROM parquet_scan(${sqlLiteral(join(flemingRoot, 'estate.parquet/observation.parquet'))}) WHERE src_id NOT IN (SELECT DISTINCT src_id FROM hot.observation) UNION ALL SELECT * FROM hot.observation; CREATE OR REPLACE TEMP VIEW page AS SELECT * FROM hot.page; CREATE OR REPLACE TEMP VIEW parcel_flat AS SELECT * FROM hot.parcel_flat; ${inner}`;
  const ident = value => /^[A-Za-z0-9._-]{1,96}$/.test(value);
  const account = value => /^[A-Za-z0-9-]{1,32}$/.test(value);
  const money = (value, field) => {
    if (value === undefined || value === null || value === '') return { error: `${field} must be a number` };
    const number = Number(value);
    if (!Number.isFinite(number)) return { error: `${field} must be a number` };
    if (number < 0 || number > 1_000_000) return { error: `${field} must be between 0 and 1000000` };
    return number;
  };
  const profileNames = new Set(oraclePolicy.profiles || []);
  const flowFiles = oraclePolicy.flows || {};
  const unitNames = new Set([...(oraclePolicy.units || []), ...[...profileNames].flatMap(profile => [`bonfyre-hypercycle@${profile}.service`, `bonfyre-hypercycle-${profile}.timer`])]);
  const runReceipt = async (argv, timeoutMs, options = {}) => ({ argv, ...await run(argv, { timeoutMs, ...options }) });
  async function commandBinary(family) {
    if (!/^Bonfyre[A-Za-z0-9_-]+$/.test(family)) throw new Error('invalid command family');
    const directory = join(commandRoot, family);
    if (!(await stat(directory).catch(() => null))?.isDirectory()) throw new Error(`command family not found: ${family}`);
    const candidates = [];
    async function visit(path) {
      for (const item of await readdir(path, { withFileTypes: true })) {
        const child = join(path, item.name);
        if (item.isDirectory()) await visit(child);
        else if (item.isFile()) {
          if (!((await stat(child)).mode & 0o111)) continue;
          const kind = await runReceipt(['file', '-b', child], 5_000);
          if (kind.stdout.includes('ELF 64-bit LSB')) candidates.push(child);
        }
      }
    }
    await visit(directory);
    candidates.sort((a, b) => a.split('/').length - b.split('/').length || a.localeCompare(b));
    if (!candidates.length) throw new Error(`no Linux ELF executable found in ${family}`);
    return candidates[0];
  }
  const positive = (value, fallback, max, min = 0) => Math.min(max, Math.max(min, Number.isInteger(Number(value ?? fallback)) ? Number(value ?? fallback) : fallback));
  async function profileReceipts(args) {
    const path = join(root, 'output/phase94-hypercycle/profile-run-receipts.jsonl');
    const contents = await readFile(path, 'utf8').catch(() => '');
    const records = [];
    for (const line of contents.split('\n')) {
      let row;
      try { row = JSON.parse(line); } catch { continue; }
      if (args.profile && row.profile !== String(args.profile)) continue;
      if (args.state && row.state !== String(args.state)) continue;
      records.push(row);
    }
    return { path, records: records.slice(-limit(args.limit, 200, 2000)) };
  }
  const outputRoot = join(root, 'output');
  async function artifactPath(userPath) {
    const output = await realpath(outputRoot);
    const selected = resolve(isAbsolute(userPath) ? userPath : join(output, userPath));
    const candidate = await realpath(selected);
    const inside = relative(output, candidate);
    if (inside === '..' || inside.startsWith('../') || isAbsolute(inside)) throw new Error('artifact path escapes output root');
    return candidate;
  }
  async function artifactSearch(args) {
    const name = String(args.name ?? '').toLowerCase();
    const contains = String(args.contains ?? '').toLowerCase();
    const count = limit(args.limit, 200, 2000);
    const output = await realpath(outputRoot);
    const matches = [];
    async function visit(directory) {
      for (const item of await readdir(directory, { withFileTypes: true })) {
        if (matches.length >= count) return;
        const path = join(directory, item.name);
        if (item.isDirectory()) { await visit(path); continue; }
        if (!item.isFile()) continue;
        const local = relative(output, path);
        if (name && !local.toLowerCase().includes(name)) continue;
        const size = (await stat(path)).size;
        if (contains) {
          if (size > 5_000_000) continue;
          if (!(await readFile(path, 'utf8')).toLowerCase().includes(contains)) continue;
        }
        matches.push({ path, relative: local, size });
      }
    }
    await visit(output);
    return { matches };
  }
  const dbCounts = () => {
    const db = new Database(dbPath, { readonly: true, strict: true });
    try { return Object.fromEntries(['calls', 'deltas', 'events', 'tasks'].map(table => [table, db.query(`SELECT count(*) AS n FROM ${table}`).get().n])); }
    finally { db.close(); }
  };
  async function releaseAttest() {
    const current = await realpath(releasePath).catch(() => resolve(releasePath));
    const directory = (await stat(current).catch(() => null))?.isDirectory() || false;
    const checks = { current, exists: directory };
    if (directory) {
      let commandElfs = 0;
      async function scan(path) {
        for (const item of await readdir(path, { withFileTypes: true })) {
          const child = join(path, item.name);
          if (item.isDirectory()) await scan(child);
          else if (item.isFile() && ((await stat(child)).mode & 0o111)) {
            const info = await runReceipt(['file', '-b', child], 5_000);
            if (info.stdout.includes('ELF 64-bit LSB')) commandElfs++;
          }
        }
      }
      if ((await stat(join(current, 'cmd')).catch(() => null))?.isDirectory()) await scan(join(current, 'cmd'));
      checks.command_elfs = commandElfs;
      checks.manifest = (await stat(join(current, 'sha256-manifest.txt')).catch(() => null))?.isFile() || false;
    }
    checks.bonfyred = await runReceipt(['file', env.BONFYRE_RUNTIME_BINARY || '/opt/bonfyre/bonfyred'], 5_000);
    try { const health = await request(`${runtime}/health`, { signal: AbortSignal.timeout(10_000) }); checks.runtime_health = [health.status, await readJson(health)]; }
    catch (error) { checks.runtime_health = [0, { error: String(error) }]; }
    checks.timers = Object.fromEntries(await Promise.all(['bonfyre-continuation-loop.timer', 'bonfyre-autonomous-worker-loop.timer'].map(async unit => [unit, await runReceipt(['systemctl', 'is-active', unit], 10_000)])));
    checks.portable_dependencies = Object.fromEntries(await Promise.all(['BonfyreQwenFPQ', 'BonfyreMoE', 'BonfyreEmbed', 'BonfyreTranscribe'].map(async family => [family, await runReceipt(['ldd', await commandBinary(family)], 15_000)])));
    return checks;
  }
  async function modelInventory() {
    const organRoot = env.BONFYRE_ORGANS_DIR || '/usr/local/bin';
    const organs = Object.fromEntries(await Promise.all([['qwen-fpq', 'bonfyre-qwen-fpq'], ['moe', 'bonfyre-moe'], ['embed', 'bonfyre-embed'], ['transcribe', 'bonfyre-transcribe']].map(async ([name, file]) => {
      const binary = join(organRoot, file), info = await stat(binary).catch(() => null);
      return [name, { binary, present: Boolean(info?.isFile() && (info.mode & 0o111)) }];
    })));
    const catalogueSql = `${sqlBoot}ATTACH ${sqlLiteral(join(stateRoot, 'models.db'))} AS m (TYPE sqlite, READ_ONLY); SELECT id, format, size_mb, fpq_size_mb FROM m.models ORDER BY size_mb DESC LIMIT 40`;
    const catalogue = await runReceipt(['/usr/local/bin/bonfyre-query', 'sql', ':memory:', catalogueSql], 60_000);
    const generation = [], other = [];
    async function scan(path, depth) {
      if (depth > 3) return;
      const items = await readdir(path, { withFileTypes: true }).catch(() => []);
      for (const item of items) {
        const file = join(path, item.name);
        if (item.isDirectory()) { if (depth < 3) await scan(file, depth + 1); continue; }
        if (!item.isFile() || !/\.(gguf|onnx|fpq)$/i.test(item.name)) continue;
        const size = (await stat(file).catch(() => null))?.size || 0;
        if (size <= 10_000_000) continue;
        const group = /\.(gguf|fpq)$/i.test(item.name) && !file.includes('/voices/') ? generation : other;
        group.push({ path: file, mb: Math.round(size / 104857.6) / 10 });
      }
    }
    await Promise.all([root, env.BONFYRE_MODEL_STATE_ROOT || '/var/lib/bonfyre'].map(path => scan(path, 0)));
    const generationWeights = generation.sort((a, b) => b.mb - a.mb).slice(0, 20);
    const otherFiles = other.sort((a, b) => b.mb - a.mb).slice(0, 10);
    let serviceOk = false;
    try { const health = await request(`${localModel}/health`, { signal: AbortSignal.timeout(8_000) }); serviceOk = health.ok && JSON.stringify(await readJson(health)).includes('"ok"'); }
    catch { /* resident service absent */ }
    let llamaCli = env.BONFYRE_LLAMA_CLI_BIN || null;
    if (!llamaCli) for (const dir of String(env.PATH || process.env.PATH || '').split(':')) {
      const path = join(dir, 'llama-cli'), info = await stat(path).catch(() => null);
      if (info?.isFile() && (info.mode & 0o111)) { llamaCli = path; break; }
    }
    organs.moe.backend = { needs: 'llama-cli', found: llamaCli, satisfied: Boolean(llamaCli) };
    organs['qwen-fpq'].backend = { needs: 'an FPQ pack (--pack / BONFYRE_QWEN_PACK), not a raw gguf', satisfied: Boolean(env.BONFYRE_QWEN_PACK) };
    const blockers = [];
    if (!generationWeights.length) blockers.push('no generation weights on disk');
    if (!serviceOk) blockers.push(`the resident local model service at ${localModel} is not answering health; systemctl status bonfyre-local-model`);
    if (!env.BONFYRE_QWEN_PACK && organs['qwen-fpq'].present) blockers.push('bonfyre-qwen-fpq has no FPQ pack configured (it needs an FPQ pack, not a gguf)');
    if (generationWeights.some(item => item.path.endsWith('.gguf')) && llamaCli && organs.moe.present) blockers.push('bonfyre-moe wrapper/backend compatibility has not been proven; generation uses the resident service');
    return { organs, catalogue, generation_weights: generationWeights, other_model_files: otherFiles, local_service: { url: localModel, healthy: serviceOk }, runnable: serviceOk, blockers, note: 'A registered or resident weight is not a runnable generation path. This tool admits only the verified resident service; no remote fallback or inferred model authority.' };
  }
  async function httpReceipt(path, accept = 'application/json', timeoutMs = 90_000, withStatus = false) {
    const started = performance.now();
    try {
      const response = await request(surface + path, { headers: { Accept: accept }, redirect: 'manual', signal: AbortSignal.timeout(timeoutMs) });
      const stdout = response.body ? await boundedPipe(response.body, 200_000) : '';
      return { argv: ['GET', surface + path], exit_code: 0, stdout: withStatus ? `${stdout}\n__STATUS__${response.status}` : stdout, stderr: '', duration_ms: Math.round(performance.now() - started) };
    } catch (error) { return { argv: ['GET', surface + path], exit_code: 7, stdout: '', stderr: String(error), duration_ms: Math.round(performance.now() - started) }; }
  }
  function query(sql, ...args) {
    const db = new Database(dbPath, { readonly: true, strict: true });
    try { return db.query(sql).all(...args); } finally { db.close(); }
  }
  async function nativeMeasurementPath(runner) {
    if (typeof runner !== 'string' || !/^[a-z0-9_-]+\.(?:py|mjs)$/.test(runner)) return null;
    const stem = runner.replace(/\.(?:py|mjs)$/, '');
    const candidates = runner.endsWith('.mjs') ? [runner] : [`${stem}.mjs`, `${stem.replaceAll('_', '-')}.mjs`];
    for (const name of candidates) {
      const path = join(scripts, name);
      try { if ((await stat(path)).isFile()) return path; } catch { /* declared but not installed */ }
    }
    return null;
  }
  async function measure(args) {
    let declared;
    try {
      const db = new Database(cmsDb, { readonly: true, strict: true });
      try { declared = db.query("SELECT measure, runner, COALESCE(subject_flag,'') subject_flag, COALESCE(subject_kind,'') subject_kind, effect_class, answers, cannot_infer FROM measurement_surface WHERE status='published' ORDER BY measure").all(); }
      finally { db.close(); }
    } catch (error) { return { error: 'measurement_surface_unreadable', detail: String(error).slice(0, 120), note: 'the measurements an agent may run are declared in the CMS; without it the gateway has nothing to offer rather than a compiled-in fallback' }; }
    if (!declared.length) return { error: 'no_measurements_declared', note: 'nothing is published in measurement_surface' };
    const op = String(args.op ?? 'list');
    if (op === 'list') {
      const measurements = await Promise.all(declared.map(async row => ({ measure: row.measure, subject_kind: row.subject_kind, effect_class: row.effect_class, answers: row.answers, cannot_infer: row.cannot_infer, runnable_here: Boolean(await nativeMeasurementPath(row.runner)) })));
      return { measurements, note: 'declared is not runnable; a measurement whose native runner is not installed here is listed with runnable_here false rather than hidden' };
    }
    if (op !== 'run') return { error: 'unknown_measure_operation', op };
    const wanted = String(args.measure ?? '');
    const row = declared.find(item => item.measure === wanted);
    if (!row) return { error: 'unknown_measure', asked: wanted, declared: declared.map(item => item.measure) };
    if (!['QUERY', 'PURE'].includes(row.effect_class)) return { error: 'not_readonly', measure: wanted, effect_class: row.effect_class, note: 'this surface runs reads; an effect goes through the organ that owns it' };
    if (typeof row.runner !== 'string' || !/^[a-z0-9_-]+\.(?:py|mjs)$/.test(row.runner)) return { error: 'unsafe_runner', runner: row.runner, note: 'a runner is a bare Python-era or Bun filename inside the estate script directory; a path is refused' };
    const path = await nativeMeasurementPath(row.runner);
    if (!path) return { error: 'runner_not_installed', measure: wanted, expected_at: join(scripts, row.runner.replace(/\.py$/, '.mjs')), note: 'the measurement is declared and its native runner is not deployed here' };
    const subject = String(args.subject ?? '').trim();
    if (subject && !/^[A-Za-z0-9_:./-]{1,120}$/.test(subject)) return { error: 'unsafe_subject', subject: subject.slice(0, 60) };
    const argv = ['bun', path];
    if (subject) {
      if (row.subject_flag && !/^--[a-z0-9-]+$/.test(row.subject_flag)) return { error: 'unsafe_subject_flag', measure: wanted };
      if (row.subject_flag) argv.push(row.subject_flag);
      argv.push(subject);
    }
    argv.push('--json');
    let result;
    try { result = await run(argv, { timeoutMs: 300_000, env: { ...env, BONFYRE_CMS_DB: cmsDb } }); }
    catch (error) { return { error: 'measurement_failed', measure: wanted, detail: String(error).slice(0, 140) }; }
    if (!String(result.stdout || '').trim()) return { error: 'measurement_produced_nothing', measure: wanted, stderr: String(result.stderr || '').slice(-400), note: 'a measurement that produced no receipt is reported rather than returned as an empty result' };
    let receipt;
    try { receipt = JSON.parse(result.stdout); } catch { return { error: 'receipt_unparsable', measure: wanted, head: String(result.stdout).slice(0, 300) }; }
    return { measure: wanted, subject: subject || null, receipt, cannot_infer: row.cannot_infer };
  }
  return async function call(name, args = {}, trace) {
    switch (name) {
      case 'institutional_measure': return measure(args);
      case 'membrane_state': return run(['/usr/local/bin/bonfyre-membrane', 'summary'], { timeoutMs: 90_000 });
      case 'foreign_discover': {
        const host = String(args.host ?? '').trim();
        if (!/^[A-Za-z0-9.:/_-]{3,255}$/.test(host)) return { error: 'host must be a domain or https:// root' };
        return runReceipt(['/usr/local/bin/bonfyre-membrane', 'refresh', host], 300_000);
      }
      case 'sitemap_delta': {
        const host = String(args.from_robots_host ?? '').trim();
        if (host) {
          if (!/^[A-Za-z0-9.:/_-]{3,255}$/.test(host)) return { error: 'bad host' };
          return runReceipt(['/usr/local/bin/bonfyre-sitemap', 'from-robots', host], 600_000);
        }
        const url = String(args.sitemap_url ?? '').trim();
        if (!/^https?:\/\//.test(url)) return { error: 'sitemap_url must be http(s), or pass from_robots_host' };
        return runReceipt(['/usr/local/bin/bonfyre-sitemap', 'check', url], 600_000);
      }
      case 'finance_price_set': {
        const service = String(args.name ?? '');
        if (!/^[A-Za-z][A-Za-z0-9_-]{0,95}$/.test(service)) return { error: 'name must be alphanumeric/dash/underscore, at most 96 chars' };
        if (args.buy_usd === undefined || args.sell_usd === undefined || args.buy_usd === null || args.sell_usd === null || args.buy_usd === '' || args.sell_usd === '') return { error: 'buy_usd and sell_usd must be numbers' };
        const buy = Number(args.buy_usd), sell = Number(args.sell_usd);
        if (!Number.isFinite(buy) || !Number.isFinite(sell)) return { error: 'buy_usd and sell_usd must be numbers' };
        if (buy < 0 || sell < 0) return { error: 'prices cannot be negative' };
        return runReceipt(['/usr/local/bin/bonfyre-finance', 'service', 'add', '--name', service, '--buy', buy.toFixed(2), '--sell', sell.toFixed(2), '--source', 'mcp'], 60_000, { env: { ...env, BONFYRE_FINANCE_DB: financeDb } });
      }
      case 'studio_client_register': {
        const slug = String(args.slug ?? '');
        if (!/^[A-Za-z0-9._@-]{1,96}$/.test(slug)) return { error: 'slug must be alphanumeric/._@- and at most 96 chars' };
        return runReceipt(['/usr/local/bin/bonfyre-entity', 'resolve', 'text', `client:${slug}`, '--kind', 'org'], 30_000, { env: { ...env, BONFYRE_ENTITY_DB: join(stateRoot, 'entity.db') } });
      }
      case 'studio_client_list': {
        const sql = `${sqlBoot}ATTACH ${sqlLiteral(join(stateRoot, 'entity.db'))} AS e (TYPE sqlite, READ_ONLY); SELECT replace(display,'client:','') AS slug, display, first_seen FROM e.entities WHERE display LIKE 'client:%' ORDER BY display`;
        return runReceipt(['/usr/local/bin/bonfyre-query', 'sql', ':memory:', sql], 60_000);
      }
      case 'studio_fleming_watch':
      case 'studio_fleming_opportunity': {
        const value = String(args.account ?? '');
        if (!account(value)) return { error: 'account must be alphanumeric/dash and at most 32 chars' };
        return runReceipt(['/usr/local/bin/bonfyre-queue', 'enqueue', name === 'studio_fleming_watch' ? 'fleming-watch' : 'fleming-opportunity', value, '--db', workDb], 60_000);
      }
      case 'studio_fleming_query': {
        const projection = String(args.projection ?? '');
        if (!allowedFleming.has(projection) || !Object.hasOwn(flemingQueries, projection)) return { error: `unknown projection; choose one of ${[...allowedFleming].sort()}` };
        return runReceipt(['/usr/local/bin/bonfyre-query', 'sql', ':memory:', estateViewSql(flemingQueries[projection])], 180_000);
      }
      case 'estate_query': {
        const admitted = admitEstateSql(args.sql);
        if (admitted.error) return admitted;
        return runReceipt(['/usr/local/bin/bonfyre-query', 'sql', ':memory:', estateViewSql(admitted.sql)], 300_000);
      }
      case 'studio_membrane_status': {
        const sql = `${sqlBoot}ATTACH ${sqlLiteral(join(stateRoot, 'economy.db'))} AS ec (TYPE sqlite, READ_ONLY); ATTACH ${sqlLiteral(join(stateRoot, 'finance.db'))} AS fi (TYPE sqlite, READ_ONLY); SELECT (SELECT count(*) FROM ec.budgets) AS budgets, (SELECT coalesce(sum(max_usd),0) FROM ec.budgets) AS usd_capped, (SELECT coalesce(sum(spent),0) FROM ec.budgets) AS usd_spent_against_caps, (SELECT count(*) FROM ec.costs) AS cost_records, (SELECT count(*) FROM fi.services) AS services`;
        return runReceipt(['/usr/local/bin/bonfyre-query', 'sql', ':memory:', sql], 60_000);
      }
      case 'studio_membrane_budget_set': {
        const recipe = String(args.recipe ?? '');
        if (!/^[A-Za-z0-9._@-]{1,96}$/.test(recipe)) return { error: 'recipe must be alphanumeric/._@- and at most 96 chars' };
        const cap = money(args.usd_cap, 'usd_cap');
        if (typeof cap !== 'number') return cap;
        return runReceipt(['/usr/local/bin/bonfyre-economy', 'budget', 'set', recipe, cap.toFixed(2)], 30_000, { env: { ...env, BONFYRE_ECONOMY_DB: join(stateRoot, 'economy.db') } });
      }
      case 'studio_work_claim': {
        const worker = String(args.worker ?? '');
        if (!ident(worker)) return { error: 'worker must be alphanumeric/._- and at most 96 chars' };
        return runReceipt(['/usr/local/bin/bonfyre-queue', 'claim-next', worker, '--db', workDb], 60_000);
      }
      case 'studio_work_complete': {
        const values = ['mission', 'node', 'worker'].map(key => String(args[key] ?? ''));
        if (!values.every(ident)) return { error: 'mission, node and worker must be alphanumeric/._- and at most 96 chars' };
        const token = String(args.token ?? '');
        if (!/^[0-9a-fA-F]{64}$/.test(token)) return { error: 'token must be the 64-hex claim token from studio_work_claim' };
        return runReceipt(['/usr/local/bin/bonfyre-queue', 'ack', ...values, token, '--db', workDb], 60_000);
      }
      case 'studio_economy_cost_record': {
        const values = ['recipe', 'stage', 'model'].map(key => String(args[key] ?? ''));
        if (!values.every(ident)) return { error: 'recipe, stage and model must be alphanumeric/._- and at most 96 chars' };
        const usd = money(args.usd, 'usd');
        if (typeof usd !== 'number') return usd;
        return runReceipt(['/usr/local/bin/bonfyre-economy', 'cost', 'record', ...values, usd.toFixed(6)], 30_000, { env: { ...env, BONFYRE_ECONOMY_DB: join(stateRoot, 'economy.db') } });
      }
      case 'finance_book': {
        const catalogue = await httpReceipt('/capability', 'application/json', 60_000);
        const db = new Database(financeDb, { readonly: true, strict: true });
        let rows;
        try { rows = db.query("SELECT name, category, buy_price, sell_price, round(sell_price - buy_price, 2) AS margin_usd, CASE WHEN sell_price > 0 THEN 'priced' ELSE 'not_for_sale' END AS status, source FROM services ORDER BY sell_price DESC, name").all(); }
        finally { db.close(); }
        const book = { rows, count: rows.length, source: financeDb, read_only: true };
        return { book, x402_executable_catalogue: catalogue, note: 'Two different sets. The book is every service with a price; the x402 catalogue is the subset an organ can execute and therefore settle. Pricing a book entry does not make it x402-sellable.' };
      }
      case 'protocol_fetch': {
        const carrier = String(args.carrier ?? '').trim().toLowerCase();
        if (!(carrier in carrierPaths)) return { error: 'unknown carrier', carriers: Object.keys(carrierPaths).sort() };
        let path = carrierPaths[carrier];
        const capability = String(args.name ?? '').trim();
        if (capability) {
          if (!/^[A-Za-z][A-Za-z0-9_-]{0,95}$/.test(capability)) return { error: 'name must be alphanumeric/dash/underscore' };
          if (carrier === 'x402') path = `/capability/${capability}`;
          if (carrier === 'mpp') path = `/mpp/quote?capability=${capability}`;
        }
        return httpReceipt(path, 'application/json', 90_000, true);
      }
      case 'desk_open_decisions': return httpReceipt('/studio/', 'text/html', 90_000);
      case 'oracle_service_status': {
        const unit = String(args.unit ?? '');
        if (!unitNames.has(unit)) throw new Error(`unit not allowlisted: ${unit}`);
        const lines = positive(args.journal_lines, 120, 1000);
        const show = await runReceipt(['systemctl', 'show', unit, '-p', 'ActiveState', '-p', 'SubState', '-p', 'Result', '-p', 'ExecMainStatus', '-p', 'ExecMainStartTimestamp', '-p', 'ExecMainExitTimestamp', '--no-pager'], 15_000);
        const journal = lines ? await runReceipt(['journalctl', '-u', unit, '-n', String(lines), '--no-pager'], 30_000) : null;
        return { unit, show, journal };
      }
      case 'oracle_command_describe': {
        const family = String(args.family ?? '');
        const binary = await commandBinary(family);
        const [file, ldd, version, help] = await Promise.all([
          runReceipt(['file', binary], 5_000), runReceipt(['ldd', binary], 10_000),
          runReceipt([binary, '--version'], 8_000), runReceipt([binary, '--help'], 8_000),
        ]);
        return { family, binary, file, ldd, version, help };
      }
      case 'oracle_command_probe': {
        const binary = await commandBinary(String(args.family ?? ''));
        const flag = String(args.mode ?? 'version') === 'version' ? '--version' : '--help';
        return runReceipt([binary, flag], positive(args.timeout_seconds, 8, 30, 1) * 1000, { cwd: join(commandRoot, String(args.family)) });
      }
      case 'oracle_profile_run': {
        const profile = String(args.profile ?? '');
        if (!profileNames.has(profile)) throw new Error('profile not allowlisted');
        const unit = `bonfyre-hypercycle@${profile}.service`;
        const started = await runReceipt(['systemctl', 'start', unit], 3_700_000);
        const show = await runReceipt(['systemctl', 'show', unit, '-p', 'Result', '-p', 'ExecMainStatus', '-p', 'ActiveState', '-p', 'SubState', '--no-pager'], 15_000);
        const state = show.stdout.includes('Result=success') && show.stdout.includes('ExecMainStatus=0') ? 'success' : 'failed';
        return { profile, state, start: started, show };
      }
      case 'oracle_profile_receipts': return profileReceipts(args);
      case 'oracle_deep_flow': {
        const flow = String(args.flow ?? '');
        if (!Object.hasOwn(flowFiles, flow)) throw new Error('flow not allowlisted');
        const tool = join(root, 'tools', flowFiles[flow]);
        if (!(await stat(tool).catch(() => null))?.isFile()) throw new Error(`flow not installed: ${tool}`);
        return runReceipt(['bun', tool], positive(args.timeout_seconds, 900, 3600, 10) * 1000, {
          cwd: root, env: { ...env, BONFYRE_BASE_URL: runtime, BONFYRE_EXTERNAL_READS_ENABLED: '1', BONFYRE_EXTERNAL_WRITES_ENABLED: '0', BONFYRE_LIVE_APPLY_ENABLED: '0', BONFYRE_EXTERNAL_IRREVERSIBLE_EFFECTS_ENABLED: '0' },
        });
      }
      case 'artifact_search': return artifactSearch(args);
      case 'artifact_read': {
        const path = await artifactPath(String(args.path ?? ''));
        const info = await stat(path);
        if (!info.isFile()) throw new Error(`artifact not a file: ${path}`);
        const maxBytes = limit(args.max_bytes, 200_000, 1_000_000);
        const handle = Bun.file(path);
        const data = Buffer.from(await handle.slice(0, maxBytes).arrayBuffer());
        return { path, size: info.size, truncated: info.size > maxBytes, text: data.toString('utf8') };
      }
      case 'mesh_status': {
        let project;
        try { project = await projectCall('project_status', {}); } catch (error) { project = { error: String(error) }; }
        const release = await realpath(releasePath).catch(() => null);
        const units = ['bonfyred.service', 'bonfyre-continuation-loop.timer', 'bonfyre-autonomous-worker-loop.timer'];
        const services = Object.fromEntries(await Promise.all(units.map(async unit => [unit, await runReceipt(['systemctl', 'show', unit, '-p', 'ActiveState', '-p', 'SubState', '-p', 'Result', '-p', 'ExecMainStatus', '--no-pager'], 10_000)])));
        return { node: 'oracle', runtime_url: runtime, project_mcp_url: env.BONFYRE_PROJECT_MCP_URL || 'http://127.0.0.1:13051/mcp', release, services, project, database: dbCounts(), tool_count: catalogueToolCount };
      }
      case 'release_attest': return releaseAttest();
      case 'model_local_inventory': return modelInventory();
      case 'model_local_generate': {
        const prompt = String(args.prompt ?? '');
        if (!prompt.trim()) return { error: 'prompt is empty' };
        const inventory = await modelInventory();
        if (!inventory.runnable) return { error: 'local_path_incomplete', blockers: inventory.blockers, detail: 'The resident local generation service is unavailable; this tool refuses instead of falling back to a remote provider.' };
        const tokens = Number.isInteger(args.max_tokens) && args.max_tokens > 0 && args.max_tokens <= 4096 ? args.max_tokens : 512;
        const payload = { model: 'bonfyre-local', messages: [{ role: 'user', content: prompt }], max_tokens: tokens };
        let response;
        try {
          response = await request(`${localModel}/v1/chat/completions`, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(payload), signal: AbortSignal.timeout(660_000) });
          const result = await readJson(response);
          const content = result?.choices?.[0]?.message?.content;
          if (!response.ok || typeof content !== 'string') return { error: 'local_generation_failed', raw: { status: response.status, result } };
          return { text: content, usage: result.usage ?? null, served_by: localModel, provider: "none -- this ran on the institution's own hardware" };
        } catch (error) { return { error: 'local_generation_failed', raw: { error: String(error), status: response?.status ?? 0 } }; }
      }
      case 'task_start': {
        if (!ledger?.createTask) throw new Error('task ledger unavailable');
        const kind = String(args.kind ?? ''), taskName = String(args.name ?? '');
        let command;
        if (kind === 'profile') {
          if (!profileNames.has(taskName)) throw new Error('profile not allowlisted');
          command = ['/bin/systemctl', 'start', `bonfyre-hypercycle@${taskName}.service`];
        } else if (kind === 'flow') {
          if (!Object.hasOwn(flowFiles, taskName)) throw new Error('flow not allowlisted');
          command = ['/usr/bin/env', `BONFYRE_BASE_URL=${runtime}`, 'BONFYRE_EXTERNAL_READS_ENABLED=1', 'BONFYRE_EXTERNAL_WRITES_ENABLED=0', 'BONFYRE_LIVE_APPLY_ENABLED=0', env.BONFYRE_BUN_BIN || '/home/ubuntu/.bun/bin/bun', join(root, 'tools', flowFiles[taskName])];
        } else throw new Error('kind must be profile or flow');
        const taskId = randomUUID().replaceAll('-', '').slice(0, 20), unit = `bonfyre-mcp-task-${taskId}`;
        await mkdir(taskDir, { recursive: true });
        const logPath = join(taskDir, `${taskId}.log`);
        const argv = ['systemd-run', '--unit', unit, '--collect', '--property', 'Type=oneshot', '--property', `StandardOutput=append:${logPath}`, '--property', `StandardError=append:${logPath}`, ...(kind === 'flow' ? ['--uid=ubuntu', '--gid=ubuntu', `--working-directory=${root}`] : []), ...command];
        const launch = await runReceipt(argv, 30_000);
        const state = launch.exit_code === 0 ? 'started' : 'failed_to_start';
        ledger.createTask({ id: taskId, kind, name: taskName, unit_name: unit, state, log_path: logPath }, trace);
        return { task_id: taskId, unit, state, launch, log_path: logPath };
      }
      case 'task_status': {
        if (!ledger?.task) throw new Error('task ledger unavailable');
        const taskId = String(args.task_id ?? '');
        if (!/^[a-f0-9]{20}$/.test(taskId)) throw new Error('invalid task id');
        const task = ledger.task(taskId);
        if (!task) throw new Error('task not found');
        const lines = positive(args.journal_lines, 200, 2000);
        const show = await runReceipt(['systemctl', 'show', task.unit_name, '-p', 'LoadState', '-p', 'ActiveState', '-p', 'SubState', '-p', 'Result', '-p', 'ExecMainStatus', '--no-pager'], 15_000);
        const journal = await runReceipt(['journalctl', '-u', task.unit_name, '-n', String(lines), '--no-pager'], 30_000);
        const expected = join(taskDir, `${taskId}.log`);
        if (task.log_path !== expected) throw new Error('task log path differs from task id');
        const log = Bun.file(expected);
        const logText = await log.exists() ? await log.slice(Math.max(0, log.size - 200_000)).text() : '';
        return { task, show, journal, log: logText };
      }
      case 'architecture_atlas': {
        const op = String(args.op ?? '').replaceAll('_', '-');
        if (!['summary', 'search', 'get', 'cannot-infer', 'expand', 'loss'].includes(op)) throw new Error('unknown Atlas operation');
        try { if (!(await stat(atlas)).isFile()) throw new Error('not a file'); }
        catch { return { error: 'atlas_query_absent', expected_at: atlas, detail: 'The registry read path is not installed; source search is not an equivalent substitute.' }; }
        const argv = [atlas, 'query', op];
        if (['search', 'get', 'cannot-infer', 'expand'].includes(op)) {
          const arg = String(args.arg ?? '').trim();
          if (!arg) return { error: `op ${op} needs arg` };
          argv.push(arg);
        }
        return run(argv, { timeoutMs: 60_000 });
      }
      case 'mcp_call_history': {
        const tool = String(args.tool ?? ''), state = String(args.state ?? ''), count = limit(args.limit, 200, 2000);
        const conditions = [], values = [];
        if (tool) { conditions.push('tool=?'); values.push(tool); }
        if (state) { conditions.push('state=?'); values.push(state); }
        const rows = query(`SELECT * FROM calls${conditions.length ? ` WHERE ${conditions.join(' AND ')}` : ''} ORDER BY ts DESC LIMIT ?`, ...values, count);
        return { calls: rows.map(({ args_json, result_json, ...row }) => ({ ...row, args: JSON.parse(args_json), result: JSON.parse(result_json) })) };
      }
      case 'project_recent_deltas': {
        const rows = query('SELECT * FROM deltas ORDER BY seq DESC LIMIT ?', limit(args.limit, 100, 1000));
        return { deltas: rows.map(({ payload_json, ...row }) => ({ ...row, payload: JSON.parse(payload_json) })) };
      }
      case 'runtime_get': {
        const path = String(args.path ?? '');
        if (path !== '/health' && !path.startsWith('/api/')) throw new Error('runtime path must be /health or /api/*');
        try { const response = await request(runtime + path, { signal: AbortSignal.timeout(60_000) }); return { status: response.status, path, result: await readJson(response) }; }
        catch (error) { return { status: 0, path, result: { error: String(error) } }; }
      }
      case 'runtime_preview': {
        const path = String(args.path ?? '');
        if (!previews.has(path)) throw new Error(`POST route not allowlisted: ${path}`);
        try { const response = await request(runtime + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(args.body || {}), signal: AbortSignal.timeout(300_000) }); return { status: response.status, path, result: await readJson(response) }; }
        catch (error) { return { status: 0, path, result: { error: String(error) } }; }
      }
      default: throw new Error(`unknown native rich tool: ${name}`);
    }
  };
}
