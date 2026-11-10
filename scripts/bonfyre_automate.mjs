#!/usr/bin/env bun
// Standing estate clock. Effects stay in their native owners; this process only
// sequences bounded work and records exact outcomes.
import { existsSync, mkdirSync, readFileSync, writeFileSync, renameSync, unlinkSync, openSync, closeSync, readSync, lstatSync } from 'node:fs';
import { resolve, join, dirname, basename, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { Database } from 'bun:sqlite';

const here = dirname(fileURLToPath(import.meta.url));
const defaults = {
  root: existsSync(resolve(here, '../cmd/BonfyreLatentAtlas/tools/link_identities.mjs')) ? resolve(here, '..') : '/opt/bonfyre/native-src',
  state: '/var/lib/bonfyre/automation', cms: '/var/lib/bonfyre/cms/bonfyre_cms.db',
  'cms-schemas': '/var/lib/bonfyre/cms/internet-source-type', 'site-root': '/opt/bonfyre/site/matrix',
  foreign: '/var/lib/bonfyre/foreign', 'atlas-db': '/var/lib/bonfyre/latent-atlas/census-recordings.db',
  'credentials-db': '/var/lib/bonfyre/credentials/credentials.db', 'credentials-key': '/etc/bonfyre/credentials.key',
  providers: '/usr/local/share/bonfyre/provider-credentials.json', bin: '/usr/local/bin',
  'acoustic-per-run': '8', 'frontier-db': '/var/lib/bonfyre/source-frontier/frontier.db',
  'archive-db': '/var/lib/bonfyre/items/items.db', 'external-estate-db': '/var/lib/bonfyre/external-estate.db',
  'scoped-frontier-db': '/var/lib/bonfyre/scoped-frontier/frontier.db',
  'subject-fabric-db': '/var/lib/bonfyre/subject-fabric/subject-fabric.db',
  'catalogue-members': '3000', 'qualify-per-run': '30',
  'workgraph-db': process.env.BONFYRE_WORKGRAPH_DB || '', 'activation-db': process.env.BONFYRE_ACTIVATION_DB || '',
  'connection-describe-per-run': '40', 'connection-describe-lease-ms': '1800000', 'compute-backend': 'auto',
};
const args = { ...defaults, weekly: false, only: '' };
for (let i = 2; i < process.argv.length; i++) {
  const token = process.argv[i];
  if (token === '--weekly') { args.weekly = true; continue; }
  if (token === '--help') { console.log('bonfyre_automate.mjs [--weekly] [--only STEP,...] [--root PATH] [--state PATH] [paths...]'); process.exit(0); }
  if (!token.startsWith('--') || !Object.hasOwn(args, token.slice(2)) || i + 1 >= process.argv.length) throw Error(`unknown or incomplete option: ${token}`);
  args[token.slice(2)] = process.argv[++i];
}
const p = (...parts) => join(args.root, ...parts);
const b = name => join(args.bin, name);
const work = join(args.state, 'work');
const items = join(work, 'census-items.jsonl');
const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\..*/, 'Z') + `-${process.pid}-${Date.now() % 1000}`;
mkdirSync(join(args.state, 'runs'), { recursive: true });
mkdirSync(work, { recursive: true });
const env = { ...process.env, BONFYRE_CMS_DB: args.cms, BONFYRE_FOREIGN_ROOT: args.foreign };
const only = args.only ? new Set(args.only.split(',')) : null;
const record = { schema: 'bonfyre-automation-run.v1', started: stamp, weekly: args.weekly, steps: [] };
class ToolMissing extends Error {}
const jwrite = (path, value) => writeFileSync(path, JSON.stringify(value, null, 1) + '\n');
const jread = path => JSON.parse(readFileSync(path, 'utf8'));
function raw(argv, { timeout = 3600, env_extra = {} } = {}) {
  for (const token of argv.slice(0, 2)) {
    if (typeof token === 'string' && token.includes('/') && !existsSync(token)) throw new ToolMissing(`not installed here: ${token}`);
  }
  const result = Bun.spawnSync(argv, { stdout: 'pipe', stderr: 'pipe', env: { ...env, ...env_extra }, timeout: timeout * 1000 });
  return { code: result.exitCode, stdout: result.stdout?.toString() || '', stderr: result.stderr?.toString() || '' };
}
function run(argv, options) {
  const done = raw(argv, options);
  if (done.code !== 0) throw Error(`${basename(argv[0])} exited ${done.code}: ${done.stderr.trim().slice(0, 300)}`);
  const lines = done.stdout.split(/\r?\n/).filter(x => x.trim());
  if (!lines.length) return null;
  try { return JSON.parse(lines.at(-1)); } catch { return lines.at(-1).slice(0, 300); }
}
function runJsonLines(argv, options) {
  const done = raw(argv, options);
  if (done.code !== 0) throw Error(`${basename(argv[0])} exited ${done.code}: ${done.stderr.trim().slice(0, 300)}`);
  return done.stdout.split(/\r?\n/).filter(x => x.trim()).map(line => {
    try { return JSON.parse(line); } catch { throw Error(`${basename(argv[0])} returned non-JSON batch output: ${line.slice(0, 200)}`); }
  });
}
function runToFile(argv, path, { timeout = 3600, env_extra = {} } = {}) {
  const fd = openSync(path, 'w');
  try {
    const done = spawnSync(argv[0], argv.slice(1), { stdio: ['ignore', fd, 'pipe'], env: { ...env, ...env_extra }, timeout: timeout * 1000, encoding: 'utf8' });
    if (done.error || done.status !== 0) throw Error(`${basename(argv[0])} exited ${done.status}: ${String(done.error?.message || done.stderr || '').slice(-300)}`);
  } finally { closeSync(fd); }
}
function countLines(path) {
  const fd = openSync(path, 'r');
  const chunk = Buffer.allocUnsafe(64 * 1024);
  let count = 0, total = 0, last = 10;
  try {
    for (;;) {
      const n = readSync(fd, chunk, 0, chunk.length, null);
      if (!n) break;
      total += n;
      for (let i = 0; i < n; i++) if (chunk[i] === 10) count++;
      last = chunk[n - 1];
    }
  } finally { closeSync(fd); }
  return count + (total && last !== 10 ? 1 : 0);
}
// Reuse native CMS's *actual retained-input* generation. We never replace a
// ForeignTwin receipt with a filesystem timestamp: the digest must come from
// that owner, and the retained body must still be located under its root.
// An unavailable source is part of this generation; a new valid receipt
// changes the fingerprint and re-opens the native admission automatically.
function retainedPublicationGeneration() {
  const db = new Database(args.cms, { readonly: true });
  let sources, occurrences, pending = [];
  try {
    sources = db.query("SELECT source, COALESCE(twin,'') AS twin FROM internet_source " +
      "WHERE status='published' AND extract LIKE 'rss:%' ORDER BY source").all();
    occurrences = db.query("SELECT COUNT(*) AS n FROM publication_occurrence WHERE namespace='root'").get().n;
    if (db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_publication_delta_outbox'").get())
      pending = db.query("SELECT source,revision FROM _publication_delta_outbox WHERE namespace='root' ORDER BY source").all();
  } finally { db.close(); }
  // Match the bound of the current native --all-published owner: never let
  // it silently truncate a larger source estate and then checkpoint it.
  if (sources.length > 4096) throw Error('native publication owner limit 4096 sources exceeded');
  const hash = createHash('sha256');
  hash.update('bonfyre.retained-publication-generation.v1\0');
  hash.update(createHash('sha256').update(readFileSync(b('bonfyre-cms'))).digest('hex'));
  // The configured FactDelta sink and durable CMS outbox are part of whether
  // this projection generation has actually reached its dependent owners.
  // Enabling a sink after an earlier disconnected run must invalidate a hit.
  const deltaSink = String(process.env.BONFYRE_FACT_DELTAS || '');
  hash.update([args.cms, args.foreign, args['cms-schemas'], occurrences, deltaSink].join('\0'));
  for (const row of pending) hash.update('\0pending:'+row.source+':'+row.revision);
  let unverified = 0;
  const foreign = resolve(args.foreign);
  for (const source of sources) {
    const twin = String(source.twin || '');
    hash.update('\n'+String(source.source)+'\0'+twin+'\0');
    if (!/^[a-zA-Z0-9._-]{1,128}$/.test(twin)) { unverified++; continue; }
    const receiptFile = join(foreign,'receipts',twin+'.json');
    if (!existsSync(receiptFile)) { hash.update('not-observed'); continue; }
    try {
      const info = lstatSync(receiptFile);
      if (!info.isFile()) { unverified++; continue; }
      const fd = openSync(receiptFile,'r');
      let tail;
      try {
        const n = Math.min(65536,info.size);
        tail = Buffer.alloc(n);
        readSync(fd,tail,0,n,info.size-n);
      } finally { closeSync(fd); }
      const rows = tail.toString('utf8').trim().split(/\r?\n/).filter(Boolean);
      const receipt = JSON.parse(rows.at(-1) || 'null');
      const digest = String(receipt?.content_sha256 || receipt?.unchanged_from_content_sha256 || '');
      if (!/^[a-f0-9]{64}$/.test(digest)) { unverified++; continue; }
      hash.update(digest);
      const pathValue = receipt.body_path;
      if (typeof pathValue !== 'string') { hash.update('body-unretained'); continue; }
      const candidate = resolve(pathValue);
      if (!candidate.startsWith(foreign+sep)) { unverified++; continue; }
      if (!existsSync(candidate)) { hash.update('body-missing'); continue; }
      const stat = lstatSync(candidate);
      if (!stat.isFile()) { unverified++; continue; }
      // Metadata makes a later body replacement force re-admission even when
      // an old source receipt still says it retained an earlier digest.
      hash.update([stat.size,stat.mtimeMs,stat.ino].join(':'));
    } catch { unverified++; }
  }
  return { digest: unverified ? null : hash.digest('hex'), sources:sources.length, unverified,
    pending_source_deltas:pending.length,delta_sink_configured:Boolean(deltaSink) };
}
function step(name, fn, weeklyOnly = false) {
  if ((weeklyOnly && !args.weekly) || (only && !only.has(name))) return;
  const start = performance.now();
  try { record.steps.push({ step: name, ok: true, seconds: Math.round((performance.now() - start) / 100) / 10, outcome: fn() }); }
  catch (error) {
    const entry = { step: name, ok: error instanceof ToolMissing, seconds: Math.round((performance.now() - start) / 100) / 10 };
    if (entry.ok) entry.outcome = { skipped: error.message };
    else { entry.error = String(error.message).slice(0, 400); entry.trace = String(error.stack || '').slice(-800); }
    record.steps.push(entry);
  }
}
function requireCurrent(name) {
  const entry = record.steps.findLast(s => s.step === name);
  if (!entry?.ok || entry.outcome?.skipped) throw Error(`required current-run step did not complete: ${name}`);
}
const tokenOf = host => Buffer.from(host).toString('base64url');
function hostOf(mission) {
  if (!mission.startsWith('connection-describe:')) throw Error(`unexpected connection mission: ${mission}`);
  return Buffer.from(mission.slice('connection-describe:'.length), 'base64url').toString('utf8');
}
function activationDb() {
  if (args['activation-db']) return resolve(args['activation-db']);
  const candidate = p('components', 'BonfyreControlPlane', 'control_plane.db');
  if (!existsSync(candidate)) return '';
  try {
    const db = new Database(candidate, { readonly: true });
    const names = new Set(db.query("SELECT name FROM sqlite_master WHERE type='table'").all().map(x => x.name));
    db.close();
    return ['authority_edges', 'provider_bids', 'provider_observations'].every(n => names.has(n)) ? candidate : '';
  } catch { return ''; }
}
function connectionCandidates() {
  const argv = ['bun', p('cmd', 'BonfyreCredentials', 'tools', 'discover_credential_needs.mjs'), '--cms', args.cms, '--list-hosts'];
  if (args['frontier-db']) argv.push('--frontier-db', args['frontier-db']);
  const done = raw(argv, { timeout: 300 });
  if (done.code !== 0) throw Error(`discover_credential_needs.mjs --list-hosts exited ${done.code}: ${done.stderr.trim().slice(0, 300)}`);
  return done.stdout.split(/\r?\n/).filter(Boolean).map(line => {
    const [host, priority] = line.split('\t'); return [host.trim(), Number(priority || 100)];
  }).filter(([host]) => host);
}
function seedConnection(queue, candidates) {
  mkdirSync(dirname(args['workgraph-db']), { recursive: true });
  const seed = join(work, 'connection-describe-work.tsv');
  const material = candidates.map(([host, priority]) => `connection-describe:${tokenOf(host)}\tconnection.describe\thttps://${host}/\tconnection-describe\t${priority}\t4\t30000\n`).join('');
  writeFileSync(seed, material);
  const digest = createHash('sha256').update(material).digest('hex');
  const marker = join(args.state, 'connection-describe-seed.json');
  let previous = {};
  if (existsSync(marker)) { try { previous = jread(marker); } catch {} }
  const depth = existsSync(args['workgraph-db']) ? run([queue, 'depth', '--family', 'connection-describe', '--db', args['workgraph-db']])
    : { family: 'connection-describe', total: 0, claimable: 0, running: 0, complete: 0, dead_letter: 0 };
  if (previous.sha256 !== digest || previous.workgraph_db !== resolve(args['workgraph-db']) || Number(depth?.total || 0) < candidates.length) {
    const seeded = run([queue, 'enqueue-batch', seed, '--db', args['workgraph-db']], { timeout: 1800 });
    jwrite(marker, { sha256: digest, hosts: candidates.length, workgraph_db: resolve(args['workgraph-db']), updated: stamp });
    return [seeded, depth, digest];
  }
  return [{ accepted: 0, failed: 0, idempotent: true, unchanged_candidate_set: true }, depth, digest];
}

step('sources', () => {
  const rows = join(work, 'live-sources.json');
  const compiled = run(['bun', p('cmd', 'BonfyreExternalEstate', 'tools', 'compile_live_sources.mjs'), '--project', b('bonfyre-project'), '--foreign', args.foreign, '--out', rows, '--evidence', join(args.state, `live-sources-evidence-${stamp}.jsonl`)], { timeout: 7200 });
  const applied = run(['bun', p('cmd', 'BonfyreWebAddress', 'tools', 'apply_census_sources.mjs'), '--json', rows, '--cms', b('bonfyre-cms'), '--db', args.cms, '--schemas', args['cms-schemas']]);
  return { compiled, applied };
}, true);
step('inventories', () => {
  mkdirSync(dirname(args['frontier-db']), { recursive: true });
  const listed = run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'compile_inventories.mjs'), '--db', args['frontier-db'], '--frontier', b('bonfyre-source-frontier'), '--out', join(work, 'listings'), '--v4-sqlite', args['external-estate-db'], '--live-sources', join(work, 'live-sources.json')], { timeout: 1800 });
  return { listed, census: run([b('bonfyre-source-frontier'), 'census-link', '--db', args['frontier-db'], '--cms', args.cms]) };
}, true);
step('channels', () => {
  const found = join(work, 'retained-channels.jsonl');
  const discovered = run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'expand_retained_channels.mjs'), '--cms', args.cms, '--foreign', args.foreign, '--out', found], { timeout: 1800 });
  return { discovered, imported: run([b('bonfyre-source-frontier'), 'import', '--db', args['frontier-db'], '--label', 'RETAINED_BODY_DISCOVERY', '--jsonl', found]) };
}, true);
step('catalogues', () => {
  const found = join(work, 'catalogue-channels.jsonl');
  const enumerated = run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'expand_catalogues.mjs'), '--project', b('bonfyre-project'), '--foreign', args.foreign, '--out', found, '--evidence', join(args.state, 'catalogue-evidence.jsonl'), '--per-catalogue', args['catalogue-members']], { timeout: 7200 });
  return { enumerated, imported: run([b('bonfyre-source-frontier'), 'import', '--db', args['frontier-db'], '--label', 'PUBLIC_CATALOGUE', '--jsonl', found]) };
}, true);
// Promotion is an owned *material cut*, not another source crawl. Every
// published RSS reading (news, podcast, public records, grants syndication)
// shares one native CMS invocation, one schema loading, and one deferred
// index rebuild. The native promoter reads only already-retained ForeignTwin
// bodies and uses its existing occurrence identities to refuse duplicates.
step('publication-material', () => {
  const generation = retainedPublicationGeneration();
  const checkpoint = join(work,'publication-material-generation.json');
  let prior = null;
  if (generation.digest && existsSync(checkpoint)) {
    try { const value = readFileSync(checkpoint,'utf8');
      if (value.length <= 8192) prior = JSON.parse(value); } catch {}
  }
  const terminal = prior?.outcome?.status === 'complete' ||
    (prior?.outcome?.status === 'material_admitted_delta_delivery_pending' && !generation.delta_sink_configured);
  if (generation.digest && prior?.digest === generation.digest && terminal &&
      (!generation.delta_sink_configured || generation.pending_source_deltas === 0) &&
      prior.outcome.sources_held === 0 && prior.outcome.unverifiable_source_receipts === 0) {
    return { ...prior.outcome, owner_operations: 0, network_fetches: 0,
      new_publication_occurrences: 0,
      skipped: 'source_receipts_and_native_CMS_generation_unchanged' };
  }
  const result = raw([b('bonfyre-cms'), 'publication', 'promote-rss',
    '--all-published', '--foreign-root', args.foreign, '--limit', '100',
    '--ns', 'root', '--db', args.cms, '--schemas', args['cms-schemas']], { timeout: 1800 });
  const lines = result.stdout.split(/\r?\n/).filter(line => line.trim().startsWith('{'));
  let summary;
  try { summary = JSON.parse(lines.at(-1) || 'null'); } catch {}
  if (!summary || summary.schema !== 'bonfyre-publication-promotion.v1' ||
      !Number.isInteger(summary.sources) || !Number.isInteger(summary.items_created) ||
      !Number.isInteger(summary.promoted) || !Number.isInteger(summary.refused) ||
      result.code !== 0) {
    throw Error(`native publication cut refused: exit ${result.code}; ${result.stderr.trim().slice(0,250)}`);
  }
  // Reread the owner's durable outbox after the native commit/forwarding cut.
  const final = retainedPublicationGeneration();
  const outcome = { owner: 'BonfyreCMS', input: 'already_retained_foreign_rss',
    source_families: 'CMS declared extract:rss:*',
    sources_examined: summary.sources, sources_admitted: summary.promoted,
    sources_held: summary.refused, new_publication_occurrences: summary.items_created,
    owner_operations: 1, network_fetches: 0,
    unverifiable_source_receipts: generation.unverified,
    pending_source_deltas: final.pending_source_deltas, delta_delivery_configured: final.delta_sink_configured,
    status: summary.refused ? 'partial_with_explicit_source_holds' :
      final.pending_source_deltas ? 'material_admitted_delta_delivery_pending' : 'complete',
    cannot_infer: 'retention or promotion is not truth, a grant, a licence, a media right or publication authorization' };
  // A partial native batch is not a stable terminal generation: even if its
  // successful rows were indexed, a refused source must remain eligible for
  // a later tick. Otherwise a single 503 or missing receipt is cached forever.
  if (generation.digest && generation.unverified === 0 &&
      summary.refused === 0 && summary.promoted === summary.sources &&
      (!final.delta_sink_configured || final.pending_source_deltas === 0)) {
    // Owner mutations may change the material count; recompute the digest
    // after the native transaction, not before it. Never cache a refusal.
    if (final.digest) {
      const next = checkpoint+'.'+stamp+'.tmp';
      try { writeFileSync(next,JSON.stringify({digest:final.digest,outcome}),{flag:'wx',mode:0o600});
        renameSync(next,checkpoint);
      } finally { if (existsSync(next)) unlinkSync(next); }
    }
  }
  return outcome;
});
// Event media and agenda cuts share the SAME native SourceScheduler budget.
// The exact published SourceCollection picks the meeting pair; the scheduler
// decides whether its source is due. Only selected sources cross provider
// observation and the existing transactional CMS import. No Dallas URL list,
// hardcoded event IDs, or parallel scheduler is maintained by this clock.
step('civic-index', () => {
  const db = new Database(args.cms, { readonly: true });
  let admitted;
  try { admitted = db.query("SELECT source FROM internet_source WHERE status='published' " +
      "AND family IN ('civic-meeting','civic-meeting-video')").all().map(r => r.source);
  } finally { db.close(); }
  if (!admitted.length) return { owner: 'BonfyreProject', owner_operations: 0,
    skipped: 'no_published_civic_source_contracts' };
  const budget = run([b('bonfyre-project'), 'foreign', 'reheat',
    '--dry-run', '--budget', '40', '--per-host', '2'],
    { timeout: 180, env_extra: { BONFYRE_CMS_DB: args.cms, BONFYRE_FOREIGN_ROOT: args.foreign } });
  if (!budget || !Array.isArray(budget.sources))
    throw Error('native SourceScheduler returned no bounded source-weather decision');
  const allowed = new Set(admitted);
  const due = [...new Set(budget.sources.filter(row =>
    row.decision === 'due' && allowed.has(row.source)).map(row => row.source))];
  if (!due.length) return { owner: 'BonfyreProject', owner_operations: 0,
    scheduler_census: budget.census, skipped: 'native_resource_weather_no_civic_source_due',
    source_backlog: budget.waiting_its_turn ?? null };
  const argv = ['bun', p('cmd','BonfyreProject','tools','civic_sync.mjs'),
    '--apply', '--cms-db', args.cms, '--schemas', args['cms-schemas'],
    '--cms-bin', b('bonfyre-cms'), '--project-bin', b('bonfyre-project'),
    '--state', join(args.state,'receipts','civic-sync'), '--foreign-root', args.foreign, '--limit', '8',
    ...due.slice(0,64).flatMap(source => ['--due-source', source])];
  const receipt = run(argv, { timeout: 900 });
  if (!receipt || receipt.schema !== 'bonfyre.project.civic-reconcile.v1' ||
      !Number.isInteger(receipt.applied) || !Number.isInteger(receipt.candidate_creates))
    throw Error('civic owner returned no verified CMS reconciliation receipt');
  return { owner: 'BonfyreProject', scheduler_census: budget.census,
    eligible_source_contracts: admitted.length, due_selected: due.length,
    paired_meetings: receipt.paired_meetings, new_publication_occurrences: receipt.applied,
    review_cases: receipt.review_cases, deferred_meetings: receipt.deferred_pairs,
    source_collections_considered: receipt.source_collections_considered, native_receipt_sha256: receipt.receipt_sha256,
    state: receipt.state, owner_operations: 1,
    cannot_infer: 'official chapter references are not vote, speech, grant, geo or media-reuse authority' };
});
step('census-items', () => {
  const temp = `${items}.${process.pid}.tmp`;
  try {
    runToFile([b('bonfyre-web-address'), 'census-items'], temp, { timeout: 900 });
    renameSync(temp, items);
  } finally { if (existsSync(temp)) unlinkSync(temp); }
  return { items: countLines(items) };
});
step('carriage', () => run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'measure_carried_material.mjs'), '--cms', args.cms, '--foreign', args.foreign, '--frontier', args['frontier-db'], '--apply'], { timeout: 1800 }));
step('archive', () => {
  mkdirSync(dirname(args['archive-db']), { recursive: true });
  return run([b('bonfyre-web-address'), 'archive-items', '--items', items], { env_extra: { BONFYRE_ITEM_ARCHIVE_DB: args['archive-db'] } });
});
step('neighbourhood', () => run(['bun', p('cmd', 'BonfyreSurfaceScene', 'tools', 'compile_neighbourhood.mjs'), '--items', items, '--db', args['archive-db'], '--k', '8', '--tile', '2048', '--backend', args['compute-backend'], '--max-km', '4'], { timeout: 3600 }));
step('atlas', () => run([b('bonfyre-latent-atlas'), 'project-items', '--db', args['atlas-db'], '--items', items]));
step('kin', () => run(['bun', p('cmd', 'BonfyreSurfaceScene', 'tools', 'compile_kin.mjs'), '--atlas', args['atlas-db'], '--db', args['archive-db'], '--k', '8', '--tile', '1024', '--min', '0.85', '--backend', args['compute-backend']], { timeout: 3600 }));
step('link', () => run(['bun', p('cmd', 'BonfyreLatentAtlas', 'tools', 'link_identities.mjs'), '--items', items, '--cms', args.cms, '--atlas', b('bonfyre-latent-atlas'), '--db', args['atlas-db']]));
step('acoustic', () => run(['bun', p('cmd', 'BonfyreLatentAtlas', 'tools', 'compile_acoustic_sweep.mjs'), '--items', items, '--atlas-db', args['atlas-db'], '--atlas-bin', b('bonfyre-latent-atlas'), '--project', b('bonfyre-project'), '--foreign', args.foreign, '--work', work, '--per-run', args['acoustic-per-run']], { timeout: 3600 }));
step('contributions', () => {
  const receipts = join(work, 'page-contributions.jsonl');
  mkdirSync(dirname(args['frontier-db']), { recursive: true });
  run([b('bonfyre-source-frontier'), 'census-link', '--db', args['frontier-db'], '--cms', args.cms]);
  runToFile([b('bonfyre-web-address'), 'page-contributions'], receipts, { env_extra: { BONFYRE_SOURCE_FRONTIER_DB: args['frontier-db'] }, timeout: 1800 });
  const surfaces = readFileSync(receipts, 'utf8').split(/\r?\n/).filter(x => x.includes('"surface_receipt"')).map(JSON.parse);
  const landed = run([b('bonfyre-source-frontier'), 'contributions', '--db', args['frontier-db'], '--jsonl', receipts]);
  return { routes: surfaces.length, thin_routes: surfaces.filter(x => x.visible_items < 3).map(x => x.surface_receipt), receipts: landed };
});
step('frontier-state', () => {
  const gaps = run([b('bonfyre-source-frontier'), 'gaps', '--db', args['frontier-db']]);
  const attention = run([b('bonfyre-source-frontier'), 'attention', '--db', args['frontier-db'], '--days', '7']);
  const starvation = run([b('bonfyre-source-frontier'), 'starvation', '--db', args['frontier-db'], '--days', '7', '--limit', '5']);
  const forward = raw([b('bonfyre-source-frontier'), 'forward', '--db', args['frontier-db'], '--limit', '60'], { timeout: 600 });
  writeFileSync(join(args.state, 'source-forward.jsonl'), forward.stdout);
  const report = run([b('bonfyre-source-frontier'), 'report', '--db', args['frontier-db']]);
  for (const [name, value] of [['source-gaps', gaps], ['source-attention', attention], ['source-starvation', starvation], ['source-frontier-report', report]]) jwrite(join(args.state, `${name}.json`), value);
  return { report: report && typeof report === 'object' ? Object.fromEntries(['roots', 'families', 'channels'].map(k => [k, report[k]])) : report,
    starved: starvation && typeof starvation === 'object' ? Object.fromEntries(Object.entries(starvation).filter(([, v]) => v && typeof v === 'object').map(([k, v]) => [k, v.count])) : starvation };
});
step('subject-fabric', () => {
  mkdirSync(dirname(args['subject-fabric-db']), { recursive: true });
  const argv = ['bun', p('cmd', 'BonfyreExternalEstate', 'tools', 'precompute_subject_fabric.mjs'), '--cms-db', args.cms, '--demand-db', args['external-estate-db'], '--frontier-db', args['frontier-db'], '--scoped-demand-bin', b('bonfyre-scoped-demand'), '--surface-scene-bin', b('bonfyre-surface-scene'), '--scene-db', args.cms,
    ...['life', 'live', 'shop', 'work', 'internet'].flatMap(s => ['--subject', `institution:aurekai:${s}`]), '--out', args['subject-fabric-db'], '--manifest', join(args.state, 'subject-fabric-summary.json')];
  if (existsSync(args['scoped-frontier-db'])) argv.push('--scoped-frontier-db', args['scoped-frontier-db']);
  return run(argv, { timeout: 3600 });
});
step('subject-projection', () => {
  for (const dependency of ['census-items', 'subject-fabric']) if (!only || only.has(dependency)) requireCurrent(dependency);
  const common = ['--fabric-db', args['subject-fabric-db'], '--cms-db', args.cms, '--items', items, '--page-composition', p('cmd', 'BonfyreWebAddress', 'generated', 'native-page-composition.json'), '--surface-programs', p('cmd', 'BonfyreWebAddress', 'generated', 'native-surface-programs.json'), '--manifest', join(args.state, 'subject-projection-summary.json')];
  const compiled = run(['bun', p('cmd', 'BonfyreExternalEstate', 'tools', 'compile_subject_projection.mjs'), ...common], { timeout: 3600 });
  // The compiler's zero-op path already ran the independent complete verifier
  // over the same input snapshot and stored its receipt. On changed material
  // retain the separate post-commit verifier. Never accept an unchecked skip.
  const reuse = compiled?.owner_operations === 0 &&
    compiled?.skipped === 'verified_input_closure_and_committed_projection_unchanged' &&
    compiled?.verification?.schema === 'bonfyre.subject-projection-verification.v1' &&
    compiled?.verification?.verified === true &&
    compiled?.verification?.input_digest === compiled?.input_digest &&
    compiled?.verification?.projection_digest === compiled?.projection_digest;
  const verified = reuse ? compiled.verification : run(['bun', p('cmd', 'BonfyreExternalEstate', 'tools', 'verify_subject_projection.mjs'), ...common], { timeout: 3600 });
  return { compiled, verified, reused_verified_owner_cut: reuse };
});
step('qualify', () => {
  const rows = join(work, 'qualified-sources.json');
  const asked = run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'qualify_channels.mjs'), '--db', args['frontier-db'], '--frontier', b('bonfyre-source-frontier'), '--project', b('bonfyre-project'), '--foreign', args.foreign, '--cms', args.cms, '--out', rows, '--limit', args['qualify-per-run'], '--evidence', join(args.state, 'qualify-evidence.jsonl')], { timeout: 3600 });
  const applied = run(['bun', p('cmd', 'BonfyreWebAddress', 'tools', 'apply_census_sources.mjs'), '--json', rows, '--cms', b('bonfyre-cms'), '--db', args.cms, '--schemas', args['cms-schemas']]);
  return { asked, applied };
});
step('warm', () => run([b('bonfyre-web-address'), 'page-warm', '--site-root', args['site-root'], '--host', 'aurekai.life'], { env_extra: { BONFYRE_SOURCE_FRONTIER_DB: args['frontier-db'], BONFYRE_ITEM_ARCHIVE_DB: args['archive-db'] }, timeout: 900 }));
step('connection-describe', () => {
  if (!args['workgraph-db']) return { skipped: 'configure --workgraph-db or BONFYRE_WORKGRAPH_DB; legacy weekly needs remains available' };
  const queue = b('bonfyre-queue');
  const candidates = connectionCandidates();
  const [seeded, before, digest] = seedConnection(queue, candidates);
  const reaped = raw([queue, 'reap-expired', '--db', args['workgraph-db']], { timeout: 120 });
  if (reaped.code !== 0) {
    let payload = {}; try { payload = JSON.parse(reaped.stdout.trim().split(/\r?\n/).at(-1)); } catch {}
    if (payload.error_code !== 'no_expired_leases') throw Error(`bonfyre-queue reap-expired exited ${reaped.code}: ${reaped.stderr.trim().slice(0, 300)} ${reaped.stdout.trim().slice(0, 300)}`);
  }
  const claimed = runJsonLines([queue, 'claim-batch', `standing-connection-${process.pid}`, '--family', 'connection-describe', '--count', args['connection-describe-per-run'], '--lease-ms', args['connection-describe-lease-ms'], '--db', args['workgraph-db']]);
  if (!claimed.length) return { candidates: candidates.length, seed: seeded, before, claimed: 0, candidate_sha256: digest };
  const hostFile = join(work, 'connection-describe-hosts.txt');
  const finishFile = join(work, 'connection-describe-finish.tsv');
  const hosts = claimed.map(c => hostOf(c.mission_id));
  writeFileSync(hostFile, hosts.map(h => `${h}\n`).join(''));
  const discover = p('cmd', 'BonfyreCredentials', 'tools', 'discover_credential_needs.mjs');
  const descriptorEvidence = join(args.state, 'credential-needs-evidence.jsonl');
  const argv = ['bun', discover, '--project', b('bonfyre-project'), '--credentials', b('bonfyre-credentials'), '--db', args['credentials-db'], '--cms', args.cms, '--foreign', args.foreign, '--evidence', descriptorEvidence, '--host-file', hostFile];
  if (args['frontier-db']) argv.push('--frontier-db', args['frontier-db']);
  let observed, demandBook, activation;
  try {
    observed = run(argv, { timeout: 3600 });
    const estateDir = dirname(resolve(args.cms));
    const demandEvidence = join(estateDir, 'demand-evidence.jsonl');
    demandBook = run(['sh', p('components', 'BonfyreProject', 'tools', 'demand-match.sh'), demandEvidence], { env_extra: { ESTATE_DIR: estateDir, DEMAND_DESCRIPTOR_EVIDENCE: descriptorEvidence, BONFYRE_FOREIGN_ROOT: args.foreign }, timeout: 600 });
    const ownerDb = activationDb();
    activation = ownerDb ? { projection: run([b('bonfyre-project'), 'activation', 'project-demand', demandEvidence, '--db', ownerDb], { timeout: 600 }), reconciliation: run([b('bonfyre-project'), 'activation', 'reconcile', '--db', ownerDb], { timeout: 600 }) }
      : { skipped: 'no existing shared ResourceActivation owner DB was explicitly configured or detected' };
  } catch (error) {
    writeFileSync(finishFile, claimed.map(c => `${c.mission_id}\t${c.node_id}\t${c.worker_id}\t${c.claim_token}\t\n`).join(''));
    raw([queue, 'fail-batch', finishFile, '--class', 'transient', '--message', 'connection descriptor or DemandBook projection failed', '--db', args['workgraph-db']], { timeout: 120 });
    throw error;
  }
  writeFileSync(finishFile, claimed.map((c, i) => `${c.mission_id}\t${c.node_id}\t${c.worker_id}\t${c.claim_token}\tbonfyre://connection-observation/${tokenOf(hosts[i])}\n`).join(''));
  const ack = run([queue, 'ack-batch', finishFile, '--db', args['workgraph-db']], { timeout: 120 });
  return { candidates: candidates.length, seed: seeded, before, claimed: claimed.length, observed, demand_book: demandBook, resource_activation: activation, ack, candidate_sha256: digest };
});
step('credentials', () => {
  const keyed = ['--db', args['credentials-db'], '--key', args['credentials-key'], '--providers', args.providers];
  return { bootstrap: run([b('bonfyre-credentials'), 'bootstrap', ...keyed]), verify: run([b('bonfyre-credentials'), 'verify-all', ...keyed]) };
});
step('calendars', () => {
  const rows = join(work, 'event-sources.json');
  const found = run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'probe_event_channels.mjs'), '--project', b('bonfyre-project'), '--foreign', args.foreign, '--out', rows, '--channels', join(work, 'event-channels.jsonl'), '--cms', args.cms, '--frontier', args['frontier-db'], '--evidence', join(args.state, 'calendar-evidence.jsonl')], { timeout: 7200 });
  const applied = run(['bun', p('cmd', 'BonfyreWebAddress', 'tools', 'apply_census_sources.mjs'), '--json', rows, '--cms', b('bonfyre-cms'), '--db', args.cms, '--schemas', args['cms-schemas']]);
  const imported = run([b('bonfyre-source-frontier'), 'import', '--db', args['frontier-db'], '--label', 'EVENT_CALENDAR', '--jsonl', join(work, 'event-channels.jsonl')]);
  return { found, applied, imported };
}, true);
step('reclaim', () => run(['bun', p('scripts', 'reclaim_space.mjs'), '--apply', '--json', '--keep-raw-days', '7'], { timeout: 3600 }));
step('lattice', () => {
  const found = join(work, 'publisher-lattice.jsonl');
  const expanded = run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'expand_publisher_lattice.mjs'), '--cms', args.cms, '--foreign', args.foreign, '--out', found], { timeout: 3600 });
  return { expanded, imported: run([b('bonfyre-source-frontier'), 'import', '--db', args['frontier-db'], '--label', 'PUBLISHER_LATTICE', '--jsonl', found]) };
}, true);
step('free-backfill', () => run(['bun', p('cmd', 'BonfyreSourceFrontier', 'tools', 'close_gaps_freely.mjs'), '--db', args['frontier-db'], '--frontier', b('bonfyre-source-frontier'), '--project', b('bonfyre-project'), '--foreign', args.foreign, '--limit', '60', '--evidence', join(args.state, 'free-backfill-evidence.jsonl')], { timeout: 3600 }), true);
step('needs', () => {
  if (args['workgraph-db']) return { superseded_by: 'connection-describe', reason: 'WorkGraph owns host selection/lease/retry' };
  const argv = ['bun', p('cmd', 'BonfyreCredentials', 'tools', 'discover_credential_needs.mjs'), '--project', b('bonfyre-project'), '--credentials', b('bonfyre-credentials'), '--db', args['credentials-db'], '--cms', args.cms, '--foreign', args.foreign, '--evidence', join(args.state, 'credential-needs-evidence.jsonl')];
  if (args['frontier-db']) argv.push('--frontier-db', args['frontier-db']);
  return run(argv, { timeout: 3600 });
}, true);
record.finished = new Date().toISOString().replace(/[-:]/g, '').replace(/\..*/, 'Z');
const path = join(args.state, 'runs', `${stamp}.json`);
jwrite(path, record);
console.log(JSON.stringify({ run: path, steps: record.steps.map(s => [s.step, s.ok]) }));
process.exit(record.steps.every(s => s.ok) ? 0 : 1);
