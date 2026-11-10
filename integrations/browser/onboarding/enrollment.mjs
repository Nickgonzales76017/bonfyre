#!/usr/bin/env bun
// Account enrollment as native work. Each item is one capability for one
// identity at one provider (400 of them: 200 targets for Gmail and Proton),
// held in BonfyreQueue as its own mission. The browser only presents an item:
// a closed tab, window or panel loses nothing, because the item, its lease
// and its revision-checked checkpoint live in the queue. Keys go to
// BonfyreCredentials under the identity's own account, on standard input.
//
//   bun integrations/browser/onboarding/enrollment.mjs seed
//   bun integrations/browser/onboarding/enrollment.mjs migrate            (the retired launcher's progress)
//   bun integrations/browser/onboarding/enrollment.mjs next|resume --identity gmail|proton
//   bun integrations/browser/onboarding/enrollment.mjs record --identity I --stage S [--note N]
//   bun integrations/browser/onboarding/enrollment.mjs wait --identity I --why email|mfa|review [--note N]
//   bun integrations/browser/onboarding/enrollment.mjs decide --identity I --why captcha|paid|unavailable|terms [--note N]
//   bun integrations/browser/onboarding/enrollment.mjs seal --identity I --provider P --field F     (value on stdin)
//   bun integrations/browser/onboarding/enrollment.mjs done --identity I
//   bun integrations/browser/onboarding/enrollment.mjs status
//
// Nothing here creates an account, signs in, solves a check or accepts terms.
// Those stay with the person at the provider's own page.
import { readFileSync, writeFileSync, mkdirSync, existsSync, renameSync, rmSync, chmodSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { Database } from 'bun:sqlite';
import { assertReviewedItem, humanStage, completionBlockers } from '../extension/enrollment-integrity.mjs';
import { pathToFileURL } from 'node:url';
import { readCheckpointMaterial, writeCheckpointMaterial } from './checkpoint-material.mjs';
import { enrollmentFields } from './enrollment-fields.mjs';
import { listEnrollmentWork, inspectEnrollmentWork } from './enrollment-work.mjs';

const repo = join(import.meta.dir, '../../..');
export const IDENTITIES = { gmail: 'nickgonzales76017@gmail.com', proton: 'nickgonzales76017@proton.me' };
export const STAGES = ['existing_access_checked', 'account_bound', 'credential_sealed', 'credential_verified', 'scope_confirmed', 'consumer_bound'];
// Waiting releases the item's place for a while and it comes back by itself; a decision holds it for the person.
export const WAITS = { email: 'waiting for an email to verify', mfa: 'waiting for a second-factor code', review: 'waiting for the provider to review an application' };
export const DECISIONS = { captcha: 'a person must complete a human check', paid: 'needs a decision about a paid plan', unavailable: 'the provider offers no developer access here', terms: 'terms need to be read and accepted by a person' };
const LEASE_MS = 20 * 60_000, WAIT_RETRIES = 40;

export function config(env = process.env) {
  const home = env.HOME ?? homedir(), root = env.BONFYRE_ENROLLMENT_HOME ?? join(home, '.bonfyre/onboarding/native');
  const declared = env.BONFYRE_CREDENTIAL_PROVIDERS, legacyDeclared = env.BONFYRE_CREDENTIALS_PROVIDERS;
  if (declared && legacyDeclared && declared !== legacyDeclared)
    throw Error('Credential provider paths disagree between the gateway and enrollment carrier. Resolve the configuration before opening work.');
  return { root, db: env.BONFYRE_ENROLLMENT_DB ?? join(root, 'enrollment.db'),
    queue: env.BONFYRE_QUEUE_BINARY ?? join(repo, 'cmd/BonfyreQueue/bonfyre-queue'),
    credentials: env.BONFYRE_CREDENTIALS_BINARY ?? join(repo, 'cmd/BonfyreCredentials/bonfyre-credentials'),
    credentialsDb: env.BONFYRE_CREDENTIALS_DB ?? join(home, '.bonfyre/credentials/credentials.db'),
    credentialsKey: env.BONFYRE_CREDENTIALS_KEY ?? join(home, '.bonfyre/credentials/credentials.key'),
    providers: declared ?? legacyDeclared ?? join(repo, 'cmd/BonfyreCredentials/provider-credentials.json'),
    ledger: env.BONFYRE_ENROLLMENT_LEDGER ?? join(repo, 'docs/engineering/handoff-backfill-20261019/onboarding-enrollment.tsv'),
    legacy: env.BONFYRE_ENROLLMENT_LEGACY ?? join(home, '.bonfyre/onboarding') };
}

const privateDir = dir => { mkdirSync(dir, { recursive: true, mode: 0o700 }); chmodSync(dir, 0o700); return dir; };
const writePrivate = (file, text) => { const tmp = file + '.' + process.pid + '.tmp'; writeFileSync(tmp, text, { mode: 0o600 }); renameSync(tmp, file); };
function tsv(text) { const [head, ...rows] = text.trim().split('\n'); const cols = head.split('\t'); return rows.map(r => Object.fromEntries(r.split('\t').map((v, i) => [cols[i], v]))); }

function run(argv, { input, env } = {}) {
  const p = Bun.spawnSync(argv, { stdin: input == null ? 'ignore' : Buffer.from(input), stdout: 'pipe', stderr: 'pipe', env: env ?? process.env });
  return { code: p.exitCode, out: p.stdout.toString(), err: p.stderr.toString() };
}
function queue(c, args) {
  const r = run([c.queue, ...args, '--db', c.db]);
  let body = null; try { body = JSON.parse(r.out.trim().split('\n').at(-1) || 'null'); } catch {}
  if (r.code !== 0 || body?.result === 'error') throw Error(`the queue refused ${args[0]}: ${(body?.error_message || r.err || r.out).trim().slice(0, 300)}`);
  return body;
}
const mission = workId => `enroll-${workId}`;
const family = identity => `enrollment-${identity}`;
const leaseFile = (c, identity) => join(privateDir(join(c.root, 'leases')), `${identity}.json`);
const checkpointFile = (c, workId) => join(privateDir(join(c.root, 'checkpoints')), `${workId}.json`);
function identityOf(v) { if (!Object.hasOwn(IDENTITIES, v)) throw Error('name the identity: gmail or proton'); return v; }

// ------------------------------------------------------------ seeding

// Every ledger row becomes one mission, once. Earlier priority goes to accounts whose history should be recovered
// before anything new is made, then to the more open tiers. Re-seeding changes nothing already there.
export function seed(c = config()) {
  privateDir(c.root);
  const rows = tsv(readFileSync(c.ledger, 'utf8'));
  if (rows.length !== 400) throw Error(`the enrollment ledger has ${rows.length} rows, not 400`);
  const known = new Set(); if (existsSync(c.db)) { const db = new Database(c.db, { readonly: true }); try { for (const r of db.query("SELECT mission_id FROM workgraph_nodes WHERE family LIKE 'enrollment-%'").all()) known.add(r.mission_id); } finally { db.close(); } }
  const lines = [];
  for (const r of rows) {
    if (!/^https:\/\//.test(r.signup_url)) throw Error(`${r.work_id} has no signup address`);
    const priority = (r.supplied_state === 'recover_history' ? 900 : 500) - 10 * (+r.source_tier || 5) - (r.supplied_state === 'shares_account_candidate' ? 100 : 0);
    const input = JSON.stringify({ work_id: r.work_id, source_id: r.source_id, platform: r.platform, signup_url: r.signup_url, identity: r.identity, account: r.credential_account,
      api_gate: r.api_gate, supplied_state: r.supplied_state, account_group: r.account_group, credential_provider: r.credential_provider || '' });
    if (/[\t\n]/.test(input)) throw Error(`${r.work_id} carries a tab or line break`);
    lines.push([mission(r.work_id), 'account.enrollment', input, family(r.identity), priority, WAIT_RETRIES, 600000].join('\t'));
  }
  // One batch, idempotent in the queue: a mission already there is left as it is, with its progress.
  const file = join(c.root, 'seed.tsv'); writePrivate(file, lines.join('\n') + '\n');
  const r = run([c.queue, 'enqueue-batch', file, '--db', c.db]); rmSync(file, { force: true });
  if (r.code !== 0) throw Error(`the queue refused the batch: ${(r.err || r.out).trim().slice(0, 300)}`);
  const added = rows.filter(x => !known.has(mission(x.work_id))).length, kept = rows.length - added;
  return { added, kept, families: Object.keys(IDENTITIES).map(family) };
}
function existsMission(c, id) { if (!existsSync(c.db)) return false; const db = new Database(c.db, { readonly: true }); try { return !!db.query('SELECT 1 FROM workgraph_nodes WHERE mission_id=?').get(id); } finally { db.close(); } }

// The retired launcher kept a cursor and per-item statuses. A visit it recorded is carried as a note on the item's
// checkpoint, never as a completed stage: closing a page proved nothing about an account.
export function migrate(c = config()) {
  const progress = join(c.legacy, 'progress.tsv'), carried = [];
  const lines = existsSync(progress) ? readFileSync(progress, 'utf8').split('\n').filter(l => l.trim()) : [];
  for (const line of lines) {
    const [lane, id, status, keys = ''] = line.split('\t');
    if (!IDENTITIES[lane] || !/^\d{3}$/.test(id)) continue;
    const file = checkpointFile(c, `${id}-${lane}`), prior = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : { stages: {} };
    prior.legacy = { launcher_status: status, key_names: keys.split(',').filter(Boolean), note: 'recorded by the retired launcher; not evidence of an account or a working key' };
    writePrivate(file, JSON.stringify(prior, null, 1)); carried.push(`${id}-${lane}`);
  }
  const cursor = join(c.legacy, 'cursor.tsv');
  return { carried: carried.length, items: carried, cursor_ignored: existsSync(cursor), why: 'the queue orders the work; a launcher cursor is not a place in it' };
}

// ------------------------------------------------------------ one item at a time per identity

function savedMaterial(c, workId, cp) {
  const legacy = checkpointFile(c, workId);
  const uri = cp.artifact_uri || (existsSync(legacy) ? pathToFileURL(legacy).href : null);
  return uri ? readCheckpointMaterial(c.root, workId, uri) : { stages: {} };
}
function readLease(c, identity) { const f = leaseFile(c, identity); return existsSync(f) ? JSON.parse(readFileSync(f, 'utf8')) : null; }
function declaredFields(c, provider) {
  try { const info = statSync(c.providers); if (!info.isFile() || info.size > 1024 * 1024) return null; return enrollmentFields(readFileSync(c.providers, 'utf8'), provider); }
  catch { return null; }
}
function present(c, lease, how, expected) {
  const workId = lease.mission.replace(/^enroll-/, ''), identity = workId.endsWith('-proton')?'proton':'gmail';
  const view = inspect(identity,workId,c);
  // Holding work and saving a checkpoint are separate native operations. Keep
  // the acquired hold, but do not adopt a newer checkpoint as already reviewed.
  const changed = expected !== undefined && view.revision !== expected.revision;
  return {...view,how:changed?'review-required':how,read_only:changed||view.status!=='running',lease_expires_at_ms:lease.lease_expires_at_ms,
    ...(changed?{review_required:true,reviewed_revision:expected.revision}:{})};
}

// The panel calls this whenever it opens: an item it was showing comes back as it was (presentation lost is not
// work lost). Only when this identity holds nothing does the queue hand out the next item.
export function resume(identity, c = config()) {
  identityOf(identity);
  const lease = readLease(c, identity);
  if (lease) {
    const renewed = run([c.queue, 'renew', lease.mission, 'job', lease.worker, lease.token, '--lease-ms', String(LEASE_MS), '--db', c.db]);
    let body = null; try { body = JSON.parse(renewed.out.trim().split('\n').at(-1)); } catch {}
    if (renewed.code === 0 && body?.result === 'ok' && body.status === 'running') { lease.lease_expires_at_ms = body.lease_expires_at_ms; writePrivate(leaseFile(c, identity), JSON.stringify(lease)); return present(c, lease, 'resumed'); }
    if (!['stale_claim', 'not_found'].includes(body?.result))
      throw Error('The queue did not confirm the current hold. Its reference was kept; no replacement item was claimed.');
    rmSync(leaseFile(c, identity), { force: true });
    return { ...inspect(identity, lease.mission.replace(/^enroll-/, ''), c), previous: { mission: lease.mission, why: 'its hold lapsed; inspect this same item before choosing whether to continue' } };
  }
  return null;
}
export function next(identity, c = config()) {
  identityOf(identity);
  const held = readLease(c, identity); if (held) return resume(identity, c);
  run([c.queue, 'reap-expired', '--db', c.db]);
  const worker = `panel-${identity}`, claimed = queue(c, ['claim-next', worker, '--family', family(identity), '--lease-ms', String(LEASE_MS)]);
  if (!claimed?.claim_token) return { how: 'empty', identity, why: 'nothing is ready for this identity: everything is done, waiting, or needs a decision' };
  const lease = { mission: claimed.mission_id, worker, token: claimed.claim_token, lease_expires_at_ms: claimed.lease_expires_at_ms };
  writePrivate(leaseFile(c, identity), JSON.stringify(lease));
  return present(c, lease, 'next');
}
function held(identity, c) { const lease = readLease(c, identityOf(identity)); if (!lease) throw Error(`${identity} holds no item; ask for the next one`); return lease; }

// Browsing a lane reads the Queue. Opening a saved setup never claims another
// item, clears a decision, changes a checkpoint or opens a provider tab.
export function list(identity, options = {}, c = config()) {
  identityOf(identity);
  return listEnrollmentWork(c, identity, IDENTITIES[identity], options);
}
export function inspect(identity, workId, c = config()) {
  identityOf(identity);
  const view = inspectEnrollmentWork(c, identity, IDENTITIES[identity], workId);
  if (!view.item.credential_provider && typeof c.resolveProvider === 'function') view.item.credential_provider = c.resolveProvider(view.item);
  const fields = declaredFields(c, view.item.credential_provider);
  return {...view, credential_fields:fields ?? [], credential_fields_available:fields !== null};
}
export function continueWork(identity, expected, c = config()) {
  identityOf(identity);
  const view = inspect(identity, expected?.work_id, c), target = mission(view.item.work_id);
  assertReviewedItem(identity, target, view.revision, expected);
  const prior = readLease(c, identity);
  if (prior && prior.mission !== target)
    throw Error('Another setup is held in this identity. Return to that item or set it aside before continuing this one.');
  if (prior) {
    const renewed = run([c.queue, 'renew', target, 'job', prior.worker, prior.token, '--lease-ms', String(LEASE_MS), '--db', c.db]);
    let body; try {body = JSON.parse(renewed.out.trim().split('\n').at(-1));} catch {}
    if (renewed.code === 0 && body?.result === 'ok' && body.status === 'running') {
      prior.lease_expires_at_ms = body.lease_expires_at_ms;
      writePrivate(leaseFile(c, identity), JSON.stringify(prior));
      return present(c, prior, 'continued', expected);
    }
    if (!['stale_claim','not_found'].includes(body?.result))
      throw Error('The queue did not confirm this hold. Reload its status before continuing.');
    rmSync(leaseFile(c, identity), {force:true});
  }
  // Only this selected mission may be reclaimed. The native scheduler still
  // enforces retry time, dependencies, cancellation, attempts and live leases.
  const reaped = run([c.queue, 'reap-expired', target, '--db', c.db]);
  let reaping; try {reaping = JSON.parse(reaped.out.trim().split('\n').at(-1));} catch {}
  if (!(reaped.code === 0 && reaping?.result === 'ok') &&
      !(reaping?.result === 'not_eligible' && reaping.error_code === 'no_expired_leases'))
    throw Error('The native queue did not confirm this item’s readiness. Reload before continuing.');
  const worker = `panel-${identity}`;
  const claim = run([c.queue, 'claim', target, 'job', worker, '--family', family(identity), '--lease-ms', String(LEASE_MS), '--db', c.db]);
  let claimed; try {claimed = JSON.parse(claim.out.trim().split('\n').at(-1));} catch {}
  if (claim.code !== 0 || claimed?.result !== 'ok' || !claimed.claim_token || claimed.mission_id !== target || claimed.node_id !== 'job')
    throw Error('The selected item is not ready to continue. Read its current native status.');
  const lease = {mission:target, worker, token:claimed.claim_token, lease_expires_at_ms:claimed.lease_expires_at_ms};
  writePrivate(leaseFile(c, identity), JSON.stringify(lease));
  return present(c, lease, 'continued', expected);
}

function reviewed(identity, c, expected) {
  const lease = held(identity, c), node = queue(c, ['mission', lease.mission]);
  const cp = queue(c, ['checkpoint-read', lease.mission, 'job']);
  assertReviewedItem(identity, lease.mission, cp.revision, expected);
  const item = JSON.parse(node.input_uri);
  if (!item.credential_provider && typeof c.resolveProvider === 'function') item.credential_provider = c.resolveProvider(item);
  if (node.status !== 'running' || node.family !== family(identity) || item.identity !== identity || item.account !== IDENTITIES[identity])
    throw Error('The native item is not held in this identity. Reload before changing it.');
  const renewed = queue(c, ['renew', lease.mission, 'job', lease.worker, lease.token, '--lease-ms', String(LEASE_MS)]);
  if (renewed?.result !== 'ok' || renewed.status !== 'running') throw Error('The native claim is not live. Nothing was changed.');
  return { lease, cp, item };
}
function saveCheckpoint(c, lease, change, expected) {
  const cp = queue(c, ['checkpoint-read', lease.mission, 'job']), workId = lease.mission.replace(/^enroll-/, '');
  assertReviewedItem(workId.endsWith('-proton') ? 'proton' : 'gmail', lease.mission, cp.revision, expected);
  const base = savedMaterial(c, workId, cp);
  const nextState = change(structuredClone({ stages: {}, notes: [], ...base }));
  const artifact = writeCheckpointMaterial(c.root, workId, cp.revision + 1, nextState);
  // Never overwrite a candidate that another successful native CAS may reference.
  // Keep unacknowledged immutable material on an uncertain response; the queue owns the pointer.
  const saved = queue(c, ['checkpoint-save', lease.mission, 'job', lease.worker, lease.token, '--expected-revision', String(cp.revision), '--artifact-uri', artifact]);
  return { revision: saved.revision ?? cp.revision + 1, state: nextState };
}
export function record(identity, stage, note, c = config(), expected) {
  const observation = humanStage(stage, note ?? '');
  const { lease } = reviewed(identity, c, expected);
  return saveCheckpoint(c, lease, s => { s.stages[stage] = observation; return s; }, expected);
}
function release(identity, c, cls, why, note, expected) {
  const { lease } = reviewed(identity, c, expected);
  saveCheckpoint(c, lease, s => { s.notes.push({ at: new Date().toISOString(), why, note: String(note ?? '').slice(0, 400) }); s.interruption = why; return s; }, expected);
  queue(c, ['fail', lease.mission, 'job', lease.worker, lease.token, '--class', cls, '--message', why]);
  rmSync(leaseFile(c, identity), { force: true });
  return { released: lease.mission, why, comes_back: cls === 'resource' ? 'by itself after a while' : 'when you decide; it is listed under decisions' };
}
export const wait = (identity, why, note, c = config(), expected) => { if (!Object.hasOwn(WAITS, why)) throw Error(`waiting is for ${Object.keys(WAITS).join(', ')}`); return release(identity, c, 'resource', WAITS[why], note, expected); };
export const decide = (identity, why, note, c = config(), expected) => { if (!Object.hasOwn(DECISIONS, why)) throw Error(`a decision is for ${Object.keys(DECISIONS).join(', ')}`); return release(identity, c, 'dependency', DECISIONS[why], note, expected); };

// A key goes from the panel's stdin to the credential boundary's stdin, under this identity's account. It is
// never in an argument, a checkpoint or a reply; the checkpoint records only that a field was sealed.
export function seal(identity, provider, field, value, c = config(), expected) {
  const { lease, item } = reviewed(identity, c, expected);
  if (!/^[a-z0-9_-]{1,64}$/.test(provider ?? '') || !/^[a-z0-9_]{1,64}$/.test(field ?? '')) throw Error('name the declared provider and field');
  if (item.credential_provider !== provider)
    throw Error('This native capability has no matching credential-provider binding. No key was written.');
  if (typeof value !== 'string' || !value.trim() || Buffer.byteLength(value, 'utf8') >= 4095 || value.includes('\0'))
    throw Error('Provide one nonempty credential smaller than the native field limit.');
  const help = run([c.credentials, '--help']);
  if (!(help.out + help.err).includes('--account')) throw Error('This credential executable has no account isolation. No key was written.');
  const r = run([c.credentials, 'set', '--db', c.credentialsDb, '--key', c.credentialsKey, '--providers', c.providers, '--provider', provider, '--field', field, '--account', IDENTITIES[identity], '--actor', `enrollment-panel-${identity}`], { input: String(value).trim() + '\n' });
  if (r.code !== 0) throw Error(/unknown provider/.test(r.err) || r.code === 1 ? `the credential boundary refused it: ${provider}/${field} is not a declared provider field, or the store could not be opened. Declare it in provider-credentials first.` : r.err.trim().slice(0, 300));
  return saveCheckpoint(c, lease, s => { s.stages.credential_sealed = { at: new Date().toISOString(), note: `${provider}/${field} sealed for ${IDENTITIES[identity]}`, evidence_kind: 'native_seal', owner: 'BonfyreCredentials' }; return s; }, expected);
}

// Completion requires owner-backed milestones on the exact reviewed checkpoint.
// Human assertions alone can never complete a capability enrollment.
export function done(identity, c = config(), expected) {
  const { lease, cp } = reviewed(identity, c, expected);
  const state = savedMaterial(c, lease.mission.replace(/^enroll-/, ''), cp);
  const missing = completionBlockers(state.stages);
  if (missing.length) throw Error(`Enrollment is not complete: ${missing.join(', ')}. Keep it open or set it aside; no completion was recorded.`);
  queue(c, ['ack', lease.mission, 'job', lease.worker, lease.token, '--checkpoint-revision', String(cp.revision), '--output-uri', cp.artifact_uri || 'file://' + checkpointFile(c, lease.mission.replace(/^enroll-/, ''))]);
  rmSync(leaseFile(c, identity), { force: true });
  return { finished: lease.mission, stages: Object.keys(state.stages), not_reached: STAGES.filter(s => !state.stages[s]) };
}

export function status(c = config()) {
  if (!existsSync(c.db)) return { seeded: false };
  const db = new Database(c.db, { readonly: true });
  try {
    const rows = db.query("SELECT family, status, failure_class, failure_message, COUNT(*) n FROM workgraph_nodes WHERE family LIKE 'enrollment-%' GROUP BY family, status, failure_class, failure_message").all();
    const decisions = db.query("SELECT mission_id, failure_message FROM workgraph_nodes WHERE family LIKE 'enrollment-%' AND status='dead_letter' AND failure_class='dependency' ORDER BY mission_id").all();
    const by = {};
    for (const r of rows) { const k = r.family.replace('enrollment-', ''), word = r.status === 'ready' ? 'ready' : r.status === 'running' ? 'open' : r.status === 'retry_wait' ? 'waiting' : r.status === 'succeeded' || r.status === 'complete' || r.status === 'completed' ? 'done' : r.failure_class === 'dependency' ? 'needs a decision' : r.status;
      (by[k] ??= {})[word] = (by[k][word] ?? 0) + r.n; }
    return { seeded: true, by_identity: by, decisions: decisions.map(d => ({ work: d.mission_id.replace(/^enroll-/, ''), why: d.failure_message })) };
  } finally { db.close(); }
}

if (import.meta.main) {
  const [verb, ...rest] = process.argv.slice(2), opt = n => { const i = rest.indexOf('--' + n); return i >= 0 ? rest[i + 1] : undefined; };
  const c = config(), expected = { work_id: opt('work'), revision: Number(opt('revision')) };
  const verbs = { seed: () => seed(c), migrate: () => migrate(c), status: () => status(c), next: () => next(opt('identity'), c), resume: () => resume(opt('identity'), c) ?? next(opt('identity'), c),
    list: () => list(opt('identity'), {filter:opt('filter'),search:opt('search'),cursor:opt('cursor')}, c), inspect: () => inspect(opt('identity'), opt('work'), c), continue: () => continueWork(opt('identity'), expected, c),
    record: () => record(opt('identity'), opt('stage'), opt('note'), c, expected), wait: () => wait(opt('identity'), opt('why'), opt('note'), c, expected), decide: () => decide(opt('identity'), opt('why'), opt('note'), c, expected),
    seal: async () => seal(opt('identity'), opt('provider'), opt('field'), await new Response(Bun.stdin.stream()).text(), c, expected), done: () => done(opt('identity'), c, expected) };
  if (!verbs[verb]) { console.error(`usage: enrollment.mjs ${Object.keys(verbs).join('|')} [--identity gmail|proton] ...`); process.exit(2); }
  try { console.log(JSON.stringify(await verbs[verb](), null, 1)); } catch (e) { console.error('enrollment: ' + e.message); process.exit(1); }
}
