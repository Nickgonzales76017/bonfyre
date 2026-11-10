#!/usr/bin/env bun
// CheapCognition over the SourceFrontier candidate queue.
//
// The exact qualifier observes every candidate channel: one fetch, a parse,
// a freshness and language check. Most fetches buy nothing (on 2026-10-16 the
// ledger held 105,666 refusals against 2,323 qualifier admissions). This asks
// the DecisionPlane, before the fetch, whether the observation is likely to
// yield a qualifying channel or a page that declares channels of its own.
//
//   exact mask   the qualifier's own candidate query and the publisher's
//                earlier outcomes, both read from SourceFrontier
//   cheap stage  a pinned local owner: logistic weights trained on the
//                qualifier's ordered evidence log, microseconds per case
//   exact stage  qualify_channels.mjs, unchanged; it still owns admission
//
// It proposes; it never admits, refuses or reorders by itself. Promotion past
// SHADOW needs assessSelectiveValue on a fixed-policy independent holdout:
// the receipts are written before the qualifier observes the same slice.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { assessDecisionCases, assessSelectiveValue, compileDecisionCase, decisionDigest,
  decisionPlanePolicy, evaluateDecisionOutcomes } from './decision-plane.mjs';

const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const MODEL_SCHEMA = 'bonfyre.channel-yield.logistic.v1';
const HOST_SLEEP_MS = 400; // qualify_channels sleeps this long after each observation on a host
// The qualifier's own candidate predicate (cmd_qualification_work and candidatesOf).
const CANDIDATE_SQL = "c.state IN ('shadow','candidate') AND COALESCE(c.refusal,'')='' " +
  "AND c.protocol NOT IN ('mcp','x402','graphql','openapi','soap')";

export function hostOf(address) {
  try { return new URL(address).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; }
}

// What the qualifier said, folded into the few outcomes that matter here.
export function refusalClass(refusal) {
  const text = String(refusal || '');
  if (!text) return 'none';
  if (text.startsWith('answered nothing')) return 'silent';
  if (text.startsWith('robots or observe policy')) return 'policy';
  if (text.startsWith('not a feed document')) return 'not_feed';
  if (text.startsWith('a comment thread')) return 'comments';
  if (text.startsWith('published in') || text.startsWith('states no language')) return 'language';
  if (text.startsWith('nothing published') || text.startsWith('nothing upcoming')) return 'stale';
  if (text.startsWith('fewer than three') || text.startsWith('most items have no title')) return 'thin';
  return 'other';
}
const declares = record => Array.isArray(record.declares) ? record.declares.length > 0 : /it declares \d+ channel/.test(String(record.refused || ''));
export const yielded = record => Boolean(record.admitted) || declares(record);

// Publisher history as of a moment: per host, how earlier observations ended.
export function emptyHistory() { return new Map(); }
export function addOutcome(history, address, { yields, refusal }) {
  const host = hostOf(address);
  if (!host) return;
  const entry = history.get(host) || { tried: 0, yielded: 0, classes: {} };
  entry.tried++; entry.yielded += Number(yields);
  const kind = yields ? 'yield' : refusalClass(refusal);
  entry.classes[kind] = (entry.classes[kind] || 0) + 1;
  history.set(host, entry);
}

// Production as-of history: everything SourceFrontier knows now.
export function frontierHistory(db) {
  const history = emptyHistory();
  for (const row of db.query(`SELECT exact_address, refusal, census_source FROM channel
      WHERE COALESCE(refusal,'')!='' OR census_source LIKE 'feed-%' OR census_source LIKE 'events-%'`).all())
    addOutcome(history, row.exact_address, { yields: !row.refusal || /it declares \d+ channel/.test(row.refusal), refusal: row.refusal });
  return history;
}

function evidenceFor(history, address) {
  const entry = history.get(hostOf(address)) || { tried: 0, yielded: 0, classes: {} };
  return { publisher_observed: entry.tried, publisher_yielded: entry.yielded,
    publisher_outcomes: Object.fromEntries(Object.entries(entry.classes).toSorted()) };
}

// The case the DecisionPlane sees: one exact frontier member, declared fields only.
export function channelCase(row, history, { gate = 'SHADOW', historyDigest }, policy = decisionPlanePolicy()) {
  const channel = { address: row.exact_address, protocol: row.protocol || 'unknown',
    material: row.material || 'publication', truth_level: row.truth_level || 'listed',
    discovered_from: String(row.discovered_from || '').slice(0, 160) };
  const evidence = evidenceFor(history, row.exact_address);
  return compileDecisionCase({ program: 'channel_qualification', owner: 'SourceFrontier',
    subject_id: `source-frontier/channel/${row.channel_id}`,
    mask_digest: decisionDigest({ predicate: CANDIDATE_SQL, channel_id: row.channel_id, channel, history: historyDigest }),
    gate_state: gate, information_policy_class: 'public',
    frontier: [{ id: row.channel_id, description: `${channel.protocol} channel ${channel.address}`.slice(0, 300),
      owner_ref: `SourceFrontier:channel/${row.channel_id}` }],
    state: { channel, evidence } }, policy);
}

// ---- the cheap owner ------------------------------------------------------

const bucket = n => n === 0 ? '0' : n < 3 ? '1-2' : n < 10 ? '3-9' : n < 50 ? '10-49' : '50+';
export function features({ channel, evidence }) {
  const out = [`protocol=${channel.protocol}`, `material=${channel.material}`, `truth=${channel.truth_level}`];
  const from = channel.discovered_from || '';
  out.push(`from=${from.startsWith('declared by') ? 'declared' : from.split(/[\s:/]/)[0].slice(0, 24) || 'none'}`);
  let url;
  try { url = new URL(channel.address); } catch { out.push('url=invalid'); return out; }
  const path = url.pathname.toLowerCase(), segments = path.split('/').filter(Boolean);
  out.push(`depth=${Math.min(segments.length, 6)}`, `query=${url.search ? 1 : 0}`, `scheme=${url.protocol}`);
  const last = segments.at(-1) || '';
  out.push(`ext=${(last.match(/\.([a-z0-9]{1,5})$/) || [, 'none'])[1]}`);
  for (const word of ['feed', 'rss', 'atom', 'comments', 'wp-json', 'calendar', 'events', 'ical', 'tag', 'category',
    'author', 'podcast', 'news', 'blog', 'api', 'sitemap', 'json', 'xml'])
    if (path.includes(word)) out.push(`path~${word}`);
  const tld = url.hostname.split('.').at(-1);
  out.push(`tld=${tld}`);
  out.push(`pub_observed=${bucket(evidence.publisher_observed)}`);
  out.push(`pub_yielded=${bucket(evidence.publisher_yielded)}`);
  if (evidence.publisher_observed) {
    const rate = evidence.publisher_yielded / evidence.publisher_observed;
    out.push(`pub_rate=${rate === 0 ? '0' : rate < 0.1 ? '<.1' : rate < 0.4 ? '<.4' : 'hi'}`);
    const [top] = Object.entries(evidence.publisher_outcomes).toSorted((a, b) => b[1] - a[1]);
    if (top) out.push(`pub_mostly=${top[0]}`);
    for (const kind of Object.keys(evidence.publisher_outcomes)) out.push(`pub_seen=${kind}`);
  }
  return out;
}

const sigmoid = z => 1 / (1 + Math.exp(-z));
export function trainLogistic(examples, { epochs = 6, rate = 0.15, l2 = 1e-5 } = {}) {
  const weights = new Map(); let bias = 0;
  const positives = examples.filter(item => item.label).length;
  bias = Math.log((positives + 1) / (examples.length - positives + 1));
  for (let epoch = 0; epoch < epochs; epoch++) {
    const step = rate / Math.sqrt(epoch + 1);
    for (const { features: keys, label } of examples) {
      let z = bias;
      for (const key of keys) z += weights.get(key) || 0;
      const gradient = sigmoid(z) - Number(label);
      bias -= step * gradient;
      for (const key of keys) {
        const w = weights.get(key) || 0;
        weights.set(key, w - step * (gradient + l2 * w));
      }
    }
  }
  return { schema: MODEL_SCHEMA, bias, weights: Object.fromEntries([...weights].filter(([, w]) => Math.abs(w) > 1e-6).toSorted()) };
}
export const predict = (model, keys) => sigmoid(keys.reduce((z, key) => z + (model.weights[key] || 0), model.bias));

// A DecisionPlane local owner bound to the exact weight file it was given.
export function logisticOwner(modelPath) {
  const bytes = readFileSync(modelPath), model = JSON.parse(bytes);
  if (model.schema !== MODEL_SCHEMA) throw new Error(`not a ${MODEL_SCHEMA} file: ${modelPath}`);
  return { model: 'channel-yield-logistic', artifact_digest: sha(bytes), deterministic: true,
    question_semantics: 'independent_evaluation', confidence_semantics: 'maximum_option_probability',
    evaluate: ({ state }) => ({ answers: { yields: { type: 'noul', noul: predict(model, features(state)) } } }) };
}

// ---- training and offline replay on the ordered evidence log ---------------

function readJsonl(path) {
  return readFileSync(path, 'utf8').split('\n').filter(Boolean).flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
}
function channelRows(db) {
  const rows = new Map();
  for (const row of db.query('SELECT channel_id, exact_address, protocol, material, truth_level, discovered_from FROM channel').all())
    if (!rows.has(row.exact_address)) rows.set(row.exact_address, row);
  return rows;
}

// Replays the log in order: each record is judged with only the history that
// existed before it, exactly as a live candidate would be.
export function replay(records, rows) {
  const history = emptyHistory(), out = [];
  for (const record of records) {
    const row = rows.get(record.address) || { channel_id: '', exact_address: record.address, protocol: record.protocol };
    const channel = { address: record.address, protocol: row.protocol || record.protocol || 'unknown',
      material: row.material || 'publication', truth_level: row.truth_level || 'listed', discovered_from: row.discovered_from || '' };
    out.push({ record, row, state: { channel, evidence: evidenceFor(history, record.address) }, label: yielded(record) });
    addOutcome(history, record.address, { yields: yielded(record), refusal: record.refused });
  }
  return out;
}

// Ordering loses nothing: every candidate is still observed, the likely ones
// first. Reported as yields found within a share of the observation budget,
// against the order the log actually ran in.
export function ordering(scored, shares = [0.1, 0.25, 0.5]) {
  const yields = scored.filter(item => item.label).length || 1;
  const ranked = scored.toSorted((a, b) => b.p - a.p);
  return shares.map(share => {
    const n = Math.round(scored.length * share);
    return { observation_budget_share: share,
      yields_found_ranked: ranked.slice(0, n).filter(item => item.label).length / yields,
      yields_found_as_run: scored.slice(0, n).filter(item => item.label).length / yields };
  });
}

export function sweep(scored, thresholds = [0.01, 0.02, 0.03, 0.05, 0.08, 0.12]) {
  const total = scored.length, yields = scored.filter(item => item.label).length;
  return thresholds.map(threshold => {
    const skipped = scored.filter(item => item.p < threshold), lost = skipped.filter(item => item.label).length;
    return { skip_below: threshold, observations_skipped: skipped.length, skipped_share: skipped.length / total,
      yields_lost: lost, yields_lost_share: yields ? lost / yields : 0 };
  });
}

function train(args) {
  const db = new Database(args.db, { readonly: true });
  let rows; try { rows = channelRows(db); } finally { db.close(); }
  const records = readJsonl(args.evidence);
  const replayed = replay(records, rows), cut = Math.floor(replayed.length * Number(args.split || 0.8));
  const learn = replayed.slice(0, cut), test = replayed.slice(cut);
  const model = trainLogistic(learn.map(item => ({ features: features(item.state), label: item.label })));
  model.trained_on = { evidence: args.evidence, evidence_sha256: sha(readFileSync(args.evidence)), records: learn.length,
    yields: learn.filter(item => item.label).length, held_out_after: cut };
  writeFileSync(args.out, JSON.stringify(model, null, 1) + '\n');
  const scored = test.map(item => ({ label: item.label, p: predict(model, features(item.state)) }));
  const base = test.filter(item => item.label).length / test.length;
  // Brier score against always guessing the base rate: below it means the
  // features carry information, not that a threshold is safe to act on.
  const brier = scored.reduce((s, item) => s + (item.p - Number(item.label)) ** 2, 0) / scored.length;
  const brierBase = scored.reduce((s, item) => s + (base - Number(item.label)) ** 2, 0) / scored.length;
  return { schema: 'bonfyre.channel-yield.offline.v1', model: args.out, artifact_sha256: sha(readFileSync(args.out)),
    learn: learn.length, held_out: test.length, held_out_yield_rate: base, brier, brier_always_base_rate: brierBase,
    sweep: sweep(scored), ordering: ordering(scored),
    cannot_infer: 'a later slice of one log -> an independent holdout; the policy was fitted with this log in view, so promotion still needs the live shadow measurement' };
}

// ---- live shadow and evaluation -------------------------------------------

async function shadow(args) {
  const ids = readFileSync(args.ids, 'utf8').split('\n').map(line => line.trim()).filter(Boolean);
  const db = new Database(args.db, { readonly: true });
  let rows, history;
  try {
    history = frontierHistory(db);
    rows = ids.length ? db.query(`SELECT c.channel_id, c.exact_address, c.protocol, c.material, c.truth_level, c.discovered_from
      FROM channel c WHERE ${CANDIDATE_SQL} AND c.channel_id IN (${ids.map(() => '?').join(',')})`).all(...ids) : [];
  } finally { db.close(); }
  const historyDigest = decisionDigest([...history].toSorted(([a], [b]) => a < b ? -1 : 1));
  const owner = logisticOwner(args.model), policy = decisionPlanePolicy();
  const cases = rows.map(row => channelCase(row, history, { historyDigest }, policy));
  const started = performance.now();
  const { receipts, summary } = cases.length ? await assessDecisionCases(cases, { localEvaluator: owner, execute: true,
    maxCalls: cases.length, concurrency: policy.max_concurrency }, policy) : { receipts: [], summary: { cases: 0 } };
  const at = new Date().toISOString();
  writeFileSync(args.out, cases.map((item, index) => JSON.stringify({ schema: 'bonfyre.channel-yield.shadow.v1',
    assessed_at: at, channel_id: item.contract.frontier[0].id, case: item, receipt: receipts[index] })).join('\n') + (cases.length ? '\n' : ''));
  return { schema: 'bonfyre.channel-yield.shadow-run.v1', assessed: cases.length, claimed: ids.length,
    model_artifact_sha256: owner.artifact_digest, wall_ms: Math.round(performance.now() - started), summary, out: args.out };
}

// WorkGraph seed lines are `source-qualify:<channel>\tkind\turi\tfamily\tpriority\t...`.
// Ranking rewrites only the priority: every line is kept, so every candidate
// is still observed; the ones likely to yield are leased first.
export function rankWork(lines, scores) {
  return lines.map(line => {
    const fields = line.split('\t'), id = fields[0]?.replace(/^source-qualify:/, '');
    if (fields.length < 5 || !scores.has(id)) return line;
    fields[4] = String(10000 + Math.round(scores.get(id) * 1_000_000));
    return fields.join('\t');
  });
}

async function rank(args) {
  const lines = readFileSync(args.work, 'utf8').split('\n').filter(Boolean);
  const ids = lines.map(line => line.split('\t')[0].replace(/^source-qualify:/, ''));
  const db = new Database(args.db, { readonly: true });
  let rows, history;
  try {
    history = frontierHistory(db);
    rows = [];
    for (let at = 0; at < ids.length; at += 900) {
      const part = ids.slice(at, at + 900);
      rows.push(...db.query(`SELECT c.channel_id, c.exact_address, c.protocol, c.material, c.truth_level, c.discovered_from
        FROM channel c WHERE c.channel_id IN (${part.map(() => '?').join(',')})`).all(...part));
    }
  } finally { db.close(); }
  const model = JSON.parse(readFileSync(args.model)), scores = new Map();
  for (const row of rows) {
    const channel = { address: row.exact_address, protocol: row.protocol || 'unknown', material: row.material || 'publication',
      truth_level: row.truth_level || 'listed', discovered_from: String(row.discovered_from || '').slice(0, 160) };
    scores.set(row.channel_id, predict(model, features({ channel, evidence: evidenceFor(history, row.exact_address) })));
  }
  writeFileSync(args.work, rankWork(lines, scores).join('\n') + (lines.length ? '\n' : ''));
  return { schema: 'bonfyre.channel-yield.rank.v1', lines: lines.length, scored: scores.size,
    model_artifact_sha256: sha(readFileSync(args.model)), action: 'priority only; every candidate is still observed' };
}

export function evaluateShadow({ shadows, evidence, threshold, minimumCases = 300, maximumErrorRate = 0.02 }, policy = decisionPlanePolicy()) {
  const outcome = new Map();
  for (const record of evidence) if (record.channel_id && Number.isFinite(record.observe_ms)) outcome.set(record.channel_id, record);
  const labeled = [], value = [];
  for (const item of shadows) {
    const record = outcome.get(item.channel_id);
    if (!record) continue;
    const labels = { yields: yielded(record) ? 'true' : 'false' };
    const unit = `source-frontier/channel/${item.channel_id}`;
    const label_receipt = { schema: 'bonfyre.decision-label.v1', evaluation_unit: unit, case_digest: item.case.case_digest,
      source_ref: `qualify_channels evidence ${sha(Buffer.from(JSON.stringify(record)))}`, labels };
    labeled.push({ case: item.case, receipt: item.receipt, labels, evaluation_unit: unit,
      label_receipt, label_receipt_digest: decisionDigest(label_receipt) });
    const p = item.receipt.answers.yields.noul, skip = p < threshold;
    // A skipped observation saves its fetch and the host's courtesy sleep; a
    // kept one pays the decision and then the full observation anyway.
    value.push({ accepted: skip, correct: labels.yields === 'false',
      baseline_ms: record.observe_ms + HOST_SLEEP_MS, decision_ms: item.receipt.latency_ms,
      fallback_ms: record.observe_ms + HOST_SLEEP_MS });
  }
  if (!labeled.length) return { schema: 'bonfyre.channel-yield.evaluation.v1', matched: 0, note: 'no shadowed candidate has a timed qualifier outcome yet' };
  return { schema: 'bonfyre.channel-yield.evaluation.v1', matched: labeled.length, skip_below: threshold,
    outcomes: evaluateDecisionOutcomes(labeled, {}, policy),
    selective_value: assessSelectiveValue(value, { minimum_cases: minimumCases, maximum_error_rate: maximumErrorRate,
      minimum_net_saved_ms: 1, sampling_assumption: 'fixed_policy_independent_holdout' }),
    cannot_infer: 'skipped observations that would have refused -> the skipped channels were worthless forever; a refusal is how one fetch ended on one day' };
}

function options(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 2) {
    if (!argv[i]?.startsWith('--') || argv[i + 1] === undefined) throw new Error('expected --key value pairs');
    const key = argv[i].slice(2);
    if (key === 'shadow') (args.shadow ||= []).push(argv[i + 1]); else args[key] = argv[i + 1];
  }
  return args;
}

if (import.meta.main) {
  const [mode, ...rest] = process.argv.slice(2);
  try {
    const args = options(rest);
    const need = keys => { for (const key of keys) if (!args[key]) throw new Error(`${mode} needs --${key}`); };
    let result;
    if (mode === 'train') { need(['db', 'evidence', 'out']); result = train(args); }
    else if (mode === 'shadow') { need(['db', 'ids', 'model', 'out']); result = await shadow(args); }
    else if (mode === 'rank') { need(['db', 'work', 'model']); result = await rank(args); }
    else if (mode === 'evaluate') {
      need(['shadow', 'evidence', 'threshold']);
      const shadows = args.shadow.filter(existsSync).flatMap(readJsonl);
      result = evaluateShadow({ shadows, evidence: readJsonl(args.evidence), threshold: Number(args.threshold),
        minimumCases: Number(args['minimum-cases'] || 300), maximumErrorRate: Number(args['maximum-error-rate'] || 0.02) });
      if (args.out) writeFileSync(args.out, JSON.stringify(result, null, 2) + '\n');
    } else throw new Error('usage: channel-yield.mjs train|shadow|rank|evaluate --key value ...');
    console.log(JSON.stringify(result));
  } catch (error) { console.error(`channel-yield: ${error.message}`); process.exitCode = 2; }
}
