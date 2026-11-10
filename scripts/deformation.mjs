#!/usr/bin/env bun
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const policyPath = resolve(dirname(fileURLToPath(import.meta.url)), 'deformation-policy.cue');
export function loadPolicy() {
  const result = spawnSync('cue', ['export', policyPath], { encoding: 'utf8' });
  if (result.status !== 0) throw new Error(result.stderr || 'CUE export failed');
  return JSON.parse(result.stdout);
}

export function canonical(address, policy) {
  const raw = address.trim();
  let url;
  try { url = new URL(raw); } catch { return raw; }
  if (!['http:', 'https:'].includes(url.protocol)) return raw;
  const tracking = new Set(policy.trackingParameters.map(name => name.toLowerCase()));
  const pairs = [...url.searchParams].filter(([name]) => !tracking.has(name.toLowerCase()));
  pairs.sort(([ak, av], [bk, bv]) => ak.localeCompare(bk) || av.localeCompare(bv));
  url.search = '';
  for (const [name, value] of pairs) url.searchParams.append(name, value);
  url.hash = '';
  return url.toString();
}

export function deformations(address, policy) {
  const url = new URL(address);
  const host = url.hostname;
  const path = url.pathname || '/';
  const rows = [['canonical', address]];
  rows.push(['tracking_noise', `${address}${url.search ? '&' : '?'}utm_source=newsletter&utm_medium=email`]);
  if (path === '/') rows.push([address.endsWith('/') ? 'no_trailing_slash' : 'added_trailing_slash', address.endsWith('/') ? address.slice(0, -1) : `${address}/`]);
  else rows.push(['decoy_trailing_slash', address.endsWith('/') ? address.slice(0, -1) : `${address}/`]);
  if (host) rows.push(['host_case', address.replace(host, host.toUpperCase())]);
  if (url.protocol === 'https:') rows.push(['stale_scheme', `http://${address.split('://', 2)[1]}`]);
  if (host) rows.push(['default_port', address.replace(host, `${host}:${url.protocol === 'https:' ? '443' : '80'}`)]);
  rows.push(['fragment', `${address}#section-2`]);
  rows.push(['decoy_other_path', `${address.replace(/\/+$/, '')}/definitely-not-the-same-resource`]);
  const self = canonical(address, policy);
  return rows.filter(([name, value]) => !name.startsWith('decoy_') || (value !== address && canonical(value, policy) !== self));
}

export async function measure(subjects, resolveCount, { policy, canonicalize = false } = {}) {
  const tally = {}, rows = [];
  for (const subject of subjects) for (const [name, deformed] of deformations(subject.url, policy)) {
    const asked = canonicalize ? canonical(deformed, policy) : deformed;
    const found = await resolveCount(asked);
    const decoy = name.startsWith('decoy_');
    const verdict = decoy ? (found ? 'false_merge' : 'correctly_refused') : (found ? 'closed' : 'missed');
    tally[name] ??= {};
    tally[name][verdict] = (tally[name][verdict] || 0) + 1;
    rows.push({ subject: subject.subject_id || '', deformation: name, asked, verdict });
  }
  const count = label => Object.values(tally).reduce((sum, verdicts) => sum + (verdicts[label] || 0), 0);
  return {
    schema: 'bonfyre-deformation-closure.v1', canonicalized: canonicalize,
    subjects: subjects.length, probes: rows.length,
    closed: count('closed'), missed: count('missed'), false_merges: count('false_merge'), correctly_refused: count('correctly_refused'),
    by_deformation: tally, rows,
    cannot_infer: [
      'a closed deformation -> a correct retrieval',
      "a closure rate -> a supplier's quality (this measures our own identity closure)",
      'no false merge on this cohort -> no false merge (the decoy is one probe, not a proof)',
      'two addresses that canonicalize alike -> the same resource; folding is a strong heuristic over addressing, and identity is settled by retained bytes',
    ],
  };
}

function owner(binary, args, timeout = 45000) {
  const result = spawnSync(binary, args, { encoding: 'utf8', timeout });
  if (result.status !== 0 || !result.stdout?.trim()) return null;
  try { return JSON.parse(result.stdout); } catch { return null; }
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
  const binary = join(option('--bin-dir', process.env.BONFYRE_BIN_DIR || '/usr/local/bin'), 'bonfyre-project');
  try {
    const policy = loadPolicy();
    const doc = owner(binary, ['foreign', 'twins'], 90000) || {};
    const twins = Array.isArray(doc) ? doc : doc.twins || [];
    const subjects = twins.filter(row => String(row.url || '').startsWith('http') && Number(row.observations || 0) > 0).slice(0, Number(option('--limit', 8)));
    const result = await measure(subjects, address => Number(owner(binary, ['foreign', 'resolve', address])?.observations || 0), { policy, canonicalize: args.includes('--canonicalize') });
    if (args.includes('--json')) console.log(JSON.stringify(result, null, 2));
    else {
      console.log(`  ${result.subjects} subjects x ${Object.keys(result.by_deformation).length} deformations = ${result.probes} probes`);
      for (const [name, counts] of Object.entries(result.by_deformation).sort(([a], [b]) => a.localeCompare(b))) console.log(`  ${name}: ${JSON.stringify(counts)}`);
      console.log(`  false merges: ${result.false_merges}   correctly refused: ${result.correctly_refused}`);
    }
  } catch (error) { console.error(`deformation: ${error.message}`); process.exit(1); }
}
