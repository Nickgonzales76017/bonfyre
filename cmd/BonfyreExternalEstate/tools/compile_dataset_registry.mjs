#!/usr/bin/env bun
// Public registry rows are candidate pointers; no route is admitted here.
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { advertisePassport } from './compile_source_passports.mjs';
import { robotsAllows } from '../../BonfyreSourceFrontier/tools/robots_policy.mjs';
export { robotsAllows } from '../../BonfyreSourceFrontier/tools/robots_policy.mjs';

let cachedPolicy;
export function datasetRegistryPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../dataset-registry.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'datasetRegistry']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const token = value => String(value).toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '').slice(0, 150);
const boundedResponseText = async (response, cap) => {
  const declared = Number(response.headers?.get?.('content-length'));
  if (Number.isFinite(declared) && declared > cap) return null;
  if (!response.body?.getReader) {
    const content = await response.text();
    return Buffer.byteLength(content) <= cap ? content : null;
  }
  const reader = response.body.getReader(), chunks = [];
  let bytes = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    bytes += value.byteLength;
    if (bytes > cap) { await reader.cancel(); return null; }
    chunks.push(value);
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
};
const entry = /^\*\s+\|(OK|FIXME)_ICON\|\s+`(.+?)\s+<(https?:\/\/[^>]+)>`_(?:\s+\[`Meta\s+<([^>]+)>`_\])?/;
export function parseRegistry(readme) {
  const lines = readme.split(/\r?\n/), rows = [];
  let category = '';
  for (let index = 0; index < lines.length; index++) {
    if (lines[index].trim() && /^-{3,}$/.test(lines[index + 1]?.trim() || '')) {
      category = lines[index].trim(); continue;
    }
    const found = entry.exec(lines[index].trim());
    if (!found || !category) continue;
    const [name, ...description] = found[2].split(' - ');
    rows.push({ category, name: name.trim(), says: description.join(' - ').replaceAll('[...]', '').trim(),
      url: found[3], meta: found[4] || '', listed_ok: found[1] === 'OK' });
  }
  return rows;
}

export async function probeRobots(host, address, cache, policy = datasetRegistryPolicy(), fetcher = fetch) {
  if (!cache.has(host)) {
    let entry = { state: 'unobserved', body: '' };
    try {
      const response = await fetcher(`https://${host}/robots.txt`, {
        headers: { 'User-Agent': policy.agent }, signal: AbortSignal.timeout(policy.probe_timeout_ms) });
      if (response.ok) {
        const body = await boundedResponseText(response, policy.robots_max_bytes);
        if (body !== null) entry = { state: 'rules', body };
      }
      else if (response.status === 404) entry = { state: 'no-rules', body: '' };
    } catch { /* unknown stays unobserved */ }
    cache.set(host, entry);
    if (policy.host_pace_ms > 0) await Bun.sleep(policy.host_pace_ms);
  }
  const entry = cache.get(host);
  return entry.state === 'rules' ? robotsAllows(entry.body, policy.agent, address) ? 'admitted' : 'refused' : entry.state;
}

export function registryCandidate(row, robots = 'unobserved', policy = datasetRegistryPolicy()) {
  const identity = `apd-${createHash('sha256').update(row.url).digest('hex').slice(0, 16)}`;
  const state = robots === 'refused' || !row.listed_ok ? 'REFUSED' : 'SHADOW';
  const access = /\bapi\b|\/api\//i.test(`${row.url} ${row.says}`) ? 'API' : 'FILE';
  const passport = { passport: identity, name: row.name.slice(0, 120), ...policy.passport,
    access: [access], subject_kinds: [token(row.category)], carrier: { route: row.url, robots },
    state, census: [] };
  const advertisement = advertisePassport(passport, undefined,
    { verifier: policy.candidate_verifier, lowerer: policy.candidate_lowerer });
  advertisement.terms = [...new Set([...advertisement.terms, `domain:${token(row.category)}`,
    `source_collection:${policy.registry_name}`])].toSorted();
  advertisement.dependencies.push(`SourcePassport:${policy.registry_name}`);
  const catalogue = { ext_id: identity, name: row.name.slice(0, 200), url: row.url,
    says: row.says.slice(0, 600), estate: policy.registry_name, grouping: row.category,
    functions: 'dataset', related: row.meta, status: 'published', cannot_infer: policy.catalogue_cannot_infer };
  return { passport, advertisement, catalogue };
}

export async function compileRegistry(readme, options = {}, policy = datasetRegistryPolicy()) {
  const rows = parseRegistry(readme).slice(0, options.limit > 0 ? options.limit : undefined);
  const cache = new Map(), results = [];
  for (const row of rows) {
    const host = new URL(row.url).hostname;
    const robots = options.probe && host ? await probeRobots(host, row.url, cache, policy, options.fetcher) : 'unobserved';
    results.push(registryCandidate(row, robots, policy));
  }
  const states = Object.fromEntries([...new Set(results.map(item => item.passport.state))].toSorted()
    .map(state => [state, results.filter(item => item.passport.state === state).length]));
  return { rows: results, summary: { entries: rows.length,
    categories: new Set(rows.map(row => row.category)).size, hosts_probed: options.probe ? cache.size : 0, states } };
}

if (import.meta.main) {
  try {
    const args = { probe: false, limit: 0 }, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index++) {
      const key = argv[index];
      if (key === '--probe') { args.probe = true; continue; }
      if (!key.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[key.slice(2)] = argv[++index];
    }
    if (!args.readme || !args.out) throw new Error('--readme and --out are required');
    args.limit = Number(args.limit) || 0;
    const result = await compileRegistry(readFileSync(args.readme, 'utf8'), args);
    const jsonl = rows => rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');
    writeFileSync(args.out, jsonl(result.rows.map(row => row.advertisement)));
    if (args.catalogue) writeFileSync(args.catalogue, jsonl(result.rows.map(row => row.catalogue)));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`dataset registry: ${error.message}`); process.exitCode = 2; }
}
