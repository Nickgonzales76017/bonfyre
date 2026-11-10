#!/usr/bin/env bun
import { createHash } from 'node:crypto';
import { access, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { cmsPath, trialsDir, digestCohort, openCMS, contractFor, providerRoutes, routeCloses, liveCohort, organ, parseOptions } from './gym_contract.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const empty = (outcome, error, latency_ms = 0) => ({ outcome, resources: [], evidence_digest: '', latency_ms, error });

export async function executeRoute(route, target, runner = organ, timeout = 45_000) {
  if (route.kind === 'internal_organ') return empty('not_applicable', 'this route answers analytics, not per-subject retrieval');
  if (route.kind !== 'direct_source') {
    if (route.credential_env && !process.env[route.credential_env]) return empty('not_credentialed', `no value present for ${route.credential_env} in this process`);
    return empty('no_adapter', `no execution adapter is written for kind=${route.kind}; a declared route is not an executable one`);
  }
  const started = performance.now();
  const result = await runner('bonfyre-project', ['foreign', 'observe', `trial:${sha(target).slice(0, 12)}`, target], timeout);
  const elapsed = Math.round(performance.now() - started);
  if (result.code === 124) return empty('timeout', 'timed out', timeout);
  if (result.code === 127) return empty('route_unavailable', 'organ not installed');
  let doc;
  try { doc = JSON.parse(result.stdout); } catch { return empty('no_result', 'source did not answer 200', elapsed); }
  const body = doc.receipt ?? doc.observation ?? doc;
  const ok = result.code === 0 && Number(body.http_status) === 200;
  return ok ? { outcome: 'retrieved', resources: [target], evidence_digest: body.content_sha256 ?? '', latency_ms: elapsed, error: '' }
    : empty('no_result', 'source did not answer 200', elapsed);
}

export function makeTrial(route, url, gym, digest, cohortSize, result, now = new Date()) {
  return {
    schema: 'bonfyre-provider-trial.v1', gym, cohort_digest: digest, cohort_size: cohortSize, query: url,
    deformation: 'canonical', route: route.route, capability: route.capability, route_kind: route.kind,
    source_frontier: `gym=${gym}`, executed_at: now.toISOString().replace('.000Z', 'Z'), ...result,
    cannot_infer: ['an executed trial -> a good result', 'a returned resource -> relevant evidence', 'a receipt -> permission to route real work here'],
  };
}

export async function runTrial({ capability, gym, limit, apply = false, db, cohort, runner = organ, dir = trialsDir }) {
  const contract = contractFor(db, gym);
  if (!/^[A-Za-z0-9_-]+$/.test(gym)) throw new Error('gym must be a safe receipt coordinate');
  const twins = cohort ?? await liveCohort(limit ?? (Number(contract.cohort_size) || 12));
  const urls = twins.map(t => t.url);
  const digest = digestCohort(urls, gym);
  const routes = providerRoutes(db).filter(r => routeCloses(r, capability));
  const summary = { schema: 'bonfyre-provider-trial-summary.v1', gym, capability, cohort_digest: digest, cohort_size: urls.length, admissible_routes: routes.map(r => r.route), receipts_written: 0, outcomes: {}, dry_run: !apply };
  if (!apply) return summary;
  if (!urls.length) throw new Error('the twin owner returned no cohort; no trial was executed');
  const planned = [];
  for (const route of routes) {
    if (!/^[A-Za-z0-9_.-]+$/.test(route.route)) throw new Error('route must be a safe receipt coordinate');
    for (const url of urls) planned.push(join(dir, `${gym}-${route.route}-${digest.slice(0, 12)}-${sha(url).slice(0, 8)}.json`));
  }
  if (new Set(planned).size !== planned.length) throw new Error('duplicate trial receipt coordinates; no supplier called');
  for (const path of planned) {
    try { await access(path); throw new Error(`trial receipt already exists: ${path}; no supplier called`); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  await mkdir(dir, { recursive: true });
  for (const route of routes) {
    for (const url of urls) {
      const result = await executeRoute(route, url, runner);
      const receipt = makeTrial(route, url, gym, digest, urls.length, result);
      const name = `${gym}-${route.route}-${digest.slice(0, 12)}-${sha(url).slice(0, 8)}.json`;
      await writeFile(join(dir, name), JSON.stringify(receipt, null, 2), { flag: 'wx' });
      summary.receipts_written++;
      const outcomes = summary.outcomes[route.route] ??= {};
      outcomes[result.outcome] = (outcomes[result.outcome] ?? 0) + 1;
    }
  }
  return summary;
}

export async function main(argv = process.argv.slice(2)) {
  const { opts, flags, positional } = parseOptions(argv, ['--limit']);
  if (positional.length !== 2 || [...flags].some(f => !['--json', '--apply'].includes(f))) throw new Error('usage: provider_trial.mjs <capability> <gym> [--limit N] [--json] [--apply]');
  const limit = opts.limit === undefined ? undefined : Number(opts.limit);
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) throw new Error('--limit must be a positive integer');
  const db = openCMS(cmsPath);
  let summary;
  try { summary = await runTrial({ capability: positional[0], gym: positional[1], limit, apply: flags.has('--apply'), db }); } finally { db.close(); }
  if (flags.has('--json')) console.log(JSON.stringify(summary, null, 2));
  else {
    console.log(`  capability ${summary.capability}, gym ${summary.gym}, cohort ${summary.cohort_size} (${summary.cohort_digest.slice(0, 12)})`);
    if (!summary.admissible_routes.length) console.log('  no route declares this capability; nothing was executed');
    for (const [route, outcomes] of Object.entries(summary.outcomes)) console.log(`    ${route.padEnd(22)} ${Object.entries(outcomes).map(([key, value]) => `${key}=${value}`).join(', ')}`);
    console.log(summary.dry_run ? '  dry run: no supplier called and no receipts written; pass --apply to execute' : `  ${summary.receipts_written} trial receipt(s) in ${trialsDir}`);
  }
  return 0;
}

if (import.meta.main) main().then(code => process.exit(code)).catch(error => { console.error(error.message); process.exit(2); });
