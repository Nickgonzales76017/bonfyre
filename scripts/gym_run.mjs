#!/usr/bin/env bun
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { cmsPath, receiptsDir, windows, digestCohort, openCMS, contractFor, providerRoutes, organ, liveCohort, trialReceipts, parseOptions } from './gym_contract.mjs';

export function sourceCeiling(receipts) {
  const direct = receipts.filter(r => String(r.capability ?? '').startsWith('discovery.direct_source'));
  if (!direct.length) return { measured: false, rung: 'executed', unmeasured_because: 'no direct-source trial has run against this cohort; re-fetching every source is an outbound cost against third parties and is a separate, rate-aware act' };
  const evidence = new Set(direct.filter(r => r.outcome === 'retrieved').map(r => r.evidence_digest).filter(Boolean));
  return { measured: true, rung: 'measured', value: evidence.size, of: direct.length, means: 'distinct evidence digests the sources themselves returned when actually asked' };
}

export function marketCeiling(routes, receipts, capability = 'retrieval.search') {
  const external = routes.filter(r => !['internal_organ', 'direct_source'].includes(r.kind));
  const admissible = external.filter(r => r.capability === capability || r.capability.startsWith(`${capability}.`));
  if (!external.length) return { measured: false, rung: 'declared', unmeasured_because: 'no external supplier is declared as a provider_route; a market cannot be measured before it is named' };
  if (!admissible.length) return { measured: false, rung: 'admissible', declared_routes: external.length, unmeasured_because: `no declared external route closes ${capability}; possessing a credential for a completion endpoint does not make a supplier a search supplier` };
  const admitted = new Set(admissible.map(r => r.route));
  const trials = receipts.filter(r => admitted.has(r.route));
  if (!trials.length) return { measured: false, rung: 'executed', admissible_routes: [...admitted], unmeasured_because: 'admissible routes exist but none has an executed trial receipt against this cohort; a declared route is not an executed one' };
  const retrieved = trials.filter(r => r.outcome === 'retrieved');
  if (!retrieved.length) {
    const outcomes = [...new Set(trials.map(r => r.outcome ?? '?'))].sort();
    return { measured: false, rung: 'retrieved', trials: trials.length, outcomes, unmeasured_because: `trials executed but nothing was retrieved; outcomes were ${outcomes.join(', ')}` };
  }
  return { measured: true, rung: 'measured', value: new Set(retrieved.map(r => r.evidence_digest).filter(Boolean)).size, trials: trials.length, routes: [...new Set(retrieved.map(r => r.route))].sort(), means: 'distinct evidence digests recovered by external suppliers against this cohort, from executed trials' };
}

function parseDate(value) {
  if (!value) return null;
  const epoch = Date.parse(value.endsWith('Z') ? value : `${value.replace(' ', 'T')}Z`);
  return Number.isFinite(epoch) ? epoch : null;
}

export async function buildGymReceipt(contract, twins, routes, receipts, org = organ, now = Date.now()) {
  const window = windows[contract.validity_window] ?? 0;
  const rows = twins.map(t => {
    const seen = parseDate(t.last_seen), age = seen === null ? null : Math.floor((now - seen) / 1000);
    return { subject: t.subject_id ?? '', url: t.url, observations: t.observations ?? 0, content_changes: t.content_changes ?? 0, held_body: Boolean(t.ever_retained), age_seconds: age, within_window: age !== null && window > 0 && age <= window };
  });
  const digest = digestCohort(rows.map(r => r.url), contract.gym);
  const scoped = receipts.filter(r => r.gym === contract.gym && r.cohort_digest === digest);
  const [dupResult, admResult] = await Promise.all([org('bonfyre-project', ['foreign', 'duplicates']), org('bonfyre-project', ['foreign', 'admissible'])]);
  function doc(result) { try { return result.code === 0 && result.stdout.trim() ? JSON.parse(result.stdout) : {}; } catch { return {}; } }
  const duplicates = doc(dupResult).duplicates?.length ?? 0;
  const adm = doc(admResult), refused = adm.refused ?? adm.refusals ?? [];
  const boundary = Array.isArray(refused) ? refused.filter(r => (r.reason ?? r.why) === 'boundary_response_not_content').length : 0;
  const internalized = rows.filter(r => r.held_body).length, fresh = rows.filter(r => r.within_window).length;
  return {
    schema: 'bonfyre-gym-run.v1', gym: contract.gym, title: contract.title, subject_kind: contract.subject_kind,
    generator: contract.generator, validity_window: contract.validity_window, ran_at: new Date(now).toISOString().replace('.000Z', 'Z'),
    cohort: rows, cohort_size: rows.length, cohort_digest: digest,
    ceilings: {
      InternalizedCeiling: { measured: true, value: internalized, of: rows.length, means: 'subjects whose bytes this institution still holds' },
      InstitutionalCeiling: { measured: true, value: adm.admissible ?? 0, means: 'observations that may lawfully support a derived claim' },
      TemporalCeiling: { measured: true, value: fresh, of: rows.length, means: `subjects still inside the declared ${contract.validity_window} window` },
      DirectSourceCeiling: sourceCeiling(scoped), MarketCeiling: marketCeiling(routes, scoped),
      ActionCeiling: { measured: false, unmeasured_because: "reachability stops at the human line; what may be executed is the authority organ's answer, not a benchmark's" },
    },
    residual: {
      TemporalResidual: { value: rows.length - fresh, of: rows.length, attribution: 'held but past its declared validity window; the fix is a refresh schedule priced against staleness, not more retrieval' },
      IdentityResidual: { value: duplicates, attribution: 'one resource carried under more than one identity; the fix is in AddressPlane and Matrix, not in the provider' },
      EvidenceQualityResidual: { value: boundary, of: adm.observations ?? 0, attribution: 'retained with a 200 but containing a boundary page rather than the resource requested; permission and transport both succeeded, so nothing before this asked whether the bytes were the answer' },
      RetentionResidual: { value: rows.length - internalized, of: rows.length, attribution: 'observed but not retained, so nothing can be re-derived without going back to the source; a hydration-regime decision' },
      MarketResidual: { value: null, attribution: 'unattributable until a supplier is admitted; this is the number that would say whether to buy, build or internalize' },
    },
    cannot_infer: String(contract.cannot_infer ?? '').split(';').map(s => s.trim()).filter(Boolean),
  };
}

function printSummary(receipt) {
  console.log(`  ${receipt.title}\n  cohort ${receipt.cohort_size} ${receipt.subject_kind}, generated from ${receipt.generator}\n`);
  for (const [name, c] of Object.entries(receipt.ceilings)) console.log(`    ${name.padEnd(22)} ${c.measured ? `${c.value}${c.of === undefined ? '' : `/${c.of}`}  ${c.means}` : `--  unmeasured: ${c.unmeasured_because}`}`);
  console.log('');
  for (const [name, r] of Object.entries(receipt.residual)) console.log(`    ${name.padEnd(22)} ${r.value === null ? '--' : `${r.value}${r.of === undefined ? '' : `/${r.of}`}`}  ${r.attribution}`);
}

export async function main(argv = process.argv.slice(2)) {
  const { positional, flags } = parseOptions(argv);
  if (positional.length !== 1 || [...flags].some(f => !['--json', '--receipt'].includes(f))) throw new Error('usage: gym_run.mjs <gym> [--json] [--receipt]');
  const db = openCMS(cmsPath);
  let contract, routes;
  try { contract = contractFor(db, positional[0]); routes = providerRoutes(db); } finally { db.close(); }
  const started = performance.now(), twins = await liveCohort(Number(contract.cohort_size) || 12);
  if (!twins.length) throw new Error('the twin owner returned no cohort');
  const digest = digestCohort(twins.map(t => t.url), contract.gym);
  const receipt = await buildGymReceipt(contract, twins, routes, await trialReceipts(contract.gym, digest));
  receipt.duration_seconds = Math.round((performance.now() - started) / 10) / 100;
  if (flags.has('--json')) console.log(JSON.stringify(receipt, null, 2)); else printSummary(receipt);
  if (flags.has('--receipt')) {
    await mkdir(receiptsDir, { recursive: true });
    const path = join(receiptsDir, `${contract.gym}-${receipt.ran_at.replaceAll(':', '')}.json`);
    await writeFile(path, JSON.stringify(receipt, null, 2), { flag: 'wx' });
    console.error(`  receipt: ${path}`);
  }
  return 0;
}

if (import.meta.main) main().then(code => process.exit(code)).catch(error => { console.error(error.message); process.exit(2); });
