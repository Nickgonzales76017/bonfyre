#!/usr/bin/env bun
import { Database } from 'bun:sqlite';
import { spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const configPath = resolve(dirname(fileURLToPath(import.meta.url)), 'funding-residuals.cue');
export function loadClasses() {
  const result = spawnSync('cue', ['export', configPath], { encoding: 'utf8' });
  if (result.status !== 0) throw new Error(result.stderr || 'CUE export failed');
  return JSON.parse(result.stdout);
}

function rows(db, table, where = '', values = [], published = true) {
  const sql = `SELECT * FROM ${table}${published ? " WHERE status='published'" : ''}${where ? `${published ? ' AND' : ' WHERE'} ${where}` : ''}`;
  try { return db.query(sql).all(...values); } catch { return []; }
}

const sum = (items, selector = () => true) => Math.round(items.filter(selector).reduce((total, row) => total + Number(row.amount || 0), 0) * 100) / 100;
const counts = (items, field) => Object.fromEntries([...new Set(items.map(row => String(row[field])))].sort().map(value => [value, items.filter(row => String(row[field]) === value).length]));

export function evaluate(institution, requirements, demandRows, routes, taxonomy, ranAt = new Date().toISOString().replace('.000Z', 'Z')) {
  if (!requirements.length) throw new Error(`no funding requirements are declared for ${institution}`);
  const committed = sum(demandRows, row => ['approved', 'committed'].includes(String(row.lifecycle_state)));
  const byState = {};
  for (const row of demandRows) {
    const key = `${row.status ?? '?'}/${row.lifecycle_state ?? '?'}`;
    byState[key] ??= { count: 0, amount: 0 };
    byState[key].count++;
    byState[key].amount = Math.round((byState[key].amount + Number(row.amount || 0)) * 100) / 100;
  }
  const satisfied = requirements.filter(row => row.state === 'SATISFIED');
  const unmet = requirements.filter(row => row.state !== 'SATISFIED');
  const residual = {};
  for (const row of unmet) {
    const kind = taxonomy.classes[row.resolvable_by] || taxonomy.fallbackClass;
    residual[kind] ??= { requirements: [], count: 0 };
    residual[kind].requirements.push({ requirement: row.requirement, state: row.state, because: row.notes || '' });
    residual[kind].count++;
  }
  const capital = routes.filter(row => String(row.capability || '').startsWith('capital'));
  const market = capital.length ? {
    measured: false, rung: 'executed', amount: null, declared_routes: capital.map(row => row.route),
    unmeasured_because: "capital routes are declared but no trial has been executed against this institution's requirement set",
  } : {
    measured: false, rung: 'declared', amount: null,
    unmeasured_because: 'no provider_route declares a capital capability, so the capital market has not been named, let alone asked',
  };
  const declared = {
    requirement_names: requirements.map(row => row.requirement).sort(),
    by_kind: counts(requirements, 'kind'), by_state: counts(requirements, 'state'),
    by_resolvable_by: counts(requirements, 'resolvable_by'),
    cannot_infer: 'a stable fingerprint -> correct requirements; it says the shape of what was read has not moved',
  };
  return {
    schema: 'bonfyre-funding-closure.v1', declared, institution, ran_at: ranAt, effect_class: 'PURE',
    declared_demand: sum(demandRows), committed_demand: committed, demand_by_state: byState,
    requirements: requirements.length, satisfied: satisfied.length, unmet: unmet.length,
    ceilings: {
      InternalizedCeiling: { measured: true, amount: sum(satisfied), means: "what this institution's own declared state already satisfies; capital observed for another institution is not counted" },
      TemporalCeiling: { measured: false, rung: 'observed', unmeasured_because: 'no requirement carries an observed_at, so nothing can be said about whether its evidence is still fresh' },
      MarketCeiling: market,
    },
    residual,
    verdict: Object.keys(residual).length ? `not declined; ${Object.entries(residual).sort(([a], [b]) => a.localeCompare(b)).map(([kind, bucket]) => `${bucket.count} ${kind}`).join(', ')}` : 'every declared requirement is satisfied',
    cannot_infer: [
      'a satisfied requirement -> funding', 'a residual -> a loan that must be raised',
      'an unmeasured market -> an absent one', 'capital observed for one institution -> capital available to another',
    ],
  };
}

export function run(dbPath, institution, taxonomy = loadClasses()) {
  const db = new Database(dbPath, { readonly: true });
  try {
    return evaluate(institution,
      rows(db, 'funding_requirement', 'institution = ?', [institution]),
      rows(db, 'budget_request', 'institution = ?', [institution], false),
      rows(db, 'provider_route'), taxonomy);
  } finally { db.close(); }
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const institution = args.includes('--institution') ? args[args.indexOf('--institution') + 1] : undefined;
  if (!institution) { console.error('usage: bun scripts/funding-closure.mjs --institution ID [--db DB] [--json]'); process.exit(2); }
  try {
    const dbPath = args.includes('--db') ? args[args.indexOf('--db') + 1] : process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db';
    const receipt = run(dbPath, institution);
    if (args.includes('--json')) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  institution ${institution}`);
      console.log(`  declared demand $${receipt.declared_demand.toFixed(2)}`);
      console.log(`  requirements ${receipt.requirements}: ${receipt.satisfied} satisfied, ${receipt.unmet} unmet`);
      console.log(`  ${receipt.verdict}`);
    }
  } catch (error) { console.error(`funding-closure: ${error.message}`); process.exit(2); }
}
