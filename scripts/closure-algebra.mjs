#!/usr/bin/env bun
import { Database } from 'bun:sqlite';

export const commitment = ['observed', 'candidate', 'interested', 'quoted', 'proposed', 'conditional', 'reserved', 'authorized', 'committed', 'executed', 'settled'];
const admitting = commitment.indexOf('conditional');
const split = value => new Set(String(value ?? '').split(',').map(v => v.trim()).filter(Boolean));
const sorted = values => [...values].sort();
const resolverName = (row, declaration) => String(row[declaration.resolver_type.split('_').at(-1)] ?? row.route ?? '?');
const resolverClass = (row, declaration) => String(row[declaration.resolver_class_field || ''] ?? '');
const rank = value => {
  const name = String(value ?? '').trim().toLowerCase();
  const found = commitment.indexOf(name);
  return found >= 0 ? found : ({ evidenced: admitting, unknown: 1, blocked: 0 }[name] ?? -1);
};
const admissible = row => rank(row.commitment || row.capacity_state) >= admitting;
const ident = value => /^[a-z_][a-z0-9_]*$/.test(value);

export function loadDomain(db, domain, subject) {
  const declaration = db.query("SELECT * FROM closure_domain WHERE domain=? AND status='published'").get(domain);
  if (!declaration) throw new Error(`no closure_domain declares ${JSON.stringify(domain)}; a domain is added by declaration, not by another evaluator`);
  const rows = table => {
    if (!ident(table) || !ident(declaration.scope_field)) throw new Error('a declaration names a table and a column, never an expression');
    try { return db.query(`SELECT * FROM ${table} WHERE ${declaration.scope_field}=? AND status='published'`).all(subject); }
    catch { return []; }
  };
  const requirements = rows(declaration.requirement_type);
  if (!requirements.length) throw new Error(`no ${declaration.requirement_type} rows for ${JSON.stringify(subject)} in domain ${JSON.stringify(domain)}`);
  return { declaration, requirements, resolvers: rows(declaration.resolver_type) };
}

export function compileResolvers(rows, declaration) {
  return rows.map((row, index) => ({ row, index, needs: split(row[declaration.requires_field]), effects: split(row[declaration.satisfies_field]), admissible: admissible(row) }));
}

// Work-list fixed point: an edge is revisited only when a missing prerequisite closes.
export function closure(initial, compiled, omitted = -1) {
  const met = new Set(initial);
  const watchers = new Map();
  const remaining = new Int32Array(compiled.length);
  const ready = [];
  const used = [];
  for (const resolver of compiled) {
    if (resolver.index === omitted || !resolver.admissible) continue;
    for (const need of resolver.needs) {
      if (met.has(need)) continue;
      remaining[resolver.index]++;
      if (!watchers.has(need)) watchers.set(need, []);
      watchers.get(need).push(resolver);
    }
    if (!remaining[resolver.index]) ready.push(resolver);
  }
  for (let cursor = 0; cursor < ready.length; cursor++) {
    const resolver = ready[cursor];
    used.push(resolver);
    for (const effect of resolver.effects) {
      if (met.has(effect)) continue;
      met.add(effect);
      for (const waiting of watchers.get(effect) ?? []) {
        remaining[waiting.index]--;
        if (!remaining[waiting.index]) ready.push(waiting);
      }
    }
  }
  return { met, used };
}

const finiteNumber = value => value === null || value === '' || value === undefined ? null : Number(value);
function declarationFaults(resolvers, declaration) {
  const faults = [];
  for (const row of resolvers) {
    const resolver = String(row[declaration.resolver_field || 'resolver'] || '?');
    for (const [field, invalid, because] of [
      ['success_probability', n => n < 0 || n > 1, 'a probability outside [0,1] is not a probability, and it is multiplied into every expected value here'],
      ['cost_usd', n => n < 0, 'a negative cost inverts every leverage-per-dollar comparison'],
    ]) {
      const value = finiteNumber(row[field]);
      if (value === null) continue;
      if (!Number.isFinite(value)) faults.push({ resolver, field, value: String(row[field]).slice(0, 40), because: 'not a number' });
      else if (invalid(value)) faults.push({ resolver, field, value, because });
    }
  }
  return faults;
}

export function evaluate(domain, subject, declaration, requirements, resolvers, ranAt = new Date().toISOString().replace('.000Z', 'Z')) {
  let key = declaration.requirement_type.split('_').at(-1);
  if (!(key in requirements[0])) key = Object.keys(requirements[0]).find(k => !['id', 'namespace', 'status'].includes(k) && typeof requirements[0][k] === 'string');
  if (!key) throw new Error('requirement declaration has no name field');
  const all = new Set(requirements.map(row => String(row[key])));
  const satisfied = new Set(requirements.filter(row => String(row[declaration.state_field]) === declaration.satisfied_state).map(row => String(row[key])));
  const unknown = new Set(requirements.filter(row => String(row[declaration.state_field]) === 'UNKNOWN').map(row => String(row[key])));
  const compiled = compileResolvers(resolvers, declaration);
  const base = closure(satisfied, compiled);
  const inAll = values => sorted([...values].filter(value => all.has(value)));
  const below = compiled.filter(r => !r.admissible).map(r => ({ resolver: resolverName(r.row, declaration), commitment: String(r.row.commitment || r.row.capacity_state || ''), rung_needed: commitment[admitting] }));
  const blocked = compiled.filter(r => r.admissible && [...r.needs].some(need => !base.met.has(need))).map(r => ({ resolver: resolverName(r.row, declaration), waiting_on: sorted([...r.needs].filter(need => !base.met.has(need))), class: resolverClass(r.row, declaration) }));
  const hypotheses = sorted([...all].filter(cond => !satisfied.has(cond))).map(condition => {
    const result = closure(new Set([...satisfied, condition]), compiled);
    return {
      condition, conditions_closed_before: inAll(base.met).length, conditions_closed_after: inAll(result.met).length,
      resolvers_admitted_before: base.used.length, resolvers_admitted_after: result.used.length,
      gain: result.used.length - base.used.length, was_unknown: unknown.has(condition),
    };
  }).sort((a, b) => b.gain - a.gain);
  const leverage = compiled.map(r => {
    const result = closure(satisfied, compiled, r.index);
    const lost = inAll(new Set(inAll(base.met).filter(cond => !result.met.has(cond))));
    const row = r.row;
    const cost = finiteNumber(row.cost_usd);
    const prob = finiteNumber(row.success_probability);
    const declaredCost = cost !== null;
    const likelihood = prob > 0 && prob <= 1 ? prob : null;
    const free = declaredCost && cost === 0 && likelihood !== null;
    const priced = free || (declaredCost && cost > 0 && likelihood !== null);
    return {
      resolver: resolverName(row, declaration), class: resolverClass(row, declaration),
      closes_directly: sorted(r.effects), conditions_lost_without_it: lost, closure_contribution: lost.length,
      capacity_state: String(row.capacity_state || ''), cost_usd: declaredCost ? cost : null,
      success_probability: likelihood,
      marginal_closure_value: priced && !free ? Math.round(lost.length * likelihood / cost * 1e6) / 1e6 : null,
      priced, free,
      ...(!priced ? { unpriced_because: 'no declared cost or probability; ranked by closure contribution alone rather than by an invented price' } : {}),
    };
  }).sort((a, b) => Number(b.free) - Number(a.free) || Number(b.priced) - Number(a.priced) || (b.marginal_closure_value ?? 0) - (a.marginal_closure_value ?? 0) || b.closure_contribution - a.closure_contribution);
  const reuse = sorted(all).map(condition => ({ condition, required_by: sorted(compiled.filter(r => r.needs.has(condition)).map(r => resolverName(r.row, declaration))) })).filter(row => row.required_by.length > 1).map(row => ({ ...row, reuse_factor: row.required_by.length })).sort((a, b) => b.reuse_factor - a.reuse_factor);
  return {
    schema: 'bonfyre-closure.v1', resolver_leverage: leverage, evidence_reuse: reuse, domain, subject,
    ran_at: ranAt, effect_class: 'PURE', effect_performed: false,
    conditions: all.size, satisfied_at_start: satisfied.size,
    unknown_not_counted_as_blocked: sorted(unknown), closed_at_fixed_point: inAll(base.met),
    condition_kinds: Object.fromEntries(requirements.map(row => [String(row[key]), String(row.kind || '')])),
    condition_resolvable_by: Object.fromEntries(requirements.map(row => [String(row[key]), String(row.resolvable_by || '')])),
    resolvers: resolvers.length, declaration_faults: declarationFaults(resolvers, declaration),
    resolvers_admitted: base.used.length, resolvers_blocked: blocked, resolvers_below_commitment: below,
    admitting_rung: commitment[admitting], commitment_ladder: commitment, closure_hypotheses: hypotheses,
    cannot_infer: [...String(declaration.cannot_infer || '').split(';').map(v => v.trim()).filter(Boolean),
      'a closed condition -> a condition satisfied in the world', 'a fixed point -> a plan',
      'an UNKNOWN condition -> an unmet one; nobody asked',
      'an interested resolver -> a committed one; a list of interest is not a plan'],
  };
}

export function run(dbPath, domain, subject) {
  const db = new Database(dbPath, { readonly: true });
  try {
    const { declaration, requirements, resolvers } = loadDomain(db, domain, subject);
    return evaluate(domain, subject, declaration, requirements, resolvers);
  } finally { db.close(); }
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = name => args[args.indexOf(name) + 1];
  const domain = option('--domain'), subject = option('--subject');
  if (!domain || !subject) { console.error('usage: bun scripts/closure-algebra.mjs --domain D --subject S [--db DB] [--json]'); process.exit(2); }
  try {
    const receipt = run(args.includes('--db') ? option('--db') : process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db', domain, subject);
    if (args.includes('--json')) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  domain ${domain}, subject ${subject}`);
      console.log(`  ${receipt.conditions} conditions, ${receipt.satisfied_at_start} satisfied at start, ${receipt.closed_at_fixed_point.length} closed at the fixed point`);
      console.log(`  ${receipt.resolvers_admitted} of ${receipt.resolvers} resolvers admitted (at least ${receipt.admitting_rung})`);
      for (const row of receipt.resolvers_blocked) console.log(`    blocked ${row.resolver}: waiting on ${row.waiting_on.join(', ')}`);
      for (const row of receipt.closure_hypotheses) console.log(`    ${row.condition}: +${row.gain} resolvers${row.was_unknown ? ' (unknown)' : ''}`);
    }
  } catch (error) { console.error(`closure-algebra: ${error.message}`); process.exit(2); }
}
