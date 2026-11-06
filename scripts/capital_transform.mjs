#!/usr/bin/env bun
import { Database } from 'bun:sqlite';

const dbPath = process.env.BONFYRE_CMS_DB ?? '/var/lib/bonfyre/cms/bonfyre_cms.db';
const reversibilityRank = { reversible: 0, costly: 1, irreversible: 2 };
const preference = 'a path that is known to reach the target beats one that might; fewer rights conceded beats more; a reversible path beats a costly one and a costly one beats an irreversible one; shorter breaks the remaining ties';
const round = (n, digits = 2) => Number(n.toFixed(digits));
export const split = value => new Set(String(value ?? '').replaceAll(';', ',').split(',').map(s => s.trim()).filter(Boolean));
export function num(value) {
  if (value === null || value === undefined || value === '' || value === 'UNKNOWN') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function rows(db, table, institution) {
  if (!new Set(['capital_transformation', 'funding_requirement', 'funding_route', 'budget_request']).has(table)) throw new Error('unsupported capital source table');
  try { return db.query(`SELECT * FROM ${table} WHERE institution=?`).all(institution); }
  catch (error) { if (/no such table/.test(error.message)) return []; throw error; }
}

export function openingState(requirements, routes) {
  const tokens = new Set(requirements.filter(r => r.state === 'SATISFIED').map(r => r.requirement));
  for (const token of ['collateral-free', 'unrestricted-use', 'revenue-unpledged']) tokens.add(token);
  return { tokens, cash: round(routes.filter(r => r.kind === 'internal' && r.capacity_state === 'EVIDENCED').reduce((sum, r) => sum + (num(r.capacity) ?? 0), 0)), ownership_retained: 1, revenue_share_committed: 0, monthly_obligation: 0, rights_granted: new Set(), obligations: new Set(), cash_known: true };
}

export function applyStep(state, t) {
  const missing = [...split(t.requires)].filter(token => !state.tokens.has(token)).sort();
  if (missing.length) return [null, `requires ${missing.join(', ')}, which the state does not hold here`];
  const conflicts = [...split(t.conflicts_with_rights)].filter(right => state.rights_granted.has(right)).sort();
  if (conflicts.length) return [null, `conflicts with ${conflicts.join(', ')}, already conceded earlier in this path`];
  const minOwnership = num(t.min_ownership_retained);
  if (minOwnership !== null && state.ownership_retained < minOwnership) return [null, `needs ${Math.round(minOwnership * 100)}% ownership retained and the path has already diluted to ${Math.round(state.ownership_retained * 100)}%`];
  const maxShare = num(t.max_revenue_share_committed);
  if (maxShare !== null && state.revenue_share_committed > maxShare) return [null, `needs revenue commitments at or under ${Math.round(maxShare * 100)}% and the path has already pledged ${Math.round(state.revenue_share_committed * 100)}%`];
  const tokens = new Set([...state.tokens].filter(token => !split(t.consumes).has(token)));
  for (const token of split(t.provides)) tokens.add(token);
  const delta = num(t.cash_delta);
  return [{ tokens, cash: delta === null ? state.cash : round(state.cash + delta), cash_known: state.cash_known && delta !== null,
    ownership_retained: state.ownership_retained + (num(t.ownership_delta) ?? 0),
    revenue_share_committed: state.revenue_share_committed + (num(t.revenue_share_delta) ?? 0),
    monthly_obligation: state.monthly_obligation + (num(t.monthly_obligation_delta) ?? 0),
    rights_granted: new Set([...state.rights_granted, ...split(t.grants_rights)]), obligations: new Set([...state.obligations, ...split(t.obligations)]) }, ''];
}

export function evaluate(path, opening) {
  let state = opening;
  for (let index = 0; index < path.length; index++) {
    const [next, because] = applyStep(state, path[index]);
    if (!next) return { admissible: false, failed_at: index, failed_on: path[index].transform, because };
    state = next;
  }
  const firstIrreversible = path.findIndex(t => t.reversibility === 'irreversible');
  const worst = Math.max(0, ...path.map(t => reversibilityRank[t.reversibility] ?? 1));
  const unpriced = path.filter(t => num(t.cost_usd) === null).map(t => t.transform);
  const probs = path.map(t => num(t.success_probability));
  return { admissible: true, path: path.map(t => t.transform), end_cash: state.cash_known ? state.cash : null,
    cash_known: state.cash_known, ownership_retained: round(state.ownership_retained, 4), revenue_share_committed: round(state.revenue_share_committed, 4), monthly_obligation: round(state.monthly_obligation),
    rights_conceded: [...state.rights_granted].sort(), obligations: [...state.obligations].sort(),
    reversibility: Object.keys(reversibilityRank).find(key => reversibilityRank[key] === worst),
    reversible_back_to_step: firstIrreversible < 0 ? null : firstIrreversible,
    longest_duration_months: Math.max(0, ...path.map(t => num(t.duration_months) ?? 0)),
    pursuit_cost_usd: unpriced.length ? null : round(path.reduce((sum, t) => sum + num(t.cost_usd), 0)), unpriced_steps: unpriced,
    joint_success_probability: probs.length && probs.every(p => p !== null) ? round(probs.reduce((a, b) => a * b, 1), 4) : null };
}

export function search(transforms, opening, depth, maxPaths = 100_000) {
  const admissible = [], refused = [], chosen = [], used = new Set();
  let enumerated = 0;
  function visit() {
    if (chosen.length) {
      if (++enumerated > maxPaths) throw new Error(`path limit ${maxPaths} exceeded; lower --depth`);
      const result = evaluate(chosen, opening);
      if (result.admissible) admissible.push(result);
      else refused.push({ path: chosen.map(t => t.transform), ...result });
    }
    if (chosen.length >= depth) return;
    for (const t of transforms) {
      if (used.has(t.transform)) continue;
      chosen.push(t); used.add(t.transform); visit(); used.delete(t.transform); chosen.pop();
    }
  }
  visit();
  return [admissible, refused];
}

export function saturatedState(transforms, opening) {
  const tokens = new Set(opening.tokens);
  for (const t of transforms) for (const token of split(t.requires)) tokens.add(token);
  return { ...opening, tokens };
}

export function orderSensitivity(transforms, admissible, refused) {
  const names = transforms.map(t => t.transform).sort(), allowed = new Set(admissible.map(r => r.path.join('\0'))), rejected = new Map(refused.map(r => [r.path.join('\0'), r.because]));
  const out = [];
  for (const a of names) for (const b of names) {
    if (a !== b && allowed.has(`${a}\0${b}`) && !allowed.has(`${b}\0${a}`)) out.push({ admissible: [a, b], refused: [b, a], because: rejected.get(`${b}\0${a}`) ?? '' });
  }
  return out;
}

const rank = (r, target) => [r.cash_known && r.end_cash !== null && r.end_cash >= target ? 0 : 1, r.rights_conceded.length, reversibilityRank[r.reversibility] ?? 1, r.path.length];
const compareRank = (a, b, target) => { const ra = rank(a, target), rb = rank(b, target); for (let i = 0; i < ra.length; i++) if (ra[i] !== rb[i]) return ra[i] - rb[i]; return 0; };

export function pureJob(admissible) {
  return { job: 'capital-paths-that-concede-nothing-and-can-be-unwound', effect_class: 'PURE', candidate_space: admissible.map(r => ({ id: r.path.join(' > '), rights_free: !r.rights_conceded.length, reversible: r.reversibility === 'reversible', cash_known: Boolean(r.cash_known) })), require: { rights_free: true, reversible: true }, cannot_infer: 'agreement among reducers -> a correct enumeration; the backends check the predicate over the paths they were handed' };
}

function declarationFingerprint(transforms) {
  const vals = key => transforms.map(t => num(t[key])).filter(n => n !== null).sort((a, b) => a - b);
  const rights = key => [...new Set(transforms.flatMap(t => [...split(t[key])]))].sort();
  const by = Object.fromEntries(Object.keys(reversibilityRank).sort().map(key => [key, transforms.filter(t => t.reversibility === key).length]));
  return { transformations: transforms.length, by_reversibility: by, rights_granted: rights('grants_rights'), rights_conflicted: rights('conflicts_with_rights'), with_known_cash_delta: vals('cash_delta').length, known_cash_deltas: vals('cash_delta'), ownership_effects: vals('ownership_delta'), revenue_pledges: vals('revenue_share_delta'), ownership_floors: vals('min_ownership_retained'), with_ownership_effect: vals('ownership_delta').length, with_revenue_pledge: vals('revenue_share_delta').length, with_ownership_floor: vals('min_ownership_retained').length, cannot_infer: 'a stable fingerprint -> correct declarations; it says the shape of what was read has not moved' };
}

export function compileCapital({ institution, transforms, requirements = [], fundingRoutes = [], budget = [], depth = 3, now = new Date() }) {
  if (!Number.isInteger(depth) || depth < 1) throw new Error('--depth must be a positive integer');
  if (!transforms.length) throw new Error(`no capital transformation is declared for ${institution}`);
  if (new Set(transforms.map(t => t.transform)).size !== transforms.length) throw new Error('duplicate transformation coordinate');
  const opening = openingState(requirements, fundingRoutes);
  const target = round(budget.reduce((sum, r) => sum + (num(r.amount) ?? 0), 0));
  const targetBasis = `${budget.length} declared budget request(s), at any status`;
  const [admissible, refused] = search(transforms, opening, depth);
  admissible.sort((a, b) => compareRank(a, b, target));
  const used = new Set(admissible.flatMap(r => r.path));
  const stranded = transforms.map(t => t.transform).filter(name => !used.has(name)).sort();
  const orderSensitive = orderSensitivity(transforms, admissible, refused);
  const [satAdmissible, satRefused] = search(transforms, saturatedState(transforms, opening), Math.min(depth, 2));
  const structuralOrder = orderSensitivity(transforms, satAdmissible, satRefused);
  const alone = new Set(satAdmissible.filter(r => r.path.length === 1).map(r => r.path[0]));
  const names = transforms.map(t => t.transform).sort(), exclusive = [];
  for (let i = 0; i < names.length; i++) for (let j = i + 1; j < names.length; j++) {
    const [a, b] = [names[i], names[j]];
    if (!alone.has(a) || !alone.has(b) || satAdmissible.some(r => r.path.length === 2 && r.path.includes(a) && r.path.includes(b))) continue;
    exclusive.push({ pair: [a, b], because: [...new Set(satRefused.filter(r => r.path.length === 2 && r.path.includes(a) && r.path.includes(b)).map(r => r.because))].sort(), reachable_today: used.has(a) && used.has(b) });
  }
  const declaredConflicts = [], conflictSeen = new Set();
  for (const t of transforms) for (const right of [...split(t.conflicts_with_rights)].sort()) for (const holder of transforms.filter(x => split(x.grants_rights).has(right))) {
    const pair = [t.transform, holder.transform].sort(), key = `${pair.join('\0')}\0${right}`;
    if (conflictSeen.has(key)) continue;
    conflictSeen.add(key);
    declaredConflicts.push({ pair, right, reachable_together: admissible.some(r => r.path.includes(t.transform) && r.path.includes(holder.transform)) });
  }
  const reach = transforms.toSorted((a, b) => a.transform.localeCompare(b.transform)).map(t => {
    const ending = admissible.filter(r => r.path.at(-1) === t.transform);
    if (!ending.length) return { transform: t.transform, reachable: false, steps: null, months_floor: null, blocked_by: refused.find(r => r.path.length === 1 && r.path[0] === t.transform)?.because ?? '' };
    const duration = r => r.path.reduce((sum, name) => sum + (num(transforms.find(x => x.transform === name).duration_months) ?? 0), 0);
    ending.sort((a, b) => a.path.length - b.path.length || duration(a) - duration(b));
    const best = ending[0];
    return { transform: t.transform, reachable: true, steps: best.path.length, via: best.path, months_floor: best.path.slice(0, -1).reduce((sum, name) => sum + (num(transforms.find(x => x.transform === name).duration_months) ?? 0), 0), rights_conceded: best.rights_conceded };
  });
  const reaching = admissible.filter(r => r.cash_known && r.end_cash !== null && r.end_cash >= target);
  return { schema: 'bonfyre-capital-transform.v1', declared: declarationFingerprint(transforms), institution, ran_at: now.toISOString().replace('.000Z', 'Z'), effect_class: 'PURE', depth, transformations: transforms.length,
    opening_state: { cash: opening.cash, tokens_held: [...opening.tokens].sort(), ownership_retained: opening.ownership_retained }, target: { amount: target, basis: targetBasis },
    paths_enumerated: admissible.length + refused.length, admissible: admissible.length, refused: refused.length, known_to_reach_target: reaching.length, unknown_cash_effect: admissible.filter(r => !r.cash_known).length,
    preference, best: admissible.slice(0, 5), mutually_exclusive_pairs: exclusive, reach, order_sensitive: orderSensitive, structurally_order_sensitive: structuralOrder, declared_rights_conflicts: declaredConflicts, stranded_transformations: stranded,
    rung: reaching.length ? 'sufficient' : 'admissible', cannot_infer: ['an admissible path -> an available one', 'an unknown cash effect -> a zero one', 'a ranked path -> a recommended one; the ranking is a declared preference', "structural admissibility -> a counterparty's willingness", 'a structurally order-sensitive pair -> a pair this institution can reach', 'no admissible path containing both -> a conflict between the two'] };
}

function printSummary(r) {
  console.log(`  institution ${r.institution}, ${r.transformations} transformations, depth ${r.depth}\n  opening: $${r.opening_state.cash.toFixed(2)} cash, ${r.opening_state.tokens_held.length} tokens held\n  target:  $${r.target.amount.toFixed(2)} (${r.target.basis})`);
  console.log(`  ${r.paths_enumerated} ordered paths, ${r.admissible} admissible, ${r.refused} refused`);
  console.log(`  ${r.known_to_reach_target} known to reach the target, ${r.unknown_cash_effect} with an unknown cash effect`);
  for (const p of r.best) console.log(`    ${p.path.join(' > ')}  ${p.cash_known ? `$${p.end_cash}` : 'cash UNKNOWN'}  ${p.reversibility}  ${p.rights_conceded.join(', ') || 'nothing conceded'}`);
  for (const item of r.reach) console.log(`    ${item.transform}: ${item.reachable ? `${item.steps} step(s), ${item.months_floor} month floor, via ${item.via.join(' > ')}` : `unreachable: ${item.blocked_by}`}`);
  console.log(`  rung: ${r.rung}`);
  for (const inference of r.cannot_infer) console.log(`    cannot infer ${inference}`);
}

export function main(argv = process.argv.slice(2)) {
  let institution, depth = 3, format = 'text';
  for (let i = 0; i < argv.length; i++) {
    switch (argv[i]) {
      case '--institution': institution = argv[++i]; break;
      case '--depth': depth = Number(argv[++i]); break;
      case '--json': format = 'json'; break;
      case '--pure-job': format = 'pure'; break;
      default: throw new Error(`unknown option: ${argv[i]}`);
    }
  }
  if (!institution) throw new Error('usage: capital_transform.mjs --institution <id> [--depth N] [--json|--pure-job]');
  const db = new Database(dbPath, { readonly: true, create: false });
  let data;
  try { data = { institution, transforms: rows(db, 'capital_transformation', institution).filter(t => t.status === 'published'), requirements: rows(db, 'funding_requirement', institution), fundingRoutes: rows(db, 'funding_route', institution), budget: rows(db, 'budget_request', institution), depth }; }
  finally { db.close(); }
  const result = compileCapital(data);
  if (format === 'pure') {
    const opening = openingState(data.requirements, data.fundingRoutes);
    const [admissible] = search(data.transforms, opening, depth);
    admissible.sort((a, b) => compareRank(a, b, result.target.amount));
    console.log(JSON.stringify(pureJob(admissible), null, 2));
  } else if (format === 'json') console.log(JSON.stringify(result, null, 2));
  else printSummary(result);
  return 0;
}

if (import.meta.main) { try { process.exit(main()); } catch (error) { console.error(error.message); process.exit(2); } }
