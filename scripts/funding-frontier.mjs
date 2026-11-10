#!/usr/bin/env bun
import { Database } from 'bun:sqlite';

const split = value => String(value || '').split(',').map(v => v.trim()).filter(Boolean);
const sorted = values => [...values].sort();
const money = value => Math.round(value * 100) / 100;

function* combinations(values, count, start = 0, chosen = []) {
  if (!count) { yield chosen; return; }
  for (let index = start; index <= values.length - count; index++) yield* combinations(values, count - 1, index + 1, [...chosen, values[index]]);
}

export function evaluate(institution, routes, requirements, ranAt = new Date().toISOString().replace('.000Z', 'Z')) {
  if (!routes.length) throw new Error(`no funding routes are declared for ${institution}`);
  const allRequirements = new Set(requirements.map(row => row.requirement));
  const satisfied = new Set(requirements.filter(row => row.state === 'SATISFIED').map(row => row.requirement));
  const vocabulary = [...new Set([...allRequirements, ...routes.flatMap(row => [...split(row.requires), ...split(row.satisfies)])])];
  const bit = new Map(vocabulary.map((value, index) => [value, 1n << BigInt(index)]));
  const mask = values => values.reduce((total, value) => total | bit.get(value), 0n);
  const compiled = routes.map(row => ({ row, needs: mask(split(row.requires)), effects: mask(split(row.satisfies)) }));
  const names = routes.map(row => row.route);
  const baseMask = mask([...satisfied]);
  const allMask = mask([...allRequirements]);
  const worlds = [];
  const closes = (selected, initial = baseMask) => {
    const chosen = compiled.filter(item => selected.includes(item.row.route));
    let met = initial;
    for (let turn = 0; turn <= chosen.length; turn++) {
      const prior = met;
      for (const item of chosen) if ((item.needs & met) === item.needs) met |= item.effects;
      if (met === prior) break;
    }
    const blocked = chosen.filter(item => (item.needs & met) !== item.needs).map(item => ({ route: item.row.route, missing: sorted(vocabulary.filter(value => !!((item.needs & bit.get(value)) !== 0n) && !(met & bit.get(value)))) }));
    const blockedNames = new Set(blocked.map(row => row.route));
    const evidenced = money(chosen.filter(item => item.row.capacity_state === 'EVIDENCED' && !blockedNames.has(item.row.route)).reduce((sum, item) => sum + Number(item.row.capacity || 0), 0));
    const unknown = chosen.filter(item => item.row.capacity_state === 'UNKNOWN' && !blockedNames.has(item.row.route)).map(item => item.row.route);
    return { routes: selected, unblocked: chosen.length - blocked.length, blocked, evidenced_capacity: evidenced, unknown_capacity_routes: unknown, requirements_closed: sorted(vocabulary.filter(value => !!((met & allMask & bit.get(value)) !== 0n))) };
  };
  for (let size = 1; size <= names.length; size++) for (const combo of combinations(names, size)) worlds.push(closes(combo));
  const fully = worlds.filter(world => !world.blocked.length);
  const best = worlds.reduce((winner, world) => !winner || world.requirements_closed.length > winner.requirements_closed.length || (world.requirements_closed.length === winner.requirements_closed.length && world.unblocked > winner.unblocked) ? world : winner, null);
  const blockers = new Map();
  for (const world of worlds) for (const row of world.blocked) for (const missing of row.missing) blockers.set(missing, (blockers.get(missing) || 0) + 1);
  const hypotheses = sorted([...allRequirements].filter(condition => !satisfied.has(condition))).map(condition => {
    const after = names.length ? [...Array(names.length)].reduce((total, _, index) => total + [...combinations(names, index + 1)].filter(combo => !closes(combo, baseMask | bit.get(condition)).blocked.length).length, 0) : 0;
    const requirement = requirements.find(row => row.requirement === condition) || {};
    return { condition, resolvable_by: requirement.resolvable_by || 'unknown', worlds_unblocked_before: fully.length, worlds_unblocked_after: after, worlds_gained: after - fully.length };
  }).sort((a, b) => b.worlds_gained - a.worlds_gained);
  const reuse = sorted(allRequirements).map(condition => {
    const needed = sorted(routes.filter(row => split(row.requires).includes(condition)).map(row => row.route));
    const produced = sorted(routes.filter(row => split(row.satisfies).includes(condition)).map(row => row.route));
    const requirement = requirements.find(row => row.requirement === condition) || {};
    return { artifact: condition, state: requirement.state || '?', required_by_routes: needed, produced_by_routes: produced, reuse_factor: needed.length };
  }).filter(row => row.reuse_factor > 1 || row.produced_by_routes.length).sort((a, b) => b.reuse_factor - a.reuse_factor);
  return {
    schema: 'bonfyre-funding-frontier.v1', closure_hypotheses: hypotheses, evidence_reuse: reuse,
    institution, ran_at: ranAt, effect_class: 'PURE', effect_performed: false,
    routes: names.length, worlds_evaluated: worlds.length, worlds_with_no_blocked_route: fully.length,
    best_world: best, binding_conditions: [...blockers].sort((a, b) => b[1] - a[1]).slice(0, 5),
    evidenced_capacity_ceiling: Math.max(...worlds.map(world => world.evidenced_capacity)),
    routes_of_unknown_capacity: sorted(new Set(routes.filter(row => row.capacity_state === 'UNKNOWN').map(row => row.route))),
    cannot_infer: [
      'a closing combination -> funding', 'an unblocked route -> an offer',
      'UNKNOWN capacity -> zero capacity', 'an evidenced ceiling -> money that can be drawn today',
    ],
  };
}

export function run(dbPath, institution) {
  const db = new Database(dbPath, { readonly: true });
  try {
    const rows = table => {
      try { return db.query(`SELECT * FROM ${table} WHERE status='published' AND institution=?`).all(institution); }
      catch { return []; }
    };
    return evaluate(institution, rows('funding_route'), rows('funding_requirement'));
  } finally { db.close(); }
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const institution = args.includes('--institution') ? args[args.indexOf('--institution') + 1] : undefined;
  if (!institution) { console.error('usage: bun scripts/funding-frontier.mjs --institution ID [--db DB] [--json]'); process.exit(2); }
  try {
    const dbPath = args.includes('--db') ? args[args.indexOf('--db') + 1] : process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db';
    const receipt = run(dbPath, institution);
    if (args.includes('--json')) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  institution ${institution}`);
      console.log(`  ${receipt.routes} routes, ${receipt.worlds_evaluated} combinations evaluated, ${receipt.worlds_with_no_blocked_route} with no blocked route`);
      console.log(`  evidenced capacity ceiling $${receipt.evidenced_capacity_ceiling.toFixed(2)}`);
      console.log('  nothing was applied for; no capital moved');
    }
  } catch (error) { console.error(`funding-frontier: ${error.message}`); process.exit(2); }
}
