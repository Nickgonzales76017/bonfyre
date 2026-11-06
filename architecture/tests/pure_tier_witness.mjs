#!/usr/bin/env bun
// Test-only assertions and fixture generation for the pure execution tier.
// The shell entrypoints still run the native owners and reducers.
import { strict as assert } from 'node:assert';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';

const readJson = path => JSON.parse(readFileSync(path, 'utf8'));
const writeJson = (path, value) => writeFileSync(path, JSON.stringify(value));
// Python sorted(str) compares Unicode code points; JS's default sort compares
// UTF-16 code units and can change the set hash for non-BMP identities.
const codepointOrder = (left, right) => {
  const a = [...left], b = [...right];
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const difference = a[i].codePointAt(0) - b[i].codePointAt(0);
    if (difference) return difference;
  }
  return a.length - b.length;
};
const rows = (path, sql) => {
  const db = new Database(path, { readonly: true });
  try { return db.query(sql).all(); }
  finally { db.close(); }
};

function portfolioJob(path) {
  const job = readJson(path);
  assert.equal(job.effect_class, 'PURE', 'a portfolio question is a pure search');
  assert.ok(Array.isArray(job.candidate_space) && job.candidate_space.length, 'no chains proposed');
  assert.equal(job.subset_budget?.field, 'cost_milli');
  for (const candidate of job.candidate_space) {
    for (const field of ['cost_milli', 'confidence_milli', 'drift_milli']) {
      assert.ok(Number.isSafeInteger(candidate[field]),
        `${field} is ${JSON.stringify(candidate[field])}; the lattice compares integers`);
    }
  }
  assert.ok(job.cannot_infer.some(value => value.includes('authority')),
    'a budget that fits must not read as authority to spend it');
}

function portfolioOutput(outputPath, jobPath) {
  const output = readJson(outputPath);
  const job = readJson(jobPath);
  const by = new Map(output.backends.map(row => [row.backend, row]));
  const net = by.get('hvm4')?.result;
  const reference = by.get('reference')?.result;
  assert.ok(net != null, `hvm4 produced nothing: ${by.get('hvm4')?.error}`);
  assert.ok(reference != null, 'the reference produced nothing');
  assert.deepEqual(new Set(net), new Set(reference),
    `the net found ${net.length} feasible portfolios and enumeration found ${reference.length}; they must agree`);

  // Neither reducer is the oracle: enumerate the budget independently.
  const costs = job.candidate_space.map(candidate => candidate.cost_milli);
  const budget = job.subset_budget.budget;
  const truth = new Set();
  for (let mask = 0; mask < 2 ** costs.length; mask++) {
    let total = 0;
    for (let i = 0; i < costs.length; i++) if (Math.floor(mask / 2 ** i) % 2) total += costs[i];
    if (total <= budget) truth.add(mask);
  }
  const decoded = new Set(reference.map(value => {
    const mask = Number(value);
    assert.ok(Number.isSafeInteger(mask), `invalid subset mask: ${JSON.stringify(value)}`);
    return mask;
  }));
  assert.deepEqual(decoded, truth, 'both backends agree and both are wrong');
  console.log(`  ${truth.size} of ${2 ** costs.length} portfolios fit a ${budget}-milli budget, three ways`);

  for (const weak of ['hvm2', 'bend']) {
    const row = by.get(weak) || {};
    assert.ok(row.result == null, `${weak} answered a subset lattice it cannot express`);
    assert.match(String(row.error || '').toLowerCase(), /refus|neither/,
      `${weak} produced no result and no reason: ${JSON.stringify(row)}`);
  }
  const per = by.get('hvm4')?.cost?.interactions_per_subset;
  assert.ok(per == null || (typeof per === 'number' && per > 0), 'the net reported no interaction cost');
}

function portfolioWide(path) {
  writeJson(path, { job: 'too-wide', effect_class: 'PURE',
    candidate_space: Array.from({ length: 24 }, (_, index) => ({ id: `c${index}`, cost_milli: 1 })),
    subset_budget: { field: 'cost_milli', budget: 5 } });
}

function numericBrute(path) {
  const prices = [500, 300, 250], budget = 800;
  const result = [];
  for (let mask = 0; mask < 2 ** prices.length; mask++) {
    const total = prices.reduce((sum, price, index) =>
      sum + (Math.floor(mask / 2 ** index) % 2 ? price : 0), 0);
    result.push(total <= budget ? mask + 1 : 0);
  }
  writeFileSync(path, result.sort((a, b) => a - b).join('\n') + '\n');
}

function planeGenerate(root, rankPath, estatePath, observationPath) {
  const plane = readJson(join(root, 'plane.json'));
  const survivors = plane.survivors.map(row => row.id);
  assert.ok(plane.rejected_before_exact > 0, 'the mask cut rejected nothing; this is not a frontier');
  assert.ok(survivors.length >= 8, `only ${survivors.length} survivors; too few to say anything about order`);

  const order = existsSync(rankPath)
    ? new Map(rows(rankPath, 'select rank, candidate_identity from ranked_candidate_envelope')
      .map(row => [row.candidate_identity, row.rank])) : new Map();
  const ranked = survivors.map((id, index) => ({ id, index }))
    .sort((a, b) => (order.get(a.id) ?? 1e6) - (order.get(b.id) ?? 1e6) || a.index - b.index)
    .map(row => row.id);

  const retained = new Set();
  for (const line of readFileSync(observationPath, 'utf8').split('\n')) {
    let observation;
    try { observation = JSON.parse(line); } catch { continue; }
    assert.ok(observation && typeof observation === 'object' && !Array.isArray(observation),
      'a parsed observation must be a JSON object');
    if (observation.body_path && observation.body_bytes && existsSync(observation.body_path))
      retained.add(observation.twin_id);
  }
  const kinds = new Map(rows(estatePath, 'select id, source_kind from occurrence')
    .map(row => [row.id, row.source_kind]));
  const candidate = id => ({ id, retained: retained.has(id),
    service: ['public_api', 'x402', 'mcp'].includes(kinds.get(id)) });
  for (const [name, sequence] of [['rank', ranked], ['reversed', [...ranked].reverse()], ['plane', survivors]]) {
    writeJson(join(root, `chain_${name}.json`), { job: `chain-${name}`, effect_class: 'PURE',
      work_class: 'discovery', data_scope: 'public', candidate_space: sequence.map(candidate),
      require: { retained: true, service: true } });
  }
}

function planeCheck(root) {
  const sets = new Map(), sums = new Map();
  for (const order of ['rank', 'reversed', 'plane']) {
    const output = readJson(join(root, `out_${order}.json`));
    const by = new Map(output.backends.map(row => [row.backend, row]));
    assert.notEqual(output.agreement, false, `${order}: backends disagreed`);
    const got = new Map();
    for (const name of ['reference', 'hvm2', 'hvm4']) {
      const result = by.get(name)?.result;
      if (result?.length) got.set(name, createHash('sha256').update([...result].sort(codepointOrder).join('|')).digest('hex'));
    }
    if (!got.size && ['reference', 'hvm2', 'hvm4'].every(name =>
      Array.isArray(by.get(name)?.result) && by.get(name).result.length === 0)) {
      console.log('SKIP rank-chain order witness: the current live estate has no retained service survivors');
      return;
    }
    assert.ok(got.size, `${order}: no backend returned a nonempty frontier; rows=${JSON.stringify([...by.values()])}`);
    assert.equal(new Set(got.values()).size, 1, `${order}: backends returned different sets`);
    sets.set(order, got.values().next().value);
    const aggregate = by.get('bend')?.aggregate;
    if (aggregate && (typeof aggregate !== 'object' || Object.keys(aggregate).length))
      sums.set(order, aggregate.index_checksum);
  }
  assert.equal(new Set(sets.values()).size, 1,
    `the survivor set changed with the order it was presented in; a ranker is deciding the answer rather than the sequence: ${JSON.stringify(Object.fromEntries(sets))}`);
  console.log(`  one survivor set across three orders: ${sets.values().next().value.slice(0, 12)}`);
  if (sums.size === 3 && new Set(sums.values()).size === 1)
    throw new Error(`Bend's positional index checksum did not move: ${JSON.stringify(Object.fromEntries(sums))}`);
  if (sums.size) console.log(`  bend index checksums move with order, as a positional sum must: ${JSON.stringify(Object.fromEntries(sums))}`);
}

const commands = new Map([
  ['portfolio-job', [1, portfolioJob]], ['portfolio-output', [2, portfolioOutput]],
  ['portfolio-wide', [1, portfolioWide]], ['numeric-brute', [1, numericBrute]],
  ['plane-generate', [4, planeGenerate]], ['plane-check', [1, planeCheck]],
]);
const [command, ...args] = process.argv.slice(2);
const selected = commands.get(command);
if (!selected || args.length !== selected[0])
  throw new Error(`unknown or incomplete pure-tier witness command: ${command ?? '(missing)'}`);
selected[1](...args);
