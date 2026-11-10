import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mutationWitness, validatePlan, wrongValue } from './witness_mutation.mjs';

const plan = { effect_class: 'PURE', tables: { capital_transformation: ['capital_guard'] }, suites: { capital_guard: { argv: ['fixture-suite'] } }, producers: { capital_transformation: { argv: ['fixture-producer'] } }, rows_per_table: 1 };

test('mutation candidates retain type and change value', () => {
  expect(wrongValue(0)).toBe(7);
  expect(wrongValue('a,b')).toBe('a');
  expect(wrongValue('UNKNOWN')).toBe('UNKNOWN-mutated');
});

test('plan refuses shell strings and non-pure effect claims', () => {
  expect(() => validatePlan({ ...plan, suites: { capital_guard: { cmd: 'sh -c anything' } } })).toThrow('argv array');
  expect(() => validatePlan({ ...plan, effect_class: 'WRITE' })).toThrow('PURE');
});

test('a computed consequence is guarded; dry-run does not execute', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bonfyre-mut-test-'));
  try {
    const source = join(dir, 'cms.db'), db = new Database(source);
    db.exec("CREATE TABLE capital_transformation(id INTEGER PRIMARY KEY, status TEXT, cash_delta REAL, notes TEXT); INSERT INTO capital_transformation VALUES(1,'published',0,'hello');");
    db.close();
    let calls = 0;
    const runner = (spec, env) => {
      calls++;
      const current = new Database(env.BONFYRE_CMS_DB, { readonly: true, create: false });
      const row = current.query("SELECT cash_delta FROM capital_transformation WHERE status='published'").get();
      current.close();
      if (spec.argv[0] === 'fixture-producer') return { code: 0, stdout: JSON.stringify({ declared: { cash_delta: row?.cash_delta }, computed: row?.cash_delta ?? null }) };
      return row ? { code: row.cash_delta === 0 ? 0 : 1, stdout: 'asserted' } : { code: 0, stdout: '(no capital transformation; skipping)' };
    };
    const dry = await mutationWitness({ source, plan, runner });
    expect(dry.note).toContain('dry run');
    expect(calls).toBe(0);
    const receipt = await mutationWitness({ source, plan, apply: true, runner, limit: 2 });
    expect(receipt.suites_at_baseline.capital_guard).toBe('pass');
    expect(receipt.guarded.some(g => g.field === 'cash_delta' && g.guard === 'consequence')).toBe(true);
    expect(receipt.skipped.some(([name]) => name === 'capital_transformation.status')).toBe(true);
    expect(receipt.blind.length).toBe(0);
    expect((await readdir(dir)).sort()).toEqual(['cms.db']);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
