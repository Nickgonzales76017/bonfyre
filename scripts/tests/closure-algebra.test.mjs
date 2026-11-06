import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { run } from '../closure-algebra.mjs';

export function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-closure-'));
  const path = join(dir, 'cms.db');
  const db = new Database(path);
  db.exec(`
    CREATE TABLE closure_domain(domain TEXT, status TEXT, requirement_type TEXT, resolver_type TEXT, scope_field TEXT, state_field TEXT, satisfied_state TEXT, requires_field TEXT, satisfies_field TEXT, resolver_class_field TEXT, resolver_field TEXT, cannot_infer TEXT);
    INSERT INTO closure_domain VALUES ('fixture','published','closure_requirement','closure_resolver','subject','state','MET','needs','closes','class','resolver','closed is not approved');
    CREATE TABLE closure_requirement(requirement TEXT, subject TEXT, state TEXT, kind TEXT, resolvable_by TEXT, status TEXT);
    INSERT INTO closure_requirement VALUES ('seed','x','MET','fact','','published'),('a','x','BLOCKED','dependency','','published'),('b','x','UNKNOWN','decision','','published'),('c','x','BLOCKED','dependency','','published');
    CREATE TABLE closure_resolver(resolver TEXT, subject TEXT, needs TEXT, closes TEXT, class TEXT, commitment TEXT, capacity_state TEXT, cost_usd TEXT, success_probability TEXT, status TEXT);
    INSERT INTO closure_resolver VALUES ('first','x','seed','a','capital','conditional','','0','1','published'),('second','x','a','c','social','reserved','','10','0.5','published'),('third','x','b','c','social','interested','','','','published'),('fourth','x','a,b','c','social','conditional','','','0.9','published'),('bad','x','seed','','other','conditional','','-1','2.2','published');
  `);
  db.close();
  return path;
}

test('work-list closure preserves unknown, commitment, recomputed gain, reuse and price distinctions', () => {
  const path = fixture();
  const receipt = run(path, 'fixture', 'x');
  expect(receipt.closed_at_fixed_point).toEqual(['a', 'c', 'seed']);
  expect(receipt.unknown_not_counted_as_blocked).toEqual(['b']);
  expect(receipt.resolvers_admitted).toBe(3);
  expect(receipt.resolvers_below_commitment.map(row => row.resolver)).toEqual(['third']);
  expect(receipt.closure_hypotheses.find(row => row.condition === 'b').was_unknown).toBe(true);
  expect(receipt.resolver_leverage.find(row => row.resolver === 'first').free).toBe(true);
  expect(receipt.resolver_leverage.find(row => row.resolver === 'fourth').priced).toBe(false);
  expect(receipt.declaration_faults.map(row => row.field)).toEqual(['success_probability', 'cost_usd']);
  expect(receipt.evidence_reuse.find(row => row.condition === 'a').reuse_factor).toBe(2);
});
