import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { run } from '../funding-closure.mjs';

test('institution-scoped demand includes draft requests while market remains unmeasured', () => {
  const path = join(mkdtempSync(join(tmpdir(), 'bonfyre-funding-')), 'cms.db');
  const db = new Database(path);
  db.exec(`
    CREATE TABLE funding_requirement(institution TEXT, requirement TEXT, state TEXT, kind TEXT, resolvable_by TEXT, amount TEXT, notes TEXT, status TEXT);
    INSERT INTO funding_requirement VALUES ('one','proof','SATISFIED','doc','cheaply','200','','published'),('one','partner','UNKNOWN','relationship','partner','0','need a partner','published'),('two','other','SATISFIED','doc','cheaply','9999','','published');
    CREATE TABLE budget_request(institution TEXT, amount TEXT, lifecycle_state TEXT, status TEXT);
    INSERT INTO budget_request VALUES ('one','100','draft','draft'),('one','50','committed','published'),('two','9999','committed','published');
    CREATE TABLE provider_route(route TEXT, capability TEXT, status TEXT);
    INSERT INTO provider_route VALUES ('capital-a','capital.route','published');
  `);
  db.close();
  const receipt = run(path, 'one');
  expect(receipt.declared_demand).toBe(150);
  expect(receipt.committed_demand).toBe(50);
  expect(receipt.ceilings.InternalizedCeiling.amount).toBe(200);
  expect(receipt.ceilings.MarketCeiling.measured).toBe(false);
  expect(receipt.residual.PartnerResidual.count).toBe(1);
  expect(receipt.declared.requirement_names).toEqual(['partner', 'proof']);
});
