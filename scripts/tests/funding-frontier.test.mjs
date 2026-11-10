import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { run } from '../funding-frontier.mjs';

test('combination closure lets one route unlock another without inventing capacity', () => {
  const path = join(mkdtempSync(join(tmpdir(), 'bonfyre-frontier-')), 'cms.db');
  const db = new Database(path);
  db.exec(`
    CREATE TABLE funding_route(institution TEXT, route TEXT, requires TEXT, satisfies TEXT, capacity TEXT, capacity_state TEXT, status TEXT);
    INSERT INTO funding_route VALUES ('one','grant','seed','equity','100','EVIDENCED','published'),('one','bank','equity','funded','1000','UNKNOWN','published'),('two','other','','funded','9999','EVIDENCED','published');
    CREATE TABLE funding_requirement(institution TEXT, requirement TEXT, state TEXT, resolvable_by TEXT, status TEXT);
    INSERT INTO funding_requirement VALUES ('one','seed','SATISFIED','evidence','published'),('one','equity','BLOCKED','capital','published'),('one','funded','BLOCKED','capital','published');
  `);
  db.close();
  const receipt = run(path, 'one');
  expect(receipt.worlds_evaluated).toBe(3);
  expect(receipt.best_world.routes).toEqual(['grant', 'bank']);
  expect(receipt.best_world.requirements_closed).toEqual(['equity', 'funded', 'seed']);
  expect(receipt.evidenced_capacity_ceiling).toBe(100);
  expect(receipt.routes_of_unknown_capacity).toEqual(['bank']);
  expect(receipt.effect_performed).toBe(false);
});
