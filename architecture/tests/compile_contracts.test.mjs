import { test, expect } from 'bun:test';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readFileSync } from 'node:fs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const compiler = resolve(root, 'cmd/BonfyreAtlas/bonfyre-atlas');
const read = (path) => readFileSync(resolve(root, path), 'utf8');

for (const mode of ['app-lineages', 'carriers', 'settlement', 'institutions']) {
  test(`${mode} output agrees with the current YAFF declaration`, () => {
    const result = Bun.spawnSync([compiler, 'compile-contracts', mode, '--check-generated'], { cwd: root });
    expect(result.exitCode).toBe(0);
  });
}

test('lineage JSON and C table retain every declared row', () => {
  const json = JSON.parse(read('integrations/mcp/app_lineages_contract.json'));
  const header = read('engine/core/include/bf_app_lineages.h');
  expect(json.lineages).toHaveLength(json.count);
  expect(header).toContain(`#define BF_APP_LINEAGE_N ${json.count}`);
  for (const row of json.lineages) expect(header).toContain(JSON.stringify(row.lineage));
});

test('institution contract names grants, not automatic admission', () => {
  const json = JSON.parse(read('integrations/mcp/institution_contract.json'));
  const header = read('engine/core/include/bf_institutions.h');
  expect(json.slice_estates).not.toContain('bonfyre');
  expect(json.forbidden_inference).toContain('never who may enter');
  for (const [child, parent] of Object.entries(json.parents)) {
    expect(header).toContain(`{${JSON.stringify(child)}, ${JSON.stringify(parent)}}`);
  }
});

test('settlement declaration is shared by all quote carriers', () => {
  const text = read('architecture/economic/settlement.contract.yaff');
  const header = read('cmd/BonfyreWebAddress/src/bf_settlement.h');
  const carriers = [...text.matchAll(/^  carrier (\S+)$/gm)].map((match) => match[1]);
  expect(header).toContain(`#define BF_SETTLEMENT_CARRIER_N ${carriers.length}`);
  for (const name of carriers) expect(header).toContain(JSON.stringify(name));
});
