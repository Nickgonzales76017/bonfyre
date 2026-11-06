import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileRegistry, robotsAllows } from '../tools/compile_dataset_registry.mjs';

const registry = `Agriculture
-----------
* |OK_ICON| \`Harvest Records - public data <https://example.org/api/harvest.json>\`_ [\`Meta <https://meta.example.org>\`_]
* |FIXME_ICON| \`Broken Dataset <https://broken.example.org/data.csv>\`_

Music
-----
* |OK_ICON| \`Public Recordings <https://example.org/music.csv>\`_
`;

test('registry entries remain candidate advertisements and preserve distinct refusal reasons', async () => {
  const compiled = await compileRegistry(registry);
  expect(compiled.summary).toEqual({ entries: 3, categories: 2, hosts_probed: 0,
    states: { REFUSED: 1, SHADOW: 2 } });
  expect(compiled.rows[0].advertisement).toMatchObject({ candidate_kind: 'external_source',
    exact_verifier: 'SourcePassport:registry-candidate', lowerer_ref: 'SourcePassport:unlowered' });
  expect(compiled.rows[0].advertisement.terms).toContain('domain:agriculture');
  expect(compiled.rows[1].passport.state).toBe('REFUSED');
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-dataset-registry.'));
  try {
    const readme = join(dir, 'README.rst'), output = join(dir, 'candidates.jsonl');
    const catalogue = join(dir, 'catalogue.jsonl');
    writeFileSync(readme, registry);
    const bun = Bun.spawnSync(['bun', new URL('../tools/compile_dataset_registry.mjs', import.meta.url).pathname,
      '--readme', readme, '--out', output, '--catalogue', catalogue]);
    expect(bun.exitCode).toBe(0);
    expect(readFileSync(output, 'utf8').trim().split('\n').map(JSON.parse))
      .toEqual(compiled.rows.map(row => row.advertisement));
    expect(readFileSync(catalogue, 'utf8').trim().split('\n').map(JSON.parse))
      .toEqual(compiled.rows.map(row => row.catalogue));
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('robots rules choose specific group and longest matching allow/disallow', async () => {
  const rules = `User-agent: *
Disallow: /private
Allow: /private/open

User-agent: BonfyreProject
Disallow: /blocked
Allow: /blocked/public
`;
  expect(robotsAllows(rules, 'BonfyreProject/1.0', 'https://example.org/blocked/a')).toBe(false);
  expect(robotsAllows(rules, 'BonfyreProject/1.0', 'https://example.org/blocked/public/a')).toBe(true);
  expect(robotsAllows('User-agent: *\nDisallow: /private/*/sealed$\n', 'BonfyreProject/1.0',
    'https://example.org/private/a/sealed')).toBe(false);
  const seen = [];
  const fetcher = async address => { seen.push(address); return { ok: true, text: async () => rules }; };
  const compiled = await compileRegistry(registry, { probe: true, fetcher }, undefined);
  expect(compiled.summary.hosts_probed).toBe(2);
  expect(seen).toEqual(['https://example.org/robots.txt', 'https://broken.example.org/robots.txt']);
  const oversized = await compileRegistry(registry, { probe: true,
    fetcher: async () => ({ ok: true, text: async () => 'x'.repeat(600000) }) }, undefined);
  expect(oversized.rows[0].passport.carrier.robots).toBe('unobserved');
});
