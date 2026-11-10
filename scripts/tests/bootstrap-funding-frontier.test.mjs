import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');

test('published ProgrammeFabric compiles offline without promoting draft leads', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-funding-frontier-'));
  const cms = join(dir, 'cms.db');
  const out = join(dir, 'public');
  const db = new Database(cms);
  db.exec(`
    CREATE TABLE program_template(template_id TEXT PRIMARY KEY, name TEXT, description TEXT, semantic_slots TEXT, funding_candidates TEXT, status TEXT);
    CREATE TABLE program_instance(instance_id TEXT PRIMARY KEY, template_id TEXT, owner_subject TEXT, site_id TEXT, archetype TEXT, status TEXT);
    CREATE TABLE funding_work_package(package_id TEXT PRIMARY KEY, instance_id TEXT, funding_program TEXT, status TEXT);
    INSERT INTO program_template VALUES ('p1','Open local knowledge fabric','A real programme.','["collections","people","events"]','["IMLS NLG-L","NSF S&CC"]','published');
    INSERT INTO program_instance VALUES ('program:knowledge','p1','person:nick','nick','knowledge-culture','published');
    INSERT INTO funding_work_package VALUES ('package:1','program:knowledge','IMLS NLG-L','published');
    INSERT INTO funding_work_package VALUES ('package:2','program:knowledge','NSF S&CC','published');
    INSERT INTO funding_work_package VALUES ('package:draft','program:knowledge','private lead','draft');
  `);
  db.close();
  const result = Bun.spawnSync(['bun', join(root, 'scripts/bootstrap_funding_frontier.mjs'), '--cms', cms, '--out', out, '--offline']);
  expect(result.exitCode).toBe(0);
  const report = JSON.parse(readFileSync(join(out, 'funding-frontier.json'), 'utf8'));
  const page = readFileSync(join(out, 'funding.html'), 'utf8');
  expect(report.schema).toBe('bonfyre-funding-frontier.v1');
  expect([report.programme_count, report.work_package_count, report.candidate_count]).toEqual([1, 2, 2]);
  expect(Object.keys(report.observations).sort()).toEqual(['IMLS NLG-L', 'NSF S&CC']);
  expect(Object.values(report.observations).every(row => row.observation_state === 'not_observed')).toBe(true);
  expect(page).not.toContain('private lead');
  for (const phrase of ['Open local knowledge fabric', 'Public edition', 'Connected edition', 'Operator edition', 'Start a pilot conversation', 'not treated as zero opportunities']) expect(page).toContain(phrase);
  expect(report.programmes[0].edition_contract.public.title).toBe('Public edition');
  expect(report.cannot_infer.some(value => value.includes('eligibility'))).toBe(true);
});
