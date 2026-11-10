import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { Database } from 'bun:sqlite';
import { spawnSync } from 'node:child_process';
import { loadDatasets, existingKeys, main } from './install_authored_rows.mjs';

test('authored CMS rows are schema-checked, unique, and locally installable', () => {
  const datasets = loadDatasets();
  expect(datasets.map(({ type, rows }) => [type, rows.length])).toEqual([
    ['content_screen', 4],
    ['reference_contract', 5],
    ['presentation_template', 6],
    ['territory_presentation', 4],
  ]);
  expect(datasets.find(dataset => dataset.type === 'content_screen').rows.filter(row => row.state === 'declared')).toHaveLength(2);
  expect(datasets.find(dataset => dataset.type === 'territory_presentation').rows.find(row => row.territory === 'work').sections).toContain('blocking');
});

test('existing authored keys refuse a second import before any write', () => {
  const dir = mkdtempSync(resolve(tmpdir(), 'bonfyre-authored-rows-'));
  try {
    const path = resolve(dir, 'cms.db');
    const db = new Database(path);
    db.run('CREATE TABLE content_screen (namespace TEXT, screen TEXT)');
    db.run("INSERT INTO content_screen VALUES ('root', 'native-text-normalised')");
    db.close();
    expect(existingKeys(path, 'root', loadDatasets()[0])).toEqual(['native-text-normalised']);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('native CMS imports all four authored datasets into a local database', () => {
  const dir = mkdtempSync(resolve(tmpdir(), 'bonfyre-authored-import-'));
  const root = resolve(import.meta.dir, '../../../..');
  const cms = resolve(root, 'cmd/BonfyreCMS/bonfyre-cms');
  const schemas = resolve(root, 'cmd/BonfyreCMS/content-types');
  const dbPath = resolve(dir, 'cms.db');
  try {
    for (const dataset of loadDatasets()) {
      const schema = resolve(schemas, `${dataset.type}.json`);
      const applied = spawnSync(cms, ['schema', 'apply', schema, '--db', dbPath, '--schemas', schemas], { encoding: 'utf8' });
      expect(applied.status).toBe(0);
      expect(main(['--apply', '--type', dataset.type, '--db', dbPath, '--ns', 'root', '--cms', cms])).toBe(0);
      const db = new Database(dbPath, { readonly: true });
      try {
        expect(db.query(`SELECT count(*) AS count FROM "${dataset.type}" WHERE namespace='root'`).get().count).toBe(dataset.rows.length);
      } finally {
        db.close();
      }
      expect(() => main(['--apply', '--type', dataset.type, '--db', dbPath, '--ns', 'root', '--cms', cms])).toThrow('existing authored keys');
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('current page files are the authored sources, not embedded Python output', () => {
  const surfaces = resolve(import.meta.dir, '..');
  for (const name of ['world', 'console', 'packs']) {
    const html = readFileSync(resolve(surfaces, `${name}.html`), 'utf8');
    expect(html.startsWith('<!doctype html>')).toBe(true);
    expect(html).toContain('</html>');
  }
  expect(readFileSync(resolve(surfaces, 'world.html'), 'utf8')).toContain('/assets/world.css');
});
