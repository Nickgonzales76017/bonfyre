import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const program = resolve(import.meta.dir, '..', 'file-sought-family.mjs');

test('search provenance files only unmeasured channels and keeps dry-run read-only', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bonfyre-sought-family-'));
  try {
    const frontier = new Database(join(root, 'frontier.db'));
    frontier.exec(`CREATE TABLE channel(channel_id TEXT PRIMARY KEY,census_source TEXT,material TEXT,contributions INTEGER,state TEXT);
      CREATE TABLE channel_origin(channel_id TEXT,label TEXT);`);
    const channel = frontier.query('INSERT INTO channel VALUES (?,?,?,?,?)');
    channel.run('new', 'new-source', 'news', 0, 'admitted');
    channel.run('measured', 'measured-source', 'news', 4, 'active');
    channel.run('waiting', '', 'news', 0, 'eligible');
    const origin = frontier.query('INSERT INTO channel_origin VALUES (?,?)');
    for (const id of ['new', 'measured', 'waiting']) origin.run(id, 'FAMILY_GAMING');
    const cms = new Database(join(root, 'cms.db'));
    cms.exec('CREATE TABLE internet_source(source TEXT,family TEXT,status TEXT)');
    cms.query('INSERT INTO internet_source VALUES (?,?,?)').run('new-source', 'news', 'published');
    cms.query('INSERT INTO internet_source VALUES (?,?,?)').run('measured-source', 'news', 'published');
    cms.close();
    const run = (...flags) => Bun.spawnSync(['bun', program, '--estate', root, ...flags], {
      stdout: 'pipe', stderr: 'pipe',
    });
    const dry = run();
    expect(dry.exitCode).toBe(0);
    expect(JSON.parse(dry.stdout.toString()).filed).toBe(1);
    expect(frontier.query("SELECT material FROM channel WHERE channel_id='new'").get().material).toBe('news');
    const applied = run('--apply');
    expect(applied.exitCode).toBe(0);
    const report = JSON.parse(applied.stdout.toString());
    expect(report.observed_carriage_preserved).toBe(1);
    expect(report.admitted_without_a_census_row).toBe(1);
    expect(frontier.query("SELECT material FROM channel WHERE channel_id='new'").get().material).toBe('gaming');
    expect(frontier.query("SELECT material FROM channel WHERE channel_id='measured'").get().material).toBe('news');
    const readCms = new Database(join(root, 'cms.db'), { readonly: true });
    expect(readCms.query("SELECT family FROM internet_source WHERE source='new-source'").get().family).toBe('gaming');
    readCms.close();
    frontier.close();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
