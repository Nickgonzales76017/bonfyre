import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cyclePolicy } from '../tools/source_fabric_cycle.mjs';

test('live Source Fabric cycle runs the Bun change-impact owner', () => {
  expect(Object.values(cyclePolicy().programs).every(program => program.runtime === 'bun')).toBe(true);
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-source-cycle-impact.'));
  try {
    const input = join(dir, 'sources.jsonl'), dbPath = join(dir, 'frontier.sqlite');
    const deltaDir = join(dir, 'deltas'), closureDir = join(dir, 'closures'), outDir = join(dir, 'out');
    const foreign = join(dir, 'foreign');
    mkdirSync(deltaDir);
    mkdirSync(closureDir);
    mkdirSync(join(foreign, 'bodies'), { recursive: true });
    writeFileSync(join(foreign, 'bodies', 'twin-1.body'), '<link rel="alternate" type="application/rss+xml" href="/feed.xml">');
    writeFileSync(input, JSON.stringify({ id: 'source:one', url: 'https://example.org/news', source_kind: 'rss' }) + '\n');
    writeFileSync(join(closureDir, 'one.json'), JSON.stringify({ subject: 'subject:one', scope: 'public',
      pack_demands: [{ identity: 'pack:one', candidate_groups: [{ source: 'source:one',
        candidates: [{ identity: 'source:one' }] }] }] }));
    writeFileSync(join(deltaDir, 'one.json'), JSON.stringify({ schema: 'bonfyre-fact-delta.v1',
      writes: ['ForeignTwin:https://example.org/news'] }));
    const db = new Database(dbPath);
    try {
      db.exec('CREATE TABLE closure_dependency (dependency_ref TEXT, subject TEXT, scope TEXT, basis TEXT)');
      db.exec('CREATE TABLE internet_source (id INTEGER, source TEXT, twin TEXT, url TEXT, family TEXT, status TEXT)');
      db.exec(`CREATE TABLE channel(channel_id TEXT,exact_address TEXT,title TEXT,material TEXT,protocol TEXT,
        carries TEXT,cadence TEXT,census_source TEXT,contributions INTEGER,state TEXT);
        INSERT INTO channel VALUES('c1','https://example.org/news','Example','text','RSS','',
          'daily','source:one',1,'admitted')`);
      db.query('INSERT INTO internet_source VALUES (?, ?, ?, ?, ?, ?)').run(
        1, 'source:one', 'twin-1', 'https://example.org/', 'publication', 'published');
      db.query('INSERT INTO closure_dependency VALUES (?, ?, ?, ?)').run(
        'ForeignTwin:https://example.org/news', 'subject:one', 'public', 'foreign_twin');
    } finally { db.close(); }
    const child = Bun.spawnSync(['bun', new URL('../tools/source_fabric_cycle.mjs', import.meta.url).pathname,
      '--input', `TEST=${input}`, '--out-dir', outDir,
      '--scoped-frontier-db', dbPath, '--fact-deltas', deltaDir, '--closure-dir', closureDir,
      '--cms-db', dbPath, '--foreign-root', foreign]);
    expect(child.exitCode).toBe(0);
    const cycle = JSON.parse(readFileSync(join(outDir, 'source-fabric-cycle.json'), 'utf8'));
    expect(cycle.steps.find(step => step.step === 'change-impact')).toMatchObject({ ok: true, returncode: 0 });
    expect(cycle.steps.find(step => step.step === 'recursive-channels')).toMatchObject({ ok: true, returncode: 0 });
    expect(cycle.steps.find(step => step.step === 'pressure')).toMatchObject({ ok: true, returncode: 0 });
    const measurement = JSON.parse(readFileSync(join(outDir, 'source-impact-measurements.jsonl'), 'utf8'));
    expect(measurement).toMatchObject({ root_address: 'https://example.org/', subject_reuse: 1, scene_reuse: 0 });
    expect(JSON.parse(readFileSync(join(outDir, 'source-pressure.jsonl'), 'utf8')))
      .toMatchObject({ candidate_occurrences: 1, candidate_subject_reuse: 1, channel_families: ['RSS'] });
    expect(JSON.parse(readFileSync(join(outDir, 'recursive-channel-candidates.jsonl'), 'utf8')))
      .toMatchObject({ channel_family: 'RSS', source_kind: 'recursive_channel_candidate' });
    expect(cycle.steps.find(step => step.step === 'frontier').ok).toBe(true);
    expect(cycle.steps.some(step => step.step.startsWith('decision'))).toBe(false);
    const experiment = Bun.spawnSync(['bun', new URL('../tools/source_fabric_cycle.mjs', import.meta.url).pathname,
      '--input', `TEST=${input}`, '--out-dir', outDir, '--decision-frontier-db', dbPath]);
    expect(experiment.exitCode).not.toBe(0);
    expect(new TextDecoder().decode(experiment.stderr)).toContain('no avoided work has been measured');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
