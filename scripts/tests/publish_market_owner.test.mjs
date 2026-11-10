import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('../../', import.meta.url).pathname;
const cms = join(root, 'cmd/BonfyreCMS/bonfyre-cms');
const projector = join(root, 'scripts/project_listings.mjs');
const publisher = join(root, 'scripts/publish_market.mjs');

test('public market renders canonical CMS offers, not stale compatibility lots', () => {
  expect(existsSync(cms)).toBe(true);
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-market-owner.'));
  try {
    const estate = join(dir, 'estate'), output = join(dir, 'out'); mkdirSync(estate);
    const archive = new Database(join(estate, 'items.db'));
    archive.exec('CREATE TABLE item(source TEXT,key TEXT,title TEXT,href TEXT,publisher TEXT,at INTEGER)');
    archive.query('INSERT INTO item VALUES(?,?,?,?,?,?)').run('bat', 'owner-lot', 'Owner-backed observed lot',
      'https://bringatrailer.com/listing/owner-backed/', 'Bring a Trailer', 1789999200);
    archive.close();
    const projected = Bun.spawnSync(['bun', projector, '--estate', estate, '--repo', root,
      '--cms', cms, '--apply']);
    expect(projected.exitCode).toBe(0);
    const db = new Database(join(estate, 'cms.db'));
    db.exec(`CREATE TABLE commerce_lot(exchange TEXT PRIMARY KEY, marketplace TEXT, source TEXT,
      title TEXT, href TEXT, observed_at TEXT, price_observed TEXT, currency TEXT, cannot_infer TEXT)`);
    db.query('INSERT INTO commerce_lot VALUES(?,?,?,?,?,?,?,?,?)').run('stale', 'example', 'legacy',
      'Stale compatibility lot', 'https://invalid.example/stale', '2020-01-01T00:00:00Z', '', '',
      'legacy boundary');
    db.close();
    const rendered = Bun.spawnSync(['bun', publisher, '--estate', estate, '--repo', root, '--out', output]);
    expect(rendered.exitCode).toBe(0);
    const page = readFileSync(join(output, 'market.html'), 'utf8');
    expect(page).toContain('Owner-backed observed lot');
    expect(page).toContain('https://bringatrailer.com/listing/owner-backed/');
    expect(page).not.toContain('Stale compatibility lot');
    expect(page).toContain('price not observed');
    expect(page).toContain('not a claim that the thing exists');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
