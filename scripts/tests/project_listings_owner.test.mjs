import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('../../', import.meta.url).pathname;
const commerce = join(root, 'cmd/BonfyreCommerce/bonfyre-commerce');
const cms = join(root, 'cmd/BonfyreCMS/bonfyre-cms');
const projector = join(root, 'scripts/project_listings.mjs');

test('listing projection writes one CMS-owned observed offer and remains idempotent', () => {
  expect(existsSync(commerce)).toBe(true);
  expect(existsSync(cms)).toBe(true);
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-listings-owner.'));
  try {
    const estate = join(dir, 'estate'); mkdirSync(estate);
    const archive = new Database(join(estate, 'items.db'));
    archive.exec('CREATE TABLE item(source TEXT,key TEXT,title TEXT,href TEXT,publisher TEXT,at INTEGER)');
    const insert = archive.query('INSERT INTO item VALUES(?,?,?,?,?,?)');
    insert.run('bat', 'lot-1', 'A lawful observed lot', 'https://bringatrailer.com/listing/example/',
      'Bring a Trailer', 1789999200);
    insert.run('bat', 'news-1', 'Editorial post', 'https://bringatrailer.com/features/example/',
      'Bring a Trailer', 1789999100);
    archive.close();
    for (let attempt = 0; attempt < 2; attempt++) {
      const result = Bun.spawnSync(['bun', projector, '--estate', estate, '--repo', root,
        '--cms', cms, '--apply']);
      expect(result.exitCode).toBe(0);
      expect(JSON.parse(new TextDecoder().decode(result.stdout))).toMatchObject({ lots: 1,
        canonical_offers_written: 1 });
    }
    const db = new Database(join(estate, 'cms.db'), { readonly: true });
    expect(db.query('SELECT COUNT(*) AS n FROM service_offer').get().n).toBe(1);
    const offer = db.query(`SELECT offer_id,source_ref,marketplace,canonical_href,observed_at,
      assertion_state,offer_status,status,cannot_infer FROM service_offer`).get();
    expect(offer).toMatchObject({ offer_id: 'lot:bringatrailer.com:lot-1', source_ref: 'bat',
      marketplace: 'bringatrailer.com', canonical_href: 'https://bringatrailer.com/listing/example/',
      observed_at: '2026-09-21T14:00:00Z', assertion_state: 'OBSERVED',
      offer_status: 'observed', status: 'published' });
    expect(offer.cannot_infer).toContain('not a claim that the thing exists');
    db.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
