import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { membraneDatabase, membraneDescriptors, membraneReadership, membraneSources,
  membraneSummary, refreshMembrane } from '../membrane-store.mjs';

const passport = descriptors => ({ root: 'https://publisher.test/', best_carrier: 'feed',
  robots_status: 200, robots_allows_us: true, sitemaps: [], feeds: [{ url: 'https://publisher.test/feed' }],
  websub_hubs: [], structured_types: [], conditional: {},
  protocol_descriptors: descriptors.map(([family, descriptor]) => ({ family, descriptor,
    truth_level: 'declared_descriptor' })) });
const temp = async fn => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-membrane.'));
  try { return await fn(directory); }
  finally { rmSync(directory, { recursive: true, force: true }); }
};

test('refresh stores source and descriptor candidates; missing descriptors cool without vanishing', async () => {
  await temp(async directory => {
    const dbPath = join(directory, 'membrane.db');
    const first = passport([['mcp-descriptor-candidate', 'https://publisher.test/.well-known/mcp.json'],
      ['oauth-server-candidate', 'https://publisher.test/.well-known/oauth-authorization-server']]);
    const second = passport([['mcp-descriptor-candidate', 'https://publisher.test/.well-known/mcp.json']]);
    let latest = first;
    expect(await refreshMembrane(['publisher.test'], { dbPath, discover: async () => latest }))
      .toEqual([{ root: first.root, carrier: 'feed', rung: 3, declined: null }]);
    latest = second;
    await refreshMembrane(['publisher.test'], { dbPath, discover: async () => latest });
    const rows = membraneDescriptors(dbPath);
    expect(rows.descriptors).toHaveLength(2);
    expect(rows.descriptors.find(row => row.family === 'oauth-server-candidate').active).toBe(0);
    expect(rows.descriptors.find(row => row.family === 'mcp-descriptor-candidate').active).toBe(1);
    expect(rows.forbidden_inference).toContain('not a live endpoint');
    const db = membraneDatabase(dbPath);
    try { expect(db.query('SELECT probes,descriptors FROM source_passport').get())
      .toMatchObject({ probes: 2, descriptors: 1 }); }
    finally { db.close(); }
  });
});

test('readership folds claimed crawler demand and surfaces without treating a user agent as identity', async () => {
  await temp(directory => {
    const dbPath = join(directory, 'membrane.db'), logPath = join(directory, 'access.log');
    writeFileSync(logPath, [
      '127.0.0.1 - - [29/Sep/2026:12:00:00 +0000] "GET /robots.txt HTTP/1.1" 404 20 "-" "Googlebot/2.1"',
      '127.0.0.1 - - [29/Sep/2026:12:00:01 +0000] "GET /feed.xml HTTP/1.1" 200 20 "-" "Feedly/1.0"',
      '127.0.0.1 - - [29/Sep/2026:12:00:02 +0000] "GET /story HTTP/1.1" 200 20 "-" "Mozilla/5.0"',
    ].join('\n'));
    expect(membraneReadership(logPath, { dbPath })).toEqual({ agents: 2, machine_requests: 2 });
    const summary = membraneSummary(dbPath);
    expect(summary.outbound).toMatchObject({ declared_agents: 2, answered: 1, missed: 1 });
    expect(summary.outbound.readers.find(row => row.agent === 'googlebot').top_path).toBe('/robots.txt');
    expect(summary.forbidden_inference).toContain('not a credential');
  });
});

test('CLI summary and source list read the same durable store', async () => {
  await temp(async directory => {
    const dbPath = join(directory, 'membrane.db');
    await refreshMembrane(['publisher.test'], { dbPath,
      discover: async () => passport([]) });
    expect(membraneSources(dbPath)).toMatchObject([{ root: 'https://publisher.test/',
      best_carrier: 'feed', rung: 3 }]);
    const cli = Bun.spawnSync(['bun', new URL('../membrane-store.mjs', import.meta.url).pathname,
      'summary'], { env: { ...process.env, BONFYRE_MEMBRANE_DB: dbPath } });
    expect(cli.exitCode).toBe(0);
    expect(JSON.parse(new TextDecoder().decode(cli.stdout)).inbound)
      .toMatchObject({ sources: 1, readable_cheaply: 1 });
  });
});
