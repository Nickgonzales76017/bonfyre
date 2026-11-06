import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { actorId, run } from '../build-actor-graph.mjs';

test('source-scoped name identity and asserted provenance survive native cutover', () => {
  const estate = mkdtempSync(join(tmpdir(), 'bonfyre-actors-'));
  const db = new Database(join(estate, 'items.db'));
  db.exec("CREATE TABLE item(source TEXT, by TEXT); INSERT INTO item VALUES ('feed-a','Alex Lee'),('feed-a','Alex Lee'),('feed-b','Alex Lee'),('feed-a','publisher.test')");
  db.close();
  expect(actorId('feed-a', 'Alex Lee').id).not.toBe(actorId('feed-b', 'Alex Lee').id);
  expect(actorId('feed-a', 'publisher.test').kind).toBe('publication');
  const dry = run({ estate, repo: '/fixture' });
  expect(dry).toEqual({ makers: 3, same_name_across_multiple_sources: 1, sample: ['Alex Lee', 'publisher.test', 'Alex Lee'] });
  const commands = [];
  const command = (binary, args) => {
    commands.push({ binary, args });
    return { status: 0, stdout: '{"unverified":3}' };
  };
  const applied = run({ estate, repo: '/fixture', apply: true, command });
  expect(applied.actors_recorded).toBe(3);
  expect(applied.unverified_actors).toBe(3);
  expect(commands.filter(({ args }) => args[0] === 'upsert')).toHaveLength(3);
  expect(commands.every(({ args }) => args[0] !== 'upsert' || args.includes('asserted'))).toBe(true);
});
