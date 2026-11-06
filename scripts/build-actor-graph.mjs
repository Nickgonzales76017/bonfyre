#!/usr/bin/env bun
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { homedir } from 'node:os';
import { join } from 'node:path';

const slug = value => value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 72);
const digest = value => createHash('sha256').update(value).digest('hex').slice(0, 16);

export function actorId(source, name) {
  const kind = name.includes('.') && !name.trim().includes(' ') ? 'publication' : 'person';
  return { kind, id: `${kind}:${slug(name) || 'unnamed'}--${digest(`${source}\0${name}`)}` };
}

export function makers(db, minItems = 1, limit = 100000) {
  const rows = db.query("SELECT source, by AS name, COUNT(*) AS made FROM item WHERE COALESCE(by,'') <> '' GROUP BY source, by HAVING made >= ? ORDER BY made DESC, source, by LIMIT ?").all(minItems, limit);
  const repeated = db.query("SELECT COUNT(*) AS n FROM (SELECT by FROM item WHERE COALESCE(by,'') <> '' GROUP BY by HAVING COUNT(DISTINCT source) > 1)").get().n;
  return { rows, repeated };
}

export function run({ estate, repo, minItems = 1, limit = 100000, apply = false, command = spawnSync }) {
  const db = new Database(join(estate, 'items.db'), { readonly: true });
  let source;
  try { source = makers(db, minItems, limit); } finally { db.close(); }
  const { rows, repeated } = source;
  if (!apply) return { makers: rows.length, same_name_across_multiple_sources: repeated, sample: rows.slice(0, 5).map(row => row.name.slice(0, 48)) };
  const binary = join(repo, 'cmd/BonfyreActors/bonfyre-actors');
  const store = join(estate, 'actors.db');
  let recorded = 0, refused = 0;
  for (const maker of rows) {
    const { id, kind } = actorId(maker.source, maker.name);
    const result = command(binary, ['upsert', '--db', store, '--id', id, '--kind', kind, '--name', maker.name.slice(0, 180), '--provenance', `source ${maker.source} credited this name on ${maker.made} things this estate kept`, '--confidence', 'asserted'], { encoding: 'utf8' });
    if (result.status === 0) recorded++;
    else refused++;
  }
  const standing = command(binary, ['standing', '--db', store], { encoding: 'utf8' });
  let unverified;
  try { unverified = JSON.parse(standing.stdout).unverified ?? null; } catch { unverified = null; }
  return {
    source_bound_name_observations: rows.length, actors_recorded: recorded, refused,
    same_name_across_multiple_sources: repeated, unverified_actors: unverified, store,
    cannot_infer: 'every actor here is a source-bound asserted name observation. None is verified, and equal names from different sources are never merged or counted as corroboration of one person',
  };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
  if (args.some(arg => arg.startsWith('--') && !['--estate', '--repo', '--min-items', '--limit', '--apply'].includes(arg))) {
    console.error('usage: bun scripts/build-actor-graph.mjs [--estate PATH] [--repo PATH] [--min-items N] [--limit N] [--apply]');
    process.exit(2);
  }
  try {
    const result = run({
      estate: option('--estate', join(homedir(), '.bonfyre/estate-local')),
      repo: option('--repo', join(homedir(), 'Developer/Bonfyre')),
      minItems: Number(option('--min-items', 1)), limit: Number(option('--limit', 100000)), apply: args.includes('--apply'),
    });
    console.log(JSON.stringify(result, null, 2));
  } catch (error) { console.error(`build-actor-graph: ${error.message}`); process.exit(1); }
}
