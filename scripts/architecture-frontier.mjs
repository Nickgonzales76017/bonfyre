#!/usr/bin/env bun
// A read-only projection over the verified Atlas graph; never authors bridges.
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { existsSync, readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const ATLAS_ORGAN = join(dirname(fileURLToPath(import.meta.url)), '../cmd/BonfyreAtlas');
// The verified Atlas index, checked by the native atlas (cmd/BonfyreAtlas).
function nativeAtlas(argv, root) {
  const bin = process.env.BONFYRE_ATLAS_BIN || join(ATLAS_ORGAN, 'bonfyre-atlas');
  if (!existsSync(bin)) spawnSync('make', ['-s', '-C', ATLAS_ORGAN]);
  const r = spawnSync(bin, argv, { encoding: 'utf8', maxBuffer: 64 << 20, env: { ...process.env, BONFYRE_ATLAS_DIR: join(root, 'architecture') } });
  if (r.status !== 0) throw new Error((r.stderr || r.stdout || 'bonfyre-atlas failed').trim());
  return r.stdout;
}
function loadVerifiedAtlas(root) {
  const index = JSON.parse(readFileSync(join(root, 'architecture', 'atlas.index.json'), 'utf8'));
  const actual = JSON.parse(nativeAtlas(['digest'], root)), recorded = index.provenance ?? {};
  if (actual.declaration_digest !== recorded.declaration_digest || actual.declaration_files !== recorded.declaration_files)
    throw new Error('Atlas index is stale: run bun architecture/atlas.mjs export before using this projection');
  return index;
}
// architecture id -> declaring file name (last declaration wins, files in localeCompare order)
function architectureDeclarationFiles(root) {
  const files = nativeAtlas(['files'], root).split('\n').filter(Boolean).sort((a, b) => a.localeCompare(b, 'en'));
  const locations = new Map();
  for (const file of files) for (const match of readFileSync(file, 'utf8').matchAll(/^architecture ([^\s]+)\s*$/gm)) locations.set(match[1], file.split('/').at(-1));
  return locations;
}

const root = resolve(process.env.BONFYRE_ATLAS_ROOT ?? dirname(dirname(fileURLToPath(import.meta.url))));
const json = process.argv.includes('--json');
const floor = 20;
let index;
try {
  index = loadVerifiedAtlas(root);
} catch (error) {
  const detail = { error: 'atlas_not_readable_here', read_from: join(root, 'architecture'),
    note: error.message };
  if (json) process.stdout.write(JSON.stringify(detail, null, 2) + '\n');
  else process.stderr.write(`  ${detail.note}\n`);
  process.exit(2);
}
const arch = index.architectures;
const declarationFiles = architectureDeclarationFiles(root);
const names = Object.keys(arch).sort();
if (names.length < floor) {
  const detail = { error: 'atlas_not_readable_here', read_from: join(root, 'architecture'),
    architectures_found: names.length, floor,
    note: 'this host has no complete architecture tree at that path; set BONFYRE_ATLAS_ROOT' };
  if (json) process.stdout.write(JSON.stringify(detail, null, 2) + '\n');
  else process.stderr.write(`  ${detail.note}\n`);
  process.exit(2);
}
const edges = Object.values(index.interactions).filter(edge => arch[edge.source] && arch[edge.destination]);
const degree = new Map(names.map(name => [name, 0]));
for (const edge of edges) {
  degree.set(edge.source, degree.get(edge.source) + 1);
  degree.set(edge.destination, degree.get(edge.destination) + 1);
}
const connected = names.filter(name => degree.get(name) > 0);
const isolated = names.filter(name => degree.get(name) === 0);
const terms = name => new Set([
  ...(arch[name].node_types ?? []), ...(arch[name].edge_types ?? []),
  ...(arch[name].state_types ?? []),
].map(value => value.toLowerCase()));
const termMap = new Map(names.map(name => [name, terms(name)]));
const population = new Map();
const add = key => population.set(key, (population.get(key) ?? 0) + 1);
for (const name of names) {
  const meta = arch[name];
  if (meta.family) add(`family:${meta.family}`);
  if (meta.domain) add(`domain:${meta.domain}`);
  for (const term of termMap.get(name)) add(`term:${term}`);
}
const round2 = value => Math.round(value * 100) / 100;
const meaningful = 1.5;
const weight = key => round2(Math.log(names.length / (population.get(key) ?? names.length)));
function affinity(a, b) {
  const found = [];
  const push = (key, signal) => {
    const score = weight(key);
    if (score >= meaningful) found.push({ signal, weight: score, held_by: population.get(key) });
  };
  if (arch[a].family && arch[a].family === arch[b].family)
    push(`family:${arch[a].family}`, `family ${arch[a].family}`);
  if (arch[a].domain && arch[a].domain === arch[b].domain)
    push(`domain:${arch[a].domain}`, `domain ${arch[a].domain}`);
  for (const term of [...termMap.get(a)].filter(term => termMap.get(b).has(term)).sort())
    push(`term:${term}`, `shares ${term}`);
  return found;
}
const hubs = [];
for (const hub of connected) {
  const reachable = [];
  for (const lonely of isolated) {
    const why = affinity(hub, lonely);
    if (why.length) reachable.push({ architecture: lonely, affinity: why,
      strength: round2(why.reduce((sum, row) => sum + row.weight, 0)), maturity: arch[lonely].maturity });
  }
  if (reachable.length) {
    reachable.sort((a, b) => b.strength - a.strength);
    hubs.push({ hub, degree: degree.get(hub), isolated_within_reach: reachable.length,
      total_strength: round2(reachable.reduce((sum, row) => sum + row.strength, 0)),
      candidates: reachable.slice(0, 8) });
  }
}
hubs.sort((a, b) => b.total_strength - a.total_strength);
const orphans = isolated.filter(name => !connected.some(other => affinity(name, other).length))
  .map(name => ({ architecture: name, maturity: arch[name].maturity,
    file: declarationFiles.get(name) ?? '' }));
const receipt = {
  schema: 'bonfyre-architecture-frontier.v1', ran_at: new Date().toISOString().slice(0, 19) + 'Z',
  effect_class: 'PURE', architectures: names.length, declared_edges: edges.length,
  connected: connected.length, isolated: isolated.length, hubs_by_reach: hubs.slice(0, 10),
  isolated_with_no_affinity_anywhere: orphans.slice(0, 12), orphan_count: orphans.length,
  signal_floor: meaningful,
  cannot_infer: [
    'a high-scoring candidate -> a bridge worth building',
    'a rare shared term -> a real exchange; rarity is evidence, not proof',
    'a shared vocabulary -> something actually exchanged',
    'an isolated architecture -> an unused one',
    'connectivity -> leverage',
  ],
};
if (json) {
  process.stdout.write(JSON.stringify(receipt, null, 2) + '\n');
} else {
  process.stdout.write(`  ${names.length} architectures, ${edges.length} declared edges, ${connected.length} connected, ${isolated.length} isolated\n\n`);
  process.stdout.write('  where one edge would do the most work:\n');
  for (const row of hubs.slice(0, 8))
    process.stdout.write(`    ${row.hub.padEnd(30)} degree ${String(row.degree).padEnd(3)} reaches ${String(row.isolated_within_reach).padStart(3)} isolated   strength ${row.total_strength.toFixed(1).padStart(6)}\n`);
  process.stdout.write(`\n  the strongest single candidates:\n`);
  for (const hub of hubs.slice(0, 3)) for (const candidate of hub.candidates.slice(0, 2)) {
    process.stdout.write(`    ${hub.hub} <-> ${candidate.architecture}\n`);
    process.stdout.write(`        ${candidate.affinity.slice(0, 3).map(row => `${row.signal} (held by ${row.held_by})`).join('; ').slice(0, 76)}\n`);
  }
  process.stdout.write(`\n  ${orphans.length} isolated architectures share nothing with anything connected;\n`);
  process.stdout.write('  those were declared alone rather than left unbridged:\n');
  for (const row of orphans.slice(0, 6))
    process.stdout.write(`    ${row.architecture.padEnd(34)}${row.maturity.padEnd(14)}${row.file}\n`);
}
