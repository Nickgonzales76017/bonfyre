#!/usr/bin/env bun
// Heterogeneous retained-recording witness through the native ForeignTwin observer.
import { readFileSync, writeFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { resolve, join, basename } from 'node:path';
import { tmpdir } from 'node:os';
import { gunzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { sampleAudio } from './audio_sample.mjs';
import { readObservationReceipt } from '../../BonfyreSourceFrontier/tools/foreign_observation.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const charts = [
  ['acoustic-fp', 'fingerprint', 32, 'haitsma-kalker-33band-v1', 'bonfyre-latent-atlas acoustic'],
  ['spectrum-24', 'distribution', 24, 'log24-v1', 'bonfyre-latent-atlas acoustic'],
  ['loudness-32', 'trajectory', 32, 'rms32-v1', 'bonfyre-latent-atlas acoustic'],
  ['words-256', 'dense', 256, 'trigram-hash-v1', 'bonfyre-latent-atlas encode-text'],
  ['roles-4096', 'hdc', 4096, 'roles-v1', 'bonfyre-latent-atlas encode-roles'],
  ['season-2', 'circular', 2, 'dayofyear-weekday-v1', 'witness'],
];
const groupCharts = ['words-256', 'spectrum-24', 'acoustic-fp'];

export const itemKey = (source, href, title) => sha(`${source}\n${href || title}`).slice(0, 24);

export function retainedBody(root, twin) {
  const receipt = readObservationReceipt(join(root, 'receipts', `${twin}.json`));
  if (!receipt.body_path) throw new Error(`missing retained receipt body: ${twin}`);
  let path = receipt.body_path;
  if (!existsSync(path)) path = join(root, 'bodies', basename(path));
  if (!existsSync(path) && existsSync(`${path}.gz`)) path += '.gz';
  if (!existsSync(path)) throw new Error(`retained body unavailable: ${twin}`);
  const bytes = readFileSync(path);
  return JSON.parse(path.endsWith('.gz') ? gunzipSync(bytes).toString('utf8') : bytes.toString('utf8'));
}

export function inventory(foreign, perSource, bodyFn = retainedBody) {
  const recordings = [];
  const bindings = bodyFn(foreign, 'wikidata-recorded-music').results.bindings;
  for (const row of bindings.slice(0, perSource * 2)) recordings.push({
    source: 'wikidata-recorded-music', title: row.title.value, maker: row.composer?.value || '',
    href: row.page?.value || '', asset: row.audio.value, group: 'group:recorded-works', date: '',
  });
  for (const [twin, group] of [['archive-great-78', 'group:78-rpm-records'], ['archive-dallas-live-music', 'group:dallas-live']]) {
    for (const row of bodyFn(foreign, twin).response.docs.slice(0, perSource * 2)) recordings.push({
      source: twin, title: row.title, maker: Array.isArray(row.creator) ? row.creator.slice(0, 3).join(', ') : (row.creator || ''),
      href: `https://archive.org/details/${row.identifier}`, group, date: row.date || '',
    });
  }
  return recordings;
}

function run(atlas, args, accepted = [0]) {
  const result = spawnSync(atlas, args, { encoding: 'utf8', timeout: 120_000 });
  if (result.error) throw result.error;
  if (!accepted.includes(result.status)) throw new Error(`${args.slice(0, 3).join(' ')} failed: ${(result.stderr || '').slice(0, 300)}`);
  const lines = result.stdout.split(/\r?\n/).filter(Boolean);
  return lines.length ? JSON.parse(lines.at(-1)) : {};
}

function season(date) {
  if (!/^\d{4}-\d{2}-\d{2}/.test(date) || date.slice(4, 10) === '-01-01') return null;
  const day = new Date(`${date.slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(day.valueOf()) || day.toISOString().slice(0, 10) !== date.slice(0, 10)) return null;
  const year = day.getUTCFullYear(), yday = Math.floor((day - Date.UTC(year, 0, 1)) / 86_400_000);
  return [yday / 365, ((day.getUTCDay() + 6) % 7) / 7];
}

export async function buildWitness(options, { bodyFn = retainedBody, sampleFn = sampleAudio, runFn = run, delayFn = Bun.sleep } = {}) {
  if (existsSync(options.db)) throw new Error(`atlas database already exists; choose a fresh generation: ${options.db}`);
  const recordings = inventory(options.foreign, options.perSource, bodyFn);
  const tmp = mkdtempSync(join(tmpdir(), 'bonfyre-audio-family-'));
  try {
    const lines = charts.map(([id, family, dims, version, encoder]) => ({ chart: { id, family, dims, version, encoder, universe: 'census-recordings' } }));
    const receipts = [], counts = {};
    for (const rec of recordings) {
      if ((counts[rec.group] || 0) >= options.perSource) continue;
      let result;
      try { result = await sampleFn({ href: rec.href, asset: rec.asset }, { project: options.project, foreign: options.foreign, atlas: options.atlas }); }
      catch (error) { receipts.push({ title: rec.title, refused: `unresolved: ${String(error.message).slice(0, 60)}` }); continue; }
      if (!result.acoustic) { receipts.push({ title: rec.title, url: result.url, refused: result.reason || 'no playable file' }); continue; }
      const address = `o/${rec.source}/${itemKey(rec.source, rec.href, rec.title)}`;
      rec.address = address; rec.url = result.url; rec.digest = result.sample_sha256;
      counts[rec.group] = (counts[rec.group] || 0) + 1;
      receipts.push({ address, title: rec.title, url: rec.url, range: [0, result.sample_bytes - 1], sha256: rec.digest, frames: result.acoustic.frames, robots: 'admitted', observation_receipt: result.receipt });
      const words = runFn(options.atlas, ['encode-text', '--text', `${rec.title} | ${rec.maker}`]);
      const era = /^\d{4}/.test(rec.date) ? `${rec.date.slice(0, 3)}0s` : 'unknown';
      const roles = runFn(options.atlas, ['encode-roles', '--roles', `creator=${rec.maker};collection=${rec.group};era=${era}`, '--dims', '4096']);
      const textDigest = sha(`${rec.title}|${rec.maker}`);
      lines.push(
        { project: { address, chart: 'acoustic-fp', bits: result.acoustic.fingerprint, source_digest: rec.digest } },
        { project: { address, chart: 'spectrum-24', values: result.acoustic.distribution, source_digest: rec.digest } },
        { project: { address, chart: 'loudness-32', values: result.acoustic.trajectory, source_digest: rec.digest } },
        { project: { address, chart: 'words-256', values: words, source_digest: textDigest } },
        { project: { address, chart: 'roles-4096', bits: roles, source_digest: textDigest } },
        { relate: { src: address, relation: 'member_of', dst: rec.group, evidence: `retained collection ${rec.source}` } },
        { label: { address, label: rec.title } },
      );
      const cycle = season(rec.date);
      if (cycle) lines.push({ project: { address, chart: 'season-2', values: cycle, source_digest: textDigest } });
      await delayFn(500);
    }
    if (!Object.keys(counts).length) throw new Error('no playable retained recording was observed; no atlas generation created');
    const input = join(tmp, 'import.jsonl');
    writeFileSync(input, lines.map(line => JSON.stringify(line)).join('\n') + '\n');
    const witness = { schema: 'bonfyre-latent-atlas-audio-family-witness.v1', sampled: receipts,
      import: runFn(options.atlas, ['import', '--db', options.db, '--jsonl', input]) };
    const groups = Object.keys(counts).sort(), relations = [];
    for (const rec of recordings) if (rec.address) for (const chart of groupCharts) relations.push({ relate: { src: rec.address, relation: 'member_of', dst: `${rec.group}@${chart}`, evidence: `retained collection ${rec.source}` } });
    const relationPath = join(tmp, 'groups.jsonl');
    writeFileSync(relationPath, relations.map(line => JSON.stringify(line)).join('\n') + '\n');
    runFn(options.atlas, ['import', '--db', options.db, '--jsonl', relationPath]);
    witness.groups = {};
    for (const group of groups) {
      witness.groups[group] = {};
      for (const chart of groupCharts) {
        const result = runFn(options.atlas, ['group', '--db', options.db, '--logical', `${group}@${chart}`, '--lens', 'collection', '--chart', chart, '--universe', 'census-recordings']);
        witness.groups[group][chart] = { snapshot: result.snapshot, members: result.members, member_type: result.member_type, representation_type: result.representation?.type };
      }
    }
    const children = groups.map(group => `${group}@words-256`).join(',');
    const parent = runFn(options.atlas, ['lift', '--db', options.db, '--logical', 'group:recorded-music@words-256', '--lens', 'collection', '--children', children, '--universe', 'census-recordings']);
    witness.hierarchy = { snapshot: parent.snapshot, type: parent.representation.type, topology: parent.representation.topology };
    const sound = runFn(options.atlas, ['lift', '--db', options.db, '--logical', 'group:recorded-music@spectrum-24', '--lens', 'sound', '--children', groups.map(group => `${group}@spectrum-24`).join(','), '--universe', 'census-recordings']);
    witness.sound_hierarchy = { snapshot: sound.snapshot, type: sound.representation.type, distances: sound.representation.distances, topology: sound.representation.topology };
    const first = recordings.find(rec => rec.address && rec.source === 'archive-great-78') || recordings.find(rec => rec.address);
    witness.cross_chart = runFn(options.atlas, ['compare', '--db', options.db, '--a', first.address, '--b', first.address, '--chart', 'acoustic-fp', '--to-chart', 'words-256'], [3]);
    const later = await sampleFn({ href: first.href, asset: first.asset }, { project: options.project, foreign: options.foreign, atlas: options.atlas, start: 600_000 });
    if (later.acoustic) {
      const digest = later.sample_sha256;
      witness.mutation = { address: first.address, from: first.digest, to: digest,
        'acoustic-fp': runFn(options.atlas, ['mutate', '--db', options.db, '--address', first.address, '--chart', 'acoustic-fp', '--bits', later.acoustic.fingerprint, '--source-digest', digest]),
        'spectrum-24': runFn(options.atlas, ['mutate', '--db', options.db, '--address', first.address, '--chart', 'spectrum-24', '--values', later.acoustic.distribution.map(value => Number(value).toFixed(6)).join(','), '--source-digest', digest]) };
      witness.reopen_before_refresh = Object.fromEntries(groups.map(group => [group, runFn(options.atlas, ['reopen', '--db', options.db, '--logical', `${group}@spectrum-24`])]));
      witness.refresh = runFn(options.atlas, ['refresh', '--db', options.db, '--universe', 'census-recordings', '--envelope', '0.02']);
      witness.reopen_after_refresh = Object.fromEntries(groups.flatMap(group => groupCharts.map(chart => [`${group}@${chart}`, runFn(options.atlas, ['reopen', '--db', options.db, '--logical', `${group}@${chart}`])])));
    }
    const lensPath = join(tmp, 'lens.json');
    writeFileSync(lensPath, JSON.stringify({ query: first.address, within: `${first.group}@acoustic-fp`, charts: ['acoustic-fp', 'spectrum-24', 'loudness-32', 'season-2'], fusion: 'rrf', slice: 5 }));
    witness.lens_sounds_like = runFn(options.atlas, ['lens', '--db', options.db, '--program', lensPath]);
    witness.neighbours_acoustic = runFn(options.atlas, ['neighbours', '--db', options.db, '--address', first.address, '--chart', 'spectrum-24', '--k', '3']);
    writeFileSync(options.out, JSON.stringify(witness, null, 2) + '\n');
    const drift = records => Object.values(records || {}).reduce((sum, row) => sum + Number(row.drifted || 0), 0);
    return { sampled: receipts.filter(row => row.sha256).length, refused: receipts.filter(row => row.refused).length,
      groups: Object.fromEntries(groups.map(group => [group, Object.keys(witness.groups[group]).length])), hierarchy: witness.hierarchy.type,
      cross_chart: witness.cross_chart.refused, changed_after_mutation: (witness.refresh?.groups || []).map(group => group.logical),
      parents_recomputed: witness.refresh?.parents_recomputed, parents_held: witness.refresh?.parents_held,
      reopen_drifted_before: drift(witness.reopen_before_refresh), reopen_drifted_after: drift(witness.reopen_after_refresh) };
  } finally { rmSync(tmp, { recursive: true, force: true }); }
}

export async function main(args = process.argv.slice(2)) {
  const options = { atlas: '', db: '', foreign: '', out: '', project: '', perSource: 8, apply: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--apply') options.apply = true;
    else if (args[i] === '--per-source' && args[i + 1]) options.perSource = Number(args[++i]);
    else if (['--atlas', '--db', '--foreign', '--out', '--project'].includes(args[i]) && args[i + 1]) options[args[i++].slice(2)] = args[i];
    else throw new Error(`unknown or incomplete argument: ${args[i]}`);
  }
  if (!options.foreign || !Number.isInteger(options.perSource) || options.perSource < 1) throw new Error('--foreign and positive --per-source are required');
  if (!options.apply) {
    const rows = inventory(options.foreign, options.perSource);
    console.log(JSON.stringify({ apply: false, candidates: rows.length, sources: Object.fromEntries([...new Set(rows.map(row => row.source))].map(source => [source, rows.filter(row => row.source === source).length])), writes: 0 }));
    return 0;
  }
  if (!options.project || !options.atlas || !options.db || !options.out) throw new Error('--apply requires --project, --atlas, --db, and --out');
  console.log(JSON.stringify(await buildWitness(options)));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
