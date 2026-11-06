#!/usr/bin/env bun
/* Compile supplied public occurrences into candidates, not admitted providers.
 * Classification vocabulary and source ranges live in CUE; this module owns
 * parsing, reversible source lineage, and deterministic projection only.
 */
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { resolve, join, basename, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const configPath = join(root, 'architecture/external/external-estate-classification.cue');
const defaultMaster = '/Users/nickgonzales/Downloads/AUREKAI_EXTERNAL_ESTATE_MASTER_INDEX_V4.md';
const defaultProtocols = '/Users/nickgonzales/Downloads/AUREKAI_MACHINE_PROTOCOLS_CONTINUATION_V3.md';
const defaultOut = join(root, 'estate/generated/external-estate-v4');
const entryId = /`(EXT-\d{5})`/;
const link = /\[([^\]]+)\]\(([^)]+)\)/;
const sourceLink = /\[source\]\(([^)]+)\)/;
const unique = (values) => [...new Set(values)];
const clean = (value) => value.replaceAll('**', '').trim().replace(/\s+/g, ' ');
const sha256 = (path) => createHash('sha256').update(readFileSync(path)).digest('hex');
const sortedCounts = (values) => Object.fromEntries([...values.entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0));

export function loadConfig(path = configPath) {
  const result = Bun.spawnSync(['cue', 'export', path]);
  if (result.exitCode) throw new Error(`CUE classification invalid: ${new TextDecoder().decode(result.stderr)}`);
  const config = JSON.parse(new TextDecoder().decode(result.stdout));
  const names = (rows) => rows.map((row) => row.name);
  for (const key of ['domain_terms', 'reuse', 'role_terms', 'affordance_terms', 'worlds']) {
    if (new Set(names(config[key])).size !== config[key].length) throw new Error(`duplicate ${key} name`);
  }
  if (new Set(config.source_defaults.map((row) => row.kind)).size !== config.source_defaults.length)
    throw new Error('duplicate source default kind');
  if (config.source_ranges.at(-1)?.max !== config.expected_counts.master ||
      config.source_ranges.some((row, index) => index && row.max <= config.source_ranges[index - 1].max))
    throw new Error('source ranges must ascend through the documented master count');
  for (const kind of [...config.source_ranges.map((row) => row.kind), 'machine_protocol']) {
    const row = config.source_defaults.find((defaultRow) => defaultRow.kind === kind);
    if (!row?.role || !row?.carrier) throw new Error(`source kind ${kind} lacks a role or carrier`);
  }
  return config;
}

function markdownNameUrl(line) {
  const boldLink = /\*\*\[([^\]]+)\]\(([^)]+)\)\*\*/.exec(line);
  if (boldLink) return [clean(boldLink[1]), boldLink[2]];
  const boldThenLink = /\*\*([^*]+)\*\*\s+—\s+\[([^\]]+)\]\(([^)]+)\)/.exec(line);
  if (boldThenLink) return [clean(boldThenLink[1]), boldThenLink[3]];
  const plain = link.exec(line);
  if (plain) return [clean(plain[1]), plain[2]];
  throw new Error(`cannot parse source occurrence: ${line}`);
}

function sourceKind(sourceRef, config) {
  const number = Number(sourceRef.slice(4));
  const row = config.source_ranges.find(({ max }) => number <= max);
  if (!row) throw new Error(`unclassified source ref ${sourceRef}`);
  return row.kind;
}

export function parseMaster(master, config) {
  const records = new Map();
  let active = false, section = '';
  const lines = readFileSync(master, 'utf8').split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i], source = entryId.exec(raw)?.[1], line = i + 1;
    if (!active && source && raw.startsWith('- ') && Number(source.slice(4)) <= config.source_ranges[0].max) {
      const [name, url] = markdownNameUrl(raw);
      if (!records.has(source)) records.set(source, { id: `external:${source.toLowerCase()}`, source_ref: source,
        observed_name: name, primary_url: url, source_kind: sourceKind(source, config),
        section: 'x402 foundation, protocol and specification resources',
        source_document: basename(master), source_line: line, raw_source_text: raw });
    }
    if (raw.startsWith('## 3. Lossless source-organized master list')) { active = true; continue; }
    if (active && raw.startsWith('# Machine Protocols & Standards Continuation')) break;
    if (!active) continue;
    if (raw.startsWith('## ')) section = clean(raw.slice(3));
    else if (raw.startsWith('### ')) section = clean(raw.slice(4));
    if (!source || !raw.startsWith('- ') || Number(source.slice(4)) > config.expected_counts.master) continue;
    const [name, url] = markdownNameUrl(raw);
    if (!records.has(source)) records.set(source, { id: `external:${source.toLowerCase()}`, source_ref: source,
      observed_name: name, primary_url: url, source_kind: sourceKind(source, config), section,
      source_document: basename(master), source_line: line, raw_source_text: raw });
  }
  if (records.size !== config.expected_counts.master)
    throw new Error(`expected ${config.expected_counts.master} non-protocol source occurrences, found ${records.size}`);
  return [...records.entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([, row]) => row);
}

export function parseProtocols(protocols, config) {
  const records = [];
  let section = '';
  const lines = readFileSync(protocols, 'utf8').split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i], line = i + 1;
    if (raw.startsWith('## ')) { section = clean(raw.slice(3)); continue; }
    let name, url;
    if (raw.startsWith('| **') && raw.includes('[source](')) {
      const cells = raw.trim().replace(/^\|/, '').replace(/\|$/, '').split('|').map((cell) => cell.trim());
      if (cells.length < 6) continue;
      name = clean(cells[0]);
      url = sourceLink.exec(cells.at(-1))?.[1];
      if (!url) throw new Error(`missing protocol source at line ${line}`);
    } else if (raw.startsWith('- **[') && entryId.test(raw)) [name, url] = markdownNameUrl(raw);
    else continue;
    const printed = entryId.exec(raw)?.[1];
    const occurrence = records.length + 1;
    const source = printed ?? `EXT-${String(config.expected_counts.master + occurrence).padStart(5, '0')}`;
    records.push({ id: `external:protocol-${String(occurrence).padStart(4, '0')}`, source_ref: source,
      source_ref_assignment: printed ? 'printed' : 'documented_continuation_range',
      protocol_occurrence: occurrence, observed_name: name, primary_url: url,
      source_kind: 'machine_protocol', section, source_document: basename(protocols),
      source_line: line, raw_source_text: raw });
  }
  if (records.length !== config.expected_counts.protocols)
    throw new Error(`expected ${config.expected_counts.protocols} protocol occurrences, found ${records.length}`);
  return records;
}

const acronym = new Map();
function termMatchesLower(text, term) {
  const lower = term.toLowerCase();
  if (lower.length > 3 || !/^[a-z0-9]+$/.test(lower)) return text.includes(lower);
  let matcher = acronym.get(lower);
  if (!matcher) { matcher = new RegExp(`(?<![a-z0-9])${lower}(?![a-z0-9])`); acronym.set(lower, matcher); }
  return matcher.test(text);
}
export const termMatches = (text, term) => termMatchesLower(text.toLowerCase(), term);
const anyTerm = (text, row) => row.terms.some((term) => termMatchesLower(text, term));

export function project(record, config) {
  const text = [record.observed_name, record.section, record.raw_source_text].join(' ').toLowerCase();
  const topic = config.domain_terms.filter((row) => anyTerm(text, row)).map((row) => row.name);
  const defaults = config.source_defaults.find((row) => row.kind === record.source_kind);
  if (!defaults) throw new Error(`no default for ${record.source_kind}`);
  const reuse = unique(topic.flatMap((domain) => config.reuse.find((row) => row.name === domain)?.terms ?? []));
  const roles = config.role_terms.filter((row) => anyTerm(text, row)).map((row) => row.name);
  roles.push(defaults.role);
  const affordances = config.affordance_terms.filter((row) => anyTerm(text, row)).map((row) => row.name);
  const worlds = config.worlds.filter((row) => row.terms.filter((domain) => topic.includes(domain)).length >= 2).map((row) => row.name);
  return { classification_level: config.semantics.classification_level, estate_roles: unique(roles),
    topic_domains: topic, carrier_default_domains: defaults.domains, direct_domains: topic,
    conditional_reuse_domains: reuse, candidate_domains: unique([...topic, ...reuse, ...defaults.domains]),
    candidate_affordances: unique(affordances.length ? affordances : defaults.affordances),
    candidate_worlds: worlds, carrier_candidate: defaults.carrier,
    temporal_candidate: config.semantics.temporal_candidate,
    spatial_candidate: config.spatial_terms.some((term) => termMatchesLower(text, term)) ? 'bearing' : 'not_evident',
    world_phases: config.semantics.world_phases, interaction_fits: config.semantics.interaction_fits };
}

export function compileCorpus(master, protocols, config = loadConfig()) {
  const records = [...parseMaster(master, config), ...parseProtocols(protocols, config)];
  const kindCounts = new Map(), domainCounts = new Map(), worldCounts = new Map();
  let atLeast2 = 0, atLeast10 = 0, maximum = 0;
  for (const record of records) {
    record.truth_level = config.semantics.truth_level;
    record.projection = project(record, config);
    record.forbidden_inferences = config.semantics.forbidden_inferences;
    kindCounts.set(record.source_kind, (kindCounts.get(record.source_kind) ?? 0) + 1);
    for (const domain of record.projection.candidate_domains) domainCounts.set(domain, (domainCounts.get(domain) ?? 0) + 1);
    for (const world of record.projection.candidate_worlds) worldCounts.set(world, (worldCounts.get(world) ?? 0) + 1);
    const width = record.projection.candidate_worlds.length;
    if (width >= 2) atLeast2++;
    if (width >= 10) atLeast10++;
    maximum = Math.max(maximum, width);
  }
  const summary = { schema: 'bonfyre.external-estate.v1', truth_level: config.semantics.truth_level,
    input: { master, master_sha256: sha256(master), protocols, protocols_sha256: sha256(protocols) },
    counts: { external_occurrences: records.length,
      non_protocol_occurrences: records.length - config.expected_counts.protocols,
      protocol_occurrences: config.expected_counts.protocols,
      by_source_kind: sortedCounts(kindCounts), candidate_domains: domainCounts.size,
      candidate_worlds: worldCounts.size, sources_in_2_or_more_worlds: atLeast2,
      sources_in_10_or_more_worlds: atLeast10, maximum_world_reuse: maximum },
    candidate_domain_occurrences: sortedCounts(domainCounts), candidate_world_occurrences: sortedCounts(worldCounts),
    limits: config.semantics.limits };
  return { records, summary };
}

function sortedDeep(value) {
  if (Array.isArray(value)) return value.map(sortedDeep);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value)
    .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, child]) => [key, sortedDeep(child)]));
  return value;
}
function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(', ')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
    .map(([key, child]) => `${JSON.stringify(key)}: ${stableJson(child)}`).join(', ')}}`;
  return JSON.stringify(value);
}

export function run(args = process.argv.slice(2)) {
  const options = { master: defaultMaster, protocols: defaultProtocols, out: defaultOut, check: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--check') options.check = true;
    else if (['--master', '--protocols', '--out'].includes(args[i])) {
      if (!args[i + 1]) throw new Error(`missing value for ${args[i]}`);
      options[args[i].slice(2)] = args[++i];
    } else throw new Error(`unknown option ${args[i]}`);
  }
  const master = resolve(options.master), protocols = resolve(options.protocols), out = resolve(options.out);
  const { records, summary } = compileCorpus(master, protocols);
  const corpus = join(out, 'observed-external-estate.jsonl'), manifest = join(out, 'manifest.json');
  if (options.check) {
    if (!existsSync(corpus) || !existsSync(manifest)) throw new Error('generated corpus or manifest is absent');
    const observed = readFileSync(corpus, 'utf8').split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
    if (!isDeepStrictEqual(observed, records) || !isDeepStrictEqual(JSON.parse(readFileSync(manifest, 'utf8')), summary))
      throw new Error('generated external estate is stale; rerun compile-external-estate.mjs');
    return 0;
  }
  mkdirSync(out, { recursive: true });
  writeFileSync(corpus, `${records.map(stableJson).join('\n')}\n`);
  writeFileSync(manifest, `${JSON.stringify(sortedDeep(summary), null, 2)}\n`);
  console.log(`wrote ${records.length} observed candidate occurrences to ${out}`);
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = run(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
