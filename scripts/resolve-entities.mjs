#!/usr/bin/env bun
// Candidate identity observations from retained titles, never aboutness claims.
import { existsSync, writeFileSync, renameSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const exported = Bun.spawnSync(["cue", "export", join(here, "entity-resolution.cue"), "-e", "entityResolution", "--out", "json"]);
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
export const policy = JSON.parse(exported.stdout.toString());
const common = new Set(policy.ordinary_words.split(/\s+/).filter(Boolean));
const quoted = value => `'${String(value).replaceAll("'", "''")}'`;
const binding = (row, name) => row?.[name]?.value ?? "";
const saneInt = (value, name, minimum) => {
  const number = Number(value);
  if (!Number.isInteger(number) || number < minimum) throw new Error(`${name} must be an integer >= ${minimum}`);
  return number;
};

export function ordinary(name) {
  const words = String(name).toLowerCase().replaceAll("-", " ").split(/\s+/).filter(Boolean);
  return words.length > 0 && words.every(word => common.has(word));
}

export function instanceLike(kind) {
  const value = String(kind || "").toLowerCase();
  if (value.startsWith("type of") || value.includes("concept") || value.includes("widget")) return false;
  return policy.instance_kinds.some(part => value.includes(part));
}

export function point(literal) {
  const match = /^POINT\(\s*([^\s)]+)\s+([^\s)]+)\s*\)$/i.exec(String(literal || ""));
  if (!match) return [null, null];
  const lon = Number(match[1]), lat = Number(match[2]);
  return Number.isFinite(lon) && Number.isFinite(lat) ? [lon, lat] : [null, null];
}

export function best(rows, wanted = []) {
  const names = new Map(wanted.map(name => [name.toLowerCase(), name]));
  const held = new Map();
  for (const row of rows) {
    const item = binding(row, "item"), qid = /\/entity\/(Q\d+)$/.exec(item)?.[1];
    if (!qid) continue;
    const stated = binding(row, "name"), name = names.get(stated.toLowerCase()) ?? stated;
    const links = Number(binding(row, "n") || 0);
    if (!Number.isInteger(links) || links < 0 || (held.has(name) && held.get(name).links >= links)) continue;
    const [lon, lat] = point(binding(row, "coord"));
    held.set(name, { name, qid, about: binding(row, "d"), kind: binding(row, "kindLabel"), links, lon, lat });
  }
  return held;
}

const candidatesSql = (catalog, minPublishers, limit) => `
WITH phrase AS (
  SELECT publisher, unnest(regexp_extract_all(title, '\\b[A-Z][a-z]{2,}(?: [A-Z][a-z]{2,}){1,3}\\b')) AS name, 2 AS words
  FROM read_parquet(${quoted(join(catalog, "material.parquet"))}) WHERE title <> ''
  UNION ALL
  SELECT publisher, unnest(regexp_extract_all(title, '\\b[A-Z][a-z]{3,}\\b')) AS name, 1 AS words
  FROM read_parquet(${quoted(join(catalog, "material.parquet"))}) WHERE title <> ''
)
SELECT name, mentions, publishers FROM (
  SELECT name, words, COUNT(*) AS mentions, COUNT(DISTINCT publisher) AS publishers,
    row_number() OVER (PARTITION BY words ORDER BY COUNT(DISTINCT publisher) DESC, COUNT(*) DESC) AS rank
  FROM phrase WHERE NOT regexp_matches(name, '^(January|February|March|April|May|June|July|August|September|October|November|December) ')
  GROUP BY name, words HAVING COUNT(DISTINCT publisher) >= (CASE WHEN words = 1 THEN ${minPublishers * 4} ELSE ${minPublishers} END)
) WHERE rank <= ${Math.floor(limit / 2)} ORDER BY publishers DESC, mentions DESC`;

export function candidates(catalog, minPublishers, limit) {
  const sql = `SELECT COALESCE(json_group_array(row_to_json(candidate)), '[]') AS payload FROM (${candidatesSql(catalog, minPublishers, limit)}) AS candidate`;
  const result = Bun.spawnSync(["duckdb", "-json", "-c", sql]);
  if (result.exitCode !== 0) throw new Error(result.stderr.toString().slice(0, 400));
  const output = result.stdout.toString();
  try {
    const payload = JSON.parse(output)[0].payload;
    return Array.isArray(payload) ? payload : JSON.parse(payload);
  }
  catch { throw new Error(`DuckDB did not return a complete candidate JSON array (${output.length} bytes): ${JSON.stringify(output.slice(-400))}; stderr=${result.stderr.toString().slice(-200)}`); }
}

const sparql = names => {
  const forms = new Set();
  for (const name of names) for (const variant of [name.replaceAll('"', ""), name.replaceAll('"', "").toLowerCase()])
    forms.add(`${JSON.stringify(variant)}@en`);
  return `PREFIX wdt: <http://www.wikidata.org/prop/direct/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX schema: <http://schema.org/> PREFIX wikibase: <http://wikiba.se/ontology#>
SELECT ?name ?item ?d ?n ?coord ?kindLabel WHERE { VALUES ?name { ${[...forms].join(" ")} } ?item rdfs:label ?name ; wikibase:sitelinks ?n . OPTIONAL { ?item schema:description ?d . FILTER(LANG(?d) = "en") } OPTIONAL { ?item wdt:P625 ?coord } OPTIONAL { ?item wdt:P31 ?kind . ?kind rdfs:label ?kindLabel . FILTER(LANG(?kindLabel) = "en") } } ORDER BY DESC(?n)`;
};

async function ask(query, fetcher, timeoutMs) {
  const url = `${policy.endpoint}?${new URLSearchParams({ query })}`;
  const response = await fetcher(url, { headers: { Accept: "application/sparql-results+json", "User-Agent": policy.user_agent }, signal: AbortSignal.timeout(timeoutMs) });
  if (!response.ok) throw new Error(`QLever ${response.status}`);
  return response.json();
}

export async function resolveEntities({ catalog = join(homedir(), ".bonfyre/estate-local/catalog"), limit = policy.default_limit,
  minPublishers = policy.default_min_publishers, batch = policy.default_batch, floor = policy.default_sitelink_floor,
  candidateRows = null, fetcher = fetch, pause = ms => Bun.sleep(ms) } = {}) {
  limit = saneInt(limit, "limit", 1);
  minPublishers = saneInt(minPublishers, "min-publishers", 1);
  batch = saneInt(batch, "batch", 1);
  floor = saneInt(floor, "floor", 1);
  if (!existsSync(catalog) || !candidateRows && !existsSync(join(catalog, "material.parquet"))) throw new Error(`catalog material missing: ${catalog}`);
  const rows = candidateRows ?? candidates(catalog, minPublishers, limit);
  const names = rows.map(row => row.name), counts = new Map(rows.map(row => [row.name, [Number(row.mentions), Number(row.publishers)]]));
  const resolved = new Map(), refused = [];
  let asked = 0;
  for (let at = 0; at < names.length; at += batch) {
    const chosen = names.slice(at, at + batch);
    try {
      const answer = await ask(sparql(chosen), fetcher, 60000);
      asked += chosen.length;
      for (const [name, entity] of best(answer?.results?.bindings ?? [], chosen)) resolved.set(name, entity);
    } catch (error) { refused.push({ batch: at, why: String(error.message).slice(0, 120) }); }
    await pause(policy.pause_ms);
  }
  const ids = [...resolved.values()].filter(entity => entity.links >= floor).map(entity => entity.qid);
  const placed = new Map();
  for (let at = 0; at < ids.length; at += batch) {
    const chosen = ids.slice(at, at + batch);
    const query = `PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wdt: <http://www.wikidata.org/prop/direct/> SELECT ?item ?coord WHERE { VALUES ?item { ${chosen.map(qid => `wd:${qid}`).join(" ")} } ?item wdt:P625 ?coord }`;
    try {
      const answer = await ask(query, fetcher, 60000);
      for (const row of answer?.results?.bindings ?? []) {
        const qid = /\/entity\/(Q\d+)$/.exec(binding(row, "item"))?.[1];
        const [lon, lat] = point(binding(row, "coord"));
        if (qid && lon !== null) placed.set(qid, [lon, lat]);
      }
    } catch (error) { refused.push({ coords_batch: at, why: String(error.message).slice(0, 120) }); }
    await pause(policy.pause_ms);
  }
  let refusedOrdinary = 0, refusedClass = 0, refusedThin = 0;
  const kept = [];
  for (const [name, entity] of resolved) {
    if (entity.links < floor) continue;
    if (ordinary(name)) { refusedOrdinary++; continue; }
    if (!name.trim().includes(" ")) {
      if (!instanceLike(entity.kind)) { refusedClass++; continue; }
      if (entity.links < policy.single_word_sitelink_floor) { refusedThin++; continue; }
    }
    if (placed.has(entity.qid)) [entity.lon, entity.lat] = placed.get(entity.qid);
    const [mentions, publishers] = counts.get(name) ?? [0, 0];
    kept.push({ ...entity, mentions, publishers });
  }
  kept.sort((a, b) => b.publishers - a.publishers || b.links - a.links || a.name.localeCompare(b.name));
  const out = join(catalog, "entity.jsonl");
  const summary = { asked, resolved: resolved.size, kept: kept.length, refused_as_ordinary_english: refusedOrdinary,
    refused_single_word_class: refusedClass, refused_single_word_thinly_linked: refusedThin,
    placed: kept.filter(entity => entity.lon !== null).length, refused_batches: refused,
    cannot_infer: policy.cannot_infer };
  if (refused.length) return { ...summary, state: "incomplete", prior_cut_preserved: existsSync(out), wrote: null };
  const temporary = `${out}.partial-${process.pid}-${crypto.randomUUID()}`;
  try { writeFileSync(temporary, kept.map(entity => JSON.stringify(entity)).join("\n") + (kept.length ? "\n" : ""), { flag: "wx" }); renameSync(temporary, out); }
  finally { if (existsSync(temporary)) rmSync(temporary); }
  return { ...summary, state: "complete", wrote: out };
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2), args = {};
    while (argv.length) {
      const key = argv.shift(), value = argv.shift();
      if (!value || !["--catalog", "--limit", "--min-publishers", "--batch", "--floor"].includes(key)) throw new Error(`unknown or incomplete argument: ${key}`);
      args[key === "--min-publishers" ? "minPublishers" : key.slice(2)] = value;
    }
    const result = await resolveEntities(args);
    console.log(JSON.stringify(result, null, 1));
    if (result.state === "incomplete") process.exitCode = 2;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
