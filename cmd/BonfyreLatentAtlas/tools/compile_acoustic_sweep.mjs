#!/usr/bin/env bun
// Fairly sample the retained audio estate into native acoustic charts.
import { Database } from "bun:sqlite";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { sampleAudio } from "./audio_sample.mjs";

const charts = [
  { chart: { id: "acoustic-fp", family: "fingerprint", dims: 32, version: "haitsma-kalker-33band-v1", encoder: "bonfyre-latent-atlas acoustic", universe: "census-recordings" } },
  { chart: { id: "spectrum-24", family: "distribution", dims: 24, version: "log24-v1", encoder: "bonfyre-latent-atlas acoustic", universe: "census-recordings" } },
  { chart: { id: "loudness-32", family: "trajectory", dims: 32, version: "rms32-v1", encoder: "bonfyre-latent-atlas acoustic", universe: "census-recordings" } },
];

export function selectWanted(items, known) {
  const bySource = new Map();
  for (const item of items) {
    if (item.kind !== "audio" || !item.source || !item.key) continue;
    const address = `o/${item.source}/${item.key}`;
    if (known.has(address)) continue;
    const rows = bySource.get(item.source) ?? []; rows.push([address, item]); bySource.set(item.source, rows);
  }
  const wanted = [];
  const sources = [...bySource.keys()].sort();
  for (let round = 0; ; round++) {
    let added = 0;
    for (const source of sources) if (round < bySource.get(source).length) { wanted.push(bySource.get(source)[round]); added++; }
    if (!added) break;
  }
  return { wanted, waiting_by_source: Object.fromEntries(sources.map(source => [source, bySource.get(source).length])) };
}

function options(argv) {
  const args = { perRun: 8 };
  const keys = { "--items": "items", "--atlas-db": "atlasDb", "--atlas-bin": "atlasBin", "--project": "project",
    "--foreign": "foreign", "--work": "work", "--per-run": "perRun" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["items", "atlasDb", "atlasBin", "project", "foreign", "work"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.perRun = Number(args.perRun);
  if (!Number.isInteger(args.perRun) || args.perRun < 0) throw new Error("invalid --per-run");
  return args;
}
function native(argv) {
  const result = Bun.spawnSync(argv, { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(`${argv[0]} exited ${result.exitCode}: ${new TextDecoder().decode(result.stderr).slice(0, 300)}`);
  const lines = new TextDecoder().decode(result.stdout).trim().split(/\r?\n/);
  try { return JSON.parse(lines.at(-1)); } catch { return null; }
}

export async function compile(args, { sampleFn = sampleAudio, runFn = native, sleepFn = Bun.sleep } = {}) {
  const db = new Database(args.atlasDb, { readonly: true });
  let known;
  try { known = new Set(db.query("SELECT address FROM projection WHERE chart_id='spectrum-24'").all().map(row => row.address)); }
  finally { db.close(); }
  const items = readFileSync(args.items, "utf8").split(/\r?\n/).filter(Boolean).map(JSON.parse);
  const selection = selectWanted(items, known), lines = [], families = new Set();
  let sampled = 0, refused = 0;
  for (const [address, item] of selection.wanted.slice(0, args.perRun)) {
    let sample;
    try { sample = await sampleFn(item, { project: args.project, foreign: args.foreign, atlas: args.atlasBin }); }
    catch (error) { sample = { acoustic: null, reason: String(error).slice(0, 80) }; }
    if (!sample.acoustic) { refused++; continue; }
    const family = item.family || "unclassified-audio";
    families.add(family);
    lines.push(
      { project: { address, chart: "acoustic-fp", bits: sample.acoustic.fingerprint, source_digest: sample.sample_sha256 } },
      { project: { address, chart: "spectrum-24", values: sample.acoustic.distribution, source_digest: sample.sample_sha256 } },
      { project: { address, chart: "loudness-32", values: sample.acoustic.trajectory, source_digest: sample.sample_sha256 } },
      { relate: { src: address, relation: "member_of", dst: `family:${family}@spectrum-24`, evidence: "census family" } },
      { relate: { src: address, relation: "member_of", dst: `family:${family}@acoustic-fp`, evidence: "census family" } },
    );
    sampled++;
    await sleepFn(500);
  }
  if (lines.length) {
    const chartPath = join(args.work, "acoustic-charts.jsonl");
    writeFileSync(chartPath, charts.map(row => JSON.stringify(row)).join("\n") + "\n");
    runFn([args.atlasBin, "import", "--db", args.atlasDb, "--jsonl", chartPath]);
    const dataPath = join(args.work, "acoustic.jsonl");
    writeFileSync(dataPath, lines.map(row => JSON.stringify(row)).join("\n") + "\n");
    runFn([args.atlasBin, "import", "--db", args.atlasDb, "--jsonl", dataPath]);
    for (const family of [...families].sort()) for (const chart of ["spectrum-24", "acoustic-fp"])
      runFn([args.atlasBin, "group", "--db", args.atlasDb, "--logical", `family:${family}@${chart}`,
        "--lens", "sound", "--chart", chart, "--universe", "census-recordings"]);
  }
  const refreshed = runFn([args.atlasBin, "refresh", "--db", args.atlasDb, "--universe", "census-recordings", "--envelope", "0.02"]);
  return { waiting: selection.wanted.length, waiting_by_source: selection.waiting_by_source,
    sources_in_round_robin: Object.keys(selection.waiting_by_source).length, sampled, refused,
    refresh: refreshed && typeof refreshed === "object" ? Object.fromEntries(["unchanged", "parents_recomputed", "parents_held"]
      .map(key => [key, refreshed[key] ?? null])) : refreshed };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await compile(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
