#!/usr/bin/env bun
// The trial measures candidate preservation; it does not extrapolate an envelope.
import { statSync } from "node:fs";
import { basename, join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { encode, load, topK } from "./admitted-frontier.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const result = Bun.spawnSync(["cue", "export", join(here, "representation-trial.cue"), "-e", "representationTrial", "--out", "json"]);
if (result.exitCode !== 0) throw new Error(result.stderr.toString());
export const policy = JSON.parse(result.stdout.toString());

const rounded = (value, places) => Math.round(value * 10 ** places) / 10 ** places;
const bytesFor = (name, dims) => {
  switch (name) {
    case "exact-f32": return 4 * dims;
    case "int8": return dims;
    case "sign-1bit": return Math.ceil(dims / 8);
    case "halved-f32": return 4 * Math.floor(dims / 2);
    default: throw new Error(`trial has no encoder for ${name}`);
  }
};

export function trial(corpus, name, note, k) {
  const ids = corpus.map(row => row[0]);
  const vectors = corpus.map(row => row[1]);
  const start = performance.now();
  const candidate = encode(vectors, name);
  const exact = encode(vectors, "exact-f32");
  let falsePrunes = 0, falseKeeps = 0, overlap = 0;
  for (let qi = 0; qi < ids.length; qi++) {
    const want = new Set(topK(ids, exact, qi, k));
    const got = new Set(topK(ids, candidate, qi, k));
    for (const id of want) if (got.has(id)) overlap++; else falsePrunes++;
    for (const id of got) if (!want.has(id)) falseKeeps++;
  }
  const perBody = bytesFor(name, vectors[0].length);
  return { representation: name, note, queries: ids.length, k,
    bytes_per_body: perBody, compression_vs_exact: rounded(4 * vectors[0].length / perBody, 2),
    false_prunes: falsePrunes, false_keeps: falseKeeps,
    recall_at_k: rounded(overlap / (ids.length * k), 4),
    seconds: rounded((performance.now() - start) / 1000, 4), rung: "preservation_measured" };
}

export function runTrial(corpus, cohort, k = 5) {
  if (corpus.length < k + 2) throw new Error(`corpus of ${corpus.length} is too small to measure recall at k=${k}`);
  const trials = policy.representations.map(rep => trial(corpus, rep.name, rep.note, k));
  const exact = trials.find(row => row.representation === "exact-f32");
  if (exact.false_prunes || exact.recall_at_k !== 1) throw new Error("the exact reference did not reproduce its own ranking; the harness is wrong before any representation is judged");
  return { schema: policy.schema, cohort, bodies: corpus.length, dimensions: corpus[0][1].length, k,
    ran_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    trials: trials.sort((a, b) => b.recall_at_k - a.recall_at_k || a.bytes_per_body - b.bytes_per_body),
    cannot_infer: ["preservation on this corpus -> preservation on another",
      "an envelope measured on hashed-token embeddings -> the same envelope on a learned model, whose geometry differs",
      "a preserved candidate -> a correct answer", "recall at k -> recall at any other k"] };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const directory = process.argv[2];
    if (!directory || !statSync(directory).isDirectory()) throw new Error("directory required");
    const argv = process.argv.slice(3);
    const k = argv.includes("--k") ? Number(argv[argv.indexOf("--k") + 1]) : 5;
    if (!Number.isInteger(k) || k <= 0) throw new Error("invalid k");
    const receipt = runTrial(load(directory), basename(directory.replace(/\/$/, "")), k);
    if (argv.includes("--json")) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  cohort ${receipt.cohort}: ${receipt.bodies} bodies, ${receipt.dimensions} dims, k=${k}\n`);
      console.log(`  ${"representation".padEnd(16)}${"bytes".padStart(7)}${"ratio".padStart(8)}${"recall@k".padStart(10)}${"false prunes".padStart(14)}${"false keeps".padStart(13)}`);
      for (const t of receipt.trials) console.log(`  ${t.representation.padEnd(16)}${String(t.bytes_per_body).padStart(7)}${String(t.compression_vs_exact).padStart(7)}x${t.recall_at_k.toFixed(3).padStart(10)}${String(t.false_prunes).padStart(14)}${String(t.false_keeps).padStart(13)}`);
      console.log("\n  a false prune loses a candidate every later stage will never see;\n  a false keep costs one exact check. They are not netted.");
    }
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
