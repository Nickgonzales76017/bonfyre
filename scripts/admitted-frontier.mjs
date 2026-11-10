#!/usr/bin/env bun
// Execute a representation admitted by the measured-envelope gate and report realised loss.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { decide } from "./resource-admission.mjs";

const unit = vector => {
  const norm = Math.sqrt(vector.reduce((n, x) => n + x * x, 0)) || 1;
  return vector.map(x => x / norm);
};

export function load(directory) {
  const corpus = [];
  for (const name of readdirSync(directory).sort()) {
    if (!name.endsWith(".json") || name.endsWith(".json.json")) continue;
    try {
      const doc = JSON.parse(readFileSync(join(directory, name), "utf8"));
      const value = doc.vector?.length ? doc.vector : doc.embedding;
      if (Array.isArray(value) && value.length) corpus.push([name.slice(0, -5), unit(value.map(Number))]);
    } catch { /* malformed candidates are not vectors */ }
  }
  return corpus;
}

const dot = (a, b) => a.reduce((n, x, i) => n + x * b[i], 0);
const roundEven = n => {
  const low = Math.floor(n);
  const fraction = n - low;
  return fraction < 0.5 ? low : fraction > 0.5 ? low + 1 : low % 2 === 0 ? low : low + 1;
};

export function encode(vectors, representation) {
  if (representation === "exact-f32") return { vectors, score: dot };
  if (representation === "int8") return { vectors: vectors.map(v => {
    let peak = 0;
    for (const x of v) peak = Math.max(peak, Math.abs(x));
    peak ||= 1;
    return [v.map(x => Math.max(-127, Math.min(127, roundEven(x / peak * 127)))), peak / 127];
  }), score: (a, b) => dot(a[0], b[0]) * a[1] * b[1] };
  if (representation === "sign-1bit") return { vectors: vectors.map(v => v.map(x => x >= 0 ? 1 : -1)), score: dot };
  if (representation === "halved-f32") return { vectors: vectors.map(v => v.slice(0, Math.floor(v.length / 2))), score: dot };
  throw new Error(`no encoder for admitted representation '${representation}'; admission named something this runner cannot execute`);
}

// Bounded stable selection costs O(n*k) and O(k) scratch rather than sorting
// every full candidate set. Equal scores retain input order, as the old trial did.
export function topK(ids, representation, qi, k) {
  const best = [];
  for (let j = 0; j < ids.length; j++) {
    if (j === qi) continue;
    const score = representation.score(representation.vectors[qi], representation.vectors[j]);
    let at = best.length;
    while (at > 0 && score > best[at - 1].score) at--;
    if (at >= k) continue;
    best.splice(at, 0, { id: ids[j], score });
    if (best.length > k) best.pop();
  }
  return best.map(row => row.id);
}

export function runFrontier(corpus, decision, { workClass = "discovery", k = 5, budget = 0.95 } = {}) {
  if (corpus.length < k + 2) throw new Error(`corpus of ${corpus.length} is too small for k=${k}`);
  const ids = corpus.map(row => row[0]);
  const vectors = corpus.map(row => row[1]);
  const admitted = encode(vectors, decision.admitted);
  const exact = encode(vectors, "exact-f32");
  let prunes = 0, keeps = 0, hits = 0, frontierSize = 0;
  for (let qi = 0; qi < ids.length; qi++) {
    const want = new Set(topK(ids, exact, qi, k));
    const got = new Set(topK(ids, admitted, qi, k));
    frontierSize += got.size;
    for (const id of want) if (got.has(id)) hits++; else prunes++;
    for (const id of got) if (!want.has(id)) keeps++;
  }
  const realised = Math.round(hits / (ids.length * k) * 10000) / 10000;
  const predicted = decision.measured_recall ?? null;
  const places = predicted == null ? 0 : String(predicted).split(".")[1]?.length ?? 0;
  const stated = n => Number.isInteger(n) ? `${n}.0` : String(n);
  const note = predicted == null ? "no envelope was predicted; the work ran exact"
    : Math.abs(realised - predicted) <= 0.5 * 10 ** -places ? "the run matched what the envelope promised"
    : `the run realised ${stated(realised)} where the envelope promised ${stated(predicted)}; an envelope is a fact about the cohort it was measured over, and this is a different cohort`;
  return { schema: "bonfyre-admitted-frontier.v1", ran_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    work_class: workClass, k, budget_min_recall: budget, admitted_representation: decision.admitted,
    admitted_because: decision.because, bodies: ids.length, frontier_size: frontierSize,
    predicted_recall: predicted, realised_recall: realised, realised_false_prunes: prunes,
    no_loss_observed_on_this_cohort: prunes === 0 && keeps === 0,
    realised_false_keeps: keeps, budget_met_on_this_cohort: realised >= budget,
    prediction_note: note, cannot_infer: ["a frontier -> a decision", "a realised loss on one cohort -> a new envelope", "budget met here -> budget met on another cohort"] };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const argv = process.argv.slice(2);
    const directory = argv[0];
    if (!directory) throw new Error("usage: admitted-frontier.mjs <embeddings-dir> [--class C] [--k N] [--min-recall R] [--json]");
    try { if (!statSync(directory).isDirectory()) throw new Error("not a directory"); }
    catch { throw new Error("usage: admitted-frontier.mjs <embeddings-dir> [--class C] [--k N] [--min-recall R] [--json]"); }
    const arg = (key, fallback) => argv.includes(key) ? argv[argv.indexOf(key) + 1] : fallback;
    const workClass = arg("--class", "discovery"), k = Number(arg("--k", 5)), budget = Number(arg("--min-recall", 0.95));
    if (!Number.isInteger(k) || k <= 0 || !Number.isFinite(budget)) throw new Error("invalid k or min-recall");
    const decision = decide({ workClass, k, minRecall: budget,
      requireNoLoss: argv.includes("--require-no-loss") });
    const receipt = runFrontier(load(directory), decision, { workClass, k, budget });
    if (argv.includes("--json")) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  work=${workClass} k=${k} budget=recall>=${budget}`);
      console.log(`  admitted: ${decision.admitted}  (${decision.because.slice(0, 60)})`);
      console.log(`  ran over ${receipt.bodies} bodies, frontier of ${receipt.frontier_size} candidates`);
      console.log(`  predicted ${receipt.predicted_recall}   realised ${receipt.realised_recall}   prunes ${receipt.realised_false_prunes}  keeps ${receipt.realised_false_keeps}`);
      console.log(`  budget met on this cohort: ${receipt.budget_met_on_this_cohort}\n  ${receipt.prediction_note}`);
    }
    if ((argv.includes("--require-no-loss") && !receipt.no_loss_observed_on_this_cohort) ||
        (!argv.includes("--json") && !receipt.budget_met_on_this_cohort)) process.exitCode = 1;
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
