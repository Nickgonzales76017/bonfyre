#!/usr/bin/env bun
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { availability, profiles, refuseIfEffectful, RUNNERS } from "./pure-job.mjs";

const sha = value => createHash("sha256").update(value).digest();
const sid = (candidate, index) => String(candidate.id ?? `candidate-${String(index).padStart(8, "0")}`);
const digest = space => createHash("sha256").update(space.map(sid).join("\n")).digest("hex").slice(0, 24);
const sizes = count => [...new Set([3, 5, 8, 13, 21, count].map(size => Math.min(count, size)))].filter(Boolean).sort((a, b) => a - b);

export function deriveTrials(job) {
  const space = job.candidate_space;
  if (!Array.isArray(space) || !space.length) throw new Error("candidate_space must be a non-empty list");
  const hashed = space.map((candidate, index) => ({ candidate, index, hash: sha(sid(candidate, index)) }))
    .sort((a, b) => Buffer.compare(a.hash, b.hash));
  const orders = { forward: space, reverse: [...space].reverse(), "id-hash": hashed.map(row => row.candidate) };
  const trials = [];
  for (const [order, candidates] of Object.entries(orders)) {
    for (const size of sizes(candidates.length)) {
      const cohort = candidates.slice(0, size);
      trials.push({ order, size, cohort_digest: digest(cohort), job: { ...job, job: `${job.job ?? "pure"}::${order}::${size}`, candidate_space: cohort } });
    }
  }
  return trials;
}

export async function runTrial(trial, declared) {
  const { job, ...base } = trial;
  const refusal = refuseIfEffectful(job);
  if (refusal) return { ...base, refused: true, reason: refusal, backends: [] };
  const rows = [];
  for (const profile of declared) {
    const backend = profile.backend;
    const [state, why] = availability(profile);
    const row = { backend, here: state };
    if (state === "available" && RUNNERS[backend]) Object.assign(row, await RUNNERS[backend](job));
    else if (state === "available") row.skipped_because = "available but no lowering is written";
    else row.unavailable_because = why;
    rows.push(row);
  }
  const produced = rows.filter(row => row.result != null);
  const reference = produced.find(row => row.backend === "reference");
  if (reference) {
    const expected = new Set(reference.result);
    for (const row of produced) {
      const result = new Set(row.result);
      row.quality_against_reference = {
        exact_set_match: result.size === expected.size && [...result].every(item => expected.has(item)),
        false_prunes: [...expected].filter(item => !result.has(item)).length,
        false_keeps: [...result].filter(item => !expected.has(item)).length,
      };
    }
  }
  return { ...base, refused: false, backends_producing_a_result: produced.length,
    agreement: produced.length > 1 ? new Set(produced.map(row => JSON.stringify(row.result))).size <= 1 : null, backends: rows };
}

export function summarize(trials) {
  const stats = {};
  let comparable = 0;
  let allAgree = true;
  for (const trial of trials) {
    if (trial.agreement != null) { comparable++; allAgree &&= trial.agreement; }
    for (const row of trial.backends ?? []) {
      const stat = stats[row.backend] ??= { results: 0, exact: 0, false_prunes: 0, false_keeps: 0, interactions: 0, wall_seconds: 0 };
      if (row.result != null) stat.results++;
      const quality = row.quality_against_reference;
      if (quality) {
        stat.exact += Number(quality.exact_set_match);
        stat.false_prunes += quality.false_prunes;
        stat.false_keeps += quality.false_keeps;
      }
      stat.interactions += row.cost?.interactions || 0;
      stat.wall_seconds += row.cost?.wall_seconds || 0;
    }
  }
  return { comparable_trials: comparable, all_comparable_trials_agree: comparable ? allAgree : null, backends: stats };
}

export async function executeSeries(job, declared = profiles()) {
  const refusal = refuseIfEffectful(job);
  if (refusal) throw new Error(`refused: ${refusal}`);
  if (!declared.length) throw new Error("refused: no execution_profile rows are readable");
  const trials = [];
  for (const trial of deriveTrials(job)) trials.push(await runTrial(trial, declared));
  const summary = summarize(trials);
  const reducers = Object.entries(summary.backends).filter(([name, stat]) => name !== "reference" && stat.results === trials.length && stat.exact === trials.length).map(([name]) => name).sort();
  return { schema: "bonfyre-pure-differential-series.v1", source_job: job.job ?? "?", ran_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    series_kind: "deterministic-shape-series-from-one-cohort", source_candidates: job.candidate_space.length, trial_count: trials.length,
    representation: { governing_law: "CompressionLaw / OptimizationCascade", exact_dense: { measured: true, body: "candidate rows + predicate" },
      latent_reduction: { measured: false, reason: "no embedding/wavelet-stage receipt" }, physical_compression: { measured: false, reason: "no FPQ/Parquet-stage receipt" },
      reduction: "backend-specific predicate_and_fan_out", proof_boundary: "frontier compared with exact reference set" },
    summary, reducers_exact_on_every_trial: reducers,
    promotion_evidence: { runtime_shape_series: reducers.length >= 2, temporal_cohort_series: false, promotion_ready: false, reason: "one source cohort cannot establish temporal stability" },
    trials, cannot_infer: ["agreement with reference -> correctness", "shape-series agreement -> temporal stability", "fewer interactions -> lower wall time, energy, or total cost", "unmeasured representation -> representation participated", "candidate frontier -> institutional decision"] };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.includes("--self-test")) {
    const candidate_space = Array.from({ length: 30 }, (_, i) => ({ id: `c${String(i).padStart(2, "0")}`, held: i % 5 !== 0, fresh: i % 7 !== 0 }));
    const trials = deriveTrials({ job: "fixture", effect_class: "PURE", require: { held: true, fresh: true }, candidate_space });
    if (trials.length !== 18 || new Set(trials.map(trial => trial.order)).size !== 3) throw new Error("shape series fixture failed");
    console.log("pure series contract: PASS (18 deterministic trials)");
    process.exit(0);
  }
  let path;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--receipt") { i++; continue; }
    if (!args[i].startsWith("--")) { path = args[i]; break; }
  }
  if (!path) { console.error("usage: pure-series.mjs <job.json> [--json] [--receipt <path>] [--self-test]"); process.exit(2); }
  try {
    const receipt = await executeSeries(JSON.parse(readFileSync(path, "utf8")));
    const receiptIndex = args.indexOf("--receipt");
    if (receiptIndex >= 0) await Bun.write(args[receiptIndex + 1], `${JSON.stringify(receipt, null, 2)}\n`);
    if (args.includes("--json")) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`pure differential series: ${receipt.trial_count} trials`);
      console.log(`  comparable: ${receipt.summary.comparable_trials}`);
      console.log(`  all agree: ${receipt.summary.all_comparable_trials_agree}`);
      for (const [name, stat] of Object.entries(receipt.summary.backends).sort(([a], [b]) => a.localeCompare(b))) console.log(`  ${name.padEnd(12)} ${String(stat.results).padStart(2)}/${receipt.trial_count} results exact=${String(stat.exact).padStart(2)} false_prunes=${stat.false_prunes} false_keeps=${stat.false_keeps}`);
      console.log("  promotion: NO — temporal cohorts remain required");
    }
  } catch (error) { console.error(String(error)); process.exitCode = 1; }
}
