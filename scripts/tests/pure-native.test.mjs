import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { availability, execute, refuseIfEffectful, runHvm4, runReference, RUNNERS, subsetRefusal } from "../pure-job.mjs";
import { deriveTrials, executeSeries } from "../pure-series.mjs";

const job = JSON.parse(readFileSync(new URL("./fixtures/pure-job.json", import.meta.url)));
const declared = ["reference", "hvm2", "hvm4", "bend"].map(backend => ({ backend, availability: "declared", cost_metric: "interactions" }));
const admission = { work_class: "discovery", data_scope: "public", per_backend: Object.fromEntries(declared.map(({ backend }) => [backend, { site: "local" }])) };

test("pure jobs are admitted, effectful variants refused before backend dispatch", () => {
  expect(refuseIfEffectful(job)).toBeNull();
  for (const effect of [{ fetch_url: "https://example.com" }, { steps: ["publish the result"] }, { on_done: { charge: "card" } }]) {
    expect(refuseIfEffectful({ ...job, ...effect })).not.toBeNull();
  }
  expect(refuseIfEffectful({ ...job, effect_class: "WRITE" })).toContain("only PURE");
});

test("reference frontier and integer subsets are exact", () => {
  expect(runReference(job).result).toEqual(["a", "d", "e"]);
  const subset = { ...job, subset_budget: { field: "price_micro_usd", budget: 750 }, candidate_space: job.candidate_space.slice(0, 2) };
  expect(subsetRefusal(subset)).toBeNull();
  expect(runReference(subset).result).toEqual(["0", "1", "2", "3"]);
  expect(subsetRefusal({ ...subset, candidate_space: Array(21).fill(job.candidate_space[0]) })).toContain("refuses past 20");
  expect(runReference({ ...job, subset_budget: {} }).result).toEqual(["a", "d", "e"]);
});

// A reducer absent from this machine is an absence, not an inability: the
// agreement cases need all three present, and the absence case below runs anywhere.
const reducersHere = ["hvm2", "hvm4", "bend"].every(backend => availability({ backend })[0] === "available");

test.skipIf(!reducersHere)("native reducers compute the same frontier and aggregate under an explicit placement", async () => {
  const receipt = await execute(job, declared, admission);
  expect(receipt.refused).toBeUndefined();
  expect(receipt.backends_producing_a_result).toBe(3);
  expect(receipt.agreement).toBe(true);
  for (const row of receipt.backends.filter(row => row.result)) expect(row.result).toEqual(["a", "d", "e"]);
  const bend = receipt.backends.find(row => row.backend === "bend");
  expect(bend.aggregate).toEqual({ survivors: 3, index_checksum: 10 });
  expect(bend.agrees_with_reference).toBe(true);
  expect(receipt.aggregate_backends).toHaveLength(1);
});

test.skipIf(availability({ backend: "hvm4" })[0] !== "available")("HVM4 computes numeric predicates and subset lattices inside the net", async () => {
  const numeric = { ...job, require_numeric: { price_micro_usd: { op: "<=", value: 1000 } } };
  const numericResult = await runHvm4(numeric);
  expect(numericResult.result).toEqual(runReference(numeric).result);
  expect(numericResult.lowered).toBe("numeric_comparison_in_net");
  expect(numericResult.cost.interactions).toBeGreaterThan(0);
  const subset = { ...job, subset_budget: { field: "price_micro_usd", budget: 750 }, candidate_space: job.candidate_space.slice(0, 3) };
  const subsetResult = await runHvm4(subset);
  expect(subsetResult.result).toEqual(runReference(subset).result);
  expect(subsetResult.lowered).toBe("subset_lattice_in_net");
  expect(subsetResult.cost.subsets).toBe(8);
});

test("an absent reducer is reported as unavailable, never as an error or a result", async () => {
  const absent = [{ backend: "reference", availability: "declared", cost_metric: "interactions" }, { backend: "no-such-reducer", availability: "declared", cost_metric: "interactions" }];
  const receipt = await execute(job, absent, { ...admission, per_backend: { ...admission.per_backend, "no-such-reducer": { site: "local" } } });
  const row = receipt.backends.find(row => row.backend === "no-such-reducer");
  expect(row.here).toBe("unavailable");
  expect(row.unavailable_because).toContain("not on PATH");
  expect(row.error).toBeUndefined();
  expect(row.result).toBeUndefined();
  expect(receipt.backends_producing_a_result).toBe(1);
  expect(receipt.agreement).toBeNull();
  expect(receipt.agreement_note).toContain("undetermined");
});

test("missing placement fails closed even when binaries are available", async () => {
  const receipt = await execute(job, declared, { ...admission, per_backend: {} });
  expect(receipt.backends_producing_a_result).toBe(0);
  expect(receipt.backends.every(row => row.here === "available" ? row.refused_by_admission : row.unavailable_because)).toBe(true);
});

test("malformed candidate cohorts refuse before any backend can read them", async () => {
  const receipt = await execute({ ...job, candidate_space: [] }, declared, admission);
  expect(receipt.refused).toBe(true);
  expect(receipt.reason).toContain("identified candidates");
});

test.skipIf(availability({ backend: "hvm2" })[0] !== "available")("a corrupted reducer result is named as a disagreement", async () => {
  const original = RUNNERS.hvm2;
  try {
    RUNNERS.hvm2 = async () => ({ result: ["a"], lowered: "predicate_and_fan_out", cost: { interactions: 1 } });
    const receipt = await execute(job, declared.filter(row => ["reference", "hvm2"].includes(row.backend)), admission);
    expect(receipt.agreement).toBe(false);
    expect(receipt.agreement_note).toContain("this is the finding");
  } finally { RUNNERS.hvm2 = original; }
});

test("shape series cannot promote one temporal cohort", async () => {
  expect(deriveTrials(job)).toHaveLength(6);
  const receipt = await executeSeries(job, [{ backend: "reference" }]);
  expect(receipt.trial_count).toBe(6);
  expect(receipt.summary.backends.reference.exact).toBe(6);
  expect(receipt.promotion_evidence.temporal_cohort_series).toBe(false);
  expect(receipt.promotion_evidence.promotion_ready).toBe(false);
});

test.skipIf(!process.env.PURE_CMS || !process.env.PURE_JOB)("live CMS declaration and admission are checked before reducer dispatch", async () => {
  process.env.BONFYRE_CMS_DB = process.env.PURE_CMS;
  const liveJob = JSON.parse(readFileSync(process.env.PURE_JOB, "utf8"));
  const receipt = await execute(liveJob);
  expect(receipt.refused).toBeUndefined();
  expect(receipt.backends.length).toBeGreaterThan(0);
  expect(receipt.backends.every(row => row.result != null || row.aggregate != null || row.unavailable_because || row.refused_by_admission || row.skipped_because || row.error)).toBe(true);
});
