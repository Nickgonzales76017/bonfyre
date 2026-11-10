#!/usr/bin/env bun
// Representation admission reads measured envelopes; a missing measure cannot grant loss.
import { Database } from "bun:sqlite";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const policyResult = Bun.spawnSync(["cue", "export", join(here, "resource-admission.cue"), "-e", "resourceAdmission", "--out", "json"]);
if (policyResult.exitCode !== 0) throw new Error(policyResult.stderr.toString());
export const policy = JSON.parse(policyResult.stdout.toString());

export function readRows(path, query) {
  try {
    const db = new Database(path, { readonly: true, create: false });
    try { return db.query(query).all(); }
    finally { db.close(); }
  } catch { return []; }
}

export function envelopes(dbPath) {
  return readRows(dbPath, "SELECT representation, axis, availability, COALESCE(bytes_per_body,'') bytes_per_body, COALESCE(envelope,'') envelope FROM representation_profile WHERE status='published'").map(row => {
    const recall = /recall@(\d+)\s+([0-9.]+)/.exec(row.envelope);
    const prunes = /([0-9]+)\s+false prunes/.exec(row.envelope);
    const bytes = /^\s*[+-]?\d+\s*$/.test(String(row.bytes_per_body)) ? Number(row.bytes_per_body) : null;
    return { ...row, measured_k: recall ? Number(recall[1]) : null,
      recall: recall ? Number(recall[2]) : null,
      false_prunes: prunes ? Number(prunes[1]) : null,
      bytes: Number.isSafeInteger(bytes) ? bytes : null };
  });
}

export function place(backend, dataScope, sites) {
  const considered = [];
  const eligible = [];
  const scopes = policy.disclosure_scopes;
  const want = scopes.includes(dataScope) ? scopes.indexOf(dataScope) : scopes.length;
  for (const site of [...sites].sort((a, b) => Number(b.cores || 0) - Number(a.cores || 0))) {
    const row = { site: site.site, cores: site.cores };
    const known = scopes.includes(site.max_data_scope);
    if (!String(site.backends).split(",").map(s => s.trim()).includes(backend)) row.verdict = `does not run ${backend}`;
    else if (!known) row.verdict = `max_data_scope '${site.max_data_scope}' is not one of ${scopes.join(", ")}; an unrecognised clearance is cleared for nothing`;
    else if (scopes.indexOf(site.max_data_scope) < want) row.verdict = `cleared for ${site.max_data_scope}, this input is ${dataScope}; capability is not permission`;
    else { row.verdict = "eligible"; eligible.push(site); }
    considered.push(row);
  }
  if (!eligible.length) return { site: null, considered, because: `no declared site both runs ${backend} and may see ${dataScope} input; the work cannot be placed` };
  eligible.sort((a, b) => Number(b.holds_live_data || 0) - Number(a.holds_live_data || 0) || Number(b.cores || 0) - Number(a.cores || 0));
  const best = eligible[0];
  return { site: best.site, cores: best.cores, considered,
    because: Number(best.holds_live_data || 0) ? "the data is already here, so nothing is disclosed and nothing moves" : `the only eligible site running ${backend}, and this input is ${dataScope} so moving it discloses nothing` };
}

export function admit(workClass, axis, k, minRecall, profiles, { requireNoLoss = false } = {}) {
  const considered = [];
  const fallback = policy.fallback_representation;
  if (policy.exact_only_classes.includes(workClass)) return { work_class: workClass, axis, k, min_recall: minRecall,
    admitted: fallback, because: `${workClass} work has no loss budget to spend; a dropped candidate here is a wrong answer rather than a slower one`, considered, exact_only: true };
  let chosen = null;
  for (const rep of [...profiles].sort((a, b) => (a.bytes == null) - (b.bytes == null) || (a.bytes || 0) - (b.bytes || 0))) {
    const row = { representation: rep.representation, bytes: rep.bytes };
    if (rep.representation === fallback) row.verdict = "reserved as the fallback";
    else if (rep.availability !== "available" || rep.recall == null) row.verdict = "no measured envelope; an envelope is earned by an executed trial";
    else if (rep.axis !== axis) row.verdict = `measured on axis ${rep.axis}, not ${axis}; an envelope does not cross axes`;
    else if (rep.measured_k !== k) row.verdict = `envelope measured at k=${rep.measured_k}, budget stated at k=${k}; recall is not monotone in k`;
    else if (rep.recall < minRecall) row.verdict = `preserves ${rep.recall}, budget requires ${minRecall}`;
    else if (requireNoLoss && (rep.recall !== 1 || rep.false_prunes !== 0))
      row.verdict = rep.false_prunes == null
        ? "no-loss requires an explicit measured false-prune count; missing is not zero"
        : `no-loss requires recall@${k}=1 and zero false prunes, observed ${rep.recall} and ${rep.false_prunes}`;
    else { row.verdict = `satisfies the budget at ${rep.recall}`; row.recall = rep.recall; chosen ??= rep; }
    considered.push(row);
  }
  if (!chosen) return { work_class: workClass, axis, k, min_recall: minRecall, admitted: fallback,
    because: requireNoLoss
      ? "no same-axis, same-k candidate proved full recall and zero false prunes; retain exact representation"
      : "no representation with a measured envelope satisfies this budget, so the work runs exact; the closest miss is not admitted, because a budget that bends is a suggestion",
    considered, exact_only: false };
  return { work_class: workClass, axis, k, min_recall: minRecall, admitted: chosen.representation,
    because: `cheapest representation at ${chosen.bytes} bytes per body whose measured envelope (${chosen.recall} at k=${k}) satisfies the budget`,
    bytes_per_body: chosen.bytes, measured_recall: chosen.recall, considered, exact_only: false };
}

export function decide(options = {}) {
  const dbPath = options.dbPath ?? process.env.BONFYRE_CMS_DB ?? "/var/lib/bonfyre/cms/bonfyre_cms.db";
  const workClass = options.workClass ?? policy.default_work_class;
  const axis = options.axis ?? policy.default_axis;
  const k = options.k ?? policy.default_k;
  const minRecall = options.minRecall ?? policy.default_min_recall;
  const dataScope = options.dataScope ?? policy.default_data_scope;
  const backend = options.backend ?? policy.default_backend;
  const requireNoLoss = options.requireNoLoss === true;
  const decision = admit(workClass, axis, k, minRecall, envelopes(dbPath), { requireNoLoss });
  if (requireNoLoss) decision.no_loss_required = true;
  decision.placement = place(backend, dataScope, readRows(dbPath, "SELECT site, cores, backends, holds_live_data, max_data_scope, COALESCE(reach_seconds,'0') reach_seconds FROM compute_site WHERE status='published'"));
  Object.assign(decision, { backend, data_scope: dataScope, schema: policy.schema, decided_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    cannot_infer: ["an admitted representation -> a correct one", "satisfying a budget on the cohort the envelope was measured over -> satisfying it on this one", "a cheaper representation -> a better one", "a site that may see this input -> a site that may see any input", "capability -> permission"] });
  return decision;
}

function optionsFrom(argv) {
  const values = {};
  const names = { "--class": "workClass", "--axis": "axis", "--k": "k", "--min-recall": "minRecall", "--data-scope": "dataScope", "--backend": "backend" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--require-no-loss") values.requireNoLoss = true;
    else if (names[argv[i]]) values[names[argv[i]]] = argv[++i];
  }
  if (values.k !== undefined) values.k = Number(values.k);
  if (values.minRecall !== undefined) values.minRecall = Number(values.minRecall);
  if (!Number.isInteger(values.k ?? policy.default_k) || (values.k ?? policy.default_k) <= 0 || !Number.isFinite(values.minRecall ?? policy.default_min_recall)) throw new Error("invalid k or min-recall");
  return values;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const result = decide(optionsFrom(process.argv.slice(2)));
    if (process.argv.includes("--json")) console.log(JSON.stringify(result, null, 2));
    else {
      console.log(`  work=${result.work_class} axis=${result.axis} k=${result.k} budget=recall>=${result.min_recall}`);
      console.log(`  admitted: ${result.admitted}\n  because:  ${result.because}`);
      console.log(`  placed:   ${result.placement.site || "REFUSED"} (${result.backend}, ${result.data_scope} input)\n  because:  ${result.placement.because}`);
      for (const c of result.placement.considered) console.log(`    ${String(c.site).padEnd(14)}${String(c.cores).padStart(3)} cores  ${c.verdict.slice(0, 58)}`);
      if (result.considered.length) console.log();
      for (const c of result.considered) console.log(`    ${String(c.representation).padEnd(19)}${String(c.bytes || "-").padStart(6)}  ${c.verdict.slice(0, 64)}`);
    }
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
