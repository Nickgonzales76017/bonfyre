#!/usr/bin/env bun
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const cmsPath = () => process.env.BONFYRE_CMS_DB ?? "/var/lib/bonfyre/cms/bonfyre_cms.db";
const receiptDir = () => process.env.BONFYRE_PURE_RECEIPTS ?? "/var/lib/bonfyre/gym/pure";
const now = () => new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
const seconds = start => Math.round((performance.now() - start) * 1000) / 1e6;
const words = new Set(["network", "http", "fetch", "write", "publish", "send", "pay", "charge", "delete", "mutate", "effect", "submit", "email", "sms"]);
const numericOps = { "<=": (a, b) => a <= b, "<": (a, b) => a < b, ">=": (a, b) => a >= b, ">": (a, b) => a > b, "==": (a, b) => a === b, "!=": (a, b) => a !== b };
const failed = (error, cost = {}) => ({ result: null, error, cost });
const present = value => value != null && typeof value === "object" && Object.keys(value).length > 0;

export function profiles() {
  try {
    const db = new Database(cmsPath(), { readonly: true });
    try {
      return db.query("SELECT backend, kind, purity, availability, COALESCE(version,'') version, COALESCE(cost_metric,'') cost_metric FROM execution_profile WHERE status='published' ORDER BY backend").all();
    } finally { db.close(); }
  } catch { return []; }
}

function hvm4Binary() {
  const override = process.env.BONFYRE_HVM4;
  if (override && existsSync(override)) return override;
  const vendored = resolve(here, "../vendor/hvm4/src/hvm4");
  return existsSync(vendored) ? vendored : "hvm4";
}

export function availability(profile) {
  const backend = profile.backend;
  if (backend === "reference") return ["available", ""];
  if (backend === "hvm4") return existsSync(hvm4Binary()) ? ["available", ""] : ["unavailable", "no hvm4 binary; build vendor/hvm4 or set BONFYRE_HVM4"];
  const binary = { hvm2: "hvm", bend: "bend" }[backend] ?? backend;
  return Bun.which(binary) ? ["available", ""] : ["unavailable", `${binary} is not on PATH here`];
}

export function refuseIfEffectful(job) {
  const declared = String(job.effect_class ?? "").toUpperCase();
  if (declared !== "PURE") return `job declares effect_class=${declared || "unset"}; only PURE may be lowered`;
  const seen = new Set();
  const collect = value => {
    for (const word of String(value).toLowerCase().split(/[^a-z0-9]+/)) seen.add(word);
  };
  const walk = node => {
    if (Array.isArray(node)) node.forEach(walk);
    else if (node && typeof node === "object") for (const [key, value] of Object.entries(node)) { collect(key); walk(value); }
    else if (typeof node === "string") collect(node);
  };
  for (const [key, value] of Object.entries(job)) if (key !== "effect_class") { collect(key); walk(value); }
  const hit = [...seen].filter(word => words.has(word)).sort()[0];
  return hit ? `job names '${hit}', which is a way to reach outside a pure reduction` : null;
}

const integer = value => Number.isSafeInteger(value);
export function numericHolds(candidate, clauses) {
  return Object.entries(clauses).every(([field, clause]) => integer(candidate[field]) && numericOps[clause.op]?.(candidate[field], clause.value) === true);
}
export function numericRefusal(job) {
  for (const [field, clause] of Object.entries(job.require_numeric ?? {})) {
    if (!clause || typeof clause !== "object" || !("op" in clause) || !("value" in clause)) return `numeric clause on '${field}' needs an op and a value`;
    if (!(clause.op in numericOps)) return `operator '${clause.op}' is not one of ${Object.keys(numericOps).sort().join(", ")}`;
    if (!integer(clause.value)) return `numeric clause on '${field}' compares against ${JSON.stringify(clause.value)}; this lowering is total on integers only and will not approximate a float`;
    for (const candidate of job.candidate_space) if (!integer(candidate[field])) return `field '${field}' on '${candidate.id}' is ${JSON.stringify(candidate[field])}, not an integer; the numeric lowering refuses rather than rounds`;
  }
  return null;
}
export function subsetRefusal(job) {
  const spec = job.subset_budget;
  if (!present(spec)) return null;
  if (!spec.field || !integer(spec.budget)) return "subset_budget needs a field and an integer budget";
  const size = job.candidate_space.length;
  if (size > 20) return `${size} candidates is 2^${size} subsets; this lowering enumerates the lattice and refuses past 20 rather than returning part of it`;
  for (const candidate of job.candidate_space) if (!integer(candidate[spec.field])) return `field '${spec.field}' on '${candidate.id}' is ${JSON.stringify(candidate[spec.field])}, not an integer`;
  return null;
}

function meets(candidate, require) { return Object.entries(require).every(([field, expected]) => candidate[field] === expected); }
export function runReference(job) {
  const start = performance.now();
  const space = job.candidate_space;
  if (present(job.subset_budget)) {
    const { field, budget } = job.subset_budget;
    const costs = space.map(candidate => candidate[field]);
    const count = 2 ** costs.length;
    const result = [];
    if (costs.reduce((sum, cost) => sum + Math.abs(cost), 0) <= Number.MAX_SAFE_INTEGER) {
      const totals = new Float64Array(count);
      for (let mask = 0; mask < count; mask++) {
        if (mask) {
          const bit = mask & -mask;
          totals[mask] = totals[mask ^ bit] + costs[31 - Math.clz32(bit)];
        }
        if (totals[mask] <= budget) result.push(String(mask));
      }
    } else {
      const totals = Array(count).fill(0n);
      const exact = costs.map(BigInt);
      for (let mask = 0; mask < count; mask++) {
        if (mask) {
          const bit = mask & -mask;
          totals[mask] = totals[mask ^ bit] + exact[31 - Math.clz32(bit)];
        }
        if (totals[mask] <= BigInt(budget)) result.push(String(mask));
      }
    }
    return { result, cost: { wall_seconds: seconds(start), subsets: count } };
  }
  const result = space.filter(candidate => meets(candidate, job.require ?? {}) && numericHolds(candidate, job.require_numeric ?? {})).map(candidate => candidate.id).sort();
  return { result, cost: { wall_seconds: seconds(start) } };
}

async function runText(binary, args, source, timeoutMs) {
  const temporary = mkdtempSync(join(tmpdir(), "bonfyre-pure-"));
  const path = join(temporary, binary === "bend" ? "job.bend" : "job.hvm");
  try {
    await Bun.write(path, source);
    const command = args.map(arg => arg === "{source}" ? path : arg);
    const start = performance.now();
    const proc = Bun.spawn(command, { stdout: "pipe", stderr: "pipe" });
    const timeout = setTimeout(() => proc.kill(), timeoutMs);
    const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
    clearTimeout(timeout);
    return { stdout, stderr, code, wall_seconds: seconds(start) };
  } catch (error) { return { error: String(error).slice(0, 100) }; }
  finally { rmSync(temporary, { recursive: true, force: true }); }
}

const sup = items => items.length === 1 ? items[0] : `{${sup(items.slice(0, Math.floor(items.length / 2)))} ${sup(items.slice(Math.floor(items.length / 2)))}}`;
const sup4 = items => items.length === 1 ? items[0] : `&A{${sup4(items.slice(0, Math.floor(items.length / 2)))},${sup4(items.slice(Math.floor(items.length / 2)))}}`;
const boolPasses = job => job.candidate_space.map(candidate => Number(meets(candidate, job.require ?? {})));
const boolRefusal = (job, suffix) => {
  for (const candidate of job.candidate_space) for (const field of Object.keys(job.require ?? {})) if (typeof candidate[field] !== "boolean") return `field '${field}' on '${candidate.id}' is not boolean${suffix}`;
  return null;
};
const collapsed = stdout => {
  let interactions = null;
  const verdicts = [];
  for (const raw of stdout.split("\n")) {
    const line = raw.replace(/\x1b\[[0-9;]*m/g, "").trim();
    if (line.startsWith("- Itrs:")) { interactions = Number(line.replace(/\D/g, "")) || 0; continue; }
    if (line.startsWith("-") || !line) continue;
    const token = line.split(/\s+/)[0];
    if (/^\d+$/.test(token)) verdicts.push(Number(token));
  }
  return { verdicts, interactions };
};

export async function runHvm2(job) {
  const space = job.candidate_space;
  if (present(job.subset_budget)) return failed("a subset lattice needs arithmetic over superposed sums; this lowering has neither and refuses");
  if (present(job.require_numeric)) return failed("this lowering is boolean-only; a numeric clause is not expressible here and is refused rather than dropped");
  const refusal = boolRefusal(job, "; this lowering encodes boolean predicates only");
  if (refusal) return { result: null, error: refusal };
  const pairs = boolPasses(job).map((pass, i) => `(${i + 1} ${pass})`);
  const source = `@keep = (c r)\n  & c ~ (id p)\n  & ?((0 id) r) ~ p\n\n@main = r\n  & @keep ~ (${sup(pairs)} r)\n`;
  const run = await runText("hvm", ["hvm", "run", "{source}"], source, 120_000);
  if (run.error) return failed(run.error);
  const lines = run.stdout.split("\n");
  const raw = lines.find(line => line.startsWith("Result:"))?.slice("Result:".length).trim();
  if (!raw) return failed((run.stderr || run.stdout).slice(0, 120));
  const tokens = raw.match(/\d+|[^\s{}]+/g) ?? [];
  if (tokens.some(token => !/^\d+$/.test(token))) return failed(`net did not fully reduce: ${raw.slice(0, 60)}`);
  const verdicts = tokens.map(Number);
  if (verdicts.length !== space.length) return failed(`expected ${space.length} verdicts, decoded ${verdicts.length}`);
  if (verdicts.some(value => value < 0 || value > space.length)) return failed("net returned an out-of-range candidate index");
  const interactionLine = lines.find(line => line.includes("ITRS"));
  return { result: verdicts.filter(Boolean).map(value => space[value - 1].id).sort(), lowered: "predicate_and_fan_out",
    cost: { interactions: interactionLine ? Number(interactionLine.replace(/\D/g, "")) || 0 : null, wall_seconds: run.wall_seconds, candidates: space.length } };
}

async function runHvm4Subsets(job) {
  const space = job.candidate_space;
  const { field, budget } = job.subset_budget;
  const labels = space.map((_, i) => String.fromCharCode(65 + i));
  const accumulate = terms => terms.slice(1).reduce((sum, term) => `(${sum} + ${term})`, terms[0]);
  const total = accumulate(space.map((candidate, i) => `&${labels[i]}{0,${candidate[field]}}`));
  const mask = accumulate(space.map((_, i) => `&${labels[i]}{0,${2 ** i}}`));
  const source = `@keep = λm. λp. λ{0: 0; λk. m}(p)\n@main = @keep((${mask} + 1), ((${total} <= ${budget}) * 1))\n`;
  const run = await runText("hvm4", [hvm4Binary(), "{source}", `-C${2 ** space.length}`, "-s"], source, 300_000);
  if (run.error) return failed(run.error);
  const { verdicts, interactions } = collapsed(run.stdout);
  if (verdicts.length !== 2 ** space.length) return failed(`expected 2^${space.length} branches, read ${verdicts.length}`);
  return { result: verdicts.filter(Boolean).map(value => String(value - 1)).sort((a, b) => Number(a) - Number(b)), lowered: "subset_lattice_in_net",
    cost: { interactions, wall_seconds: run.wall_seconds, subsets: 2 ** space.length, interactions_per_subset: interactions ? Math.round(interactions / 2 ** space.length * 10) / 10 : null } };
}

export async function runHvm4(job) {
  if (present(job.subset_budget)) {
    const refusal = subsetRefusal(job);
    return refusal ? failed(refusal) : runHvm4Subsets(job);
  }
  const space = job.candidate_space;
  const refusal = boolRefusal(job, "");
  if (refusal) return { result: null, error: refusal };
  if (present(job.require_numeric)) {
    const numeric = numericRefusal(job);
    if (numeric) return failed(numeric);
  }
  const ids = sup4(space.map((_, i) => String(i + 1)));
  let mask = sup4(boolPasses(job).map(String));
  let lowered = "predicate_and_fan_out";
  let clauses = 0;
  if (present(job.require_numeric)) {
    const terms = Object.entries(job.require_numeric).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([field, clause]) => {
      const values = sup4(space.map(candidate => String(candidate[field])));
      return `(${values} ${clause.op} ${clause.value})`;
    });
    mask = `(${terms.reduce((product, term) => product ? `(${product} * ${term})` : term, "")} * ${mask})`;
    lowered = "numeric_comparison_in_net";
    clauses = terms.length;
  }
  const source = `@keep = λid. λp. λ{0: 0; λk. id}(p)\n@main = @keep(${ids}, ${mask})\n`;
  const run = await runText("hvm4", [hvm4Binary(), "{source}", `-C${space.length + 4}`, "-s"], source, 120_000);
  if (run.error) return failed(run.error);
  const { verdicts, interactions } = collapsed(run.stdout);
  if (verdicts.length !== space.length) return failed(`expected ${space.length} collapsed verdicts, read ${verdicts.length}${run.stderr.trim() ? `; stderr ${run.stderr.trim().slice(0, 80)}` : ""}`);
  if (verdicts.some(value => value < 0 || value > space.length)) return failed("net returned an out-of-range candidate index");
  const cost = { interactions, wall_seconds: run.wall_seconds, candidates: space.length };
  if (clauses) cost.clauses = clauses;
  return { result: verdicts.filter(Boolean).map(value => space[value - 1].id).sort(), lowered, cost };
}

export async function runBend(job) {
  const space = job.candidate_space;
  if (present(job.subset_budget)) return failed("a subset lattice needs arithmetic over superposed sums; this lowering has neither and refuses");
  if (present(job.require_numeric)) return failed("the Bend fold encodes a boolean predicate; a numeric clause is not expressible in it and is refused");
  const refusal = boolRefusal(job, "");
  if (refusal) return { result: null, error: refusal };
  const passes = boolPasses(job);
  const tree = (lo, hi) => hi - lo === 1 ? `Cand/Leaf(${lo + 1}, ${passes[lo]})` : `Cand/Node(${tree(lo, Math.floor((lo + hi) / 2))}, ${tree(Math.floor((lo + hi) / 2), hi)})`;
  const source = `type Cand:\n  Leaf { id, pass_ }\n  Node { ~left, ~right }\n\ndef keep(id, pass_):\n  switch pass_:\n    case 0:\n      return 0\n    case _:\n      return id\n\ndef tally(t):\n  fold t:\n    case Cand/Leaf:\n      k = keep(t.id, t.pass_)\n      switch k:\n        case 0:\n          return (0, 0)\n        case _:\n          return (k, 1)\n    case Cand/Node:\n      (ls, lc) = t.left\n      (rs, rc) = t.right\n      return (ls + rs, lc + rc)\n\ndef main():\n  return tally(${tree(0, space.length)})\n`;
  const run = await runText("bend", ["bend", "run-rs", "{source}"], source, 180_000);
  if (run.error) return failed(run.error);
  const match = run.stdout.match(/Result:\s*\((\d+),\s*(\d+)\)/);
  if (!match) return failed((run.stderr || run.stdout || "no result").slice(0, 140));
  const checksum = Number(match[1]);
  const count = Number(match[2]);
  const reference = { survivors: passes.reduce((sum, value) => sum + value, 0), index_checksum: passes.reduce((sum, value, i) => sum + value * (i + 1), 0) };
  return { aggregate: { survivors: count, index_checksum: checksum }, agrees_with_reference: count === reference.survivors && checksum === reference.index_checksum,
    reference_aggregate: reference, lowered: "predicate_compiled_by_bend", answers: "an aggregate over the frontier, not the frontier itself", cost: { wall_seconds: run.wall_seconds, candidates: space.length } };
}

export const RUNNERS = { reference: runReference, hvm2: runHvm2, hvm4: runHvm4, bend: runBend };

export async function admissionFor(job) {
  const workClass = String(job.work_class ?? "discovery");
  const scope = String(job.data_scope ?? "public");
  const perBackend = {};
  for (const backend of Object.keys(RUNNERS)) {
    try {
      const proc = Bun.spawn(["bun", join(here, "resource-admission.mjs"), "--class", workClass, "--data-scope", scope, "--backend", backend, "--json"], {
        env: { ...process.env, BONFYRE_CMS_DB: cmsPath() }, stdout: "pipe", stderr: "pipe",
      });
      const stdout = await new Response(proc.stdout).text();
      await proc.exited;
      perBackend[backend] = JSON.parse(stdout).placement;
    } catch { perBackend[backend] = { site: null, because: "admission could not be reached; a backend is not placed by default" }; }
  }
  return { work_class: workClass, data_scope: scope, per_backend: perBackend };
}

export async function execute(job, declared = profiles(), admission = null) {
  const refusal = refuseIfEffectful(job);
  if (refusal) return { schema: "bonfyre-pure-job.v1", job: job.job ?? "?", refused: true, reason: refusal };
  if (!Array.isArray(job.candidate_space) || !job.candidate_space.length || job.candidate_space.some(candidate => candidate == null || typeof candidate !== "object" || candidate.id == null)) {
    return { schema: "bonfyre-pure-job.v1", job: job.job ?? "?", refused: true, reason: "candidate_space must contain identified candidates" };
  }
  const subsetReason = subsetRefusal(job);
  if (subsetReason) return { schema: "bonfyre-pure-job.v1", job: job.job ?? "?", refused: true, reason: subsetReason };
  if (!declared.length) return { schema: "bonfyre-pure-job.v1", job: job.job ?? "?", refused: true, reason: `no execution_profile rows are readable at ${cmsPath()}; the backends are declared in the CMS and this process cannot see it` };
  admission ??= await admissionFor(job);
  const backends = [];
  for (const profile of declared) {
    const [state, why] = availability(profile);
    const row = { backend: profile.backend, declared: profile.availability, here: state, cost_metric: profile.cost_metric };
    const placement = admission.per_backend?.[profile.backend] ?? {};
    row.placement = placement.site ?? null;
    if (state === "available" && !placement.site) row.refused_by_admission = placement.because ?? "not placed";
    else if (state === "available" && RUNNERS[profile.backend]) Object.assign(row, await RUNNERS[profile.backend](job));
    else if (state === "available") row.skipped_because = "available but no lowering is written for this backend";
    else row.unavailable_because = why;
    backends.push(row);
  }
  const produced = backends.filter(row => row.result != null);
  const aggregates = backends.filter(row => row.aggregate != null);
  const agreement = new Set(produced.map(row => JSON.stringify(row.result))).size <= 1;
  return { schema: "bonfyre-pure-job.v1", job: job.job ?? "?", effect_class: "PURE", admission, candidates: job.candidate_space.length, ran_at: now(),
    backends, backends_producing_a_result: produced.length, agreement: produced.length > 1 ? agreement : null,
    aggregate_backends: aggregates.map(row => ({ backend: row.backend, aggregate: row.aggregate, agrees_with_reference: row.agrees_with_reference, answers: row.answers })),
    agreement_note: produced.length < 2 ? "only one backend produced a result, so agreement is undetermined" : agreement ? "every backend that produced a result produced the same one" : "backends disagree on a pure computation; this is the finding",
    cannot_infer: ["agreement between backends -> correctness", "an interaction count -> wall time or energy", "a candidate frontier -> a decision", "a backend exercising a job's shape -> a backend that computed its result"] };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const path = args.find(arg => !arg.startsWith("--"));
  if (!path || !existsSync(path)) { console.error("usage: pure-job.mjs <job.json> [--json] [--receipt]"); process.exit(2); }
  const receipt = await execute(JSON.parse(readFileSync(path, "utf8")));
  if (args.includes("--json")) console.log(JSON.stringify(receipt, null, 2));
  else if (receipt.refused) console.log(`  refused: ${receipt.reason}`);
  else {
    console.log(`  job ${receipt.job}  candidates=${receipt.candidates}  effect_class=PURE`);
    console.log(`  admission: work=${receipt.admission.work_class} scope=${receipt.admission.data_scope}`);
    for (const row of receipt.backends) {
      if (row.result != null) console.log(`    ${row.backend.padEnd(12)} ${String(row.result.length).padStart(4)} survivors   ${row.cost?.wall_seconds ?? "?"}s`);
      else if (row.aggregate != null) console.log(`    ${row.backend.padEnd(12)} ${String(row.aggregate.survivors).padStart(4)} survivors   checksum ${row.aggregate.index_checksum} (${row.agrees_with_reference ? "agrees" : "DISAGREES"} with reference)`);
      else console.log(`    ${row.backend.padEnd(12)}   --             ${String(row.refused_by_admission ?? row.unavailable_because ?? row.skipped_because ?? row.error ?? "produced no result and gave no reason").slice(0, 58)}`);
    }
    console.log(`  ${receipt.agreement_note}`);
  }
  if (args.includes("--receipt") && !receipt.refused) {
    const name = `${receipt.job}-${receipt.ran_at.replaceAll(":", "")}.json`;
    const destination = join(receiptDir(), name);
    const { mkdirSync } = await import("node:fs");
    mkdirSync(receiptDir(), { recursive: true });
    await Bun.write(destination, JSON.stringify(receipt, null, 2));
    console.error(`  receipt: ${destination}`);
  }
  if (receipt.refused) process.exitCode = 1;
}
