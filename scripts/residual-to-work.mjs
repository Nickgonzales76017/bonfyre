#!/usr/bin/env bun
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { flowWorkCase } from "../components/BonfyreDecision/frontier-adapters.mjs";
import { decisionDigest } from "../components/BonfyreDecision/decision-plane.mjs";

const policyPath = fileURLToPath(new URL("./residual-work-owners.cue", import.meta.url));

export function owners() {
  const result = Bun.spawnSync(["cue", "export", policyPath, "--out", "json"]);
  if (result.exitCode !== 0) throw new Error(`CUE owners invalid: ${result.stderr.toString()}`);
  return JSON.parse(result.stdout.toString());
}

export function splitCommand(source) {
  const words = [];
  let word = "";
  let quote = "";
  let active = false;
  for (let i = 0; i < source.length; i++) {
    const c = source[i];
    if (c === "\\" && quote !== "'" && i + 1 < source.length) {
      word += source[++i];
      active = true;
    } else if (quote) {
      if (c === quote) quote = "";
      else word += c;
    } else if (c === "'" || c === '"') {
      quote = c;
      active = true;
    } else if (/\s/.test(c)) {
      if (active) words.push(word);
      word = "";
      active = false;
    } else {
      word += c;
      active = true;
    }
  }
  if (quote) throw new Error("unclosed quote in --from source");
  if (active) words.push(word);
  return words;
}

export function commandParts(source) {
  const words = splitCommand(source);
  const env = { ...process.env };
  while (words.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[0])) {
    const assignment = words.shift();
    const equals = assignment.indexOf("=");
    env[assignment.slice(0, equals)] = assignment.slice(equals + 1);
  }
  return { words, env };
}

export async function runSource(source) {
  const { words, env } = commandParts(source);
  if (!words.length) return { error: "a --from source names no program, only assignments" };
  try {
    const proc = Bun.spawn(words, { env, stdout: "pipe", stderr: "pipe" });
    const timeout = setTimeout(() => proc.kill(), 300_000);
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited,
    ]);
    clearTimeout(timeout);
    if (code !== 0 && !stdout.trim()) {
      return { error: `a --from source exited ${code}: ${stderr.trim().split("\n").at(-1)?.slice(0, 140) || "no output"}` };
    }
    try { return { receipt: JSON.parse(stdout) }; }
    catch { return { error: `a --from source produced no JSON: ${stdout.trim().slice(0, 100)}` }; }
  } catch (error) {
    return { error: `a --from source could not be run: ${words[0]}: ${String(error).slice(0, 120)}` };
  }
}

export function itemsFrom(receipt, source, policy) {
  if (!receipt || typeof receipt !== "object" || Array.isArray(receipt)) return [];
  const out = [];
  const owner = kind => policy.owners[kind] ?? policy.unknown;
  const add = (kind, size, because, subject) => {
    const item = { source, kind, size, owner: owner(kind).plane, closable: owner(kind).closable, because: String(because ?? "").slice(0, 160) };
    if (subject !== undefined) item.subject = subject;
    out.push(item);
  };
  for (const [kind, body] of Object.entries(receipt.residual ?? {})) {
    if (!body || typeof body !== "object" || Array.isArray(body)) continue;
    if (body.value) add(kind, body.value, body.attribution);
    if (Array.isArray(body.requirements)) {
      for (const requirement of body.requirements) add(kind, 1, requirement.because, requirement.requirement);
    }
  }
  for (const pair of receipt.binding_conditions ?? []) {
    if (Array.isArray(pair) && pair.length >= 2) add("BindingCondition", pair[1], `blocks a route in ${pair[1]} combinations`, pair[0]);
  }
  if (String(receipt.schema ?? "").startsWith("bonfyre-product-close")) {
    const count = (receipt.unreconciled_under ?? 0) + (receipt.unreconciled_over ?? 0);
    if (count) add("ReconciliationResidual", count, `${count} obligations do not reconcile; net ${receipt.net_amount_left}`, receipt.namespace);
  }
  for (const trial of receipt.trials ?? []) {
    if (trial.false_prunes) add("PreservationResidual", trial.false_prunes, `${trial.false_prunes} candidates lost at ${trial.bytes_per_body} bytes per body`, trial.representation);
  }
  return out;
}

function workKey(item) {
  const subject = item.subject || item.source;
  return `residual:${createHash("sha256").update(`${item.kind}|${subject}|${item.owner}`).digest("hex").slice(0, 16)}`;
}

export function decisionFrontier(item, exactReadDigest) {
  // ResidualWork's CUE mapping supplies exactly one candidate owner. The
  // Decision program adds its own `none` abstention; a model cannot mint a
  // different WorkGraph family or turn this review case into a queue claim.
  const role = `role_${decisionDigest(item.owner).slice(0, 16)}`;
  return flowWorkCase({ id: workKey(item), information_policy_class: "private", ...item },
    [{ id: role, description: item.owner, owner_ref: `ResidualWork:${item.kind}` }], exactReadDigest);
}

export async function submit(items, endpoint, auth) {
  const created = [];
  for (const item of items) {
    if (!item.closable) continue;
    const subject = item.subject || item.source;
    const key = workKey(item);
    const details = `${item.because}\n\nOwner plane: ${item.owner}\nMeasured size: ${item.size}\nSource receipt: ${item.source}\n\nRaised from a measured residual. The measurement states the gap; it does not state that closing it is the right next move.`;
    const body = { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "operator_goal_add", arguments: {
      title: `Close ${item.kind} on ${subject}`.slice(0, 120), details,
      priority: Math.min(90, 40 + Math.trunc(Math.min(Number(item.size), 50))),
      source: "residual-to-work", dedupe_key: key,
    } } };
    try {
      const response = await fetch(endpoint, { method: "POST", headers: { "Content-Type": "application/json", Authorization: auth }, body: JSON.stringify(body), signal: AbortSignal.timeout(60_000) });
      const answer = await response.json();
      created.push({ kind: item.kind, subject, dedupe_key: key, accepted: response.ok && !JSON.stringify(answer).slice(0, 400).toLowerCase().includes("error") });
    } catch (error) {
      created.push({ kind: item.kind, subject, dedupe_key: key, accepted: false, error: String(error).slice(0, 100) });
    }
  }
  return created;
}

export async function execute(args, sourceRunner = runSource, policy = owners()) {
  const sources = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--from") sources.push(args[++i]);
    else if (!["--json", "--write"].includes(args[i])) throw new Error(`unknown argument: ${args[i]}`);
  }
  if (!sources.length || sources.some(source => !source)) throw new Error("usage: residual-to-work.mjs --from '<command producing a json receipt>' [--from ...] [--json] [--write]");
  const items = [];
  const exactReads = new Map();
  const unread = [];
  for (const source of sources) {
    const result = await sourceRunner(source);
    if (!result.receipt) {
      if (result.error) console.error(result.error);
      unread.push(splitCommand(source).at(-1) ?? source);
      continue;
    }
    const found = itemsFrom(result.receipt, result.receipt.schema ?? "unknown", policy);
    const readDigest = decisionDigest(result.receipt);
    for (const item of found) exactReads.set(item, readDigest);
    if (!found.length) unread.push(result.receipt.schema ?? source);
    items.push(...found);
  }
  const bySize = (a, b) => Number(b.size) - Number(a.size);
  const buildable = items.filter(item => item.closable).sort(bySize);
  const waiting = items.filter(item => !item.closable).sort(bySize);
  const decision_frontiers = [...buildable, ...waiting]
    .map(item => decisionFrontier(item, exactReads.get(item)));
  const write = args.includes("--write");
  if (write && !process.env.BONFYRE_OPERATOR_AUTH) throw new Error("refusing to write: no BONFYRE_OPERATOR_AUTH is set");
  const submitted = write ? await submit(buildable, process.env.BONFYRE_OPERATOR_MCP ?? "http://127.0.0.1:3051/mcp", process.env.BONFYRE_OPERATOR_AUTH) : [];
  return { schema: "bonfyre-residual-work.v1", ran_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"), sources_read: sources.length - unread.length,
    sources_without_a_recognised_residual: unread, buildable, waiting_on_a_decision: waiting,
    decision_frontiers, written: write, submitted,
    cannot_infer: ["a ranked item -> an approved one", "a residual -> a defect", "the plane named as owner -> who will do the work", "a typed decision frontier -> a model assessment or authorized effect", "a source with no recognised residual -> a source with no residual"] };
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2);
    const receipt = await execute(args);
    if (args.includes("--json")) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  read ${receipt.sources_read} of ${receipt.sources_read + receipt.sources_without_a_recognised_residual.length} sources`);
      if (receipt.sources_without_a_recognised_residual.length) console.log(`  no recognised residual from: ${receipt.sources_without_a_recognised_residual.join(", ")}`);
      console.log(`\n  buildable (${receipt.buildable.length}), by size:`);
      for (const item of receipt.buildable) console.log(`    ${String(item.size).padStart(5)}  ${item.kind.padEnd(26)}${item.owner.padEnd(24)}${String(item.subject ?? "").slice(0, 26)}`);
      if (receipt.waiting_on_a_decision.length) {
        console.log(`\n  waiting on a decision, not a build (${receipt.waiting_on_a_decision.length}):`);
        for (const item of receipt.waiting_on_a_decision) console.log(`    ${String(item.size).padStart(5)}  ${item.kind.padEnd(26)}${item.owner}`);
      }
      console.log(`\n  ${receipt.decision_frontiers.length} private typed decision frontier(s) compiled; no model was called`);
      console.log(`\n  ${receipt.written ? `created ${receipt.submitted.filter(x => x.accepted).length} of ${receipt.submitted.length} goal(s)` : "nothing was written; --write would create work from these"}`);
    }
  } catch (error) {
    console.error(String(error));
    process.exitCode = 2;
  }
}
