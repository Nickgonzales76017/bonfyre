#!/usr/bin/env bun
// Apply an authored retention regime to public ForeignTwin bodies only.
// Receipts remain immutable; --apply is the sole deletion path.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { existsSync, lstatSync, readFileSync, readdirSync, realpathSync, unlinkSync } from "node:fs";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

const defaults = { receipts: "/var/lib/bonfyre/foreign/receipts", cms: "/var/lib/bonfyre/cms/bonfyre_cms.db",
  binary: "/usr/local/bin/bonfyre-project", bodies: "", apply: false };

export function options(args) {
  const result = { ...defaults };
  const values = new Map([["--receipts", "receipts"], ["--cms", "cms"], ["--binary", "binary"], ["--bodies", "bodies"]]);
  for (let i = 0; i < args.length; i++) {
    const key = values.get(args[i]);
    if (key) {
      if (!args[i + 1]) throw new Error(`${args[i]} requires a value`);
      result[key] = args[++i];
    } else if (args[i] === "--apply") result.apply = true;
    else throw new Error(`unknown option: ${args[i]}`);
  }
  result.bodies ||= join(dirname(resolve(result.receipts)), "bodies");
  return result;
}

export function subjectId(url) {
  const text = String(url), scheme = text.indexOf("://");
  const body = (scheme >= 0 ? text.slice(scheme + 3) : text).toLowerCase();
  return `foreign_twin:${body.replace(/[^\p{L}\p{N}]+/gu, "-").replace(/^-|-$/g, "")}`;
}

function policies(path) {
  const db = new Database(path, { readonly: true });
  try {
    return new Map(db.query("SELECT twin_subject,regime,retain_body FROM twin_policy WHERE status='published'")
      .all().map(row => [String(row.twin_subject), { regime: String(row.regime), retain: Number(row.retain_body || 0) }]));
  } finally { db.close(); }
}

function hydration(binary, receipts) {
  const child = spawnSync(binary, ["foreign", "hydration"], { encoding: "utf8", timeout: 120_000,
    maxBuffer: 64 * 1024 * 1024, env: { ...process.env, BONFYRE_FOREIGN_RECEIPTS: receipts } });
  if (child.error || child.status !== 0) throw new Error(`foreign hydration failed: ${child.error?.message ?? child.stderr.trim()}`);
  let report;
  try { report = JSON.parse(child.stdout); } catch { throw new Error("foreign hydration returned invalid JSON"); }
  if (!Array.isArray(report?.twins)) throw new Error("foreign hydration did not return twins");
  return report.twins;
}

function receiptBodies(root) {
  const byUrl = new Map(), byPath = new Map(), invalid = [];
  for (const name of readdirSync(root).filter(name => name.endsWith(".json")).sort()) {
    let receipt;
    try {
      const stat = lstatSync(join(root, name));
      if (!stat.isFile() || stat.isSymbolicLink())
        throw new Error("receipt is not a regular file");
      receipt = JSON.parse(readFileSync(join(root, name), "utf8"));
    }
    catch { invalid.push(name); continue; }
    if (typeof receipt?.source_url !== "string" || typeof receipt?.body_path !== "string") continue;
    const paths = byUrl.get(receipt.source_url) ?? new Set();
    paths.add(receipt.body_path);
    byUrl.set(receipt.source_url, paths);
    const coordinate = resolve(receipt.body_path);
    const owners = byPath.get(coordinate) ?? new Set();
    owners.add(receipt.source_url);
    byPath.set(coordinate, owners);
  }
  return { byUrl, byPath, invalid };
}

function boundedBody(path, bodiesRoot) {
  if (!isAbsolute(path) || dirname(resolve(path)) !== bodiesRoot) throw new Error(`body outside public body root: ${path}`);
  if (!existsSync(path)) return null;
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || dirname(realpathSync(path)) !== realpathSync(bodiesRoot))
    throw new Error(`body is not a direct regular file: ${path}`);
  return stat.size;
}

export function run(config) {
  const bodyRoot = resolve(config.bodies);
  const rootStat = lstatSync(bodyRoot);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error("public body root must be a direct directory");
  const policy = policies(config.cms);
  const twins = hydration(config.binary, config.receipts);
  const receipts = receiptBodies(config.receipts);
  const failures = [...receipts.invalid.map(name => `invalid receipt: ${name}`)];
  let kept = 0, unpoliced = 0;
  const planned = [];
  for (const twin of twins) {
    const url = twin?.url;
    if (typeof url !== "string") { failures.push("hydration twin has no URL"); continue; }
    const regime = policy.get(subjectId(url)) ?? policy.get(url);
    if (!regime) { unpoliced++; continue; }
    if (regime.retain) { kept++; continue; }
    for (const path of receipts.byUrl.get(url) ?? []) {
      try {
        if ((receipts.byPath.get(resolve(path))?.size ?? 0) > 1) throw new Error(`body shared by multiple sources: ${path}`);
        const size = boundedBody(path, bodyRoot);
        if (size === null) continue;
        planned.push({ path, url, regime: regime.regime, body: basename(path), bytes: size });
      } catch (error) { failures.push(error.message); }
    }
  }
  const cooled = [];
  if (!config.apply) cooled.push(...planned);
  else if (!failures.length) for (const item of planned) {
    try { unlinkSync(item.path); cooled.push(item); }
    catch (error) { failures.push(`cannot cool ${item.path}: ${error.message}`); }
  }
  const summaries = cooled.map(({ path: _path, ...rest }) => rest);
  return { schema: "bonfyre.foreign.cool.v1", applied: config.apply && !failures.length,
    cooled: summaries, bodies_cooled: summaries.length,
    bytes_freed: summaries.reduce((sum, item) => sum + item.bytes, 0), retained_by_regime: kept, unpoliced,
    note: "receipts are never removed; cooling reduces what is held, never what is known to have been seen",
    forbidden_inference: "a cooled twin is not a forgotten one, and a cooled body is not a drifted one -- the authored regime is what distinguishes an absent body that was not meant to be kept from one that no longer matches its receipt",
    failures };
}

if (import.meta.main) {
  try {
    const report = run(options(process.argv.slice(2)));
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    if (report.failures.length) process.exitCode = 1;
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
