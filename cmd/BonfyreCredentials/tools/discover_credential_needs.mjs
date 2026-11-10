#!/usr/bin/env bun
// Descriptor discovery is observation, not authority. Credential schemes may
// enter Credentials; payment presentation stays economic descriptor evidence.
import { Database } from "bun:sqlite";
import { createHash, randomBytes } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { appendFileSync, closeSync, existsSync, mkdirSync, openSync, readFileSync, readSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const execFileAsync = promisify(execFile);
const paths = ["/.well-known/api-catalog", "/.well-known/openapi.json", "/openapi.json", "/swagger.json", "/api/openapi.json", "/v3/api-docs"];
const descriptorProtocols = new Set(["openapi", "swagger", "api_catalog", "rfc9727"]);
const machineProtocols = new Set(["mcp", "a2a", "x402", "graphql", "jsonrpc", "webhook", "ogc", "beckn", "oauth"]);
const paymentHeaders = new Set(["payment-signature", "x-payment", "x-payment-response", "payment-required", "x402-payment"]);
const digest = value => createHash("sha256").update(value).digest("hex");

export function normalizeHost(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  try { return (raw.includes("://") ? new URL(raw).host : raw.split("/")[0]).trim().toLowerCase(); }
  catch { return ""; }
}

export function candidateHosts(cmsPath, frontierPath = "") {
  const priority = new Map();
  if (cmsPath && existsSync(cmsPath)) {
    const db = new Database(cmsPath, { readonly: true });
    try {
      for (const row of db.query("SELECT url FROM internet_source WHERE status='published' AND url LIKE 'http%'").all()) {
        const host = normalizeHost(row.url);
        if (host) priority.set(host, Math.max(priority.get(host) ?? 0, 5100));
      }
    } finally { db.close(); }
  }
  if (frontierPath && existsSync(frontierPath)) {
    const db = new Database(frontierPath, { readonly: true });
    try {
      const rootHosts = new Map();
      for (const row of db.query("SELECT root_id, host FROM root WHERE COALESCE(host,'')<>''").all()) {
        const host = normalizeHost(row.host);
        if (!host) continue;
        rootHosts.set(row.root_id, host);
        priority.set(host, Math.max(priority.get(host) ?? 0, 100));
      }
      const bonuses = new Map();
      for (const row of db.query("SELECT root_id, LOWER(COALESCE(protocol,'')) AS protocol, LOWER(COALESCE(truth_level,'')) AS truth_level, "
        + "LOWER(COALESCE(refusal,'')) AS refusal, COALESCE(credential,'') AS credential FROM channel WHERE root_id IS NOT NULL").all()) {
        const host = rootHosts.get(row.root_id);
        if (!host) continue;
        const protocol = row.protocol.trim(), truth = row.truth_level.trim(), refusal = row.refusal.trim();
        const observed = truth.startsWith("observed") || truth.startsWith("admitted");
        const derived = truth.includes("derived") || truth.endsWith("candidate");
        const usable = !refusal;
        const blocked = refusal.includes("robots") || refusal.includes("policy");
        let machine = 0, credential = 0;
        if (!blocked && usable) {
          if (descriptorProtocols.has(protocol)) machine = observed ? 12000 : derived ? 6000 : 0;
          else if (machineProtocols.has(protocol)) machine = observed ? 7000 : derived ? 750 : 0;
          if (row.credential.trim()) credential = observed ? 8000 : 1500;
        }
        const old = bonuses.get(host) ?? [0, 0];
        bonuses.set(host, [Math.max(old[0], machine), Math.max(old[1], credential)]);
      }
      for (const [host, [machine, credential]] of bonuses) priority.set(host, (priority.get(host) ?? 100) + machine + credential);
    } catch (error) {
      if (!String(error.message).includes("no such table") && !String(error.message).includes("no such column")) throw error;
    } finally { db.close(); }
  }
  return priority;
}

export function described(text) {
  let doc;
  try { doc = JSON.parse(text); } catch { return null; }
  if (!doc || typeof doc !== "object" || Array.isArray(doc) || !(doc.openapi || doc.swagger)) return null;
  return Object.keys(doc.components?.securitySchemes ?? doc.securityDefinitions ?? {}).length ? doc : null;
}
function paymentScheme(name, scheme) {
  return String(name ?? "").toLowerCase().includes("x402") || paymentHeaders.has(String(scheme?.name ?? "").toLowerCase());
}
export function securitySummary(doc) {
  const credential = [], economic = [];
  for (const [name, scheme] of Object.entries(doc.components?.securitySchemes ?? doc.securityDefinitions ?? {})) {
    if (!scheme || typeof scheme !== "object" || Array.isArray(scheme)) continue;
    (paymentScheme(name, scheme) ? economic : credential).push(name);
  }
  return { credential_schemes: credential.sort(), economic_schemes: economic.sort() };
}
export function paymentProtocols(doc) {
  const carriers = new Set();
  for (const [name, scheme] of Object.entries(doc.components?.securitySchemes ?? doc.securityDefinitions ?? {})) {
    if (!scheme || typeof scheme !== "object" || Array.isArray(scheme)) continue;
    const key = name.toLowerCase();
    if (paymentScheme(name, scheme)) carriers.add("x402");
    if (key.includes("mpp")) carriers.add("mpp");
    if (key.includes("l402")) carriers.add("l402");
  }
  return [...carriers].sort();
}

export async function observeDescriptor(project, foreign, twin, url) {
  const receipts = join(foreign, "receipts"), bodies = join(foreign, "bodies");
  mkdirSync(receipts, { recursive: true }); mkdirSync(bodies, { recursive: true });
  // Every path gets its own immutable carrier coordinate instead of replacing
  // the preceding path's receipt under one host-wide filename.
  // A repeat observation must never read a preceding run's receipt or body.
  const observation = `${twin}-${digest(url).slice(0, 12)}-${randomBytes(6).toString("hex")}`;
  const receiptPath = join(receipts, `${observation}.json`), bodyPath = join(bodies, `${observation}.body`);
  try {
    await execFileAsync(project, ["foreign", "observe", observation, url, "--receipt", receiptPath, "--body", bodyPath, "--observe-right", "public"],
      { timeout: 90000, maxBuffer: 1024 * 1024 });
  } catch (error) {
    if (error.killed) return [{ policy_decision: "timeout" }, ""];
  }
  let receipt = {};
  if (existsSync(receiptPath)) {
    const lines = readFileSync(receiptPath, "utf8").split(/\r?\n/).filter(line => line.startsWith("{"));
    try { receipt = JSON.parse(lines.at(-1) ?? "{}"); } catch {}
  }
  let text = "";
  if (existsSync(bodyPath) && (receipt.policy_decision == null || receipt.policy_decision === "allow")
    && Number(receipt.http_status ?? 0) >= 200 && Number(receipt.http_status ?? 0) < 300) {
    const fd = openSync(bodyPath, "r");
    try {
      const bytes = Buffer.alloc(4_000_000);
      text = bytes.subarray(0, readSync(fd, bytes, 0, bytes.length, 0)).toString("utf8");
    } finally { closeSync(fd); }
  }
  return [receipt, text];
}

function readExactHosts(path) {
  const hosts = [], seen = new Set();
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    const host = normalizeHost(line.split("\t")[0]);
    if (host && !seen.has(host)) { seen.add(host); hosts.push(host); }
  }
  return hosts;
}
function options(argv) {
  const args = { project: "", credentials: "", db: "", cms: "", frontierDb: "", foreign: "", evidence: "",
    hostsPerRun: 40, hostFile: "", listHosts: false };
  const keys = { "--project": "project", "--credentials": "credentials", "--db": "db", "--cms": "cms",
    "--frontier-db": "frontierDb", "--foreign": "foreign", "--evidence": "evidence", "--hosts-per-run": "hostsPerRun", "--host-file": "hostFile" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--list-hosts") { args.listHosts = true; continue; }
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  if (!args.cms) throw new Error("missing --cms");
  args.hostsPerRun = Number(args.hostsPerRun);
  if (!Number.isInteger(args.hostsPerRun) || args.hostsPerRun < 0) throw new Error("invalid --hosts-per-run");
  if (!args.listHosts) for (const key of ["project", "credentials", "db", "foreign", "evidence"]) if (!args[key]) throw new Error(`missing --${key}`);
  return args;
}

function nativeCredentialsImport(argv) {
  const result = Bun.spawnSync(argv, { stdout: "pipe", stderr: "pipe" });
  return { exitCode: result.exitCode, stderr: new TextDecoder().decode(result.stderr).trim() };
}

export async function discover(args, { observeFn = observeDescriptor, importFn = nativeCredentialsImport, sleepFn = Bun.sleep } = {}) {
  const priorities = candidateHosts(args.cms, args.frontierDb);
  const hosts = [...priorities.keys()].sort();
  if (args.listHosts) return { hosts: [...priorities.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])), report: null };
  const selected = args.hostFile ? readExactHosts(args.hostFile) : (() => {
    const start = (Math.floor(Date.now() / 86400000) * args.hostsPerRun) % Math.max(1, hosts.length);
    return [...hosts.slice(start), ...hosts.slice(0, start)].slice(0, args.hostsPerRun);
  })();
  const selection = args.hostFile ? "workgraph" : "legacy-day-slice";
  const stats = { hosts_known: hosts.length, asked: selected.length, described: 0, declared: 0, credential_declared: 0,
    economic_hosts: 0, economic_schemes: 0, payment_protocol_hosts: 0, payment_protocols: {}, selection };
  for (const host of selected) {
    const twin = `descriptor-${digest(host).slice(0, 16)}`;
    let found = null, descriptorUrl = "", lastRecord = null;
    for (const path of paths) {
      const address = `https://${host}${path}`;
      const [receipt, text] = await observeFn(args.project, args.foreign, twin, address);
      const record = { host, path, http_status: receipt.http_status ?? null, policy: receipt.policy_decision ?? null };
      lastRecord = record;
      if (receipt.policy_decision != null && receipt.policy_decision !== "allow") {
        appendFileSync(args.evidence, JSON.stringify(record) + "\n");
        break;
      }
      if (text && path === "/.well-known/api-catalog") {
        let links = [];
        try {
          const catalog = JSON.parse(text);
          links = (catalog.linkset ?? []).flatMap(entry => (entry["service-desc"] ?? []).map(link => link?.href)).filter(Boolean);
        } catch {}
        for (const href of links.slice(0, 3)) {
          if (!href.startsWith("https://")) continue;
          const [, subText] = await observeFn(args.project, args.foreign, `${twin}-desc`, href);
          if (described(subText)) { found = subText; descriptorUrl = href; record.service_desc = href; break; }
        }
      } else if (text && described(text)) { found = text; descriptorUrl = address; }
      appendFileSync(args.evidence, JSON.stringify(record) + "\n");
      if (found) break;
      await sleepFn(200);
    }
    if (!found) continue;
    const spec = join(args.foreign, "bodies", `${twin}.spec.json`);
    mkdirSync(join(args.foreign, "bodies"), { recursive: true });
    writeFileSync(spec, found);
    const provider = host.toLowerCase().replace(/[^a-z0-9-]+/g, "-").replace(/^-|-$/g, "").slice(0, 60);
    const doc = JSON.parse(found);
    const summary = securitySummary(doc), carriers = paymentProtocols(doc);
    stats.described++;
    if (summary.economic_schemes.length) { stats.economic_hosts++; stats.economic_schemes += summary.economic_schemes.length; }
    if (carriers.length) {
      stats.payment_protocol_hosts++;
      for (const carrier of carriers) stats.payment_protocols[carrier] = (stats.payment_protocols[carrier] ?? 0) + 1;
    }
    const imported = summary.credential_schemes.length
      ? importFn([args.credentials, "openapi", "--db", args.db, "--provider", provider, "--spec", spec, "--name", host, "--actor", "discovery"])
      : null;
    if (imported?.exitCode === 0) { stats.declared++; stats.credential_declared++; }
    appendFileSync(args.evidence, JSON.stringify({ schema: "bonfyre-demand-descriptor.v1", occurrence_id: `external:${provider}`,
      host, descriptor_url: descriptorUrl || `https://${host}${lastRecord?.path ?? ""}`, descriptor_path: spec,
      descriptor_sha256: digest(found), described: provider, credential_imported: imported?.exitCode === 0 ? provider : null,
      credential_schemes: summary.credential_schemes, economic_schemes: summary.economic_schemes,
      payment_protocols: carriers, produced_by: "discover_credential_needs.mjs",
      cannot_infer: ["a published descriptor -> this estate is authorized to call it",
        "a payment carrier declaration -> spend approval, payment verification or settlement",
        "credential material -> payment authority"],
      detail: imported ? imported.stderr.slice(0, 200) : "descriptor retained outside credential boundary" }) + "\n");
  }
  stats.payment_protocols = Object.fromEntries(Object.entries(stats.payment_protocols).sort(([a], [b]) => a.localeCompare(b)));
  return { report: stats };
}

if (import.meta.main) {
  try {
    const result = await discover(options(process.argv.slice(2)));
    if (result.hosts) for (const [host, priority] of result.hosts) console.log(`${host}\t${priority}`);
    else console.log(JSON.stringify(result.report));
  } catch (error) { console.error(error); process.exitCode = 1; }
}
