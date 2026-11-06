#!/usr/bin/env bun
// One owner-routed read, exact bytes and a signed receipt; no Python or SQL interpolation.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { createWriteStream, existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable, Transform } from "node:stream";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "../../..");
const exported = Bun.spawnSync(["cue", "export", join(here, "served-answer.cue"), "-e", "servedAnswer", "--out", "json"]);
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
export const policy = JSON.parse(exported.stdout.toString());
const digest = value => createHash("sha256").update(value).digest("hex");
const safeName = value => String(value).replace(/[^A-Za-z0-9._-]/g, "_");
const number = (value, name) => {
  const result = Number(value);
  if (!Number.isFinite(result) || result < 0) throw new Error(`${name} is not a finite nonnegative number`);
  return result;
};
const whole = (value, name) => {
  const result = number(value, name);
  if (!Number.isSafeInteger(result)) throw new Error(`${name} is not a safe integer`);
  return result;
};

export function choose(route) {
  let selected = null;
  for (const candidate of route?.candidates ?? []) {
    if (candidate.basis !== policy.readable_basis || !candidate.identity || !candidate.name) continue;
    const cost = Number(candidate.cost_usd);
    if (!Number.isFinite(cost) || cost < 0) continue;
    if (!selected || cost < selected.cost_usd) selected = { ...candidate, cost_usd: cost };
  }
  return selected;
}

function priorDigest(path) {
  if (!existsSync(path)) return null;
  const lines = readFileSync(path, "utf8").trim().split(/\r?\n/).reverse();
  for (const line of lines) {
    try {
      const value = JSON.parse(line);
      if (typeof value.content_sha256 === "string") return value.content_sha256;
    } catch { /* incomplete receipt is not evidence */ }
  }
  return null;
}

async function retain(response, outDir, fileId) {
  const hash = createHash("sha256"), temporary = join(outDir, `${fileId}.partial-${process.pid}-${crypto.randomUUID()}`);
  let bytes = 0;
  try {
    const meter = new Transform({ transform(chunk, _encoding, callback) { hash.update(chunk); bytes += chunk.length; callback(null, chunk); } });
    await pipeline(Readable.fromWeb(response.body), meter, createWriteStream(temporary, { flags: "wx" }));
    const sha256 = hash.digest("hex"), target = join(outDir, `${fileId}-${sha256}.body`);
    renameSync(temporary, target);
    return { bytes, sha256, path: target };
  } finally { if (existsSync(temporary)) rmSync(temporary); }
}

function atomicWrite(path, content) {
  const temporary = `${path}.partial-${process.pid}-${crypto.randomUUID()}`;
  try { writeFileSync(temporary, content, { flag: "wx" }); renameSync(temporary, path); }
  finally { if (existsSync(temporary)) rmSync(temporary); }
}

export async function serve({ need, outDir = join(homedir(), ".bonfyre/estate/served"),
  estate = process.env.ESTATE_DIR || join(homedir(), ".bonfyre/estate"),
  identityDir = process.env.IDENTITY_DIR || join(homedir(), ".bonfyre/identity"),
  foreignReceipts = join(homedir(), ".bonfyre/foreign-sweep/receipts"),
  plane = join(repo, "cmd/BonfyreExternalEstate/bonfyre-external-estate"),
  signer = join(repo, "cmd/BonfyreSign/bonfyre-sign"),
  route = null, fetcher = fetch } = {}) {
  if (!need || typeof need !== "string") throw new Error("need is required");
  mkdirSync(outDir, { recursive: true });
  if (!route) {
    const result = Bun.spawnSync([plane, "route-cost", "--db", join(estate, "estate.db"), "--need", need,
      "--price-book", join(estate, "price-book.jsonl"), "--free-proof", join(estate, "enactment-evidence.jsonl")]);
    if (result.exitCode !== 0) throw new Error(`route-cost refused: ${result.stderr.toString().slice(-300)}`);
    route = JSON.parse(result.stdout.toString());
  }
  const chosen = choose(route);
  if (!chosen) return { served: false, need, reason: "no proven-readable source on this coordinate" };
  if (!existsSync(signer) || !existsSync(join(identityDir, "institution.ed25519.secret")))
    throw new Error("served answers require the native signer and institution key");
  const db = new Database(join(estate, "estate.db"), { readonly: true, create: false });
  let coordinate;
  try { coordinate = db.query("SELECT primary_url FROM occurrence WHERE id=?").get(chosen.identity)?.primary_url; }
  finally { db.close(); }
  if (!coordinate) throw new Error(`routed occurrence has no address: ${chosen.identity}`);
  const url = new URL(coordinate);
  if (!["http:", "https:"].includes(url.protocol)) throw new Error(`routed occurrence has a non-HTTP address: ${chosen.identity}`);
  if (url.username || url.password) throw new Error(`routed occurrence address contains credentials: ${chosen.identity}`);
  const slug = safeName(chosen.identity), fileId = `${slug}-${digest(chosen.identity).slice(0, 12)}`;
  const prior = priorDigest(join(foreignReceipts, `${slug}.json`));
  let answer = null;
  let status = "000", bytes = 0, current = "", bodyPath = null;
  let response = null;
  try {
    response = await fetcher(url, { headers: { "User-Agent": policy.user_agent },
      redirect: "manual", signal: AbortSignal.timeout(policy.timeout_ms) });
  } catch (_) { /* a failed read is a refusal, not an empty successful answer */ }
  if (response) {
    status = String(response.status);
    if (response.status === 200 && response.body) {
      const retained = await retain(response, outDir, fileId);
      bytes = retained.bytes; current = retained.sha256; bodyPath = retained.path;
    }
  }
  const freshness = status !== "200" ? "unreachable_now" : current === prior ? "unchanged_since_observation" : "changed_since_observation";
  const receipt = { schema: policy.schema, need, served: status === "200" && bodyPath !== null, answer: { body_path: bodyPath, bytes,
    http_status: status, content_sha256: current }, route: { source: chosen.identity, name: chosen.name, url: url.href,
    cost_usd: number(chosen.cost_usd, "cost_usd"), basis: policy.route_basis,
    frontier_considered: whole(route.frontier, "frontier"), metered_alternatives: whole(route.metered, "metered"),
    avoided_cost_usd_median: number(route.avoided_cost_usd?.median, "avoided_cost_usd.median") },
    freshness: { state: freshness, observed_sha256: prior }, cannot_infer: policy.cannot_infer };
  const encoded = JSON.stringify(receipt, null, 2);
  answer = join(outDir, `${fileId}-${digest(encoded)}.answer.json`);
  const temporary = `${answer}.partial-${process.pid}-${crypto.randomUUID()}`;
  try {
    writeFileSync(temporary, encoded, { flag: "wx" });
    const signed = Bun.spawnSync([signer, "sign", "--key-dir", identityDir, "--file", temporary]);
    if (signed.exitCode !== 0 || !signed.stdout.length) throw new Error(`native signer refused the served answer: ${signed.stderr.toString().slice(-300)}`);
    atomicWrite(`${answer}.sig`, signed.stdout);
    renameSync(temporary, answer);
  } finally {
    if (existsSync(temporary)) rmSync(temporary);
  }
  return receipt;
}

if (import.meta.main) {
  try {
    const [need, outDir] = process.argv.slice(2);
    const result = await serve({ need, outDir });
    console.log(JSON.stringify(result, null, 2));
    if (!result.served) process.exitCode = 1;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
