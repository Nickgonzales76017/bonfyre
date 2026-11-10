#!/usr/bin/env bun
// Project's read-only published-price projection and native-owner spread registration.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { closeSync, constants, existsSync, fstatSync, openSync, readSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { atomicReport } from "./economic-reports.mjs";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const sha256 = value => createHash("sha256").update(value).digest("hex");
const estateDir = () => process.env.ESTATE_DIR || join(homedir(), ".bonfyre/estate");
const safeName = value => value.replace(/[^A-Za-z0-9._-]/g, "_");
const matches = (text, pattern) => [...text.matchAll(pattern)].map(match => {
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value < 0 || value > Number.MAX_SAFE_INTEGER)
    throw new Error("published price is not a finite, safely representable nonnegative number");
  return value;
});
const baseUnitPrices = text => [...text.matchAll(/"(?:maxAmountRequired|amount)"\s*:\s*"([0-9]+)"/g)].map(match => {
  const units = BigInt(match[1]);
  if (units > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("base-unit price exceeds exact integer range");
  return Number(units) / 1_000_000;
});

// Python's repr(float) uses scientific notation outside [1e-4, 1e16),
// unlike JS. The legacy receipt hashes repr(sorted(set(prices))), so preserve
// that exact common decimal spelling for its nonnegative, finite price domain.
export function priceFloatRepr(value) {
  if (!Number.isFinite(value) || value < 0) throw new Error("invalid price");
  if (value === 0) return "0.0";
  if (value < 1e-4 || value >= 1e16) {
    const [mantissa, exponent] = value.toExponential().split("e");
    const n = Number(exponent);
    return `${mantissa}e${n < 0 ? "-" : "+"}${String(Math.abs(n)).padStart(2, "0")}`;
  }
  const decimal = String(value);
  return Number.isInteger(value) ? `${decimal}.0` : decimal;
}

function readPrefix(path, length) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd);
    if (!before.isFile() || !Number.isSafeInteger(before.size))
      throw new Error(`retained body is not a regular file: ${path}`);
    const buffer = Buffer.alloc(Math.min(length, before.size));
    let offset = 0;
    while (offset < buffer.length) {
      const bytes = readSync(fd, buffer, offset, buffer.length - offset, offset);
      if (bytes === 0) throw new Error(`retained body changed while reading: ${path}`);
      offset += bytes;
    }
    const after = fstatSync(fd);
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs)
      throw new Error(`retained body changed while reading: ${path}`);
    return buffer;
  } finally { closeSync(fd); }
}

function withEstateDb(estate, fn) {
  const db = new Database(join(estate, "estate.db"), { readonly: true, create: false });
  try { return fn(db); }
  finally { db.close(); }
}

export function buildPriceBook({ estate = estateDir(), bodies = process.env.BODIES || join(homedir(), ".bonfyre/foreign-sweep/bodies"),
  out = join(estate, "price-book.jsonl"), now = new Date() } = {}) {
  const rows = withEstateDb(estate, db => db.query("SELECT id, primary_url FROM occurrence WHERE source_kind='x402'").all());
  const recordedAt = now.toISOString().replace(/\.\d{3}Z$/, "Z");
  const counts = { quoted: 0, freeTier: 0, proseOnly: 0, silent: 0 };
  const result = [];
  for (const { id, primary_url: sourceUrl } of rows) {
    if (typeof id !== "string") throw new Error("x402 occurrence has no string identity");
    const path = join(bodies, `${safeName(id)}.body`);
    if (!existsSync(path)) { counts.silent++; continue; }
    const raw = readPrefix(path, 400000);
    const text = new TextDecoder("utf-8", { fatal: false }).decode(raw);
    let values = matches(text, /"price"\s*:\s*"\$?([0-9]+\.?[0-9]*)"/g);
    values.push(...matches(text, /"humanAmount"\s*:\s*"([0-9]+\.?[0-9]*)"/g));
    if (values.length === 0) values = baseUnitPrices(text);
    if (values.length === 0) {
      if (/\$\d+\.\d{2,6}/.test(text)) counts.proseOnly++;
      else counts.silent++;
      continue;
    }
    const prices = [...new Set(values)].sort((a, b) => a - b);
    const paid = prices.filter(value => value > 0);
    const freeTier = prices.includes(0);
    if (freeTier) counts.freeTier++;
    if (paid.length === 0) continue;
    const evidence = sha256(`${id}\x1f[${paid.map(priceFloatRepr).join(", ")}]\x1fquoted`);
    result.push({
      schema: "bonfyre-owner-participation.v1", occurrence_id: id, axis: "economic", state: "quoted",
      owner: "ForeignQuote", from_owner: "ForeignTwin", to_owner: "ForeignQuote",
      basis_sha256: sha256(raw), evidence_sha256: evidence, produced_by: "bonfyre-project",
      recorded_at: recordedAt, min_price_usd: paid[0], max_price_usd: paid.at(-1),
      free_tier_published: freeTier, source_url: sourceUrl, spend: 0,
      cannot_infer: [
        "a published price -> a contracted rate or a price anyone was charged",
        "a free tier -> a free answer to the need being routed",
        "quoted -> committed, paid, settled or reconciled",
      ],
    });
    counts.quoted++;
  }
  const summary = {
    schema: "bonfyre-price-book.v1", metered_sources: rows.length, quoted: counts.quoted,
    publishes_a_free_tier: counts.freeTier, prose_price_not_parsed: counts.proseOnly,
    no_published_price: counts.silent, spend: 0,
    truth: "prices_sources_published_about_themselves_read_from_bytes_already_held",
  };
  return atomicReport(out, write => {
    for (const value of result) write(`${JSON.stringify(value)}\n`);
    return summary;
  });
}

export function routeCandidate(route) {
  if (!route || !Number.isSafeInteger(route.free_and_proven) || route.free_and_proven <= 0 ||
      !Number.isSafeInteger(route.metered) || route.metered < 4 || route.thin_market !== false ||
      typeof route.route !== "string" || route.route.length === 0) return null;
  const price = route.avoided_cost_usd?.median;
  if (typeof price !== "number" || !Number.isFinite(price) || price <= 0) return null;
  const sell = price.toFixed(6);
  if (!/^\d+\.\d{6}$/.test(sell) || Number(sell) <= 0) return null;
  return { source: route.route, sell };
}

export function registerRouteArbitrage({ estate = estateDir(), financeDb = process.env.FINANCE_DB || join(estate, "finance.db"),
  plane = process.env.BONFYRE_EXTERNAL_ESTATE_BINARY || join(repo, "cmd/BonfyreExternalEstate/bonfyre-external-estate"),
  finance = process.env.BONFYRE_FINANCE_BINARY || join(repo, "cmd/BonfyreFinance/bonfyre-finance") } = {}) {
  const needs = withEstateDb(estate, db => db.query(`SELECT axis||':'||term AS need FROM occurrence_term
    WHERE axis IN ('affordance','domain') GROUP BY axis, term HAVING count(*) >= 40 ORDER BY count(*) DESC`).all());
  let registered = 0, skipped = 0, failedRouteReads = 0;
  for (const { need } of needs) {
    if (!need) { skipped++; continue; }
    let route;
    try { route = Bun.spawnSync([plane, "route-cost", "--db", join(estate, "estate.db"), "--need", need,
      "--price-book", join(estate, "price-book.jsonl"), "--free-proof", join(estate, "enactment-evidence.jsonl")]); }
    catch { failedRouteReads++; continue; }
    if (route.exitCode !== 0) { failedRouteReads++; continue; }
    let candidate;
    try { candidate = routeCandidate(JSON.parse(route.stdout.toString())); }
    catch { failedRouteReads++; continue; }
    if (!candidate) { skipped++; continue; }
    const name = need.replaceAll(":", "-");
    try {
      const add = Bun.spawnSync([finance, "--db", financeDb, "service", "add", "--name", name,
        "--buy", "0", "--sell", candidate.sell, "--source", candidate.source]);
      if (add.exitCode === 0) registered++;
      else skipped++;
    } catch { skipped++; }
  }
  return {
    schema: "bonfyre-route-arbitrage.v1", needs_considered: needs.length,
    services_registered: registered, skipped, failed_route_reads: failedRouteReads,
    buy_basis: "a source this estate has proven it can read for nothing under a published permission",
    sell_basis: "the median published price of the metered sources serving the same need, thin markets of fewer than four sellers excluded",
    truth: "spreads_registered_for_ranking_not_revenue",
    cannot_infer: [
      "a spread -> revenue; nobody was charged and nobody paid",
      "a published price -> a price anyone agreed with this estate",
      "a registered service -> a service anyone has asked for",
    ],
  };
}

if (import.meta.main) {
  try {
    const [command, out] = process.argv.slice(2);
    const result = command === "price-book" ? buildPriceBook({ out: out || join(estateDir(), "price-book.jsonl") }) :
      command === "route-arbitrage" ? registerRouteArbitrage() : null;
    if (!result) throw new Error("usage: economic-views.mjs price-book [out] | route-arbitrage");
    console.log(JSON.stringify(result));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
