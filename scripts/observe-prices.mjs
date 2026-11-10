#!/usr/bin/env bun
// Publisher statements, not estate rates or merchant authority.
import { Database } from "bun:sqlite";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { boundedText, publicSourceFetch } from "./public-source-fetch.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const exported = Bun.spawnSync(["cue", "export", join(here, "price-observation.cue"), "-e", "priceObservation", "--out", "json"]);
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
export const policy = JSON.parse(exported.stdout.toString());
const date = () => new Date().toISOString().slice(0, 10);
const str = value => value == null ? "" : String(value);
const positive = value => {
  const amount = Number(value);
  return Number.isFinite(amount) && amount > 0 ? amount : null;
};
const row = (source, fields) => ({ source: source.source, kind: source.kind, ...fields });
const last = value => str(value).split("/").pop();
const origin = source => new URL(source.url).origin;

async function get(source, url, fetcher, robotCache, post, accept = "application/json") {
  const response = await publicSourceFetch(url, { origin: origin(source), agent: policy.user_agent,
    fetcher, robots: robotCache, method: post ? "POST" : "GET", accept,
    body: post ? JSON.stringify(post) : undefined });
  if (!response.ok) throw new Error(`${source.source} returned HTTP ${response.status}`);
  return JSON.parse(await boundedText(response, policy.max_json_bytes));
}

export async function observe(name, limit, fetcher = fetch, now = date()) {
  const source = policy.sources[name];
  if (!source) throw new Error(`unknown price source: ${name}`);
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("limit must be a positive safe integer");
  const out = [];
  const robotCache = new Map();
  if (name === "openrouter") {
    const data = await get(source, source.url, fetcher, robotCache);
    for (const m of (data.data ?? []).slice(0, limit)) {
      for (const [field, unit] of [["prompt", "token in"], ["completion", "token out"]]) {
        const amount = positive(m.pricing?.[field]);
        if (amount === null) continue;
        out.push(row(source, { publisher: str(m.id).split("/")[0] || "openrouter", item: str(m.id), amount,
          currency: "USD", unit, region: "", evidence_ref: `openrouter.ai/api/v1/models stated pricing.${field}=${m.pricing[field]} for ${str(m.id)} on ${now}` }));
      }
    }
  } else if (name === "azure") {
    let url = source.url;
    while (url && out.length < limit) {
      const data = await get(source, url, fetcher, robotCache);
      for (const item of data.Items ?? []) {
        const amount = positive(item.unitPrice);
        if (amount === null) continue;
        out.push(row(source, { publisher: "microsoft-azure", item: `${str(item.productName)} / ${str(item.meterName)}`,
          amount, currency: str(item.currencyCode ?? "USD"), unit: str(item.unitOfMeasure), region: str(item.armRegionName),
          evidence_ref: `prices.azure.com/api/retail/prices stated unitPrice=${str(item.unitPrice)} ${str(item.currencyCode)} per ${str(item.unitOfMeasure)} for ${str(item.meterName)} on ${now}` }));
        if (out.length >= limit) break;
      }
      url = data.NextPageLink;
    }
  } else if (name === "usaspending") {
    for (let page = 1; page <= 20 && out.length < limit; page++) {
      const data = await get(source, source.url, fetcher, robotCache, { filters: { award_type_codes: ["A", "B", "C", "D"] },
        fields: ["Award ID", "Recipient Name", "Award Amount", "Awarding Agency"], limit: 100, page });
      if (!data.results?.length) break;
      for (const item of data.results) {
        const amount = positive(item["Award Amount"]);
        if (amount === null) continue;
        out.push(row(source, { publisher: str(item["Awarding Agency"] || "us-federal"),
          item: `${str(item["Award ID"])} to ${str(item["Recipient Name"])}`, amount, currency: "USD", unit: "award", region: "US",
          evidence_ref: `api.usaspending.gov spending_by_award stated Award Amount=${str(item["Award Amount"])} for ${str(item["Award ID"])} on ${now}` }));
        if (out.length >= limit) break;
      }
    }
  } else if (name === "openprices") {
    for (let page = 1; page <= 40 && out.length < limit; page++) {
      const data = await get(source, `${source.url}?size=100&page=${page}`, fetcher, robotCache);
      if (!data.items?.length) break;
      for (const item of data.items) {
        const amount = positive(item.price);
        if (amount === null) continue;
        out.push(row(source, { publisher: str(item.location?.osm_name || "a shop"),
          item: str(item.product_name || item.product_code), amount, currency: str(item.currency), unit: "item",
          region: str(item.location?.osm_address_country),
          evidence_ref: `prices.openfoodfacts.org/api/v1/prices stated price=${str(item.price)} ${str(item.currency)} for ${str(item.product_code)} on ${now}` }));
        if (out.length >= limit) break;
      }
    }
  } else if (name === "nadac") {
    for (let offset = 0; offset < 20000 && out.length < limit; offset += 500) {
      const data = await get(source, `${source.url}?limit=500&offset=${offset}`, fetcher, robotCache);
      if (!data.results?.length) break;
      for (const item of data.results) {
        const amount = positive(item.nadac_per_unit);
        if (amount === null) continue;
        out.push(row(source, { publisher: "us-medicaid", item: `${str(item.ndc_description)} (${str(item.ndc)})`, amount,
          currency: "USD", unit: "unit", region: "US",
          evidence_ref: `data.medicaid.gov NADAC stated nadac_per_unit=${str(item.nadac_per_unit)} for NDC ${str(item.ndc)} effective ${str(item.effective_date)}, read ${now}` }));
        if (out.length >= limit) break;
      }
    }
  } else if (name === "wikidata") {
    const query = `PREFIX wdt: <http://www.wikidata.org/prop/direct/> PREFIX p: <http://www.wikidata.org/prop/> PREFIX psv: <http://www.wikidata.org/prop/statement/value/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> SELECT ?item ?name ?amount ?unit WHERE { ?item p:P2284 ?st . ?st psv:P2284 ?v . ?v <http://wikiba.se/ontology#quantityAmount> ?amount ; <http://wikiba.se/ontology#quantityUnit> ?unit . ?item rdfs:label ?name . FILTER(LANG(?name)="en") } LIMIT ${limit}`;
    const url = `${source.url}?${new URLSearchParams({ query })}`;
    const data = await get(source, url, fetcher, robotCache, null, "application/sparql-results+json");
    for (const item of data.results?.bindings ?? []) {
      const value = key => str(item[key]?.value);
      const amount = positive(value("amount"));
      if (amount === null) continue;
      out.push(row(source, { publisher: "wikidata", item: value("name"), amount, currency: last(value("unit")),
        unit: "stated", region: "", evidence_ref: `wikidata P2284 via qlever stated ${value("amount")} for ${value("name")} (${last(value("item"))}) on ${now}` }));
    }
  }
  return out;
}

const schema = `CREATE TABLE IF NOT EXISTS observed_price (
 id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, publisher TEXT NOT NULL,
 item TEXT NOT NULL, amount REAL NOT NULL, currency TEXT NOT NULL, unit TEXT NOT NULL,
 region TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL, evidence_ref TEXT NOT NULL,
 observed_at TEXT NOT NULL, UNIQUE(source, publisher, item, unit, region, amount));
 CREATE INDEX IF NOT EXISTS idx_price_kind ON observed_price(kind, publisher);`;

export async function collect({ sources = Object.keys(policy.sources).sort(), limit = 2000,
  store = join(homedir(), ".bonfyre/estate-local/prices.db"), write = false, fetcher = fetch, now = date() } = {}) {
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("limit must be a positive safe integer");
  const db = write ? new Database(store, { create: true }) : null;
  if (db) db.exec(schema);
  const tally = {}, refused = {};
  try {
    const results = new Array(sources.length);
    let next = 0;
    const worker = async () => {
      for (;;) {
        const index = next++;
        if (index >= sources.length) return;
        try { results[index] = { rows: await observe(sources[index], limit, fetcher, now) }; }
        catch (error) { results[index] = { error: str(error.message).slice(0, 140) }; }
      }
    };
    await Promise.all(Array.from({ length: Math.min(policy.max_parallel_sources, sources.length) }, worker));
    const insert = db?.query(`INSERT OR IGNORE INTO observed_price
      (source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)`);
    const observed_at = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    for (let index = 0; index < sources.length; index++) {
      const name = sources[index], result = results[index];
      if (result.error) { refused[name] = result.error; continue; }
      const rows = result.rows;
      tally[name] = rows.length;
      if (db) {
        try {
          db.transaction(() => { for (const item of rows) insert.run(item.source, item.publisher, item.item,
            item.amount, item.currency, item.unit, item.region, item.kind, item.evidence_ref, observed_at); })();
        } catch (error) { delete tally[name]; refused[name] = str(error.message).slice(0, 140); }
      }
    }
    return { observed: tally, sources_that_refused: refused,
      held_in_store: db ? db.query("SELECT COUNT(*) AS n FROM observed_price").get().n : 0,
      store: write ? store : "(not written)", cannot_infer: policy.cannot_infer };
  } finally { db?.close(); }
}

if (import.meta.main) {
  try {
    const sources = [], args = process.argv.slice(2);
    let limit = 2000, store = join(homedir(), ".bonfyre/estate-local/prices.db"), write = false;
    for (let i = 0; i < args.length; i++) {
      if (args[i] === "--source") sources.push(args[++i]);
      else if (args[i] === "--limit") limit = Number(args[++i]);
      else if (args[i] === "--store") store = args[++i];
      else if (args[i] === "--write") write = true;
      else throw new Error(`unknown argument: ${args[i]}`);
    }
    console.log(JSON.stringify(await collect({ sources: sources.length ? sources : undefined, limit, store, write }), null, 1));
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
