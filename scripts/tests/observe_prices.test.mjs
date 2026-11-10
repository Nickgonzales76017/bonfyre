import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { collect, observe, policy } from "../observe-prices.mjs";

const fixture = async (url, options) => {
  const u = new URL(url);
  expect(u.protocol).toBe("https:");
  expect(options.headers["User-Agent"]).toBe(policy.user_agent);
  if (u.pathname === "/robots.txt") return new Response("User-agent: *\nAllow: /\n", { status: 200 });
  let data;
  if (u.hostname === "openrouter.ai") data = { data: [
    { id: "vendor/model", pricing: { prompt: "0.001", completion: "0" } },
    { id: "vendor/free", pricing: { prompt: "-1", completion: "0" } }] };
  else if (u.hostname === "prices.azure.com") data = { Items: [
    { productName: "VM", meterName: "Hour", unitPrice: 2, currencyCode: "USD", unitOfMeasure: "hour", armRegionName: "east" },
    { productName: "Free", meterName: "Hour", unitPrice: 0 }], NextPageLink: null };
  else if (u.hostname === "api.usaspending.gov") {
    expect(options.method).toBe("POST");
    expect(JSON.parse(options.body).filters.award_type_codes).toEqual(["A", "B", "C", "D"]);
    data = { results: [{ "Award ID": "A1", "Recipient Name": "Builder", "Award Amount": 20,
      "Awarding Agency": "Agency" }] };
  } else if (u.hostname === "prices.openfoodfacts.org") data = { items: [{ price: 4, currency: "EUR",
    product_name: "Bread", product_code: "123", location: { osm_name: "Shop", osm_address_country: "FR" } }] };
  else if (u.hostname === "data.medicaid.gov") data = { results: [{ nadac_per_unit: 5, ndc_description: "Drug", ndc: "N1", effective_date: "2026-01-01" }] };
  else if (u.hostname === "qlever.dev") {
    expect(u.searchParams.get("query")).toContain("p:P2284");
    data = { results: { bindings: [{ item: { value: "http://www.wikidata.org/entity/Q1" },
      name: { value: "Thing" }, amount: { value: "7" }, unit: { value: "http://www.wikidata.org/entity/Q4917" } }] } };
  } else throw new Error("unadmitted fixture URL");
  return new Response(JSON.stringify(data), { status: 200, headers: { "Content-Type": "application/json" } });
};

test("six publisher families lower to dated statements without zero-as-free or guessed currency", async () => {
  const names = Object.keys(policy.sources).sort();
  expect(names).toHaveLength(6);
  for (const name of names) {
    const rows = await observe(name, 1, fixture, "2026-09-29");
    expect(rows).toHaveLength(1);
    expect(rows[0].amount).toBeGreaterThan(0);
    expect(rows[0].evidence_ref).toContain("2026-09-29");
  }
  expect((await observe("wikidata", 1, fixture, "2026-09-29"))[0].currency).toBe("Q4917");
  expect((await observe("openrouter", 1, fixture, "2026-09-29"))[0].unit).toBe("token in");
});

test("the collector persists publisher claims idempotently for WebAddress and keeps refusals explicit", async () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-prices-native-")), store = join(base, "prices.db");
  try {
    const first = await collect({ store, write: true, limit: 1, now: "2026-09-29", fetcher: fixture });
    expect(first.sources_that_refused).toEqual({});
    expect(first.observed).toEqual({ azure: 1, nadac: 1, openprices: 1, openrouter: 1, usaspending: 1, wikidata: 1 });
    expect(first.held_in_store).toBe(6);
    const second = await collect({ store, write: true, limit: 1, now: "2026-09-29", fetcher: fixture });
    expect(second.held_in_store).toBe(6);
    const db = new Database(store, { readonly: true });
    expect(db.query("SELECT COUNT(DISTINCT kind) AS n FROM observed_price").get().n).toBe(6);
    expect(db.query("SELECT currency FROM observed_price WHERE kind='catalogued'").get().currency).toBe("Q4917");
    db.close();
    const failed = await collect({ sources: ["azure"], fetcher: async () => { throw new Error("network refused"); } });
    expect(failed.sources_that_refused.azure).toBe("network refused");
    expect(failed.held_in_store).toBe(0);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("unadmitted pagination origin is refused before a second fetch", async () => {
  let reads = 0;
  await expect(observe("azure", 2, async url => {
    reads++;
    if (new URL(url).pathname === "/robots.txt") return new Response("User-agent: *\nAllow: /\n", { status: 200 });
    return new Response(JSON.stringify({ Items: [{ unitPrice: 1 }], NextPageLink: "https://other.example/prices" }), { status: 200 });
  })).rejects.toThrow("left its admitted origin");
  expect(reads).toBe(2);
});

test("robots refusal prevents price requests and never becomes a zero price", async () => {
  let reads = 0;
  await expect(observe("openrouter", 1, async () => {
    reads++;
    return new Response("User-agent: *\nDisallow: /api/\n", { status: 200 });
  })).rejects.toThrow("robots policy refuses");
  expect(reads).toBe(1);
});

test("independent publisher requests run concurrently but receipts retain source order", async () => {
  let arrivals = 0, release;
  const gate = new Promise(resolve => { release = resolve; });
  const fetcher = async url => {
    const target = new URL(url);
    if (target.pathname === "/robots.txt") return new Response("User-agent: *\nAllow: /\n");
    arrivals++;
    if (arrivals === 2) release();
    await gate;
    return new Response(JSON.stringify(target.hostname === "openrouter.ai"
      ? { data: [{ id: "one/model", pricing: { prompt: "1" } }] }
      : { Items: [{ productName: "VM", meterName: "Hour", unitPrice: 2 }], NextPageLink: null }));
  };
  const result = await Promise.race([
    collect({ sources: ["openrouter", "azure"], limit: 1, fetcher }),
    new Promise((_, reject) => setTimeout(() => reject(new Error("publisher requests serialized")), 1000)),
  ]);
  expect(arrivals).toBe(2);
  expect(Object.keys(result.observed)).toEqual(["openrouter", "azure"]);
});

test("an oversized publisher JSON response is refused before parsing", async () => {
  const fetcher = async url => new URL(url).pathname === "/robots.txt"
    ? new Response("User-agent: *\nAllow: /\n")
    : new Response("{}", { headers: { "Content-Length": "999999999" } });
  await expect(observe("openrouter", 1, fetcher)).rejects.toThrow("response exceeds");
});
