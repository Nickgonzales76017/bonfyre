#!/usr/bin/env bun
// Catalogue members are virtual claims from retained observed catalogues.
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { observePublic } from "./foreign_observation.mjs";

const seeds = [
  { name: "Dallas Open Data", kind: "socrata", url: "https://www.dallasopendata.com", place: "Dallas" },
  { name: "Texas Open Data Portal", kind: "socrata", url: "https://data.texas.gov", place: "Texas" },
  { name: "Austin Open Data", kind: "socrata", url: "https://data.austintexas.gov", place: "Austin" },
  { name: "Fort Worth Open Data", kind: "dcat", url: "https://data.fortworthtexas.gov/api/feed/dcat-us/1.1.json", place: "Fort Worth" },
  { name: "North Central Texas COG open data", kind: "dcat", url: "https://data-nctcoggis.opendata.arcgis.com/api/feed/dcat-us/1.1.json", place: "North Texas" },
  { name: "City of Dallas GIS services", kind: "arcgis", url: "https://gis.dallascityhall.com/arcgis/rest/services", place: "Dallas" },
  { name: "Tarrant County GIS services", kind: "arcgis", url: "https://gis.tarrantcounty.com/arcgis/rest/services", place: "Tarrant County" },
  { name: "Texas Natural Resources GIS", kind: "arcgis", url: "https://gis.tnris.org/arcgis/rest/services", place: "Texas" },
  { name: "Houston Open Data", kind: "socrata", url: "https://data.houstontx.gov", place: "Houston" },
  { name: "San Antonio Open Data", kind: "dcat", url: "https://data.sanantonio.gov/api/feed/dcat-us/1.1.json", place: "San Antonio" },
  { name: "HealthData.gov", kind: "dcat", url: "https://healthdata.gov/data.json", place: "United States" },
  { name: "CDC open data", kind: "socrata", url: "https://data.cdc.gov", place: "United States" },
  { name: "US Census Bureau data services", kind: "arcgis", url: "https://tigerweb.geo.census.gov/arcgis/rest/services", place: "United States" },
  { name: "data.gov", kind: "ckan", url: "https://catalog.data.gov", place: "United States", query: "Texas", skip: true },
];
const json = text => { try { return JSON.parse(text); } catch { return null; } };

export function dcatRows(text, seed, base, limit) {
  const doc = json(text), datasets = doc?.dataset ?? doc?.datasets ?? [];
  if (!Array.isArray(datasets)) return [];
  const rows = [];
  for (const entry of datasets.slice(0, limit)) {
    if (!entry || typeof entry !== "object") continue;
    let picked = "";
    for (const dist of entry.distribution ?? []) {
      const address = dist?.downloadURL || dist?.accessURL || "";
      const format = String(dist?.format || dist?.mediaType || "").toLowerCase();
      if (address && ["json", "csv", "geojson"].some(token => format.includes(token))) { picked = address; break; }
    }
    const address = picked || entry.landingPage || "";
    if (!String(address).startsWith("http")) continue;
    rows.push({ address, title: String(entry.title ?? "").slice(0, 400), material: "dataset",
      protocol: address.includes("/api/views/") || address.includes("/resource/") ? "socrata" : "dataset_file",
      state: "candidate", virtual: true, name: seed.name, kind: seed.place ?? "", rights: entry.license ?? "",
      cadence: entry.accrualPeriodicity ?? "", discovered_from: `${seed.name} catalogue at ${base}` });
  }
  return rows;
}

function evidenceAt(path, entry) { appendFileSync(path, JSON.stringify(entry) + "\n"); }
async function readCatalogue(seed, url, args, observeFn) {
  const observation = await observeFn(args.project, args.foreign, `catalogue-${createHash("sha256").update(url).digest("hex").slice(0, 16)}`, url,
    { maxBytes: 24_000_000, timeout: 120000 });
  evidenceAt(args.evidence, { catalogue: seed.name, url, http_status: observation.receipt.http_status ?? null,
    policy: observation.receipt.policy_decision ?? null, receipt: observation.receiptPath });
  return observation;
}

export async function enumerateSeed(seed, args, { observeFn = observePublic, sleepFn = Bun.sleep } = {}) {
  const base = seed.url.replace(/\/+$/, ""), limit = args.perCatalogue;
  if (seed.kind === "socrata" || seed.kind === "dcat") {
    const url = seed.kind === "socrata" ? `${base}/data.json` : seed.url;
    const answer = await readCatalogue(seed, url, args, observeFn);
    return dcatRows(answer.text, seed, base, limit);
  }
  if (seed.kind === "ckan") {
    const rows = [];
    for (let start = 0; rows.length < limit; start += 1000) {
      const url = `${base}/api/3/action/package_search?q=${encodeURIComponent(seed.query || "*:*")}&rows=${Math.min(1000, limit - rows.length)}&start=${start}`;
      const answer = await readCatalogue(seed, url, args, observeFn);
      const results = json(answer.text)?.result?.results;
      if (!Array.isArray(results) || !results.length) break;
      for (const entry of results) for (const resource of entry.resources ?? []) {
        const address = resource?.url ?? "", format = String(resource?.format ?? "").toLowerCase();
        if (!address.startsWith("http") || !["csv", "json", "geojson", "api", "rss", "xml"].includes(format)) continue;
        rows.push({ address, title: String(entry.title ?? "").slice(0, 400), material: "dataset", protocol: "ckan",
          state: "candidate", virtual: true, name: seed.name, kind: seed.place ?? "", rights: entry.license_title ?? "",
          discovered_from: `${seed.name} catalogue at ${base}` });
        break;
      }
      await sleepFn(500);
    }
    return rows.slice(0, limit);
  }
  if (seed.kind === "arcgis") {
    const rows = [], folders = [""], visited = new Set();
    while (folders.length && rows.length < limit) {
      const folder = folders.shift();
      if (visited.has(folder)) continue;
      visited.add(folder);
      const url = `${base}${folder ? `/${folder}` : ""}?f=json`;
      const answer = await readCatalogue(seed, url, args, observeFn);
      const doc = json(answer.text);
      if (!doc || typeof doc !== "object") continue;
      for (const sub of (doc.folders ?? []).slice(0, 40)) folders.push(`${folder}/${sub}`.replace(/^\/+|\/+$/g, ""));
      for (const service of doc.services ?? []) {
        if (rows.length >= limit) break;
        if (!service?.name || !service?.type) continue;
        rows.push({ address: `${base}/${service.name}/${service.type}`, title: service.name.split("/").at(-1),
          material: "dataset", protocol: "arcgis", state: "candidate", virtual: true, name: seed.name,
          kind: seed.place ?? "", discovered_from: `${seed.name} services directory` });
      }
      await sleepFn(300);
    }
    return rows.slice(0, limit);
  }
  return [];
}

function options(argv) {
  const args = { evidence: "/dev/null", perCatalogue: 2000 };
  const keys = { "--project": "project", "--foreign": "foreign", "--out": "out", "--evidence": "evidence",
    "--seeds": "seeds", "--per-catalogue": "perCatalogue", "--only": "only" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["project", "foreign", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.perCatalogue = Number(args.perCatalogue);
  if (!Number.isInteger(args.perCatalogue) || args.perCatalogue < 0) throw new Error("invalid --per-catalogue");
  return args;
}

export async function expand(args, deps = {}) {
  const extra = args.seeds && existsSync(args.seeds) ? JSON.parse(readFileSync(args.seeds, "utf8")) : [];
  const selected = [...seeds, ...extra].filter(seed => !seed.skip && (!args.only || seed.url.includes(args.only)));
  const rows = [], totals = {};
  for (const seed of selected) {
    let members = [];
    try { members = await enumerateSeed(seed, args, deps); }
    catch (error) { evidenceAt(args.evidence, { catalogue: seed.name, error: String(error).slice(0, 300) }); }
    rows.push({ address: seed.url, name: seed.name, protocol: seed.kind, material: "catalogue",
      state: members.length ? "eligible" : "candidate", kind: seed.place ?? "",
      refusal: members.length ? "" : "the catalogue answered nothing this run" });
    rows.push(...members);
    totals[seed.name] = members.length;
    evidenceAt(args.evidence, { catalogue: seed.name, members: members.length });
  }
  writeFileSync(args.out, rows.map(row => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : ""));
  return { catalogues: selected.length, virtual_channels: Object.values(totals).reduce((sum, count) => sum + count, 0), by_catalogue: totals };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await expand(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
