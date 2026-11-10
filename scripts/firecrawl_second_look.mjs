#!/usr/bin/env bun
// A rendered second look is a bounded, metered question to a previously silent
// publisher. Links remain candidates; rendering never admits a source.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

const endpoint = "https://api.firecrawl.dev/v1/scrape";
const worthAsking = /(\/feed\b|\/rss\b|\.rss\b|atom\.xml|feed\.xml|index\.xml|feed\.json|data\.json|\/api\/3\/action\/|\/api\/views|\/arcgis\/rest|\/api\/v3\/datasets|openapi\.json|swagger\.json|\/v3\/api-docs|\.ics\b|oai\?verb=)/i;
const notWorth = /(\.jpg|\.png|\.gif|\.pdf|\.zip|\/login|\/signin|\/cart|facebook\.com|twitter\.com|x\.com\/|linkedin\.com)/i;

function stamp() { return new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z"); }
function now() { return new Date().toISOString().replace(/\.\d+Z$/, "Z"); }
function hostOf(address) {
  try { return new URL(address).hostname.toLowerCase().replace(/\.$/, "").replace(/^www\./, ""); }
  catch { return ""; }
}
function options(argv) {
  const args = { estate: join(process.env.HOME ?? "", ".bonfyre/estate-local"), limit: 20, out: "", apply: false };
  const keys = { "--estate": "estate", "--limit": "limit", "--out": "out" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--apply") { args.apply = true; continue; }
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  args.limit = Number(args.limit);
  if (!Number.isInteger(args.limit) || args.limit < 0) throw new Error("invalid --limit");
  return args;
}

export async function renderedLinks(key, address, fetchFn = fetch) {
  try {
    const answer = await fetchFn(endpoint, { method: "POST", headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url: address, formats: ["links"], onlyMainContent: false }), signal: AbortSignal.timeout(70000) });
    if (answer.status === 402) return { links: null, why: "payment required", paymentRequired: true };
    if (!answer.ok) return { links: null, why: `refused ${answer.status}` };
    const doc = await answer.json();
    if (!doc.success) return { links: null, why: String(doc.error).slice(0, 80) };
    return { links: doc.data?.links ?? [], why: "" };
  } catch (error) { return { links: null, why: String(error.message).slice(0, 80) }; }
}

export async function secondLook(args, { env = process.env, renderFn = renderedLinks } = {}) {
  if (env.BONFYRE_EXTERNAL_READS_ENABLED !== "1") return { asked: 0, why: "this estate's governance has external reads switched off" };
  const key = (env.FIRECRAWL_API_KEY2 ?? "").trim() || (env.FIRECRAWL_API_KEY ?? "").trim();
  if (!key) return { asked: 0, why: "no Firecrawl credential in this environment" };
  const askedDb = join(args.estate, "probe", "asked.db");
  if (!existsSync(askedDb)) return { asked: 0, why: "nothing has been probed yet, so nothing has answered nothing" };
  const state = new Database(askedDb);
  try {
    state.exec("CREATE TABLE IF NOT EXISTS looked_again(host TEXT PRIMARY KEY, at TEXT NOT NULL, links INTEGER NOT NULL DEFAULT 0, why TEXT NOT NULL DEFAULT '')");
    const hosts = state.query("SELECT host FROM asked WHERE answered=0 AND host NOT IN (SELECT host FROM looked_again) ORDER BY at LIMIT ?").all(args.limit).map(row => row.host);
    if (!hosts.length) return { asked: 0, why: "every host that answered nothing has already been looked at again" };
    if (!args.apply) return { would_ask: hosts, credits: hosts.length };
    const outPath = args.out || join(args.estate, "discovery", `rendered-${stamp()}.jsonl`);
    mkdirSync(dirname(outPath), { recursive: true });
    const seen = new Set();
    const rows = [];
    let refused = 0;
    let attempted = 0;
    let providerState = "ready";
    const record = state.query("INSERT OR REPLACE INTO looked_again(host,at,links,why) VALUES(?,?,?,?)");
    for (const host of hosts) {
      attempted++;
      const result = await renderFn(key, `https://${host}/`);
      if (result.paymentRequired) { providerState = "payment_required"; break; }
      if (!result.links) {
        refused++;
        record.run(host, now(), 0, result.why ?? "refused");
        continue;
      }
      let kept = 0;
      for (const raw of result.links) {
        const link = String(raw ?? "").trim();
        if (!/^https?:\/\//i.test(link) || notWorth.test(link) || !worthAsking.test(link) || seen.has(link)) continue;
        const target = hostOf(link);
        if (!target) continue;
        seen.add(link);
        rows.push({ root: target, address: link, protocol: "unknown", title: "", truth_level: "observed_link_candidate",
          discovered_from: `linked by ${host} on a page this estate could only read rendered` });
        kept++;
      }
      record.run(host, now(), kept, "");
    }
    writeFileSync(outPath, rows.map(row => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : ""));
    return { hosts_asked: attempted, credit_requests: attempted, billed_credits: "unmeasured", refused,
      addresses_found: rows.length, out: outPath, provider_state: providerState,
      cannot_infer: "a rendered-page link is not an admitted channel; an attempted metered request does not prove a charge, and rendering grants no permission beyond the source's robots policy" };
  } finally { state.close(); }
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await secondLook(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
