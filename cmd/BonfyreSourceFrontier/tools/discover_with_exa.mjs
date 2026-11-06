#!/usr/bin/env bun
// Exa supplies candidate addresses, never admitted channels or authority.
import { writeFileSync } from "node:fs";

const endpoint = "https://api.exa.ai/search";
const subjectsByFamily = {
  vulnerabilities: [
    "security advisories RSS feed", "vulnerability disclosure RSS feed", "CVE announcements RSS feed",
    "product security bulletin RSS feed", "incident response advisory feed",
    "national cyber security centre advisories RSS", "open source security advisory feed",
    "exploit disclosure mailing list RSS", "vendor patch notes security RSS feed", "threat intelligence blog RSS feed",
  ],
  gaming: [
    "video game news RSS feed", "indie game release RSS feed", "game developer blog RSS feed",
    "game studio patch notes RSS feed", "tabletop game publisher news feed", "games press release RSS feed",
    "esports tournament schedule calendar feed", "game preservation archive RSS feed",
    "game soundtrack release feed", "games criticism RSS feed",
  ],
};
const subjects = [
  "city government news RSS feed", "county government RSS feed", "public library events calendar ics",
  "university news RSS feed", "museum exhibitions RSS feed", "symphony orchestra concert calendar feed",
  "theatre season calendar ics feed", "public radio station podcast RSS feed", "local newspaper RSS feed",
  "neighbourhood association newsletter feed", "school district news RSS feed", "parks and recreation events feed",
  "botanical garden events calendar", "science museum podcast feed", "historical society newsletter RSS",
  "community radio podcast feed", "city council meeting agenda calendar feed", "public health department RSS feed",
  "transit authority service alerts RSS", "state agency newsroom RSS feed", "art gallery exhibition calendar feed",
  "independent bookstore events calendar", "live music venue calendar feed", "farmers market events calendar",
  "observatory public programs calendar", "zoo events calendar feed",
];
const places = ["Fort Worth Texas", "Dallas Texas", "Tarrant County Texas", "Denton Texas", "Arlington Texas", "Plano Texas", "", ""];
const feedish = /(rss|atom|feed|\.ics|\.xml|podcast)/i;
const notAFeed = /(\.jpg|\.png|\.pdf|\.zip|\/login|\/signin|facebook\.com|twitter\.com|x\.com\/)/i;

export class PaymentRequired extends Error {}

export function hostOf(address) {
  try {
    const host = new URL(address).hostname.toLowerCase().replace(/\.$/, "");
    return /^[a-z0-9.-]+\.[a-z]{2,}$/.test(host) ? host.replace(/^www\./, "") : "";
  } catch { return ""; }
}

export async function search(key, query, want, kind, fetchFn = fetch) {
  try {
    const answer = await fetchFn(endpoint, { method: "POST", headers: { "x-api-key": key, "Content-Type": "application/json" },
      body: JSON.stringify({ query, numResults: want, type: kind }), signal: AbortSignal.timeout(60000) });
    if (answer.status === 402) throw new PaymentRequired((await answer.text()).slice(0, 400));
    if (!answer.ok) { console.error(`  refused ${answer.status}`); return []; }
    return (await answer.json()).results ?? [];
  } catch (error) {
    if (error instanceof PaymentRequired) throw error;
    console.error(`  ${String(error.message).slice(0, 80)}`);
    return [];
  }
}

function options(argv) {
  const args = { family: "", subjects: "", queries: 120, perQuery: 50, pause: 0.4 };
  const keys = { "--out": "out", "--family": "family", "--subjects": "subjects", "--queries": "queries",
    "--per-query": "perQuery", "--pause": "pause" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  if (!args.out) throw new Error("missing --out");
  for (const key of ["queries", "perQuery", "pause"]) {
    args[key] = Number(args[key]);
    if (!Number.isFinite(args[key]) || args[key] < 0) throw new Error(`invalid ${key}`);
  }
  return args;
}

export async function discover(args, { env = process.env, searchFn = search, sleepFn = Bun.sleep } = {}) {
  if (env.BONFYRE_EXTERNAL_READS_ENABLED !== "1") return { rows: [], report: { asked: 0, why: "this estate's governance has external reads switched off" }, write: false };
  const key = env.EXAAI_API_KEY ?? "";
  if (!key) return { rows: [], report: { asked: 0, why: "no Exa credential in this environment" }, write: false };
  const ceiling = Number(env.BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN ?? "5000");
  const wanted = Math.min(args.queries, Math.max(1, Math.floor(ceiling / Math.max(1, args.perQuery))));
  const sought = args.subjects ? args.subjects.split(/[\n;]/).map(value => value.trim()).filter(Boolean)
    : subjectsByFamily[args.family] ?? subjects;
  const locations = args.subjects || Object.hasOwn(subjectsByFamily, args.family) ? [""] : places;
  const seen = new Set();
  const rows = [];
  let asked = 0;
  for (const subject of sought) {
    for (const place of locations) {
      if (asked >= wanted) break;
      const query = `${subject} ${place}`.trim();
      asked++;
      let results;
      try { results = await searchFn(key, query, args.perQuery, "keyword"); }
      catch (error) {
        if (!(error instanceof PaymentRequired)) throw error;
        return { rows, write: true, report: {
          queries_asked: asked, addresses_listed: rows.length, provider_state: "payment_required",
          provider_said: error.message.slice(0, 300),
          needs: "this estate's Exa credit is spent; discovery stops here rather than reporting an empty result as though nothing existed to find",
          cannot_infer: "a provider asking to be paid is not this estate agreeing to pay, and nothing here authorizes a payment",
        } };
      }
      let found = 0;
      for (const result of results) {
        const address = String(result.url ?? "").trim();
        if (!/^https?:\/\//i.test(address) || notAFeed.test(address)) continue;
        const host = hostOf(address);
        if (!host) continue;
        const title = String(result.title ?? "").slice(0, 160);
        const channel = feedish.test(address);
        const listed = channel ? address : `https://${host}/`;
        if (seen.has(listed)) continue;
        seen.add(listed);
        rows.push({ root: host, address: listed, protocol: channel ? (address.toLowerCase().endsWith(".ics") ? "ics" : "rss") : "unknown",
          title, truth_level: channel ? "observed_index_candidate" : "listed",
          discovered_from: `a search for ${JSON.stringify(query)} returned this ${channel ? "address" : "publisher"}` });
        found++;
      }
      console.error(`${query.slice(0, 58).padEnd(58)} ${found}`);
      if (args.pause) await sleepFn(args.pause * 1000);
    }
    if (asked >= wanted) break;
  }
  return { rows, write: true, report: { queries_asked: asked, addresses_listed: rows.length,
    publishers: new Set(rows.map(row => row.root)).size,
    cannot_infer: "a search result is an address a search engine returned; it is not a channel this estate reads, not a claim that it answers, and not an endorsement of what it carries" } };
}

if (import.meta.main) {
  try {
    const args = options(process.argv.slice(2));
    const result = await discover(args);
    if (result.write) writeFileSync(args.out, result.rows.map(row => JSON.stringify(row)).join("\n") + (result.rows.length ? "\n" : ""));
    console.log(JSON.stringify(result.report));
  } catch (error) { console.error(error); process.exitCode = 1; }
}
