#!/usr/bin/env bun
// Candidate addresses become census rows only after ForeignTwin observation,
// parser and freshness checks. Refusals stay on the SourceFrontier channel.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { declaredFeeds, observe } from "./sweep_root_grammars.mjs";

const item = /<(item|entry)\b/gi;
const title = /<title[^>]*>(.*?)<\/title>/gis;
const language = /<language[^>]*>([^<]+)<\/language>|xml:lang=["']([^"']+)["']/i;
const date = /<(pubDate|published|updated|dc:date)[^>]*>([^<]+)</gi;
const event = /BEGIN:VEVENT/gi;
const starts = /^DTSTART[^:\r\n]*:([0-9TZ]+)/gm;
const comments = /<title[^>]*>\s*(?:<!\[CDATA\[)?\s*Comments on[: ]/i;
const familyOfMaterial = { event: "events", publication: "news", conversation: "forums", photo: "images" };
const englishWords = new Set(["the", "and", "of", "to", "in", "for", "on", "with", "is", "are", "at",
  "from", "after", "over", "how", "why", "new", "says", "as", "by", "a"]);

function options(argv) {
  const args = { limit: 40, workers: 6, freshnessDays: 120, cms: "", evidence: "", onlyLabel: "", channelIdFile: "", discovered: "" };
  const keys = { "--db": "db", "--frontier": "frontier", "--project": "project", "--foreign": "foreign", "--out": "out",
    "--limit": "limit", "--workers": "workers", "--freshness-days": "freshnessDays", "--cms": "cms", "--evidence": "evidence",
    "--only-label": "onlyLabel", "--channel-id-file": "channelIdFile", "--discovered": "discovered" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["db", "frontier", "project", "foreign", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  for (const key of ["limit", "workers", "freshnessDays"]) {
    args[key] = Number(args[key]);
    if (!Number.isInteger(args[key]) || args[key] < 0) throw new Error(`invalid ${key}`);
  }
  return args;
}

function hostOf(address) {
  try { return new URL(address).hostname.toLowerCase().replace(/^www\./, ""); }
  catch { return ""; }
}
function sourceId(url, protocol) {
  const host = hostOf(url).replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 40);
  return `${protocol}-${host}-${createHash("sha256").update(url).digest("hex").slice(0, 6)}`;
}

// Each family reads by its own document's rules.  A JSON Feed is items with
// titles and dates like any other feed; dates are compared as moments, never
// as strings; a feed that states no readable date cannot show it is current.
function newestOf(stamps) {
  let newest = NaN, readable = 0;
  for (const raw of stamps) {
    const when = Date.parse(String(raw).trim());
    if (!Number.isFinite(when)) continue;
    readable++;
    if (!(when <= newest)) newest = when;
  }
  return { newest, readable };
}
function judgeCurrent(stamps, freshnessDays, clock) {
  const { newest, readable } = newestOf(stamps);
  if (!readable) return "no item says when it was published";
  const age = (clock - newest) / 86400000;
  if (age > freshnessDays) return `nothing published in ${Math.floor(age)} days`;
  return null;
}
function judgeJsonFeed(text, freshnessDays, clock) {
  let doc;
  try { doc = JSON.parse(text); } catch { return [null, "not a feed document"]; }
  const items = Array.isArray(doc?.items) ? doc.items : null;
  if (!items || !/jsonfeed\.org\/version/.test(String(doc.version || ""))) return [null, "not a feed document"];
  if (items.length < 3) return [null, "fewer than three items"];
  const titled = items.filter(entry => typeof entry?.title === "string" && entry.title.trim());
  if (titled.length < 3) return [null, "most items have no title"];
  const stale = judgeCurrent(items.map(entry => entry?.date_published || entry?.date_modified || ""), freshnessDays, clock);
  if (stale) return [null, stale];
  return [{ items: items.length, language: String(doc.language || "unstated").toLowerCase(), format: "jsonfeed",
    channel_title: String(doc.title || titled[0].title).replace(/\s+/g, " ").trim().slice(0, 120) }, null];
}

export function judgeFeed(text, freshnessDays, clock = Date.now()) {
  if (!text.trim()) return [null, "answered nothing"];
  const first = text.trimStart()[0];
  if (first === "{") return judgeJsonFeed(text, freshnessDays, clock);
  if (first !== "<") return [null, "not a feed document"];
  if (comments.test(text)) return [null, "a comment thread on one article, not a channel"];
  const items = [...text.matchAll(item)];
  if (items.length < 3) return [null, "fewer than three items"];
  const titles = [...text.matchAll(title)].map(match => match[1]);
  if (titles.length < 3) return [null, "most items have no title"];
  const lang = language.exec(text);
  const tongue = (lang?.[1] ?? lang?.[2] ?? "").trim().toLowerCase();
  const stale = judgeCurrent([...text.matchAll(date)].slice(0, 80).map(match => match[2]), freshnessDays, clock);
  if (stale) return [null, stale];
  return [{ items: items.length, language: tongue || "unstated",
    channel_title: titles[0].replace(/<!\[CDATA\[|\]\]>|\s+/g, " ").trim().slice(0, 120) }, null];
}

// An event counts as upcoming only when its start is a real date on or after
// today and its organizer has not cancelled it.
function realDay(stamp) {
  const m = /^(\d{4})(\d{2})(\d{2})/.exec(stamp);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const at = new Date(Date.UTC(y, mo - 1, d));
  return at.getUTCFullYear() === y && at.getUTCMonth() === mo - 1 && at.getUTCDate() === d ? m[0] : null;
}
export function judgeCalendar(text, clock = Date.now()) {
  const blocks = text.split(/BEGIN:VEVENT/i).slice(1).map(block => block.split(/END:VEVENT/i)[0]);
  if (blocks.length < 3) return [null, "fewer than three events"];
  const today = new Date(clock).toISOString().slice(0, 10).replaceAll("-", "");
  const upcoming = blocks.filter(block => {
    if (/^STATUS:CANCELLED\s*$/im.test(block)) return false;
    const start = /^DTSTART[^:\r\n]*:([0-9TZ]+)/m.exec(block);
    const day = start && realDay(start[1]);
    return day && day >= today;
  });
  if (!upcoming.length) return [null, "nothing upcoming"];
  return [{ events: blocks.length, upcoming: upcoming.length }, null];
}

function knownFamilies(cmsPath) {
  const known = new Map();
  if (!cmsPath || !existsSync(cmsPath)) return known;
  const db = new Database(cmsPath, { readonly: true });
  try {
    for (const row of db.query("SELECT COALESCE(url,'') AS url, COALESCE(family,'') AS family FROM internet_source WHERE status='published' AND url LIKE 'http%'").all()) {
      const host = hostOf(row.url);
      if (host && row.family && !known.has(host)) known.set(host, row.family);
    }
  } finally { db.close(); }
  return known;
}

function candidatesOf(args) {
  const exact = args.channelIdFile ? readFileSync(args.channelIdFile, "utf8").split(/\r?\n/).map(x => x.trim()).filter(Boolean) : [];
  if (exact.length > 900) throw new Error("--channel-id-file may contain at most 900 ids per qualifier pass");
  const params = [];
  let sql = "SELECT c.channel_id, c.exact_address, c.protocol, COALESCE(NULLIF(c.material,''),'publication') AS material, "
    + "COALESCE(NULLIF(c.title,''),r.name,r.host) AS name, r.host, COALESCE(NULLIF(c.identity,''),r.identity,'') AS identity "
    + "FROM channel c JOIN root r ON r.root_id=c.root_id WHERE c.state IN ('shadow','candidate') "
    + "AND COALESCE(c.refusal,'')='' AND c.protocol NOT IN ('mcp','x402','graphql','openapi','soap')";
  if (exact.length) { sql += ` AND c.channel_id IN (${exact.map(() => "?").join(",")})`; params.push(...exact); }
  if (args.onlyLabel) {
    sql += " AND EXISTS (SELECT 1 FROM channel_origin o WHERE o.channel_id=c.channel_id AND o.label=?)";
    params.push(args.onlyLabel);
  }
  sql += exact.length ? " LIMIT ?" : " ORDER BY (c.truth_level='observed_link_candidate') DESC, c.first_seen LIMIT ?";
  params.push(exact.length || args.limit);
  const db = new Database(args.db, { readonly: true });
  let rows;
  try { rows = db.query(sql).all(...params); } finally { db.close(); }
  if (exact.length) {
    const rank = new Map(exact.map((id, index) => [id, index]));
    rows.sort((a, b) => (rank.get(a.channel_id) ?? exact.length) - (rank.get(b.channel_id) ?? exact.length));
  }
  return [rows, exact.length];
}

async function mapLimit(items, limit, fn) {
  const output = new Array(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(items.length, Math.max(1, limit)) }, async () => {
    while (next < items.length) { const index = next++; output[index] = await fn(items[index]); }
  }));
  return output;
}

function importStates(frontier, db, label, path) {
  const result = Bun.spawnSync([frontier, "import", "--db", db, "--label", label, "--jsonl", path], { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr) || `SourceFrontier import exited ${result.exitCode}`);
}

export async function qualify(args, { observeFn = observe, sleepFn = Bun.sleep, importFn = importStates } = {}) {
  const known = knownFamilies(args.cms);
  const [candidates, selected] = candidatesOf(args);
  const perHost = new Map();
  for (const row of candidates) {
    if (!perHost.has(row.host)) perHost.set(row.host, []);
    perHost.get(row.host).push(row);
  }
  const answers = (await mapLimit([...perHost.values()], args.workers, async queue => {
    const rows = [];
    for (const candidate of queue) {
      const twin = `qualify-${createHash("sha256").update(candidate.exact_address).digest("hex").slice(0, 16)}`;
      // The wall time of one observation is the cost a cheaper stage would
      // have to beat; it is kept with the outcome it bought.
      const started = performance.now();
      const [receipt, text] = await observeFn(args.project, args.foreign, twin, candidate.exact_address, 60000, 4_000_000);
      // Judged as it arrives and released: a batch holds verdicts, not bodies.
      const looksCalendar = text.slice(0, 4000).trimStart().slice(0, 15).toUpperCase().startsWith("BEGIN:VCALENDAR");
      const readsAs = candidate.protocol === "ics" || looksCalendar ? "ics" : "feed";
      const [reading, reason] = readsAs === "ics" ? judgeCalendar(text) : judgeFeed(text, args.freshnessDays);
      const declared = reason ? declaredFeeds(text, candidate.exact_address) : [];
      rows.push({ candidate, receipt, readsAs, reading, reason, declared, observe_ms: Math.round(performance.now() - started) });
      await sleepFn(400);
    }
    return rows;
  })).flat(1);
  const rows = [];
  const states = [];
  const evidence = [];
  const discovered = [];
  const discoveredSeen = new Set();
  let admitted = 0, refused = 0;
  for (const { candidate, receipt, readsAs, reading, reason, declared: found, observe_ms } of answers) {
    const { exact_address: address, protocol, material, host, identity } = candidate;
    let name = candidate.name;
    const record = { address, channel_id: candidate.channel_id, protocol, http_status: receipt.http_status ?? null,
      policy: receipt.policy_decision ?? null, observe_ms };
    if (receipt.policy_decision !== undefined && receipt.policy_decision !== null && receipt.policy_decision !== "allow") {
      const why = `robots or observe policy: ${receipt.policy_decision}`;
      states.push({ address, state: "candidate", refusal: why });
      record.refused = why; evidence.push(record); refused++;
      continue;
    }
    if (reason) {
      const declared = found.map(([feedAddress, feedProtocol, feedTitle]) =>
        ({ address: feedAddress, protocol: feedProtocol, title: feedTitle }));
      for (const feed of declared) {
        if (discoveredSeen.has(feed.address)) continue;
        discoveredSeen.add(feed.address);
        discovered.push({ root: host, ...feed, material, truth_level: "observed_link_candidate",
          discovered_from: `declared by ${address} in its own markup` });
      }
      const why = declared.length ? `${reason}; it declares ${declared.length} channel${declared.length === 1 ? "" : "s"} of its own` : reason;
      if (declared.length) record.declares = declared.map(feed => feed.address);
      states.push({ address, state: "candidate", refusal: why });
      record.refused = why; evidence.push(record); refused++;
      continue;
    }
    const family = known.get(host) ?? familyOfMaterial[material] ?? (readsAs === "ics" ? "events" : "news");
    name = reading.channel_title || (name.startsWith("http") ? host : name);
    const ident = sourceId(address, readsAs === "ics" ? "events" : "feed");
    rows.push({ family, source: ident, label: (name || host).slice(0, 120), says: `what ${name || host} publishes`,
      note: `${name || host}, read from the channel it publishes`, url: address, twin: ident, tier: "WARM",
      kind: readsAs === "ics" ? "event" : "text", cadence_seconds: readsAs === "ics" ? 21600 : 1800,
      status: "published", reading: readsAs === "ics" ? { extract: "ics:vevent" }
        : reading.format === "jsonfeed" ? { extract: "items", title_field: "title", link_field: "url", image_field: "image", summary_field: "summary" }
        : { extract: "rss:item" },
      publisher_ident: identity, publisher_name: name === host ? "" : name, compiled_by: "channel-qualifier" });
    states.push({ address, state: "eligible", census_source: ident, material, title: name });
    Object.assign(record, reading, { admitted: ident });
    evidence.push(record); admitted++;
  }
  writeFileSync(args.out, JSON.stringify(rows, null, 2));
  if (args.evidence) appendFileSync(args.evidence, evidence.map(row => JSON.stringify(row)).join("\n") + (evidence.length ? "\n" : ""));
  if (states.length) {
    const listing = join(dirname(args.out), "qualified-states.jsonl");
    writeFileSync(listing, states.map(state => JSON.stringify(state)).join("\n") + "\n");
    importFn(args.frontier, args.db, "CHANNEL_QUALIFIER", listing);
  }
  // Always clear an old discovery file: a previous run's publisher links are
  // not discoveries in this run.
  if (args.discovered) {
    writeFileSync(args.discovered, discovered.map(row => JSON.stringify(row)).join("\n") + (discovered.length ? "\n" : ""));
    if (discovered.length) importFn(args.frontier, args.db, "DECLARED_BY_PUBLISHER", args.discovered);
  }
  return { asked: candidates.length, publishers: perHost.size, admitted, refused,
    selected_by_workgraph: selected, declared_channels_found: discovered.length,
    cannot_infer: "an admitted channel is one that answered and parsed, never one whose content was judged true; a declared channel is an address a publisher named, never one this estate has read" };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await qualify(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
