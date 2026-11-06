#!/usr/bin/env bun
// Calendar conventions are questions. A census row needs a fresh canonical body.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { observePublic, discardProbeBody, publishCanonicalObservation } from "./foreign_observation.mjs";

const paths = [
  ["/calendar.ics", "Localist"], ["/events.ics", "Localist"], ["/calendar/ical", "Drupal"], ["/events/feed/ical", "Drupal"],
  ["/events/?ical=1", "The Events Calendar"], ["/?post_type=tribe_events&ical=1", "The Events Calendar"],
  ["/common/modules/iCalendar/iCalendar.aspx?feed=calendar", "CivicPlus"],
  ["/RSSFeed.aspx?ModID=58&CID=All-calendar.xml", "CivicPlus"], ["/events/upcoming.ics", "LibraryMarket"],
  ["/calendar/export/ics", "Trumba"],
];

export function judge(text, today = new Date().toISOString().slice(0, 10).replaceAll("-", "")) {
  if (!text.includes("BEGIN:VCALENDAR")) return [null, "not an iCalendar document"];
  const events = [...text.matchAll(/BEGIN:VEVENT([\s\S]*?)END:VEVENT/g)].map(match => match[1]);
  if (events.length < 3) return [null, "fewer than three events"];
  let upcoming = 0, titled = 0, latest = "";
  for (const block of events) {
    const start = /^DTSTART[^:\r\n]*:([0-9TZ]+)/m.exec(block);
    const title = /^SUMMARY[^:\r\n]*:(.+)$/m.exec(block);
    if (title?.[1].trim()) titled++;
    if (start) {
      const stamp = start[1].slice(0, 8);
      if (stamp > latest) latest = stamp;
      if (stamp >= today) upcoming++;
    }
  }
  if (!upcoming) return [null, `nothing upcoming (latest ${latest || "unknown"})`];
  if (titled < Math.floor(events.length / 2)) return [null, "most events have no title"];
  return [{ events: events.length, upcoming, latest }, null];
}

function sqlRows(path, sql) {
  if (!path || !existsSync(path)) return [];
  const db = new Database(path, { readonly: true });
  try { return db.query(sql).all(); }
  catch (error) { if (String(error.message).includes("no such table") || String(error.message).includes("no such column")) return []; throw error; }
  finally { db.close(); }
}
export function candidateHosts(args) {
  const hosts = [], seen = new Set();
  const add = (host, name = "", identity = "") => {
    const key = String(host ?? "").trim();
    if (!key || seen.has(key)) return;
    seen.add(key); hosts.push({ host: key, name: name ?? "", identity: identity ?? "" });
  };
  if (args.hosts && existsSync(args.hosts)) for (const line of readFileSync(args.hosts, "utf8").split(/\r?\n/)) {
    const [host, name, identity] = line.trim().split("|").map(part => part.trim());
    add(host, name, identity);
  }
  for (const r of sqlRows(args.cms, "SELECT COALESCE(url,'') AS url, COALESCE(publisher_name,'') AS name, "
    + "COALESCE(publisher_ident,'') AS identity FROM internet_source WHERE status='published' AND url LIKE 'http%'")) {
    try { add(new URL(r.url).host, r.name, r.identity); } catch {}
  }
  for (const r of sqlRows(args.frontier, "SELECT host, COALESCE(name,'') AS name, COALESCE(identity,'') AS identity "
    + "FROM root WHERE COALESCE(identity,'')<>''")) add(r.host, r.name, r.identity);
  return hosts.slice(0, args.limit);
}

function options(argv) {
  const args = { evidence: "/dev/null", limit: 200 };
  const keys = { "--project": "project", "--foreign": "foreign", "--out": "out", "--channels": "channels", "--cms": "cms",
    "--frontier": "frontier", "--hosts": "hosts", "--evidence": "evidence", "--limit": "limit" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["project", "foreign", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.limit = Number(args.limit);
  if (!Number.isInteger(args.limit) || args.limit < 0) throw new Error("invalid --limit");
  return args;
}

export async function probe(args, { observeFn = observePublic, publishFn = publishCanonicalObservation,
  discardFn = discardProbeBody, sleepFn = Bun.sleep, today } = {}) {
  const hosts = candidateHosts(args), rows = [], channels = [];
  for (const { host, name, identity } of hosts) {
    const sourceId = `events-${host.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48)}`;
    for (const [path, platform] of paths) {
      const url = `https://${host}${path}`;
      const probeTwin = `calendar-${createHash("sha256").update(url).digest("hex").slice(0, 16)}`;
      const observation = await observeFn(args.project, args.foreign, probeTwin, url);
      const record = { host, url, platform, http_status: observation.receipt.http_status ?? null,
        policy: observation.receipt.policy_decision ?? null };
      if (observation.receipt.policy_decision != null && observation.receipt.policy_decision !== "allow") {
        appendFileSync(args.evidence, JSON.stringify(record) + "\n");
        discardFn(observation);
        break;
      }
      const [reading, refusal] = judge(observation.text, today);
      if (refusal) {
        record.refused = refusal;
        appendFileSync(args.evidence, JSON.stringify(record) + "\n");
        discardFn(observation);
        await sleepFn(200);
        continue;
      }
      record.probe = reading;
      discardFn(observation);
      // The source's twin must own a fresh matching receipt and body before a
      // CMS census row can truthfully name it. A probe receipt is not enough.
      const canonical = await observeFn(args.project, args.foreign, sourceId, url);
      const [verified, secondRefusal] = judge(canonical.text, today);
      if (canonical.receipt.policy_decision !== "allow" || secondRefusal || !publishFn(canonical, args.foreign, sourceId)) {
        record.refused = `canonical observation: ${secondRefusal || canonical.receipt.policy_decision || "not retained"}`;
        appendFileSync(args.evidence, JSON.stringify(record) + "\n");
        discardFn(canonical);
        await sleepFn(200);
        continue;
      }
      record.canonical = verified;
      record.canonical_receipt = canonical.receiptPath;
      appendFileSync(args.evidence, JSON.stringify(record) + "\n");
      rows.push({ family: "events", source: sourceId, label: `${name || host} events`, says: `what ${name || host} says is happening`,
        note: `${name || host}, read from the calendar it publishes`, url, twin: sourceId, tier: "WARM", kind: "event",
        cadence_seconds: 21600, status: "published", reading: { extract: "ics:vevent" },
        publisher_ident: identity, publisher_name: name, compiled_by: "event-channel-prober" });
      channels.push({ address: url, name: name || host, identity, protocol: "ics", material: "event", state: "eligible",
        census_source: sourceId, kind: platform, discovered_from: `${platform} calendar convention` });
      break;
    }
    await sleepFn(200);
  }
  writeFileSync(args.out, JSON.stringify(rows, null, 1));
  if (args.channels) writeFileSync(args.channels, channels.map(row => JSON.stringify(row)).join("\n") + (channels.length ? "\n" : ""));
  return { hosts: hosts.length, calendars: rows.length };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await probe(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
