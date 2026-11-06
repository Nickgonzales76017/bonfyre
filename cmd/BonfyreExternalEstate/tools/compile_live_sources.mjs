#!/usr/bin/env bun
// Public identity may suggest a feed; only retained, judged material enters the census.
import { createHash } from "node:crypto";
import { appendFileSync, writeFileSync } from "node:fs";
import { observePublic, discardProbeBody, publishCanonicalObservation } from "../../BonfyreSourceFrontier/tools/foreign_observation.mjs";

const prefixes = "PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
  + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX wikibase: <http://wikiba.se/ontology#> ";
const queries = {
  local: prefixes + `SELECT ?org (SAMPLE(?l) AS ?name) (SAMPLE(?feed) AS ?feedurl) (SAMPLE(?web) AS ?site) (SAMPLE(?ma) AS ?mastodon) (SAMPLE(?tl) AS ?type) (MAX(?n) AS ?links) WHERE {
    VALUES ?county { wd:Q110412 wd:Q111168 wd:Q111174 wd:Q109265 }
    ?org wdt:P131+ ?county ; wikibase:sitelinks ?n ; rdfs:label ?l . FILTER(LANG(?l)="en")
    FILTER NOT EXISTS { ?org wdt:P31 wd:Q5 }
    OPTIONAL { ?org wdt:P1019 ?feed } OPTIONAL { ?org wdt:P856 ?web } OPTIONAL { ?org wdt:P4033 ?ma }
    FILTER(BOUND(?feed) || BOUND(?web) || BOUND(?ma))
    OPTIONAL { ?org wdt:P31 ?t . ?t rdfs:label ?tl . FILTER(LANG(?tl)="en") }
  } GROUP BY ?org HAVING(MAX(?n) >= 8) ORDER BY DESC(?links) LIMIT 150`,
  media: prefixes + `SELECT ?org (SAMPLE(?l) AS ?name) (SAMPLE(?feed) AS ?feedurl) (SAMPLE(?cls) AS ?class) (MAX(?n) AS ?links) WHERE {
    VALUES ?cls { wd:Q11032 wd:Q1110794 wd:Q1616075 wd:Q14350 wd:Q1126006 }
    ?org wdt:P31 ?cls ; wdt:P1019 ?feed ; wdt:P17 wd:Q30 ; wikibase:sitelinks ?n ; rdfs:label ?l . FILTER(LANG(?l)="en")
  } GROUP BY ?org HAVING(MAX(?n) >= 10) ORDER BY DESC(?links) LIMIT 80`,
  texas: prefixes + `SELECT ?org (SAMPLE(?l) AS ?name) (SAMPLE(?feed) AS ?feedurl) (SAMPLE(?web) AS ?site) (SAMPLE(?cls) AS ?class) (MAX(?n) AS ?links) WHERE {
    VALUES ?cls { wd:Q33506 wd:Q3918 wd:Q7075 wd:Q17431399 wd:Q1110794 wd:Q11032 wd:Q1616075 wd:Q14350 }
    ?org wdt:P31 ?cls ; wdt:P131+ wd:Q1439 ; wikibase:sitelinks ?n ; rdfs:label ?l . FILTER(LANG(?l)="en")
    OPTIONAL { ?org wdt:P1019 ?feed } OPTIONAL { ?org wdt:P856 ?web }
    FILTER(BOUND(?feed) || BOUND(?web))
  } GROUP BY ?org HAVING(MAX(?n) >= 6) ORDER BY DESC(?links) LIMIT 120`,
  podcasts: prefixes + `SELECT ?org (SAMPLE(?l) AS ?name) (SAMPLE(?feed) AS ?feedurl) (MAX(?n) AS ?links) WHERE {
    ?org wdt:P31 wd:Q24634210 ; wdt:P1019 ?feed ; wikibase:sitelinks ?n ; rdfs:label ?l . FILTER(LANG(?l)="en")
  } GROUP BY ?org HAVING(MAX(?n) >= 3) ORDER BY DESC(?links) LIMIT 80`,
};
const conventionalFeeds = ["/RSSFeed.aspx?ModID=1&CID=All-newsflash.xml", "/feed/", "/rss.xml", "/news/rss", "/feed", "/rss", "/index.xml", "/atom.xml"];
const newsClasses = new Set(["Q11032", "Q1110794", "Q1616075", "Q14350", "Q1126006"]);
const freshDays = { local: 180, media: 60, podcasts: 365, texas: 180 };
const sha = value => createHash("sha256").update(value).digest("hex");
export function slug(name, qid) {
  const flat = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 40).replace(/-$/, "");
  return `auto-${flat || "source"}-${qid.toLowerCase()}`;
}
export function discoverFeed(html, base) {
  for (const match of html.slice(0, 400_000).matchAll(/<link\b[^>]*>/gi)) {
    const tag = match[0];
    if (!/rel=["']?alternate/i.test(tag) || !/type=["']?application\/(rss|atom)\+xml/i.test(tag)) continue;
    const href = /href=["']([^"']+)/i.exec(tag)?.[1];
    if (!href) continue;
    try {
      const url = new URL(href.replaceAll("&amp;", "&"), base).href;
      if (/^https?:\/\//.test(url) && !url.toLowerCase().includes("comments")) return url;
    } catch {}
  }
  return "";
}

function parseDate(value) {
  const parsed = Date.parse(value.trim());
  return Number.isFinite(parsed) ? parsed : null;
}
export function judge(text, channel, now = Date.now()) {
  const element = /<item[\s>]/.test(text) ? "item" : /<entry[\s>]/.test(text) ? "entry" : "";
  const report = { element: element || null };
  if (!element) return { ...report, refused: "not an RSS or Atom feed" };
  const head = text.slice(0, text.indexOf(`<${element}`));
  if (/<itunes:explicit>\s*(yes|true|explicit)\s*<\/itunes:explicit>/i.test(head))
    return { ...report, refused: "the show declares itself explicit" };
  const language = /<language>\s*([A-Za-z-]+)\s*<\/language>/.exec(head)?.[1]
    ?? /<feed[^>]+xml:lang=["']([A-Za-z-]+)/.exec(head)?.[1] ?? null;
  report.language = language?.toLowerCase() ?? null;
  if (report.language && !report.language.startsWith("en"))
    return { ...report, refused: `the feed is published in ${report.language}` };
  const blocks = [...text.matchAll(new RegExp(`<${element}[\\s>][\\s\\S]*?<\\/${element}>`, "g"))].slice(0, 40).map(match => match[0]);
  let titled = 0, audio = 0, video = 0, pictured = 0;
  const dates = [];
  for (const block of blocks) {
    if (/<title[^>]*>\s*(<!\[CDATA\[)?\s*\S/.test(block)) titled++;
    const date = /<(pubDate|published|updated|dc:date)[^>]*>([^<]+)</.exec(block);
    if (date) {
      const stamp = parseDate(date[2]);
      if (stamp !== null && stamp - now <= 2 * 86400000) dates.push(stamp);
    }
    if (/<enclosure[^>]+type=["']audio\//.test(block)) audio++;
    if (/<enclosure[^>]+type=["']video\//.test(block)) video++;
    if (/<media:(thumbnail|content)|<enclosure[^>]+type=["']image\/|itunes:image/.test(block)) pictured++;
  }
  const newest = dates.length ? Math.max(...dates) : null;
  const ageDays = newest === null ? null : Math.floor((now - newest) / 86400000);
  Object.assign(report, { items: blocks.length, titled, dated: dates.length, newest: newest === null ? null : new Date(newest).toISOString(),
    age_days: ageDays, audio, video, pictured });
  if (blocks.length < 3) report.refused = "fewer than three items";
  else if (titled < 0.8 * blocks.length) report.refused = "too few titled items";
  else if (ageDays === null || ageDays > freshDays[channel]) report.refused = `nothing published in the last ${freshDays[channel]} days`;
  return report;
}

export function rowFor(candidate, feed, report, receipt, now = Date.now()) {
  const { channel, name, qid } = candidate;
  const kind = report.audio >= Math.max(1, Math.floor(report.items / 2)) ? "audio"
    : report.video >= Math.max(1, Math.floor(report.items / 2)) ? "video" : "text";
  const family = channel === "podcasts" ? "podcasts" : channel === "local" ? "local-voices"
    : newsClasses.has(candidate.class) ? "news" : "institutions";
  const source = slug(name, qid);
  return { source, family, kind, label: name.slice(0, 60), says: `what ${name} publishes`.slice(0, 200), twin: source,
    tier: family === "news" ? "WARM" : "COLD", cadence_seconds: family === "news" ? 3600 : family === "local-voices" ? 21600 : 86400,
    url: feed, front_door: "no", reading: { extract: `rss:${report.element}`, title_field: "title" },
    publisher_ident: qid, publisher_name: name.slice(0, 200), compiled_by: "live-source-compiler",
    note: `compiled from Wikidata ${qid} (${channel}) on ${new Date(now).toISOString().slice(0, 10)}: ${report.items} items, newest ${report.age_days} days old, receipt ${(receipt.content_sha256 ?? "").slice(0, 16)}; posts are ${name}'s own statements`.slice(0, 600) };
}

export async function candidates(channels, limit, args, observeFn = observePublic) {
  const found = [], seen = new Set();
  for (const channel of channels) {
    if (!(channel in queries)) throw new Error(`unknown channel: ${channel}`);
    const url = `https://qlever.dev/api/wikidata?query=${encodeURIComponent(queries[channel].replace(/\s+/g, " "))}`;
    const observed = await observeFn(args.project, args.foreign, `live-discovery-${channel}-${sha(url).slice(0, 12)}`, url,
      { maxBytes: 4_000_000, timeout: 180000 });
    let bindings = [];
    try { bindings = JSON.parse(observed.text)?.results?.bindings ?? []; } catch {}
    for (const b of bindings) {
      const qid = b?.org?.value?.split("/").at(-1), name = b?.name?.value;
      if (!/^Q[0-9]+$/.test(qid ?? "") || !name || seen.has(qid)) continue;
      seen.add(qid);
      const c = { channel, qid, name, links: Number(b.links?.value ?? 0) };
      for (const key of ["feedurl", "site", "mastodon", "type"]) if (b[key]?.value) c[key] = b[key].value;
      if (b.class?.value) c.class = b.class.value.split("/").at(-1);
      found.push(c);
    }
  }
  return limit ? found.slice(0, limit) : found;
}

function options(argv) {
  const args = { channels: "local,texas,media,podcasts", limit: 0, pace: 0.5 };
  const keys = { "--project": "project", "--foreign": "foreign", "--out": "out", "--evidence": "evidence",
    "--channels": "channels", "--limit": "limit", "--pace": "pace" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["project", "foreign", "out", "evidence"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.limit = Number(args.limit); args.pace = Number(args.pace);
  if (!Number.isInteger(args.limit) || args.limit < 0 || !Number.isFinite(args.pace) || args.pace < 0) throw new Error("invalid limit or pace");
  return args;
}

export async function compile(args, { candidateFn = candidates, observeFn = observePublic,
  publishFn = publishCanonicalObservation, discardFn = discardProbeBody, sleepFn = Bun.sleep, now = Date.now() } = {}) {
  const rows = [];
  let admitted = 0, refused = 0;
  writeFileSync(args.evidence, "");
  for (const candidate of await candidateFn(args.channels.split(","), args.limit, args, observeFn)) {
    const record = { qid: candidate.qid, name: candidate.name, channel: candidate.channel };
    let feed = candidate.feedurl ?? "";
    if (!feed && candidate.mastodon?.includes("@")) {
      const [user, host] = candidate.mastodon.replace(/^@/, "").split("@");
      if (user && host) { feed = `https://${host}/@${user}.rss`; record.route = "mastodon"; }
    }
    if (!feed && candidate.site) {
      const home = await observeFn(args.project, args.foreign, `probe-home-${sha(candidate.site).slice(0, 16)}`, candidate.site);
      record.homepage = { url: candidate.site, http_status: home.receipt.http_status ?? null, policy: home.receipt.policy_decision ?? null,
        receipt: home.receiptPath };
      feed = discoverFeed(home.text, candidate.site);
      discardFn(home);
      if (feed) record.route = "homepage feed link";
      else if (home.receipt.policy_decision === "allow" && home.receipt.http_status) {
        let origin = "";
        try { origin = new URL(candidate.site).origin; } catch {}
        for (const path of conventionalFeeds) {
          if (!origin) break;
          const guess = `${origin}${path}`;
          const probe = await observeFn(args.project, args.foreign, `probe-feed-${sha(guess).slice(0, 16)}`, guess);
          if (probe.text && /<(rss|feed)[\s>]/.test(probe.text.slice(0, 4000))) {
            feed = guess; record.route = "platform feed path"; discardFn(probe); break;
          }
          discardFn(probe);
          if (probe.receipt.policy_decision != null && probe.receipt.policy_decision !== "allow") break;
          await sleepFn(args.pace * 1000);
        }
      }
      await sleepFn(args.pace * 1000);
    } else if (feed) record.route ??= "wikidata feed";
    if (!feed) { record.refused = "no feed found"; refused++; appendFileSync(args.evidence, JSON.stringify(record) + "\n"); continue; }
    const source = slug(candidate.name, candidate.qid);
    const observed = await observeFn(args.project, args.foreign, source, feed, { maxBytes: 2_000_000, timeout: 120000 });
    Object.assign(record, { feed, http_status: observed.receipt.http_status ?? null,
      policy: observed.receipt.policy_decision ?? null, receipt_sha256: observed.receipt.content_sha256 ?? null,
      receipt: observed.receiptPath });
    if (observed.receipt.policy_decision !== "allow") record.refused = `robots or observe policy: ${observed.receipt.policy_decision ?? "unproven"}`;
    else if (!observed.text) record.refused = `the feed did not answer (${observed.receipt.http_status ?? "unknown"})`;
    else {
      const report = judge(observed.text, candidate.channel, now);
      record.sample = report;
      if (report.refused) record.refused = report.refused;
      else if (!publishFn(observed, args.foreign, source)) record.refused = "sample has no canonical retained receipt and body";
      else { rows.push(rowFor(candidate, feed, report, observed.receipt, now)); record.admitted = source; }
    }
    if (record.refused) { refused++; discardFn(observed); } else admitted++;
    appendFileSync(args.evidence, JSON.stringify(record) + "\n");
    await sleepFn(args.pace * 1000);
  }
  writeFileSync(args.out, JSON.stringify(rows, null, 1));
  const families = {};
  for (const row of rows) families[row.family] = (families[row.family] ?? 0) + 1;
  return { admitted, refused, families };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await compile(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
