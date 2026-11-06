import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { judgeCalendar, judgeFeed, qualify } from "../tools/qualify_channels.mjs";

const now = Date.now();
const future = new Date(now + 86400000).toISOString().slice(0, 10).replaceAll("-", "");
const past = new Date(now - 200 * 86400000).toUTCString();
assert.equal(judgeFeed("<rss><item/></rss>", 120)[1], "fewer than three items");
// Language is recorded, never a reason to refuse.
const fresh = new Date(Date.now() - 86400000).toUTCString();
assert.equal(judgeFeed(`<rss><language>fr</language>${`<item><title>Bonjour</title><pubDate>${fresh}</pubDate></item>`.repeat(3)}</rss>`, 120)[0].language, "fr");
// Dates are moments: an old RFC date beside current ones does not age the feed.
assert.equal(judgeFeed(`<rss>${`<item><title>Now</title><pubDate>${fresh}</pubDate></item>`.repeat(2)}<item><title>Then</title><pubDate>Wed, 01 Jan 2020 00:00:00 GMT</pubDate></item></rss>`, 120)[1], null);
// A feed with no readable date cannot show it is current.
assert.equal(judgeFeed(`<rss>${"<item><title>Undated</title></item>".repeat(3)}</rss>`, 120)[1], "no item says when it was published");
assert.equal(judgeFeed(`<rss>${"<item><title>Bad</title><pubDate>sometime soon</pubDate></item>".repeat(3)}</rss>`, 120)[1], "no item says when it was published");
// A JSON Feed qualifies by the same rules.
const jf = JSON.stringify({ version: "https://jsonfeed.org/version/1.1", title: "JSON News", items: Array.from({ length: 3 }, (_, i) => ({ id: String(i), title: `Item ${i}`, url: `https://j.test/${i}`, date_published: new Date(Date.now() - 3600000).toISOString() })) });
assert.equal(judgeFeed(jf, 120)[0].format, "jsonfeed");
// Cancelled and impossible events are not upcoming.
const soon = new Date(Date.now() + 7 * 86400000).toISOString().slice(0, 10).replaceAll("-", "");
assert.equal(judgeCalendar(`BEGIN:VEVENT\nDTSTART:${soon}T120000Z\nSTATUS:CANCELLED\nEND:VEVENT\n`.repeat(3))[1], "nothing upcoming");
assert.equal(judgeCalendar("BEGIN:VEVENT\nDTSTART:20991399T120000Z\nEND:VEVENT\n".repeat(3))[1], "nothing upcoming");
assert.match(judgeFeed(`<rss>${`<item><title>Old news</title><pubDate>${past}</pubDate></item>`.repeat(3)}</rss>`, 120)[1], /nothing published/);
assert.equal(judgeCalendar("BEGIN:VEVENT\nDTSTART:20200101\n".repeat(3))[1], "nothing upcoming");

const temp = mkdtempSync(join(tmpdir(), "bonfyre-qualify-"));
try {
  const frontier = join(temp, "frontier.db");
  const db = new Database(frontier);
  db.exec("CREATE TABLE root(root_id TEXT, host TEXT, name TEXT, identity TEXT); CREATE TABLE channel(channel_id TEXT, root_id TEXT, exact_address TEXT, protocol TEXT, material TEXT, title TEXT, identity TEXT, state TEXT, refusal TEXT, truth_level TEXT, first_seen TEXT); CREATE TABLE channel_origin(channel_id TEXT, label TEXT);");
  const hosts = ["feed.test", "event.test", "page.test", "deny.test"];
  hosts.forEach((host, index) => {
    db.query("INSERT INTO root VALUES(?,?,?,?)").run(String(index), host, host, "");
    db.query("INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?,?,?)").run(String(index), String(index), `https://${host}/`,
      index === 1 ? "ics" : "unknown", index === 1 ? "event" : "publication", host, "", "candidate", "", "listed", `2026-09-0${index + 1}`);
  });
  db.close();
  const cms = join(temp, "cms.db");
  const census = new Database(cms);
  census.exec("CREATE TABLE internet_source(url TEXT, family TEXT, status TEXT); INSERT INTO internet_source VALUES('https://feed.test/','education','published');");
  census.close();
  const feed = `<rss><channel><language>en</language><title>School News</title>${Array.from({ length: 3 }, (_, i) => `<item><title>The school news ${i}</title><pubDate>${new Date(Date.now() - 86400000).toUTCString()}</pubDate></item>`).join("")}</channel></rss>`;
  const event = `BEGIN:VCALENDAR\n${`BEGIN:VEVENT\nDTSTART:${future}T120000Z\nEND:VEVENT\n`.repeat(3)}END:VCALENDAR`;
  const page = '<html><head><link rel="alternate" type="application/rss+xml" href="/news.xml" title="News &amp; updates"></head></html>';
  const args = { db: frontier, frontier: "/not-used", project: "/not-used", foreign: temp, out: join(temp, "qualified.json"),
    limit: 10, workers: 2, freshnessDays: 120, cms, evidence: join(temp, "evidence.jsonl"), onlyLabel: "",
    channelIdFile: "", discovered: join(temp, "declared.jsonl") };
  const imports = [];
  const result = await qualify(args, { sleepFn: async () => {}, importFn: (...call) => imports.push(call),
    observeFn: async (_project, _foreign, _twin, address) => {
      if (address.includes("deny.test")) return [{ policy_decision: "deny", http_status: 403 }, ""];
      const text = address.includes("feed.test") ? feed : address.includes("event.test") ? event : page;
      return [{ policy_decision: "allow", http_status: 200 }, text];
    } });
  assert.equal(result.asked, 4);
  assert.equal(result.publishers, 4);
  assert.equal(result.admitted, 2);
  assert.equal(result.refused, 2);
  assert.equal(result.declared_channels_found, 1);
  const rows = JSON.parse(readFileSync(args.out, "utf8"));
  assert.deepEqual(rows.map(row => [row.family, row.kind]), [["education", "text"], ["events", "event"]]);
  assert.deepEqual(imports.map(call => call[2]), ["CHANNEL_QUALIFIER", "DECLARED_BY_PUBLISHER"]);
  const discovered = readFileSync(args.discovered, "utf8").trim().split("\n").map(JSON.parse);
  assert.deepEqual(discovered.map(row => [row.address, row.title, row.truth_level]),
    [["https://page.test/news.xml", "News & updates", "observed_link_candidate"]]);
  const evidence = readFileSync(args.evidence, "utf8").trim().split("\n").map(JSON.parse);
  assert.equal(evidence.length, 4);
  assert.ok(evidence.some(row => row.policy === "deny" && row.refused.includes("robots")));

  writeFileSync(args.discovered, "stale previous run\n");
  const empty = await qualify({ ...args, onlyLabel: "absent" }, { sleepFn: async () => {}, importFn: () => {}, observeFn: () => { throw Error("nothing selected"); } });
  assert.equal(empty.asked, 0);
  assert.equal(readFileSync(args.discovered, "utf8"), "");
  console.log("Channel qualification, refusal and declared-link closure: PASS");
} finally { rmSync(temp, { recursive: true, force: true }); }
