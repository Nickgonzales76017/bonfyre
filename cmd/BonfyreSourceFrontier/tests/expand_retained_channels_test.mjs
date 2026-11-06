import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { discoveries, expand, familyFor } from "../tools/expand_retained_channels.mjs";

test("retained links become shadow candidates with material and lineage", () => {
  const body = Buffer.from('<link rel="alternate" type="application/rss+xml" href="/feed.xml">'
    + '<link rel="hub" href="https://hub.test/subscribe">'
    + '<a href="https://events.test/calendar.ics">Events</a>'
    + '<link rel="service-desc" href="https://api.test/openapi.json">');
  const rows = discoveries(body, "https://publisher.test/", "source-1", "twin-1", "abc");
  expect(rows.map(row => [row.address, row.protocol, row.material ?? ""])).toEqual([
    ["https://publisher.test/feed.xml", "rss", "publication"],
    ["https://hub.test/subscribe", "websub_hub", ""],
    ["https://api.test/openapi.json", "openapi", ""],
    ["https://events.test/calendar.ics", "ics", "event"],
  ]);
  expect(rows.every(row => row.state === "shadow" && row.truth_level === "observed_link_candidate" && row.body_sha256 === "abc")).toBe(true);
  expect(familyFor("https://x.test/.well-known/agent-card.json")).toBe("a2a");
});

test("retained CMS bodies write candidates but refuse own host lineage", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-retained-channels-"));
  try {
    const cms = join(dir, "cms.sqlite"), foreign = join(dir, "foreign"), out = join(dir, "channels.jsonl");
    mkdirSync(join(foreign, "receipts"), { recursive: true });
    mkdirSync(join(foreign, "bodies"), { recursive: true });
    const db = new Database(cms);
    db.exec("CREATE TABLE internet_source (source TEXT, twin TEXT, url TEXT, status TEXT)");
    db.query("INSERT INTO internet_source VALUES (?, ?, ?, ?)").run("source-1", "twin-1", "https://publisher.test/", "published");
    db.close();
    const body = Buffer.from('<link rel="alternate" type="application/rss+xml" href="https://aurekai.work/feed">'
      + '<link rel="alternate" type="application/rss+xml" href="https://remote.test/feed">');
    const bodyPath = join(foreign, "bodies", "twin-1.body");
    writeFileSync(bodyPath, body);
    writeFileSync(join(foreign, "receipts", "twin-1.json"), JSON.stringify({ body_path: bodyPath, http_status: 200 }) + "\n");
    const report = expand({ cms, foreign, out, selfHosts: "aurekai.work", maxBytes: 4_000_000 });
    expect(report).toMatchObject({ bodies_read: 1, links: 1, refused_self_lineage: 1, by_family: { rss: 1 } });
    const rows = readFileSync(out, "utf8").trim().split("\n").map(JSON.parse);
    expect(rows[0].address).toBe("https://remote.test/feed");
    expect(rows[0].body_sha256).toBe(createHash("sha256").update(body).digest("hex"));
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
