import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { gzipSync } from "node:zlib";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bodyOf, channelsFromBody, expand } from "../tools/expand_publisher_lattice.mjs";

test("publisher grammar compiles conventions without laundering their truth", () => {
  const body = Buffer.from('<meta name="generator" content="WordPress 6"><link type="application/rss+xml" href="/news.xml">'
    + '<a href="https://publisher.test/category/books/">Books</a><a href="https://other.test/category/books/">Other</a>');
  const { grammar, rows } = channelsFromBody(body, "source-1", "https://publisher.test/", 40);
  expect(grammar).toBe("wordpress");
  expect(rows[0]).toMatchObject({ address: "https://publisher.test/news.xml", truth_level: "observed_link_candidate", state: "shadow" });
  expect(rows.find(row => row.address === "https://publisher.test/feed/")?.truth_level).toBe("protocol_derived_candidate");
  expect(rows.find(row => row.address === "https://publisher.test/category/books/feed/")?.truth_level).toBe("protocol_derived_candidate");
  expect(rows.some(row => row.address.includes("other.test"))).toBe(false);
  expect(channelsFromBody(body, "source-1", "https://publisher.test/", 2).rows).toHaveLength(2);
});

test("gzipped retained body expands through shared grammar detector and excludes own hosts", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-publisher-lattice-"));
  try {
    const cms = join(dir, "cms.sqlite"), foreign = join(dir, "foreign"), out = join(dir, "channels.jsonl");
    mkdirSync(join(foreign, "receipts"), { recursive: true }); mkdirSync(join(foreign, "bodies"), { recursive: true });
    const db = new Database(cms);
    db.exec("CREATE TABLE internet_source (source TEXT, twin TEXT, url TEXT, status TEXT)");
    const add = db.query("INSERT INTO internet_source VALUES (?, ?, ?, ?)");
    add.run("remote", "remote-twin", "https://publisher.test/", "published");
    add.run("self", "self-twin", "https://aurekai.work/", "published");
    db.close();
    const path = join(foreign, "bodies", "remote-twin.body");
    writeFileSync(`${path}.gz`, gzipSync(Buffer.from('<meta name="generator" content="Ghost"><link type="text/calendar" href="/events.ics">')));
    writeFileSync(join(foreign, "receipts", "remote-twin.json"), JSON.stringify({ body_path: path }) + "\n");
    const selfPath = join(foreign, "bodies", "self.body");
    writeFileSync(selfPath, '<meta name="generator" content="WordPress">');
    writeFileSync(join(foreign, "receipts", "self-twin.json"), JSON.stringify({ body_path: selfPath }) + "\n");
    expect((await bodyOf(foreign, "remote-twin")).toString()).toContain("Ghost");
    const report = await expand({ cms, foreign, out, selfHosts: "aurekai.work", perRoot: 40 });
    expect(report).toMatchObject({ roots_read: 1, grammars: { ghost: 1 }, channels: 3 });
    const rows = readFileSync(out, "utf8").trim().split("\n").map(JSON.parse);
    expect(rows.map(row => row.protocol)).toEqual(["ics", "rss", "sitemap"]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
