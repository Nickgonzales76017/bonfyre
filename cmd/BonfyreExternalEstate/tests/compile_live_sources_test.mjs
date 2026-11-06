import { test, expect } from "bun:test";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { compile, discoverFeed, judge, slug } from "../tools/compile_live_sources.mjs";

const now = Date.parse("2026-09-29T12:00:00Z");
const feed = '<rss><channel><language>en</language>' + [1, 2, 3].map(i =>
  `<item><title>Story ${i}</title><pubDate>Tue, 29 Sep 2026 10:00:00 GMT</pubDate></item>`).join("") + '</channel></rss>';

test("feed judgment refuses weak, stale, explicit or non-English samples", () => {
  expect(judge(feed, "local", now)).toMatchObject({ items: 3, titled: 3, dated: 3, age_days: 0 });
  expect(judge(feed.replace("<language>en</language>", "<language>fr</language>"), "local", now).refused).toContain("published in fr");
  expect(judge(feed.replace("<language>en</language>", "<itunes:explicit>yes</itunes:explicit>"), "podcasts", now).refused).toContain("explicit");
  expect(judge("<rss><item><title>Only one</title></item></rss>", "local", now).refused).toBe("fewer than three items");
  expect(discoverFeed('<link rel="alternate" type="application/rss+xml" href="/feed.xml">', "https://publisher.test/")).toBe("https://publisher.test/feed.xml");
  expect(slug("City Library", "Q123")).toBe("auto-city-library-q123");
});

test("published census row requires exact source twin receipt and body", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-live-source-"));
  try {
    const foreign = join(dir, "foreign"), out = join(dir, "sources.json"), evidence = join(dir, "evidence.jsonl");
    mkdirSync(join(foreign, "receipts"), { recursive: true }); mkdirSync(join(foreign, "bodies"), { recursive: true });
    const candidateFn = async () => [{ channel: "local", qid: "Q123", name: "City Library", feedurl: "https://library.test/feed.xml" }];
    const observeFn = async (_project, _foreign, twin) => {
      const receiptPath = join(foreign, "receipts", `${twin}.fresh.json`), bodyPath = join(foreign, "bodies", `${twin}.fresh.body`);
      const receipt = { policy_decision: "allow", http_status: 200, content_sha256: "abcdef1234567890", body_path: bodyPath };
      writeFileSync(receiptPath, JSON.stringify(receipt) + "\n"); writeFileSync(bodyPath, feed);
      return { receipt, text: feed, receiptPath, bodyPath };
    };
    const report = await compile({ project: "project", foreign, out, evidence, channels: "local", limit: 0, pace: 0 },
      { candidateFn, observeFn, sleepFn: async () => {}, now });
    expect(report).toEqual({ admitted: 1, refused: 0, families: { "local-voices": 1 } });
    const row = JSON.parse(readFileSync(out, "utf8"))[0];
    expect(row).toMatchObject({ source: "auto-city-library-q123", twin: "auto-city-library-q123", publisher_ident: "Q123" });
    const canonical = JSON.parse(readFileSync(join(foreign, "receipts", `${row.twin}.json`), "utf8"));
    expect(readFileSync(canonical.body_path, "utf8")).toBe(feed);
    expect(readFileSync(evidence, "utf8")).toContain('"admitted":"auto-city-library-q123"');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("missing retained receipt refuses otherwise valid feed", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-live-source-refusal-"));
  try {
    const report = await compile({ project: "project", foreign: dir, out: join(dir, "sources.json"),
      evidence: join(dir, "evidence.jsonl"), channels: "local", limit: 0, pace: 0 }, {
      candidateFn: async () => [{ channel: "local", qid: "Q123", name: "City Library", feedurl: "https://library.test/feed.xml" }],
      observeFn: async () => ({ receipt: { policy_decision: "allow", http_status: 200 }, text: feed,
        receiptPath: join(dir, "missing.json"), bodyPath: join(dir, "missing.body") }),
      sleepFn: async () => {}, now,
    });
    expect(report).toMatchObject({ admitted: 0, refused: 1 });
    expect(JSON.parse(readFileSync(join(dir, "sources.json"), "utf8"))).toEqual([]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
