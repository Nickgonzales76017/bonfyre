import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { declaredFeeds, grammarOf, sweep } from "../tools/sweep_root_grammars.mjs";

assert.deepEqual(grammarOf('<meta name="generator" content="WordPress 6.0">'), ["wordpress", "its own generator says so"]);
assert.equal(grammarOf("a script references /assets/built/main.js")[0], "ghost");
assert.deepEqual(declaredFeeds('<link rel="alternate" type="application/rss+xml" href="/events/feed" title="Events">', "https://example.test/")[0],
  ["https://example.test/events/feed", "rss", "Events"]);
assert.equal(declaredFeeds('<link rel="alternate" type="text/html" href="/feed">', "https://example.test/").length, 0);

const temp = mkdtempSync(join(tmpdir(), "bonfyre-sweep-grammar-"));
try {
  const dbPath = join(temp, "frontier.db");
  const db = new Database(dbPath);
  db.exec("CREATE TABLE root(root_id TEXT, host TEXT, name TEXT, first_seen TEXT); CREATE TABLE channel(root_id TEXT, state TEXT);");
  db.exec("INSERT INTO root VALUES('1','example.test','Example','2026-09-28'),('2','denied.test','Denied','2026-09-27'),('3','read.test','Read','2026-09-26');");
  db.exec("INSERT INTO channel VALUES('3','admitted');");
  db.close();
  const seen = [];
  const result = await sweep({ db: dbPath, project: "/not-used", foreign: temp, out: join(temp, "out.jsonl"),
    limit: 10, workers: 2, perRoot: 2, universal: false }, {
    observeFn: async (_project, _foreign, twin, url) => {
      seen.push([twin, url]);
      return url.includes("denied")
        ? [{ policy_decision: "deny", http_status: 403 }, ""]
        : [{ policy_decision: "allow", http_status: 200 },
          '<meta name="generator" content="WordPress 6.0"><link rel="alternate" type="application/rss+xml" href="/news.xml" title="News">'];
    },
    sleepFn: async () => {},
  });
  assert.equal(result.report.asked, 2);
  assert.equal(result.report.answered, 1);
  assert.equal(result.report.refused, 1);
  assert.equal(result.report.channels_listed, 3);
  assert.deepEqual(result.report.by_truth_level, { observed_link_candidate: 1, protocol_derived_candidate: 2 });
  assert.deepEqual(result.rows.map(row => [row.address, row.truth_level]), [
    ["https://example.test/news.xml", "observed_link_candidate"],
    ["https://example.test/feed/", "protocol_derived_candidate"],
    ["https://example.test/comments/feed/", "protocol_derived_candidate"],
  ]);
  assert.ok(result.rows.every(row => row.root === "example.test"));
  assert.deepEqual(seen.map(row => row[1]), ["https://example.test/", "https://denied.test/"]);
  assert.deepEqual(result.evidence.map(row => row.policy), ["allow", "deny"]);
  console.log("Publisher grammar sweep candidate truth: PASS");
} finally { rmSync(temp, { recursive: true, force: true }); }
