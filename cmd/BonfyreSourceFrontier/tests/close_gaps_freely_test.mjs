import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { closeGaps, machineChannels, wikidataAnswers } from "../tools/close_gaps_freely.mjs";

test("official-site identities and own head links are distinct evidence", () => {
  const text = JSON.stringify({ results: { bindings: [{ item: { value: "http://www.wikidata.org/entity/Q123" },
    name: { value: "City Library" }, website: { value: "https://library.test/" } }] } });
  expect(wikidataAnswers(text).get("library.test")).toEqual(["Q123", "City Library", "https://library.test/"]);
  expect(machineChannels('<link type="application/rss+xml" href="/feed.xml" title="News">'
    + '<link type="text/calendar" href="/calendar.ics">', "https://library.test/")).toEqual([
    ["https://library.test/feed.xml", "rss", "News"], ["https://library.test/calendar.ics", "ics", ""]]);
});

test("free gap closure writes native identity and shadow channel, never admission", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-free-gaps-"));
  try {
    const dbPath = join(dir, "frontier.sqlite"), channels = join(dir, "channels.jsonl"), evidence = join(dir, "evidence.jsonl");
    const db = new Database(dbPath);
    db.exec("CREATE TABLE root (root_id TEXT, host TEXT, name TEXT); CREATE TABLE gap (root_id TEXT, kind TEXT, status TEXT); "
      + "CREATE TABLE channel (root_id TEXT, contributions INTEGER)");
    db.query("INSERT INTO root VALUES (?, ?, ?)").run("r1", "library.test", "Library");
    const addGap = db.query("INSERT INTO gap VALUES (?, ?, ?)");
    addGap.run("r1", "IDENTITY", "open"); addGap.run("r1", "STRUCTURED_CHANNEL", "open");
    db.close();
    const imported = [], observed = [];
    const result = await closeGaps({ db: dbPath, frontier: "frontier", project: "project", foreign: dir, channels, evidence, limit: 60 }, {
      observeFn: async (_project, _foreign, twin, url) => {
        observed.push([twin, url]);
        const text = url.includes("qlever.dev")
          ? JSON.stringify({ results: { bindings: [{ item: { value: "http://www.wikidata.org/entity/Q123" },
            name: { value: "City Library" }, website: { value: "https://library.test/" } }] } })
          : '<link type="text/calendar" href="/calendar.ics">';
        return { receipt: { policy_decision: "allow", http_status: 200 }, receiptPath: `${twin}.json`, text };
      }, importFn: argv => imported.push(argv), sleepFn: async () => {},
    });
    expect(result).toMatchObject({ identities_closed: 1, machine_channels_found: 1 });
    expect(observed).toHaveLength(2);
    expect(imported).toHaveLength(2);
    const identities = readFileSync(join(dir, "free-identities.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
    expect(identities[0]).toMatchObject({ address: "https://library.test/", identity: "Q123", state: "candidate" });
    const rows = readFileSync(channels, "utf8").trim().split("\n").map(JSON.parse);
    expect(rows[0]).toMatchObject({ address: "https://library.test/calendar.ics", state: "shadow",
      truth_level: "observed_link_candidate", material: "event" });
    expect(readFileSync(evidence, "utf8")).toContain("gapclose-");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
