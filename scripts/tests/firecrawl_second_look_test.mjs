import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { renderedLinks, secondLook } from "../firecrawl_second_look.mjs";

const temp = mkdtempSync(join(tmpdir(), "bonfyre-render-second-look-"));
try {
  const dbPath = join(temp, "asked.db");
  const stateDir = join(temp, "probe");
  const { mkdirSync, renameSync } = await import("node:fs");
  mkdirSync(stateDir);
  const db = new Database(dbPath);
  db.exec("CREATE TABLE asked(host TEXT PRIMARY KEY, at TEXT NOT NULL, answered INTEGER NOT NULL);");
  db.exec("INSERT INTO asked VALUES('silent.test','2026-09-01',0),('denied.test','2026-09-02',0),('known.test','2026-09-03',1);");
  db.close();
  renameSync(dbPath, join(stateDir, "asked.db"));
  const args = { estate: temp, out: join(temp, "rendered.jsonl"), limit: 20, apply: true };
  const env = { BONFYRE_EXTERNAL_READS_ENABLED: "1", FIRECRAWL_API_KEY: "test" };
  assert.equal((await secondLook(args, { env: {} })).asked, 0);
  assert.equal((await secondLook(args, { env: { BONFYRE_EXTERNAL_READS_ENABLED: "1" } })).asked, 0);
  const dry = await secondLook({ ...args, apply: false }, { env });
  assert.deepEqual(dry.would_ask, ["silent.test", "denied.test"]);
  assert.equal(existsSync(args.out), false);
  const result = await secondLook(args, { env, renderFn: async (_key, address) => address.includes("denied")
    ? { links: null, why: "refused 403" }
    : { links: ["https://silent.test/feed.xml", "https://silent.test/feed.xml", "https://silent.test/login",
      "https://silent.test/photo.jpg", "https://another.test/openapi.json"], why: "" } });
  assert.equal(result.hosts_asked, 2);
  assert.equal(result.refused, 1);
  assert.equal(result.addresses_found, 2);
  assert.equal(result.billed_credits, "unmeasured");
  const rows = readFileSync(args.out, "utf8").trim().split("\n").map(JSON.parse);
  assert.deepEqual(rows.map(row => [row.root, row.truth_level]), [
    ["silent.test", "observed_link_candidate"], ["another.test", "observed_link_candidate"],
  ]);
  const checked = new Database(join(stateDir, "asked.db"));
  assert.equal(checked.query("SELECT links FROM looked_again WHERE host='silent.test'").get().links, 2);
  assert.equal(checked.query("SELECT why FROM looked_again WHERE host='denied.test'").get().why, "refused 403");
  checked.exec("INSERT INTO asked VALUES('payment.test','2026-09-04',0);");
  checked.close();
  const paid = await secondLook(args, { env, renderFn: async () => ({ links: null, why: "payment required", paymentRequired: true }) });
  assert.equal(paid.provider_state, "payment_required");
  const after = new Database(join(stateDir, "asked.db"));
  assert.equal(after.query("SELECT count(*) AS n FROM looked_again WHERE host='payment.test'").get().n, 0);
  after.close();
  const response = await renderedLinks("test", "https://silent.test/", async () => new Response("pay", { status: 402 }));
  assert.equal(response.paymentRequired, true);
  console.log("Rendered second look candidate and payment boundary: PASS");
} finally { rmSync(temp, { recursive: true, force: true }); }
