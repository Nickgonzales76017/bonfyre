// What this does not prove:
// - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { ask, probe, recordAnswers, rowsAt, sourceId } from "../tools/probe_catalogues.mjs";

assert.deepEqual(rowsAt({ result: { results: [1, 2] } }, "result.results"), [1, 2]);
assert.equal(rowsAt({ result: {} }, "result.results"), null);
assert.match(sourceId("www.example.test", "dcat"), /^dcat-www-example-test-[a-f0-9]{6}$/);
const temp = mkdtempSync(join(tmpdir(), "bonfyre-probe-catalogue-"));
try {
  const hosts = join(temp, "hosts.txt");
  writeFileSync(hosts, "www.example.test\nexample.test\nnot a host\n");
  const dcat = JSON.stringify({ dataset: Array.from({ length: 5 }, (_, id) => ({ title: `Dataset ${id}`, landingPage: `/dataset/${id}` })) });
  const feed = `<rss>${Array.from({ length: 5 }, (_, id) => `<item><title>News ${id}</title></item>`).join("")}</rss>`;
  const args = { hosts, out: join(temp, "channels.jsonl"), census: join(temp, "census.json"), project: "/unused",
    foreign: join(temp, "foreign"), limit: 10, workers: 2, minRows: 5 };
  const asked = [];
  const result = await probe(args, { askFn: async (host, convention) => {
    asked.push([host, convention.name]);
    const text = convention.name === "dcat" ? dcat : convention.name === "feed" ? feed
      : convention.name === "openapi" ? '{"openapi":"3.0.0"}' : "";
    return { address: `https://${host}${convention.path}`, text, said: "", receipt: { policy_decision: "allow", http_status: 200 } };
  } });
  assert.equal(result.report.hosts_asked, 1);
  assert.equal(result.report.answered, 3);
  assert.equal(result.report.readable_sources, 2);
  assert.equal(result.report.rows_listed, 10);
  assert.deepEqual(asked.map(row => row[1]), ["dcat", "feed", "openapi"]);
  assert.deepEqual(result.found.map(row => row.truth_level), Array(3).fill("protocol_derived_candidate"));
  assert.deepEqual(result.census.map(row => [row.family, row.reading.extract]), [["datasets", "dataset"], ["news", "rss:item"]]);

  // The real observer adapter keeps the receipt and discards only its bounded
  // sampling body. A denied receipt cannot be promoted by stray body bytes.
  const mock = join(temp, "mock-observer.mjs");
  writeFileSync(mock, `#!/usr/bin/env bun\nimport { writeFileSync } from "node:fs";\nconst a = process.argv.slice(2);\nconst at = flag => a[a.indexOf(flag)+1];\nwriteFileSync(at("--receipt"), JSON.stringify({policy_decision:"allow",http_status:200})+"\\n");\nwriteFileSync(at("--body"), ${JSON.stringify(dcat)});\n`);
  chmodSync(mock, 0o755);
  const observed = await ask("example.test", { name: "dcat", path: "/data.json" }, { project: mock, foreign: args.foreign });
  assert.equal(observed.text, dcat);
  assert.equal(existsSync(observed.receiptPath), true);
  assert.equal(JSON.parse(readFileSync(observed.receiptPath, "utf8")).policy_decision, "allow");
  const slice = join(temp, "slice.txt");
  const channels = join(temp, "channels.jsonl");
  const askedDb = join(temp, "asked.db");
  writeFileSync(slice, "example.test\nsilent.test\n");
  writeFileSync(channels, JSON.stringify({ root: "example.test" }) + "\n");
  assert.deepEqual(recordAnswers(askedDb, slice, channels), { asked: 2, answered: 1 });
  const history = new Database(askedDb);
  assert.deepEqual(history.query("SELECT host, answered FROM asked ORDER BY host").all(),
    [{ host: "example.test", answered: 1 }, { host: "silent.test", answered: 0 }]);
  history.close();
  console.log("Convention probe and retained observation receipt: PASS");
} finally { rmSync(temp, { recursive: true, force: true }); }
