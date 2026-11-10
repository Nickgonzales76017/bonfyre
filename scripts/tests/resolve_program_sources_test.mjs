import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { resolveProgram } from "../resolve_program_sources.mjs";

const temp = mkdtempSync(join(tmpdir(), "bonfyre-program-source-"));
try {
  const cms = new Database(join(temp, "cms.db"));
  cms.exec("CREATE TABLE program_instance(instance_id TEXT, archetype TEXT, owner_subject TEXT, template_id TEXT, status TEXT); "
    + "CREATE TABLE program_template(template_id TEXT, name TEXT, status TEXT); "
    + "CREATE TABLE source_collection(collection_id TEXT, status TEXT, source_selector TEXT, updated_at TEXT); "
    + "CREATE TABLE program_requirement(instance_id TEXT, kind TEXT, requirement_id TEXT, state TEXT, satisfied_by TEXT, updated_at TEXT);");
  cms.exec("INSERT INTO program_template VALUES('template','Permit work','published'); "
    + "INSERT INTO program_instance VALUES('permit','housing-human-services','institution:1','template','published'); "
    + "INSERT INTO source_collection VALUES('permit-sources','published','{\"source_refs\":[\"already\"]}',''); "
    + "INSERT INTO program_requirement VALUES('permit','capability','permit-needs-source-intake','open','','');");
  for(const table of ['program_instance','program_template','source_collection','program_requirement'])cms.exec('ALTER TABLE '+table+" ADD COLUMN namespace TEXT DEFAULT 'root'");
  cms.close();
  const frontier = new Database(join(temp, "frontier.db"));
  frontier.exec("CREATE TABLE channel(channel_id TEXT, census_source TEXT, state TEXT); CREATE TABLE channel_origin(channel_id TEXT, label TEXT); "
    + "INSERT INTO channel VALUES('c1','source-answered','eligible'); INSERT INTO channel_origin VALUES('c1','PROGRAM_PERMIT');");
  frontier.close();
  const args = { estate: temp, repo: temp, instance: "permit", needs: "permits; zoning", queries: 2, perQuery: 5, apply: false };
  const dry = resolveProgram(args, { runFn: () => { throw Error("dry-run must not call providers"); } });
  assert.deepEqual(dry.report.would_ask_for, ["permits", "zoning"]);
  assert.equal(resolveProgram({ ...args, instance: "missing" }).exitCode, 2);
  const calls = [];
  const runFn = argv => {
    calls.push(argv);
    const at = flag => argv[argv.indexOf(flag) + 1];
    if (argv.some(value => value.endsWith("discover_with_exa.mjs"))) writeFileSync(at("--out"), '{"root":"example.test"}\n');
    if (argv.some(value => value.endsWith("qualify_channels.mjs"))) {
      writeFileSync(at("--out"), '[{"source":"source-answered"}]\n');
      if (argv.includes("--discovered")) writeFileSync(at("--discovered"), "");
    }
    return { exitCode: 0, stdout: '{"admitted":1}\n', stderr: "" };
  };
  const applied = resolveProgram({ ...args, apply: true, discover: true }, { runFn });
  assert.equal(applied.exitCode, 0);
  assert.equal(applied.report.listed, 1);
  assert.equal(applied.report.sources_now_in_the_collection, 2);
  assert.ok(calls.some(argv => argv.some(value => value.endsWith("discover_with_exa.mjs"))));
  assert.ok(calls.some(argv => argv.some(value => value.endsWith("qualify_channels.mjs"))));
  assert.ok(calls.some(argv => argv.some(value => value.endsWith("apply_census_sources.mjs"))));
  const check = new Database(join(temp, "cms.db"));
  const selector = JSON.parse(check.query("SELECT source_selector FROM source_collection WHERE collection_id='permit-sources'").get().source_selector);
  assert.deepEqual(selector.source_refs, ["already", "source-answered"]);
  assert.deepEqual(selector.asked_for, ["permits", "zoning"]);
  assert.equal(check.query("SELECT state FROM program_requirement WHERE instance_id='permit'").get().state, "open");
  check.close();
  console.log("Program source demand to admitted collection: PASS");
} finally { rmSync(temp, { recursive: true, force: true }); }
