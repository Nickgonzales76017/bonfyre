import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const repo = resolve(import.meta.dir, "../..");
const script = join(repo, "scripts/bonfyre-command-evidence");

test("command evidence commits only a complete non-invented native census", () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-command-census-"));
  const database = join(root, "fabric.db");
  const destination = join(root, "completion", "command-evidence.tsv");
  mkdirSync(join(root, "completion"));
  const db = new Database(database);
  try {
    db.exec(`CREATE TABLE fabric_meta(key TEXT,value TEXT);
      CREATE TABLE catalog(id TEXT,maturity TEXT);
      CREATE TABLE catalog_bindings(operator_id TEXT,binding_state TEXT,catalog_generation TEXT);
      CREATE TABLE operator_contract_bindings(operator_id TEXT,family TEXT,generation TEXT);
      CREATE TABLE events(operator_id TEXT,end_at TEXT,id TEXT,status TEXT,input_uri TEXT,output_uri TEXT,receipt_id TEXT);
      CREATE TABLE receipts(id TEXT,payload TEXT);
      INSERT INTO fabric_meta VALUES ('catalog_generation','isolated-test');`);
    const run = (path = database) => Bun.spawnSync(["/bin/sh", script], {
      cwd: repo, stdout: "pipe", stderr: "pipe", timeout: 10000,
      env: { ...process.env, BONFYRE_STATE_DIR: root, BONFYRE_FABRIC_DB: path,
        BONFYRE_COMMAND_EVIDENCE: destination },
    });
    const add = db.transaction(id => {
      db.query("INSERT INTO catalog VALUES (?,?)").run(id, "declared");
      db.query("INSERT INTO catalog_bindings VALUES (?,?,?)").run(id, "bound", "isolated-test");
      db.query("INSERT INTO operator_contract_bindings VALUES (?,?,?)").run(id, "fixture", "isolated-test");
    });
    writeFileSync(destination, "previous-valid-matrix\n");
    expect(run().exitCode).not.toBe(0);
    expect(readFileSync(destination, "utf8")).toBe("previous-valid-matrix\n");
    for (let i = 0; i < 93; i++) add(`command.fixture${String(i).padStart(3, "0")}`);
    const valid = run();
    expect(valid.exitCode).toBe(0);
    const published = readFileSync(destination, "utf8");
    const rows = published.trimEnd().split("\n");
    expect(rows).toHaveLength(94);
    expect(rows.slice(1).every(row => row.endsWith("bound_not_workload_proven"))).toBe(true);
    add("command.fixture093");
    expect(run().exitCode).not.toBe(0);
    expect(readFileSync(destination, "utf8")).toBe(published);
    const empty = join(root, "missing-schema.db");
    new Database(empty).close();
    expect(run(empty).exitCode).not.toBe(0);
    expect(readFileSync(destination, "utf8")).toBe(published);
    expect(readdirSync(join(root, "completion")).filter(name => name.startsWith(".bonfyre-command-evidence."))).toEqual([]);
  } finally { db.close(); rmSync(root, { recursive: true, force: true }); }
});
