import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { apply, portfolio, status } from "../representation-manifold-shift.mjs";

test("one source snapshot lowers to a digest-bound Parquet representation without rewriting architecture", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-manifold-"));
  try {
    const sourcePath = join(dir, "cms.db");
    const cms = new Database(sourcePath);
    cms.exec('CREATE TABLE internet_source(source TEXT); INSERT INTO internet_source VALUES ("school-one");');
    cms.close();
    const atlas = resolve(import.meta.dir, "../../architecture/storage/representation-manifold.yaff");
    const beforeAtlas = createHash("sha256").update(readFileSync(atlas)).digest("hex");
    const first = await apply(dir, join(dir, "no-substrates"));
    const physical = first.manifest.find(row => row.kind === "cms.internet_source");
    expect(physical.state).toBe("physical");
    expect(physical.frontier).toContain("sqlite-sha256:");
    expect(existsSync(physical.target)).toBe(true);
    expect(first.status.counts.semantic_object).toBe(1);
    expect(first.status.counts.representation_instance).toBe(1);
    expect(first.status.targets.find(row => row.status === "physical").n).toBe(1);
    expect(portfolio.classes.table).toContain("parquet");
    expect(createHash("sha256").update(readFileSync(atlas)).digest("hex")).toBe(beforeAtlas);
    const stage = join(dir, "installed");
    const staged = Bun.spawnSync(["make", "-C", resolve(import.meta.dir, ".."), "install-representation-manifold", `MEMBRANE_ROOT=${stage}`]);
    expect(staged.exitCode).toBe(0);
    const installedStatus = Bun.spawnSync(["bun", join(stage, "scripts/representation-manifold-shift.mjs"), "--estate", dir, "--json"]);
    expect(installedStatus.exitCode).toBe(0);
    expect(JSON.parse(installedStatus.stdout.toString()).counts.semantic_object).toBe(1);

    const targetSnapshot = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    const originalTarget = targetSnapshot.query("SELECT rowid,status,substrate_path,reason FROM representation_target WHERE semantic_kind=? AND representation_kind=?")
      .get("cms.internet_source","parquet");
    targetSnapshot.close();
    const repeat = await apply(dir, join(dir, "no-substrates"));
    expect(repeat.manifest[0].digest).toBe(physical.digest);
    const replaySnapshot = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    const replayTarget = replaySnapshot.query("SELECT rowid,status,substrate_path,reason FROM representation_target WHERE semantic_kind=? AND representation_kind=?")
      .get("cms.internet_source","parquet");
    replaySnapshot.close();
    expect(replayTarget).toEqual(originalTarget); // no write or loss on identical compiler replay
    const modified = new Database(sourcePath);
    modified.exec('INSERT INTO internet_source VALUES ("school-two");');
    modified.close();
    const previousPath = process.env.PATH;
    process.env.PATH = join(dir, "no-executables");
    try {
      const pending = await apply(dir, join(dir, "no-substrates"));
      expect(pending.manifest[0].state).toBe("pending");
      expect(pending.status.targets.find(row => row.status === "compilation_pending").n).toBe(1);
      expect(createHash("sha256").update(readFileSync(physical.target)).digest("hex")).toBe(physical.digest);
    } finally { process.env.PATH = previousPath; }
    const next = await apply(dir, join(dir, "no-substrates"));
    expect(next.manifest[0].digest).not.toBe(physical.digest);
    expect(next.status.counts.semantic_object).toBe(2);
    expect(next.status.counts.transformation_occurrence).toBe(2);
    expect(status(dir).state).toBe("MATERIALIZED");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("snapshot includes live uncheckpointed WAL rows before physical compilation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-manifold-wal-"));
  try {
    const cms = new Database(join(dir, "cms.db"));
    cms.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE internet_source(source TEXT); INSERT INTO internet_source VALUES('school-from-wal')");
    const result = await apply(dir, join(dir, "no-substrates"));
    expect(result.status.counts.semantic_object).toBe(1);
    const physical = result.manifest.find(row => row.kind === "cms.internet_source");
    expect(physical.state).toBe("physical");
    expect(physical.frontier).toContain("rows:1");
    cms.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("one consistent source snapshot drives several Parquet tables and streamed seed rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-manifold-multi-table-"));
  try {
    const cms = new Database(join(dir, "cms.db"));
    cms.exec(`
      CREATE TABLE internet_source(source TEXT NOT NULL);
      WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<512)
        INSERT INTO internet_source SELECT printf('source-%05d',x) FROM n;
      CREATE TABLE other_collection(code TEXT NOT NULL);
      INSERT INTO other_collection VALUES ('alpha'),('beta');
    `);
    cms.close();
    const first = await apply(dir, join(dir, "no-substrates"));
    const source = first.manifest.find(row => row.kind === "cms.internet_source");
    const other = first.manifest.find(row => row.kind === "cms.other_collection");
    expect(source.state).toBe("physical");
    expect(other.state).toBe("physical");
    expect(first.status.counts.semantic_object).toBe(512);
    expect(first.status.counts.representation_instance).toBe(2);
    const sourceCut = /^sqlite-sha256:([0-9a-f]{64})/.exec(source.frontier)?.[1];
    const otherCut = /^sqlite-sha256:([0-9a-f]{64})/.exec(other.frontier)?.[1];
    expect(sourceCut?.length).toBe(64);
    expect(otherCut).toBe(sourceCut);
    const replay = await apply(dir, join(dir, "no-substrates"));
    expect(replay.manifest.find(row => row.kind === "cms.internet_source").digest).toBe(source.digest);
    expect(replay.manifest.find(row => row.kind === "cms.other_collection").digest).toBe(other.digest);
    expect(replay.status.counts.semantic_object).toBe(512);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
