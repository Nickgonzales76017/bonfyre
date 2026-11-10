import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { appendFileSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { apply } from "../representation-manifold-shift.mjs";
import { loadPolicy, materializeGraph, plan, planAllGraphs, queryVirtualGraph, registerAllVirtualGraphs, registerVirtualGraph, status } from "../representation-manifold-forge.mjs";
import { bodyDigest } from "../columnar_body.mjs";

test("installed forge owns the live entrypoint while the eager Python worker is archived", () => {
  expect(existsSync(fileURLToPath(new URL("../representation_manifold_forge.py", import.meta.url)))).toBe(false);
  expect(existsSync(fileURLToPath(new URL("../representation_manifold_forge.py.archive", import.meta.url)))).toBe(true);
  const install = readFileSync(new URL("../Makefile", import.meta.url), "utf8")
    .match(/^install-representation-forge:[^\n]*\n(?:\t[^\n]*\n)+/m)?.[0];
  expect(install).toContain("install -m 0755 representation-manifold-forge.mjs");
  expect(install).not.toMatch(/representation_manifold_forge\.py/);
});

test("native forge lowers one verified Parquet root into candidate graph edges with complete receipts", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-forge-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec(`CREATE TABLE links(source TEXT, target TEXT, amount DECIMAL(12,2));
      INSERT INTO links VALUES ('school', 'program', 10.25), ('program', 'household', 5.00), (NULL, 'orphan', 0);`);
    source.close();
    await apply(dir, join(dir, "no-substrates"));
    const policy = loadPolicy();
    expect(policy.directedPairs.some(pair => pair.source === "source" && pair.target === "target")).toBe(true);
    const before = await plan(dir, "cms.links");
    expect(before.pairs).toEqual([{ source: "source", target: "target", state: "candidate_only" }]);
    expect(before.input_digest).toMatch(/^[0-9a-f]{64}$/);
    const stage = join(dir, "stage");
    const installed = Bun.spawnSync(["make", "-C", resolve(import.meta.dir, ".."), "install-representation-forge", `MEMBRANE_ROOT=${stage}`]);
    expect(installed.exitCode).toBe(0);
    const stagedPlan = Bun.spawnSync(["bun", join(stage, "scripts", "representation-manifold-forge.mjs"), "plan-graph", "--estate", dir, "--kind", "cms.links"]);
    expect(stagedPlan.exitCode).toBe(0);
    expect(JSON.parse(stagedPlan.stdout.toString()).pairs).toEqual(before.pairs);

    const result = await materializeGraph(dir, "cms.links");
    expect(result.pairs).toHaveLength(1);
    expect(result.pairs[0].state).toBe("physical");
    expect(result.pairs[0].rows).toBe(2);
    expect(existsSync(result.pairs[0].coordinate)).toBe(true);
    const query = `SELECT src,dst,relation,semantic_kind FROM read_parquet('${result.pairs[0].coordinate.replaceAll("'", "''")}') ORDER BY src`;
    const rows = Bun.spawnSync(["duckdb", "-json", ":memory:", query]);
    expect(rows.exitCode).toBe(0);
    expect(JSON.parse(rows.stdout.toString())).toEqual([
      { src: "program", dst: "household", relation: "source->target", semantic_kind: "cms.links" },
      { src: "school", dst: "program", relation: "source->target", semantic_kind: "cms.links" },
    ]);
    const db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    const instance = db.query("SELECT kind,authority,frontier,digest,state FROM representation_instance WHERE id=?").get(result.pairs[0].representation_id);
    expect(instance.kind).toBe("graph-edge-parquet");
    expect(instance.authority).toBe("derived");
    expect(instance.frontier).toBe(before.frontier);
    expect(instance.digest).toBe(result.pairs[0].digest);
    expect(instance.state).toBe("physical");
    const receipt = db.query("SELECT compiler,input_digest,output_digest,proof_state FROM transformation_occurrence WHERE output_ref=?").get(result.pairs[0].coordinate);
    expect(receipt.compiler.startsWith("bonfyre-representation-forge.v1+duckdb+policy:")).toBe(true);
    expect(receipt.input_digest).toBe(before.input_digest);
    expect(receipt.output_digest).toBe(result.pairs[0].digest);
    expect(receipt.proof_state).toBe("materialized");
    expect(db.query("SELECT count(*) n FROM representation_edge WHERE child_representation_id=?").get(result.pairs[0].representation_id).n).toBe(1);
    expect(db.query("SELECT count(*) n FROM forge_event WHERE kind='graph' AND state='physical'").get().n).toBe(1);
    db.close();
    const again = await materializeGraph(dir, "cms.links");
    expect(again.pairs[0].state).toBe("verified_existing");
    expect(status(dir).representations.some(row => row.kind === "graph-edge-parquet" && row.n === 1)).toBe(true);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("forge refuses a changed canonical Parquet root without writing a projection", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-forge-digest-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec("CREATE TABLE links(source TEXT,target TEXT); INSERT INTO links VALUES ('a','b')");
    source.close();
    const initial = await apply(dir, join(dir, "no-substrates"));
    const parquet = initial.manifest.find(row => row.kind === "cms.links").target;
    const original = await bodyDigest(parquet);
    appendFileSync(parquet, "tamper");
    expect(await bodyDigest(parquet)).not.toBe(original);
    expect(materializeGraph(dir, "cms.links")).rejects.toThrow("no digest-valid Parquet root");
    const db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    expect(db.query("SELECT count(*) n FROM graph_projection").get().n).toBe(0);
    db.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("virtual graph keeps candidate semantics, reads without duplicate bytes, and refuses drift", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-virtual-graph-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec(`CREATE TABLE links(source TEXT,target TEXT,weight DECIMAL(12,2));
      INSERT INTO links VALUES ('a','b',1.25),('b','c',2.50),(NULL,'orphan',3);`);
    source.close();
    const initial = await apply(dir, join(dir, "no-substrates"));
    const parquet = initial.manifest.find(row => row.kind === "cms.links").target;
    const beforeGraph = join(dir, "representations", "graph");
    const registered = await registerVirtualGraph(dir, "cms.links");
    expect(registered.programs).toHaveLength(1);
    expect(registered.programs[0].state).toBe("executable_candidate_only");
    const repeated = await registerVirtualGraph(dir, "cms.links");
    expect(repeated.programs[0].state).toBe("verified_existing");
    expect(repeated.programs[0].proof_digest).toBe(registered.programs[0].proof_digest);
    expect(existsSync(beforeGraph)).toBe(false);
    const query = await queryVirtualGraph(dir, "cms.links", "source", "target", 1);
    expect(query.candidate_only).toBe(true);
    expect(query.rows).toEqual([{ semantic_kind: "cms.links", source_column: "source", src: "a",
      target_column: "target", dst: "b", relation: "source->target" }]);
    expect(existsSync(beforeGraph)).toBe(false);
    const db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    expect(db.query("SELECT count(*) n FROM representation_instance WHERE kind='graph-virtual' AND state='executable'").get().n).toBe(1);
    expect(db.query("SELECT count(*) n FROM physical_realization WHERE representation_id=?").get(registered.programs[0].representation_id).n).toBe(0);
    expect(db.query("SELECT proof_state FROM transformation_occurrence WHERE id=?").get(registered.programs[0].proof_digest).proof_state).toBe("planned");
    db.close();
    appendFileSync(parquet, "tamper");
    expect(queryVirtualGraph(dir, "cms.links", "source", "target")).rejects.toThrow("no digest-valid Parquet root");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("forge refuses ambiguous physical roots instead of silently selecting a historical one", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-ambiguous-root-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec("CREATE TABLE links(source TEXT,target TEXT); INSERT INTO links VALUES ('a','b')");
    source.close();
    await apply(dir, join(dir, "no-substrates"));
    const db = new Database(join(dir, "representation-manifold.db"));
    const physical = db.query(`SELECT id,representation_id,carrier,coordinate,bytes,digest,temperature,available,checked_at
      FROM physical_realization WHERE representation_id IN
      (SELECT id FROM representation_instance WHERE schema_ref='cms.links' AND kind='parquet')`).get();
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)").run(
      `${physical.id}-duplicate`, physical.representation_id, physical.carrier, physical.coordinate,
      physical.bytes, physical.digest, physical.temperature, physical.available, physical.checked_at);
    db.close();
    expect(plan(dir, "cms.links")).rejects.toThrow("ambiguous active Parquet roots");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("estate-wide native forge preflights every root and registers only authored virtual graph candidates", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-forge-all-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec(`CREATE TABLE a_links(source TEXT,target TEXT);
      INSERT INTO a_links VALUES ('a','b'),(NULL,'unowned');
      CREATE TABLE b_links(parent TEXT,child TEXT);
      INSERT INTO b_links VALUES ('p','c');
      CREATE TABLE notes(id TEXT,note TEXT);
      INSERT INTO notes VALUES ('n','no inferred edge');`);
    source.close();
    const shifted = await apply(dir, join(dir, "no-substrates"));
    const planned = await planAllGraphs(dir);
    expect(planned.roots).toBe(3);
    expect(planned.candidates.map(item => item.kind)).toEqual(["cms.a_links", "cms.b_links"]);
    expect(planned.without_authored_pair).toEqual(["cms.notes"]);
    const staged = join(dir, "staged");
    const installed = Bun.spawnSync(["make", "-C", resolve(import.meta.dir, ".."), "install-representation-forge", `MEMBRANE_ROOT=${staged}`]);
    expect(installed.exitCode).toBe(0);
    const stagedPlan = Bun.spawnSync(["bun", join(staged, "scripts", "representation-manifold-forge.mjs"),
      "plan-all-graphs", "--estate", dir]);
    expect(stagedPlan.exitCode).toBe(0);
    expect(JSON.parse(stagedPlan.stdout.toString()).candidates.map(item => item.kind)).toEqual(
      planned.candidates.map(item => item.kind));

    const second = shifted.manifest.find(item => item.kind === "cms.b_links").target;
    const original = readFileSync(second);
    appendFileSync(second, "tamper");
    expect(registerAllVirtualGraphs(dir)).rejects.toThrow("no digest-valid Parquet root");
    let db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    expect(db.query("SELECT count(*) n FROM virtual_graph_projection_v2").get().n).toBe(0);
    db.close();
    writeFileSync(second, original);

    const stagedBind = Bun.spawnSync(["bun", join(staged, "scripts", "representation-manifold-forge.mjs"),
      "virtual-all-graphs", "--estate", dir]);
    expect(stagedBind.exitCode).toBe(0);
    const bound = JSON.parse(stagedBind.stdout.toString());
    expect(bound).toMatchObject({ roots: 3, candidate_roots: 2, programs: 2,
      without_authored_pair: ["cms.notes"] });
    expect(bound.registered.every(item => item.programs.every(program =>
      program.state === "executable_candidate_only"))).toBe(true);
    const repeated = await registerAllVirtualGraphs(dir);
    expect(repeated.registered.every(item => item.programs.every(program =>
      program.state === "verified_existing"))).toBe(true);
    expect(existsSync(join(dir, "representations", "graph"))).toBe(false);
    db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    expect(db.query("SELECT count(*) n FROM virtual_graph_projection_v2").get().n).toBe(2);
    expect(db.query("SELECT count(*) n FROM physical_realization WHERE representation_id IN (SELECT id FROM representation_instance WHERE kind='graph-virtual')").get().n).toBe(0);
    db.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
