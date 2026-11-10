import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { closeSync, mkdtempSync, openSync, rmSync, symlinkSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { apply } from "../representation-manifold-shift.mjs";
import { bindNativeFsCarrier, planNativeFsCarrier, readNativeFsCarrier } from "../representation-manifold-carrier.mjs";

test("native filesystem carrier verifies Parquet, binds once, and reads exact seek bytes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-carrier-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec("CREATE TABLE objects(id INTEGER, name TEXT); INSERT INTO objects VALUES (1,'alpha'),(2,NULL)");
    source.close();
    const shifted = await apply(dir, join(dir, "no-substrates"));
    const planned = await planNativeFsCarrier(dir);
    expect(planned).toHaveLength(1);
    expect(planned[0].semantic_kind).toBe("cms.objects");
    expect(planned[0].capability).toEqual({ stat: true, read: true, seek: true,
      format: "parquet", digest: planned[0].digest });
    const binding = await bindNativeFsCarrier(dir);
    expect(binding).toEqual({ state: "BOUND_VERIFIED", representations: 1 });
    const db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    const row = db.query("SELECT state,logical_path,physical_root FROM carrier_binding WHERE representation_id=? AND carrier='native-fs'")
      .get(planned[0].id);
    expect(row.state).toBe("ready_verified");
    expect(row.logical_path).toBe("parquet/cms/objects.parquet");
    db.close();
    const prefix = readNativeFsCarrier(dir, planned[0].id, 0, 4);
    expect(prefix.toString()).toBe("PAR1");
    const tail = readNativeFsCarrier(dir, planned[0].id, planned[0].bytes - 4, 4);
    expect(tail.toString()).toBe("PAR1");
    const cli = Bun.spawnSync(["bun", join(import.meta.dir, "../representation-manifold-carrier.mjs"),
      "read", "--estate", dir, "--representation-id", planned[0].id, "--offset", "0", "--length", "4"],
      { stdout: "pipe", stderr: "pipe" });
    expect(cli.exitCode).toBe(0);
    expect(cli.stdout.toString()).toBe("PAR1");
    const stage = join(dir, "stage");
    const install = Bun.spawnSync(["make", "-C", resolve(import.meta.dir, ".."),
      "install-representation-carrier", `MEMBRANE_ROOT=${stage}`], { stdout: "pipe", stderr: "pipe" });
    expect(install.exitCode).toBe(0);
    const stagedRead = Bun.spawnSync(["bun", join(stage, "scripts", "representation-manifold-carrier.mjs"),
      "read", "--estate", dir, "--representation-id", planned[0].id, "--offset", "0", "--length", "4"],
      { stdout: "pipe", stderr: "pipe" });
    expect(stagedRead.exitCode).toBe(0);
    expect(stagedRead.stdout.toString()).toBe("PAR1");
    const invalidCli = Bun.spawnSync(["bun", join(import.meta.dir, "../representation-manifold-carrier.mjs"),
      "read", "--estate", dir, "--representation-id", planned[0].id, "--length", "4"],
      { stdout: "pipe", stderr: "pipe" });
    expect(invalidCli.exitCode).toBe(1);
    expect(invalidCli.stderr.toString()).toContain("requires --representation-id, --offset, and --length");
    const parquet = shifted.manifest.find(row => row.kind === "cms.objects").target;
    const tamper = openSync(parquet, "r+");
    writeSync(tamper, Buffer.from([0x5a]), 0, 1, 5);
    closeSync(tamper);
    expect(() => readNativeFsCarrier(dir, planned[0].id, 0, 4)).toThrow("carrier body drift");
    expect(bindNativeFsCarrier(dir)).rejects.toThrow("canonical Parquet carrier drift");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("carrier refuses an escaped coordinate before recording any binding", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-carrier-escape-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec("CREATE TABLE objects(id INTEGER); INSERT INTO objects VALUES (1)");
    source.close();
    const shifted = await apply(dir, join(dir, "no-substrates"));
    const parquet = shifted.manifest.find(row => row.kind === "cms.objects").target;
    const link = join(dir, "outside.parquet");
    symlinkSync(parquet, link);
    const db = new Database(join(dir, "representation-manifold.db"));
    db.query("UPDATE physical_realization SET coordinate=? WHERE representation_id=(SELECT id FROM representation_instance WHERE kind='parquet' AND schema_ref='cms.objects')")
      .run(link);
    db.close();
    expect(planNativeFsCarrier(dir)).rejects.toThrow("unsafe carrier coordinate");
    expect(bindNativeFsCarrier(dir)).rejects.toThrow("unsafe carrier coordinate");
    const check = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    expect(check.query("SELECT count(*) n FROM carrier_binding WHERE carrier='native-fs'").get().n).toBe(0);
    check.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
