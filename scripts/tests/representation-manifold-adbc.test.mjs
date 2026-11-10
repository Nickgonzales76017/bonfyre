import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync, readFileSync, writeFileSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { probeAdbcDriver, bindAdbcDriver } from "../representation-manifold-adbc.mjs";

const source = join(import.meta.dir, "../../cmd/BonfyreADBCProbe/src/main.c");
const schema = join(import.meta.dir, "../../schemas/storage/representation-manifold.sql");
const header = process.env.ADBC_TEST_INCLUDE;
const driver = process.env.ADBC_TEST_DRIVER;

test.skipIf(!header || !driver)("native ADBC Flight SQL driver is ABI-probed and registered unserved", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-adbc-test-"));
  try {
    const probe = join(dir, "probe");
    const cc = Bun.spawnSync(["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-I", header,
      source, "-o", probe], { stdout: "pipe", stderr: "pipe" });
    expect(cc.exitCode).toBe(0);
    const receipt = await probeAdbcDriver({ driver, probe });
    expect(receipt.state).toBe("driver_initialized_unserved");
    expect([1000000, 1001000]).toContain(receipt.abi_version);
    expect(receipt.driver_digest).toMatch(/^[a-f0-9]{64}$/);
    const db = new Database(join(dir, "representation-manifold.db"));
    db.exec(readFileSync(schema, "utf8"));
    db.query("INSERT INTO metadata_fabric VALUES(?,?,?,?,?)").run("adbc-flight-sql-client",
      "python://adbc_driver_flightsql", "client_ready", "legacy import-only receipt", "2026-01-01");
    db.close();
    await bindAdbcDriver(dir, { driver, probe });
    const bound = new Database(join(dir, "representation-manifold.db"));
    const row = bound.query("select coordinate,state,detail from metadata_fabric where kind='adbc-flight-sql-client'").get();
    expect(row.state).toBe("driver_initialized_unserved");
    expect(row.coordinate.startsWith("file://")).toBe(true);
    expect(JSON.parse(row.detail).driver_digest).toBe(receipt.driver_digest);
    expect(JSON.parse(row.detail).limitation).toContain("no SQL execution");
    expect(bound.query("select count(*) as n from metadata_fabric where kind='adbc-flight-sql-client'").get().n).toBe(1);
    bound.close();
    const cli = Bun.spawnSync([process.execPath, join(import.meta.dir, "../representation-manifold-adbc.mjs"),
      "bind", "--estate", dir, "--driver", driver, "--probe", probe],
      { stdout: "pipe", stderr: "pipe" });
    expect(cli.exitCode).toBe(0);
    expect(JSON.parse(cli.stdout.toString()).state).toBe("driver_initialized_unserved");
    const fake = join(dir, "fake.so"); writeFileSync(fake, "not a native driver");
    expect(probeAdbcDriver({ driver: fake, probe })).rejects.toThrow("native ADBC ABI probe failed");
    const linked = join(dir, "linked.so"); symlinkSync(driver, linked);
    expect(probeAdbcDriver({ driver: linked, probe })).rejects.toThrow("non-symlink");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("ADBC admission requires explicit native artifacts", () => {
  expect(probeAdbcDriver({})).rejects.toThrow("explicit --driver and --probe");
});
