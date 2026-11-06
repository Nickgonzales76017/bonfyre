import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { assertExistingZarrReceipt } from "../representation-manifold-zarr.mjs";

test("existing Zarr bytes cannot be re-receipted after mutation or compiler drift", () => {
  const db = new Database(":memory:");
  try {
    db.exec(`CREATE TABLE transformation_occurrence (
      transform TEXT, output_ref TEXT, proof_state TEXT,
      input_digest TEXT, output_digest TEXT, compiler TEXT)`);
    const receipt = (input, output, compiler) => db.query(`INSERT INTO transformation_occurrence
      VALUES('parquet->zarr-numeric','/fixture.zarr','materialized',?,?,?)`).run(input, output, compiler);
    expect(() => assertExistingZarrReceipt(db, "/fixture.zarr", "input", "original", "compiler-a"))
      .toThrow("exact materialization receipt");
    receipt("input", "original", "compiler-a");
    expect(() => assertExistingZarrReceipt(db, "/fixture.zarr", "input", "original", "compiler-a"))
      .not.toThrow();
    expect(() => assertExistingZarrReceipt(db, "/fixture.zarr", "input", "mutated", "compiler-a"))
      .toThrow("exact materialization receipt");
    expect(() => assertExistingZarrReceipt(db, "/fixture.zarr", "input", "original", "compiler-b"))
      .toThrow("exact materialization receipt");
    expect(() => assertExistingZarrReceipt(db, "/fixture.zarr", "different-input", "original", "compiler-a"))
      .toThrow("exact materialization receipt");
    receipt("input", "mutated", "compiler-a");
    expect(() => assertExistingZarrReceipt(db, "/fixture.zarr", "input", "original", "compiler-a"))
      .toThrow("exact materialization receipt");
  } finally { db.close(); }
});
