import { afterAll, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { inspect } from "../inspect_foreign_field.mjs";

const root = mkdtempSync(join(tmpdir(), "foreign-field-"));
afterAll(() => rmSync(root, { recursive: true, force: true }));
const field = join(root, "field");
mkdirSync(field);
writeFileSync(join(field, ".zarray"), JSON.stringify({ shape: [2, 2], dtype: "<i4", compressor: null }));
writeFileSync(join(field, ".zattrs"), JSON.stringify({ "bonfyre:derivedFrom": { digest: "abc" },
  "bonfyre:derivation": "lower", "bonfyre:frontier": "seen" }));
const bytes = Buffer.alloc(16);
for (const [index, value] of [1, 2, -3, 8].entries()) bytes.writeInt32LE(value, index * 4);
writeFileSync(join(field, "0.0"), bytes);

test("foreign field inspector checks exact shape, sum and lineage", () => {
  expect(inspect(field)).toEqual({ shape: [2, 2], total: 8, derived_from_digest: "abc", has_derivation_frontier: true });
});

test("truncated chunk is refused, not counted as a smaller field", () => {
  writeFileSync(join(field, "0.0"), bytes.subarray(0, 12));
  expect(() => inspect(field)).toThrow("chunk length");
});
