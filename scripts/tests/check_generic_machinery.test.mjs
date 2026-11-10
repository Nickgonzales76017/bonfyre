import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { check } from "../../architecture/check_generic_machinery.mjs";

const root = mkdtempSync(join(tmpdir(), "generic-machinery-"));
afterAll(() => rmSync(root, { recursive: true, force: true }));
mkdirSync(join(root, "architecture/institutional"), { recursive: true });
mkdirSync(join(root, "packs/demo"), { recursive: true });
mkdirSync(join(root, "scripts/tests"), { recursive: true });
writeFileSync(join(root, "architecture/institutional/namespaces.yaff"),
  "namespace demo/managed\nnamespace demo/managed/example-item\n  product_pack packs/demo/contract.yaff\n");
writeFileSync(join(root, "packs/demo/contract.yaff"), "table tblExample\n");

test("the generic guard derives products and tables from declarations", () => {
  writeFileSync(join(root, "scripts/consumer.mjs"), "export const load = pack => pack.subject;\n");
  expect(check(root)).toMatchObject({ products: 1, tables: 1, violations: [] });
});

test("code and filenames carrying product grammar fail while tests and declarations remain exempt", () => {
  writeFileSync(join(root, "scripts/consumer.mjs"), "export const table = 'tblExample';\n");
  writeFileSync(join(root, "scripts/example_item.c"), "int value = 1;\n");
  writeFileSync(join(root, "scripts/tests/fixture.mjs"), "const table = 'tblExample';\n");
  expect(check(root).violations).toEqual([
    ["scripts/consumer.mjs", ["names a declared table 'tblExample'"]],
    ["scripts/example_item.c", ["filename names product 'example-item'"]],
  ]);
});
