import { test, expect } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dir, "../..");
const registeredOwners = [...readFileSync(resolve(root, "architecture/commands/native-dispatch.yaff"), "utf8")
  .matchAll(/^  source_path (cmd\/[^\s]+\/src\/main\.c)$/gm)]
  .map((match) => match[1])
  .sort();
const tableOwners = [...new Bun.Glob("cmd/**/src/main.c").scanSync({ cwd: root })]
  .map((path) => ({ path, source: readFileSync(resolve(root, path), "utf8") }))
  .flatMap(({ path, source }) => [...source.matchAll(/^#define ([A-Z_]+_COMMANDS)\(X\)/gm)]
    .map((match) => ({ path, source, macro: match[1] })));

test("the atlas registers every indexed command owner and keeps its other native evidence valid", () => {
  expect(new Set(registeredOwners).size).toBe(registeredOwners.length);
  for (const owner of tableOwners) expect(registeredOwners).toContain(owner.path);
  expect(registeredOwners).toContain("cmd/BonfyreRuntime/src/main.c");
  for (const path of registeredOwners) expect(existsSync(resolve(root, path))).toBe(true);
});

for (const { path, source, macro } of tableOwners) {
  test(`${macro} is one sorted, complete verb index`, () => {
    const start = source.indexOf(`#define ${macro}(X)`);
    expect(start).toBeGreaterThan(-1);
    const end = source.indexOf("\n\n", start);
    expect(end).toBeGreaterThan(start);
    const rows = [...source.slice(start, end).matchAll(/X\("([^"]+)",\s*([A-Z][A-Z0-9_]+)/g)]
      .map((match) => ({ name: match[1], id: match[2] }));
    expect(rows.length).toBeGreaterThan(1);
    const names = rows.map((row) => row.name);
    expect(names).toEqual([...names].sort());
    expect(new Set(names).size).toBe(names.length);
    expect(new Set(rows.map((row) => row.id)).size).toBe(rows.length);
    expect(source).toContain(`bf_command_lookup(`);
    expect(source).not.toMatch(/strcmp\(argv\[1\]/);
    for (const row of rows) {
      expect(source.includes(`case ${row.id}:`) || source.includes(`command->id == ${row.id}`) || source.includes(`CMS_IS(${row.id})`)).toBe(true);
    }
  });
}

test("Runtime indexes its verbs and resolves delegated binaries on demand", () => {
  const source = readFileSync(resolve(root, "cmd/BonfyreRuntime/src/main.c"), "utf8");
  const table = source.match(/static const BfCommandEntry runtime_commands\[\] = \{([\s\S]*?)\n\};/);
  expect(table).not.toBeNull();
  const rows = [...table[1].matchAll(/\{"([^"]+)",\s*(R[BC]_[A-Z_]+),\s*(RC_[A-Z]+),\s*NULL\}/g)]
    .map((match) => ({ name: match[1], id: match[2], mode: match[3] }));
  expect(rows).toHaveLength(28);
  const names = rows.map((row) => row.name);
  expect(names).toEqual([...names].sort());
  expect(new Set(names).size).toBe(names.length);
  expect(new Set(rows.map((row) => row.id)).size).toBe(rows.length);
  expect(rows.filter((row) => row.mode === "RC_DELEGATE")).toHaveLength(13);
  expect(source).toContain("bf_command_lookup(runtime_commands,");
  expect(source).not.toMatch(/strcmp\(argv\[1\]/);
  expect(source).toContain("runtime_delegate(runtime_binary_get(&binary_cache, command->id)");
  for (const row of rows.filter((row) => row.mode === "RC_SPECIAL")) {
    expect(source).toContain(`RUNTIME_IS(${row.id})`);
  }
});
