#!/usr/bin/env bun
// Contract names and legacy table grammar belong in packs, not generic machinery.
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const exemptSuffix = [".yaff", ".json", ".md", ".pyc", ".so", ".archive", ".bak"];
const excludedDirs = new Set(["__pycache__", ".git", "tests", "test"]);

export function declarations(source, home = root) {
  const products = new Set(), packs = new Set();
  let namespace;
  for (const raw of readFileSync(source, "utf8").split(/\r?\n/)) {
    if (raw.startsWith("namespace ")) {
      namespace = raw.slice(10).trim();
      const parts = namespace.split("/");
      if (parts.at(-2) === "managed") products.add(parts.at(-1));
    }
    else if (namespace && raw.trimStart().startsWith("product_pack ")) {
      const leaf = namespace.split("/").at(-1);
      if (leaf) products.add(leaf);
      packs.add(resolve(home, raw.trim().slice("product_pack ".length).trim()));
    }
  }
  const tables = new Set();
  for (const pack of packs) {
    if (!existsSync(pack)) continue;
    for (const raw of readFileSync(pack, "utf8").split(/\r?\n/)) {
      const line = raw.trim();
      if (!/^(table|reconcile_entity|reconcile_allocation)\s/.test(line)) continue;
      for (const token of line.split(/\s+/, 2)[1]?.split(/[ ,]+/) ?? [])
        if (/^(tbl|tlkp)/.test(token)) tables.add(token);
      if (line.startsWith("table ")) for (const token of line.slice(6).split(/[ ,]+/))
        if (/^(tbl|tlkp)/.test(token)) tables.add(token);
    }
  }
  return { products, tables };
}

function* files(base) {
  for (const entry of readdirSync(base, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!excludedDirs.has(entry.name)) yield* files(join(base, entry.name));
    } else if (entry.isFile() && !exemptSuffix.some(suffix => entry.name.endsWith(suffix)))
      yield join(base, entry.name);
  }
}

function proseAt(body, index) {
  const lineStart = body.lastIndexOf("\n", index - 1) + 1;
  const line = body.slice(lineStart, body.indexOf("\n", index) < 0 ? undefined : body.indexOf("\n", index)).trimStart();
  if (/^(#|\*|\/\/)/.test(line)) return true;
  const prior = body.slice(0, index);
  return ["'''", '"""'].some(quote => prior.split(quote).length % 2 === 0);
}

export function violationsFor(path, products, tables) {
  const name = basename(path), body = readFileSync(path, "utf8"), hits = new Set();
  for (const product of products) {
    const spelling = product.replaceAll("-", "_");
    if (name.includes(product) || name.includes(spelling)) hits.add(`filename names product '${product}'`);
    const pattern = new RegExp(product.split(/[-_]/).map(part => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("[-_]"), "g");
    for (const match of body.matchAll(pattern)) {
      if (!proseAt(body, match.index)) { hits.add(`names product '${product}'`); break; }
    }
  }
  for (const table of tables) {
    const pattern = new RegExp(`\\b${table.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`);
    if (pattern.test(body)) hits.add(`names a declared table '${table}'`);
  }
  return [...hits].sort();
}

export function check(home = root) {
  const { products, tables } = declarations(join(home, "architecture/institutional/namespaces.yaff"), home);
  const violations = [];
  for (const directory of ["scripts", "architecture"]) {
    const base = join(home, directory);
    if (!existsSync(base)) continue;
    for (const path of files(base)) {
      const hits = violationsFor(path, products, tables);
      if (hits.length) violations.push([relative(home, path), hits]);
    }
  }
  return { products: products.size, tables: tables.size, violations: violations.sort(([a], [b]) => a.localeCompare(b)) };
}

if (import.meta.main) {
  const report = check();
  for (const [path, hits] of report.violations) {
    console.error(`  ${path}`);
    for (const hit of hits) console.error(`      ${hit}`);
  }
  if (report.violations.length) {
    console.error(`generic machinery names ${report.violations.length} declared product(s); move the grammar into the pack`);
    process.exitCode = 1;
  } else console.log(`generic machinery names no declared product (${report.products} products, ${report.tables} declared tables checked)`);
}
