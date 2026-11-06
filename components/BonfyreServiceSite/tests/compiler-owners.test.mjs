// P2 boundary witness: existing imports remain compatible with explicit owners.
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { kinds as registry } from "../section-registry.mjs";
import { kinds, pagesOf, validate } from "../site-contract.mjs";
import { renderSite, renderPage } from "../site-renderer.mjs";
import * as oldEntry from "../build.mjs";

const record = JSON.parse(readFileSync(join(import.meta.dir, "fixtures/sample-lawn/content.json"), "utf8"));

test("one registry and one contract owner serve both entrypoints", () => {
  expect(oldEntry.kinds).toBe(registry);
  expect(kinds).toBe(registry);
  expect(oldEntry.pagesOf).toBe(pagesOf);
  expect(oldEntry.validate).toBe(validate);
  expect(oldEntry.renderSite).toBe(renderSite);
  expect(oldEntry.renderPage).toBe(renderPage);
  expect(pagesOf(record)[0].path).toBe("");
  expect(Object.keys(kinds)).toContain("hero");
});

test("rendering is independent of filesystem publishing", () => {
  expect(() => validate(record)).not.toThrow();
  const output = renderSite(record);
  expect(output.pages.length).toBeGreaterThan(0);
  expect(output.pages[0].html).toContain("Sample Lawn Care");
  expect(renderPage(record).html).toBe(output.pages[0].html);
  expect(output.pages[0].kinds).toContain("hero");
  expect(output.forms).toHaveProperty("quote");
});

test("the contract refuses broken content before rendering", () => {
  const broken = structuredClone(record);
  broken.pages = [{ path: "../outside", title: "Wrong", menu: "Wrong", sections: [{ kind: "hero", headline: "Wrong" }] }];
  expect(() => validate(broken)).toThrow(/path must be lower-case/);
  const unknown = structuredClone(record);
  unknown.sections[0].kind = "foreign-unregistered-kind";
  expect(() => validate(unknown)).toThrow(/the kit has/);
});

test("compiler modules have a single textual owner per responsibility", () => {
  const read = name => readFileSync(join(import.meta.dir, "..", name), "utf8");
  expect(read("build.mjs")).not.toContain("function validate(");
  expect(read("build.mjs")).not.toContain("function renderSite(");
  expect(read("build.mjs")).not.toContain("function compileCss(");
  expect(read("build.mjs")).not.toContain("function withMaterial(");
  expect(read("site-contract.mjs")).toContain("export function validate(");
  expect(read("site-renderer.mjs")).toContain("export function renderSite(");
  expect(read("site-material.mjs")).toContain("export async function withMaterial(");
  expect(read("site-styles.mjs")).toContain("export function compileCss(");
});
