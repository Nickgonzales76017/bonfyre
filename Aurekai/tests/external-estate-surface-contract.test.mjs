import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { compileExternalSurfaceSlice } from "../src/external-estate-runtime.mjs";

const repoRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const read = (path) => readFileSync(join(repoRoot, path), "utf8");

test("public Discover is a native PageProgram rather than a browser candidate catalogue", () => {
  const composition = read("schemas/control/page-composition.cue");
  const address = read("cmd/BonfyreWebAddress/src/main.c");
  assert.match(composition, /route: "\/discover", title: "Discover"/);
  assert.match(composition, /families: \["news", "attention", "wikimedia", "books", "space", "history", "institutions", "datasets", "live-music", "commerce", "gaming", "vulnerabilities", "food", "science", "sports"\]/);
  assert.match(address, /if \(emit_page_program_route\(root, target, host, query\)\) return 1;/);
  assert.doesNotMatch(address, /serve_composed_public_route/);
});

test("external material remains an explicitly unadmitted candidate slice", () => {
  const slice = compileExternalSurfaceSlice(repoRoot, { grammar: "observations", extent: "section" });
  assert.equal(slice.schema_version, "aurekai.external_estate.surface_candidate_slice.v1");
  assert.equal(slice.truth_level, "observed_candidate");
  assert.equal(slice.actor_closure, "not accepted or resolved by external candidate lane");
  assert.ok(slice.forbidden_inferences.includes(
    "candidate slice membership -> provider readiness, admitted evidence, institutional subject, or lawful data combination",
  ));
});

test("external candidates are not client-rendered into a second browser application", () => {
  const address = read("cmd/BonfyreWebAddress/src/main.c");
  const account = read("site/matrix/surfaces/account-native.html");
  /* The compatibility asset may be served, but no public page dispatcher
   * invokes a bridge renderer or browser-side ExternalEstate composition. */
  assert.match(address, /"\/assets\/bonfyre-surface-bridge\.js"/);
  assert.doesNotMatch(address, /serve_composed_public_route/);
  assert.doesNotMatch(address, /external-observation-candidates/);
  assert.doesNotMatch(address, /fetch\('\/external-estate/);
  assert.doesNotMatch(address, /localStorage\.getItem\('bonfyre_token'\)/);
  assert.doesNotMatch(account, /localStorage/);
});

test("served Matrix surfaces never use a browser-stored bearer as session proof", () => {
  const surfaces = readdirSync(join(repoRoot, "site/matrix/surfaces"))
    .filter((entry) => entry.endsWith(".html"));
  for (const surface of surfaces) {
    assert.doesNotMatch(read(`site/matrix/surfaces/${surface}`), /localStorage[^\n]*bonfyre_token|bonfyre_token[^\n]*localStorage/, surface);
  }
  assert.doesNotMatch(read("site/matrix/bonfyre-kit.js"), /localStorage[^\n]*bonfyre_token|bonfyre_token[^\n]*localStorage/);
  assert.doesNotMatch(read("site/matrix/surfaces/_kit.js"), /localStorage[^\n]*bonfyre_token|bonfyre_token[^\n]*localStorage/);
});

test("the public service exposes grammar slices through a dedicated route", () => {
  const server = read("Aurekai/src/external-estate-server.mjs");
  assert.match(server, /compileExternalSurfaceSlice/);
  assert.match(server, /path\.startsWith\(`\$\{api\}\/surfaces\/`\)/);
  assert.match(server, /error\.code/);
});
