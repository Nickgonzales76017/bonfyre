import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:net";
import { once } from "node:events";
import { dirname, join } from "node:path";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const repoRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const noCorpus = () => !existsSync(join(repoRoot, "estate/generated/external-estate-v4/observed-external-estate.jsonl"));
const serverPath = join(repoRoot, "Aurekai", "src", "external-estate-server.mjs");

async function freePort() {
  const probe = createServer();
  probe.listen(0, "127.0.0.1");
  await once(probe, "listening");
  const { port } = probe.address();
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

async function startPublicEstate(extraEnv = {}) {
  const port = await freePort();
  const child = spawn(process.execPath, [serverPath], {
    cwd: repoRoot,
    env: { ...process.env, ...extraEnv, AUREKAI_EXTERNAL_ESTATE_PORT: String(port) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let startup = "";
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => { startup += chunk; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`external-estate server did not start: ${startup}`)), 5000);
    child.once("error", reject);
    const ready = () => {
      if (!startup.includes("external-estate service on")) return;
      clearTimeout(timer);
      child.stdout.off("data", ready);
      resolve();
    };
    child.stdout.on("data", ready);
    child.once("exit", (code) => reject(new Error(`external-estate server exited ${code}: ${startup}`)));
  });
  return { child, origin: `http://127.0.0.1:${port}` };
}

test("public World ships one cacheable closure through HTML and JSON", async (t) => {
  if (noCorpus()) return t.skip("machine-local corpus absent");
  const { child, origin } = await startPublicEstate();
  t.after(() => { if (!child.killed) child.kill("SIGTERM"); });
  const path = "/external-estate/worlds/farm-cold-chain-household";
  const human = await fetch(`${origin}${path}`, { headers: { Host: "farm.aurekai.live" } });
  assert.equal(human.status, 200);
  assert.match(human.headers.get("cache-control") || "", /public/);
  const digest = human.headers.get("x-bonfyre-closure-digest");
  assert.match(digest || "", /^sha256:[a-f0-9]{64}$/);
  const page = await human.text();
  assert.ok(page.includes(digest));

  const machine = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/closure`, { headers: { Host: "farm.aurekai.live" } });
  assert.equal(machine.status, 200);
  assert.equal(machine.headers.get("x-bonfyre-closure-digest"), digest);
  const closure = await machine.json();
  assert.equal(closure.closure_digest, digest);
  assert.equal(closure.occurrence_count, 131);
  assert.ok(closure.lenses.length > 0);
  assert.match(closure.formation.formation_digest, /^sha256:[a-f0-9]{64}$/);

  const formation = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/formation`);
  assert.equal(formation.status, 200);
  assert.equal(formation.headers.get("x-bonfyre-closure-digest"), closure.formation.formation_digest);
  const formationBody = await formation.json();
  assert.equal(formationBody.truth_level, "observed_candidate");
  assert.ok(formationBody.recipes.some((recipe) => recipe.id === "living-publication"));

  const formationCatalogue = await fetch(`${origin}/external-estate/v1/formations`);
  assert.equal(formationCatalogue.status, 200);
  const formationCatalogueBody = await formationCatalogue.json();
  assert.equal(formationCatalogueBody.world_count, 30);
  assert.equal(formationCatalogueBody.formations.length, 30);
  assert.match(formationCatalogue.headers.get("x-bonfyre-closure-digest"), /^sha256:[a-f0-9]{64}$/);

  const bridges = await fetch(`${origin}/external-estate/v1/worlds/school-township/bridges`);
  assert.equal(bridges.status, 200);
  const bridgesBody = await bridges.json();
  assert.ok(bridgesBody.bridge_count > 0);
  assert.ok(bridgesBody.bridges.some((bridge) => bridge.world === "household-daily-life"));
  assert.match(bridges.headers.get("x-bonfyre-closure-digest"), /^sha256:[a-f0-9]{64}$/);

  const sceneResponse = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/scene`, { headers: { Host: "farm.aurekai.live" } });
  assert.equal(sceneResponse.status, 200);
  assert.equal(sceneResponse.headers.get("x-bonfyre-closure-digest"), digest);
  const scene = await sceneResponse.json();
  assert.equal(scene.schema, "bonfyre-surface-scene.v1");
  assert.equal(scene.source_digest, digest);
  assert.equal(scene.runtime_queries.public_scene, "/external-estate/v1/worlds/farm-cold-chain-household/scene");

  const cachedScene = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/scene`, { headers: { "If-None-Match": sceneResponse.headers.get("etag") } });
  assert.equal(cachedScene.status, 304);
  assert.equal(await cachedScene.text(), "");

  const cached = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/closure`, { headers: { "If-None-Match": human.headers.get("etag") } });
  assert.equal(cached.status, 304);
  assert.equal(await cached.text(), "");

  const lensHuman = await fetch(`${origin}/external-estate/worlds/farm-cold-chain-household/listings`, { headers: { Host: "farm.aurekai.shop" } });
  assert.equal(lensHuman.status, 200);
  const lensDigest = lensHuman.headers.get("x-bonfyre-closure-digest");
  assert.notEqual(lensDigest, digest, "a focused lens is a distinct addressed closure");
  assert.ok((await lensHuman.text()).includes(lensDigest));
  const lensMachine = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/listings/closure`, { headers: { Host: "farm.aurekai.shop" } });
  assert.equal(lensMachine.status, 200);
  assert.equal(lensMachine.headers.get("x-bonfyre-closure-digest"), lensDigest);
  const lens = await lensMachine.json();
  assert.equal(lens.view, "lens");
  assert.deepEqual(lens.lenses.map((entry) => entry.grammar), ["listings"]);

  const invalid = await fetch(`${origin}/external-estate/worlds/farm-cold-chain-household/not-a-grammar`);
  assert.equal(invalid.status, 400);
  assert.deepEqual((await invalid.json()).invalid_coordinates, ["grammar"]);
});

test("product map exposes every World and lens route through one digest", async (t) => {
  if (noCorpus()) return t.skip("machine-local corpus absent");
  const { child, origin } = await startPublicEstate();
  t.after(() => { if (!child.killed) child.kill("SIGTERM"); });
  const human = await fetch(`${origin}/external-estate/worlds`);
  assert.equal(human.status, 200);
  const digest = human.headers.get("x-bonfyre-closure-digest");
  const page = await human.text();
  assert.ok(page.includes(digest));
  assert.match(page, /450 focused product apertures/);
  const machine = await fetch(`${origin}/external-estate/v1/worlds`);
  assert.equal(machine.status, 200);
  assert.equal(machine.headers.get("x-bonfyre-closure-digest"), digest);
  const catalogue = await machine.json();
  assert.equal(catalogue.closure_digest, digest);
  assert.equal(catalogue.world_count, 30);
  assert.equal(catalogue.lens_route_count, 450);
});

test("production World carrier can require the native forwarding witness", async (t) => {
  if (noCorpus()) return t.skip("machine-local corpus absent");
  const addressIndexPath = join(process.env.HOME || "", ".bonfyre", "estate", "estate.db");
  const addressPlaneBinary = join(repoRoot, "cmd", "BonfyreExternalEstate", "bonfyre-external-estate");
  if (!existsSync(addressIndexPath) || !existsSync(addressPlaneBinary)) {
    t.skip("local native AddressPlane estate is not installed");
    return;
  }
  const { child, origin } = await startPublicEstate({
    BONFYRE_ADDRESS_INDEX: addressIndexPath,
    BONFYRE_ADDRESSPLANE_BINARY: addressPlaneBinary,
    BONFYRE_WORLD_REQUIRE_NATIVE_FORWARDING: "1",
  });
  t.after(() => { if (!child.killed) child.kill("SIGTERM"); });
  const response = await fetch(`${origin}/external-estate/worlds/farm-cold-chain-household`, { headers: { Host: "farm.aurekai.live" } });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.match(body, /native AddressPlane mask: 131 eligible survivors; 4760 rejected before exact ownership/);

  const closure = await fetch(`${origin}/external-estate/v1/worlds/farm-cold-chain-household/closure`);
  assert.equal(closure.status, 200);
  assert.equal((await closure.json()).native_forwarding.state, "verified");
});

test("a required native forwarding witness fails closed instead of falling back to a catalogue route", async (t) => {
  if (noCorpus()) return t.skip("machine-local corpus absent");
  const { child, origin } = await startPublicEstate({
    BONFYRE_ADDRESS_INDEX: "/definitely/not/an/address-plane.db",
    BONFYRE_WORLD_REQUIRE_NATIVE_FORWARDING: "1",
  });
  t.after(() => { if (!child.killed) child.kill("SIGTERM"); });
  const response = await fetch(`${origin}/external-estate/worlds/farm-cold-chain-household`);
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error, "native_forwarding_unavailable");
});
