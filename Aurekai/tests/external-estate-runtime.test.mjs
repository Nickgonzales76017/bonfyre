import test from "node:test";
import assert from "node:assert/strict";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chmodSync, cpSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import {
  clearExternalEstateCache,
  compileExternalSurfaceSlice,
  compileExternalWorldFormation,
  compileExternalWorldFormationCatalogue,
  compileExternalWorldBridges,
  compileExternalWorldClosure,
  compileExternalWorldCatalogue,
  composeExternalWorld,
  externalEstateManifest,
  getExternalOccurrence,
  listExternalEstate,
  presentationCellCatalog,
  probeExternalOccurrence,
  renderExternalOccurrenceHtml,
  renderExternalProbeHtml,
  renderExternalEstateHtml,
  renderExternalWorldHtml,
  renderExternalWorldCatalogueHtml,
  reagentRolesForRecord,
  territoryFromAddressHost,
  validateExternalSurfaceSlice,
} from "../src/external-estate-runtime.mjs";
import { PRESENTATION_CELLS, presentationCell, validatePresentationCells } from "../src/presentation-cell-catalog.mjs";

const repoRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));

test("the live-query corpus preserves every supplied external occurrence", () => {
  clearExternalEstateCache();
  const manifest = externalEstateManifest(repoRoot);
  assert.equal(manifest.counts.external_occurrences, 4891);
  assert.equal(manifest.counts.non_protocol_occurrences, 4152);
  assert.equal(manifest.counts.protocol_occurrences, 739);
  assert.equal(manifest.counts.sources_in_2_or_more_worlds, 1107);
});

test("a rebuilt corpus invalidates only disposable public candidate indexes", () => {
  const root = mkdtempSync(join(tmpdir(), "aurekai-estate-refresh-"));
  const corpusPath = join(root, "observed.jsonl");
  const manifestPath = join(root, "manifest.json");
  const sourceCorpus = join(repoRoot, "estate/generated/external-estate-v4/observed-external-estate.jsonl");
  const sourceManifest = join(repoRoot, "estate/generated/external-estate-v4/manifest.json");
  try {
    cpSync(sourceCorpus, corpusPath);
    cpSync(sourceManifest, manifestPath);
    const before = getExternalOccurrence(repoRoot, "EXT-00001", { corpusPath, manifestPath });
    const next = readFileSync(corpusPath, "utf8").replace("x402 Protocol Specification", "x402 Protocol Specification Revised");
    writeFileSync(corpusPath, next);
    const after = getExternalOccurrence(repoRoot, "EXT-00001", { corpusPath, manifestPath });
    assert.equal(before.observed_name, "x402 Protocol Specification");
    assert.equal(after.observed_name, "x402 Protocol Specification Revised");
  } finally {
    rmSync(root, { recursive: true, force: true });
    clearExternalEstateCache();
  }
});

test("occurrences remain source-derived candidates rather than provider assertions", () => {
  const x402 = getExternalOccurrence(repoRoot, "EXT-00001");
  assert.equal(x402.id, "external:ext-00001");
  assert.equal(x402.truth_level, "observed_candidate");
  assert.ok(x402.raw_source_text.includes("EXT-00001"));
  assert.ok(x402.forbidden_inferences.includes("listing -> verified capability"));
});

test("external occurrences derive typed reagent roles without changing source truth", () => {
  const figma = getExternalOccurrence(repoRoot, "EXT-03014");
  const spotify = getExternalOccurrence(repoRoot, "EXT-01974");
  const protocol = getExternalOccurrence(repoRoot, "EXT-04153");
  assert.ok(reagentRolesForRecord(figma).includes("design_reference"));
  assert.ok(reagentRolesForRecord(figma).includes("compiler_tool"));
  assert.ok(reagentRolesForRecord(spotify).includes("interaction_reference"));
  assert.ok(reagentRolesForRecord(spotify).includes("dataset"));
  assert.ok(reagentRolesForRecord(protocol).includes("transport_carrier"));
  assert.equal(figma.truth_level, "observed_candidate");
  assert.equal(spotify.truth_level, "observed_candidate");
  assert.equal(protocol.truth_level, "observed_candidate");
});

test("every documented EXT identifier resolves to exactly one inspectable occurrence", () => {
  const sourceRefs = new Set();
  for (let n = 1; n <= 4891; n += 1) {
    const ref = `EXT-${String(n).padStart(5, "0")}`;
    const record = getExternalOccurrence(repoRoot, ref);
    assert.ok(record, `${ref} is absent from the live-query corpus`);
    assert.equal(record.source_ref, ref);
    assert.ok(record.raw_source_text.length > 0, `${ref} lost its supplied source line`);
    assert.ok(Array.isArray(record.projection.candidate_worlds), `${ref} lost its declared World coordinate`);
    sourceRefs.add(record.id);
  }
  assert.equal(sourceRefs.size, 4891, "two documented source occurrences collapsed into one inspectable record");
});

test("one source graph can be queried horizontally through a human World", () => {
  const composition = composeExternalWorld(repoRoot, "farm-cold-chain-household", { limit: 30 });
  assert.equal(composition.truth_level, "observed_candidate");
  assert.ok(composition.occurrence_count > 100);
  assert.ok(composition.occurrences.every((record) => record.projection.candidate_worlds.includes("farm-cold-chain-household")));
  assert.ok(composition.required_followups.includes("CapabilityClosure"));
});

test("a World compiles formation candidates from distinct observed signals without launching anything", () => {
  const formation = compileExternalWorldFormation(repoRoot, "farm-cold-chain-household");
  const repeated = compileExternalWorldFormation(repoRoot, "farm-cold-chain-household");
  assert.equal(formation.truth_level, "observed_candidate");
  assert.equal(formation.occurrence_count, 131);
  assert.match(formation.formation_digest, /^sha256:[a-f0-9]{64}$/);
  assert.ok(formation.recipes.length >= 6);
  const publication = formation.recipes.find((recipe) => recipe.id === "living-publication");
  assert.ok(publication.signal_evidence.some((signal) => signal.signal === "material" && signal.occurrence_count > 0));
  assert.deepEqual(publication.possible_operator_archetypes, ["newsletter-writer", "forum-maintainer", "supporter-creator"]);
  assert.ok(publication.possible_project_facets.includes("recruiting"));
  assert.ok(formation.recipes.every((recipe) => /formation_candidate_/.test(recipe.state)));
  assert.ok(formation.forbidden_inferences.some((limit) => limit.includes("market")));
  assert.equal(repeated, formation, "a stable corpus reuses only the disposable formation projection");
  const closure = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household");
  assert.equal(closure.formation.formation_digest, formation.formation_digest);
  assert.match(renderExternalWorldHtml(repoRoot, "farm-cold-chain-household"), /Formation candidates, not launches/);
  assert.match(renderExternalWorldHtml(repoRoot, "farm-cold-chain-household"), /Possible operators/);
  assert.match(renderExternalWorldHtml(repoRoot, "farm-cold-chain-household"), /Possible project facets/);
});

test("formation roles are only possible operators and remain aligned to the native operator declaration", () => {
  const formation = compileExternalWorldFormation(repoRoot, "school-township");
  const nativeOperators = readFileSync(join(repoRoot, "cmd/BonfyreWebAddress/generated/native_operators.h"), "utf8");
  for (const recipe of formation.recipes) {
    for (const archetype of recipe.possible_operator_archetypes) {
      assert.match(nativeOperators, new RegExp(`\\{\\"${archetype}\\"`), `${archetype} must be a generated native operator archetype`);
    }
  }
  assert.ok(formation.recipes.every((recipe) => recipe.cannot_infer.includes("person in a role")));
});

test("the entire declared World estate compiles an inspectable formation census without prioritizing or launching any World", () => {
  const catalogue = compileExternalWorldFormationCatalogue(repoRoot);
  assert.equal(catalogue.truth_level, "observed_candidate");
  assert.equal(catalogue.world_count, 30);
  assert.equal(catalogue.formation_kind_count, 6);
  assert.equal(catalogue.formations.length, 30);
  assert.match(catalogue.closure_digest, /^sha256:[a-f0-9]{64}$/);
  assert.ok(catalogue.formations.every((formation) => formation.occurrence_count > 0));
  assert.ok(catalogue.formations.every((formation) => formation.formation_digest.startsWith("sha256:")));
  assert.ok(catalogue.forbidden_inferences.some((limit) => limit.includes("launched product")));
});

test("a World can expose exact cross-World reagent overlaps without inventing a relationship or shared institution", () => {
  const bridges = compileExternalWorldBridges(repoRoot, "school-township");
  assert.equal(bridges.truth_level, "observed_candidate");
  assert.ok(bridges.bridge_count > 0);
  assert.match(bridges.bridge_digest, /^sha256:[a-f0-9]{64}$/);
  assert.ok(bridges.bridges.every((bridge) => bridge.state === "shared_observed_reagent_membership"));
  assert.ok(bridges.bridges.every((bridge) => bridge.shared_occurrence_count === bridge.shared_occurrence_ids.length));
  assert.ok(bridges.bridges.some((bridge) => bridge.world === "household-daily-life"));
  assert.ok(bridges.forbidden_inferences.some((limit) => limit.includes("lawful data combination")));
});

test("one compiled public World closure drives both human and machine carriers", () => {
  const closure = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", { host: "farm.aurekai.live" });
  const html = renderExternalWorldHtml(repoRoot, "farm-cold-chain-household", { basePath: "/external-estate", host: "farm.aurekai.live" });
  assert.equal(closure.truth_level, "observed_candidate");
  assert.equal(closure.world.resolved, "farm-cold-chain-household");
  assert.equal(closure.resolved_territory, "live");
  assert.ok(closure.occurrence_count > 100);
  assert.ok(closure.lenses.length > 0);
  assert.ok(closure.lenses.length <= 6);
  assert.ok(closure.candidate_count > 0);
  assert.match(closure.closure_digest, /^sha256:[a-f0-9]{64}$/);
  assert.match(html, new RegExp(closure.closure_digest));
  assert.match(html, /\/external-estate\/v1\/worlds\/farm-cold-chain-household\/closure/);
  assert.match(html, /\/external-estate\/v1\/worlds\/farm-cold-chain-household\/scene/);
  assert.match(html, /bonfyre:surface-reconnect/);
  assert.match(html, /Public candidate boundary/);
  assert.match(html, /method="post" action="\/external-estate\/occurrences\/external%3Aext-/);
  assert.doesNotMatch(html, /authorize an effect/i);
});

test("a configured public World is witnessed by the real native AddressPlane before presentation", (t) => {
  const addressIndexPath = join(process.env.HOME || "", ".bonfyre", "estate", "estate.db");
  const addressPlaneBinary = join(repoRoot, "cmd", "BonfyreExternalEstate", "bonfyre-external-estate");
  if (!existsSync(addressIndexPath) || !existsSync(addressPlaneBinary)) {
    t.skip("local native AddressPlane estate is not installed");
    return;
  }
  const closure = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", {
    addressIndexPath, addressPlaneBinary, requireNativeForwarding: true,
  });
  assert.equal(closure.native_forwarding.state, "verified");
  assert.equal(closure.native_forwarding.truth, "eligible_for_further_consideration");
  assert.equal(closure.native_forwarding.considered, 4891);
  assert.equal(closure.native_forwarding.returned, 131);
  assert.equal(closure.native_forwarding.rejected_before_exact, 4760);
  assert.equal(closure.native_forwarding.displayed_candidate_membership, true);
});

test("a native forwarding witness materially restricts the candidates emitted by a required closure", () => {
  const root = mkdtempSync(join(tmpdir(), "aurekai-native-forwarding-"));
  const indexPath = join(root, "address-plane.db");
  const binaryPath = join(root, "address-plane-witness");
  const unfiltered = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", { grammar: "listings" });
  const survivor = unfiltered.lenses[0].candidates[0].id;
  try {
    writeFileSync(indexPath, "fixture");
    writeFileSync(binaryPath, `#!/bin/sh\nprintf '%s\\n' '${JSON.stringify({
      truth: "eligible_for_further_consideration",
      considered: 4891,
      rejected_before_exact: 4890,
      returned: 1,
      survivors: [{ id: survivor }],
    })}'\n`);
    chmodSync(binaryPath, 0o755);
    const closure = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", {
      grammar: "listings", addressIndexPath: indexPath, addressPlaneBinary: binaryPath,
      requireNativeForwarding: true,
    });
    assert.equal(closure.native_forwarding.state, "verified");
    assert.equal(closure.native_forwarding.forwarded_candidate_count, 1);
    assert.equal(closure.native_forwarding.withheld_candidate_count, unfiltered.candidate_count - 1);
    assert.deepEqual(closure.lenses[0].candidates.map((candidate) => candidate.id), [survivor]);
    assert.equal(closure.candidate_count, 1);
    assert.ok(!Object.hasOwn(closure.native_forwarding, "_eligible_candidate_ids"));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("every compiled interaction grammar can become a focused World product lens", () => {
  const closure = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", { grammar: "listings", host: "farm.aurekai.shop" });
  const html = renderExternalWorldHtml(repoRoot, "farm-cold-chain-household", { grammar: "listings", basePath: "/external-estate", host: "farm.aurekai.shop" });
  assert.equal(closure.view, "lens");
  assert.equal(closure.lenses.length, 1);
  assert.equal(closure.lenses[0].grammar, "listings");
  assert.ok(closure.available_lenses.length >= closure.lenses.length);
  assert.match(html, /Farm Cold Chain Household · listings/);
  assert.match(html, /external-estate\/worlds\/farm-cold-chain-household\/community/);
  assert.match(html, /external-estate\/v1\/worlds\/farm-cold-chain-household\/listings\/closure/);
  assert.throws(() => compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", { grammar: "not-a-grammar" }),
    (error) => error.code === "invalid_world_lens");
});

test("a focused product lens visibly lowers its declared presentation cell instead of a route-local template", () => {
  const listings = renderExternalWorldHtml(repoRoot, "farm-cold-chain-household", {
    grammar: "listings", basePath: "/external-estate", host: "farm.aurekai.shop",
  });
  const channels = renderExternalWorldHtml(repoRoot, "farm-cold-chain-household", {
    grammar: "channels", basePath: "/external-estate", host: "farm.aurekai.live",
  });
  assert.match(listings, /renderer-listing_grid/);
  assert.match(listings, /renderer-compare_tray/);
  assert.match(listings, /<span>availability<\/span>/);
  assert.match(listings, /<span>terms<\/span>/);
  assert.match(channels, /renderer-threaded_stream/);
  assert.match(channels, /renderer-topic_index/);
  assert.match(channels, /<span>topics<\/span>/);
  assert.match(channels, /<span>participants<\/span>/);
  assert.notEqual(listings, channels, "distinct grammar cells must not lower to the same human carrier");
});

test("every public grammar lowers all of its declared renderer and section contracts", () => {
  for (const grammar of presentationCellCatalog().grammars) {
    const cell = presentationCell(grammar, "anonymous");
    const html = renderExternalWorldHtml(repoRoot, "farm-cold-chain-household", {
      grammar, basePath: "/external-estate", host: "farm.aurekai.live",
    });
    for (const renderer of cell.preferred_renderers) {
      assert.match(html, new RegExp(`renderer-${renderer}`), `${grammar} omitted ${renderer}`);
    }
    for (const section of cell.visible_sections) {
      assert.match(html, new RegExp(`<span>${section}</span>`), `${grammar} omitted ${section}`);
    }
  }
});

test("the full measured estate exposes all declared Worlds and grammar verticals as product routes", () => {
  const worlds = Object.keys(externalEstateManifest(repoRoot).candidate_world_occurrences || {});
  const grammars = presentationCellCatalog().grammars;
  const hubs = worlds.map((world) => compileExternalWorldClosure(repoRoot, world));
  assert.equal(worlds.length, 30);
  assert.ok(hubs.every((closure) => closure.available_lenses.length === grammars.length));
  assert.equal(hubs.reduce((total, closure) => total + closure.available_lenses.length, 0), 450);
  for (const grammar of grammars) {
    const lens = compileExternalWorldClosure(repoRoot, "farm-cold-chain-household", { grammar });
    assert.deepEqual(lens.lenses.map((entry) => entry.grammar), [grammar], `${grammar} did not compile as a focused product lens`);
  }
});

test("the World product map is one compact human and machine closure over all verticals", () => {
  const catalogue = compileExternalWorldCatalogue(repoRoot);
  const html = renderExternalWorldCatalogueHtml(repoRoot, { basePath: "/external-estate" });
  assert.equal(catalogue.world_count, 30);
  assert.equal(catalogue.grammar_count, 15);
  assert.equal(catalogue.lens_route_count, 450);
  assert.match(html, new RegExp(catalogue.closure_digest));
  assert.match(html, /external-estate\/worlds\/farm-cold-chain-household\/listings/);
  assert.match(html, /external-estate\/v1\/worlds/);
});

test("search filters do not trigger a network probe", () => {
  const result = listExternalEstate(repoRoot, { domain: "weather", affordance: "OBSERVE", limit: 10 });
  assert.ok(result.total > 0);
  assert.ok(result.items.every((record) => record.projection.candidate_domains.includes("weather")));
  assert.ok(result.items.every((record) => record.projection.candidate_affordances.includes("OBSERVE")));
});

test("reagent-role slices keep design/compiler tools in their own usable reservoir", () => {
  const design = listExternalEstate(repoRoot, { reagentRole: "design_reference", limit: 20 });
  assert.ok(design.total > 0);
  assert.ok(design.items.every((record) => reagentRolesForRecord(record).includes("design_reference")));
  const studio = compileExternalSurfaceSlice(repoRoot, { grammar: "studio", query: "figma", host: "aurekai.live", extent: "section" });
  assert.ok(studio.candidates.length > 0);
  assert.ok(studio.candidates.some((record) => record.surface_relevance.matched_reagent_roles.includes("design_reference")));
  assert.ok(studio.surface_demand.ranking_reagent_roles.includes("design_reference"));
  assert.equal(studio.truth_level, "observed_candidate");
});

test("surface candidate slices narrow only observed candidate coordinates", () => {
  const slice = compileExternalSurfaceSlice(repoRoot, {
    grammar: "discover", host: "aurekai.work", world: "farm-cold-chain-household",
    affordance: "OBSERVE", near: "dallas", state: "live", time: "now", extent: "section",
  });
  assert.equal(slice.truth_level, "observed_candidate");
  assert.equal(slice.resolved_territory, "work");
  assert.equal(slice.extent, "section");
  assert.ok(slice.candidates.length <= 8);
  assert.ok(slice.candidates.every((record) => record.projection.candidate_worlds.includes("farm-cold-chain-household")));
  assert.ok(slice.candidates.every((record) => record.projection.candidate_affordances.includes("OBSERVE")));
  assert.deepEqual(slice.unresolved_shape.map((item) => item.coordinate), ["near", "state", "time"]);
  assert.match(slice.forbidden_inferences[0], /authority/);
});

test("SurfaceDemand ranks heterogeneous source candidates instead of leaking corpus order into every grammar", () => {
  const stage = compileExternalSurfaceSlice(repoRoot, { grammar: "stage", host: "aurekai.live", extent: "section" });
  const spatial = compileExternalSurfaceSlice(repoRoot, { grammar: "spatial", host: "aurekai.live", extent: "section" });
  assert.ok(stage.candidate_count > 0);
  assert.ok(spatial.candidate_count > 0);
  assert.ok(stage.candidates.some((record) => record.surface_relevance.matched_demand_affordances.includes("STREAM") || record.surface_relevance.matched_demand_affordances.includes("OBSERVE")));
  assert.ok(spatial.candidates.some((record) => record.surface_relevance.matched_demand_affordances.includes("LOCATE") || record.surface_relevance.matched_demand_affordances.includes("ROUTE")));
  assert.ok(new Set(spatial.candidates.map((record) => record.source_kind)).size > 1, "first viewport preserves carrier diversity");
  assert.equal(stage.surface_demand.limit.includes("does not admit a provider"), true);
});

test("a declared World demand alias narrows the same candidate reservoir without inventing a new source identity", () => {
  const storm = compileExternalSurfaceSlice(repoRoot, { grammar: "spatial", host: "aurekai.life", world: "regional-storm", extent: "section" });
  assert.equal(storm.surface_demand.world_demand.requested, "regional-storm");
  assert.equal(storm.surface_demand.world_demand.resolved, "natural-disaster-response");
  assert.equal(storm.surface_demand.world_demand.resolution, "declared_world_demand_alias");
  assert.ok(storm.candidate_count > 0);
  assert.ok(storm.candidates.every((record) => record.projection.candidate_worlds.includes("natural-disaster-response")));
});

test("address shape cannot manufacture actor or authority", () => {
  const rejected = validateExternalSurfaceSlice({ grammar: "discover", actor: "operator-1", audience: "operator" });
  assert.equal(rejected.ok, false);
  assert.deepEqual(rejected.forbidden, ["actor", "audience"]);
  assert.throws(() => compileExternalSurfaceSlice(repoRoot, { grammar: "discover", role: "operator" }),
    (error) => error.code === "forbidden_slice_coordinate");
});

test("fifteen grammars by four presentation classes compile into exactly sixty presentation-only cells", () => {
  const catalogue = presentationCellCatalog();
  const validation = validatePresentationCells();
  assert.equal(catalogue.cell_count, 60);
  assert.equal(PRESENTATION_CELLS.length, 60);
  assert.equal(validation.ok, true, validation.errors.join(", "));
  assert.deepEqual(catalogue.presentation_classes, ["anonymous", "personal", "member_worker", "operator"]);
  assert.equal(presentationCell("spatial", "operator").question.includes("dense observatory"), true);
  assert.equal(presentationCell("inbox", "personal").allowed_affordance_families.includes("save"), true);
  assert.equal(Object.hasOwn(presentationCell("discover", "operator"), "authority"), false);
});

test("a public candidate slice is anonymous presentation even when it has many candidates", () => {
  const anonymous = compileExternalSurfaceSlice(repoRoot, { grammar: "spatial", host: "aurekai.live", extent: "card" });
  assert.equal(anonymous.presentation_cell.presentation_class, "anonymous");
  assert.equal(anonymous.presentation_cell.grammar, "spatial");
  assert.equal(anonymous.presentation_cell.allowed_affordance_families.includes("configure"), false);
});

test("only the addressed host derives a territory", () => {
  assert.equal(territoryFromAddressHost("garden.aurekai.life:443"), "life");
  assert.equal(territoryFromAddressHost("market.aurekai.shop"), "shop");
  assert.equal(territoryFromAddressHost("aurekai.work"), "work");
  assert.equal(territoryFromAddressHost("unmapped.example"), "public");
});

test("a probe refuses a non-http target before any transport action", async () => {
  const result = await probeExternalOccurrence({ id: "test", source_ref: "TEST", primary_url: "file:///private/state" });
  assert.deepEqual(result, { status: "refused", reason: "non_http_primary_url" });
});

test("an explicit transport result has a human carrier without promoting the source", () => {
  const record = getExternalOccurrence(repoRoot, "EXT-00001");
  const html = renderExternalProbeHtml(record, {
    status: "refused", reason: "non_http_primary_url", requested_url: "file:///not-fetched",
  }, { basePath: "/external-estate" });
  assert.match(html, /Explicit transport observation/);
  assert.match(html, /no body retained/);
  assert.match(html, /does not follow redirects, retain a body, create a provider relationship/);
  assert.match(html, /external-estate\/occurrences\/external%3Aext-00001/);
});

test("human pages expose pagination and a per-occurrence inspection path", () => {
  const index = renderExternalEstateHtml(repoRoot, { basePath: "/external-estate", offset: 48, limit: 2 });
  const detail = renderExternalOccurrenceHtml(repoRoot, "EXT-04891", { basePath: "/external-estate" });
  assert.match(index, /offset=50/);
  assert.match(index, /external-estate\/occurrences\/external%3Aext-00049/);
  assert.match(index, /external-estate\/worlds\/farm-cold-chain-household/);
  assert.match(detail, /Allotrope Foundation Ontologies/);
  assert.match(detail, /Probe transport explicitly/);
});
