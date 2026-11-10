import assert from "node:assert/strict";
import {composeSurfaceRuntime, SurfaceRuntime} from "../runtime/surface-runtime.mjs";

const scene = {
  schema: "bonfyre-surface-scene.v1",
  source_digest: "sha256:test",
  focus: {subject_address: "world:test", world: "test", actor_scope: "public"},
  lenses: ["NOW", "MAP", "Community"],
  slots: [{slot_id: "primary", extent: "page", accepts: ["PublicationFeed"], max_items: 40, htmx_boundary: "scene:primary"}],
  fragments: [
    {fragment_id: "now", surface_grammar: "NOW", renderer_family: "vertical", renderer_variant: "stream", extent: "page", continuity_group: "world:test", source_projection: "/_surface/feed", media_posture: "full-bleed", temporal_posture: "now", action_aperture: "save,share"},
    {fragment_id: "map", surface_grammar: "MAP", renderer_family: "spatial", renderer_variant: "map", extent: "page", continuity_group: "world:test", source_projection: "", temporal_posture: "now", action_aperture: "route"},
  ],
  bindings: [{binding_id: "talk", slot_id: "primary", application_kind: "Community", material_query: {community: "test"}, source_collection: "", action_aperture: "read,reply", continuity_key: "talk:test"}],
  stages: [{stage_id: "stage", application_kind: "Stage", renderer: "timeline", extent: "page", material_queries: [{kind: "EventOccurrence"}], actions: ["observe"], next_stage_rules: []}],
  patterns: [{pattern: "feed", reads_type: "publication_occurrence", reads_organ: "CMS", organ_command: "query", effect_class: "QUERY", requires_authority: "", time_field: "published_at", time_mode: "recent"}],
  presentation: [],
  renderers: [],
  operations: {apertures: ["save", "share"], object_contracts: [{birth_operation: "Plan.propose", birth_mode: "PROPOSE"}], proposal_contracts: [{operation: "Plan.propose", owner: "ForeignTwin"}]},
  interaction: {
    default_lens: "NOW",
    lens_order: ["NOW", "MAP", "Community"],
    semantic_zoom: [{resolution: 0, name: "world"}, {resolution: 2, name: "subject"}, {resolution: 4, name: "occurrence"}],
    interaction_depths: ["peek", "expand", "focus"],
    context_rail: {mode: "floating"},
    relation_halo: {enabled: true, max: 8},
    command_aperture: {shortcut: "Meta+K", mode: "contextual"},
    transition_policy: {object_permanence: true},
    mobile_posture: {primary: "vertical"},
    desktop_posture: {inspector: "persistent"},
    live_update_policy: {mode: "SurfaceDelta", transport: "htmx"},
  },
  runtime_queries: {source_collections: ["test"]},
  dependencies: ["cms:world:test"],
};

const model = composeSurfaceRuntime(scene);
assert.equal(model.schema, "bonfyre-surface-runtime.v1");
assert.equal(model.activeLens, "NOW");
assert.equal(model.depth, "peek");
assert.equal(model.regions.length, 3);
assert.ok(model.liveRegions.some(x => x.id === "primary" && x.liveBoundary === "scene:primary"));
assert.ok(model.liveRegions.some(x => x.id === "now" && x.sourceProjection === "/_surface/feed"));
assert.deepEqual(model.continuity.sort(), ["talk:test", "world:test"]);
assert.ok(model.lenses.find(x => x.name === "NOW").actions.includes("save"));
assert.ok(model.lenses.find(x => x.name === "Community").actions.includes("reply"));
assert.ok(model.temporalAxis.some(x => x.id === "feed" && x.field === "published_at"));
assert.ok(model.queryPlan.some(x => x.owner === "CMS" && x.effectClass === "QUERY"));
assert.equal(model.relationHalo.enabled, true);
assert.equal(model.commandAperture.shortcut, "Meta+K");
assert.equal(model.truth, "interaction projection only; exact owners remain authoritative");
assert.equal(model.planes.actual.sourceDigest, "sha256:test");
assert.deepEqual(model.planes.possible.lenses, ["NOW", "MAP", "Community", "Stage"]);
assert.equal(model.planes.presence.actorScope, "public");
assert.equal(model.planes.presence.connectivity, "online");
assert.ok(model.planes.material.queryPlan.some(x => x.id === "feed"));

const runtime = new SurfaceRuntime(scene, {root: null});
assert.equal(runtime.setLens("MAP"), true);
assert.equal(runtime.model.activeLens, "MAP");
assert.equal(runtime.setLens("DOES_NOT_EXIST"), false);
assert.equal(runtime.setResolution(4), true);
assert.equal(runtime.model.resolution, 4);
assert.equal(runtime.setDepth("focus"), true);
assert.equal(runtime.model.depth, "focus");
assert.equal(runtime.setEphemeral("selected", "venue:one"), true);
assert.equal(runtime.model.ephemeral.selected, "venue:one");
assert.equal(runtime.patchRegion("now", "<p>x</p>"), false);

assert.equal(runtime.setConnectivity(false), true);
assert.equal(runtime.model.planes.presence.connectivity, "offline");
assert.equal(runtime.applyDelta({
  schema: "bonfyre-surface-delta.v1", before_digest: "sha256:test", after_digest: "sha256:next", scene,
}), false, "offline state never applies a queued owner result");
assert.equal(runtime.state.needsRefresh, true);
assert.equal(runtime.setConnectivity(true), true);
assert.equal(runtime.model.planes.presence.connectivity, "online");
assert.equal(runtime.applyDelta({
  schema: "bonfyre-surface-delta.v1", before_digest: "sha256:wrong", after_digest: "sha256:next", scene,
}), false, "stale deltas request reconciliation rather than overwriting the current scene");

const updated = structuredClone(scene);
updated.source_digest = "sha256:next";
updated.bindings[0].action_aperture = "read,reply,follow";
runtime.updateScene(updated);
assert.equal(runtime.model.sourceDigest, "sha256:next");
assert.ok(runtime.model.lenses.find(x => x.name === "Community").actions.includes("follow"));

const newest = structuredClone(updated);
newest.source_digest = "sha256:newest";
assert.equal(runtime.applyDelta({
  schema: "bonfyre-surface-delta.v1", before_digest: "sha256:next", after_digest: "sha256:newest", scene: newest,
}), true, "a complete successor scene applies only from its exact predecessor");
assert.equal(runtime.model.sourceDigest, "sha256:newest");
assert.equal(runtime.applyDelta({
  schema: "bonfyre-surface-delta.v1", before_digest: "sha256:newest", after_digest: "sha256:future", regionId: "now", html: "<p>future</p>",
}), false, "a changed revision cannot arrive as an unbound HTML patch");

/* The incoming delta event and the locally reported applied event are distinct:
 * re-emitting the incoming name would recurse through the bound listener. */
const eventRoot = new EventTarget();
eventRoot.documentElement = {dataset: {}};
const eventRuntime = new SurfaceRuntime(scene, {root: eventRoot}).bind();
const eventSuccessor = structuredClone(scene);
eventSuccessor.source_digest = "sha256:event-next";
eventRoot.dispatchEvent(new CustomEvent("bonfyre:surface-delta", {detail: {
  schema: "bonfyre-surface-delta.v1", before_digest: "sha256:test", after_digest: "sha256:event-next", scene: eventSuccessor,
}}));
assert.equal(eventRuntime.model.sourceDigest, "sha256:event-next");

console.log("surface_runtime_test: PASS");
