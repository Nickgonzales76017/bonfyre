import {WorkspaceSession} from './renderers/workspace-composition.mjs';
const asArray = value => Array.isArray(value) ? value : [];
const asObject = value => value && typeof value === "object" && !Array.isArray(value) ? value : {};
const uniq = values => [...new Set(values.filter(Boolean))];
const csv = value => typeof value === "string" ? value.split(",").map(x => x.trim()).filter(Boolean) : [];
const transitionName = value => `bf-${String(value || "focus").replace(/[^a-zA-Z0-9_-]+/g, "-").replace(/^-+|-+$/g, "") || "focus"}`;

function lensNames(scene) {
  const interaction = asObject(scene.interaction);
  return uniq([
    ...asArray(interaction.lens_order),
    ...asArray(scene.lenses),
    ...asArray(scene.fragments).map(x => x.surface_grammar),
    ...asArray(scene.bindings).map(x => x.application_kind),
    ...asArray(scene.stages).map(x => x.application_kind),
  ]);
}

function regions(scene) {
  return [
    ...asArray(scene.slots).map(slot => ({
      id: slot.slot_id, kind: "semantic_slot", extent: slot.extent,
      accepts: asArray(slot.accepts), maxItems: slot.max_items,
      liveBoundary: slot.htmx_boundary || "",
    })),
    ...asArray(scene.fragments).map(fragment => ({
      id: fragment.fragment_id, kind: "world_fragment", lens: fragment.surface_grammar,
      renderer: fragment.renderer_family, variant: fragment.renderer_variant || "",
      extent: fragment.extent, continuityKey: fragment.continuity_group || "",
      sourceProjection: fragment.source_projection || "", mediaPosture: fragment.media_posture || "",
      temporalPosture: fragment.temporal_posture || "", actions: csv(fragment.action_aperture),
    })),
  ];
}

function lenses(scene) {
  return lensNames(scene).map(name => {
    const fragments = asArray(scene.fragments).filter(x => x.surface_grammar === name);
    const bindings = asArray(scene.bindings).filter(x => x.application_kind === name);
    const stages = asArray(scene.stages).filter(x => x.application_kind === name);
    return {
      name,
      fragments: fragments.map(x => x.fragment_id),
      bindings: bindings.map(x => x.binding_id),
      stages: stages.map(x => x.stage_id),
      renderers: uniq([...fragments.map(x => x.renderer_family), ...stages.map(x => x.renderer)]),
      extents: uniq([...fragments.map(x => x.extent), ...stages.map(x => x.extent)]),
      continuity: uniq([...fragments.map(x => x.continuity_group), ...bindings.map(x => x.continuity_key)]),
      actions: uniq([
        ...fragments.flatMap(x => csv(x.action_aperture)),
        ...bindings.flatMap(x => csv(x.action_aperture)),
        ...stages.flatMap(x => asArray(x.actions)),
      ]),
    };
  });
}

function temporalAxis(scene) {
  return [
    ...asArray(scene.fragments).filter(x => x.temporal_posture)
      .map(x => ({source: "world_fragment", id: x.fragment_id, posture: x.temporal_posture})),
    ...asArray(scene.patterns).filter(x => x.time_field || x.time_mode)
      .map(x => ({source: "surface_pattern", id: x.pattern, field: x.time_field || "", mode: x.time_mode || ""})),
  ];
}

function queryPlan(scene) {
  return [
    ...asArray(scene.patterns).map(pattern => ({
      source: "surface_pattern", id: pattern.pattern, readsType: pattern.reads_type || "",
      owner: pattern.reads_organ || "", command: pattern.organ_command || "",
      effectClass: pattern.effect_class || "QUERY", requiresAuthority: pattern.requires_authority || "",
      timeField: pattern.time_field || "", timeMode: pattern.time_mode || "",
    })),
    ...asArray(scene.bindings).map(binding => ({
      source: "application_binding", id: binding.binding_id, applicationKind: binding.application_kind,
      materialQuery: asObject(binding.material_query), sourceCollection: binding.source_collection || "",
    })),
    ...asArray(scene.stages).flatMap(stage => asArray(stage.material_queries).map((materialQuery, index) => ({
      source: "experience_stage", id: `${stage.stage_id}:${index}`,
      applicationKind: stage.application_kind, materialQuery,
    }))),
  ];
}

/* Keep the four things a continuity surface can say distinct.  These are
 * runtime coordinates, not a second owner model: Actual is the particular
 * derived scene revision in hand, Possible is what that revision exposes as
 * a next door, Presence is the caller's transient interaction posture, and
 * Material is the declared owner-query plan. */
function continuityPlanes(scene, model, ephemeral) {
  return {
    actual: {
      sourceDigest: scene.source_digest || "",
      focus: asObject(scene.focus),
    },
    possible: {
      lenses: model.lenses.map(lens => lens.name),
      apertures: model.operations.apertures,
      objectContracts: model.operations.objectContracts.map(contract => ({
        birthOperation: contract.birth_operation || "",
        birthMode: contract.birth_mode || "",
      })),
    },
    presence: {
      actorScope: scene.focus?.actor_scope || "",
      connectivity: ephemeral.online === false ? "offline" : "online",
      expanded: ephemeral.expanded || "",
      selected: ephemeral.selected || "",
      hovered: ephemeral.hovered || "",
      dragging: ephemeral.dragging || "",
    },
    material: {
      regions: model.regions.map(region => ({id: region.id, kind: region.kind})),
      queryPlan: model.queryPlan,
      dependencies: model.dependencies,
    },
  };
}

export function composeSurfaceRuntime(scene, ephemeral = {}) {
  if (!scene || scene.schema !== "bonfyre-surface-scene.v1") {
    throw new TypeError("SurfaceRuntime requires bonfyre-surface-scene.v1");
  }
  const interaction = asObject(scene.interaction);
  const zoom = asArray(interaction.semantic_zoom);
  const depths = asArray(interaction.interaction_depths);
  const regionList = regions(scene);
  const lensList = lenses(scene);
  const model = {
    schema: "bonfyre-surface-runtime.v1",
    sourceDigest: scene.source_digest || "",
    focus: asObject(scene.focus),
    activeLens: ephemeral.lens || interaction.default_lens || lensList[0]?.name || "",
    resolution: ephemeral.resolution ?? zoom[0]?.resolution ?? 0,
    depth: ephemeral.depth || depths[0] || "focus",
    lenses: lensList,
    regions: regionList,
    continuity: uniq([
      ...asArray(scene.fragments).map(x => x.continuity_group),
      ...asArray(scene.bindings).map(x => x.continuity_key),
    ]),
    liveRegions: regionList.filter(x => x.liveBoundary || x.sourceProjection),
    temporalAxis: temporalAxis(scene),
    semanticZoom: zoom,
    contextRail: asObject(interaction.context_rail),
    relationHalo: asObject(interaction.relation_halo),
    commandAperture: asObject(interaction.command_aperture),
    transitionPolicy: asObject(interaction.transition_policy),
    mobilePosture: asObject(interaction.mobile_posture),
    desktopPosture: asObject(interaction.desktop_posture),
    liveUpdatePolicy: asObject(interaction.live_update_policy),
    queryPlan: queryPlan(scene),
    operations: {
      apertures: uniq(asArray(scene.operations?.apertures)),
      objectContracts: asArray(scene.operations?.object_contracts),
      proposalContracts: asArray(scene.operations?.proposal_contracts),
    },
    presentation: asArray(scene.presentation),
    renderers: asArray(scene.renderers),
    runtimeQueries: asObject(scene.runtime_queries),
    dependencies: asArray(scene.dependencies),
    ephemeral: {
      expanded: ephemeral.expanded || "", selected: ephemeral.selected || "",
      hovered: ephemeral.hovered || "", dragging: ephemeral.dragging || "",
      online: ephemeral.online !== false,
    },
    truth: "interaction projection only; exact owners remain authoritative",
  };
  model.planes = continuityPlanes(scene, model, ephemeral);
  return model;
}

export class SurfaceRuntime {
  constructor(scene, {root = globalThis.document, state = {}} = {}) {
    this.root = root;
    this.scene = scene;
    this.state = {...state};
    this.model = composeSurfaceRuntime(scene, this.state);
  }
  composeWorkspace(contributions) {
    if (this.workspace && !this.workspace.closed) this.workspace.rebind(this.scene, contributions);
    else this.workspace = new WorkspaceSession(this.scene, contributions);
    this.workspaceContributions = contributions;
    return this.workspace;
  }
  disposeWorkspace() {
    this.workspace?.dispose();
    this.workspace = null;
    this.workspaceContributions = null;
  }
  updateScene(scene) {
    const next = composeSurfaceRuntime(scene, this.state);
    // Validate binding compatibility and draft safety before replacing either
    // projection. A valid source update is not permission to discard open work.
    if (this.workspace && !this.workspace.closed) {
      this.workspace.rebind(scene, this.workspaceContributions);
    }
    this.scene = scene;
    this.model = next;
    this.renderState();
    this.emit("bonfyre:scene", {model: this.model});
    return this.model;
  }
  transition(change) {
    const apply = () => {
      change();
      this.model = composeSurfaceRuntime(this.scene, this.state);
      this.renderState();
    };
    if (this.model.transitionPolicy?.object_permanence !== false && globalThis.document?.startViewTransition) {
      return globalThis.document.startViewTransition(apply);
    }
    apply();
  }
  setLens(lens) {
    if (!this.model.lenses.some(x => x.name === lens)) return false;
    this.transition(() => { this.state.lens = lens; });
    this.emit("bonfyre:lens", {lens, model: this.model});
    return true;
  }
  setResolution(resolution) {
    if (!this.model.semanticZoom.some(x => Number(x.resolution) === Number(resolution))) return false;
    this.transition(() => { this.state.resolution = Number(resolution); });
    this.emit("bonfyre:resolution", {resolution: Number(resolution), model: this.model});
    return true;
  }
  setDepth(depth) {
    const allowed = asArray(this.scene.interaction?.interaction_depths);
    if (allowed.length && !allowed.includes(depth)) return false;
    this.transition(() => { this.state.depth = depth; });
    this.emit("bonfyre:depth", {depth, model: this.model});
    return true;
  }
  setEphemeral(key, value) {
    if (!["expanded", "selected", "hovered", "dragging"].includes(key)) return false;
    this.state[key] = value || "";
    this.model = composeSurfaceRuntime(this.scene, this.state);
    this.renderState();
    return true;
  }
  setConnectivity(online) {
    const next = online !== false;
    if ((this.state.online !== false) === next) return false;
    this.state.online = next;
    this.model = composeSurfaceRuntime(this.scene, this.state);
    this.renderState();
    if (next) {
      this.emit("bonfyre:surface-reconnect", {
        sourceDigest: this.model.sourceDigest,
        needsRefresh: this.state.needsRefresh === true,
        model: this.model,
      });
    } else {
      this.emit("bonfyre:surface-offline", {sourceDigest: this.model.sourceDigest, model: this.model});
    }
    return true;
  }
  applyDelta(delta) {
    if (!delta || delta.schema !== "bonfyre-surface-delta.v1") return false;
    if (this.state.online === false) {
      this.state.needsRefresh = true;
      this.emit("bonfyre:surface-reconcile", {reason: "offline", sourceDigest: this.model.sourceDigest});
      return false;
    }
    const before = delta.before_digest || "";
    const after = delta.after_digest || "";
    if (before && before !== this.model.sourceDigest) {
      this.state.needsRefresh = true;
      this.emit("bonfyre:surface-reconcile", {
        reason: "digest_mismatch", sourceDigest: this.model.sourceDigest, delta,
      });
      return false;
    }
    if (delta.scene) {
      if (delta.scene.schema !== "bonfyre-surface-scene.v1" ||
          (after && delta.scene.source_digest !== after)) return false;
      try {
        this.updateScene(delta.scene);
      } catch (error) {
        this.state.needsRefresh = true;
        this.emit("bonfyre:surface-reconcile", {
          reason: "workspace_busy_or_incompatible", sourceDigest: this.model.sourceDigest,
        });
        return false;
      }
      this.state.needsRefresh = false;
      this.emit("bonfyre:surface-delta-applied", {delta, model: this.model});
      return true;
    }
    /* A changed revision needs its complete Scene.  A partial HTML patch may
     * redraw a region only when it is still exactly this revision; it cannot
     * smuggle a new owner result into a stale scene. */
    if (after && after !== this.model.sourceDigest) {
      this.state.needsRefresh = true;
      this.emit("bonfyre:surface-reconcile", {
        reason: "scene_required", sourceDigest: this.model.sourceDigest, delta,
      });
      return false;
    }
    if (typeof delta.regionId !== "string" || typeof delta.html !== "string") return false;
    return this.patchRegion(delta.regionId, delta.html);
  }
  patchRegion(regionId, html) {
    if (!this.model.regions.some(x => x.id === regionId)) return false;
    const selector = `[data-bonfyre-region="${String(regionId).replace(/["\\]/g, "\\$&")}"]`;
    const host = this.root?.querySelector?.(selector);
    if (!host) return false;
    this.transition(() => { host.innerHTML = html; });
    this.emit("bonfyre:region", {regionId, model: this.model});
    return true;
  }
  applyContinuityNames() {
    if (!this.root?.querySelectorAll) return;
    for (const node of this.root.querySelectorAll("[data-bonfyre-continuity]")) {
      const key = node.dataset?.bonfyreContinuity;
      if (key && node.style) node.style.viewTransitionName = transitionName(key);
    }
  }
  renderState() {
    const host = this.root?.documentElement || this.root;
    if (!host?.dataset) return;
    host.dataset.bonfyreLens = this.model.activeLens;
    host.dataset.bonfyreResolution = String(this.model.resolution);
    host.dataset.bonfyreDepth = this.model.depth;
    host.dataset.bonfyreSubject = this.model.focus.subject_address || "";
    host.dataset.bonfyreConnectivity = this.model.ephemeral.online ? "online" : "offline";
    this.applyContinuityNames();
  }
  bind() {
    if (!this.root?.addEventListener) return this;
    this.renderState();
    this.root.addEventListener("click", event => {
      const lens = event.target?.closest?.("[data-bonfyre-lens-target]")?.dataset?.bonfyreLensTarget;
      if (lens) { event.preventDefault(); this.setLens(lens); return; }
      const depth = event.target?.closest?.("[data-bonfyre-depth-target]")?.dataset?.bonfyreDepthTarget;
      if (depth) { event.preventDefault(); this.setDepth(depth); return; }
      const resolution = event.target?.closest?.("[data-bonfyre-resolution-target]")?.dataset?.bonfyreResolutionTarget;
      if (resolution !== undefined) { event.preventDefault(); this.setResolution(Number(resolution)); }
    });
    this.root.addEventListener("keydown", event => {
      const configured = String(this.model.commandAperture?.shortcut || "").toLowerCase();
      const wantsK = configured === "meta+k" || configured === "cmd+k" || configured === "ctrl+k";
      if (wantsK && event.key?.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        this.emit("bonfyre:command-aperture", {focus: this.model.focus, operations: this.model.operations, model: this.model});
      }
    });
    this.root.addEventListener("bonfyre:surface-scene", event => {
      if (event.detail?.scene) this.updateScene(event.detail.scene);
    });
    this.root.addEventListener("bonfyre:surface-delta", event => {
      this.applyDelta(event.detail);
    });
    const view = this.root.defaultView || globalThis.window;
    view?.addEventListener?.("offline", () => this.setConnectivity(false));
    view?.addEventListener?.("online", () => this.setConnectivity(true));
    return this;
  }
  emit(name, detail) {
    if (!this.root?.dispatchEvent || typeof globalThis.CustomEvent !== "function") return;
    this.root.dispatchEvent(new CustomEvent(name, {detail}));
  }
}
