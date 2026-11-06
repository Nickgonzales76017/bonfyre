/*
 * Authored presentation algebra for the Web lowerer.
 *
 * These are intentionally cells of presentation only. They have no actor
 * identifiers, grants, provider state, payment state, or effect endpoints.
 * Native closure supplies those facts before anything effectful is offered.
 */
const CLASSES = Object.freeze({
  anonymous: {
    experience_character: "public orientation and legible provenance",
    allowed_affordance_families: ["inspect", "follow_link", "share"],
    requires: ["information_policy"],
    density: "low",
    refusal_character: "Explain what is public and point to the lawful next door.",
  },
  personal: {
    experience_character: "relation- and Pack-aware personal orientation",
    allowed_affordance_families: ["save", "join", "go", "meet"],
    requires: ["relation", "pack", "habitat", "information_policy"],
    density: "medium",
    refusal_character: "Keep the public slice visible and name the missing personal closure.",
  },
  member_worker: {
    experience_character: "scoped shared work and handoff",
    allowed_affordance_families: ["annotate", "handoff", "review", "coordinate"],
    requires: ["relation", "pack", "institution", "work_scope", "information_policy"],
    density: "high",
    refusal_character: "Show the shared boundary and direct the person to the admitted working context.",
  },
  operator: {
    experience_character: "dense observatory and owner-bound control disclosure",
    allowed_affordance_families: ["inspect", "configure", "review_exception", "publish_request"],
    requires: ["institution", "work_scope", "information_policy"],
    density: "very_high",
    refusal_character: "Expose the owner and exception state; never simulate an admission or effect.",
  },
});

const GRAMMARS = Object.freeze({
  discover: { question: "What useful material intersects this addressed world?", visible_sections: ["candidate_material", "why_here", "next_doors"], renderers: ["faceted_stream", "candidate_cards"], demand_roles: ["discover", "compare", "match"], empty: "No lawful candidate material matches this slice yet.", extent: "page", neighbors: ["spatial", "board", "listings"] },
  stage: { question: "What is progressing now, and what can this person lawfully follow?", visible_sections: ["timeline", "participants", "context"], renderers: ["timeline", "focus_panel"], demand_roles: ["observe", "schedule", "stream"], empty: "No progressing occurrence is available for this addressed cut.", extent: "page", neighbors: ["channels", "spatial", "observations"] },
  channels: { question: "Where can this addressed conversation be understood or continued?", visible_sections: ["topics", "participants", "context"], renderers: ["threaded_stream", "topic_index"], demand_roles: ["communicate", "translate", "notify"], empty: "No lawful channel is available in this context.", extent: "section", neighbors: ["community", "inbox", "stage"] },
  community: { question: "What shared body, norms, and contributions are visible here?", visible_sections: ["body", "norms", "shared_activity"], renderers: ["community_profile", "activity_wall"], demand_roles: ["identify", "communicate", "coordinate"], empty: "This community has no public projection at this cut.", extent: "page", neighbors: ["channels", "board", "worlds"] },
  board: { question: "What heterogeneous things belong together for this purpose?", visible_sections: ["arrangement", "references", "context"], renderers: ["masonry_board", "planning_canvas"], demand_roles: ["compare", "plan", "organize"], empty: "Nothing has been arranged for this addressed purpose.", extent: "page", neighbors: ["discover", "worlds", "work"] },
  spatial: { question: "How is this world arranged by location and capacity?", visible_sections: ["map", "layers", "place_explanation"], renderers: ["map", "layer_inspector"], demand_roles: ["locate", "route", "measure"], empty: "No observed spatial material is available for this cut.", extent: "page", neighbors: ["stage", "discover", "observations"] },
  listings: { question: "What declared capacity is available, without claiming it is committed?", visible_sections: ["availability", "terms", "comparison"], renderers: ["listing_grid", "compare_tray"], demand_roles: ["discover", "quote", "reserve"], empty: "No declared availability matches this slice.", extent: "page", neighbors: ["discover", "inbox", "territory"] },
  inbox: { question: "What is waiting for a lawful response from this context?", visible_sections: ["requests", "changes", "next_moves"], renderers: ["priority_queue", "handoff_list"], demand_roles: ["notify", "schedule", "review"], empty: "Nothing is waiting in this addressed inbox.", extent: "workspace", neighbors: ["work", "channels", "territory"] },
  territory: { question: "Why is this material here, for this address and disclosure boundary?", visible_sections: ["provenance", "addressing", "withholding"], renderers: ["provenance_panel", "closure_explainer"], demand_roles: ["verify", "inspect", "report"], empty: "No additional address explanation is available.", extent: "section", neighbors: ["observations", "packs", "link"] },
  observations: { question: "What was observed, retained, changed, or withheld?", visible_sections: ["receipts", "freshness", "conflicts"], renderers: ["evidence_ledger", "change_stream"], demand_roles: ["observe", "measure", "verify"], empty: "No admitted observations exist for this cut.", extent: "page", neighbors: ["territory", "spatial", "console"] },
  worlds: { question: "Which composed situations can make this material useful?", visible_sections: ["worlds", "dependencies", "entry_points"], renderers: ["world_gallery", "composition_map"], demand_roles: ["discover", "match", "simulate"], empty: "No composed World is available here.", extent: "page", neighbors: ["packs", "board", "discover"] },
  packs: { question: "What reusable outside-world appetite is declared here?", visible_sections: ["demands", "closure", "candidates"], renderers: ["dependency_graph", "demand_table"], demand_roles: ["discover", "compare", "optimize"], empty: "No Pack demand is declared at this cut.", extent: "page", neighbors: ["worlds", "territory", "console"] },
  link: { question: "What machine or foreign relationship is being inspected?", visible_sections: ["carrier", "identity", "limits"], renderers: ["relationship_inspector", "machine_card"], demand_roles: ["identify", "verify", "authenticate"], empty: "No lawful machine relationship is available to inspect.", extent: "section", neighbors: ["territory", "observations", "packs"] },
  studio: { question: "What can be composed, drafted, or previewed in this context?", visible_sections: ["composition", "preview", "owner_doors"], renderers: ["studio_canvas", "preview_panel"], demand_roles: ["compose", "compare", "publish"], empty: "No composition material is available in this Studio context.", extent: "workspace", neighbors: ["board", "packs", "console"] },
  console: { question: "What needs operator attention across sources, closure, derivation, and work?", visible_sections: ["source_weather", "demand_pressure", "exceptions", "proof_frontier"], renderers: ["observatory", "dense_tables"], demand_roles: ["inspect", "optimize", "report"], empty: "No operator residue is currently projected.", extent: "workspace", neighbors: ["observations", "packs", "inbox"] },
});

const FORBIDDEN_KEYS = new Set(["actor", "actor_id", "role", "authority", "audience", "entitlement", "provider_admission", "payment_authority", "spend_authority", "effect_authority"]);

function cellFor(grammar, presentationClass) {
  const g = GRAMMARS[grammar], p = CLASSES[presentationClass];
  return Object.freeze({
    grammar,
    presentation_class: presentationClass,
    question: `${g.question} ${p.experience_character}.`,
    experience_character: p.experience_character,
    visible_sections: g.visible_sections,
    preferred_renderers: g.renderers,
    interaction_density: p.density,
    allowed_affordance_families: p.allowed_affordance_families,
    external_demand_roles: g.demand_roles,
    empty_character: g.empty,
    refusal_character: p.refusal_character,
    extent_defaults: g.extent,
    navigation_neighbors: g.neighbors,
    requires: p.requires,
  });
}

export const SURFACE_GRAMMARS = Object.freeze(Object.keys(GRAMMARS));
export const PRESENTATION_CLASSES = Object.freeze(Object.keys(CLASSES));
export const PRESENTATION_CELLS = Object.freeze(SURFACE_GRAMMARS.flatMap((grammar) =>
  PRESENTATION_CLASSES.map((presentationClass) => cellFor(grammar, presentationClass))));

export function presentationCell(grammar, presentationClass = "anonymous") {
  const cell = PRESENTATION_CELLS.find((candidate) => candidate.grammar === grammar && candidate.presentation_class === presentationClass);
  if (!cell) throw new RangeError(`unknown presentation cell: ${grammar} × ${presentationClass}`);
  return cell;
}

export function validatePresentationCells(cells = PRESENTATION_CELLS) {
  const keys = new Set();
  const errors = [];
  if (cells.length !== SURFACE_GRAMMARS.length * PRESENTATION_CLASSES.length) errors.push("cell_count");
  for (const cell of cells) {
    const key = `${cell.grammar}\u0000${cell.presentation_class}`;
    if (keys.has(key)) errors.push(`duplicate:${key}`);
    keys.add(key);
    if (!cell.question || !cell.experience_character || !cell.empty_character || !cell.refusal_character) errors.push(`human_wording:${key}`);
    for (const field of Object.keys(cell)) if (FORBIDDEN_KEYS.has(field)) errors.push(`forbidden_field:${field}:${key}`);
  }
  return { ok: errors.length === 0, expected: SURFACE_GRAMMARS.length * PRESENTATION_CLASSES.length, actual: cells.length, errors };
}
