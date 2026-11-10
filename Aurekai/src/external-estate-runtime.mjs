/*
 * Read-only runtime for the supplied Aurekai external estate.
 *
 * It is intentionally an index, composition lens, and explicitly-triggered
 * public-URL probe.  It cannot authenticate to a provider, enact a contract,
 * create a payment, promote evidence, or emit a Bonfyre effect.  Those moves
 * remain behind the native owners named in the response limits.
 */
import { existsSync, readFileSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { PRESENTATION_CELLS, PRESENTATION_CLASSES, SURFACE_GRAMMARS, presentationCell } from "./presentation-cell-catalog.mjs";

const DEFAULT_LIMIT = 48;
const MAX_LIMIT = 200;
const EXTENT_LIMITS = Object.freeze({ chip: 1, card: 3, section: 8, page: 48, workspace: 200 });
const WORLD_PRODUCT_LENS_LIMIT = 6;

/*
 * SurfaceDemand is the small, declared bridge between an interaction grammar
 * and the external candidate reservoir.  It is a ranking contract, never a
 * provider admission rule: every supplied record remains an observed
 * candidate and the native owners still decide whether it may be used.
 *
 * This avoids the catalogue failure mode where every page is merely the first
 * few corpus rows.  The profiles intentionally speak the estate's reusable
 * affordance/domain vocabulary, not a vendor name or a vertical application.
 */
const SURFACE_DEMAND_PROFILES = Object.freeze({
  discover: { affordances: ["DISCOVER", "SEARCH", "MATCH", "COMPARE"], domains: ["community", "people", "travel", "education", "research"], sourceKinds: ["public_api", "rss", "mcp"] },
  stage: { affordances: ["OBSERVE", "STREAM", "SCHEDULE", "ALERT"], domains: ["sports", "weather", "transport", "spacecraft", "media"], sourceKinds: ["public_api", "rss", "machine_protocol"] },
  channels: { affordances: ["COMMUNICATE", "TRANSLATE", "ALERT", "PUBLISH"], domains: ["community", "people", "media", "education"], sourceKinds: ["mcp", "rss", "public_api"] },
  community: { affordances: ["IDENTIFY", "COMMUNICATE", "PUBLISH", "COORDINATE"], domains: ["community", "people", "government", "religion"], sourceKinds: ["public_api", "mcp", "rss"] },
  board: { affordances: ["COMPARE", "SEARCH", "DISCOVER", "OPTIMIZE"], domains: ["research", "art", "construction", "food", "travel"], sourceKinds: ["public_api", "mcp", "rss"] },
  spatial: { affordances: ["LOCATE", "ROUTE", "MEASURE", "OBSERVE"], domains: ["weather", "roads", "transport", "ships", "aircraft", "telecommunications", "water", "energy"], sourceKinds: ["public_api", "machine_protocol", "rss"] },
  listings: { affordances: ["QUOTE", "RESERVE", "PURCHASE", "COMPARE"], domains: ["trade", "hospitality", "labor", "travel", "food", "construction"], sourceKinds: ["public_api", "x402", "mcp"] },
  inbox: { affordances: ["ALERT", "NOTIFY", "SCHEDULE", "REVIEW"], domains: ["community", "transport", "weather", "medicine", "utilities"], sourceKinds: ["rss", "public_api", "mcp"] },
  territory: { affordances: ["VERIFY", "INSPECT", "REPORT", "IDENTIFY"], domains: ["government", "law", "land", "research"], sourceKinds: ["machine_protocol", "public_api", "mcp"] },
  observations: { affordances: ["OBSERVE", "MEASURE", "VERIFY", "ALERT"], domains: ["weather", "science", "medicine", "energy", "water", "oceans"], sourceKinds: ["public_api", "rss", "machine_protocol"] },
  worlds: { affordances: ["MATCH", "DISCOVER", "SIMULATE", "PREDICT"], domains: ["community", "family", "household", "farms", "buildings", "research"], sourceKinds: ["public_api", "mcp", "rss"] },
  packs: { affordances: ["DISCOVER", "COMPARE", "OPTIMIZE", "SEARCH"], domains: ["computers", "agents", "telecommunications", "trade", "research"], sourceKinds: ["mcp", "machine_protocol", "public_api"] },
  link: { affordances: ["IDENTIFY", "VERIFY", "AUTHENTICATE", "INSPECT"], domains: ["computers", "agents", "telecommunications", "government"], sourceKinds: ["machine_protocol", "mcp", "public_api"] },
  studio: { affordances: ["PUBLISH", "COMPARE", "SEARCH", "OPTIMIZE"], domains: ["art", "media", "film", "music", "research"], sourceKinds: ["mcp", "public_api", "rss"] },
  console: { affordances: ["INSPECT", "OPTIMIZE", "REPORT", "VERIFY"], domains: ["computers", "agents", "finance", "government", "telecommunications"], sourceKinds: ["machine_protocol", "mcp", "public_api"] },
});

const SURFACE_REAGENT_ROLES = Object.freeze({
  discover: ["material_source", "interaction_reference", "dataset"],
  stage: ["material_source", "interaction_reference", "action_adapter"],
  channels: ["interaction_reference", "action_adapter", "material_source"],
  community: ["interaction_reference", "material_source", "action_adapter"],
  board: ["material_source", "interaction_reference", "dataset"],
  spatial: ["material_source", "dataset", "action_adapter"],
  listings: ["action_adapter", "commerce_carrier", "material_source"],
  inbox: ["action_adapter", "material_source", "interaction_reference"],
  territory: ["dataset", "material_source", "verification_tool"],
  observations: ["dataset", "material_source", "verification_tool"],
  worlds: ["material_source", "interaction_reference", "dataset"],
  packs: ["compiler_tool", "infrastructure_adapter", "verification_tool", "action_adapter"],
  link: ["transport_carrier", "infrastructure_adapter", "verification_tool"],
  studio: ["design_reference", "compiler_tool", "publication_target", "interaction_reference", "material_source"],
  console: ["compiler_tool", "infrastructure_adapter", "verification_tool", "transport_carrier"],
});

const REAGENT_ROLE_TERMS = Object.freeze({
  design_reference: ["figma", "storybook", "design system", "design-system", "design token", "design-token", "shadcn", "tailwind", "theme", "template", "typography", "component library", "ui kit"],
  compiler_tool: ["compiler", "codegen", "code generation", "coding agent", "coding agents", "developer tool", "developer tools", "browser automation", "playwright", "source code", "framework-aware code"],
  verification_tool: ["visual regression", "screenshot", "audit", "validator", "validation", "accessibility", "wcag", "benchmark", "golden eval", "discrepanc"],
  interaction_reference: ["spotify", "youtube", "discord", "reddit", "tiktok", "instagram", "bandcamp", "soundcloud", "forum", "community", "chat", "kanban", "calendar", "playlist", "marketplace", "social network"],
  publication_target: ["publish", "publication", "upload", "post", "social platform", "video platform", "podcast"],
  material_source: ["feed", "rss", "catalog", "catalogue", "dataset", "open data", "metadata", "transcript", "subtitle", "chapter", "image", "photo", "event", "news"],
  dataset: ["dataset", "database", "catalog", "catalogue", "metadata", "registry", "open data", "feed"],
  infrastructure_adapter: ["kubernetes", "terraform", "docker", "cloud infrastructure", "deployment", "observability", "database server", "ci/cd", "devops"],
  commerce_carrier: ["x402", "payment", "checkout", "price", "quote", "commerce", "order", "settlement"],
});

const ACTION_AFFORDANCES = new Set(["LOCATE", "MATCH", "ROUTE", "SCHEDULE", "QUOTE", "PURCHASE", "PAY", "COMMUNICATE", "TRANSLATE", "PUBLISH", "AUTHENTICATE", "AUTHORIZE", "FILE", "REPORT", "ALERT", "REPAIR", "NEGOTIATE", "CONTRACT", "HIRE"]);

/* An addressed World can declare a demand alias without changing the identity
 * of either World or source.  This is deliberately small and explicit: an
 * unknown world remains an honest empty result rather than fuzzy matching. */
const WORLD_DEMAND_ALIASES = Object.freeze({
  "regional-storm": "natural-disaster-response",
  "home": "household-daily-life",
  "farm": "farm-cold-chain-household",
  "school": "school-township",
  "elder-care": "elder-care-network",
  "port": "port-township",
  "space": "space-mission",
});

/* A World is more useful than a topical result set when its heterogeneous
 * observed material can name a possible thing people might make together.
 * These are formation templates, not automatic product launches.  Each
 * requires independently visible candidate signals and reports the exact
 * source occurrences that made the candidate legible. */
const WORLD_FORMATION_TEMPLATES = Object.freeze([
  { id: "living-publication", title: "Living publication", signals: ["material"], artifacts: ["editorial publication", "source-backed archive", "reading and follow-up surface"], operator_archetypes: ["newsletter-writer", "forum-maintainer", "supporter-creator"], project_facets: ["campaign", "recurring", "recruiting"] },
  { id: "decision-service", title: "Decision service", signals: ["observation", "comparison"], artifacts: ["comparison", "explainable recommendation candidate", "change-aware decision brief"], operator_archetypes: ["deal-curator", "household-organiser"], project_facets: ["campaign", "bounty", "recruiting"] },
  { id: "coordination-space", title: "Coordination space", signals: ["schedule", "communication"], artifacts: ["shared schedule", "participant coordination", "human-approved work proposals"], operator_archetypes: ["room-parent", "team-manager", "local-promoter"], project_facets: ["recruiting", "governance", "recurring"] },
  { id: "market-candidate", title: "Market candidate", signals: ["market", "identity"], artifacts: ["catalogue/comparison", "availability and quote investigation", "human-gated transaction design"], operator_archetypes: ["marketplace-seller", "deal-curator"], project_facets: ["ledger", "bounty", "recruiting"] },
  { id: "place-service", title: "Place service", signals: ["place", "observation"], artifacts: ["map or spatial brief", "local condition surface", "route and access investigation"], operator_archetypes: ["neighbourhood-organiser", "local-promoter"], project_facets: ["campaign", "recruiting", "governance"] },
  { id: "small-institution", title: "Small institution", signals: ["publication", "communication", "identity"], artifacts: ["public edition", "member/operator edition proposal", "governance and budget workbench"], operator_archetypes: ["club-admin", "pta-officer", "neighbourhood-organiser"], project_facets: ["governance", "ledger", "recurring", "recruiting"] },
]);

const FORMATION_SIGNAL_AFFORDANCES = Object.freeze({
  observation: ["OBSERVE", "MEASURE", "VERIFY", "ALERT"],
  comparison: ["COMPARE", "SEARCH", "MATCH", "OPTIMIZE"],
  schedule: ["SCHEDULE", "ROUTE"],
  communication: ["COMMUNICATE", "PUBLISH", "REPORT"],
  market: ["QUOTE", "PURCHASE", "NEGOTIATE", "CONTRACT", "MATCH"],
  identity: ["IDENTIFY", "AUTHENTICATE", "AUTHORIZE"],
  place: ["LOCATE", "ROUTE"],
});
const FORBIDDEN_SLICE_COORDINATES = new Set([
  "actor", "actor_id", "audience", "authority", "role", "institution", "entitlement",
  "delegation", "permission", "effect_authority", "financial_authority", "spend_authority",
]);
const DEFAULT_RELATIVE_CORPUS = join("estate", "generated", "external-estate-v4", "observed-external-estate.jsonl");
const DEFAULT_RELATIVE_MANIFEST = join("estate", "generated", "external-estate-v4", "manifest.json");

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

/**
 * Classify why an observed occurrence may be useful without promoting it to a
 * provider, capability, authority, or evidence source. The raw occurrence is
 * unchanged; these roles only stop unlike reagents competing in one ranking.
 */
export function reagentRolesForRecord(record) {
  const text = normalize([record.observed_name, record.section, record.raw_source_text].filter(Boolean).join(" "));
  const roles = new Set();
  const sourceKind = normalize(record.source_kind);
  const affordances = new Set((record.projection?.candidate_affordances || []).map((value) => String(value).toUpperCase()));

  if (sourceKind === "rss") {
    roles.add("material_source");
    roles.add("publication_target");
  }
  if (sourceKind === "public_api") roles.add("dataset");
  if (sourceKind === "mcp") roles.add("action_adapter");
  if (sourceKind === "x402") {
    roles.add("action_adapter");
    roles.add("commerce_carrier");
  }
  if (sourceKind === "machine_protocol" || sourceKind === "x402_spec") roles.add("transport_carrier");
  if ([...affordances].some((value) => ACTION_AFFORDANCES.has(value))) roles.add("action_adapter");

  for (const [role, terms] of Object.entries(REAGENT_ROLE_TERMS)) {
    if (terms.some((term) => text.includes(term))) roles.add(role);
  }
  if (roles.size === 0) roles.add("reference");
  return [...roles];
}

function tokenize(value) {
  return normalize(value).split(/[^a-z0-9]+/).filter(Boolean);
}

function boundedNumber(value, fallback = DEFAULT_LIMIT) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, MAX_LIMIT);
}

function valueOrNull(value) {
  const normalized = normalize(value);
  return normalized || null;
}

function normalizeExtent(value) {
  const extent = valueOrNull(value) || "page";
  return Object.hasOwn(EXTENT_LIMITS, extent) ? extent : "page";
}

/**
 * Accept only shape coordinates.  This candidate lane deliberately does not
 * accept actor, audience, role, or authority coordinates: those are resolved
 * by AddressPlane/native owners, never supplied by a browser query string.
 */
export function validateExternalSurfaceSlice(options = {}) {
  const forbidden = Object.keys(options).filter((key) =>
    FORBIDDEN_SLICE_COORDINATES.has(normalize(key)) && valueOrNull(options[key]));
  const grammar = valueOrNull(options.grammar);
  const invalid = [];
  if (!grammar || !SURFACE_GRAMMARS.includes(grammar)) invalid.push("grammar");
  if (options.extent && !Object.hasOwn(EXTENT_LIMITS, normalize(options.extent))) invalid.push("extent");
  return { ok: forbidden.length === 0 && invalid.length === 0, forbidden, invalid };
}

export function territoryFromAddressHost(host) {
  const value = normalize(String(host || "").split(":")[0]);
  if (value.endsWith(".life")) return "life";
  if (value.endsWith(".shop")) return "shop";
  if (value.endsWith(".work")) return "work";
  if (value.endsWith(".live")) return "live";
  /* Host resolution is an AddressPlane concern. Unknown hosts have no
   * territory claim; public is a rendering disposition, not an audience. */
  return "public";
}

function externalPath(repoRoot, relative) {
  return join(repoRoot, relative);
}

function sourceStamp(corpusPath, manifestPath) {
  const corpus = statSync(corpusPath);
  const manifest = statSync(manifestPath);
  return `${corpus.mtimeMs}:${corpus.size}:${manifest.mtimeMs}:${manifest.size}`;
}

const caches = new Map();
const worldLensCaches = new Map();
const worldFormationCaches = new Map();
const worldBridgeCaches = new Map();

/** Load the compiled source occurrences. No network or provider action occurs. */
export function loadExternalEstate(repoRoot, options = {}) {
  const corpusPath = options.corpusPath || externalPath(repoRoot, DEFAULT_RELATIVE_CORPUS);
  const manifestPath = options.manifestPath || externalPath(repoRoot, DEFAULT_RELATIVE_MANIFEST);
  const cacheKey = `${corpusPath}\u0000${manifestPath}`;
  const stamp = sourceStamp(corpusPath, manifestPath);
  const cached = caches.get(cacheKey);
  if (cached && cached.source_stamp === stamp) return cached;
  /* A compiler publication swaps a new corpus/manifest into this read-only
   * carrier. Its derived World lenses are disposable physical indexes, so a
   * changed source stamp drops them before the next closure is compiled. */
  if (cached) {
    worldLensCaches.clear();
    worldFormationCaches.clear();
    worldBridgeCaches.clear();
  }
  const records = readFileSync(corpusPath, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  const byId = new Map();
  const bySourceRef = new Map();
  for (const record of records) {
    byId.set(normalize(record.id), record);
    if (!bySourceRef.has(normalize(record.source_ref))) bySourceRef.set(normalize(record.source_ref), record);
  }
  const state = { records, byId, bySourceRef, manifest: JSON.parse(readFileSync(manifestPath, "utf8")), corpusPath, source_stamp: stamp };
  caches.set(cacheKey, state);
  return state;
}

export function clearExternalEstateCache() {
  caches.clear();
  worldLensCaches.clear();
  worldFormationCaches.clear();
  worldBridgeCaches.clear();
}

function recordMatches(record, query) {
  const projection = record.projection || {};
  const haystack = [record.id, record.source_ref, record.observed_name, record.section, record.source_kind,
    ...(projection.candidate_domains || []), ...(projection.candidate_affordances || []), ...(projection.candidate_worlds || []),
    ...reagentRolesForRecord(record)]
    .join(" ").toLowerCase();
  return tokenize(query).every((term) => haystack.includes(term));
}

function includesCandidate(record, field, value) {
  if (!value) return true;
  const values = record.projection?.[field] || [];
  return values.map(normalize).includes(normalize(value));
}

function matchingExternalRecords(state, options = {}) {
  return state.records.filter((record) =>
    (!options.query || recordMatches(record, options.query)) &&
    (!options.sourceKind || normalize(record.source_kind) === normalize(options.sourceKind)) &&
    (!options.reagentRole || reagentRolesForRecord(record).map(normalize).includes(normalize(options.reagentRole))) &&
    includesCandidate(record, "candidate_domains", options.domain) &&
    includesCandidate(record, "candidate_worlds", options.world) &&
    includesCandidate(record, "candidate_affordances", options.affordance));
}

function surfaceCandidateScore(record, profile) {
  const affordances = new Set((record.projection?.candidate_affordances || []).map(normalize));
  const domains = new Set((record.projection?.candidate_domains || []).map(normalize));
  const reagentRoles = new Set(reagentRolesForRecord(record).map(normalize));
  const matchedAffordances = profile.affordances.filter((value) => affordances.has(normalize(value)));
  const matchedDomains = profile.domains.filter((value) => domains.has(normalize(value)));
  const matchedReagentRoles = (SURFACE_REAGENT_ROLES[profile.grammar] || []).filter((value) => reagentRoles.has(normalize(value)));
  const sourceKindRank = profile.sourceKinds.map(normalize).indexOf(normalize(record.source_kind));
  const score = matchedReagentRoles.length * 250 + matchedAffordances.length * 100 + matchedDomains.length * 10 +
    (sourceKindRank < 0 ? 0 : profile.sourceKinds.length - sourceKindRank);
  return { score, matchedReagentRoles, matchedAffordances, matchedDomains, sourceKindRank };
}

function rankSurfaceCandidates(records, grammar, limit) {
  const profile = SURFACE_DEMAND_PROFILES[grammar] ? { ...SURFACE_DEMAND_PROFILES[grammar], grammar } : null;
  if (!profile) return { profile: null, matched: [], selected: [] };
  const matched = records.map((record) => ({ record, ...surfaceCandidateScore(record, profile) }))
    .filter((candidate) => candidate.score > 0)
    .sort((a, b) => b.score - a.score || a.sourceKindRank - b.sourceKindRank || a.record.source_ref.localeCompare(b.record.source_ref));
  /* The first viewport should reveal the source graph's heterogeneity.  Do not
   * let three rows from one carrier impersonate a composed World. */
  const selected = [];
  const usedKinds = new Set();
  for (const candidate of matched) {
    if (selected.length >= limit) break;
    const kind = normalize(candidate.record.source_kind);
    if (usedKinds.has(kind) && matched.some((other) => !usedKinds.has(normalize(other.record.source_kind)))) continue;
    selected.push(candidate);
    usedKinds.add(kind);
  }
  for (const candidate of matched) {
    if (selected.length >= limit) break;
    if (!selected.includes(candidate)) selected.push(candidate);
  }
  return { profile, matched, selected };
}

/** Search occurrences through the same cross-domain projection used by the site. */
export function listExternalEstate(repoRoot, options = {}) {
  const state = loadExternalEstate(repoRoot, options);
  const offset = Math.max(0, Number.parseInt(options.offset || "0", 10) || 0);
  const limit = boundedNumber(options.limit);
  const filtered = matchingExternalRecords(state, options);
  return {
    schema_version: "aurekai.external_estate.list.v1",
    truth_level: "observed_candidate",
    query: { query: options.query || null, source_kind: options.sourceKind || null, reagent_role: options.reagentRole || null, domain: options.domain || null, world: options.world || null, affordance: options.affordance || null },
    total: filtered.length,
    offset,
    limit,
    items: filtered.slice(offset, offset + limit),
    limits: ["Results are source-derived candidates; listing and probe outcomes do not admit a provider or authorize an action."],
  };
}

/**
 * Lower a bounded, public external-candidate slice into a SurfaceIR input.
 *
 * This is intentionally not SurfaceIR itself: the estate may supply candidate
 * material, but it cannot resolve an ActorClosure, exact audience, purpose,
 * evidence admission, provider readiness, or an effect. URL fields such as
 * near/state/time/relation are preserved as requested shape when this corpus
 * has no observed field capable of answering them. They therefore never turn
 * into a false positive filter or a claim about a source occurrence.
 */
export function compileExternalSurfaceSlice(repoRoot, options = {}) {
  const validation = validateExternalSurfaceSlice(options);
  if (!validation.ok) {
    const error = new Error(validation.forbidden.length
      ? "forbidden_slice_coordinate"
      : "invalid_surface_slice");
    error.code = validation.forbidden.length ? "forbidden_slice_coordinate" : "invalid_surface_slice";
    error.forbidden = validation.forbidden;
    error.invalid = validation.invalid;
    throw error;
  }

  const grammar = normalize(options.grammar);
  const extent = normalizeExtent(options.extent);
  const requestedShape = {
    kind: valueOrNull(options.kind), relation: valueOrNull(options.relation), near: valueOrNull(options.near),
    state: valueOrNull(options.state), time: valueOrNull(options.time), context: valueOrNull(options.context),
    mode: valueOrNull(options.mode),
  };
  const unresolvedShape = Object.entries(requestedShape)
    .filter(([, value]) => value !== null)
    .map(([coordinate, value]) => ({ coordinate, value, reason: "no_observed_candidate_field_in_this_corpus" }));
  const kindTerms = requestedShape.kind ? `${options.query || ""} ${requestedShape.kind}`.trim() : options.query;
  const requestedWorld = valueOrNull(options.world);
  const resolvedWorldDemand = requestedWorld && WORLD_DEMAND_ALIASES[requestedWorld]
    ? WORLD_DEMAND_ALIASES[requestedWorld] : requestedWorld;
  const state = loadExternalEstate(repoRoot);
  const matching = matchingExternalRecords(state, {
    query: kindTerms,
    sourceKind: options.sourceKind,
    reagentRole: options.reagentRole,
    domain: options.domain,
    world: resolvedWorldDemand,
    affordance: options.affordance || options.capability,
  });
  const ranking = rankSurfaceCandidates(matching, grammar, EXTENT_LIMITS[extent]);
  const candidates = ranking.selected.map((candidate) => ({
    ...candidate.record,
    surface_relevance: {
      score: candidate.score,
      matched_reagent_roles: candidate.matchedReagentRoles,
      matched_demand_affordances: candidate.matchedAffordances,
      matched_demand_domains: candidate.matchedDomains,
      limit: "Relevance is a deterministic presentation ranking, not provider admission, evidence admission, or a claim about the source.",
    },
  }));
  return {
    schema_version: "aurekai.external_estate.surface_candidate_slice.v1",
    truth_level: "observed_candidate",
    grammar,
    /* A direct public candidate request always receives the anonymous cell.
     * A native caller may later lower an already-resolved closure through a
     * different cell, but this endpoint never accepts that choice from URL or
     * browser state. */
    presentation_cell: presentationCell(grammar, "anonymous"),
    resolved_territory: territoryFromAddressHost(options.host),
    extent,
    mode: requestedShape.mode || "default",
    /* candidate_count is the number relevant to this grammar's declared
     * outside-world appetite.  reservoir_count remains visible so ranking is
     * never mistaken for a different truth set. */
    candidate_count: ranking.matched.length,
    candidate_reservoir_count: matching.length,
    candidates,
    surface_demand: {
      grammar,
      demand_roles: presentationCell(grammar, "anonymous").external_demand_roles,
      ranking_reagent_roles: SURFACE_REAGENT_ROLES[grammar] || [],
      ranking_affordances: ranking.profile.affordances,
      ranking_domains: ranking.profile.domains,
      carrier_diversity: ranking.profile.sourceKinds,
      world_demand: requestedWorld ? {
        requested: requestedWorld,
        resolved: resolvedWorldDemand,
        resolution: requestedWorld === resolvedWorldDemand ? "exact_observed_world" : "declared_world_demand_alias",
      } : null,
      limit: "SurfaceDemand ranks supplied candidates for presentation. It does not admit a provider, establish a World fact, or authorize an effect.",
    },
    requested_shape: {
      kind: requestedShape.kind, world: requestedWorld, relation: requestedShape.relation,
      near: requestedShape.near, state: requestedShape.state, time: requestedShape.time,
      source_kind: valueOrNull(options.sourceKind), capability: valueOrNull(options.capability),
      reagent_role: valueOrNull(options.reagentRole), affordance: valueOrNull(options.affordance), domain: valueOrNull(options.domain),
      context: requestedShape.context,
    },
    applied_candidate_filters: {
      query: kindTerms || null, source_kind: options.sourceKind || null, reagent_role: options.reagentRole || null, domain: options.domain || null,
      world: resolvedWorldDemand || null, affordance: options.affordance || options.capability || null,
    },
    unresolved_shape: unresolvedShape,
    actor_closure: "not accepted or resolved by external candidate lane",
    required_followups: ["AddressPlane actor/audience resolution", "InformationPolicy", "CapabilityClosure", "EvidenceGraph admission", "native effect gate"],
    forbidden_inferences: [
      "URL shape -> actor, audience, authority, institutional role, financial authority, or effect authority",
      "candidate slice membership -> provider readiness, admitted evidence, institutional subject, or lawful data combination",
      "near/state/time/relation request -> source occurrence matches that coordinate when no observed field exists",
    ],
  };
}

/** Public declarative catalogue. Reading a cell does not establish that the
 * caller belongs to that class; the native closure chooses a cell for a real
 * addressed surface. */
export function presentationCellCatalog() {
  return {
    schema_version: "aurekai.surface_presentation_cells.v1",
    cell_count: PRESENTATION_CELLS.length,
    grammars: SURFACE_GRAMMARS,
    presentation_classes: PRESENTATION_CLASSES,
    cells: PRESENTATION_CELLS,
    limits: ["Cells describe presentation only.", "A cell does not identify an actor, grant a role, admit a provider, disclose a field, authorize spending, or authorize an effect."],
  };
}

/** Resolve by stable occurrence id or supplied source reference. */
export function getExternalOccurrence(repoRoot, value, options = {}) {
  const state = loadExternalEstate(repoRoot, options);
  const key = normalize(decodeURIComponent(value || ""));
  return state.byId.get(key) || state.bySourceRef.get(key) || null;
}

/** Materialize a World as a queryable composition of existing source occurrences. */
export function composeExternalWorld(repoRoot, world, options = {}) {
  const result = listExternalEstate(repoRoot, { ...options, world, limit: options.limit || MAX_LIMIT });
  return {
    schema_version: "aurekai.external_estate.world_composition.v1",
    truth_level: "observed_candidate",
    world: normalize(world),
    occurrence_count: result.total,
    occurrences: result.items,
    required_followups: ["CapabilityClosure", "AuthorityGraph", "InformationPolicy", "EvidenceGraph", "ProtocolContract when multiple parties owe one another", "native effect gate before any external effect"],
    forbidden_inferences: ["composition membership -> provider readiness", "source listing -> lawful data combination", "candidate payment affordance -> authorized spend or settlement"],
  };
}

/**
 * Compile evidence-backed formation possibilities from a World reservoir.
 * This does not create an institution, a market, a budget, a member list, or
 * an action.  It tells a human exactly which different kinds of observed
 * material would make one worth designing, and which owner gates remain.
 */
export function compileExternalWorldFormation(repoRoot, world, options = {}) {
  const requestedWorld = normalize(world);
  const resolvedWorld = WORLD_DEMAND_ALIASES[requestedWorld] || requestedWorld;
  const estate = loadExternalEstate(repoRoot, options);
  /* Formation is a disposable read-only physical projection of one corpus
   * stamp. Caching it makes a full-estate census cheap after its first exact
   * compilation, while a corpus swap clears it alongside World lens indexes.
   * It never caches an owner fact or a provider observation. */
  const cacheKey = `${estate.corpusPath}\u0000${estate.source_stamp}\u0000${resolvedWorld}`;
  const cached = worldFormationCaches.get(cacheKey);
  if (cached) return cached;
  const records = matchingExternalRecords(estate, { world: resolvedWorld });
  const evidenceFor = (signal) => records.filter((record) => {
    const affordances = new Set((record.projection?.candidate_affordances || []).map(normalize));
    const roles = new Set(reagentRolesForRecord(record).map(normalize));
    if (signal === "material") return roles.has("material_source") || roles.has("dataset");
    if (signal === "publication") return affordances.has("publish") || roles.has("publication_target") || roles.has("material_source");
    return (FORMATION_SIGNAL_AFFORDANCES[signal] || []).some((value) => affordances.has(normalize(value)));
  });
  const recipes = WORLD_FORMATION_TEMPLATES.map((template) => {
    const signalEvidence = template.signals.map((signal) => {
      const matches = evidenceFor(signal);
      return {
        signal,
        occurrence_count: matches.length,
        occurrence_ids: matches.slice(0, 12).map((record) => record.id),
        state: matches.length ? "observed_candidate_material" : "no_observed_candidate_material",
      };
    });
    const missing = signalEvidence.filter((entry) => entry.occurrence_count === 0).map((entry) => entry.signal);
    return {
      id: template.id,
      title: template.title,
      state: missing.length ? "formation_candidate_incomplete" : "formation_candidate_for_human_design",
      candidate_artifacts: template.artifacts,
      possible_operator_archetypes: template.operator_archetypes,
      possible_project_facets: template.project_facets,
      signal_evidence: signalEvidence,
      missing_candidate_signals: missing,
      required_human_and_owner_lines: ["human decision to form or decline", "AddressPlane and identity owner", "InformationPolicy", "CapabilityClosure and evidence admission", "Authority and native effect gate before any action, spend, membership, governance, or transaction"],
      cannot_infer: "candidate signal coverage and a possible operator grammar do not establish a shared need, person in a role, participant consent, legitimacy, provider admission, budget, market liquidity, right to combine data, authority, or permission to launch",
    };
  });
  const material = {
    schema_version: "aurekai.external_estate.world_formation.v1",
    requested_world: requestedWorld,
    resolved_world: resolvedWorld,
    source_ids: records.map((record) => record.id).sort(),
    recipes: recipes.map((recipe) => ({ id: recipe.id, state: recipe.state, signal_evidence: recipe.signal_evidence })),
  };
  const formation = {
    schema_version: "aurekai.external_estate.world_formation.v1",
    truth_level: "observed_candidate",
    formation_digest: closureDigest(material),
    world: { requested: requestedWorld, resolved: resolvedWorld },
    occurrence_count: records.length,
    recipes,
    material,
    forbidden_inferences: [
      "a formation candidate -> an institution, market, project, software product, or public publication",
      "candidate source coverage -> a right to combine personal, institutional, or external data",
      "candidate market signals -> price, availability, provider admission, payment, settlement, or fulfillment",
    ],
  };
  worldFormationCaches.set(cacheKey, formation);
  return formation;
}

/* The estate-wide formation catalogue is deliberately a census, not a
 * recommendation queue.  It makes every declared World equally inspectable
 * as a possible composition and records which *kinds* of formation have
 * sufficient observed signal coverage.  It never ranks a World as more
 * deserving, asserts an actual shared need, or opens a project. */
export function compileExternalWorldFormationCatalogue(repoRoot, options = {}) {
  const estate = loadExternalEstate(repoRoot, options);
  const worlds = Object.keys(estate.manifest.candidate_world_occurrences || {}).sort();
  const formations = worlds.map((world) => compileExternalWorldFormation(repoRoot, world, options));
  const material = {
    schema_version: "aurekai.external_estate.formation_catalogue.v1",
    corpus_digests: estate.manifest.input,
    worlds: formations.map((formation) => ({
      world: formation.world.resolved,
      occurrence_count: formation.occurrence_count,
      formation_digest: formation.formation_digest,
      recipes: formation.recipes.map((recipe) => ({ id: recipe.id, state: recipe.state })),
    })),
  };
  return {
    schema_version: "aurekai.external_estate.formation_catalogue.v1",
    truth_level: "observed_candidate",
    closure_digest: closureDigest(material),
    world_count: formations.length,
    formation_kind_count: WORLD_FORMATION_TEMPLATES.length,
    formations: formations.map((formation) => ({
      world: formation.world,
      occurrence_count: formation.occurrence_count,
      formation_digest: formation.formation_digest,
      designable_formation_kinds: formation.recipes
        .filter((recipe) => recipe.state === "formation_candidate_for_human_design")
        .map((recipe) => recipe.id),
      incomplete_formation_kinds: formation.recipes
        .filter((recipe) => recipe.state === "formation_candidate_incomplete")
        .map((recipe) => ({ id: recipe.id, missing_candidate_signals: recipe.missing_candidate_signals })),
    })),
    material,
    forbidden_inferences: [
      "a World listed in the formation catalogue -> a launched product, project, institution, market, or public publication",
      "formation-kind coverage -> shared need, legitimacy, participant consent, budget, data-combination right, provider admission, authority, or effect permission",
      "more observed signals -> greater public value, priority, demand, or eligibility",
    ],
  };
}

/* Reagent membership can be shared by several addressed Worlds without those
 * Worlds being the same subject or being entitled to one another's state. The
 * bridge is an exact observed overlap index: useful for a human to see which
 * existing public material might support a future composition, never evidence
 * of a relationship, a common audience, a data-combination right or a launch. */
export function compileExternalWorldBridges(repoRoot, world, options = {}) {
  const requestedWorld = normalize(world);
  const resolvedWorld = WORLD_DEMAND_ALIASES[requestedWorld] || requestedWorld;
  const estate = loadExternalEstate(repoRoot, options);
  const cacheKey = `${estate.corpusPath}\u0000${estate.source_stamp}\u0000${resolvedWorld}`;
  const cached = worldBridgeCaches.get(cacheKey);
  if (cached) return cached;
  const records = matchingExternalRecords(estate, { world: resolvedWorld });
  const byNeighbour = new Map();
  for (const record of records) {
    const worlds = [...new Set(record.projection?.candidate_worlds || [])].map(normalize);
    const affordances = new Set((record.projection?.candidate_affordances || []).map(normalize));
    const roles = new Set(reagentRolesForRecord(record).map(normalize));
    const signals = new Set();
    if (roles.has("material_source") || roles.has("dataset")) signals.add("material");
    if (affordances.has("publish") || roles.has("publication_target") || roles.has("material_source")) signals.add("publication");
    for (const [signal, candidates] of Object.entries(FORMATION_SIGNAL_AFFORDANCES)) {
      if (candidates.some((candidate) => affordances.has(normalize(candidate)))) signals.add(signal);
    }
    for (const neighbour of worlds) {
      if (!neighbour || neighbour === resolvedWorld) continue;
      if (!byNeighbour.has(neighbour)) byNeighbour.set(neighbour, { ids: new Set(), sourceKinds: new Set(), signals: new Map() });
      const bridge = byNeighbour.get(neighbour);
      bridge.ids.add(record.id);
      bridge.sourceKinds.add(normalize(record.source_kind));
      for (const signal of signals) bridge.signals.set(signal, (bridge.signals.get(signal) || 0) + 1);
    }
  }
  const bridges = [...byNeighbour.entries()].map(([neighbour, bridge]) => ({
    world: neighbour,
    state: "shared_observed_reagent_membership",
    shared_occurrence_count: bridge.ids.size,
    shared_occurrence_ids: [...bridge.ids].sort(),
    source_kinds: [...bridge.sourceKinds].sort(),
    shared_candidate_signals: [...bridge.signals.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([signal, occurrence_count]) => ({ signal, occurrence_count })),
    cannot_infer: "shared source-candidate membership does not establish a relationship between Worlds, shared participants, common audience, spatial proximity, data-combination right, provider standing, authority, legitimacy, or permission to compose them",
  })).sort((a, b) => b.shared_occurrence_count - a.shared_occurrence_count || a.world.localeCompare(b.world));
  const material = {
    schema_version: "aurekai.external_estate.world_bridges.v1",
    corpus_digests: estate.manifest.input,
    requested_world: requestedWorld,
    resolved_world: resolvedWorld,
    bridges: bridges.map((bridge) => ({ world: bridge.world, shared_occurrence_ids: bridge.shared_occurrence_ids })),
  };
  const result = {
    schema_version: "aurekai.external_estate.world_bridges.v1",
    truth_level: "observed_candidate",
    bridge_digest: closureDigest(material),
    world: { requested: requestedWorld, resolved: resolvedWorld },
    occurrence_count: records.length,
    bridge_count: bridges.length,
    bridges,
    material,
    forbidden_inferences: [
      "shared observed reagent membership -> a semantic relationship, common audience, geographic adjacency, shared need, or lawful data combination",
      "a cross-World bridge -> a shared project, institution, market, publication, provider, authority, payment, settlement, or effect",
    ],
  };
  worldBridgeCaches.set(cacheKey, result);
  return result;
}

function readableWorldName(value) {
  return normalize(value).split("-").filter(Boolean)
    .map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1)}`).join(" ") || "World";
}

function closureDigest(value) {
  return `sha256:${createHash("sha256").update(JSON.stringify(canonicalClosureValue(value))).digest("hex")}`;
}

function canonicalClosureValue(value) {
  if (Array.isArray(value)) return value.map(canonicalClosureValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonicalClosureValue(value[key])]));
  }
  return value;
}

/* Bind the public product to the native forwarding plane when deployment has
 * supplied its sealed index and executable.  This is deliberately a read-only
 * witness: the public runtime never recreates mask calculus in JavaScript and
 * never treats an eligible survivor as an exact-owner selection. */
function nativeForwardingWitness(repoRoot, resolvedWorld, displayedIds, options = {}) {
  const indexPath = options.addressIndexPath || process.env.BONFYRE_ADDRESS_INDEX;
  const binaryPath = options.addressPlaneBinary || process.env.BONFYRE_ADDRESSPLANE_BINARY
    || join(repoRoot, "cmd", "BonfyreExternalEstate", "bonfyre-external-estate");
  if (!indexPath || !existsSync(indexPath)) {
    return { state: "not_configured", truth: "public_candidate_closure_without_native_forwarding_witness" };
  }
  if (!existsSync(binaryPath)) {
    return { state: "unavailable", reason: "address_plane_binary_missing", truth: "public_candidate_closure_without_native_forwarding_witness" };
  }
  try {
    const raw = execFileSync(binaryPath, ["address", "--db", indexPath, "--need", `world:${resolvedWorld}`, "--limit", "256"], {
      encoding: "utf8", timeout: 5000, maxBuffer: 1024 * 1024, stdio: ["ignore", "pipe", "ignore"],
    });
    const result = JSON.parse(raw);
    if (!Array.isArray(result.survivors) || !Number.isInteger(result.considered) ||
      !Number.isInteger(result.returned) || result.returned !== result.survivors.length ||
      !Number.isInteger(result.rejected_before_exact) || result.truth !== "eligible_for_further_consideration") {
      throw new Error("invalid_address_plane_witness");
    }
    const survivors = result.survivors.map((candidate) => String(candidate?.id || "")).filter(Boolean);
    if (survivors.length !== result.returned || new Set(survivors).size !== survivors.length ||
      result.returned > result.considered || result.rejected_before_exact > result.considered) {
      throw new Error("invalid_address_plane_witness");
    }
    const corpusIds = new Set(loadExternalEstate(repoRoot).records
      .filter((record) => record.projection?.candidate_worlds?.includes(resolvedWorld))
      .map((record) => record.id));
    if (survivors.some((identity) => !corpusIds.has(identity))) {
      const error = new Error("native_forwarding_mismatch");
      error.code = "native_forwarding_mismatch";
      throw error;
    }
    const possiblyTruncated = result.returned === 256;
    const survivorSet = new Set(survivors);
    return {
      state: "verified",
      truth: "eligible_for_further_consideration",
      requested_need: `world:${resolvedWorld}`,
      considered: result.considered,
      rejected_before_exact: result.rejected_before_exact,
      returned: result.returned,
      possibly_truncated: possiblyTruncated,
      survivor_digest: closureDigest(survivors),
      displayed_candidate_membership: possiblyTruncated
        ? "not_assessed_native_return_cap"
        : displayedIds.every((identity) => survivorSet.has(identity)),
      _eligible_candidate_ids: survivors,
      limits: ["Native mask survival is eligible-for-further-consideration only.", "Exact owner, information policy, evidence, rights, admission and effect checks remain downstream."],
    };
  } catch (error) {
    if (error?.code === "native_forwarding_mismatch") throw error;
    return { state: "unavailable", reason: "native_address_plane_query_failed", truth: "public_candidate_closure_without_native_forwarding_witness" };
  }
}

function restrictLensesToNativeEligibility(lenses, eligibleIds) {
  const allowed = new Set(eligibleIds);
  return lenses.map((lens) => ({
    ...lens,
    candidates: lens.candidates.filter((candidate) => allowed.has(candidate.id)),
  }));
}

function compiledWorldLenses(repoRoot, resolvedWorld, host) {
  /* The human World and every focused lens share the same deterministic lens
   * field. Cache the derived field, never an owner fact: clearing the corpus
   * cache clears this representation at the same boundary. */
  const cacheKey = `${repoRoot}\u0000${resolvedWorld}\u0000${territoryFromAddressHost(host)}`;
  if (worldLensCaches.has(cacheKey)) return worldLensCaches.get(cacheKey);
  const lenses = SURFACE_GRAMMARS.map((grammar) => compileExternalSurfaceSlice(repoRoot, {
    grammar, world: resolvedWorld, host, extent: "section",
  })).filter((slice) => slice.candidate_count > 0)
    .sort((a, b) => b.candidate_count - a.candidate_count || a.grammar.localeCompare(b.grammar))
    .map((slice) => ({
      grammar: slice.grammar,
      presentation_cell: slice.presentation_cell,
      candidate_count: slice.candidate_count,
      candidate_reservoir_count: slice.candidate_reservoir_count,
      candidates: slice.candidates,
      surface_demand: slice.surface_demand,
      forbidden_inferences: slice.forbidden_inferences,
    }));
  worldLensCaches.set(cacheKey, lenses);
  return lenses;
}

/**
 * Compile a public World into one bounded, content-addressed closure.
 *
 * This is deliberately more than an estate search page: each lens is lowered
 * from the same addressed World candidate reservoir, and both the HTML page
 * and the machine document consume this exact record.  It remains a public
 * candidate aperture -- it cannot resolve an ActorClosure, admit a provider,
 * or turn a source into a published institutional fact.
 */
export function compileExternalWorldClosure(repoRoot, world, options = {}) {
  const requestedWorld = normalize(world);
  const resolvedWorld = WORLD_DEMAND_ALIASES[requestedWorld] || requestedWorld;
  const host = options.host || "";
  const requestedGrammar = options.grammar ? normalize(options.grammar) : null;
  if (requestedGrammar && !SURFACE_GRAMMARS.includes(requestedGrammar)) {
    const error = new Error("invalid_world_lens");
    error.code = "invalid_world_lens";
    error.invalid = ["grammar"];
    throw error;
  }
  const worldComposition = composeExternalWorld(repoRoot, resolvedWorld, { limit: MAX_LIMIT });
  const formation = compileExternalWorldFormation(repoRoot, resolvedWorld);
  const availableLenses = compiledWorldLenses(repoRoot, resolvedWorld, host);
  const requestedLenses = requestedGrammar
    ? availableLenses.filter((lens) => lens.grammar === requestedGrammar)
    : availableLenses.slice(0, WORLD_PRODUCT_LENS_LIMIT);
  const requestedCandidateIds = [...new Set(requestedLenses.flatMap((lens) => lens.candidates.map((candidate) => candidate.id)))].sort();
  const nativeForwardingResult = nativeForwardingWitness(repoRoot, resolvedWorld, requestedCandidateIds, options);
  const { _eligible_candidate_ids: eligibleCandidateIds = [], ...nativeForwarding } = nativeForwardingResult;
  if (options.requireNativeForwarding && nativeForwarding.state !== "verified") {
    const error = new Error("native_forwarding_unavailable");
    error.code = "native_forwarding_unavailable";
    throw error;
  }
  const lenses = nativeForwarding.state === "verified"
    ? restrictLensesToNativeEligibility(requestedLenses, eligibleCandidateIds)
    : requestedLenses;
  const candidateIds = [...new Set(lenses.flatMap((lens) => lens.candidates.map((candidate) => candidate.id)))].sort();
  if (nativeForwarding.state === "verified") {
    nativeForwarding.displayed_candidate_membership = true;
    nativeForwarding.forwarded_candidate_count = candidateIds.length;
    nativeForwarding.withheld_candidate_count = requestedCandidateIds.length - candidateIds.length;
  }
  const material = {
    schema_version: "aurekai.external_estate.world_closure.v1",
    corpus_digests: loadExternalEstate(repoRoot).manifest.input,
    requested_world: requestedWorld,
    resolved_world: resolvedWorld,
    requested_grammar: requestedGrammar,
    lenses: lenses.map((lens) => ({ grammar: lens.grammar, candidate_ids: lens.candidates.map((candidate) => candidate.id) })),
    candidate_ids: candidateIds,
    native_forwarding: nativeForwarding,
    formation: formation.material,
  };
  const digest = closureDigest(material);
  return {
    schema_version: "aurekai.external_estate.world_closure.v1",
    truth_level: "observed_candidate",
    closure_digest: digest,
    world: {
      requested: requestedWorld,
      resolved: resolvedWorld,
      title: readableWorldName(requestedWorld),
      resolution: requestedWorld === resolvedWorld ? "exact_observed_world" : "declared_world_demand_alias",
    },
    resolved_territory: territoryFromAddressHost(host),
    occurrence_count: worldComposition.occurrence_count,
    candidate_count: candidateIds.length,
    available_lenses: availableLenses.map((lens) => ({ grammar: lens.grammar, candidate_count: lens.candidate_count })),
    view: requestedGrammar ? "lens" : "world",
    lenses,
    native_forwarding: nativeForwarding,
    formation,
    material,
    required_followups: ["AddressPlane actor/audience resolution", "InformationPolicy", "CapabilityClosure", "EvidenceGraph admission", "native effect gate"],
    forbidden_inferences: [
      "World candidate membership -> provider readiness, admitted evidence, institutional fact, authority, payment, settlement, or effect",
      "public closure digest -> an ActorClosure or a private/institutional projection",
      "lens ranking -> semantic truth or exact-owner selection",
    ],
  };
}

/* A public World closure can use the same scene/runtime ABI as a CMS-authored
 * SurfaceScene without pretending that this candidate lane owns CMS state.
 * The browser receives interaction coordinates and derived regions only; it
 * cannot obtain an ActorClosure or turn a candidate operation into an effect. */
export function compileExternalWorldSurfaceScene(repoRoot, world, options = {}) {
  const closure = compileExternalWorldClosure(repoRoot, world, options);
  const machineBase = options.basePath || "/external-estate";
  const worldPath = `${machineBase}/v1/worlds/${encodeURIComponent(closure.world.requested)}`;
  const lensPath = (grammar) => `${worldPath}/${encodeURIComponent(grammar)}/closure`;
  const sceneLenses = closure.lenses.map((lens) => lens.grammar);
  const selected = closure.lenses.length ? closure.lenses : closure.available_lenses.slice(0, 1).map((lens) => ({
    ...lens,
    presentation_cell: presentationCell(lens.grammar, "anonymous"),
    candidates: [], surface_demand: {},
  }));
  return {
    schema: "bonfyre-surface-scene.v1",
    source_digest: closure.closure_digest,
    focus: {
      subject_address: `world:${closure.world.resolved}`,
      world: closure.world.resolved,
      requested_world: closure.world.requested,
      actor_scope: "public",
      truth: "observed_candidate",
    },
    lenses: sceneLenses,
    slots: selected.map((lens) => ({
      slot_id: `external-world:${closure.world.resolved}:${lens.grammar}`,
      extent: "page", accepts: ["ExternalEstateCandidate"], max_items: lens.candidates.length,
      htmx_boundary: `public-world:${closure.world.resolved}:${lens.grammar}`,
    })),
    fragments: selected.map((lens) => ({
      fragment_id: `external-world:${closure.world.resolved}:${lens.grammar}`,
      surface_grammar: lens.grammar,
      renderer_family: lens.presentation_cell.preferred_renderers[0] || "candidate_stream",
      renderer_variant: lens.presentation_cell.preferred_renderers[1] || "",
      extent: "page",
      continuity_group: `world:${closure.world.resolved}`,
      source_projection: lensPath(lens.grammar),
      media_posture: "source-derived-candidate",
      temporal_posture: lens.surface_demand?.ranking_affordances?.includes("STREAM") ? "live_candidate" : "observed_candidate",
      action_aperture: "inspect,transport_check",
    })),
    bindings: [], stages: [],
    patterns: selected.map((lens) => ({
      pattern: `external-world:${lens.grammar}`,
      reads_type: "ExternalEstateOccurrence",
      reads_organ: "ExternalEstate",
      organ_command: "address-candidate-cut",
      effect_class: "QUERY",
      requires_authority: "",
      time_field: "",
      time_mode: "candidate_frontier",
    })),
    presentation: selected.map((lens) => ({
      grammar: lens.grammar,
      presentation_class: lens.presentation_cell.presentation_class,
      visible_sections: lens.presentation_cell.visible_sections,
      renderers: lens.presentation_cell.preferred_renderers,
    })),
    renderers: selected.flatMap((lens) => lens.presentation_cell.preferred_renderers),
    operations: {
      apertures: ["inspect", "transport_check"],
      object_contracts: [], proposal_contracts: [],
    },
    interaction: {
      default_lens: options.grammar || selected[0]?.grammar || sceneLenses[0] || "",
      lens_order: sceneLenses,
      semantic_zoom: [
        { resolution: 0, name: "world" }, { resolution: 2, name: "lens" }, { resolution: 4, name: "occurrence" },
      ],
      interaction_depths: ["peek", "expand", "focus"],
      context_rail: { mode: "floating", source: "closure" },
      relation_halo: { enabled: true, max: 8, source: "candidate_coordinates" },
      command_aperture: { shortcut: "Meta+K", mode: "contextual", state: "informational_only" },
      transition_policy: { object_permanence: true, view_transition: "auto" },
      mobile_posture: { primary: "vertical", lens_gesture: "swipe" },
      desktop_posture: { inspector: "persistent", keyboard: "primary" },
      live_update_policy: { mode: "SurfaceDelta", transport: "closure_refresh" },
    },
    runtime_queries: {
      public_world_closure: `${worldPath}/closure`,
      public_scene: `${worldPath}/scene`,
      focused_lenses: selected.map((lens) => lensPath(lens.grammar)),
    },
    dependencies: [
      `ExternalEstate:world:${closure.world.resolved}`,
      `PublicWorldClosure:${closure.closure_digest}`,
    ],
    cannot_infer: "a public SurfaceScene is an interaction projection over observed candidates; it does not grant identity, visibility, authority, availability, price, proof, commitment, presence, target completion or effect state",
  };
}

/**
 * Product map for the public estate. This is a declaration-level aperture:
 * it names every measured World and every reusable grammar route without
 * claiming that any candidate is admitted, current, or actionable.
 */
export function compileExternalWorldCatalogue(repoRoot) {
  const manifest = loadExternalEstate(repoRoot).manifest;
  const worlds = Object.entries(manifest.candidate_world_occurrences || {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([world, occurrenceCount]) => ({
      world,
      title: readableWorldName(world),
      occurrence_count: occurrenceCount,
      grammars: [...SURFACE_GRAMMARS],
    }));
  const material = {
    schema_version: "aurekai.external_estate.world_catalogue.v1",
    corpus_digests: manifest.input,
    worlds: worlds.map((world) => ({ world: world.world, occurrence_count: world.occurrence_count, grammars: world.grammars })),
  };
  return {
    schema_version: "aurekai.external_estate.world_catalogue.v1",
    truth_level: "observed_candidate",
    closure_digest: closureDigest(material),
    world_count: worlds.length,
    grammar_count: SURFACE_GRAMMARS.length,
    lens_route_count: worlds.length * SURFACE_GRAMMARS.length,
    worlds,
    material,
    required_followups: ["AddressPlane actor/audience resolution", "InformationPolicy", "CapabilityClosure", "EvidenceGraph admission", "native effect gate"],
    forbidden_inferences: [
      "a declared World route -> an admitted candidate, provider, institutional fact, authority, payment, settlement, or effect",
      "a grammar route -> an ActorClosure or private/institutional projection",
    ],
  };
}

/**
 * Perform one explicit, no-body HEAD request against an http(s) primary URL.
 * Redirects are deliberately not followed and the result remains transport
 * telemetry only.  The caller must invoke this; list/search never probe.
 */
export async function probeExternalOccurrence(record, options = {}) {
  const timeoutMs = Math.max(1000, Math.min(Number(options.timeoutMs) || 8000, 15000));
  let url;
  try { url = new URL(record?.primary_url); } catch { return { status: "refused", reason: "invalid_primary_url" }; }
  if (url.protocol !== "https:" && url.protocol !== "http:") return { status: "refused", reason: "non_http_primary_url" };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "HEAD", redirect: "manual", signal: controller.signal,
      headers: { "User-Agent": "Aurekai-ExternalEstate/1.0 (+read-only-probe)", "Accept": "*/*" },
    });
    return {
      schema_version: "aurekai.external_estate.transport_probe.v1",
      truth_level: "transport_observation_candidate",
      source_ref: record.source_ref, occurrence_id: record.id, requested_url: url.toString(),
      status: "responded", http_status: response.status, location: response.headers.get("location"),
      observed_at: new Date().toISOString(),
      limits: ["HEAD response only; no body is retained.", "A transport response is not capability admission, evidence, authority, settlement, delivery, or completion."],
    };
  } catch (error) {
    return {
      schema_version: "aurekai.external_estate.transport_probe.v1", truth_level: "transport_observation_candidate",
      source_ref: record?.source_ref || null, occurrence_id: record?.id || null, requested_url: url.toString(),
      status: "unreachable", reason: error?.name === "AbortError" ? "timeout" : "transport_error", observed_at: new Date().toISOString(),
      limits: ["No provider capability is inferred from an unreachable probe."],
    };
  } finally { clearTimeout(timer); }
}

function escapeHtml(value) {
  return String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function queryLink(base, params = {}) {
  const query = new URLSearchParams(Object.entries(params).filter(([, value]) => value !== null && value !== undefined && value !== ""));
  return `${base || "/"}${query.size ? `?${query}` : ""}`;
}

function occurrenceCard(base, record) {
  const p = record.projection || {};
  const id = encodeURIComponent(record.id);
  return `<article class="external-card" data-bonfyre-occurrence="${escapeHtml(record.id)}" data-bonfyre-source-kind="${escapeHtml(record.source_kind)}"><p class="external-kicker">${escapeHtml(record.source_kind)} · ${escapeHtml(record.source_ref)}</p><h2><a href="${base}/occurrences/${id}">${escapeHtml(record.observed_name)}</a></h2><p>${escapeHtml(record.section)}</p><p class="external-tags">${(p.candidate_domains || []).slice(0, 10).map((d) => `<a href="${queryLink(base, { domain: d })}">${escapeHtml(d)}</a>`).join(" ")}</p><p><a rel="noreferrer" href="${escapeHtml(record.primary_url)}">Source</a> · <a href="${base}/occurrences/${id}">inspect</a> · <a href="${base}/v1/occurrences/${id}">JSON</a></p><form class="transport-check" method="post" action="${base}/occurrences/${id}/probe"><button>Check transport</button></form></article>`;
}

function rendererClasses(cell) {
  return (cell?.preferred_renderers || []).map((renderer) => `renderer-${String(renderer).replace(/[^a-z0-9_-]/gi, "-")}`).join(" ");
}

export function renderExternalEstateHtml(repoRoot, options = {}) {
  const base = options.basePath || "";
  const result = listExternalEstate(repoRoot, options);
  const domains = Object.keys(loadExternalEstate(repoRoot, options).manifest.candidate_domain_occurrences || {}).slice(0, 63);
  const worlds = Object.keys(loadExternalEstate(repoRoot, options).manifest.candidate_world_occurrences || {});
  const previous = result.offset > 0 ? `<a href="${queryLink(base, { ...result.query, offset: Math.max(0, result.offset - result.limit), limit: result.limit })}">← previous</a>` : "";
  const next = result.offset + result.items.length < result.total ? `<a href="${queryLink(base, { ...result.query, offset: result.offset + result.limit, limit: result.limit })}">next →</a>` : "";
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Aurekai external estate</title><style>body{margin:0;background:#101513;color:#e9eee8;font:16px/1.45 system-ui,sans-serif}main{max-width:1200px;margin:auto;padding:2rem}a{color:#a7e3bd}.eyebrow,.external-kicker{color:#94a89a;font-size:.78rem;text-transform:uppercase;letter-spacing:.08em}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:1rem}.external-card{border:1px solid #334239;border-radius:12px;padding:1rem;background:#172019}.external-card h2{font-size:1.1rem;margin:.2rem 0}.external-tags a{display:inline-block;border:1px solid #41624d;border-radius:99px;padding:.12rem .45rem;margin:.1rem;font-size:.78rem;text-decoration:none}input,select,button{background:#e9eee8;color:#102015;border:0;border-radius:7px;padding:.55rem;margin:.2rem}form{display:flex;flex-wrap:wrap;gap:.3rem;margin:1rem 0}.note{border-left:3px solid #d6ae4b;padding:.8rem 1rem;background:#252117}.worlds a,.pager a{margin-right:.7rem}.pager{display:flex;justify-content:space-between;margin:1.3rem 0}.product-link{display:inline-block;border:1px solid #a7e3bd;border-radius:8px;padding:.65rem .9rem;text-decoration:none}</style><main><p class="eyebrow">Aurekai / Bonfyre · external reagent library</p><h1>External estate, as usable candidate composition</h1><p>${result.total.toLocaleString()} matching source occurrences. Every card preserves its exact supplied source line; candidates require admission before use.</p><p><a class="product-link" href="${base}/worlds">Browse ${worlds.length} compiled World products →</a></p><form method="get" action="${base || "/"}"><input name="q" value="${escapeHtml(options.query || "")}" placeholder="search source, domain, world or affordance"><select name="domain"><option value="">any domain</option>${domains.map((d) => `<option ${options.domain === d ? "selected" : ""}>${escapeHtml(d)}</option>`).join("")}</select><select name="world"><option value="">any World</option>${worlds.map((w) => `<option ${options.world === w ? "selected" : ""}>${escapeHtml(w)}</option>`).join("")}</select><button>Search</button></form><p class="worlds"><strong>Worlds:</strong> ${worlds.map((w) => `<a href="${base}/worlds/${encodeURIComponent(w)}">${escapeHtml(w)}</a>`).join("")}</p><p class="note">A source link, a protocol, or a successful HEAD probe does not establish a provider relationship, permission, payment, settlement, delivery, evidence, or institutional truth.</p><section class="grid">${result.items.map((record) => occurrenceCard(base, record)).join("")}</section><nav class="pager"><span>${previous}</span><span>${result.offset + 1}–${result.offset + result.items.length} of ${result.total}</span><span>${next}</span></nav></main></html>`;
}

export function renderExternalWorldCatalogueHtml(repoRoot, options = {}) {
  const base = options.basePath || "";
  const catalogue = compileExternalWorldCatalogue(repoRoot);
  const cards = catalogue.worlds.map((world) => `<article class="world-card"><p>${world.occurrence_count.toLocaleString()} observed candidate occurrences</p><h2><a href="${base}/worlds/${encodeURIComponent(world.world)}">${escapeHtml(world.title)}</a></h2><details><summary>${world.grammars.length} product lenses</summary><p>${world.grammars.map((grammar) => `<a href="${base}/worlds/${encodeURIComponent(world.world)}/${encodeURIComponent(grammar)}">${escapeHtml(grammar)}</a>`).join(" ")}</p></details></article>`).join("");
  const machinePath = `${base}/v1/worlds`;
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="bonfyre-closure-digest" content="${escapeHtml(catalogue.closure_digest)}"><link rel="alternate" type="application/json" href="${escapeHtml(machinePath)}"><title>Aurekai Worlds</title><style>body{margin:0;background:#101513;color:#e9eee8;font:16px/1.45 system-ui,sans-serif}main{max-width:1280px;margin:auto;padding:2rem}a{color:#a7e3bd}.eyebrow{color:#94a89a;font-size:.78rem;text-transform:uppercase;letter-spacing:.08em}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:1rem}.world-card{border:1px solid #334239;border-radius:12px;padding:1rem;background:#172019}.world-card h2{font-size:1.25rem;margin:.2rem 0}.world-card p{color:#c4cec5}.world-card details{margin-top:.8rem}.world-card details a{display:inline-block;border:1px solid #41624d;border-radius:99px;padding:.12rem .45rem;margin:.12rem;font-size:.78rem;text-decoration:none}.machine{font:12px ui-monospace,monospace;color:#94a89a;word-break:break-all}</style><main data-closure-digest="${escapeHtml(catalogue.closure_digest)}"><p class="eyebrow">Aurekai / Bonfyre · product map</p><p><a href="${base || "/"}">← external estate</a></p><h1>${catalogue.world_count} Worlds. ${catalogue.grammar_count} interaction grammars. ${catalogue.lens_route_count} focused product apertures.</h1><p>Every World route and focused lens is compiled from the measured candidate estate. Human and machine carriers name the same closure digest; neither creates authority or promotes a candidate into institutional truth.</p><p class="machine">closure ${escapeHtml(catalogue.closure_digest)} · <a href="${escapeHtml(machinePath)}">machine catalogue</a></p><section class="grid">${cards}</section></main></html>`;
}

/** Render the human carrier from the same closure returned by the World closure route. */
export function renderExternalWorldHtml(repoRoot, world, options = {}) {
  const base = options.basePath || "";
  const closure = compileExternalWorldClosure(repoRoot, world, options);
  const worldPath = `${base}/worlds/${encodeURIComponent(closure.world.requested)}`;
  const machinePath = `${base}/v1/worlds/${encodeURIComponent(closure.world.requested)}${options.grammar ? `/${encodeURIComponent(normalize(options.grammar))}` : ""}/closure`;
  const scene = compileExternalWorldSurfaceScene(repoRoot, world, { ...options, basePath: base });
  const sceneJson = JSON.stringify(scene).replace(/</g, "\\u003c");
  const sceneControls = `<section class="surface-scene-shell" data-bonfyre-scene="public-world"><aside class="context-rail" aria-label="Context rail"><p class="external-kicker">Focus</p><strong>${escapeHtml(scene.focus.subject_address)}</strong><p>${closure.candidate_count} presented candidates<br>${closure.occurrence_count} World occurrences</p><p class="machine">${escapeHtml(scene.source_digest)}</p></aside><nav class="scene-lens-nav" aria-label="Surface lenses">${scene.lenses.map((lens) => `<button type="button" data-bonfyre-lens-target="${escapeHtml(lens)}">${escapeHtml(lens)}</button>`).join("")}</nav><div class="scene-zoom" aria-label="Semantic zoom">${scene.interaction.semantic_zoom.map((level) => `<button type="button" data-bonfyre-resolution-target="${level.resolution}">${escapeHtml(level.name)}</button>`).join("")}</div><aside class="relation-halo" aria-label="Relation halo"><p class="external-kicker">Relation halo</p><span>World</span><span>territory:${escapeHtml(closure.resolved_territory)}</span><span>AddressPlane</span><span>observed candidates</span></aside><aside class="scene-inspector" aria-live="polite"><p class="external-kicker">Inspector</p><strong data-bonfyre-inspector-title>World focus</strong><p data-bonfyre-inspector-detail>Select a candidate to preserve this World while inspecting its source coordinate.</p></aside><dialog class="command-aperture"><p class="external-kicker">Contextual aperture</p><p>This public scene exposes inspection and explicit transport checks only. Exact owners decide all other operations.</p><button type="button" data-bonfyre-command-close>Close</button></dialog></section>`;
  const lensStyle = `<style>.renderer-faceted_stream .grid,.renderer-candidate_cards .grid,.renderer-activity_wall .grid{grid-template-columns:repeat(auto-fit,minmax(290px,1fr))}.renderer-focus_panel .external-card:first-child,.renderer-preview_panel .external-card:first-child{grid-column:1/-1;background:#203526}.renderer-topic_index .external-card,.renderer-handoff_list .external-card,.renderer-change_stream .external-card{border-left:3px solid #67b5d8}.renderer-community_profile .external-card{background:#1e2c25}.renderer-planning_canvas .grid,.renderer-studio_canvas .grid{grid-template-columns:repeat(auto-fit,minmax(300px,1fr));padding:1rem;background:linear-gradient(90deg,#18251d 1px,transparent 1px),linear-gradient(#18251d 1px,transparent 1px);background-size:24px 24px}.renderer-compare_tray .external-card{border-top:3px solid #d6ae4b}.renderer-dependency_graph .external-card{border-radius:4px;border-style:dashed}.renderer-composition_map .external-card{outline:1px solid #41624d;outline-offset:4px}.renderer-relationship_inspector .external-card,.renderer-machine_card .external-card{font-family:ui-monospace,monospace}.renderer-evidence_ledger .external-card:before,.renderer-observatory .external-card:before{content:"observed candidate";display:block;color:#94a89a;font:11px ui-monospace,monospace;margin-bottom:.4rem}</style>`;
  /* Reconnect asks the owner for a complete, content-addressed Scene. The
   * browser does not recompile candidate material or replay a queued patch. */
  const sceneRuntime = `<script type="module">import {SurfaceRuntime} from "${escapeHtml(`${base}/assets/surface-runtime.mjs`)}";const scene=${sceneJson};const runtime=new SurfaceRuntime(scene,{root:document}).bind();const sync=model=>{for(const region of document.querySelectorAll("[data-bonfyre-world-lens]"))region.hidden=region.dataset.bonfyreWorldLens!==model.activeLens;for(const button of document.querySelectorAll("[data-bonfyre-lens-target]"))button.setAttribute("aria-pressed",String(button.dataset.bonfyreLensTarget===model.activeLens));};const reconcile=async()=>{const digest=runtime.model.sourceDigest.replace(/^sha256:/,"");const response=await fetch(scene.runtime_queries.public_scene,{credentials:"same-origin",headers:{Accept:"application/json","If-None-Match":"\\\""+digest+"\\\""}});if(response.status===304||!response.ok)return;const successor=await response.json();document.dispatchEvent(new CustomEvent("bonfyre:surface-delta",{detail:{schema:"bonfyre-surface-delta.v1",before_digest:runtime.model.sourceDigest,after_digest:successor.source_digest,scene:successor}}));};sync(runtime.model);document.addEventListener("bonfyre:lens",event=>sync(event.detail.model));document.addEventListener("bonfyre:surface-reconnect",event=>{if(event.detail?.needsRefresh)reconcile().catch(()=>{});});document.addEventListener("bonfyre:command-aperture",()=>document.querySelector(".command-aperture")?.showModal());document.addEventListener("click",event=>{const close=event.target.closest("[data-bonfyre-command-close]");if(close){close.closest("dialog")?.close();return;}const card=event.target.closest("[data-bonfyre-occurrence]");if(!card)return;runtime.setEphemeral("selected",card.dataset.bonfyreOccurrence);const title=document.querySelector("[data-bonfyre-inspector-title]"),detail=document.querySelector("[data-bonfyre-inspector-detail]");if(title)title.textContent=card.querySelector("h2")?.textContent||card.dataset.bonfyreOccurrence;if(detail)detail.textContent=card.dataset.bonfyreOccurrence+" · "+card.dataset.bonfyreSourceKind+" · observed candidate; inspect or explicitly check transport before any owner decision.";});</script>`;
  const lensHtml = `${lensStyle}${sceneControls}<div class="surface-scene-regions">${closure.lenses.map((lens) => `<section class="world-lens ${escapeHtml(rendererClasses(lens.presentation_cell))}" id="${escapeHtml(lens.grammar)}" data-bonfyre-region="external-world:${escapeHtml(closure.world.resolved)}:${escapeHtml(lens.grammar)}" data-bonfyre-world-lens="${escapeHtml(lens.grammar)}"><header><p class="external-kicker">${escapeHtml(lens.presentation_cell.question)}</p><h2>${escapeHtml(lens.grammar)}</h2><p class="section-roles">${lens.presentation_cell.visible_sections.map((section) => `<span>${escapeHtml(section)}</span>`).join("")}</p><p>${lens.candidate_count.toLocaleString()} candidates in this lens; showing ${lens.candidates.length} diverse source records through ${escapeHtml(lens.presentation_cell.preferred_renderers.join(" + "))}.</p></header><div class="grid">${lens.candidates.map((candidate) => occurrenceCard(base, candidate)).join("")}</div></section>`).join("")}</div>${sceneRuntime}`;
  const formationPanel = `<section class="formation"><p class="external-kicker">What this World could become</p><h2>Formation candidates, not launches</h2><p>The estate can show which distinct observed materials would support a living publication, service, market, coordination space, place surface, or small institution. A person and the existing owners still decide whether any of those things should exist.</p><div class="formation-grid">${closure.formation.recipes.map((recipe) => `<article class="formation-card"><p class="external-kicker">${escapeHtml(recipe.state)}</p><h3>${escapeHtml(recipe.title)}</h3><p>${recipe.candidate_artifacts.map(escapeHtml).join(" · ")}</p><p><strong>Possible operators</strong><br>${recipe.possible_operator_archetypes.map((archetype) => escapeHtml(archetype.replaceAll("-", " "))).join(" · ")}</p><p><strong>Possible project facets</strong><br>${recipe.possible_project_facets.map(escapeHtml).join(" · ")}</p><p class="machine">${recipe.signal_evidence.map((signal) => `${escapeHtml(signal.signal)}: ${signal.occurrence_count}`).join(" · ")}</p>${recipe.missing_candidate_signals.length ? `<p class="note">Missing candidate signals: ${escapeHtml(recipe.missing_candidate_signals.join(", "))}</p>` : `<p class="note">Human design is possible; no person, institution, market, budget, or effect has been created.</p>`}</article>`).join("")}</div><p class="machine">formation ${escapeHtml(closure.formation.formation_digest)}</p></section>`;
  const empty = closure.lenses.length === 0 ? "<section class=\"note\"><strong>No candidate lens is currently materialized for this World.</strong> The World remains an empty public projection; no source is invented to fill it.</section>" : "";
  const forwarding = closure.native_forwarding.state === "verified"
    ? `native AddressPlane mask: ${closure.native_forwarding.returned} eligible survivors; ${closure.native_forwarding.rejected_before_exact} rejected before exact ownership`
    : "native AddressPlane witness is not configured for this public carrier";
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="bonfyre-closure-digest" content="${escapeHtml(closure.closure_digest)}"><link rel="alternate" type="application/json" href="${escapeHtml(machinePath)}"><title>${escapeHtml(closure.world.title)}${options.grammar ? ` · ${escapeHtml(normalize(options.grammar))}` : ""} · Aurekai</title><style>body{margin:0;background:#101513;color:#e9eee8;font:16px/1.45 system-ui,sans-serif}main{max-width:1280px;margin:auto;padding:2rem}a{color:#a7e3bd}.eyebrow,.external-kicker{color:#94a89a;font-size:.78rem;text-transform:uppercase;letter-spacing:.08em}.hero{border-bottom:1px solid #334239;padding:1rem 0 1.5rem;margin-bottom:1.5rem}.hero h1{font-size:clamp(2rem,5vw,4.6rem);line-height:1;margin:.25rem 0}.hero p{max-width:50rem;color:#c4cec5}.lens-nav{display:flex;flex-wrap:wrap;gap:.5rem;margin:1rem 0}.lens-nav a{border:1px solid #41624d;border-radius:99px;padding:.3rem .7rem;text-decoration:none}.lens-nav a[aria-current=page]{background:#a7e3bd;color:#102015}.world-lens{margin:2.6rem 0}.world-lens h2{font-size:1.7rem;margin:.2rem 0}.section-roles{display:flex;flex-wrap:wrap;gap:.35rem}.section-roles span{color:#c4cec5;border:1px solid #334239;border-radius:99px;padding:.08rem .45rem;font:11px ui-monospace,monospace}.grid,.formation-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:1rem}.external-card,.formation-card{border:1px solid #334239;border-radius:12px;padding:1rem;background:#172019}.external-card h2,.formation-card h3{font-size:1.1rem;margin:.2rem 0}.external-tags a{display:inline-block;border:1px solid #41624d;border-radius:99px;padding:.12rem .45rem;margin:.1rem;font-size:.78rem;text-decoration:none}.formation{margin:2rem 0;padding:1rem;border:1px solid #41624d;background:#14231b}.formation h2{margin:.2rem 0}.formation-card{background:#101913}.renderer-timeline .grid,.renderer-threaded_stream .grid,.renderer-priority_queue .grid,.renderer-evidence_ledger .grid,.renderer-observatory .grid{grid-template-columns:1fr}.renderer-timeline .external-card,.renderer-threaded_stream .external-card{border-left:4px solid #a7e3bd}.renderer-masonry_board .grid,.renderer-world_gallery .grid{grid-template-columns:repeat(auto-fill,minmax(210px,1fr));align-items:start}.renderer-masonry_board .external-card:nth-child(3n){transform:translateY(1.2rem)}.renderer-listing_grid .external-card{border-color:#d6ae4b}.renderer-map .external-card,.renderer-layer_inspector .external-card{border-radius:3px}.renderer-dense_tables .external-card,.renderer-demand_table .external-card{border-radius:0;display:grid;grid-template-columns:1fr auto;gap:.6rem}.renderer-provenance_panel .external-card,.renderer-closure_explainer .external-card{background:#121a15}.note{border-left:3px solid #d6ae4b;padding:.8rem 1rem;background:#252117}.machine{font:12px ui-monospace,monospace;color:#94a89a;word-break:break-all}@media(max-width:640px){main{padding:1rem}.renderer-masonry_board .external-card:nth-child(3n){transform:none}}</style><main data-closure-digest="${escapeHtml(closure.closure_digest)}"><p class="eyebrow">Aurekai / Bonfyre · addressed public ${options.grammar ? "lens" : "World"}</p><header class="hero"><p><a href="${escapeHtml(worldPath)}">← ${escapeHtml(closure.world.title)}</a> · <a href="${base || "/"}">external estate</a></p><h1>${escapeHtml(closure.world.title)}${options.grammar ? ` <em>· ${escapeHtml(normalize(options.grammar))}</em>` : ""}</h1><p>${closure.occurrence_count.toLocaleString()} observed candidate occurrences are available to this World. The page selects bounded interaction lenses from that same reservoir; it does not create a provider, institutional fact, authority, payment, or external effect.</p><p class="machine">closure ${escapeHtml(closure.closure_digest)} · <a href="${escapeHtml(machinePath)}">machine closure</a><br>${escapeHtml(forwarding)}</p></header><nav class="lens-nav" aria-label="World lenses">${closure.available_lenses.map((lens) => `<a ${options.grammar === lens.grammar ? "aria-current=\"page\"" : ""} href="${escapeHtml(`${worldPath}/${encodeURIComponent(lens.grammar)}`)}">${escapeHtml(lens.grammar)} <small>${lens.candidate_count}</small></a>`).join("")}</nav>${formationPanel}${empty}${lensHtml}<aside class="note"><strong>Public candidate boundary.</strong> Each item is a retained source-derived candidate. AddressPlane and exact owners must still resolve actor, information policy, capability, evidence, rights, and any effect.</aside></main></html>`;
}

export function renderExternalOccurrenceHtml(repoRoot, id, options = {}) {
  const record = getExternalOccurrence(repoRoot, id, options);
  if (!record) return null;
  const base = options.basePath || "";
  const p = record.projection || {};
  const encoded = encodeURIComponent(record.id);
  const list = (values) => `<ul>${values.map((value) => `<li>${escapeHtml(value)}</li>`).join("")}</ul>`;
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escapeHtml(record.observed_name)} · Aurekai</title><style>body{margin:0;background:#101513;color:#e9eee8;font:16px/1.5 system-ui,sans-serif}main{max-width:860px;margin:auto;padding:2rem}a{color:#a7e3bd}.eyebrow{color:#94a89a;font-size:.78rem;text-transform:uppercase;letter-spacing:.08em}.panel{border:1px solid #334239;border-radius:12px;padding:1rem;margin:1rem 0;background:#172019}code{white-space:pre-wrap;word-break:break-word}button{background:#e9eee8;color:#102015;border:0;border-radius:7px;padding:.6rem} </style><main><p><a href="${base || "/"}">← external estate</a></p><p class="eyebrow">${escapeHtml(record.source_kind)} · ${escapeHtml(record.source_ref)} · ${escapeHtml(record.truth_level)}</p><h1>${escapeHtml(record.observed_name)}</h1><p>${escapeHtml(record.section)}</p><p><a rel="noreferrer" href="${escapeHtml(record.primary_url)}">Open supplied source</a> · <a href="${base}/v1/occurrences/${encoded}">machine JSON</a></p><section class="panel"><h2>Candidate composition</h2><h3>Direct domains</h3>${list(p.direct_domains || [])}<h3>Conditional reuse domains</h3>${list(p.conditional_reuse_domains || [])}<h3>Candidate Worlds</h3>${list(p.candidate_worlds || [])}<h3>Candidate affordances</h3>${list(p.candidate_affordances || [])}</section><section class="panel"><h2>Manual transport check</h2><p>This sends one HTTP(S) HEAD to the supplied URL, follows no redirect, stores no body, and does not admit the source as a provider.</p><form method="post" action="${base}/v1/occurrences/${encoded}/probe"><button>Probe transport explicitly</button></form></section><section class="panel"><h2>Exact supplied source line</h2><code>${escapeHtml(record.raw_source_text)}</code></section><section class="panel"><h2>What this cannot establish</h2>${list(record.forbidden_inferences || [])}</section></main></html>`;
}

/** Human carrier for one explicitly requested no-body transport observation. */
export function renderExternalProbeHtml(record, result, options = {}) {
  const base = options.basePath || "";
  const id = encodeURIComponent(record.id);
  const status = result.status === "responded" ? `responded with HTTP ${result.http_status}` : result.status;
  return `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Transport check · ${escapeHtml(record.observed_name)} · Aurekai</title><style>body{margin:0;background:#101513;color:#e9eee8;font:16px/1.5 system-ui,sans-serif}main{max-width:760px;margin:auto;padding:2rem}a{color:#a7e3bd}.panel{border:1px solid #334239;border-radius:12px;padding:1rem;background:#172019}.kicker{font:12px ui-monospace,monospace;color:#94a89a;text-transform:uppercase}.note{border-left:3px solid #d6ae4b;padding:.8rem 1rem;background:#252117}</style><main><p><a href="${base}/occurrences/${id}">← ${escapeHtml(record.observed_name)}</a></p><p class="kicker">Explicit transport observation · no body retained</p><h1>${escapeHtml(status)}</h1><section class="panel"><p><strong>Requested URL:</strong> ${escapeHtml(result.requested_url || record.primary_url)}</p><p><strong>Source:</strong> ${escapeHtml(record.source_ref)} · ${escapeHtml(record.source_kind)}</p>${result.location ? `<p><strong>Redirect location reported:</strong> ${escapeHtml(result.location)}</p>` : ""}</section><aside class="note"><strong>Boundary.</strong> This is one manually requested HTTP(S) HEAD check. It does not follow redirects, retain a body, create a provider relationship, admit evidence, establish authority, authorize payment, or perform an external effect.</aside></main></html>`;
}

export function externalEstateManifest(repoRoot, options = {}) {
  return loadExternalEstate(repoRoot, options).manifest;
}
