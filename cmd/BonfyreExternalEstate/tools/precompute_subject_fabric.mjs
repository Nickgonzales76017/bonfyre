#!/usr/bin/env bun
// Offline dense closure for WebAddress; exact owners remain authoritative.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, mkdtempSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { canon, digest, parsed, runJson, tableExists } from "./subject_fabric_common.mjs";

const sceneFields = ["scene_key", "subject_address", "world", "site_id", "actor_scope", "source_digest", "focus", "lenses",
  "world_material", "fragments", "slots", "bindings", "stages", "renderers", "patterns", "presentation", "operations",
  "interaction", "runtime_queries", "dependencies"];
const sceneListFields = new Set(["lenses", "fragments", "slots", "bindings", "stages", "renderers", "patterns", "presentation", "dependencies"]);
const sorted = iterable => [...iterable].sort();
const string = value => value == null ? "" : String(value);

export function subjectContexts(cms, explicit = []) {
  const out = new Map();
  const put = (subject, site, world, origin) => {
    if (!subject) return;
    const context = out.get(subject) ?? { sites: new Set(), worlds: new Set(), origins: new Set() };
    if (site) context.sites.add(string(site));
    if (world) context.worlds.add(string(world));
    context.origins.add(origin); out.set(subject, context);
  };
  for (const subject of explicit) put(subject, "", "", "explicit");
  const queries = [
    ["site_presentation", "SELECT subject_address AS subject, COALESCE(site_id,'') AS site, '' AS world FROM site_presentation WHERE status='published' AND COALESCE(subject_address,'')<>''"],
    ["world", "SELECT subject,'' AS site,COALESCE(world,'') AS world FROM world WHERE status='published' AND COALESCE(subject,'')<>''"],
    ["world_fragment", "SELECT subject_address AS subject,'' AS site,COALESCE(world,'') AS world FROM world_fragment WHERE status='published' AND COALESCE(subject_address,'')<>''"],
  ];
  for (const [table, sql] of queries) if (tableExists(cms, table)) for (const row of cms.query(sql).all())
    put(string(row.subject), row.site, row.world, table);
  return out;
}

export function candidateOccurrences(closure) {
  const rows = [];
  for (const demand of closure.pack_demands ?? []) {
    if (!demand || typeof demand !== "object") continue;
    for (const group of demand.candidate_groups ?? []) {
      if (!group || typeof group !== "object") continue;
      for (const candidate of group.candidates ?? []) if (candidate?.identity)
        rows.push({ identity: candidate.identity, basis: "pack_demand", demand_id: demand.identity ?? "", source: group.source ?? "" });
    }
  }
  for (const world of closure.world_demands ?? []) {
    if (!world || typeof world !== "object") continue;
    for (const dependency of world.dependencies ?? []) {
      if (!dependency || typeof dependency !== "object") continue;
      for (const candidate of dependency.candidates ?? []) if (candidate?.identity)
        rows.push({ identity: candidate.identity, basis: "world_demand", demand_id: world.identity ?? "", source: dependency.dependency ?? "" });
    }
  }
  return rows;
}

function closureFor(binary, demandDb, conditionDb, subject, scope, limit) {
  const argv = [binary, "resolve", "--db", demandDb, "--subject", subject, "--scope", scope, "--limit", String(limit)];
  if (conditionDb) argv.push("--condition-db", conditionDb);
  const [value, error] = runJson(argv);
  return value && string(value.schema).startsWith("bonfyre-scoped-demand-closure.v")
    ? [value, null] : [null, error || "ScopedDemand returned wrong schema"];
}
function persistedScenes(db, subject, scope) {
  if (!db || !tableExists(db, "surface_scene")) return [];
  const cols = new Set(db.query("PRAGMA table_info(surface_scene)").all().map(row => row.name));
  if (!sceneFields.every(field => cols.has(field))) return [];
  const rows = db.query(`SELECT ${sceneFields.join(",")} FROM surface_scene WHERE status='published' AND subject_address=? AND actor_scope=? ORDER BY scene_key`).all(subject, scope);
  return rows.map(row => {
    for (const field of sceneFields.slice(6)) row[field] = parsed(row[field], sceneListFields.has(field) ? [] : {});
    return row;
  });
}
function compiledScene(binary, cmsDb, subject, scope, context) {
  if (!binary) return [null, null];
  const argv = [binary, "compile", "--db", cmsDb, "--subject", subject, "--actor-scope", scope];
  if (context.sites.size) argv.push("--site", sorted(context.sites)[0]);
  if (context.worlds.size) argv.push("--world", sorted(context.worlds)[0]);
  const [value, error] = runJson(argv);
  return value?.schema === "bonfyre-surface-scene.v1" ? [value, null] : [null, error];
}
function splitActions(value) {
  if (Array.isArray(value)) return value.map(string).filter(Boolean);
  if (typeof value === "string") return value.split(",").map(part => part.trim()).filter(Boolean);
  return [];
}

export function sceneProjection(scene) {
  const lenses = new Set(), actions = new Set(), renderers = new Set(), applications = new Set(), queries = [];
  const dependencies = new Set(parsed(scene.dependencies, []).map(string).filter(Boolean));
  for (const lens of parsed(scene.lenses, [])) if (lens) lenses.add(string(lens));
  const operations = parsed(scene.operations, {});
  if (operations && !Array.isArray(operations)) for (const aperture of operations.apertures ?? []) if (aperture) actions.add(string(aperture));
  for (const pattern of parsed(scene.patterns, [])) {
    if (!pattern || typeof pattern !== "object") continue;
    queries.push({ source: "surface_pattern", id: string(pattern.pattern), owner: string(pattern.reads_organ),
      reads_type: string(pattern.reads_type), command: string(pattern.organ_command),
      effect_class: string(pattern.effect_class || "QUERY"), time_field: string(pattern.time_field), time_mode: string(pattern.time_mode) });
  }
  for (const binding of parsed(scene.bindings, [])) {
    if (!binding || typeof binding !== "object") continue;
    const kind = string(binding.application_kind);
    if (kind) applications.add(kind);
    for (const action of splitActions(binding.action_aperture)) actions.add(action);
    queries.push({ source: "application_binding", id: string(binding.binding_id), application_kind: kind,
      material_query: parsed(binding.material_query, {}), source_collection: string(binding.source_collection) });
  }
  for (const stage of parsed(scene.stages, [])) {
    if (!stage || typeof stage !== "object") continue;
    const kind = string(stage.application_kind);
    if (kind) applications.add(kind);
    for (const action of splitActions(stage.actions)) actions.add(action);
    if (stage.renderer) renderers.add(string(stage.renderer));
    parsed(stage.material_queries, []).forEach((query, index) => queries.push({ source: "experience_stage",
      id: `${stage.stage_id || ""}:${index}`, application_kind: kind, material_query: query }));
  }
  for (const fragment of parsed(scene.fragments, [])) {
    if (!fragment || typeof fragment !== "object") continue;
    if (fragment.surface_grammar) lenses.add(string(fragment.surface_grammar));
    if (fragment.renderer_family) renderers.add(string(fragment.renderer_family));
    for (const action of splitActions(fragment.action_aperture)) actions.add(action);
  }
  for (const renderer of parsed(scene.renderers, [])) if (renderer?.renderer) renderers.add(string(renderer.renderer));
  return { scene_key: string(scene.scene_key), site_id: string(scene.site_id), world: string(scene.world),
    source_digest: string(scene.source_digest), lenses: sorted(lenses), applications: sorted(applications), renderers: sorted(renderers),
    actions: sorted(actions), queries, dependencies: sorted(dependencies), interaction: parsed(scene.interaction, {}),
    presentation: parsed(scene.presentation, []) };
}

function frontierMatches(db, occurrence) {
  if (!tableExists(db, "channel") || !tableExists(db, "root")) return [];
  const identity = string(occurrence.identity), source = string(occurrence.source);
  const keys = new Set();
  if (identity) {
    keys.add(identity);
    if (identity.startsWith("external:")) keys.add(identity.slice(9));
    if (identity.toLowerCase().startsWith("ext-")) keys.add(`external:${identity.toLowerCase()}`);
  }
  if (source) keys.add(source);
  if (!keys.size) return [];
  const values = [...keys], marks = values.map(() => "?").join(",");
  const sql = `SELECT DISTINCT c.channel_id,c.root_id,r.host,r.name AS root_name,r.identity AS root_identity,`
    + `c.address,c.protocol,c.state,COALESCE(NULLIF(cc.material,''),c.material) AS material,c.rights,c.credential,`
    + `COALESCE(NULLIF(cc.census_source,''),c.census_source) AS census_source,c.virtual,c.last_contributed,c.contributions,c.first_seen `
    + `FROM channel c JOIN root r ON r.root_id=c.root_id LEFT JOIN channel_census cc ON cc.channel_id=c.channel_id `
    + `WHERE c.identity IN (${marks}) OR c.census_source IN (${marks}) OR cc.census_source IN (${marks}) ORDER BY c.root_id,c.channel_id`;
  try { return db.query(sql).all(...values, ...values, ...values); } catch { return []; }
}
function closureDependencies(db, subject, scope) {
  if (!db || !tableExists(db, "closure_dependency")) return [];
  try { return db.query("SELECT DISTINCT dependency_ref,basis FROM closure_dependency WHERE subject=? AND scope=? ORDER BY dependency_ref,basis")
    .all(subject, scope).map(row => ({ ref: string(row.dependency_ref), basis: string(row.basis) })); }
  catch { return []; }
}
function existingDigests(path) {
  if (!path || !existsSync(path)) return new Map();
  let db;
  try {
    db = new Database(path, { readonly: true });
    return tableExists(db, "subject_bundle") ? new Map(db.query("SELECT subject,scope,bundle_digest FROM subject_bundle")
      .all().map(row => [canon([row.subject, row.scope]), row.bundle_digest])) : new Map();
  } catch { return new Map(); }
  finally { db?.close(); }
}

export function buildBundle(subject, scope, context, closure, scenes, frontier, scoped) {
  const candidates = candidateOccurrences(closure), candidateRows = [], unmapped = [], mapped = new Set();
  const roots = new Map(), readers = new Map();
  for (const occurrence of candidates) {
    const matches = frontierMatches(frontier, occurrence);
    const occurrenceKey = canon([string(occurrence.identity), string(occurrence.basis), string(occurrence.demand_id), string(occurrence.source)]);
    if (!matches.length) {
      unmapped.push(string(occurrence.identity));
      candidateRows.push({ ...occurrence, root_id: "", channel_id: "" });
      continue;
    }
    mapped.add(occurrenceKey);
    for (const match of matches) {
      candidateRows.push({ ...occurrence, root_id: string(match.root_id), channel_id: string(match.channel_id) });
      const rootId = string(match.root_id);
      const root = roots.get(rootId) ?? { root_id: rootId, host: string(match.host), name: string(match.root_name),
        identity: string(match.root_identity), channels: new Set(), protocols: new Set(), states: new Set(), materials: new Set(),
        rights: new Set(), credentials: new Set() };
      root.channels.add(string(match.channel_id));
      for (const [field, target] of [["protocol", "protocols"], ["state", "states"], ["material", "materials"],
        ["rights", "rights"], ["credential", "credentials"]]) if (match[field]) root[target].add(string(match[field]));
      roots.set(rootId, root);
      if (match.census_source) {
        const reader = { census_source: string(match.census_source), root_id: rootId, channel_id: string(match.channel_id),
          material: string(match.material), protocol: string(match.protocol), state: string(match.state),
          last_contributed: string(match.last_contributed), contributions: Number(match.contributions ?? 0),
          first_seen: string(match.first_seen) };
        readers.set(canon([reader.census_source, reader.channel_id, reader.material]), reader);
      }
    }
  }
  const sceneRows = scenes.map(sceneProjection);
  const actions = sorted(new Set(sceneRows.flatMap(scene => scene.actions)));
  const dependencies = closureDependencies(scoped, subject, scope);
  dependencies.push(...sceneRows.flatMap(scene => scene.dependencies.map(ref => ({ ref, basis: "surface_scene" }))));
  const uniqueDeps = [...new Map(dependencies.map(row => [canon([row.ref, row.basis]), row])).values()];
  const demands = {
    active_packs: closure.active_packs ?? [],
    pack_demands: (closure.pack_demands ?? []).filter(row => row && typeof row === "object").map(row =>
      Object.fromEntries(["identity", "pack", "demand_kind", "requirement", "regime", "state"].map(key => [key, row[key] ?? null]))),
    world_demands: (closure.world_demands ?? []).filter(row => row && typeof row === "object").map(row => ({
      identity: row.identity ?? null, state: row.state ?? null,
      dependencies: (row.dependencies ?? []).filter(d => d && typeof d === "object").map(d => d.dependency ?? null) })),
    counts: closure.counts ?? {},
  };
  const compactRoots = [...roots.values()].sort((a, b) => a.root_id.localeCompare(b.root_id)).map(root => ({
    root_id: root.root_id, host: root.host, name: root.name, identity: root.identity, channels: root.channels.size,
    protocols: sorted(root.protocols), states: sorted(root.states), materials: sorted(root.materials),
    rights: sorted(root.rights), credentials: sorted(root.credentials) }));
  const queries = sceneRows.flatMap(scene => scene.queries);
  const counts = { candidate_occurrences: candidates.length, candidate_edges: candidateRows.length,
    mapped_candidate_occurrences: mapped.size, unmapped_candidate_occurrences: candidates.length - mapped.size,
    source_roots: roots.size, channels: new Set(candidateRows.map(row => row.channel_id).filter(Boolean)).size,
    readers: readers.size, protocols: new Set(compactRoots.flatMap(root => root.protocols)).size, scenes: sceneRows.length,
    lenses: new Set(sceneRows.flatMap(scene => scene.lenses)).size, applications: new Set(sceneRows.flatMap(scene => scene.applications)).size,
    renderers: new Set(sceneRows.flatMap(scene => scene.renderers)).size, material_queries: queries.length, actions: actions.length,
    dependencies: uniqueDeps.length };
  const closureDigest = digest(closure), sceneDigest = digest(sceneRows.map(scene => scene.source_digest));
  const core = { schema: "bonfyre.subject-fabric.v1", subject, scope, origins: sorted(context.origins), sites: sorted(context.sites),
    worlds: sorted(context.worlds), closure_digest: closureDigest, scene_digest: sceneDigest, counts, demands, source_roots: compactRoots,
    candidate_identities: sorted(new Set(candidates.map(row => string(row.identity)).filter(Boolean))), scene_models: sceneRows,
    actions, dependencies: uniqueDeps, unmapped_candidate_identities: sorted(new Set(unmapped)),
    truth: "precomputed_subject_closure_exact_owners_remain_authoritative",
    cannot_infer: ["candidate membership -> observation, freshness, rights, authority or contribution",
      "precomputed query plan -> request-time owner answer", "action aperture -> authority or completed effect",
      "scene membership -> visible rendering", "cached subject closure -> unchanged external reality"] };
  return { core, digest: digest(core), closure_digest: closureDigest, scene_digest: sceneDigest,
    candidates: candidateRows, roots: compactRoots,
    readers: [...readers.values()].sort((a, b) => canon([a.material, a.root_id, a.channel_id, a.census_source])
      .localeCompare(canon([b.material, b.root_id, b.channel_id, b.census_source]))),
    scenes: sceneRows, actions, dependencies: uniqueDeps };
}

const schema = `PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE subject_bundle(subject TEXT NOT NULL,scope TEXT NOT NULL,bundle_digest TEXT NOT NULL,closure_digest TEXT NOT NULL,scene_digest TEXT NOT NULL DEFAULT '',compiled_at TEXT NOT NULL,counts_json TEXT NOT NULL,payload_json TEXT NOT NULL,PRIMARY KEY(subject,scope));
CREATE TABLE subject_candidate(subject TEXT NOT NULL,scope TEXT NOT NULL,identity TEXT NOT NULL,root_id TEXT NOT NULL DEFAULT '',channel_id TEXT NOT NULL DEFAULT '',basis TEXT NOT NULL,demand_id TEXT NOT NULL DEFAULT '',source TEXT NOT NULL DEFAULT '',PRIMARY KEY(subject,scope,identity,root_id,channel_id,basis,demand_id,source));
CREATE TABLE subject_root(subject TEXT NOT NULL,scope TEXT NOT NULL,root_id TEXT NOT NULL,host TEXT NOT NULL DEFAULT '',name TEXT NOT NULL DEFAULT '',identity TEXT NOT NULL DEFAULT '',channels INTEGER NOT NULL,protocols_json TEXT NOT NULL,states_json TEXT NOT NULL,materials_json TEXT NOT NULL,rights_json TEXT NOT NULL,credentials_json TEXT NOT NULL,PRIMARY KEY(subject,scope,root_id));
CREATE TABLE subject_reader(subject TEXT NOT NULL,scope TEXT NOT NULL,census_source TEXT NOT NULL,root_id TEXT NOT NULL,channel_id TEXT NOT NULL,material TEXT NOT NULL DEFAULT '',protocol TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT '',last_contributed TEXT NOT NULL DEFAULT '',contributions INTEGER NOT NULL DEFAULT 0,first_seen TEXT NOT NULL DEFAULT '',PRIMARY KEY(subject,scope,census_source,channel_id,material));
CREATE TABLE subject_scene(subject TEXT NOT NULL,scope TEXT NOT NULL,scene_key TEXT NOT NULL,site_id TEXT NOT NULL DEFAULT '',world TEXT NOT NULL DEFAULT '',source_digest TEXT NOT NULL DEFAULT '',lenses_json TEXT NOT NULL,applications_json TEXT NOT NULL,renderers_json TEXT NOT NULL,actions_json TEXT NOT NULL,interaction_json TEXT NOT NULL,presentation_json TEXT NOT NULL,PRIMARY KEY(subject,scope,scene_key));
CREATE TABLE subject_query(subject TEXT NOT NULL,scope TEXT NOT NULL,scene_key TEXT NOT NULL,ordinal INTEGER NOT NULL,query_json TEXT NOT NULL,PRIMARY KEY(subject,scope,scene_key,ordinal));
CREATE TABLE subject_action(subject TEXT NOT NULL,scope TEXT NOT NULL,action TEXT NOT NULL,PRIMARY KEY(subject,scope,action));
CREATE TABLE subject_dependency(subject TEXT NOT NULL,scope TEXT NOT NULL,dependency_ref TEXT NOT NULL,basis TEXT NOT NULL,PRIMARY KEY(subject,scope,dependency_ref,basis));
CREATE INDEX subject_candidate_root ON subject_candidate(root_id,subject,scope);
CREATE INDEX subject_candidate_channel ON subject_candidate(channel_id,subject,scope);
CREATE INDEX subject_reader_material ON subject_reader(subject,scope,material,state);
CREATE INDEX subject_dependency_reverse ON subject_dependency(dependency_ref,subject,scope);
CREATE INDEX subject_action_lookup ON subject_action(action,subject,scope);`;

function writeDb(path, bundles, old) {
  const directory = dirname(resolve(path)); mkdirSync(directory, { recursive: true });
  const temporary = mkdtempSync(join(directory, ".subject-fabric-"));
  const dbPath = join(temporary, "index.db");
  let db, changed = 0, unchanged = 0;
  const totals = {};
  try {
    db = new Database(dbPath);
    db.exec(schema);
    const compiledAt = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    const put = (table, count) => db.query(`INSERT OR IGNORE INTO ${table} VALUES(${Array(count).fill("?").join(",")})`);
    const bundleInsert = put("subject_bundle", 8), candidateInsert = put("subject_candidate", 8), rootInsert = put("subject_root", 12),
      readerInsert = put("subject_reader", 11), sceneInsert = put("subject_scene", 12), queryInsert = put("subject_query", 5),
      actionInsert = put("subject_action", 3), dependencyInsert = put("subject_dependency", 4);
    const transaction = db.transaction(() => {
      for (const [[subject, scope], bundle] of [...bundles.values()].sort((a, b) => canon(a[0]).localeCompare(canon(b[0])))) {
        if (old.get(canon([subject, scope])) === bundle.digest) unchanged++; else changed++;
        for (const [key, value] of Object.entries(bundle.core.counts)) totals[key] = (totals[key] ?? 0) + Number(value);
        bundleInsert.run(subject, scope, bundle.digest, bundle.closure_digest, bundle.scene_digest, compiledAt,
          canon(bundle.core.counts), canon(bundle.core));
        for (const row of bundle.candidates) candidateInsert.run(subject, scope, string(row.identity), row.root_id,
          row.channel_id, string(row.basis), string(row.demand_id), string(row.source));
        for (const root of bundle.roots) rootInsert.run(subject, scope, root.root_id, root.host, root.name, root.identity,
          root.channels, canon(root.protocols), canon(root.states), canon(root.materials), canon(root.rights), canon(root.credentials));
        for (const reader of bundle.readers) readerInsert.run(subject, scope, reader.census_source, reader.root_id, reader.channel_id,
          reader.material, reader.protocol, reader.state, reader.last_contributed, reader.contributions, reader.first_seen);
        let ordinal = 0;
        for (const scene of bundle.scenes) {
          sceneInsert.run(subject, scope, scene.scene_key, scene.site_id, scene.world, scene.source_digest,
            canon(scene.lenses), canon(scene.applications), canon(scene.renderers), canon(scene.actions), canon(scene.interaction), canon(scene.presentation));
          for (const query of scene.queries) queryInsert.run(subject, scope, scene.scene_key, ordinal++, canon(query));
        }
        for (const action of bundle.actions) actionInsert.run(subject, scope, action);
        for (const dep of bundle.dependencies) dependencyInsert.run(subject, scope, dep.ref, dep.basis);
      }
      db.query("INSERT INTO meta VALUES('schema','bonfyre.subject-fabric-index.v1')").run();
      db.query("INSERT INTO meta VALUES('compiled_at',?)").run(compiledAt);
      db.query("INSERT INTO meta VALUES('subjects',?)").run(String(bundles.size));
    });
    transaction();
    if (db.query("PRAGMA integrity_check").get()?.integrity_check !== "ok") throw new Error("subject-fabric integrity check failed");
    db.close(); db = null;
    renameSync(dbPath, path);
    return [changed, unchanged, totals];
  } finally { db?.close(); rmSync(temporary, { recursive: true, force: true }); }
}

export function repairActionsFromScenes(path) {
  const db = new Database(path);
  try {
    if (!tableExists(db, "subject_scene") || !tableExists(db, "subject_action"))
      throw new Error("subject fabric lacks subject_scene or subject_action");
    const scenes = db.query("SELECT subject,scope,actions_json FROM subject_scene ORDER BY subject,scope,scene_key").all();
    const put = db.query("INSERT OR IGNORE INTO subject_action(subject,scope,action) VALUES(?,?,?)");
    db.transaction(() => {
      db.exec("DELETE FROM subject_action");
      for (const row of scenes) {
        const values = parsed(row.actions_json, []);
        if (!Array.isArray(values)) throw new Error("subject_scene.actions_json is not an array");
        for (const action of values.map(string).filter(Boolean)) put.run(row.subject, row.scope, action);
      }
    })();
    return { schema: "bonfyre.subject-action-repair.v1", scenes: scenes.length,
      actions: db.query("SELECT COUNT(*) AS n FROM subject_action").get().n,
      truth: "actions_rebuilt_only_from_compiled_subject_scene_apertures" };
  } finally { db.close(); }
}

function options(argv) {
  const args = { subject: [], scope: "public", candidateLimit: 4096, repairActions: "" };
  const keys = { "--cms-db": "cmsDb", "--demand-db": "demandDb", "--frontier-db": "frontierDb",
    "--scoped-demand-bin": "scopedDemandBin", "--surface-scene-bin": "surfaceSceneBin", "--scene-db": "sceneDb",
    "--scoped-frontier-db": "scopedFrontierDb", "--condition-db": "conditionDb", "--scope": "scope",
    "--candidate-limit": "candidateLimit", "--out": "out", "--manifest": "manifest",
    "--repair-actions-from-scenes": "repairActions" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--subject") { if (i + 1 >= argv.length) throw new Error("missing --subject value"); args.subject.push(argv[++i]); continue; }
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  if (args.repairActions) return args;
  for (const key of ["cmsDb", "demandDb", "frontierDb", "scopedDemandBin", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.candidateLimit = Math.max(1, Math.min(4096, Number(args.candidateLimit)));
  if (!Number.isInteger(args.candidateLimit)) throw new Error("invalid --candidate-limit");
  return args;
}

export function precompute(args) {
  const cms = new Database(args.cmsDb, { readonly: true }), frontier = new Database(args.frontierDb, { readonly: true });
  const scenePath = args.sceneDb || args.cmsDb;
  const sceneDb = existsSync(scenePath) ? new Database(scenePath, { readonly: true }) : null;
  const scoped = args.scopedFrontierDb && existsSync(args.scopedFrontierDb) ? new Database(args.scopedFrontierDb, { readonly: true }) : null;
  const bundles = new Map(), failures = [];
  let contexts, compiledScenes = 0, persistedSceneCount = 0;
  try {
    contexts = subjectContexts(cms, args.subject);
    for (const [subject, context] of [...contexts].sort(([a], [b]) => a.localeCompare(b))) {
      const [closure, closureError] = closureFor(args.scopedDemandBin, args.demandDb, args.conditionDb,
        subject, args.scope, args.candidateLimit);
      if (!closure) { failures.push({ subject, stage: "scoped_demand", error: closureError }); continue; }
      let scenes = persistedScenes(sceneDb, subject, args.scope);
      if (scenes.length) persistedSceneCount += scenes.length;
      else {
        const [scene, sceneError] = compiledScene(args.surfaceSceneBin, args.cmsDb, subject, args.scope, context);
        if (scene) { scenes = [scene]; compiledScenes++; }
        else if (sceneError) failures.push({ subject, stage: "surface_scene", error: sceneError });
      }
      bundles.set(canon([subject, args.scope]), [[subject, args.scope], buildBundle(subject, args.scope, context, closure, scenes, frontier, scoped)]);
    }
  } finally { cms.close(); frontier.close(); sceneDb?.close(); scoped?.close(); }
  const [changed, unchanged, totals] = writeDb(args.out, bundles, existingDigests(args.out));
  const summary = { schema: "bonfyre.subject-fabric-precompute.v1", subjects_discovered: contexts.size,
    subjects_compiled: bundles.size, subjects_failed: new Set(failures.map(row => row.subject)).size,
    bundles_changed: changed, bundles_unchanged: unchanged, persisted_scenes: persistedSceneCount,
    scenes_compiled_on_demand: compiledScenes, totals, output: args.out,
    truth: "precomputed_subject_closure_exact_owners_remain_authoritative", failures: failures.slice(0, 100) };
  if (args.manifest) writeFileSync(args.manifest, JSON.stringify(summary, null, 2));
  return summary;
}

if (import.meta.main) {
  try { const args = options(process.argv.slice(2));
    if (args.repairActions) console.log(JSON.stringify(repairActionsFromScenes(args.repairActions)));
    else { const summary = precompute(args); console.log(JSON.stringify(summary));
      if (!summary.subjects_compiled) process.exitCode = 3; } }
  catch (error) { console.error(error); process.exitCode = 1; }
}
