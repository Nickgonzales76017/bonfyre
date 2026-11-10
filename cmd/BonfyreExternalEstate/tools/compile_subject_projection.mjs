#!/usr/bin/env bun
// Request-ready WebAddress route cut over already-admitted normalized material.
import { Database } from "bun:sqlite";
import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { canon, digest, tableExists } from "./subject_fabric_common.mjs";
import { compilerDigest, verify as verifyProjection, inputSnapshot, outputSnapshot, openVerifiedCmsReader } from "./verify_subject_projection.mjs";
import { parseItems, cmsCollections, sceneCollectionRoutes, subjectTerritories, routeLens, routeMaterials,
  indexCollections, candidateCollections, collectionMaterialAcceptance, normalizedItem, compareItems } from "./subject_projection_rules.mjs";

const materialFields = ["source", "key", "material", "family", "title", "byline", "publisher", "href", "group_name",
  "image", "asset", "poster", "body", "ident", "about", "rights", "rights_href", "origin", "origin_href", "people",
  "where_text", "transcript_href", "chapters_href", "talk_href", "at", "duration_ms", "placed", "lon", "lat"];
const schema = `
DROP TABLE IF EXISTS subject_material_body;
DROP TABLE IF EXISTS subject_material_member;
DROP TABLE IF EXISTS subject_route;
DROP TABLE IF EXISTS subject_route_phase;
DROP TABLE IF EXISTS subject_route_reader;
DROP TABLE IF EXISTS subject_route_region;
DROP TABLE IF EXISTS subject_route_item;
DROP TABLE IF EXISTS subject_projection_build;
CREATE TABLE subject_projection_build(build_id INTEGER PRIMARY KEY CHECK(build_id=1),compiled_at TEXT NOT NULL,compiler_sha256 TEXT NOT NULL,input_digest TEXT NOT NULL,projection_digest TEXT NOT NULL,manifest_json TEXT NOT NULL);
CREATE TABLE subject_material_body(source TEXT NOT NULL,item_key TEXT NOT NULL,material TEXT NOT NULL,family TEXT NOT NULL DEFAULT '',title TEXT NOT NULL DEFAULT '',byline TEXT NOT NULL DEFAULT '',publisher TEXT NOT NULL DEFAULT '',href TEXT NOT NULL DEFAULT '',group_name TEXT NOT NULL DEFAULT '',image TEXT NOT NULL DEFAULT '',asset TEXT NOT NULL DEFAULT '',poster TEXT NOT NULL DEFAULT '',body TEXT NOT NULL DEFAULT '',ident TEXT NOT NULL DEFAULT '',about TEXT NOT NULL DEFAULT '',rights TEXT NOT NULL DEFAULT '',rights_href TEXT NOT NULL DEFAULT '',origin TEXT NOT NULL DEFAULT '',origin_href TEXT NOT NULL DEFAULT '',people TEXT NOT NULL DEFAULT '',where_text TEXT NOT NULL DEFAULT '',transcript_href TEXT NOT NULL DEFAULT '',chapters_href TEXT NOT NULL DEFAULT '',talk_href TEXT NOT NULL DEFAULT '',at INTEGER NOT NULL DEFAULT 0,duration_ms INTEGER NOT NULL DEFAULT 0,placed INTEGER NOT NULL DEFAULT 0,lon REAL NOT NULL DEFAULT 0,lat REAL NOT NULL DEFAULT 0,PRIMARY KEY(source,item_key,material));
CREATE TABLE subject_material_member(subject TEXT NOT NULL,scope TEXT NOT NULL,source TEXT NOT NULL,item_key TEXT NOT NULL,material TEXT NOT NULL,PRIMARY KEY(subject,scope,source,item_key,material));
CREATE VIEW subject_material AS SELECT x.subject,x.scope,b.source,b.item_key,b.material,b.family,b.title,b.byline,b.publisher,b.href,b.group_name,b.image,b.asset,b.poster,b.body,b.ident,b.about,b.rights,b.rights_href,b.origin,b.origin_href,b.people,b.where_text,b.transcript_href,b.chapters_href,b.talk_href,b.at,b.duration_ms,b.placed,b.lon,b.lat FROM subject_material_member x JOIN subject_material_body b ON b.source=x.source AND b.item_key=x.item_key AND b.material=x.material;
CREATE TABLE subject_route(subject TEXT NOT NULL,scope TEXT NOT NULL,route TEXT NOT NULL,lens TEXT NOT NULL,renderer TEXT NOT NULL DEFAULT '',basis TEXT NOT NULL,territory TEXT NOT NULL DEFAULT '',title TEXT NOT NULL DEFAULT '',lede TEXT NOT NULL DEFAULT '',character TEXT NOT NULL DEFAULT '',cannot_infer TEXT NOT NULL DEFAULT '',sections_json TEXT NOT NULL DEFAULT '[]',collections_json TEXT NOT NULL,families_json TEXT NOT NULL,materials_json TEXT NOT NULL,regions_json TEXT NOT NULL,reader_count INTEGER NOT NULL,item_count INTEGER NOT NULL,route_digest TEXT NOT NULL,compiled_at TEXT NOT NULL,PRIMARY KEY(subject,scope,route));
CREATE TABLE subject_route_phase(subject TEXT NOT NULL,scope TEXT NOT NULL,route TEXT NOT NULL,territory TEXT NOT NULL,from_hour INTEGER NOT NULL,title TEXT NOT NULL DEFAULT '',lede TEXT NOT NULL DEFAULT '',sections_json TEXT NOT NULL DEFAULT '[]',PRIMARY KEY(subject,scope,route,from_hour));
CREATE TABLE subject_route_reader(subject TEXT NOT NULL,scope TEXT NOT NULL,route TEXT NOT NULL,source TEXT NOT NULL,root_id TEXT NOT NULL DEFAULT '',channel_id TEXT NOT NULL DEFAULT '',protocol TEXT NOT NULL DEFAULT '',basis TEXT NOT NULL,PRIMARY KEY(subject,scope,route,source,channel_id));
CREATE TABLE subject_route_region(subject TEXT NOT NULL,scope TEXT NOT NULL,route TEXT NOT NULL,ordinal INTEGER NOT NULL,role TEXT NOT NULL,material TEXT NOT NULL,topology TEXT NOT NULL DEFAULT '',language TEXT NOT NULL DEFAULT '',min_items INTEGER NOT NULL,max_items INTEGER NOT NULL,PRIMARY KEY(subject,scope,route,ordinal));
CREATE TABLE subject_route_item(subject TEXT NOT NULL,scope TEXT NOT NULL,route TEXT NOT NULL,ordinal INTEGER NOT NULL,source TEXT NOT NULL,item_key TEXT NOT NULL,material TEXT NOT NULL,PRIMARY KEY(subject,scope,route,source,item_key,material));
CREATE INDEX subject_material_member_kind ON subject_material_member(subject,scope,material);
CREATE INDEX subject_material_body_time ON subject_material_body(material,at);
CREATE INDEX subject_route_reader_lookup ON subject_route_reader(subject,scope,route,source);
CREATE INDEX subject_route_item_lookup ON subject_route_item(subject,scope,route,material,ordinal);`;

function options(argv) {
  const args = {};
  const keys = { "--fabric-db": "fabricDb", "--cms-db": "cmsDb", "--items": "items",
    "--page-composition": "pageComposition", "--surface-programs": "surfacePrograms", "--manifest": "manifest" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["fabricDb", "cmsDb", "items", "pageComposition", "surfacePrograms"]) if (!args[key]) throw new Error(`missing --${key}`);
  return args;
}
function atomicJson(path, value) {
  const temporary = join(dirname(resolve(path)), `.${basename(path)}.${randomBytes(6).toString("hex")}.tmp`);
  try { writeFileSync(temporary, JSON.stringify(value, null, 2) + "\n"); renameSync(temporary, path); }
  finally { if (existsSync(temporary)) unlinkSync(temporary); }
}
function groups(rows, key) {
  const map = new Map();
  for (const row of rows) { const id = key(row); const found = map.get(id) ?? []; found.push(row); map.set(id, found); }
  return map;
}
function integer(value) { return Number.isFinite(Number(value)) ? Math.trunc(Number(value)) : 0; }

export async function compile(args) {
  // The verification owner already knows the complete input closure, exact
  // normalized route rows, renderer declarations and compiler revision. Reuse
  // only when it independently reads back the whole committed projection; an
  // unmatched CMS/source/scene input or missing manifest reruns normally.
  if (existsSync(args.fabricDb)) {
    try {
      const verified = await verifyProjection(args);
      if (verified.verified) return {
        schema: "bonfyre.subject-projection-precompute.v4",
        owner: "SubjectProjection", owner_operations: 0, routes: verified.routes,
        route_items: verified.route_items, input_digest: verified.input_digest,
        projection_digest: verified.projection_digest, verification: verified,
        skipped: "verified_input_closure_and_committed_projection_unchanged",
        truth: "request_ready_projection_over_already_admitted_normalized_material",
      };
    } catch {
      // A dirty, absent or superseded projection is never cached as success.
      // The canonical compiler below owns its transactional replacement.
    }
  }
  const items = parseItems(readFileSync(args.items, "utf8"), args.items);
  const page = JSON.parse(readFileSync(args.pageComposition, "utf8")), surface = JSON.parse(readFileSync(args.surfacePrograms, "utf8"));
  const programs = page.programs ?? [], regionByRole = new Map((page.regions ?? []).map(region => [String(region.role), region]));
  const fabric = new Database(args.fabricDb), cms = openVerifiedCmsReader(args.cmsDb);
  try {
    if (!tableExists(fabric, "subject_bundle") || !tableExists(fabric, "subject_reader"))
      throw new Error("subject fabric must be compiled before subject projection");
    // Anchor the CMS read snapshot before changing derived Fabric.
    cms.exec("BEGIN");
    fabric.exec("BEGIN IMMEDIATE");
    const input = await inputSnapshot(fabric, cms, args), inputDigest = digest(input);
    const oldMaterial = fabric.query("SELECT type FROM sqlite_master WHERE name='subject_material'").get();
    if (oldMaterial?.type === "view") fabric.exec("DROP VIEW subject_material");
    else if (oldMaterial?.type === "table") fabric.exec("DROP TABLE subject_material");
    fabric.exec(schema);
    const collections = cmsCollections(cms), collectionIndex = indexCollections(collections), sceneRefs = sceneCollectionRoutes(fabric);
    const readerRows = fabric.query("SELECT subject,scope,census_source,root_id,channel_id,protocol,state "
      + "FROM subject_reader WHERE state IN ('admitted','active')").all().map(row => ({
        subject: String(row.subject), scope: String(row.scope), source: String(row.census_source), root_id: String(row.root_id),
        channel_id: String(row.channel_id), protocol: String(row.protocol), state: String(row.state) }));
    const readers = groups(readerRows, row => canon([row.subject, row.scope]));
    const normalizedBySource = new Map(), itemViewsBySource = new Map(), familiesBySource = new Map(), payloads = new Map();
    for (const item of items) {
      const families = familiesBySource.get(item.source) ?? new Set();
      families.add(String(item.family || ""));
      familiesBySource.set(item.source, families);
      const base = normalizedItem(item), place = item.placed ? normalizedItem(item, "place") : null;
      const views = itemViewsBySource.get(item.source) ?? [];
      views.push({ raw: item, base, place });
      itemViewsBySource.set(item.source, views);
      const normalized = place ? [base, place] : [base];
      for (const row of normalized) {
        const coordinate = canon([row.source, row.key, row.material]);
        if (payloads.has(coordinate)) {
          if (canon(payloads.get(coordinate)) !== canon(row)) throw new Error(`conflicting normalized item at ${coordinate}`);
          continue;
        }
        payloads.set(coordinate, row);
        const sourceRows = normalizedBySource.get(row.source) ?? []; sourceRows.push(row); normalizedBySource.set(row.source, sourceRows);
      }
    }
    const surfaceRenderer = new Map((surface.programs ?? []).map(program => [String(program.route ?? ""), String(program.renderer ?? "")]));
    const routePlans = programs.map(program => {
      const route = String(program.route || ""), territory = String(program.territory || "any");
      const acceptedMaterials = routeMaterials(program, regionByRole);
      const regionRows = [], regionOrdinals = [];
      for (const [ordinal, role] of (program.regions ?? []).entries()) {
        const region = regionByRole.get(String(role));
        if (!region) continue;
        regionOrdinals.push(ordinal);
        regionRows.push({ role: String(region.role || ""), material: String(region.material || ""),
          topology: String(region.topology || ""), language: String(region.language || ""),
          min_items: integer(region.min_items), max_items: integer(region.max_items) });
      }
      return { program, route, territory, lens: routeLens(route, surface), renderer: surfaceRenderer.get(route) ?? "",
        legacyFamilies: (program.families ?? []).map(String).filter(Boolean), acceptedMaterials,
        acceptedSet: new Set(acceptedMaterials), regionRows, regionOrdinals };
    });
    const compiledAt = new Date().toISOString().replace(/\.\d{3}Z$/, "Z"), totals = {}, basisCounts = {}, storedBodies = new Set();
    const bodyInsert = fabric.query(`INSERT INTO subject_material_body VALUES(${Array(29).fill("?").join(",")})`);
    const memberInsert = fabric.query("INSERT INTO subject_material_member VALUES(?,?,?,?,?)");
    const routeReaderInsert = fabric.query("INSERT OR IGNORE INTO subject_route_reader VALUES(?,?,?,?,?,?,?,?)");
    const regionInsert = fabric.query("INSERT INTO subject_route_region VALUES(?,?,?,?,?,?,?,?,?,?)");
    const routeInsert = fabric.query("INSERT INTO subject_route(subject,scope,route,lens,renderer,basis,territory,title,lede,character,"
      + "cannot_infer,sections_json,collections_json,families_json,materials_json,regions_json,reader_count,item_count,route_digest,compiled_at) "
      + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    const itemInsert = fabric.query("INSERT OR IGNORE INTO subject_route_item VALUES(?,?,?,?,?,?,?)");
    for (const bundle of fabric.query("SELECT subject,scope FROM subject_bundle ORDER BY subject,scope").all()) {
      const subject = String(bundle.subject), scope = String(bundle.scope), territories = subjectTerritories(cms, subject);
      const subjectReaders = readers.get(canon([subject, scope])) ?? [], allowed = new Set(subjectReaders.map(reader => reader.source));
      for (const source of [...allowed].sort()) for (const item of normalizedBySource.get(source) ?? []) {
        const coordinate = canon([item.source, item.key, item.material]);
        if (!storedBodies.has(coordinate)) {
          bodyInsert.run(...materialFields.map(field => item[field]));
          storedBodies.add(coordinate); totals.subject_material_body = (totals.subject_material_body ?? 0) + 1;
        }
        memberInsert.run(subject, scope, item.source, item.key, item.material);
        totals.subject_material = (totals.subject_material ?? 0) + 1;
      }
      for (const plan of routePlans) {
        const { program, route, territory, lens, renderer, legacyFamilies, acceptedMaterials, acceptedSet, regionRows } = plan;
        if (territory !== "any" && !territories.has(territory)) continue;
        if (!route) continue;
        const exact = candidateCollections(subject, lens, route, collectionIndex, sceneRefs, legacyFamilies, acceptedMaterials);
        const acceptsMaterial = collectionMaterialAcceptance(exact);
        let selectedSources, basis;
        if (exact.length) {
          selectedSources = new Set(exact.flatMap(c => c.sources).filter(source => allowed.has(source)));
          basis = exact.some(c => c.subject === "estate:aurekai") ? "estate_source_collection_exact" : "scene_source_collection_exact";
        } else {
          selectedSources = new Set([...allowed].filter(source => legacyFamilies
            .some(family => familiesBySource.get(source)?.has(family))));
          basis = "legacy_page_program_family_precompiled";
        }
        basisCounts[basis] = (basisCounts[basis] ?? 0) + 1;
        const selectedReaders = subjectReaders.filter(reader => selectedSources.has(reader.source));
        for (const reader of selectedReaders) routeReaderInsert.run(subject, scope, route, reader.source, reader.root_id,
          reader.channel_id, reader.protocol, basis);
        for (const [index, row] of regionRows.entries()) regionInsert.run(subject, scope, route,
          plan.regionOrdinals[index], row.role, row.material, row.topology, row.language, row.min_items, row.max_items);
        const routeItems = [];
        for (const source of [...selectedSources].sort()) for (const { raw, base, place } of itemViewsBySource.get(source) ?? []) {
          const material = raw._material;
          if (!acceptedSet.has("mix") && !acceptedSet.has(material)
            && !(raw.placed && acceptedSet.has("place"))) continue;
          if (!acceptsMaterial(material)) continue;
          if (acceptedSet.has(material) || acceptedSet.has("mix")) routeItems.push(base);
          if (place && acceptedSet.has("place")) routeItems.push(place);
        }
        routeItems.sort(compareItems);
        const routeDigest = digest({ subject, scope, route, lens, renderer, basis,
          program, collections: exact, readers: selectedReaders, items: routeItems, regions: regionRows });
        routeInsert.run(subject, scope, route, lens, renderer, basis, territory,
          String(program.title || ""), String(program.lede || ""), String(program.character || ""),
          String(program.cannot_infer || ""), canon((program.sections ?? []).map(String).filter(Boolean)),
          canon(exact.map(c => c.collection_id)), canon(legacyFamilies), canon(acceptedMaterials), canon(regionRows),
          selectedReaders.length, routeItems.length, routeDigest, compiledAt);
        routeItems.forEach((item, ordinal) => itemInsert.run(subject, scope, route, ordinal, item.source, item.key, item.material));
        totals.routes = (totals.routes ?? 0) + 1;
        totals.route_readers = (totals.route_readers ?? 0) + selectedReaders.length;
        totals.route_items = (totals.route_items ?? 0) + routeItems.length;
      }
    }
    const output = outputSnapshot(fabric);
    const summary = { schema: "bonfyre.subject-projection-precompute.v4", compiled_at: compiledAt,
      compiler_sha256: await compilerDigest(), input_digest: inputDigest, input_snapshot: input,
      projection_digest: digest(output), projection_tables: output, totals, basis_counts: basisCounts,
      truth: "request_ready_projection_over_already_admitted_normalized_material",
      cannot_infer: ["route membership -> truth or endorsement", "normalized retained item -> current external state",
        "SourceCollection membership -> rights, authority or provider availability", "precomputed action or region -> authorized effect"] };
    fabric.query("INSERT INTO subject_projection_build VALUES(1,?,?,?,?,?)").run(compiledAt, summary.compiler_sha256,
      inputDigest, summary.projection_digest, canon(summary));
    fabric.exec("COMMIT");
    cms.exec("COMMIT");
    if (args.manifest) atomicJson(args.manifest, summary);
    return { ...Object.fromEntries(Object.entries(summary).filter(([key]) => key !== "input_snapshot" && key !== "projection_tables")), owner_operations: 1 };
  } catch (error) {
    try { fabric.exec("ROLLBACK"); } catch {}
    try { cms.exec("ROLLBACK"); } catch {}
    throw error;
  } finally { fabric.close(); cms.close(); }
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await compile(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
