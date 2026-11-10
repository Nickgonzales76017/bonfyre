#!/usr/bin/env bun
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { createReadStream, readFileSync, realpathSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const tables = [
  "subject_material_body", "subject_material_member", "subject_material",
  "subject_route", "subject_route_phase", "subject_route_reader",
  "subject_route_region", "subject_route_item",
];

function canon(value) {
  if (Array.isArray(value)) return `[${value.map(canon).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canon(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function display(value) {
  if (Array.isArray(value)) return `[${value.map(display).join(", ")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}: ${display(value[key])}`).join(", ")}}`;
  }
  return JSON.stringify(value);
}

function digest(value) {
  return createHash("sha256").update(canon(value)).digest("hex");
}

export async function fileDigest(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

/* The material compiler's semantics include its shared normalization rules
 * and readback implementation; hashing only its entrypoint can incorrectly
 * reuse a stale route when a downstream lowering rule changes. */
export async function compilerDigest() {
  const directory = dirname(fileURLToPath(import.meta.url));
  const paths = ["compile_subject_projection.mjs", "subject_projection_rules.mjs",
    "subject_fabric_common.mjs", "verify_subject_projection.mjs"];
  const parts = await Promise.all(paths.map(async file => [file, await fileDigest(join(directory, file))]));
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

// Snapshot CMS under SQLite WAL without changing a source row. A published
// server can leave WAL coordination files needing a write-capable *open*,
// despite the reader performing no writes. Prefer a strict readonly handle;
// on SQLITE_CANTOPEN, reopen ONLY the existing DB, set query_only, then read.
export function openVerifiedCmsReader(file) {
  let db;
  try {
    db = new Database(file, { readonly: true });
    db.query('SELECT name FROM sqlite_master LIMIT 1').get();
    return db;
  } catch (error) {
    db?.close();
    if (error?.code !== 'SQLITE_CANTOPEN') throw error;
  }
  db = new Database(file, { readwrite: true, create: false });
  try {
    db.exec('PRAGMA query_only=ON');
    if (db.query('PRAGMA query_only').get()?.query_only !== 1)
      throw new Error('CMS WAL reader did not enter query_only');
    db.query('SELECT name FROM sqlite_master LIMIT 1').get();
    return db;
  } catch (error) { db.close(); throw error; }
}

function rows(db, query, columns, params = []) {
  return db.query(query).all(...params).map(row => columns.map(column => row[column]));
}

function exists(db, table) {
  return !!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table);
}

export async function inputSnapshot(fabric, cms, args) {
  const bundles = rows(fabric,
    "SELECT subject,scope,bundle_digest FROM subject_bundle ORDER BY subject,scope",
    ["subject", "scope", "bundle_digest"]);
  const fabricSnapshot = {
    subject_bundle: bundles,
    subject_reader: rows(fabric,
      "SELECT subject,scope,census_source,root_id,channel_id,protocol,state " +
      "FROM subject_reader WHERE state IN ('admitted','active') " +
      "ORDER BY subject,scope,census_source,channel_id",
      ["subject", "scope", "census_source", "root_id", "channel_id", "protocol", "state"]),
  };
  if (exists(fabric, "subject_scene")) {
    fabricSnapshot.subject_scene = rows(fabric,
      "SELECT scene_key,subject,scope FROM subject_scene ORDER BY scene_key",
      ["scene_key", "subject", "scope"]);
  }
  if (exists(fabric, "subject_query")) {
    fabricSnapshot.subject_query = rows(fabric,
      "SELECT scene_key,ordinal,query_json FROM subject_query ORDER BY scene_key,ordinal",
      ["scene_key", "ordinal", "query_json"]);
  }
  let collections = [];
  if (exists(cms, "source_collection")) {
    const subjects = new Set(bundles.map(row => String(row[0])));
    if ([...subjects].some(subject => subject.startsWith("institution:aurekai:"))) {
      subjects.add("estate:aurekai");
    }
    collections = rows(cms,
      "SELECT collection_id,subject_address,source_selector,material_kind,refresh_policy " +
      "FROM source_collection WHERE status='published' ORDER BY id",
      ["collection_id", "subject_address", "source_selector", "material_kind", "refresh_policy"])
      .filter(row => subjects.has(row[1]));
  }
  const presentations = [];
  if (exists(cms, "site_presentation")) {
    try {
      for (const [subject] of bundles) {
        presentations.push(...rows(cms,
          "SELECT subject_address,territory FROM site_presentation " +
          "WHERE subject_address=? AND status='published' " +
          "AND COALESCE(territory,'')<>'' ORDER BY territory",
          ["subject_address", "territory"], [subject]));
      }
    } catch {
      // Older CMS fixtures do not carry a territory field.
    }
  }
  const [itemsHash, pageHash, surfaceHash] = await Promise.all([
    fileDigest(args.items), fileDigest(args.pageComposition), fileDigest(args.surfacePrograms),
  ]);
  return {
    source_coordinates: {
      cms_db: realpathSync(args.cmsDb), items: realpathSync(args.items),
      page_composition: realpathSync(args.pageComposition),
      surface_programs: realpathSync(args.surfacePrograms),
    },
    items_sha256: itemsHash,
    page_composition_sha256: pageHash,
    surface_programs_sha256: surfaceHash,
    fabric: fabricSnapshot,
    published_source_collections: collections,
    published_site_presentations: presentations,
  };
}

function quote(identifier) {
  return `"${identifier.replaceAll('"', '""')}"`;
}

export function outputSnapshot(fabric) {
  const out = {};
  for (const table of tables) {
    const object = fabric.query("SELECT type,sql FROM sqlite_master WHERE name=? AND type IN ('table','view')").get(table);
    if (!object || (table === "subject_material" && object.type !== "view"))
      throw new Error(`missing or noncanonical projection relation: ${table}`);
    const columns = fabric.query(`PRAGMA table_info(${quote(table)})`).all();
    if (!columns.length) throw new Error(`missing projection table: ${table}`);
    let keys = columns.filter(column => column.pk).sort((a, b) => a.pk - b.pk).map(column => column.name);
    if (table === "subject_material") keys = ["subject", "scope", "source", "item_key", "material"];
    if (!keys.length) throw new Error(`projection table has no primary key: ${table}`);
    const names = columns.map(column => quote(column.name)).join(",");
    const order = keys.map(quote).join(",");
    const statement = fabric.query(`SELECT json_array(${names}) AS row_json FROM ${quote(table)} ORDER BY ${order}`);
    const hash = createHash("sha256");
    let count = 0;
    for (const row of statement.iterate()) {
      hash.update(row.row_json);
      hash.update("\n");
      count++;
    }
    out[table] = { rows: count, sha256: hash.digest("hex"), object_type: object.type,
      schema_sha256: createHash("sha256").update(object.sql || "").digest("hex") };
  }
  return out;
}

function argsFrom(argv) {
  const args = {};
  const names = {
    "--fabric-db": "fabricDb", "--cms-db": "cmsDb", "--items": "items",
    "--page-composition": "pageComposition", "--surface-programs": "surfacePrograms",
    "--manifest": "manifest",
  };
  for (let i = 0; i < argv.length; i += 2) {
    const name = names[argv[i]];
    if (!name || i + 1 >= argv.length) throw new Error(`unknown or incomplete argument: ${argv[i]}`);
    args[name] = argv[i + 1];
  }
  for (const required of ["fabricDb", "cmsDb", "items", "pageComposition", "surfacePrograms"]) {
    if (!args[required]) throw new Error(`missing ${required}`);
  }
  return args;
}

export async function verify(args) {
  const fabric = new Database(args.fabricDb, { readonly: true });
  const cms = openVerifiedCmsReader(args.cmsDb);
  try {
    fabric.exec("BEGIN");
    cms.exec("BEGIN");
    const row = fabric.query(
      "SELECT compiled_at,compiler_sha256,input_digest,projection_digest,manifest_json " +
      "FROM subject_projection_build WHERE build_id=1").get();
    if (!row) throw new Error("projection has no committed build record");
    const manifest = JSON.parse(row.manifest_json);
    if (manifest.schema !== "bonfyre.subject-projection-precompute.v4") {
      throw new Error("unsupported projection build schema");
    }
    for (const [column, field] of [
      ["compiled_at", "compiled_at"], ["compiler_sha256", "compiler_sha256"],
      ["input_digest", "input_digest"], ["projection_digest", "projection_digest"],
    ]) {
      if (row[column] !== manifest[field]) throw new Error("build record differs from its committed manifest");
    }
    if (manifest.compiler_sha256 !== await compilerDigest()) {
      throw new Error("compiler or shared lowering rules changed since this projection was built");
    }
    const input = await inputSnapshot(fabric, cms, args);
    if (canon(input) !== canon(manifest.input_snapshot) || digest(input) !== manifest.input_digest) {
      throw new Error("projection input frontier changed");
    }
    const output = outputSnapshot(fabric);
    if (canon(output) !== canon(manifest.projection_tables) || digest(output) !== manifest.projection_digest) {
      throw new Error("projection rows differ from committed build");
    }
    if (args.manifest && canon(JSON.parse(readFileSync(args.manifest, "utf8"))) !== canon(manifest)) {
      throw new Error("external build manifest differs from committed build");
    }
    return {
      schema: "bonfyre.subject-projection-verification.v1", verified: true,
      input_digest: manifest.input_digest, projection_digest: manifest.projection_digest,
      routes: output.subject_route.rows, route_items: output.subject_route_item.rows,
    };
  } finally {
    fabric.close();
    cms.close();
  }
}

if (import.meta.main) {
  try { console.log(display(await verify(argsFrom(Bun.argv.slice(2))))); }
  catch (error) { console.error(`subject projection verification failed: ${error.message}`); process.exitCode = 1; }
}
