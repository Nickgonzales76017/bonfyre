#!/usr/bin/env bun
// Compose person-host surfaces from published native cells and receipted estate material.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const occurrenceBoundary = "this is what this estate observed a source publish at the time it read it; it is not a claim that the source still publishes it, that what it says is true, or that the source knows this estate read it";
const worldBoundary = "these are the surfaces bound to this person's address, not things the person has made; an empty surface means nothing has been added to it, never that nothing exists";
const resultBoundary = "a member's page is the surfaces this estate binds to their address plus this estate's own reading; it is not material the person published, chose, or endorsed, and an empty surface means nothing has been added to it";
const slug = (text, cap = 48) => String(text || "").toLowerCase().replace(/[^a-z0-9]/g, "-")
  .replace(/-+/g, "-").replace(/^-|-$/g, "").slice(0, cap);
const digest = (...parts) => createHash("sha1").update(parts.join("|")).digest("hex").slice(0, 24);
const iso = seconds => new Date(Number(seconds) * 1000).toISOString().replace(/\.\d{3}Z$/, "Z");
const title = text => text.replace(/\b\w/g, char => char.toUpperCase());

function argsOf(argv) {
  const args = { estate: join(homedir(), ".bonfyre/estate-local"), repo: join(homedir(), "Developer/Bonfyre"),
    occurrences: 12000, apply: false };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--apply") { args.apply = true; continue; }
    if (!["--estate", "--repo", "--occurrences"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2)] = argv[++i];
  }
  args.occurrences = Number(args.occurrences);
  if (!Number.isInteger(args.occurrences) || args.occurrences < 0)
    throw new Error("--occurrences must be a nonnegative integer");
  return args;
}

function cellsByClass(repo) {
  const raw = JSON.parse(readFileSync(join(repo, "cmd/BonfyreWebAddress/generated/native-presentation-cells.json"), "utf8"));
  const rows = Array.isArray(raw) ? raw : Object.values(raw).find(Array.isArray);
  const classes = new Map();
  for (const cell of rows) {
    const klass = cell.presentation_class || "anonymous";
    if (!classes.has(klass)) classes.set(klass, new Map());
    if (!classes.get(klass).has(cell.grammar)) classes.get(klass).set(cell.grammar, cell);
  }
  return classes;
}

function surfacePrograms(repo) {
  const file = join(repo, "cmd/BonfyreWebAddress/generated/native-surface-programs.json");
  return new Set(JSON.parse(readFileSync(file, "utf8")).programs.map(row => row.program));
}

class Store {
  constructor(path, now) {
    this.db = new Database(path);
    this.db.exec("PRAGMA busy_timeout=60000; PRAGMA journal_mode=WAL");
    this.ns = "root";
    this.now = now;
    this.columns = new Map();
    this.keyedCache = new Map();
    this.db.exec("BEGIN");
  }
  cols(table) {
    if (!this.columns.has(table)) this.columns.set(table,
      this.db.query(`PRAGMA table_info("${table}")`).all().map(row => row.name));
    return this.columns.get(table);
  }
  has(table) { return this.cols(table).length > 0; }
  keyed(table, keyField) {
    const mark = `${table}:${keyField}`;
    if (!this.keyedCache.has(mark)) {
      const found = this.db.query("SELECT 1 FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql LIKE '%UNIQUE%' AND sql LIKE ?").get(table, `%"${keyField}"%`);
      this.keyedCache.set(mark, !!found);
    }
    return this.keyedCache.get(mark);
  }
  put(table, keyField, row) {
    const record = { namespace: this.ns, created_at: this.now, updated_at: this.now,
      published_at: "", status: "published" };
    for (const [field, value] of Object.entries(row))
      record[field] = value && typeof value === "object" ? JSON.stringify(value) : value;
    const fields = this.cols(table).filter(field => field !== "id" && Object.hasOwn(record, field));
    const values = fields.map(field => record[field]);
    const names = fields.map(field => `"${field}"`).join(",");
    const marks = fields.map(() => "?").join(",");
    if (this.keyed(table, keyField)) {
      const updates = fields.filter(field => field !== "created_at")
        .map(field => `"${field}"=excluded."${field}"`).join(",");
      this.db.query(`INSERT INTO "${table}"(${names}) VALUES(${marks}) ON CONFLICT(namespace,"${keyField}") DO UPDATE SET ${updates}`).run(...values);
    } else {
      this.db.query(`DELETE FROM "${table}" WHERE namespace=? AND "${keyField}"=?`).run(this.ns, record[keyField]);
      this.db.query(`INSERT INTO "${table}"(${names}) VALUES(${marks})`).run(...values);
    }
  }
  dropWhere(table, clause, args) {
    this.db.query(`DELETE FROM "${table}" WHERE namespace=? AND ${clause}`).run(this.ns, ...args);
  }
  commit() { this.db.exec("COMMIT; BEGIN"); }
  done() { this.db.exec("COMMIT"); this.db.close(); }
  abort() { try { this.db.exec("ROLLBACK"); } catch (_) {} this.db.close(); }
}

function projectOccurrences(store, estate, limit) {
  const items = new Database(join(estate, "items.db"), { readonly: true });
  const twins = new Map(store.db.query("SELECT source, COALESCE(twin,'') AS twin FROM internet_source WHERE status='published'").all()
    .map(row => [row.source, row.twin]));
  const receiptRoot = join(estate, "foreign", "receipts");
  let written = 0, skippedReceipt = 0, skippedSource = 0;
  const families = new Map(), kinds = new Map();
  try {
    const rows = items.query("SELECT source,key,kind,family,title,COALESCE(href,'') AS href,COALESCE(publisher,'') AS publisher,at FROM item WHERE title <> '' ORDER BY at DESC LIMIT ?").iterate(limit);
    for (const row of rows) {
      if (!twins.has(row.source)) { skippedSource++; continue; }
      if (!existsSync(join(receiptRoot, `${row.source}.json`))) { skippedReceipt++; continue; }
      const when = row.at ? iso(row.at) : store.now;
      store.put("publication_occurrence", "occurrence_id", {
        occurrence_id: digest(row.source, row.key), source_twin: twins.get(row.source) || row.source,
        source_ref: row.source, canonical_href: Array.from(row.href).slice(0, 1000).join(""),
        title: Array.from(row.title).slice(0, 480).join(""), summary: "",
        published_at: when, observed_at: when, receipt_ref: `foreign/receipts/${row.source}.json`,
        rights_basis: "public_feed_observation", retention: "archive_local", visibility: "public",
        assertion_state: "OBSERVED", cannot_infer: occurrenceBoundary,
      });
      written++;
      if (row.family) {
        if (!families.has(row.family)) families.set(row.family, new Set());
        families.get(row.family).add(row.source);
      }
      if (row.kind) {
        if (!kinds.has(row.kind)) kinds.set(row.kind, new Set());
        kinds.get(row.kind).add(row.source);
      }
    }
  } finally { items.close(); }
  return [{ occurrences: written, without_receipt: skippedReceipt, unknown_source: skippedSource }, families, kinds];
}

function publishCollections(store, families, kinds, perCollection = 300) {
  const published = [];
  for (const [grouping, groups] of [["family", families], ["carries", kinds]]) {
    const ordered = [...groups].sort((a, b) => b[1].size - a[1].size);
    for (const [label, sources] of ordered) {
      const name = slug(label);
      if (!name || !sources.size) continue;
      const refs = [...sources].sort().slice(0, perCollection);
      const collectionId = `estate-${grouping}-${name}`;
      store.put("source_collection", "collection_id", {
        collection_id: collectionId, subject_address: "estate:aurekai",
        source_selector: { source_refs: refs, grouping, value: label },
        material_kind: ["publication_occurrence"], refresh_policy: "on_observation",
      });
      published.push([collectionId, label, refs.length]);
    }
  }
  return published;
}

function publishWorld(store, world, subject, label, cells) {
  store.put("world", "world", { world, title: label, character: "workspace",
    lede: "The surfaces bound to this address.", subject, sources: "native_presentation_cells",
    grammars: [...cells.keys()].sort().join(","), cannot_infer: worldBoundary });
  store.dropWhere("world_fragment", "world=?", [world]);
  let ordinal = 0;
  for (const [grammar, cell] of [...cells].sort((a, b) => a[0].localeCompare(b[0]))) {
    store.put("world_fragment", "fragment_id", {
      fragment_id: `${world}-${grammar}`, world, ordinal: ++ordinal,
      subject_address: cell.route, surface_grammar: grammar,
      renderer_family: cell.renderer_family, renderer_variant: cell.renderer_variant || "",
      extent: cell.extent_defaults || "section", visible_sections: (cell.visible_sections || []).join(", "),
      provenance: cell.question || "", cannot_infer: cell.refusal_character || worldBoundary,
    });
  }
  return ordinal;
}

function publishSlots(store, site, world, collections) {
  store.dropWhere("semantic_slot", "site_id=? AND slot_id NOT IN (SELECT slot_id FROM program_binding WHERE namespace=? AND status='published')", [site, store.ns]);
  store.dropWhere("application_binding", "site_id=? AND slot_id NOT IN (SELECT slot_id FROM program_binding WHERE namespace=? AND status='published')", [site, store.ns]);
  let ordinal = 0;
  const slots = [];
  function slot(name, kind, extent, maxItems, collection = "", query = {}, boundary = "") {
    const slotId = `${site}-${name}`;
    store.put("semantic_slot", "slot_id", { slot_id: slotId, site_id: site, slot_name: name,
      ordinal: ++ordinal, extent, accepts: [kind], min_material_quality: "observed",
      max_items: maxItems, htmx_boundary: boundary || `slot-${name}` });
    store.put("application_binding", "binding_id", {
      binding_id: `${slotId}-${slug(kind)}`, site_id: site, slot_id: slotId,
      application_kind: kind, material_query: query, source_collection: collection,
      continuity_key: `${site}:${name}` });
    slots.push({ slot: name, application: kind, collection });
  }
  slot("surfaces", "Board", "workspace", 24, "", { world });
  const lead = collections.filter(row => row[0].startsWith("estate-family-"));
  const carried = collections.filter(row => row[0].startsWith("estate-carries-"));
  for (const [index, row] of lead.entries())
    slot(`reading-${slug(row[1], 24)}`, index % 2 ? "PublicationFeed" : "Newsroom", "section", index < 10 ? 14 : 6, row[0]);
  for (const row of carried)
    slot(`carrying-${slug(row[1], 24)}`, "PublicationFeed", "section", 12, row[0]);
  slot("commons", "Community", "section", 12, "", { community: site });
  slot("listening", "Listen", "card", 1, "", { recording: `${site}-latest` });
  slot("watching", "Watch", "card", 1, "", { program: `${site}-latest` });
  slot("conditions", "Weather", "chip", 1);
  return slots;
}

function publishableCapabilityPrograms(store, collections) {
  if (!store.has("capability_program")) return [];
  const available = new Set(["publication"]);
  for (const [, label] of collections) {
    const value = String(label || "").trim().toLowerCase();
    if (value) available.add(value);
  }
  const selected = [];
  for (const row of store.db.query("SELECT program_id,consumes FROM capability_program WHERE namespace=? AND status='published' ORDER BY program_id").all(store.ns)) {
    let consumes = [];
    try { consumes = JSON.parse(row.consumes || "[]"); } catch (_) { continue; }
    if (Array.isArray(consumes) && consumes.some(value => available.has(String(value).toLowerCase())))
      selected.push(row.program_id);
  }
  return selected;
}

function publishProgram(store, site, slots, collections, grammars) {
  const capabilityPrograms = publishableCapabilityPrograms(store, collections);
  store.put("site_program", "program_id", {
    program_id: `${site}-world`, site_id: site,
    experience_programs: [...grammars].sort().map(grammar => ({ grammar, route: `/${grammar}` })),
    application_bindings: slots, renderer_preferences: { board: "masonry_board", river: "vertical" },
    source_collections: collections.map(row => row[0]), capability_programs: capabilityPrograms,
    actor_presentations: { anonymous: "public surfaces and this estate's own reading",
      personal: "the same surfaces, with what this person has added" },
  });
  store.db.query("UPDATE site_presentation SET navigation=?, updated_at=? WHERE namespace=? AND site_id=?")
    .run(JSON.stringify([...grammars].sort()), store.now, store.ns, site);
}

function members(estate) {
  const db = new Database(join(estate, "matrix.db"), { readonly: true });
  try { return db.query("SELECT s.subject_id, p.territory, p.scope FROM matrix_subjects s LEFT JOIN matrix_presences p ON p.subject_id=s.subject_id AND p.active=1 WHERE s.subject_kind='person'").all(); }
  finally { db.close(); }
}

export function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const now = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  const programs = surfacePrograms(args.repo);
  const byClass = new Map([...cellsByClass(args.repo)].map(([klass, cells]) =>
    [klass, new Map([...cells].filter(([grammar]) => programs.has(grammar)))]));
  const cells = byClass.get("anonymous") || new Map();
  const people = members(args.estate);
  if (!args.apply) {
    console.log(JSON.stringify({ members: people, surfaces: [...cells.keys()].sort(),
      would_write: ["publication_occurrence", "source_collection", "world", "world_fragment",
        "semantic_slot", "application_binding", "site_program"] }, null, 1));
    return 0;
  }
  const store = new Store(join(args.estate, "cms.db"), now);
  try {
    for (const table of ["publication_occurrence", "source_collection", "world", "world_fragment",
      "semantic_slot", "application_binding", "site_program", "site_presentation"]) {
      if (!store.has(table)) { console.log(JSON.stringify({ error: `the CMS has no ${table} type registered` })); store.abort(); return 2; }
    }
    const [read, families, kinds] = projectOccurrences(store, args.estate, args.occurrences);
    store.commit();
    const collections = publishCollections(store, families, kinds);
    store.commit();
    const composed = [];
    for (const person of people) {
      const subject = person.subject_id;
      const label = slug(subject.split(":").slice(1).join(":"));
      const site = `person-${label}`;
      const present = store.db.query("SELECT host FROM site_presentation WHERE namespace=? AND site_id=? AND status='published'").get(store.ns, site);
      if (!present) { composed.push({ subject, skipped: "no published site_presentation" }); continue; }
      const world = `${label}-surfaces`;
      const worldTitle = `${title(label.replaceAll("-", " "))}'s surfaces`;
      let fragments = publishWorld(store, world, subject, worldTitle, cells);
      for (const [klass, klassCells] of [...byClass].sort((a, b) => a[0].localeCompare(b[0]))) {
        if (klass === "anonymous" || !klassCells.size) continue;
        fragments += publishWorld(store, `${world}-${klass}`, subject, worldTitle, klassCells);
      }
      const slots = publishSlots(store, site, world, collections);
      publishProgram(store, site, slots, collections, cells.keys());
      composed.push({ subject, host: present.host, site, world, fragments, slots: slots.length });
      store.commit();
    }
    store.done();
    console.log(JSON.stringify({ read, collections: collections.length, members: composed,
      cannot_infer: resultBoundary }, null, 1));
    return 0;
  } catch (error) { store.abort(); throw error; }
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
