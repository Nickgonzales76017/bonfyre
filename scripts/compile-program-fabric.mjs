#!/usr/bin/env bun
// Program declarations and bindings only; no effect, funding, or authority grant.
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { demandContracts, resolveCollection } from "../cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const config = Bun.spawnSync(["cue", "export", join(here, "program-fabric.cue"), "-e", "programFabric", "--out", "json"]);
if (config.exitCode !== 0) throw new Error(config.stderr.toString());
export const policy = JSON.parse(config.stdout.toString());
const now = () => new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
const slug = (value, cap = 64) => String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "").slice(0, cap).replace(/-$/g, "");
const encode = value => value && typeof value === "object" ? JSON.stringify(value) : value;

function store(db, stamp, newStatus = "published") {
  if (!["published", "draft"].includes(newStatus)) throw new Error("invalid program declaration status");
  const cache = new Map();
  const columns = table => {
    if (!cache.has(table)) cache.set(table, db.query(`PRAGMA table_info("${table}")`).all().map(row => row.name));
    return cache.get(table);
  };
  return (table, key, row, preserve = []) => {
    const allowed = columns(table);
    if (!allowed.length) throw new Error(`the CMS has no ${table} type registered`);
    const absent = Object.keys(row).filter(field => !allowed.includes(field));
    if (absent.length) throw new Error(`${table} cannot preserve declared fields: ${absent.join(", ")}`);
    const record = { namespace: policy.namespace, created_at: stamp, updated_at: stamp, published_at: "", status: newStatus };
    for (const [name, value] of Object.entries(row)) record[name] = encode(value);
    const fields = allowed.filter(field => field !== "id" && Object.hasOwn(record, field));
    if (!fields.includes(key) || !fields.includes("namespace")) throw new Error(`${table} has no namespace or ${key} key`);
    const quoted = field => `"${field}"`;
    // Recompilation owns declarations, not publication state or another owner's decisions.
    const protectedFields = new Set([key, "namespace", "created_at", "published_at", "status", ...preserve]);
    const updates = fields.filter(field => !protectedFields.has(field));
    const compared = updates.filter(field => field !== "updated_at");
    const conflict = compared.length ? `DO UPDATE SET ${updates.map(field => `"${field}"=excluded."${field}"`).join(",")} WHERE ${compared.map(field => `"${table}"."${field}" IS NOT excluded."${field}"`).join(" OR ")}` : "DO NOTHING";
    const sql = `INSERT INTO "${table}"(${fields.map(quoted).join(",")}) VALUES(${fields.map(() => "?").join(",")}) ON CONFLICT(namespace,"${key}") ${conflict}`;
    db.query(sql).run(...fields.map(field => record[field]));
  };
}

function loadTemplates(put, packsPath, hasDemandColumn) {
  const doc = JSON.parse(readFileSync(packsPath, "utf8"));
  const packs = Array.isArray(doc) ? doc : doc.packs;
  if (!Array.isArray(packs)) throw new Error("program pack needs a packs array");
  for (const pack of packs) put("program_template", "template_id", {
    template_id: pack.program_id, name: pack.name ?? pack.program_id, archetype: pack.archetype ?? "", ordinal: pack.ordinal ?? 0,
    subject_kinds: pack.subject_kinds ?? [], semantic_slots: pack.semantic_slots ?? [], capability_families: pack.capability_families ?? [],
    ...(hasDemandColumn||Object.hasOwn(pack,'address_demands')?{address_demands:Object.hasOwn(pack,'address_demands')?demandContracts(pack.address_demands):null}:{}),
    owner_bindings: pack.owner_bindings ?? [], funding_candidates: pack.grant_stack ?? [],
    surface_policy: pack.surface_policy ?? "bind_into_existing_world", page_policy: pack.page_policy ?? "no_dedicated_page_required",
    description: pack.description ?? "", cannot_infer: policy.template_cannot_infer,
  });
  return { templates: packs.length, cannot_infer: policy.template_cannot_infer };
}

function compileInstance(db, put, subject, siteId, templateId, ordinalBase, addressIndex, hasDemandColumn, options = {}) {
  const demandColumn=hasDemandColumn?'address_demands':'NULL AS address_demands';
  const row = db.query("SELECT name,archetype,semantic_slots,capability_families,funding_candidates,"+demandColumn+" FROM program_template WHERE namespace=? AND template_id=? AND status='published'").get(policy.namespace, templateId);
  if (!row) return null;
  const slots = JSON.parse(row.semantic_slots || "[]"), families = JSON.parse(row.capability_families || "[]"), funders = JSON.parse(row.funding_candidates || "[]");
  if (![slots, families, funders].every(Array.isArray)) throw new Error(`invalid template arrays: ${templateId}`);
  const templateSlug = slug(templateId);
  if (!templateSlug) throw new Error(`template has no addressable identity: ${templateId}`);
  const roleSlugs = slots.map(role => slug(role, 24));
  if (roleSlugs.some(role => !role) || new Set(roleSlugs).size !== roleSlugs.length) throw new Error(`template has colliding or empty slot identities: ${templateId}`);
  const instance = `${siteId}-${templateSlug}`;
  const prior = db.query("SELECT template_id,owner_subject,site_id FROM program_instance WHERE namespace=? AND instance_id=?").get(policy.namespace, instance);
  if (prior && (prior.template_id !== templateId || prior.owner_subject !== subject || prior.site_id !== siteId))
    throw new Error(`program identity collision: ${instance}; existing owner binding is not transferable`);
  const familySlugs = families.map(family => slug(family, 24));
  if (familySlugs.some(value => !value) || new Set(familySlugs).size !== familySlugs.length)
    throw new Error(`template has colliding or empty capability identities: ${templateId}`);
  let nextOrdinal = ordinalBase;
  put("program_instance", "instance_id", { instance_id: instance, template_id: templateId, owner_subject: subject,
    site_id: siteId, archetype: row.archetype, opened_at: now(), cannot_infer: options.instanceBoundary ?? policy.instance_cannot_infer }, ["opened_at"]);
  let lowered = 0, declared = 0;
  for (const [offset, role] of slots.entries()) {
    const roleSlug = roleSlugs[offset], slotId = `${instance}-${roleSlug}`;
    const previous = db.query("SELECT site_id,ordinal FROM semantic_slot WHERE namespace=? AND slot_id=?").get(policy.namespace, slotId);
    if (previous && previous.site_id !== siteId) throw new Error(`slot identity belongs to another site: ${slotId}`);
    if (previous && !Number.isSafeInteger(previous.ordinal)) throw new Error(`slot has no valid ordinal: ${slotId}`);
    const ordinal = previous ? previous.ordinal : ++nextOrdinal;
    put("semantic_slot", "slot_id", { slot_id: slotId, site_id: siteId, slot_name: roleSlug,
      ordinal, extent: policy.slot.extent, accepts: [role],
      min_material_quality: policy.slot.min_material_quality, max_items: policy.slot.max_items, htmx_boundary: `slot-${roleSlug}` },
      ["ordinal", "extent", "max_items", "min_material_quality", "htmx_boundary"]);
    const lowering = Object.hasOwn(policy.lowerings, role) ? policy.lowerings[role] : null;
    if (lowering) {
      const collection = lowering.selector === "source_collection" ? `${instance}-sources` : "";
      put("application_binding", "binding_id", { binding_id: `${slotId}-app`, site_id: siteId, slot_id: slotId,
        application_kind: lowering.application, material_query: lowering.selector === "source_collection" ? {} : { [lowering.selector]: `${instance}-world` },
        source_collection: collection, continuity_key: `${instance}:${role}` }, ["material_query", "source_collection"]);
      lowered++;
    } else declared++;
    put("program_binding", "binding_id", { binding_id: slotId, instance_id: instance, site_id: siteId,
      slot_id: slotId, program_role: role, lowered_by: lowering?.application ?? "",
      continuity_key: `${instance}:${role}`, cannot_infer: policy.binding_cannot_infer });
  }
  const previousCollection = db.query("SELECT subject_address,source_selector FROM source_collection WHERE namespace=? AND collection_id=?").get(policy.namespace, `${instance}-sources`);
  if (previousCollection && previousCollection.subject_address !== subject) throw new Error(`collection belongs to another subject: ${instance}`);
  const selector = previousCollection ? JSON.parse(previousCollection.source_selector || "{}") : {};
  if (!selector || Array.isArray(selector) || typeof selector !== "object" ||
      (Object.hasOwn(selector, "source_refs") && !Array.isArray(selector.source_refs)))
    throw new Error(`invalid retained source selector: ${instance}`);
  let sourceSelector={...selector,template_ref:templateId,source_refs:selector.source_refs??[],grouping:policy.source.grouping,value:row.name,
    capability_families:families,resolver:policy.source.resolver};
  const inherited=row.address_demands?JSON.parse(row.address_demands):null;
  let localResolution=null;
  if(sourceSelector.address_demands!==undefined||inherited!==null||families.length){
    const resolution=resolveCollection(sourceSelector,inherited,{cms:db,namespace:policy.namespace,index:addressIndex,
      binary:join(here,'../cmd/BonfyreExternalEstate/bonfyre-external-estate'),runFn:options.addressRunFn});
    sourceSelector=resolution.selector;localResolution=resolution.local;
  }
  put("source_collection", "collection_id", { collection_id: `${instance}-sources`, subject_address: subject,
    source_selector: sourceSelector, material_kind: policy.source.material_kind,
    refresh_policy: policy.source.refresh_policy }, ["refresh_policy"]);
  for (const family of families) put("program_requirement", "requirement_id", {
    requirement_id: `${instance}-needs-${slug(family, 24)}`, instance_id: instance, kind: "capability",
    statement: `this program needs ${family.replace(/_/g, " ")}, and this estate has not shown that it has it here`,
    owner: "CapabilityClosure", state: "open", cannot_infer: "an open requirement is an obligation recorded, never one met, and never authority to meet it" }, ["state"]);
  for (const [offset, funder] of funders.entries()) {
    const packageId = `${instance}-funding-${offset + 1}`;
    const previousPackage = db.query("SELECT instance_id,funding_program FROM funding_work_package WHERE namespace=? AND package_id=?").get(policy.namespace, packageId);
    if (previousPackage && (previousPackage.instance_id !== instance || previousPackage.funding_program !== funder))
      throw new Error(`funding slot changed: ${packageId}; reconcile its scoped work before rebinding`);
    put("funding_work_package", "package_id", { package_id: packageId, instance_id: instance, funding_program: funder,
      scope: [], cost_codes: [], distinctness_rule: "scope and cost codes must not overlap another package on this instance",
      cannot_infer: policy.package_cannot_infer }, ["scope", "cost_codes"]);
    put("program_requirement", "requirement_id", { requirement_id: `${packageId}-scope`, instance_id: instance,
      kind: "funding_scope", statement: `the scope and cost codes this work package covers are not stated, so no cost can be allocated to ${funder} here`,
      owner: "Finance", state: "open", cannot_infer: policy.package_cannot_infer }, ["state"]);
  }
  return { instance, name: row.name, archetype: row.archetype, slots: slots.length, lowered,
    declared_unlowered: declared, local_resolution:localResolution, requirements: families.length + funders.length, work_packages: funders.length, ordinal_end: nextOrdinal };
}

// The caller owns the SQLite transaction. Account preparation uses this same
// compiler with private draft status; the historical CLI retains published
// declaration semantics. Neither path performs external discovery or effects.
export function compilePrograms(db, {subject, siteId, programs, addressIndex,
  newStatus = "published", instanceBoundary, addressRunFn} = {}) {
  if (!subject || !siteId || !Array.isArray(programs) || !programs.length)
    throw new Error("program compilation needs a subject, site and templates");
  const put = store(db, now(), newStatus);
  const hasDemandColumn=db.query('PRAGMA table_info(program_template)').all().some(c=>c.name==='address_demands');
  let base = db.query("SELECT COALESCE(MAX(ordinal),0) AS ordinal FROM semantic_slot WHERE namespace=? AND site_id=?").get(policy.namespace, siteId).ordinal;
  const compiled = [], missing = [];
  for (const template of programs) {
    const result = compileInstance(db, put, subject, siteId, template, base, addressIndex, hasDemandColumn, {instanceBoundary,addressRunFn});
    if (result) { compiled.push(result); base = result.ordinal_end; }
    else missing.push(template);
  }
  return { subject, site_id: siteId, compiled, no_such_template: missing, cannot_infer: instanceBoundary ?? policy.instance_cannot_infer };
}

export function run({ verb, estate = join(homedir(), ".bonfyre/estate-local"), packs = "", subject = "", siteId = "", programs = [],
  addressIndex = process.env.BONFYRE_ADDRESS_INDEX ?? join(homedir(),".bonfyre/estate/estate.db") }) {
  if (verb === "load" && !packs) return { error: "load needs --packs" };
  if (verb === "compile" && (!subject || !siteId || !programs.length)) return { error: "compile needs --subject, --site-id and at least one --program" };
  if (!["load", "compile"].includes(verb)) throw new Error(`unknown verb: ${verb}`);
  const cmsPath = join(estate, "cms.db");
  if (!existsSync(cmsPath)) throw new Error(`CMS database missing: ${cmsPath}`);
  const db = new Database(cmsPath);
  db.exec("PRAGMA journal_mode=WAL");
  try {
    return db.transaction(() => {
      const put = store(db, now());
      const hasDemandColumn=db.query('PRAGMA table_info(program_template)').all().some(c=>c.name==='address_demands');
      if (verb === "load") return loadTemplates(put, packs, hasDemandColumn);
      return compilePrograms(db, {subject, siteId, programs, addressIndex});
    })();
  } finally { db.close(); }
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2), verb = argv.shift();
    const args = { verb, programs: [] };
    while (argv.length) {
      const key = argv.shift(), value = argv.shift();
      if (!value || !["--estate", "--packs", "--subject", "--site-id", "--program", "--address-index"].includes(key)) throw new Error(`unknown or incomplete argument: ${key}`);
      if (key === "--program") args.programs.push(value);
      else args[key === "--site-id" ? "siteId" : key === "--address-index" ? "addressIndex" : key.slice(2)] = value;
    }
    const result = run(args);
    console.log(JSON.stringify(result, null, 1));
    if (result.error) process.exitCode = 2;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
