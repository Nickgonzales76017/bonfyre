// Pure route-selection rules. SourceCollection exact refs beat family fallback.
import { tableExists, parsed } from "./subject_fabric_common.mjs";

const string = value => value == null ? "" : String(value);
const materialByKind = { story: "publication", publication: "publication", video: "video", photo: "photo",
  audio: "audio", post: "post", event: "event", place: "place" };

export function parseItems(text, path = "items") {
  const rows = [];
  for (const [index, line] of text.split(/\r?\n/).entries()) {
    if (!line.trim()) continue;
    let row;
    try { row = JSON.parse(line); } catch (error) { throw new Error(`${path}:${index + 1}: ${error.message}`); }
    if (!row?.source || !row?.key) continue;
    row._material = materialByKind[string(row.kind).toLowerCase()] ?? "publication";
    rows.push(row);
  }
  return rows;
}

export function cmsCollections(db) {
  const out = new Map();
  if (!tableExists(db, "source_collection")) return out;
  for (const row of db.query("SELECT collection_id,subject_address,source_selector,material_kind,refresh_policy "
    + "FROM source_collection WHERE status='published' ORDER BY id").all()) {
    const selector = parsed(row.source_selector, {});
    out.set(string(row.collection_id), { collection_id: string(row.collection_id), subject: string(row.subject_address),
      domains: (selector.domains ?? []).map(string).filter(Boolean), sources: (selector.source_refs ?? []).map(string).filter(Boolean),
      grouping: string(selector.grouping), value: string(selector.value),
      material_kind: parsed(row.material_kind, []).map(string).filter(Boolean), refresh_policy: string(row.refresh_policy) });
  }
  return out;
}

export function sceneCollectionRoutes(db) {
  const out = new Map();
  if (!tableExists(db, "subject_query")) return out;
  const subjectScope = new Map();
  if (tableExists(db, "subject_scene")) for (const row of db.query("SELECT scene_key,subject,scope FROM subject_scene").all())
    subjectScope.set(string(row.scene_key), [string(row.subject), string(row.scope)]);
  const add = (subject, lens, collection) => {
    const key = JSON.stringify([subject, lens]);
    const ids = out.get(key) ?? new Set(); ids.add(collection); out.set(key, ids);
  };
  for (const row of db.query("SELECT scene_key,query_json FROM subject_query ORDER BY ordinal").all()) {
    const [subject, scope] = subjectScope.get(string(row.scene_key)) ?? ["", "public"];
    if (!subject || scope !== "public") continue;
    const query = parsed(row.query_json, {}), collection = string(query.source_collection);
    if (!collection) continue;
    const lens = string(query.application_kind);
    if (lens) add(subject, lens, collection);
    add(subject, "*", collection);
  }
  return out;
}

export function subjectTerritories(db, subject) {
  const territories = new Set();
  if (subject.startsWith("institution:aurekai:")) {
    const suffix = subject.slice("institution:aurekai:".length);
    if (["life", "live", "shop", "work"].includes(suffix)) territories.add(suffix);
  }
  if (tableExists(db, "site_presentation")) {
    try {
      for (const row of db.query("SELECT DISTINCT territory FROM site_presentation "
        + "WHERE subject_address=? AND status='published' AND COALESCE(territory,'')<>''").all(subject))
        if (row.territory) territories.add(string(row.territory));
    } catch {}
  }
  return territories;
}

export function routeLens(route, surfacePrograms) {
  for (const program of surfacePrograms.programs ?? []) if (string(program.route) === route)
    return string(program.program || route.replace(/^\/+|\/+$/g, "") || "front");
  return route.replace(/^\/+|\/+$/g, "") || "front";
}
export function routeMaterials(program, regionByRole) {
  return [...new Set((program.regions ?? []).map(role => regionByRole.get(string(role))?.material).filter(Boolean).map(string))].sort();
}
function collectionCarriesMaterial(collection, accepted) {
  const aliases = { story: "publication", article: "publication", image: "photo", recording: "audio", film: "video" };
  const value = collection.value.toLowerCase();
  return new Set(accepted.map(x => string(x).toLowerCase())).has(aliases[value] ?? value);
}
export function indexCollections(collections) {
  const bySubject = new Map();
  for (const [id, collection] of collections) {
    const rows = bySubject.get(collection.subject) ?? [];
    rows.push([id, collection]);
    bySubject.set(collection.subject, rows);
  }
  return { byId: collections, bySubject };
}
export function candidateCollections(subject, lens, route, index, sceneRefs, legacyFamilies, acceptedMaterials) {
  const ids = new Set(sceneRefs.get(JSON.stringify([subject, lens])) ?? []);
  const domains = new Set([lens, route.replace(/^\/+|\/+$/g, "")]);
  if (route === "/") domains.add("front");
  for (const [id, collection] of index.bySubject.get(subject) ?? []) {
    if (collection.domains.some(domain => domains.has(domain))) ids.add(id);
  }
  if (subject.startsWith("institution:aurekai:")) {
    // A route reads the families it names.  What a source merely carries
    // (any picture, any story) stands in only when none of those families has
    // a collection yet; mixed in alongside them it filled Style or Food with
    // every source that happens to publish photographs.
    const estate = index.bySubject.get("estate:aurekai") ?? [];
    const named = estate.filter(([, collection]) => collection.grouping === "family" && legacyFamilies.includes(collection.value));
    for (const [id] of named) ids.add(id);
    if (!named.length)
      for (const [id, collection] of estate)
        if (collection.grouping === "carries" && collectionCarriesMaterial(collection, acceptedMaterials)) ids.add(id);
  }
  return [...ids].sort().filter(id => index.byId.has(id)).map(id => index.byId.get(id));
}

const materialAliases = {
  publication: ["publicationoccurrence", "publication", "article", "story"],
  video: ["mediaprogram", "video", "publicationoccurrence"],
  photo: ["mediaasset", "photo", "image", "publicationoccurrence"],
  audio: ["recording", "audio", "mediaprogram", "publicationoccurrence"],
  event: ["event", "eventoccurrence"], post: ["communitypost", "post"],
  place: ["place", "spatialobservation", "publicationoccurrence"],
  mix: ["publicationoccurrence", "publication", "mediaasset", "mediaprogram", "recording"],
};
export function collectionMaterialAcceptance(collections) {
  if (!collections.length) return () => true;
  const token = value => string(value).toLowerCase().replace(/[^a-z0-9]/g, "");
  const names = new Set(collections.flatMap(c => c.material_kind.map(token)));
  if (!names.size) return () => true;
  return material => (materialAliases[material] ?? [material]).some(name => names.has(name));
}
export function collectionMaterialAccepts(collections, material) {
  return collectionMaterialAcceptance(collections)(material);
}

export function normalizedItem(row, material = row._material) {
  const fields = { source: "source", key: "key", family: "family", title: "title", byline: "by", publisher: "publisher",
    href: "href", group_name: "group", image: "image", asset: "asset", poster: "poster", body: "body", ident: "ident",
    about: "about", rights: "rights", rights_href: "rights_href", origin: "origin", origin_href: "origin_href",
    people: "people", where_text: "where", transcript_href: "transcript_href", chapters_href: "chapters_href", talk_href: "talk_href" };
  const out = { material };
  for (const [target, source] of Object.entries(fields)) out[target] = string(row[source]);
  out.at = Number(row.at || 0); out.duration_ms = Number(row.duration_ms || 0);
  out.placed = row.placed ? 1 : 0; out.lon = Number(row.lon || 0); out.lat = Number(row.lat || 0);
  return out;
}
export function compareItems(a, b) {
  const aAt = Number(a.at || 0), bAt = Number(b.at || 0);
  const aMissing = Number(aAt <= 0), bMissing = Number(bAt <= 0);
  if (aMissing !== bMissing) return aMissing - bMissing;
  const aOrder = a.material === "event" ? (aAt > 0 ? aAt : 2 ** 62) : (aAt > 0 ? -aAt : 0);
  const bOrder = b.material === "event" ? (bAt > 0 ? bAt : 2 ** 62) : (bAt > 0 ? -bAt : 0);
  if (aOrder < bOrder) return -1;
  if (aOrder > bOrder) return 1;
  const aSource = string(a.source), bSource = string(b.source);
  if (aSource < bSource) return -1;
  if (aSource > bSource) return 1;
  const aKey = string(a.key), bKey = string(b.key);
  if (aKey < bKey) return -1;
  if (aKey > bKey) return 1;
  return 0;
}
