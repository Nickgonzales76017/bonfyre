#!/usr/bin/env bun
// Exact, source-bound identity links. A multi-pattern automaton scans each
// retained item once; it never promotes geometric similarity to identity.
import { Database } from "bun:sqlite";
import { existsSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash, randomUUID } from "node:crypto";

const context = /\b(Texas|TX|North Texas|DFW|Dallas[- ]Fort Worth|Metroplex|Tarrant|Collin County|Denton County|Dallas County)\b/;
const localFamilies = new Set(["local-voices", "local-photos", "local-nature", "places", "live-music"]);
const stopNames = new Set(["Texas", "United States", "America", "Downtown", "Uptown", "Main Street"]);
const wordOrHyphen = /[\p{L}\p{N}_-]/u;

function options(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--only-music-identities") args.onlyMusic = true;
    else if (["--items", "--cms", "--atlas", "--db", "--out"].includes(key)) args[key.slice(2)] = argv[++i];
    else throw new Error(`unknown argument: ${key}`);
  }
  for (const key of ["items", "cms", "atlas", "db"]) if (!args[key]) throw new Error(`missing --${key}`);
  return args;
}

function normTitle(title) {
  return String(title ?? "").toLowerCase().replace(/\([^)]*\)/g, " ").trim()
    .replace(/^(the|a|an)\s+/, "").replace(/[^a-z0-9]+/g, " ").trim();
}

function musicObservations(items, targets = null) {
  const relations = [];
  for (const it of items) {
    if (targets && !targets.has(it.source)) continue;
    const ident = String(it.ident ?? "").trim();
    const href = String(it.href ?? "").trim();
    if (it.source === "wikidata-recorded-music" && it.kind === "audio"
      && /^Q[1-9][0-9]*$/.test(ident)
      && /^https?:\/\/(?:[a-z-]+\.)?wikipedia\.org\/wiki\//.test(href)) {
      relations.push({ relate: { src: `o/${it.source}/${it.key}`, relation: "observed_as", dst: `qid:${ident}`,
        evidence: `linker: Wikidata recorded-music identifies this retained audio as ${ident}; ${href}` } });
    }
  }
  return relations;
}

// Aho-Corasick gives one pass through each text, independent of gazetteer size.
// Outputs retain Python's longest-name-first priority when multiple names match.
function matcher(names) {
  const nodes = [{ next: new Map(), fail: 0, out: [] }];
  const chars = names.map(name => Array.from(name));
  chars.forEach((letters, index) => {
    let state = 0;
    for (const letter of letters) {
      let next = nodes[state].next.get(letter);
      if (next === undefined) {
        next = nodes.length;
        nodes[state].next.set(letter, next);
        nodes.push({ next: new Map(), fail: 0, out: [] });
      }
      state = next;
    }
    nodes[state].out.push(index);
  });
  const queue = [...nodes[0].next.values()];
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const state = queue[cursor];
    for (const [letter, child] of nodes[state].next) {
      let fail = nodes[state].fail;
      while (fail && !nodes[fail].next.has(letter)) fail = nodes[fail].fail;
      nodes[child].fail = nodes[fail].next.get(letter) ?? 0;
      nodes[child].out.push(...nodes[nodes[child].fail].out);
      queue.push(child);
    }
  }
  return text => {
    const letters = Array.from(text);
    const hits = new Set();
    let state = 0;
    letters.forEach((letter, at) => {
      while (state && !nodes[state].next.has(letter)) state = nodes[state].fail;
      state = nodes[state].next.get(letter) ?? 0;
      for (const index of nodes[state].out) {
        const before = at - chars[index].length;
        const after = at + 1;
        if ((before < 0 || !wordOrHyphen.test(letters[before]))
          && (after === letters.length || !wordOrHyphen.test(letters[after]))) hits.add(index);
      }
    });
    return [...hits].sort((a, b) => a - b);
  };
}

function publisherMaterial(cms, gazetteer) {
  const publisherOf = new Map();
  const db = new Database(cms, { readonly: true });
  try {
    for (const row of db.query("SELECT source, COALESCE(publisher_ident,'') AS ident, COALESCE(publisher_name,'') AS name FROM internet_source WHERE status='published' AND COALESCE(publisher_ident,'')<>''").all()) {
      publisherOf.set(row.source, row.ident);
      if (row.name && !stopNames.has(row.name) && row.name.length >= 4 && !gazetteer.has(row.name)) gazetteer.set(row.name, row.ident);
    }
  } catch (error) {
    if (!String(error.message).includes("no such table")) throw error;
  } finally { db.close(); }
  return publisherOf;
}

function compile(items, cms, targets = null) {
  const gazetteer = new Map();
  for (const it of items) {
    if (it.family === "places" && it.ident && !stopNames.has(it.title) && it.title.length >= 4 && !gazetteer.has(it.title)) gazetteer.set(it.title, it.ident);
  }
  const publisherOf = publisherMaterial(cms, gazetteer);
  const names = [...gazetteer.keys()].sort((a, b) => Array.from(b).length - Array.from(a).length);
  const scan = matcher(names);
  const relations = [];
  for (const it of items) {
    if (targets && !targets.has(it.source)) continue;
    if (it.family === "places") continue;
    const text = `${it.title} ${it.body}`;
    const hasContext = context.test(text);
    const local = localFamilies.has(it.family) || hasContext;
    const found = new Set();
    for (const index of scan(text)) {
      const name = names[index];
      const qid = gazetteer.get(name);
      const multiword = name.split(/\s+/).length >= 2;
      if (found.has(qid) || qid === it.ident || qid === it.about || qid === publisherOf.get(it.source) || (!multiword && !local)) continue;
      found.add(qid);
      const rule = multiword ? "multi-word name" : hasContext ? "single-word name with North Texas context" : "single-word name in a local source";
      relations.push({ relate: { src: `o/${it.source}/${it.key}`, relation: "mentions", dst: `qid:${qid}`, evidence: `linker: ${rule} '${name}'` } });
      if (found.size >= 6) break;
    }
  }
  relations.push(...musicObservations(items, targets));
  for (const it of items) {
    if (targets && !targets.has(it.source)) continue;
    if (it.kind !== "event") continue;
    const where = String(it.where ?? "").trim();
    let placed = false;
    for (const index of scan(where)) {
      const name = names[index];
      const multiword = name.split(/\s+/).length >= 2;
      if (!multiword && !(context.test(where) || localFamilies.has(it.family))) continue;
      relations.push({ relate: { src: `o/${it.source}/${it.key}`, relation: "happens_at", dst: `qid:${gazetteer.get(name)}`, evidence: `linker: the organizer's own place line names '${name}'` } });
      placed = true;
      break;
    }
    if (!placed && publisherOf.has(it.source)) relations.push({ relate: { src: `o/${it.source}/${it.key}`, relation: "happens_at", dst: `qid:${publisherOf.get(it.source)}`, evidence: "linker: published by the organization whose calendar it is" } });
  }
  const films = new Map();
  const surnames = new Set();
  for (const it of items) {
    if (it.source !== "wikidata-public-domain-films" || !it.ident) continue;
    const title = normTitle(it.title);
    if (!films.has(title)) films.set(title, []);
    films.get(title).push(it);
    const surname = String(it.by ?? "").trim().split(/\s+/).at(-1) ?? "";
    if (surname.length >= 3) surnames.add(surname);
  }
  const directorNames = [...surnames];
  const scanDirectors = matcher(directorNames);
  for (const it of items) {
    if (targets && !targets.has(it.source)) continue;
    if (it.source !== "archive-feature-films") continue;
    const credits = `${it.by} ${it.body} ${it.title}`;
    const credited = new Set(scanDirectors(credits).map(index => directorNames[index]));
    for (const film of films.get(normTitle(it.title)) ?? []) {
      const director = String(film.by ?? "").trim();
      const surname = director.split(/\s+/).at(-1) ?? "";
      if (!credited.has(surname)) continue;
      relations.push({ relate: { src: `o/${it.source}/${it.key}`, relation: "same_subject_as", dst: `qid:${film.ident}`, evidence: `linker: same title '${film.title}' and director ${director} in the Archive's credits` } });
    }
  }
  return { gazetteer: gazetteer.size, relations };
}

function runAtlas(atlas, argv) {
  const result = Bun.spawnSync([atlas, ...argv], { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr) || `atlas exited ${result.exitCode}`);
  return new TextDecoder().decode(result.stdout).trim();
}

// A source is invalidated by its retained items OR any shared reference used
// by the link compiler. This frontier belongs to native Atlas, not a sidecar
// cache; exact source replacements and their revisions commit atomically.
const digest = data => createHash("sha256").update(data).digest("hex");
const digestValue = value => digest(JSON.stringify(value));
const compareText = (a,b) => a < b ? -1 : a > b ? 1 : 0;
const validSource = source => typeof source === "string" && /^[A-Za-z0-9._-]{1,127}$/.test(source);
const sortRows = rows => rows.toSorted((a,b) =>
  compareText(a.src,b.src) || compareText(a.relation,b.relation) || compareText(a.dst,b.dst));
const digestRows = rows => digestValue(sortRows(rows));
const relationKey = row => JSON.stringify([row.src,row.relation,row.dst]);
function ownerOf(src) {
  const parts = /^o\/([^/]+)\/.+$/.exec(src);
  if (!parts || !validSource(parts[1])) throw Error("unrecognized derived item address: "+src);
  return parts[1];
}
function compilerContext(items, cms, atlas) {
  const db = new Database(cms,{readonly:true});
  let publishers = [];
  try {
    publishers = db.query("SELECT source,COALESCE(publisher_ident,'') AS ident,COALESCE(publisher_name,'') AS name FROM internet_source WHERE status='published' AND COALESCE(publisher_ident,'')<>'' ORDER BY source").all();
  } catch (error) {
    if (!String(error.message).includes("no such table")) throw error;
  } finally { db.close(); }
  return digestValue({ compiler: digest(readFileSync(import.meta.path)),
    native_atlas: digest(readFileSync(atlas)),
    publishers, references: items.filter(it =>
      it.family === "places" || it.source === "wikidata-public-domain-films") });
}
function readLinkerRows(db) {
  const grouped = new Map();
  for (const row of db.query("SELECT src,relation,dst,evidence FROM relation WHERE evidence LIKE 'linker:%' ORDER BY src,relation,dst").all()) {
    const id = ownerOf(row.src);
    if (!grouped.has(id)) grouped.set(id, []);
    grouped.get(id).push(row);
  }
  return grouped;
}
function main() {
  const args = options(process.argv.slice(2));
  const items = readFileSync(args.items,"utf8").split(/\r?\n/).filter(Boolean).map(JSON.parse);
  if (args.onlyMusic) {
    // The narrow audio importer remains unchanged. A subsequent general pass
    // detects its modifications by comparing real Atlas relations to the ledger.
    const relations = musicObservations(items), out = args.out ?? join(tmpdir(),"bonfyre-link-"+randomUUID()+".jsonl");
    try {
      writeFileSync(out, relations.map(JSON.stringify).join("\n")+(relations.length?"\n":""));
      runAtlas(args.atlas,["forget","--db",args.db,"--relation","observed_as","--evidence-prefix","linker:"]);
      const imported = runAtlas(args.atlas,["import","--db",args.db,"--jsonl",out]);
      console.log(JSON.stringify({scope:"exact_wikidata_recorded_music",items:items.length,
        relations:{observed_as:relations.length},import:imported}));
    } finally { if (!args.out) unlinkSync(out); }
    return;
  }
  if (!existsSync(args.db)) runAtlas(args.atlas,["init","--db",args.db]);
  const input=new Map();
  for (const it of items) {
    if (!validSource(it.source) || it.key===undefined || it.key===null || it.key==="")
      throw Error("invalid item source/key; source frontier refused");
    if (!input.has(it.source)) input.set(it.source,[]);
    input.get(it.source).push(it);
  }
  const inputDigests=new Map([...input].map(([id,rows])=>[id,digestValue(rows)]));
  const contextDigest=compilerContext(items,args.cms,args.atlas);
  const previous=new Map(), current=new Map(), authored=new Set();
  const db=new Database(args.db,{readonly:true});
  try {
    if (db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_linker_source_generation'").get())
      for (const row of db.query("SELECT source,input_digest,context_digest,relation_digest FROM _linker_source_generation").all())
        previous.set(row.source,row);
    for (const [id,rows] of readLinkerRows(db)) current.set(id,digestRows(rows));
    for (const row of db.query("SELECT src,relation,dst FROM relation WHERE evidence NOT LIKE 'linker:%'").all())
      authored.add(relationKey(row));
  } finally { db.close(); }
  const ids=new Set([...input.keys(),...previous.keys(),...current.keys()]);
  const changed=[...ids].filter(id=>{
    if (!validSource(id)) throw Error("invalid persisted linker source: "+id);
    const before=previous.get(id);
    return !input.has(id) || !before ||
      before.input_digest!==inputDigests.get(id) ||
      before.context_digest!==contextDigest ||
      before.relation_digest!==(current.get(id)||digestRows([]));
  }).sort(compareText);
  if (!changed.length) {
    if (args.out) writeFileSync(args.out,"");
    console.log(JSON.stringify({schema:"bonfyre.linker-source-generation.v1",
      owner_operations:0,source_cuts:0,corpus_sources:input.size,
      avoided_source_compilations:input.size,relations:{},
      status:"unchanged_native_generation_verified"}));
    return;
  }
  const targets=new Set(changed.filter(id=>input.has(id)));
  const compiled=compile(items,args.cms,targets);
  const deduped=new Map();
  for (const {relate:row} of compiled.relations) {
    if (!targets.has(ownerOf(row.src))) throw Error("linker emitted outside selected source");
    if (!authored.has(relationKey(row))) deduped.set(relationKey(row),row);
  }
  const rows=sortRows([...deduped.values()]), grouped=new Map();
  for (const row of rows) {
    const id=ownerOf(row.src);
    if (!grouped.has(id)) grouped.set(id,[]);
    grouped.get(id).push(row);
  }
  const manifest=changed.map(id=>[id,inputDigests.get(id)||"-",
    inputDigests.has(id)?contextDigest:"-",digestRows(grouped.get(id)||[])].join("\t")).join("\n")+"\n";
  const out=args.out??join(tmpdir(),"bonfyre-link-"+randomUUID()+".jsonl");
  const scopes=join(tmpdir(),"bonfyre-link-sources-"+randomUUID()+".tsv");
  try {
    writeFileSync(out,rows.map(row=>JSON.stringify({relate:row})).join("\n")+(rows.length?"\n":""));
    writeFileSync(scopes,manifest,{flag:"wx",mode:0o600});
    const imported=JSON.parse(runAtlas(args.atlas,
      ["import","--db",args.db,"--jsonl",out,"--linker-sources",scopes]));
    if (imported.schema!=="bonfyre.atlas.linker-source-cut.v1" ||
        imported.source_cuts!==changed.length) throw Error("native Atlas atomic cut refused");
    const verify=new Database(args.db,{readonly:true});
    try {
      const ledger=new Map(verify.query("SELECT source,input_digest,context_digest,relation_digest FROM _linker_source_generation").all().map(r=>[r.source,r]));
      const now=readLinkerRows(verify);
      for (const id of changed) {
        const expected=digestRows(grouped.get(id)||[]);
        if (digestRows(now.get(id)||[])!==expected) throw Error("source relation readback mismatch: "+id);
        const row=ledger.get(id);
        if (input.has(id) && (!row || row.input_digest!==inputDigests.get(id) ||
            row.context_digest!==contextDigest || row.relation_digest!==expected))
          throw Error("source generation readback mismatch: "+id);
        if (!input.has(id) && row) throw Error("retired source still tracked: "+id);
      }
    } finally { verify.close(); }
    const counts={};
    for (const row of rows) counts[row.relation]=(counts[row.relation]||0)+1;
    console.log(JSON.stringify({schema:"bonfyre.linker-source-generation.v1",
      status:"native_source_cuts_applied_and_verified",owner:"BonfyreLatentAtlas",
      corpus_sources:input.size,source_cuts:changed.length,owner_operations:1,
      retired_sources:changed.filter(id=>!input.has(id)).length,
      avoided_source_compilations:input.size-targets.size,
      relations:counts,import:imported,
      cannot_infer:"a derived relation is not source truth, access, authority or downstream effect"}));
  } finally {
    if (!args.out) unlinkSync(out);
    unlinkSync(scopes);
  }
}
try { main(); } catch(error) { console.error(error); process.exitCode=1; }
