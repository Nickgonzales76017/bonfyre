#!/usr/bin/env bun
// Resolve a published program's stated source demand through SourceFrontier.
// Listed candidates never become collection members until they answer.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { resolveCollection } from "../cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs";

const timestamp = () => new Date().toISOString().replace(/\.\d+Z$/, "Z");
const runStamp = () => timestamp().replace(/[-:]/g, "") + `-${process.pid}`;
function options(argv) {
  const args = { estate: join(process.env.HOME ?? "", ".bonfyre/estate-local"), repo: resolve(dirname(fileURLToPath(import.meta.url)), ".."),
    needs: "", queries: 8, perQuery: 25, apply: false, discover: false, namespace: "root" };
  const keys = { "--estate": "estate", "--repo": "repo", "--instance": "instance", "--needs": "needs",
    "--queries": "queries", "--per-query": "perQuery", "--namespace": "namespace", "--address-index": "addressIndex", "--cms-db": "cmsDb" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--discover") { args.discover = true; continue; }
    if (argv[i] === "--apply") { args.apply = true; continue; }
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  if (!args.instance) throw new Error("missing --instance");
  for (const key of ["queries", "perQuery"]) {
    args[key] = Number(args[key]);
    if (!Number.isInteger(args[key]) || args[key] < 0) throw new Error(`invalid ${key}`);
  }
  return args;
}
function nativeRun(argv) {
  const result = Bun.spawnSync(argv, { stdout: "pipe", stderr: "pipe" });
  return { exitCode: result.exitCode, stdout: new TextDecoder().decode(result.stdout), stderr: new TextDecoder().decode(result.stderr) };
}
function requireRun(runFn, argv) {
  const result = runFn(argv);
  if (result.exitCode !== 0) throw new Error(`${basename(argv[0])} exited ${result.exitCode}: ${result.stderr.slice(0, 300)}`);
  return result;
}
function lastLine(text) { return text.trim().split(/\r?\n/).at(-1) ?? ""; }
function size(path) { try { return statSync(path).size; } catch { return 0; } }

export function resolveProgram(args, { runFn = nativeRun } = {}) {
  const cmsPath = args.cmsDb ?? join(args.estate, "cms.db");
  const cms = new Database(cmsPath, args.apply ? { readwrite: true } : { readonly: true });
  const namespace = args.namespace ?? "root";
  try {
    const hasDemands=cms.query("PRAGMA table_info(program_template)").all().some(c=>c.name==="address_demands");
    const demandColumn=hasDemands?"t.address_demands":"NULL";
    const program = cms.query("SELECT i.archetype, i.owner_subject, t.name, i.template_id, "+demandColumn+" AS address_demands FROM program_instance i "
      + "LEFT JOIN program_template t ON t.template_id=i.template_id AND t.namespace=i.namespace AND t.status='published' "
      + "WHERE i.instance_id=? AND i.namespace=? AND i.status='published'").get(args.instance, namespace);
    if (!program) return { report: { error: "no such program instance", instance: args.instance }, exitCode: 2 };
    const collection = args.instance + '-sources';
    const existing = cms.query("SELECT source_selector FROM source_collection WHERE collection_id=? AND namespace=? AND status='published'").get(collection,namespace);
    if(!existing)throw Error('published source collection missing: '+collection);
    const selector = JSON.parse(existing.source_selector || '{}');
    if(!selector||Array.isArray(selector)||typeof selector!=='object'||(selector.source_refs!==undefined&&!Array.isArray(selector.source_refs)))throw Error('invalid source selector');
    if(selector.template_ref&&selector.template_ref!==program.template_id)throw Error("source collection references another template");
    const index=args.addressIndex??process.env.BONFYRE_ADDRESS_INDEX??join(process.env.HOME??'','.bonfyre/estate/estate.db');
    const resolved=resolveCollection(selector,program.address_demands?JSON.parse(program.address_demands):undefined,
      {cms,namespace,index,binary:join(args.repo,'cmd/BonfyreExternalEstate/bonfyre-external-estate'),runFn});
    const {local,contracts,sourceRefs:localSources,candidateRefs,selector:changed}=resolved;
    const needs=resolved.discoveryQueries;
    if(!contracts.length||local.resolutions.some(r=>!r.candidates.length))needs.push(...String(args.needs??'').split(/[\n;]/).map(s=>s.trim()).filter(Boolean));
    const store = value => cms.transaction(()=>{
      const current=cms.query("SELECT source_selector FROM source_collection WHERE collection_id=? AND namespace=? AND status='published'").get(collection,namespace);
      if(!current||current.source_selector!==existing.source_selector)throw Error('collection changed during resolution; retry against current owner state');
      const updated=JSON.stringify(value);
      if(updated!==existing.source_selector)cms.query("UPDATE source_collection SET source_selector=?,updated_at=? WHERE collection_id=? AND namespace=? AND status='published'").run(updated,timestamp(),collection,namespace);
      // Discovery does not own acceptance. Existing requirement decisions are
      // preserved; no candidate or source publication can mark one met.
    })();
    const base={instance:args.instance,program:program.name,archetype:program.archetype,local_resolution:local,
      published_source_refs:localSources,candidate_refs:candidateRefs,requirements_satisfied:0,residual_discovery:needs,
      cannot_infer:'Candidate matching is not source admission, useful material, authority or requirement satisfaction.'};
    if(!args.apply)return {report:{...base,would_ask_for:needs},exitCode:0};
    if(!args.discover||!needs.length){store(changed);return {report:{...base,discovery_started:false,sources_now_in_the_collection:changed.source_refs.length},exitCode:0};}

    const label = `PROGRAM_${args.instance.toUpperCase().replace(/[^A-Z0-9]+/g, "_").slice(0, 56)}`;
    const discovery = join(args.estate, "discovery");
    mkdirSync(discovery, { recursive: true });
    const runId = `${args.instance.replace(/[^a-z0-9]+/g, "-").slice(0, 40)}-${runStamp()}`;
    const found = join(discovery, `program-${runId}.jsonl`);
    const firstOut = join(discovery, `program-${runId}-qualified.json`);
    const declared = join(discovery, `program-${runId}-declared.jsonl`);
    const frontierDb = join(args.estate, "frontier.db");
    const foreign = join(args.estate, "foreign");
    const schemas = join(args.estate, "schemas");
    const frontier = join(args.repo, "cmd/BonfyreSourceFrontier/bonfyre-source-frontier");
    const qualifier = join(args.repo, "cmd/BonfyreSourceFrontier/tools/qualify_channels.mjs");
    const applySources = join(args.repo, "cmd/BonfyreWebAddress/tools/apply_census_sources.mjs");
    const discover = requireRun(runFn, ["bun", join(args.repo, "cmd/BonfyreSourceFrontier/tools/discover_with_exa.mjs"),
      "--out", found, "--subjects", needs.join("; "), "--queries", String(args.queries), "--per-query", String(args.perQuery)]);
    const listed = existsSync(found) ? readFileSync(found, "utf8").split(/\r?\n/).filter(Boolean).length : 0;
    if (!listed) { store(changed); return { report: { instance: args.instance, asked_for: needs, listed: 0,
      why: (discover.stderr || discover.stdout).trim().slice(0, 160),
      local_resolution:local,collection_left:"existing references preserved; discovery returned no candidates" }, exitCode: 0 }; }

    requireRun(runFn, [frontier, "import", "--db", frontierDb, "--label", label, "--jsonl", found]);
    const qualifyBase = ["--db", frontierDb, "--frontier", frontier,
      "--project", join(args.repo, "components/BonfyreProject/bonfyre-project"), "--foreign", foreign,
      "--limit", "80", "--workers", "8", "--cms", cmsPath];
    const qualified = requireRun(runFn, ["bun", qualifier, ...qualifyBase, "--out", firstOut,
      "--only-label", label, "--discovered", declared, "--evidence", join(discovery, `program-${runId}-evidence.jsonl`)]);
    let second = {};
    const apply = path => requireRun(runFn, ["bun", applySources, "--json", path, "--db", cmsPath,
      "--schemas", schemas, "--cms", join(args.repo, "cmd/BonfyreCMS/bonfyre-cms")]);
    if (size(declared)) {
      requireRun(runFn, [frontier, "import", "--db", frontierDb, "--label", `${label}_DECLARED`, "--jsonl", declared]);
      const secondOut = `${firstOut}.declared`;
      const again = requireRun(runFn, ["bun", qualifier, ...qualifyBase, "--out", secondOut,
        "--only-label", `${label}_DECLARED`, "--evidence", join(discovery, `program-${runId}-declared-evidence.jsonl`)]);
      try { second = JSON.parse(lastLine(again.stdout) || "{}"); } catch {}
      if (size(secondOut)) apply(secondOut);
    }
    if (size(firstOut)) apply(firstOut);

    const ledger = new Database(frontierDb, { readonly: true });
    let admitted;
    try {
      admitted = ledger.query("SELECT DISTINCT c.census_source FROM channel c JOIN channel_origin o ON o.channel_id=c.channel_id "
        + "WHERE o.label IN (?,?) AND c.state IN ('eligible','admitted','active') AND COALESCE(c.census_source,'')<>''")
        .all(label, `${label}_DECLARED`).map(row => row.census_source);
    } finally { ledger.close(); }
    changed.source_refs=[...new Set([...changed.source_refs,...admitted])].sort();
    // Newly discovered/admitted members are not a local-address projection;
    // do not remove them on a later local-only refresh.
    if(changed.source_refs.length>300)throw Error('collection capacity exceeded; no references silently dropped');
    changed.asked_for=needs;
    store(changed);
    return { exitCode: 0, report: { instance: args.instance, program: program.name, archetype: program.archetype,
      asked_for: needs.length, listed, first_pass: [lastLine(qualified.stdout)].filter(Boolean), declared_pass: second,
      sources_now_in_the_collection: changed.source_refs.length, local_resolution:local, requirements_satisfied:0,
      cannot_infer: "a source in this collection is one this estate reads that answered a question this program asked; it is not evidence the program has what it needs, and not a claim the source is about this program at all" } };
  } finally { cms.close(); }
}

if (import.meta.main) {
  try { const result = resolveProgram(options(process.argv.slice(2))); console.log(JSON.stringify(result.report, null, 2)); process.exitCode = result.exitCode; }
  catch (error) { console.error(error); process.exitCode = 1; }
}
