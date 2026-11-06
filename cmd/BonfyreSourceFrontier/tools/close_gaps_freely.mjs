#!/usr/bin/env bun
// Free public identity and head-link evidence closes ledger gaps, not authority.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { appendFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { observePublic } from "./foreign_observation.mjs";

const qlever = "https://qlever.dev/api/wikidata?query=";
const validHost = host => /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/i.test(host) && !host.includes("..");
const feedTypes = { "application/rss+xml": "rss", "application/atom+xml": "atom", "application/feed+json": "json_feed", "text/calendar": "ics" };
const queryFor = hosts => `PREFIX wd: <http://www.wikidata.org/entity/>\nPREFIX wdt: <http://www.wikidata.org/prop/direct/>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT ?item (SAMPLE(?label) AS ?name) (SAMPLE(?site) AS ?website) WHERE {\n  ?item wdt:P856 ?site ; rdfs:label ?label .\n  FILTER(LANG(?label) = "en")\n  VALUES ?site { ${hosts.map(host => `<https://${host}/> <http://${host}/> <https://${host}> <http://${host}>`).join(" ")} }\n} GROUP BY ?item LIMIT 200`;

function gapRows(dbPath, kind, limit) {
  const db = new Database(dbPath, { readonly: true });
  try {
    const order = kind === "IDENTITY"
      ? "ORDER BY (SELECT COALESCE(SUM(c.contributions),0) FROM channel c WHERE c.root_id=r.root_id) DESC, "
        + "(SELECT COUNT(*) FROM channel c2 WHERE c2.root_id=r.root_id) DESC"
      : "ORDER BY (SELECT COALESCE(SUM(c.contributions),0) FROM channel c WHERE c.root_id=r.root_id) DESC";
    return db.query("SELECT r.root_id, r.host, COALESCE(r.name,'') AS name FROM root r "
      + "JOIN gap g ON g.root_id=r.root_id AND g.kind=? AND g.status='open' " + order + " LIMIT ?").all(kind, limit);
  } finally { db.close(); }
}

export function machineChannels(text, base) {
  const rows = [];
  for (const tag of text.match(/<link\b[^>]*>/gi) ?? []) {
    const attrs = {};
    for (const match of tag.matchAll(/(\w[\w:-]*)\s*=\s*["']([^"']*)["']/g)) attrs[match[1].toLowerCase()] = match[2];
    const protocol = feedTypes[(attrs.type ?? "").toLowerCase()];
    if (!protocol || !attrs.href) continue;
    try {
      const address = new URL(attrs.href, base).href;
      if (/^https?:\/\//.test(address)) rows.push([address, protocol, (attrs.title ?? "").slice(0, 200)]);
    } catch {}
  }
  return rows;
}

export function wikidataAnswers(text) {
  let doc;
  try { doc = JSON.parse(text); } catch { return new Map(); }
  const found = new Map();
  for (const binding of doc?.results?.bindings ?? []) {
    const item = binding.item?.value ?? "", name = binding.name?.value ?? "", site = binding.website?.value ?? "";
    const qid = item.split("/").at(-1);
    let host = "";
    try { host = new URL(site).host.toLowerCase().replace(/^www\./, ""); } catch {}
    if (/^Q[0-9]+$/.test(qid) && validHost(host) && !found.has(host)) found.set(host, [qid, name, site]);
  }
  return found;
}

function options(argv) {
  const args = { limit: 60, evidence: "/dev/null" };
  const keys = { "--db": "db", "--frontier": "frontier", "--project": "project", "--foreign": "foreign", "--limit": "limit",
    "--channels": "channels", "--evidence": "evidence" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["db", "frontier", "project", "foreign"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.limit = Number(args.limit);
  if (!Number.isInteger(args.limit) || args.limit < 0) throw new Error("invalid --limit");
  args.channels ??= join(dirname(resolve(args.db)), "free-channels.jsonl");
  return args;
}
function nativeImport(argv) {
  const result = Bun.spawnSync(argv, { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).slice(0, 300));
}

export async function closeGaps(args, { observeFn = observePublic, importFn = nativeImport, sleepFn = Bun.sleep } = {}) {
  const evidence = entry => appendFileSync(args.evidence, JSON.stringify(entry) + "\n");
  const identityRoots = gapRows(args.db, "IDENTITY", args.limit), answers = new Map();
  for (let start = 0; start < identityRoots.length; start += 40) {
    const batch = identityRoots.slice(start, start + 40).map(row => row.host).filter(validHost);
    if (!batch.length) continue;
    const url = qlever + encodeURIComponent(queryFor(batch));
    const observation = await observeFn(args.project, args.foreign,
      `gapclose-${createHash("sha256").update(url).digest("hex").slice(0, 16)}`, url,
      { maxBytes: 3_000_000, timeout: 120000 });
    evidence({ asked: "wikidata official website", hosts: batch.length, http_status: observation.receipt.http_status ?? null,
      policy: observation.receipt.policy_decision ?? null, receipt: observation.receiptPath });
    for (const [host, identity] of wikidataAnswers(observation.text)) answers.set(host, identity);
    await sleepFn(500);
  }
  const identities = [];
  for (const { host, name } of identityRoots) {
    const answer = answers.get(host);
    if (!answer) continue;
    const [qid, label, site] = answer;
    identities.push({ address: `https://${host}/`, identity: qid, name: label || name, state: "candidate",
      discovered_from: `wikidata says ${site} is the official website of ${qid}` });
  }
  if (identities.length) {
    const listing = join(dirname(args.channels), "free-identities.jsonl");
    writeFileSync(listing, identities.map(row => JSON.stringify(row)).join("\n") + "\n");
    importFn([args.frontier, "import", "--db", args.db, "--label", "FREE_BACKFILL", "--jsonl", listing]);
  }

  const channels = [];
  for (const { host } of gapRows(args.db, "STRUCTURED_CHANNEL", args.limit)) {
    if (!validHost(host)) continue;
    const url = `https://${host}/`;
    const observation = await observeFn(args.project, args.foreign,
      `gapchan-${createHash("sha256").update(url).digest("hex").slice(0, 16)}`, url,
      { maxBytes: 3_000_000 });
    const links = machineChannels(observation.text, url);
    evidence({ host, http_status: observation.receipt.http_status ?? null, policy: observation.receipt.policy_decision ?? null,
      receipt: observation.receiptPath, machine_channels: links.length });
    for (const [address, protocol, title] of links.slice(0, 6)) channels.push({ address, protocol, title, state: "shadow",
      truth_level: "observed_link_candidate", material: protocol === "ics" ? "event" : "publication",
      discovered_from: `linked in the head of ${url}` });
    await sleepFn(300);
  }
  writeFileSync(args.channels, channels.map(row => JSON.stringify(row)).join("\n") + (channels.length ? "\n" : ""));
  if (channels.length) importFn([args.frontier, "import", "--db", args.db, "--label", "FREE_BACKFILL", "--jsonl", args.channels]);
  return { identities_closed: identities.length, machine_channels_found: channels.length,
    cannot_infer: "a named root and a shadow channel are bookkeeping, not admission; no page shows either until the census admits it" };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await closeGaps(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
