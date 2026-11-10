#!/usr/bin/env bun
// Ask one bounded catalogue, feed and interface convention per host. The
// observer owns robots policy; its receipts are retained even for refusals.
import { createHash } from "node:crypto";
import { Database } from "bun:sqlite";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { closeSync, existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, readSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const execFileAsync = promisify(execFile);
const conventions = [
  { name: "dcat", path: "/data.json", reads: "json", extract: "dataset", title: "title", meta: "description", link: "landingPage" },
  { name: "dcat-catalog", path: "/catalog/data.json", reads: "json", extract: "dataset", title: "title", meta: "description", link: "landingPage" },
  { name: "ckan", path: "/api/3/action/current_package_list_with_resources?limit=50", reads: "json", extract: "result", title: "title", meta: "notes", link: "url" },
  { name: "ckan-search", path: "/api/3/action/package_search?rows=50", reads: "json", extract: "result.results", title: "title", meta: "notes", link: "url" },
  { name: "socrata", path: "/api/views.json?limit=50", reads: "json", extract: "", title: "name", meta: "description", link: "id" },
  { name: "arcgis", path: "/arcgis/rest/services?f=json", reads: "json", extract: "services", title: "name", meta: "type", link: "url" },
  { name: "arcgis-hub", path: "/api/v3/datasets", reads: "json", extract: "data", title: "id", meta: "type", link: "id" },
  ...[["feed", "/feed"], ["feed-xml", "/feed.xml"], ["rss-xml", "/rss.xml"], ["atom-xml", "/atom.xml"],
    ["index-xml", "/index.xml"], ["rss-feed", "/rss"], ["blog-feed", "/blog/feed"], ["news-feed", "/news/feed"]]
    .map(([name, path]) => ({ name, path, reads: "rss" })),
  { name: "json-feed", path: "/feed.json", reads: "json", extract: "items", title: "title", meta: "summary", link: "url" },
  ...[["openapi", "/openapi.json"], ["openapi-v3", "/v3/api-docs"], ["swagger", "/swagger.json"],
    ["openapi-wellknown", "/.well-known/openapi.json"], ["nodeinfo", "/.well-known/nodeinfo"]]
    .map(([name, path]) => ({ name, path, reads: "capability" })),
];
const feedish = /<(rss|feed|rdf:RDF)\b/i;
const feedItem = /<(item|entry)\b/gi;

export function sourceId(host, kind) {
  const flat = host.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 40);
  return `${kind}-${flat}-${createHash("sha256").update(host).digest("hex").slice(0, 6)}`;
}
function hostOf(address) {
  try { return new URL(address).hostname.toLowerCase().replace(/\.$/, "").replace(/^www\./, ""); }
  catch { return ""; }
}
export function rowsAt(doc, extract) {
  let node = doc;
  for (const step of extract ? extract.split(".") : []) {
    if (!node || typeof node !== "object" || Array.isArray(node) || !(step in node)) return null;
    node = node[step];
  }
  return Array.isArray(node) ? node : null;
}
function options(argv) {
  const args = { census: "", project: join(process.env.HOME ?? "", "Developer/Bonfyre/components/BonfyreProject/bonfyre-project"),
    foreign: process.env.BONFYRE_FOREIGN_ROOT ?? join(process.env.HOME ?? "", ".bonfyre/estate-local/foreign"),
    limit: 400, workers: 8, minRows: 5 };
  const keys = { "--hosts": "hosts", "--out": "out", "--census": "census", "--project": "project", "--foreign": "foreign",
    "--limit": "limit", "--workers": "workers", "--min-rows": "minRows" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["hosts", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  for (const key of ["limit", "workers", "minRows"]) {
    args[key] = Number(args[key]);
    if (!Number.isInteger(args[key]) || args[key] < 0) throw new Error(`invalid ${key}`);
  }
  return args;
}

// The body is transient sampling material. The observation receipt is not:
// unlike the Python carrier, this keeps the receipt under ForeignTwin.
export async function ask(host, convention, args) {
  const address = `https://${host}${convention.path}`;
  const ident = sourceId(host, convention.name);
  const temp = mkdtempSync(join(tmpdir(), "bonfyre-convention-probe-"));
  const body = join(temp, "answer.body");
  const receipts = join(args.foreign, "receipts");
  mkdirSync(receipts, { recursive: true });
  const receiptPath = join(receipts, `${ident}-${Date.now()}-${process.pid}.json`);
  let said = "";
  try {
    try {
      const result = await execFileAsync(args.project,
        ["foreign", "observe", ident, address, "--observe-right", "public", "--body", body, "--receipt", receiptPath],
        { timeout: 60000, maxBuffer: 1024 * 1024 });
      said = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    } catch (error) { said = `${error.stdout ?? ""}${error.stderr ?? ""}${error.message ?? ""}`; }
    let receipt = {};
    if (existsSync(receiptPath)) {
      const lines = readFileSync(receiptPath, "utf8").split(/\r?\n/).filter(line => line.startsWith("{"));
      try { receipt = JSON.parse(lines.at(-1) ?? "{}"); } catch {}
    }
    let text = "";
    if (existsSync(body) && receipt.policy_decision === "allow"
      && Number(receipt.http_status ?? 0) >= 200 && Number(receipt.http_status ?? 0) < 300) {
      const fd = openSync(body, "r");
      try {
        const bytes = Buffer.alloc(600000);
        text = bytes.subarray(0, readSync(fd, bytes, 0, bytes.length, 0)).toString("utf8");
      } finally { closeSync(fd); }
    }
    return { address, text, said, receipt, receiptPath };
  } finally { rmSync(temp, { recursive: true, force: true }); }
}

function judge(answer, host, convention, minRows, tally) {
  const { address, text, said, receipt } = answer;
  if (!text.trim()) {
    tally[receipt.policy_decision && receipt.policy_decision !== "allow" || /robots/i.test(said) ? "robots_denied" : "no_answer"]++;
    return null;
  }
  let reading = null;
  let seenRows = 0;
  if (convention.reads === "rss") {
    if (!feedish.test(text.slice(0, 4000))) { tally.unusable++; return null; }
    seenRows = [...text.matchAll(feedItem)].length;
    if (seenRows < minRows) { tally.unusable++; return null; }
    reading = { extract: "rss:item", titleField: "title", metaField: "", linkField: "link",
      family: "news", kind: "text", cadence: 1800 };
  } else {
    let doc;
    try { doc = JSON.parse(text); } catch { tally.unusable++; return null; }
    if (convention.reads === "capability") {
      if (!doc || typeof doc !== "object" || Array.isArray(doc) || !(doc.openapi || doc.swagger || doc.links || doc.paths)) {
        tally.unusable++; return null;
      }
    } else {
      const rows = rowsAt(doc, convention.extract);
      if (!rows || rows.length < minRows || !rows.find(row => row && typeof row === "object" && !Array.isArray(row))?.[convention.title]) {
        tally.unusable++; return null;
      }
      seenRows = rows.length;
      reading = { extract: convention.extract, titleField: convention.title, metaField: convention.meta ?? "",
        linkField: convention.link ?? "", family: "datasets", kind: "text", cadence: 86400 };
    }
  }
  const channel = { root: host, address, protocol: convention.name, title: `${convention.name} at ${host}`,
    truth_level: "protocol_derived_candidate",
    discovered_from: `asked at the address a ${convention.name} is conventionally kept at` };
  const row = !reading ? null : {
    family: reading.family, source: sourceId(host, convention.name), label: `${host} ${reading.family}`,
    says: `what ${host} publishes at its ${convention.name}`, note: `${convention.name}, read from the address the host answers at`,
    url: address, twin: sourceId(host, convention.name), tier: "WARM", kind: reading.kind,
    cadence_seconds: reading.cadence, status: "published",
    reading: { extract: reading.extract, title_field: reading.titleField, meta_field: reading.metaField, link_field: reading.linkField },
    publisher_ident: "", publisher_name: host, compiled_by: "convention-probe", rows_seen: seenRows,
  };
  return [channel, row];
}

async function mapLimit(items, limit, fn) {
  const output = new Array(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(items.length, Math.max(1, limit)) }, async () => {
    while (next < items.length) { const index = next++; output[index] = await fn(items[index]); }
  }));
  return output;
}

export async function probe(args, { askFn = ask } = {}) {
  const seen = new Set();
  const hosts = [];
  for (const line of readFileSync(args.hosts, "utf8").split(/\r?\n/)) {
    let host = line.includes("://") ? hostOf(line.trim()) : line.trim().toLowerCase().replace(/\.$/, "").replace(/^www\./, "");
    if (!/^[a-z0-9.-]+\.[a-z]{2,}$/.test(host) || seen.has(host)) continue;
    seen.add(host);
    hosts.push(host);
    if (hosts.length >= args.limit) break;
  }
  const tally = { no_answer: 0, unusable: 0, robots_denied: 0 };
  const answers = await mapLimit(hosts, args.workers, async host => {
    const found = [];
    for (const category of ["json", "rss", "capability"]) {
      for (const convention of conventions) {
        if (convention.reads !== category) continue;
        const got = judge(await askFn(host, convention, args), host, convention, args.minRows, tally);
        if (!got) continue;
        found.push(got);
        break;
      }
    }
    return found;
  });
  const found = answers.flat(1).map(pair => pair[0]);
  const census = answers.flat(1).map(pair => pair[1]).filter(Boolean);
  const byProtocol = {};
  for (const channel of found) byProtocol[channel.protocol] = (byProtocol[channel.protocol] ?? 0) + 1;
  return { found, census, report: { hosts_asked: hosts.length, answered: found.length, readable_sources: census.length,
    by_protocol: byProtocol, did_not_answer: tally.no_answer, answered_with_nothing_usable: tally.unusable,
    robots_denied: tally.robots_denied, rows_listed: census.reduce((sum, row) => sum + row.rows_seen, 0),
    cannot_infer: "a catalogue answering here is a host publishing a list of datasets; it is not permission to use them, not a claim about what any dataset contains, and not a relationship with the publisher" } };
}

export function recordAnswers(dbPath, hostsPath, channelsPath) {
  const answered = new Set();
  if (existsSync(channelsPath)) for (const line of readFileSync(channelsPath, "utf8").split(/\r?\n/)) {
    try { answered.add(JSON.parse(line).root); } catch {}
  }
  const hosts = readFileSync(hostsPath, "utf8").split(/\r?\n/).map(host => host.trim()).filter(Boolean);
  const db = new Database(dbPath);
  try {
    db.exec("CREATE TABLE IF NOT EXISTS asked(host TEXT PRIMARY KEY, at TEXT NOT NULL, answered INTEGER NOT NULL DEFAULT 0)");
    const put = db.query("INSERT OR REPLACE INTO asked(host,at,answered) VALUES(?,?,?)");
    const now = new Date().toISOString().replace(/\.\d+Z$/, "Z");
    db.transaction(() => { for (const host of hosts) put.run(host, now, answered.has(host) ? 1 : 0); })();
    return { asked: hosts.length, answered: hosts.filter(host => answered.has(host)).length };
  } finally { db.close(); }
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2);
    if (argv[0] === "record-answers") {
      const selected = {};
      for (let index = 1; index < argv.length; index += 2) selected[argv[index]] = argv[index + 1];
      for (const flag of ["--db", "--hosts", "--channels"]) if (!selected[flag]) throw new Error(`missing ${flag}`);
      console.log(JSON.stringify(recordAnswers(selected["--db"], selected["--hosts"], selected["--channels"])));
    } else {
      const args = options(argv);
      const result = await probe(args);
      writeFileSync(args.out, result.found.map(row => JSON.stringify(row)).join("\n") + (result.found.length ? "\n" : ""));
      if (args.census) writeFileSync(args.census, JSON.stringify(result.census, null, 2) + "\n");
      console.log(JSON.stringify(result.report));
    }
  } catch (error) { console.error(error); process.exitCode = 1; }
}
