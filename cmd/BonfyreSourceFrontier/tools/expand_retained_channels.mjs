#!/usr/bin/env bun
// Retained bodies can name candidate channels; they cannot admit sources.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { closeSync, existsSync, openSync, readFileSync, readSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { publisherGrammarPolicy } from "./publisher_grammar_policy.mjs";

let compiledHints;
const urlHints = () => compiledHints ??= publisherGrammarPolicy().retained_url_hints
  .map(({ pattern, family }) => ({ pattern: new RegExp(pattern), family }));

export function familyFor(url, declaredType = "") {
  const type = declaredType.toLowerCase();
  for (const { token, family } of publisherGrammarPolicy().retained_link_types)
    if (type.includes(token)) return family;
  const address = url.toLowerCase();
  for (const { pattern, family } of urlHints()) if (pattern.test(address)) return family;
  return "";
}

export function linkTokens(body) {
  const text = typeof body === "string" ? body : body.toString("utf8");
  return [...text.matchAll(/<(link|atom:link|a|outline)\b[^>]*>/gi)].map(match => {
    const attrs = {};
    for (const part of match[0].matchAll(/([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*(["'])(.*?)\2/gs))
      attrs[part[1].toLowerCase()] = part[3].trim();
    return { kind: match[1].toLowerCase(), raw: match[0], attrs };
  });
}

export function discoveries(body, base, source, twin, bodySha256) {
  const found = new Map();
  const add = (href, declaredFamily, why) => {
    if (!href) return;
    let address;
    try { address = new URL(href.trim(), base).href; } catch { return; }
    if (!/^https?:\/\//.test(address)) return;
    const protocol = declaredFamily || familyFor(address);
    if (!protocol || found.has(address)) return;
    const row = { address, protocol, state: "shadow", truth_level: "observed_link_candidate",
      discovered_from: `${source} via ${twin} (${why})`, body_sha256: bodySha256 };
    const material = publisherGrammarPolicy().retained_material;
    if (Object.hasOwn(material, protocol)) row.material = material[protocol];
    found.set(address, row);
  };
  const text = body.toString("utf8");
  const tokens = linkTokens(text);
  for (const { attrs } of tokens.filter(token => token.kind === "link" || token.kind === "atom:link")) {
    const href = attrs.href, rel = (attrs.rel ?? "").toLowerCase();
    if (!href) continue;
    if (rel === "hub") add(href, "websub_hub", "declared hub");
    else if (["alternate", "self", "service-desc", "describedby", "related", "enclosure"].includes(rel))
      add(href, familyFor(href, attrs.type ?? ""), `rel=${rel}`);
    else add(href, familyFor(href, attrs.type ?? ""), "link tag");
  }
  for (const { attrs } of tokens.filter(token => token.kind === "a"))
    if (/\.ics(?:\?[^"']*)?$/i.test(attrs.href || "")) add(attrs.href, "ics", "calendar link");
  if (body[0] === 123 || body[0] === 91) {
    for (const match of text.slice(0, 2_000_000).matchAll(/"(https?:\/\/[^"\s]{6,400})"/g)) {
      const family = familyFor(match[1]);
      if (family) add(match[1], family, "named in the retained JSON");
    }
  }
  return [...found.values()];
}

function receiptBody(receiptPath) {
  if (!existsSync(receiptPath)) return "";
  const lines = readFileSync(receiptPath, "utf8").split(/\r?\n/).filter(line => line.startsWith("{"));
  try { return JSON.parse(lines.at(-1) ?? "{}").body_path ?? ""; } catch { return ""; }
}

function argsFrom(argv) {
  const args = { selfHosts: "aurekai.work,aurekai.life,bonfyre.local", maxBytes: 4_000_000 };
  const keys = { "--cms": "cms", "--foreign": "foreign", "--out": "out", "--self-hosts": "selfHosts", "--max-bytes": "maxBytes" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["cms", "foreign", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.maxBytes = Number(args.maxBytes);
  if (!Number.isInteger(args.maxBytes) || args.maxBytes < 0) throw new Error("invalid --max-bytes");
  return args;
}

export function expand(args) {
  const mine = new Set(args.selfHosts.split(",").map(host => host.trim().toLowerCase()).filter(Boolean));
  const db = new Database(args.cms, { readonly: true });
  let sources;
  try { sources = db.query("SELECT source, COALESCE(twin,'') AS twin, COALESCE(url,'') AS url FROM internet_source "
    + "WHERE status='published' AND COALESCE(twin,'')<>''").all(); } finally { db.close(); }
  let read = 0, refusedSelf = 0;
  const rows = [], families = {};
  for (const { source, twin, url } of sources) {
    const bodyPath = receiptBody(join(args.foreign, "receipts", `${twin}.json`));
    if (!bodyPath || !existsSync(bodyPath)) continue;
    const fd = openSync(bodyPath, "r");
    let body;
    try {
      const buffer = Buffer.alloc(args.maxBytes);
      body = buffer.subarray(0, readSync(fd, buffer, 0, buffer.length, 0));
    } finally { closeSync(fd); }
    if (!body.length) continue;
    read++;
    const sha = createHash("sha256").update(body).digest("hex");
    for (const row of discoveries(body, url || `https://${twin}`, source, twin, sha)) {
      const host = new URL(row.address).host.toLowerCase().replace(/^www\./, "");
      if ([...mine].some(self => host === self || host.endsWith(`.${self}`))) { refusedSelf++; continue; }
      rows.push(row);
      families[row.protocol] = (families[row.protocol] ?? 0) + 1;
    }
  }
  writeFileSync(args.out, rows.map(row => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : ""));
  return { bodies_read: read, links: rows.length, refused_self_lineage: refusedSelf,
    by_family: Object.fromEntries(Object.entries(families).toSorted((a, b) => b[1] - a[1])) };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(expand(argsFrom(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
