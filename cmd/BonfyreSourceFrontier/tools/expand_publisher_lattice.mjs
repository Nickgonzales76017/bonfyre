#!/usr/bin/env bun
// Compile a publisher's retained body into shadow channels; no network or admission.
import { Database } from "bun:sqlite";
import { createReadStream, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createGunzip } from "node:zlib";
import { grammarOf } from "./sweep_root_grammars.mjs";
import { publisherGrammar, publisherGrammarPolicy } from "./publisher_grammar_policy.mjs";

async function boundedBody(path, cap = 3_000_000) {
  const source = createReadStream(path);
  const stream = path.endsWith(".gz") ? source.pipe(createGunzip()) : source;
  const chunks = [];
  let length = 0;
  try {
    for await (const chunk of stream) {
      const take = Math.min(chunk.length, cap - length);
      if (take > 0) { chunks.push(chunk.subarray(0, take)); length += take; }
      if (length >= cap) { stream.destroy(); break; }
    }
  } catch { return Buffer.alloc(0); }
  finally { source.destroy(); }
  return Buffer.concat(chunks, length);
}

export async function bodyOf(foreign, twin, cap = 3_000_000) {
  const receipt = join(foreign, "receipts", `${twin}.json`);
  if (!existsSync(receipt)) return Buffer.alloc(0);
  const lines = readFileSync(receipt, "utf8").split(/\r?\n/).filter(line => line.startsWith("{"));
  let path;
  try { path = JSON.parse(lines.at(-1) ?? "{}").body_path; } catch { return Buffer.alloc(0); }
  for (const candidate of [path, `${path ?? ""}.gz`]) if (candidate && existsSync(candidate)) return boundedBody(candidate, cap);
  return Buffer.alloc(0);
}

export function channelsFromBody(body, source, rootUrl, perRoot = 40) {
  const base = new URL("/", rootUrl).href;
  const host = new URL(base).host.toLowerCase().replace(/^www\./, "");
  const text = body.toString("utf8"), rows = [], seen = new Set();
  const emit = (address, protocol, material, truth, why) => {
    if (rows.length >= perRoot || seen.has(address) || !/^https?:\/\//.test(address)) return;
    seen.add(address);
    const row = { address, protocol, state: "shadow", truth_level: truth, discovered_from: `${source}: ${why}` };
    if (material) row.material = material;
    rows.push(row);
  };
  for (const tag of text.match(/<link\b[^>]*>/gi) ?? []) {
    const attrs = {};
    for (const match of tag.matchAll(/(\w[\w:-]*)\s*=\s*["']([^"']*)["']/g)) attrs[match[1].toLowerCase()] = match[2];
    const types = publisherGrammarPolicy().feed_types;
    const mime = (attrs.type ?? "").toLowerCase();
    const kind = Object.hasOwn(types, mime) ? types[mime] : undefined;
    if (!kind || !attrs.href) continue;
    try { emit(new URL(attrs.href, base).href, kind, kind === "ics" ? "event" : "publication", "observed_link_candidate", "linked in its own head"); }
    catch {}
  }
  const [grammar] = grammarOf(text);
  if (grammar) for (const route of publisherGrammar(grammar)?.retained ?? []) {
    emit(new URL(route.path, base).href, route.protocol, route.material, "protocol_derived_candidate",
      `this root publishes with ${grammar}, which addresses ${route.path}`);
  }
  if (publisherGrammarPolicy().archive_grammars.includes(grammar)) {
    const archivePattern = /href=["'](https?:\/\/[^"']*\/(?:category|tag|author|topic|section)\/[a-z0-9][a-z0-9\-/]{1,60}\/?)["']/gi;
    for (const match of [...text.matchAll(archivePattern)].slice(0, perRoot)) {
      const archive = match[1].replace(/\/+$/, "");
      if (new URL(archive).host.toLowerCase().replace(/^www\./, "") !== host) continue;
      emit(`${archive}/feed/`, "rss", "publication", "protocol_derived_candidate",
        "an editorial archive this root links, whose feed its grammar addresses");
    }
  }
  return { grammar: grammar ?? "", rows };
}

function options(argv) {
  const args = { selfHosts: "aurekai.work,aurekai.life", perRoot: 40 };
  const keys = { "--cms": "cms", "--foreign": "foreign", "--out": "out", "--self-hosts": "selfHosts", "--per-root": "perRoot" };
  for (let i = 0; i < argv.length; i++) {
    const key = keys[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["cms", "foreign", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.perRoot = Number(args.perRoot);
  if (!Number.isInteger(args.perRoot) || args.perRoot < 0) throw new Error("invalid --per-root");
  return args;
}

export async function expand(args) {
  const mine = new Set(args.selfHosts.split(",").map(host => host.trim().toLowerCase()).filter(Boolean));
  const db = new Database(args.cms, { readonly: true });
  let sources;
  try { sources = db.query("SELECT source, COALESCE(twin,'') AS twin, COALESCE(url,'') AS url FROM internet_source "
    + "WHERE status='published' AND COALESCE(twin,'')<>'' AND url LIKE 'http%'").all(); }
  finally { db.close(); }
  const rows = [], grammars = {};
  let roots = 0;
  for (const { source, twin, url } of sources) {
    // Retained bodies are read serially to bound memory across the source estate.
    // eslint-disable-next-line no-await-in-loop
    const body = await bodyOf(args.foreign, twin);
    if (!body.length) continue;
    const host = new URL(url).host.toLowerCase().replace(/^www\./, "");
    if ([...mine].some(self => host === self || host.endsWith(`.${self}`))) continue;
    roots++;
    const result = channelsFromBody(body, source, url, args.perRoot);
    rows.push(...result.rows);
    if (result.grammar) grammars[result.grammar] = (grammars[result.grammar] ?? 0) + 1;
  }
  writeFileSync(args.out, rows.map(row => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : ""));
  return { roots_read: roots, channels: rows.length, grammars,
    cannot_infer: "a grammar's convention is not an endpoint that answers, and a shadow channel is not a source" };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await expand(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
