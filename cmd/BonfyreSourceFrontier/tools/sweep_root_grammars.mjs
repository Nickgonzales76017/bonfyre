#!/usr/bin/env bun
// A publisher's own page may declare channels or prove a publishing grammar.
// Both are candidate listings; only the census can later admit a source.
import { Database } from "bun:sqlite";
import { appendFileSync, existsSync, mkdirSync, openSync, closeSync, readFileSync, readSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { publisherGrammar, publisherGrammarPolicy } from "./publisher_grammar_policy.mjs";

const execFileAsync = promisify(execFile);
const generator = /<meta[^>]+name=["']generator["'][^>]+content=["']([^"']+)/i;
const alternate = /<link\b[^>]*?rel=["']?alternate["']?[^>]*?>/gi;
const linkType = /type=["']?(application\/(?:rss\+xml|atom\+xml|feed\+json|json))/i;
const linkHref = /href=["']([^"']+)["']/i;
const linkTitle = /title=["']([^"']*)["']/i;
export function grammarOf(text) {
  const said = generator.exec(text);
  if (said) {
    const name = said[1].toLowerCase();
    for (const grammar of publisherGrammarPolicy().grammars)
      if (name.includes(grammar.id)) return [grammar.id, "its own generator says so"];
  }
  const lower = text.toLowerCase();
  for (const grammar of publisherGrammarPolicy().grammars)
    if (grammar.markers.some(mark => lower.includes(mark.toLowerCase())))
      return [grammar.id, `its body carries ${grammar.markers[0]}`];
  return [null, ""];
}

export function declaredFeeds(text, pageUrl) {
  const found = [];
  const seen = new Set();
  for (const tag of text.slice(0, 400000).match(alternate) ?? []) {
    const kind = linkType.exec(tag);
    const href = linkHref.exec(tag);
    if (!kind || !href) continue;
    let address;
    try { address = new URL(unescapeHtml(href[1].trim()), pageUrl).href; } catch { continue; }
    if (!/^https?:\/\//i.test(address) || seen.has(address)) continue;
    seen.add(address);
    found.push([address, kind[1].toLowerCase().includes("json") ? "json_feed" : "rss", unescapeHtml(linkTitle.exec(tag)?.[1] ?? "").trim()]);
  }
  return found;
}

function unescapeHtml(value) {
  return value.replace(/&(#x[0-9a-f]+|#[0-9]+|amp|quot|apos|lt|gt);/gi, (_match, entity) => {
    const named = { amp: "&", quot: '"', apos: "'", lt: "<", gt: ">" };
    if (entity[0] !== "#") return named[entity.toLowerCase()] ?? _match;
    const code = entity[1].toLowerCase() === "x" ? parseInt(entity.slice(2), 16) : parseInt(entity.slice(1), 10);
    return Number.isFinite(code) && code <= 0x10ffff ? String.fromCodePoint(code) : _match;
  });
}

export async function observe(project, foreign, twin, url, timeout = 45000, maxBytes = 2_000_000) {
  const receipts = join(foreign, "receipts");
  const bodies = join(foreign, "bodies");
  mkdirSync(receipts, { recursive: true });
  mkdirSync(bodies, { recursive: true });
  const receipt = join(receipts, `${twin}.json`);
  const body = join(bodies, `${twin}.body`);
  try { await execFileAsync(project, ["foreign", "observe", twin, url, "--receipt", receipt, "--body", body, "--observe-right", "public"],
    { timeout, maxBuffer: 1024 * 1024 }); } catch {}
  let last = {};
  if (existsSync(receipt)) {
    const lines = readFileSync(receipt, "utf8").split(/\r?\n/).filter(line => line.startsWith("{"));
    try { last = JSON.parse(lines.at(-1) ?? "{}"); } catch {}
  }
  let text = "";
  if (existsSync(body) && Number(last.http_status ?? 0) >= 200 && Number(last.http_status ?? 0) < 300) {
    const fd = openSync(body, "r");
    try {
      const bytes = Buffer.alloc(maxBytes);
      text = bytes.subarray(0, readSync(fd, bytes, 0, bytes.length, 0)).toString("utf8");
    } finally { closeSync(fd); }
  }
  try { unlinkSync(body); } catch {}
  return [last, text];
}

function options(argv) {
  const args = { limit: 3000, workers: 12, perRoot: 24, evidence: "", universal: false };
  const keys = { "--db": "db", "--project": "project", "--foreign": "foreign", "--out": "out", "--limit": "limit",
    "--workers": "workers", "--per-root": "perRoot", "--evidence": "evidence" };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--universal") { args.universal = true; continue; }
    const key = keys[argv[i]];
    if (!key) throw new Error(`unknown argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["db", "project", "foreign", "out"]) if (!args[key]) throw new Error(`missing --${key}`);
  for (const key of ["limit", "workers", "perRoot"]) {
    args[key] = Number(args[key]);
    if (!Number.isInteger(args[key]) || args[key] < 0) throw new Error(`invalid ${key}`);
  }
  return args;
}

async function mapLimit(items, limit, fn) {
  const output = Array.from({ length: items.length });
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(items.length, Math.max(1, limit)) }, async () => {
    while (next < items.length) {
      const index = next++;
      // Each worker is sequential; the worker pool provides bounded concurrency.
      // eslint-disable-next-line no-await-in-loop
      output[index] = await fn(items[index]);
    }
  }));
  return output;
}

export async function sweep(args, { observeFn = observe, sleepFn = Bun.sleep } = {}) {
  const db = new Database(args.db, { readonly: true });
  let roots;
  try {
    roots = db.query("SELECT r.root_id, r.host, COALESCE(NULLIF(r.name,''), r.host) AS name FROM root r "
      + "WHERE (SELECT COUNT(*) FROM channel c WHERE c.root_id=r.root_id AND c.state IN ('admitted','active'))=0 "
      + "ORDER BY (SELECT COUNT(*) FROM channel c WHERE c.root_id=r.root_id) ASC, r.first_seen DESC LIMIT ?").all(args.limit);
  } finally { db.close(); }
  if (!roots.length) return { rows: [], evidence: [], report: { asked: 0, note: "every listed publisher is already read here" }, write: false };
  const answers = await mapLimit(roots, args.workers, async root => {
    const url = `https://${root.host}/`;
    const twin = `sweep-${root.host.toLowerCase().replace(/[^a-z0-9]+/g, "-").slice(0, 48)}`;
    const [receipt, text] = await observeFn(args.project, args.foreign, twin, url);
    await sleepFn(300);
    return { ...root, url, receipt, text };
  });
  const rows = [];
  const evidence = [];
  const seen = new Set();
  const byKind = {};
  let refused = 0;
  const emit = (host, address, protocol, truth, why, title = "") => {
    if (seen.has(address)) return;
    seen.add(address);
    rows.push({ root: host, address, protocol, title, truth_level: truth, discovered_from: why });
    byKind[truth] = (byKind[truth] ?? 0) + 1;
  };
  for (const { host, url, receipt, text } of answers) {
    const policy = receipt.policy_decision;
    evidence.push({ root: host, http_status: receipt.http_status ?? null, policy: policy ?? null });
    if ((policy !== undefined && policy !== null && policy !== "allow") || !text) { refused++; continue; }
    for (const [address, protocol, title] of declaredFeeds(text, url)) emit(host, address, protocol, "observed_link_candidate", `declared by ${host} in its own markup`, title);
    const [grammar, why] = grammarOf(text);
    if (grammar) for (const route of (publisherGrammar(grammar)?.sweep ?? []).slice(0, args.perRoot)) {
      emit(host, new URL(route.path, url).href, route.protocol, "protocol_derived_candidate", `${host} runs ${grammar}: ${why}`);
    }
    if (args.universal) for (const route of publisherGrammarPolicy().universal) {
      emit(host, new URL(route.path, url).href, route.protocol, "protocol_derived_candidate", `a convention of the open web, not anything ${host} said`);
    }
  }
  return { rows, evidence, write: true, report: { asked: roots.length, answered: roots.length - refused, refused,
    channels_listed: rows.length, by_truth_level: byKind,
    cannot_infer: "a listed channel is an address, not a source this estate reads; a protocol-derived address is a convention of a grammar, never a promise that it answers" } };
}

if (import.meta.main) {
  try {
    const args = options(process.argv.slice(2));
    const result = await sweep(args);
    if (result.write) {
      writeFileSync(args.out, result.rows.map(row => JSON.stringify(row)).join("\n") + (result.rows.length ? "\n" : ""));
      if (args.evidence) appendFileSync(args.evidence, result.evidence.map(row => JSON.stringify(row)).join("\n") + (result.evidence.length ? "\n" : ""));
    }
    console.log(JSON.stringify(result.report));
  } catch (error) { console.error(error); process.exitCode = 1; }
}
