#!/usr/bin/env bun
// Candidate-only spelling screen. A flag boxes presentation, not truth or bytes.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, openSync, closeSync, fstatSync, readSync, readdirSync, readFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const cue = Bun.spawnSync(["cue", "export", join(here, "content-screen.cue"), "-e", "contentScreen", "--out", "json"]);
if (cue.exitCode !== 0) throw new Error(cue.stderr.toString());
export const policy = JSON.parse(cue.stdout.toString());
const leet = { "0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t", "8": "b", "9": "g", "@": "a", "$": "s", "!": "i", "|": "i", "+": "t" };

export function normalise(value) {
  return value.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase()
    .replace(/(?<=[a-z])([0-9@$!|+])(?=[a-z])/g, m => leet[m])
    .replace(/^([0-9@$!|+])(?=[a-z])/, m => leet[m])
    .replace(/[^a-z]+/g, " ").trim()
    .replace(/\b(?:[a-z] ){2,}[a-z]\b/g, m => m.replaceAll(" ", ""));
}

export function loadTerms(path) {
  if (!existsSync(path)) return null;
  return readFileSync(path, "utf8").split(/\r?\n/)
    .map(line => normalise(line.split("#", 1)[0].trim())).filter(Boolean);
}

// A receipt that is no longer a loose file is asked of the owner, which reads
// it out of the pack beside the files. The last complete line is the receipt.
function packedReceipt(path) {
  const owner = process.env.BONFYRE_PROJECT_BINARY ?? join(import.meta.dir, "../components/BonfyreProject/bonfyre-project");
  if (!existsSync(owner)) return {};
  const out = Bun.spawnSync([owner, "foreign", "receipt", dirname(path), basename(path)], { stdout: "pipe", stderr: "ignore" });
  if (out.exitCode !== 0) return {};
  for (const line of out.stdout.toString("utf8").split(/\r?\n/).reverse()) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try { return JSON.parse(trimmed); } catch { /* an incomplete last line; take the one before */ }
  }
  return {};
}

export function lastReceipt(path) {
  if (!existsSync(path)) return packedReceipt(path);
  const fd = openSync(path, "r");
  try {
    const size = fstatSync(fd).size;
    const len = Math.min(size, 65536), buffer = Buffer.alloc(len);
    readSync(fd, buffer, 0, len, size - len);
    for (const line of buffer.toString("utf8").split(/\r?\n/).reverse()) {
      const trimmed = line.trim();
      if (!trimmed.startsWith("{")) continue;
      try { return JSON.parse(trimmed); } catch { /* a truncated receipt is not evidence */ }
    }
    return {};
  } finally { closeSync(fd); }
}

function existingFlags(path) {
  try {
    const db = new Database(path, { readonly: true, create: false });
    try { return new Set(db.query("SELECT flag FROM content_flag").all().map(row => row.flag)); }
    finally { db.close(); }
  } catch { return new Set(); }
}

const count = (haystack, needle) => {
  let found = 0, at = 0;
  while ((at = haystack.indexOf(needle, at)) >= 0) { found++; at += needle.length; }
  return found;
};

export const redact = term => term ? term[0] + "*".repeat(Math.max(term.length - 1, 1)) : "";

function createFlag(row, cms, dbPath) {
  const proc = Bun.spawnSync([cms, "entry", "create", "content_flag", "--db", dbPath, "--ns", "root", JSON.stringify(row)], { env: { ...process.env } });
  const tail = (proc.stdout.toString().trim() || proc.stderr.toString().trim()).split("\n").at(-1) ?? "";
  try { return proc.exitCode === 0 && JSON.parse(tail).created === true; }
  catch { return false; }
}

export async function screen(options = {}) {
  const bodies = options.bodies ?? process.env.BONFYRE_FOREIGN_BODIES ?? "/var/lib/bonfyre/foreign/bodies";
  const receipts = options.receipts ?? process.env.BONFYRE_FOREIGN_RECEIPTS ?? "/var/lib/bonfyre/foreign/receipts";
  const termsPath = options.termsPath ?? process.env.BONFYRE_SCREEN_TERMS ?? "/var/lib/bonfyre/content-screen/terms.txt";
  const cms = options.cms ?? process.env.BONFYRE_CMS ?? "/usr/local/bin/bonfyre-cms";
  const dbPath = options.dbPath ?? process.env.BONFYRE_CMS_DB ?? "/var/lib/bonfyre/cms/bonfyre_cms.db";
  const terms = loadTerms(termsPath);
  if (terms === null) return { receipt: { schema: policy.schema, screen: policy.screen, state: "installed_without_terms", terms_path: termsPath,
    screened: 0, flagged: 0, cannot_infer: policy.absent_terms_cannot_infer }, exitCode: 2 };
  if (!terms.length) throw new Error(`term file ${termsPath} is present and empty`);
  if (options.twin && (basename(options.twin) !== options.twin || options.twin === "." || options.twin === ".."))
    throw new Error("twin must be a single retained-body name");
  const have = existingFlags(dbPath);
  const names = options.twin ? [options.twin] : readdirSync(bodies).filter(name => name.endsWith(".body")).map(name => name.slice(0, -5)).sort();
  let screened = 0, flagged = 0;
  for (const twin of names) {
    const path = join(bodies, `${twin}.body`);
    if (!existsSync(path)) continue;
    const content = await Bun.file(path).slice(0, policy.body_prefix_limit).text();
    const text = ` ${normalise(content)} `;
    screened++;
    const receipt = lastReceipt(join(receipts, `${twin}.json`));
    for (const term of [...new Set(terms)].sort()) {
      const n = count(text, ` ${term} `);
      if (!n) continue;
      const flag = `cf-${createHash("sha256").update(`${twin}|${policy.screen}|${term}`).digest("hex").slice(0, 16)}`;
      if (have.has(flag)) continue;
      const row = { flag, twin, source: twin, observed_at: receipt.observed_at ?? "", receipt_ref: receipt.content_sha256 ?? "",
        screen: policy.screen, screen_version: policy.screen_version, signal: policy.signal, matched: redact(term),
        sample_count: String(n), review_state: policy.review_state, cannot_infer: policy.cannot_infer, status: "published" };
      if (options.dryRun) { console.log(`  would flag ${flag}`); flagged++; }
      else if (createFlag(row, cms, dbPath)) { flagged++; have.add(flag); }
      else console.error(`  FAILED ${flag}: CMS refused flag creation`);
    }
  }
  return { receipt: { schema: policy.schema, screen: policy.screen, state: "installed", terms: terms.length,
    screened, flagged, cannot_infer: policy.cannot_infer }, exitCode: 0 };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const argv = process.argv.slice(2);
    const twin = argv.includes("--twin") ? argv[argv.indexOf("--twin") + 1] : undefined;
    const result = await screen({ dryRun: argv.includes("--dry-run"), twin });
    console.log(JSON.stringify(result.receipt, null, 2));
    process.exitCode = result.exitCode;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
