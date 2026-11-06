#!/usr/bin/env bun
// Compute, never apply, a declared product reconciliation consequence.
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Database } from "bun:sqlite";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const namespaces = join(root, "architecture/institutional/namespaces.yaff");
const lawKeys = new Set(["obligation_prefix", "obligation_target", "reconcile_entity",
  "reconcile_entity_key", "reconcile_entity_amount", "reconcile_allocation",
  "reconcile_allocation_key", "reconcile_allocation_amount", "reconcile_allocation_active",
  "reconcile_tolerance", "reconcile_note", "reconcile_witness", "credential_env"]);
const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;
const sourceAnswered = "the system of record answered";

export function declaredPack(namespace, source = namespaces) {
  let current;
  for (const raw of readFileSync(source, "utf8").split(/\r?\n/)) {
    if (raw.startsWith("namespace ")) current = raw.slice(10).trim();
    else if (current === namespace && raw.trimStart().startsWith("product_pack ")) {
      const path = raw.trim().slice("product_pack ".length).trim();
      return resolve(root, path);
    }
  }
  return null;
}

export function declaredLaw(pack) {
  const law = {};
  for (const raw of readFileSync(pack, "utf8").split(/\r?\n/)) {
    const match = raw.trim().match(/^([^\s]+)\s+(.+)$/);
    if (match && lawKeys.has(match[1]) && !(match[1] in law)) law[match[1]] = match[2].trim();
  }
  return law;
}

export function credentials(contents) {
  const cfg = new Map();
  for (const raw of contents.split(/\r?\n/)) {
    const line = raw.trim().replace(/^export\s+/, "");
    const equal = line.indexOf("=");
    if (equal > 0 && !line.startsWith("#")) cfg.set(line.slice(0, equal).trim(),
      line.slice(equal + 1).trim().replace(/^(['"])(.*)\1$/, "$2"));
  }
  const suffix = end => [...cfg].find(([key]) => key.endsWith(end))?.[1];
  return { host: suffix("_DB_HOST"), user: suffix("_DB_USER"), password: suffix("_DB_PASS"),
    name: suffix("_DB_NAME"), port: suffix("_DB_PORT") ?? "3306" };
}

export function sourceAmounts(law, ids, env = process.env) {
  const credentialPath = env[law.credential_env ?? ""];
  if (!credentialPath || !existsSync(credentialPath) || !statSync(credentialPath).isFile())
    return [{}, "no credentials file is configured for this pack"];
  const fields = ["reconcile_entity", "reconcile_entity_key", "reconcile_entity_amount",
    "reconcile_allocation", "reconcile_allocation_key", "reconcile_allocation_amount",
    "reconcile_allocation_active"];
  for (const key of fields) if (!identifier.test(law[key] ?? ""))
    return [{}, `the pack's ${key} is not a plain identifier and was refused`];
  const cfg = credentials(readFileSync(credentialPath, "utf8"));
  if (![cfg.host, cfg.user, cfg.password, cfg.name].every(Boolean))
    return [{}, "the credentials file does not name a host, user, password and database"];
  if (!/^[0-9]+$/.test(cfg.port)) return [{}, "the credentials file has an invalid database port"];
  const numeric = ids.filter(id => /^[0-9]+$/.test(id)).join(",") || "0";
  const sql = `SELECT e.${law.reconcile_entity_key}, e.${law.reconcile_entity_amount}, `
    + `COALESCE(SUM(CASE WHEN a.${law.reconcile_allocation_active}=1 `
    + `THEN a.${law.reconcile_allocation_amount} END),0) `
    + `FROM ${law.reconcile_entity} e LEFT JOIN ${law.reconcile_allocation} a `
    + `ON a.${law.reconcile_allocation_key} = e.${law.reconcile_entity_key} `
    + `WHERE e.${law.reconcile_entity_key} IN (${numeric}) `
    + `GROUP BY e.${law.reconcile_entity_key}, e.${law.reconcile_entity_amount}`;
  const child = spawnSync("mysql", [`--host=${cfg.host}`, `--port=${cfg.port}`, `--user=${cfg.user}`,
    `--database=${cfg.name}`, "--batch", "--skip-column-names", "--connect-timeout=20", "-e", sql],
  { encoding: "utf8", timeout: 30_000, maxBuffer: 64 * 1024 * 1024,
    env: { ...env, MYSQL_PWD: cfg.password } });
  if (child.error?.code === "ENOENT")
    return [{}, "the mysql client is not on PATH, so the system of record was not asked"];
  if (child.error || child.status !== 0) {
    const error = child.stderr?.split(/\r?\n/).reverse().find(line => line.trim().startsWith("ERROR"));
    return [{}, `the system of record could not be reached: ${error?.trim().slice(0, 200) || `mysql exited ${child.status}`}`];
  }
  const amounts = {};
  for (const line of child.stdout.split(/\r?\n/)) {
    if (!line) continue;
    const parts = line.split("\t");
    if (parts.length !== 3 || !/^[0-9]+$/.test(parts[0]))
      return [{}, "the system of record returned a malformed row"];
    const paid = Number(parts[1]), allocated = Number(parts[2]);
    if (!Number.isFinite(paid) || !Number.isFinite(allocated))
      return [{}, "the system of record returned a nonnumeric amount"];
    if (parts[0] in amounts) return [{}, "the system of record returned duplicate payment rows"];
    amounts[parts[0]] = { amount: paid, allocated, amount_left: Math.round((paid - allocated) * 100) / 100 };
  }
  return [amounts, sourceAnswered];
}

export function consequence(namespace, store, env = process.env) {
  const pack = declaredPack(namespace);
  if (!pack || !existsSync(pack)) throw new Error(`namespace ${JSON.stringify(namespace)} declares no product_pack, or it is missing`);
  const law = declaredLaw(pack);
  const missing = ["obligation_prefix", "reconcile_entity", "reconcile_allocation_amount"].filter(key => !(key in law));
  if (missing.length) throw new Error(`${basename(pack)} declares no reconciliation law; missing ${missing.join(", ")}`);
  let rows = [];
  if (store && existsSync(store)) {
    const db = new Database(store, { readonly: true });
    try {
      rows = db.query("SELECT id, state FROM target_obligations WHERE id LIKE ?")
        .all(`${law.obligation_prefix}%`).map(row => ({ obligation: row.id,
          entity: row.id.split(":", 2)[1] ?? "", state: row.state }));
    } finally { db.close(); }
  }
  const tolerance = Number(law.reconcile_tolerance ?? "0.005");
  if (!Number.isFinite(tolerance) || tolerance < 0) throw new Error("invalid reconciliation tolerance");
  const [source, status] = sourceAmounts(law, rows.map(row => row.entity), env);
  const reached = status === sourceAnswered;
  let resolved = 0, unresolved = 0, under = 0, over = 0, net = 0;
  const detail = [];
  for (const row of rows) {
    const entry = source[row.entity];
    if (!entry) {
      unresolved++;
      detail.push({ obligation: row.obligation, verdict: "unresolvable",
        because: reached ? "the system of record returned no row for it" : status });
      continue;
    }
    resolved++;
    const left = entry.amount_left;
    net += left;
    if (Math.abs(left) < tolerance) detail.push({ obligation: row.obligation, verdict: "reconciled" });
    else {
      const direction = left > 0 ? "under-allocated" : "over-allocated";
      if (left > 0) under++; else over++;
      detail.push({ obligation: row.obligation, verdict: "unreconciled", direction, amount_left: left });
    }
  }
  return { schema: "bonfyre-product-close-consequence.v1", namespace, pack: relative(root, pack),
    law_declared_by_pack: Object.fromEntries(Object.entries(law).sort(([a], [b]) => a.localeCompare(b))),
    ran_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"), effect_class: "PURE", effect_performed: false,
    source_of_record: { reached, status,
      cannot_infer: "an unreachable system of record -> an obligation that cannot reconcile; nothing was asked, so nothing is known" },
    obligations: rows.length, resolved_against_source: resolved, unresolvable: unresolved,
    unreconciled_under: under, unreconciled_over: over, net_amount_left: Math.round(net * 100) / 100, detail,
    cannot_infer: ["a computed consequence -> an approval", "a closable obligation -> a closed one",
      "a law read from a contract -> a law anyone validated; the pack carries its own witness for that",
      "a net position -> money owed"] };
}

export function options(args) {
  const result = { store: process.env.BONFYRE_OBLIGATION_DB ?? "", json: false };
  for (let i = 0; i < args.length; i++) {
    if (["--namespace", "--obligations"].includes(args[i])) {
      if (!args[i + 1]) throw new Error(`${args[i]} requires a value`);
      result[args[i] === "--namespace" ? "namespace" : "store"] = args[++i];
    } else if (args[i] === "--json") result.json = true;
    else throw new Error(`unknown option: ${args[i]}`);
  }
  if (!result.namespace) throw new Error("usage: product_close_consequence.mjs --namespace <ns> [--obligations <sqlite>] [--json]");
  return result;
}

if (import.meta.main) {
  try {
    const config = options(process.argv.slice(2));
    const receipt = consequence(config.namespace, config.store);
    if (config.json) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  namespace ${receipt.namespace}\n  grammar from ${receipt.pack}`);
      console.log(`  law: ${receipt.law_declared_by_pack.reconcile_entity}.${receipt.law_declared_by_pack.reconcile_entity_amount} - SUM(${receipt.law_declared_by_pack.reconcile_allocation}.${receipt.law_declared_by_pack.reconcile_allocation_amount} WHERE ${receipt.law_declared_by_pack.reconcile_allocation_active}=1)`);
      console.log(`  witness: ${receipt.law_declared_by_pack.reconcile_witness ?? "(none declared)"}`);
      console.log(`\n  obligations ${receipt.obligations}   resolved ${receipt.resolved_against_source}   unresolvable ${receipt.unresolvable}`);
      console.log(`  unreconciled: ${receipt.unreconciled_under} under, ${receipt.unreconciled_over} over   net $${receipt.net_amount_left.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`);
      console.log("\n  nothing was written; this is the consequence, not the act");
    }
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
