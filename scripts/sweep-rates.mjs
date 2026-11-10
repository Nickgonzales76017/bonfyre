#!/usr/bin/env bun
// 402 discovery is observation; only a stated, qualified rate may cross Meter admission.
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { boundedText, publicSourceFetch } from "./public-source-fetch.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const exported = Bun.spawnSync(["cue", "export", join(here, "rate-discovery.cue"), "-e", "rateDiscovery", "--out", "json"]);
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
export const policy = JSON.parse(exported.stdout.toString());

export function statedPrice(status, headers, body) {
  if (status !== 402) return null;
  let parsed = null;
  try { parsed = JSON.parse(body); } catch { /* a 402 can be text or headers */ }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) parsed = {};
  const hay = `${body} ${[...headers].map(([key, value]) => `${key}: ${value}`).join(" ")}`;
  let field = null, amount = null;
  for (const name of policy.amount_fields) {
    const raw = parsed[name] ?? hay.match(new RegExp(`"${name}"\\s*:\\s*"?([0-9.]+)"?`))?.[1];
    const value = Number(raw);
    if (raw != null && Number.isFinite(value) && value > 0) { field = name; amount = value; break; }
  }
  if (amount === null) {
    const match = hay.match(/breakdown\.[a-z]+\s*=\s*([0-9.]+)/);
    if (match && Number.isFinite(Number(match[1])) && Number(match[1]) > 0) {
      field = "breakdown"; amount = Number(match[1]);
    }
  }
  if (amount === null) return null;
  let currency = "";
  for (const name of policy.currency_fields) {
    const raw = parsed[name] ?? hay.match(new RegExp(`"${name}"\\s*:\\s*"([A-Za-z0-9:_-]+)"`))?.[1];
    if (typeof raw === "string" && /^[A-Za-z0-9:_-]+$/.test(raw)) { currency = raw; break; }
  }
  if (!currency) currency = hay.match(/\b(USDC|USDT|DAI|ETH|USD|EUR|GBP)\b/)?.[1] ?? "unstated";
  // x402 maxAmountRequired is normally an asset's base-unit quantity. Without
  // an explicit denomination/decimals it cannot be a Meter rate.
  const admissible = field !== "maxAmountRequired" && field !== "breakdown" &&
    policy.admitted_currencies.includes(currency);
  return { amount, currency, field, admissible };
}

export function frontierUrls(path, limit) {
  const db = new Database(path, { readonly: true, create: false });
  try {
    const protocols = policy.front_protocols;
    const placeholders = protocols.map(() => "?").join(",");
    return db.query(`SELECT exact_address FROM channel WHERE (protocol IN (${placeholders})
      OR exact_address LIKE '%/api/%') AND exact_address LIKE 'https://%'
      ORDER BY state='admitted' DESC LIMIT ?`).all(...protocols, limit).map(item => item.exact_address);
  } finally { db.close(); }
}

export async function sweep({ urls, frontier = join(homedir(), ".bonfyre/estate-local/frontier.db"),
  meter = join(homedir(), ".bonfyre/estate-local/meter.db"), limit = 40, admit = false,
  meterBinary = join(homedir(), ".bonfyre/bin/bonfyre-meter"), fetcher = fetch,
  runner = Bun.spawnSync, today = new Date().toISOString().slice(0, 10) } = {}) {
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("limit must be a positive safe integer");
  const targets = (urls ?? frontierUrls(frontier, limit)).slice(0, limit), robots = new Map();
  const found = [], admissionRefused = [];
  let asked = 0, declined = 0, unreachable = 0, unpriced = 0, admitted = 0;
  for (const coordinate of targets) {
    let url;
    try { url = new URL(coordinate); } catch { unreachable++; continue; }
    let response;
    try { response = await publicSourceFetch(url, { origin: url.origin, agent: policy.user_agent,
      fetcher, robots, timeoutMs: 12000 }); }
    catch (error) {
      if (error.message.includes("robots policy")) declined++;
      else unreachable++;
      continue;
    }
    asked++;
    let body;
    try { body = await boundedText(response, 4096); }
    catch { unpriced++; continue; }
    const price = statedPrice(response.status, response.headers, body);
    if (!price) { unpriced++; continue; }
    const evidence = `${url.href} answered HTTP 402 on ${today} stating ${price.field}=${price.amount}${price.currency === "unstated" ? "" : ` ${price.currency}`}`.slice(0, 480);
    const rate = { provider: url.host, op: policy.op, unit: policy.unit,
      amount: price.amount, currency: price.currency, evidence, admissible: price.admissible };
    found.push(rate);
    if (!admit) continue;
    if (!price.admissible) { admissionRefused.push({ provider: url.host, reason: "amount denomination or currency is not established" }); continue; }
    if (!existsSync(meterBinary)) { admissionRefused.push({ provider: url.host, reason: "native Meter unavailable" }); continue; }
    const result = runner([meterBinary, "admit-rate", "--provider", rate.provider,
      "--op", rate.op, "--unit", rate.unit, "--amount", String(rate.amount), "--currency", rate.currency,
      "--effective-at", `${today}T00:00:00Z`, "--evidence", evidence],
      { env: { ...process.env, BONFYRE_METER_DB: meter } });
    if (result.exitCode === 0) admitted++;
    else admissionRefused.push({ provider: url.host, reason: result.stderr?.toString().slice(-140) || "native Meter refused" });
  }
  return { asked, declined_by_robots: declined, unreachable_transport: unreachable,
    answered_without_a_stated_price: unpriced,
    stated_a_price: found.length, admitted, admission_refused: admissionRefused, rates: found.slice(0, 8),
    cannot_infer: policy.cannot_infer };
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), value = key => { const i = args.indexOf(key); return i >= 0 ? args[i + 1] : null; };
    const known = new Set(["--frontier", "--meter", "--limit", "--admit", "--from"]);
    for (let i = 0; i < args.length; i++) {
      if (!known.has(args[i])) throw new Error(`unknown argument: ${args[i]}`);
      if (args[i] !== "--admit") i++;
    }
    const list = value("--from"), urls = list ? readFileSync(list, "utf8").split(/\r?\n/)
      .map(line => line.trim()).filter(line => line && !line.startsWith("#")) : undefined;
    console.log(JSON.stringify(await sweep({ urls, frontier: value("--frontier") ?? undefined,
      meter: value("--meter") ?? undefined, limit: value("--limit") ? Number(value("--limit")) : 40,
      admit: args.includes("--admit") }), null, 1));
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
