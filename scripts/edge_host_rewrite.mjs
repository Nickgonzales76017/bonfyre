#!/usr/bin/env bun
// Subject pages map to /h/<host>/; shared public assets keep their own paths.
// Existing exclusions and unrelated rules survive an exact-rule PATCH.
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const TERRITORIES = ["life", "live", "shop", "work"];
const KEEP_HOSTS = ["auth", "signin"];
const KEEP_PATHS = ["/assets/", "/apps/", "/mcp", "/.well-known/", "/h/"];

export function tokenOf(name, file = join(homedir(), ".bonfyre/secrets/cloud.refs.env")) {
  for (const line of readFileSync(file, "utf8").split(/\r?\n/)) {
    const m = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
    if (m && m[1] === name) { const t = m[2].match(/[A-Za-z0-9_-]{40,60}/); if (t) return t[0]; }
  }
  throw new Error(`no token in ${name}`);
}

export function ruleFor(tld, { apexes = false, pinned = [] } = {}) {
  const zone = `aurekai.${tld}`;
  const hosts = apexes ? `(http.host eq "${zone}" or ends_with(http.host, ".${zone}"))` : `ends_with(http.host, ".${zone}")`;
  // Hosts a route pins to another origin (a client site, a member's own
  // server) are theirs, not the estate's files.
  const keepHosts = [...KEEP_HOSTS.map(h => `${h}.${zone}`), ...pinned].map(h => `"${h}"`).join(" ");
  const keepPaths = KEEP_PATHS.map(p => `not starts_with(http.request.uri.path, "${p}")`).join(" and ");
  return {
    description: `estate pages on ${zone} hosts read as static assets by host`,
    expression: `(${hosts} and not http.host in {${keepHosts}} and ${keepPaths})`,
    action: "rewrite",
    action_parameters: { uri: { path: { expression: 'concat("/h/", http.host, http.request.uri.path)' } } },
  };
}

async function cf(token, method, path, body) {
  const response = await fetch(`https://api.cloudflare.com/client/v4${path}`, { method,
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  return response.json();
}

export async function main(argv = process.argv.slice(2)) {
  const apply = argv.includes("--apply"), apexes = argv.includes("--apexes");
  const tokenVar = argv.includes("--token-var") ? argv[argv.indexOf("--token-var") + 1] : "CLOUDFLARE_info_zones";
  const token = tokenOf(tokenVar);
  for (const tld of TERRITORIES) {
    const zones = await cf(token, "GET", `/zones?name=aurekai.${tld}`);
    const zone = zones.result?.[0]?.id;
    if (!zone) { console.log(JSON.stringify({ zone: `aurekai.${tld}`, error: "zone not visible to this token" })); continue; }
    const routes = (await cf(token, "GET", `/zones/${zone}/workers/routes`)).result || [];
    const pinned = routes.filter(r => !r.script && !r.pattern.startsWith("*") && r.pattern !== `aurekai.${tld}/*`)
      .map(r => r.pattern.replace(/\/\*$/, "")).filter(h => /^[a-z0-9.-]+$/.test(h));
    const rule = ruleFor(tld, { apexes, pinned });
    if (!apply) { console.log(JSON.stringify({ zone: `aurekai.${tld}`, rule })); continue; }
    const endpoint = `/zones/${zone}/rulesets/phases/http_request_transform/entrypoint`, held = await cf(token, "GET", endpoint);
    if (!held.success) throw new Error(`Read current transform rules before changing ${tld}`);
    const matches = held.result.rules.filter(r => r.description === rule.description);
    if (matches.length !== 1) throw new Error(`Expected one existing estate rewrite on ${tld}; refusing replacement`);
    const old = matches[0], fields = ["action", "action_parameters", "description", "enabled", "ref", "expression"];
    const next = Object.fromEntries(fields.filter(k => old[k] !== undefined).map(k => [k, old[k]]));
    for (const p of KEEP_PATHS) if (!next.expression.includes(`not starts_with(http.request.uri.path, "${p}")`)) next.expression = `(${next.expression}) and not starts_with(http.request.uri.path, "${p}")`;
    const fresh = await cf(token, "GET", endpoint); if (!fresh.success || JSON.stringify(fresh.result) !== JSON.stringify(held.result)) throw new Error(`Concurrent ruleset change on ${tld}`);
    const put = next.expression === old.expression ? held : await cf(token, "PATCH", `/zones/${zone}/rulesets/${held.result.id}/rules/${old.id}`, next);
    if (!put.success) throw new Error(`Estate rewrite refused on ${tld}: ${(put.errors || []).map(e => e.message).join('; ')}`);
    console.log(`EDGE_REWRITE aurekai.${tld} shared assets preserved; existing host exclusions retained`);
  }
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = await main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
