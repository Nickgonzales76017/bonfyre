// Where a site is published and where its requests are kept: one Cloudflare
// account, named by the record. Credentials are read from the estate's secrets
// file by name and are never printed, logged or written anywhere else.
//
// A record with no cloud_account uses the estate's own account. One that says
// cloud_account: "proton" uses CLOUDFLARE_proton_api_token and
// CLOUDFLARE_proton_account_id, so a client's site can live on an account that
// is not the estate's.
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const secrets = process.env.BONFYRE_CLOUD_SECRETS ?? join(homedir(), ".bonfyre/secrets/cloud.refs.env");
// Upgrade deliberately after a deployment witness; avoid changing the tool during publication.
const wrangler = "wrangler@4.147.0";

export function account(label) {
  if (label && !/^[a-z][a-z0-9_]*$/.test(label)) throw new Error(`cloud_account "${label}" is not a usable name`);
  if (!existsSync(secrets)) throw new Error(`no secrets file at ${secrets}`);
  const names = label ? [`CLOUDFLARE_${label}_api_token`, `CLOUDFLARE_${label}_account_id`] : ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"];
  // Read the way a shell would, so a value that refers to another resolves; only the two named values leave the shell.
  const read = Bun.spawnSync(["sh", "-c", `set -a; . "$1" >/dev/null 2>&1; eval "printf '%s\\n%s' \\"\\$$2\\" \\"\\$$3\\""`, "sh", secrets, ...names], { stdout: "pipe", stderr: "ignore" });
  const [token, id] = read.stdout.toString().split("\n").map(value => value.trim());
  if (!token || !id) throw new Error(`the secrets file has no ${names.filter((_, i) => ![token, id][i]).join(" or ")}`);
  const api = async (method, path, body, raw = false) => {
    const answer = await fetch(`https://api.cloudflare.com/client/v4/accounts/${id}${path}`, { method, headers: { authorization: `Bearer ${token}`, ...(body ? { "content-type": "application/json" } : {}) }, body: body ? JSON.stringify(body) : undefined });
    if (raw) { if (!answer.ok) throw new Error(`Cloudflare ${method} ${path}: ${answer.status}`); return answer.text(); }
    const data = await answer.json().catch(() => ({}));
    if (!answer.ok || data.success === false) { const error = new Error(`Cloudflare ${method} ${path}: ${answer.status} ${(data.errors ?? []).map(e => e.message).join("; ")}`); error.status = answer.status; throw error; }
    return data;
  };
  return { label: label ?? "estate", api, env: { CLOUDFLARE_API_TOKEN: token, CLOUDFLARE_ACCOUNT_ID: id } };
}

const leadsName = project => `${project}-leads`;
async function leadsNamespace(cloud, project, create) {
  for (let page = 1; ; page++) {
    const list = await cloud.api("GET", `/storage/kv/namespaces?per_page=100&page=${page}`);
    const hit = list.result.find(ns => ns.title === leadsName(project));
    if (hit) return hit.id;
    if (list.result.length < 100) break;
  }
  return create ? (await cloud.api("POST", "/storage/kv/namespaces", { title: leadsName(project) })).result.id : null;
}

// Publish a built site. The project is made if it is not there; a site that
// keeps requests gets its own store, bound to the site and to nothing else.
export function publicationBranch(previewBranch, productionBranch = "main") {
  if (previewBranch === undefined) return "main";
  if (typeof previewBranch !== "string" || !/^[a-z0-9](?:[a-z0-9-]{0,26}[a-z0-9])?$/.test(previewBranch))
    throw new Error("previewBranch must be a lowercase branch alias of 1 to 28 letters, digits or hyphens");
  if (previewBranch === "main" || previewBranch === productionBranch)
    throw new Error("A preview branch cannot be the production branch");
  return previewBranch;
}

export async function publish(built, project, label, { previewBranch } = {}) {
  const branch = publicationBranch(previewBranch);
  const cloud = account(label);
  let found; try { found = (await cloud.api("GET", `/pages/projects/${project}`)).result; } catch (error) { if (error.status !== 404) throw error; }
  // Made through the API: this wrangler hands `pages project create` to the Workers flow, which refuses a plain static directory.
  if (!found) found = (await cloud.api("POST", "/pages/projects", { name: project, production_branch: "main" })).result;
  publicationBranch(previewBranch, found.production_branch);
  if (existsSync(join(built, "_worker.js"))) {
    const id = await leadsNamespace(cloud, project, true), bound = { LEADS: { namespace_id: id } };
    if (found.deployment_configs?.production?.kv_namespaces?.LEADS?.namespace_id !== id)
      await cloud.api("PATCH", `/pages/projects/${project}`, { deployment_configs: { production: { kv_namespaces: bound }, preview: { kv_namespaces: bound } } });
  }
  const run = Bun.spawnSync(["bunx", wrangler, "pages", "deploy", built, "--project-name", project, "--branch", branch, "--commit-dirty=true"], { env: { ...process.env, ...cloud.env }, stdout: "pipe", stderr: "pipe" });
  if (run.exitCode !== 0) {
    let details = `${run.stderr.toString()}\n${run.stdout.toString()}`.trim();
    for (const secret of Object.values(cloud.env)) details = details.replaceAll(secret, "[redacted]");
    throw new Error(`deploy did not complete (${wrangler}, exit ${run.exitCode}): ${details.slice(0, 6000)}`);
  }
  return { account: cloud.label, project, address: `https://${previewBranch ? `${branch}.` : ""}${found.subdomain ?? `${project}.pages.dev`}/`, branch, preview: previewBranch !== undefined, keeps_requests: existsSync(join(built, "_worker.js")) };
}

// Every request the site is holding, oldest first.
export async function heldLeads(project, label) {
  const cloud = account(label), id = await leadsNamespace(cloud, project, false);
  if (!id) return [];
  const keys = [];
  for (let cursor = ""; ;) {
    const list = await cloud.api("GET", `/storage/kv/namespaces/${id}/keys?prefix=lead:&limit=1000${cursor ? `&cursor=${cursor}` : ""}`);
    keys.push(...list.result.map(k => k.name));
    cursor = list.result_info?.cursor; if (!cursor) break;
  }
  const leads = [];
  for (const key of keys.sort()) leads.push({ key, ...JSON.parse(await cloud.api("GET", `/storage/kv/namespaces/${id}/values/${encodeURIComponent(key)}`, null, true)) });
  return leads;
}
export async function dropLead(project, label, key) {
  const cloud = account(label), id = await leadsNamespace(cloud, project, false);
  if (id) await cloud.api("DELETE", `/storage/kv/namespaces/${id}/values/${encodeURIComponent(key)}`);
}
