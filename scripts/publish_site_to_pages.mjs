// usage: bun scripts/publish_site_to_pages.mjs <project> <host> <export-root> [--attach] [--also NAME:HOST ...]
// Publishes one exported host as a static Pages project and attaches the host
// to it, so the address is served without passing through the edge Worker.
import { account } from "../components/BonfyreServiceSite/cloud.mjs";
import { cpSync, mkdirSync, rmSync, existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
const [project, host, root] = process.argv.slice(2);
const cloud = account();
const built = join(root, ".pages-" + project);
rmSync(built, { recursive: true, force: true }); mkdirSync(built, { recursive: true });
cpSync(join(root, "hosts", host), built, { recursive: true });
cpSync(join(root, "assets"), join(built, "assets"), { recursive: true });
// --also NAME:HOST mounts another exported host under /_sites/NAME/.  A zone
// rewrite rule sends that host's requests there, so several addresses can
// share the one project the zone's wildcard record points at.
for (let i = 0; i < process.argv.length; i++) if (process.argv[i] === "--also") {
  const [name, other] = process.argv[i + 1].split(":");
  if (!/^[a-z][a-z0-9-]*$/.test(name) || !existsSync(join(root, "hosts", other))) throw new Error(`cannot mount ${process.argv[i + 1]}`);
  cpSync(join(root, "hosts", other), join(built, "_sites", name), { recursive: true });
  cpSync(join(root, "assets"), join(built, "_sites", name, "assets"), { recursive: true });
}
writeFileSync(join(built, "_headers"), "/assets/*\n  Cache-Control: public, max-age=300\n");
// Without a 404 page Pages answers every unknown path with the front page.
if (!existsSync(join(built, "404.html"))) writeFileSync(join(built, "404.html"),
  `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Not found</title><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,600&family=Instrument+Serif&display=swap"><style>body{margin:0;min-height:100vh;display:grid;align-content:center;padding:0 max(22px,calc(50vw - 620px));background:#0d130f;color:#dde6da;font-family:'DM Sans',system-ui,sans-serif}h1{margin:0;font:600 clamp(3.4rem,11.5vw,11rem)/.86 'DM Sans',sans-serif;letter-spacing:-.065em;color:#f5f7ee}p{max-width:34rem;font:400 clamp(1.3rem,2vw,1.85rem)/1.25 'Instrument Serif',Georgia,serif}a{color:#b5df32}</style></head><body><h1>Not here.</h1><p>There is no page at this address. <a href="/">Go to the front page</a>.</p></body></html>`);
let found; try { found = (await cloud.api("GET", `/pages/projects/${project}`)).result; } catch (e) { if (e.status !== 404) throw e; }
if (!found) found = (await cloud.api("POST", "/pages/projects", { name: project, production_branch: "main" })).result;
const run = Bun.spawnSync(["bunx", "wrangler@4.147.0", "pages", "deploy", built, "--project-name", project, "--branch", "main", "--commit-dirty=true"], { env: { ...process.env, ...cloud.env }, stdout: "pipe", stderr: "pipe" });
if (run.exitCode !== 0) { let d = (run.stderr.toString() + run.stdout.toString()); for (const s of Object.values(cloud.env)) d = d.replaceAll(s, "[redacted]"); console.log("deploy failed", d.slice(-600)); process.exit(1); }
console.log("deployed", project, found.subdomain ?? project + ".pages.dev");
if (process.argv.includes("--attach")) {
  let domains = (await cloud.api("GET", `/pages/projects/${project}/domains`)).result ?? [];
  if (!domains.find(d => d.name === host)) {
    try { const r = await cloud.api("POST", `/pages/projects/${project}/domains`, { name: host }); console.log("attached", host, r.result?.status); }
    catch (e) { console.log("attach refused:", String(e.message).slice(0, 300)); }
  }
  domains = (await cloud.api("GET", `/pages/projects/${project}/domains`)).result ?? [];
  for (const d of domains) console.log("domain", d.name, d.status, JSON.stringify(d.validation_data ?? {}).slice(0, 160), JSON.stringify(d.verification_data ?? {}).slice(0, 160));
}
rmSync(built, { recursive: true, force: true });
