#!/usr/bin/env bun
// Read-only P2 inventory of authoring, vendor, generated and runtime roots.
import { lstatSync, readdirSync } from "node:fs";
import { resolve, relative, join, sep } from "node:path";

export const ROOTS = Object.freeze([
  ["integrations/frappe-bench","BonfyreFrappeCompiler"],
  ["components/BonfyreServiceSite","ServiceSite feature owners"],
  ["tools/creator-ingest","BonfyreIngest discovery adapter"],
  ["deploy/worker","WebAddress edge delivery"],
  ["site/matrix","PageComposition public sources"],
]);
export const KINDS = ["author","vendor","generated","runtime"];
export function classify(path) {
  const p=String(path).replaceAll("\\","/");
  if (p.includes("/../") || !ROOTS.some(([r])=>p===r||p.startsWith(r+"/"))) throw Error("outside registered source roots");
  if (/(^|\/)node_modules\//.test(p)) return "vendor";
  if (p.startsWith("integrations/frappe-bench/")) {
    if (/^integrations\/frappe-bench\/(env|sites)\//.test(p) || /bonfyre_cms\.db/.test(p)) return "runtime";
    if (/^integrations\/frappe-bench\/apps\/(frappe|erpnext|crm|helpdesk|hrms|lms|wiki|insights|drive)\//.test(p)) return "vendor";
  }
  if (p.startsWith("deploy/worker/")) {
    if (p.startsWith("deploy/worker/.wrangler/")) return "runtime";
    if (p.startsWith("deploy/worker/edge-assets/")) return "generated";
  }
  if (p.startsWith("site/matrix/")) {
    if (/^site\/matrix\/(surfaces\/build\/|duckdb-eh\.wasm$|matrix-vgpu\.js$|matrix-htmx\.js$|bonfyre-kit\.js$)/.test(p)) return "generated";
    if (/^site\/matrix\/(DONOR-LICENSES\/|donor-[^/]+$)/.test(p)) return "vendor";
  }
  if (p.startsWith("tools/creator-ingest/") && /^tools\/creator-ingest\/(output|dist)\//.test(p)) return "generated";
  if (p.startsWith("components/BonfyreServiceSite/")) {
    if (/\/(vendor|fonts)\//.test(p)) return "vendor";
    if (/\/(dist|build|output)\//.test(p)) return "generated";
  }
  return "author";
}
export function editOwner(path) {
  const p=String(path).replaceAll("\\","/");
  if (p.startsWith("components/BonfyreServiceSite/")) {
    if (/\/sections\/[^/]+\.mjs$/.test(p)) return "section definition";
    const last=p.split("/").at(-1);
    return {
      "site.mjs":"command dispatch","site-cms.mjs":"native CMS adapter",
      "site-contract.mjs":"validation","site-renderer.mjs":"HTML composition",
      "site-material.mjs":"section material","site-styles.mjs":"stylesheet",
      "section-registry.mjs":"section registry","build.mjs":"publication output"
    }[last]||"feature owner, not central dispatcher";
  }
  if (p.startsWith("integrations/frappe-bench/")) return "compiled Frappe grammar (not controller parity)";
  if (p.startsWith("tools/creator-ingest/")) return "native intake plus source discovery adapter";
  if (p.startsWith("deploy/worker/")) return "edge read-only routing/cache";
  if (p.startsWith("site/matrix/")) return "PageComposition source and native publication closure";
  throw Error("no recorded source owner");
}
function add(root,dir,totals,quota) {
  for (const e of readdirSync(dir,{withFileTypes:true})) {
    if (quota.remaining-- <= 0) throw Error("scan bound exceeded");
    const full=join(dir,e.name),rel=relative(root,full).split(sep).join("/");
    const kind=classify(rel);
    let stat;
    try {stat=lstatSync(full)}catch(e){if(e.code==="ENOENT")continue;throw e}
    if(stat.isSymbolicLink()){totals[kind].links++;continue}
    if(stat.isDirectory()){totals[kind].dirs++;add(root,full,totals,quota);continue}
    if(stat.isFile()){totals[kind].files++;totals[kind].bytes+=stat.size}
  }
}
export function inventory(root=resolve(import.meta.dir,"../..")) {
  return ROOTS.map(([path,owner])=>{
    const totals=Object.fromEntries(KINDS.map(k=>[k,{dirs:0,files:0,links:0,bytes:0}]));
    let exists=true;
    try{lstatSync(join(root,path))}catch(e){if(e.code==="ENOENT")exists=false;else throw e}
    if(exists)add(root,join(root,path),totals,{remaining:150000});
    return {path,owner,exists,totals};
  });
}
export function render(rows) {
  const out=["Bonfyre P2 physical source inventory (read-only, not a deletion proof)",
    "root\towner\tauthor\tvendor\tgenerated\truntime\ttotal_bytes"];
  for(const row of rows) {
    const counts=KINDS.map(k=>row.totals[k].files);
    const bytes=KINDS.reduce((sum,k)=>sum+row.totals[k].bytes,0);
    out.push([row.path,row.owner,...counts,bytes].join("\t"));
  }
  out.push("Edit a section in sections/<kind>.mjs; edit CMS in site-cms.mjs; render in site-renderer.mjs.");
  out.push("A vendor or generated classification never authorizes deletion without parity and rollback.");
  return out.join("\n");
}
if(import.meta.main) {
  if(process.argv.slice(2).some(x=>x!=="--check"))throw Error("usage: bun repository_maturity.mjs [--check]");
  const rows=inventory();
  console.log(render(rows));
  if(process.argv.includes("--check")&&rows.some(x=>!x.exists))process.exitCode=1;
}
