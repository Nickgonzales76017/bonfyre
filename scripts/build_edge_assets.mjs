#!/usr/bin/env bun
// Host pages plus their native public asset closure. Private workspace data
// is never exportable. --export DIR --out DIR [--max-files 20000]
import { cpSync, existsSync, lstatSync, mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { guardEdgePaths } from "./release/edge_asset_guard.mjs";
const root = join(import.meta.dir, "..");

export function files(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    const status = lstatSync(path);
    if (status.isSymbolicLink()) throw Error("public asset closure refuses symlink: " + path);
    if (status.isDirectory()) out.push(...files(path));
    else if (status.isFile()) out.push(path);
  }
  return out;
}

export function main(argv = process.argv.slice(2)) {
  const args = { export: "", out: "", maxFiles: 20000 };
  for (let i = 0; i < argv.length; i++) {
    const key = { "--export": "export", "--out": "out", "--max-files": "maxFiles" }[argv[i]];
    if (!key || argv[i + 1] === undefined) throw new Error(`unknown or incomplete argument: ${argv[i]}`);
    args[key] = key === "maxFiles" ? Number(argv[++i]) : argv[++i];
  }
  guardEdgePaths(args, root);
  const hosts = join(args.export, "hosts");
  if (!existsSync(hosts)) throw new Error(`no hosts/ in ${args.export}`);
  if (lstatSync(hosts).isSymbolicLink()) throw Error("public hosts directory cannot be a symlink");
  // Validate every export input before deleting the previous build. Never
  // follow symlinks into source/private material or leave a broken half-build.
  const admittedHosts = [];
  for (const host of readdirSync(hosts)) {
    if (!/^[a-z0-9.-]+$/.test(host)) continue;
    const hostDir = join(hosts, host);
    const status = lstatSync(hostDir);
    if (status.isSymbolicLink()) throw Error("public host cannot be a symlink: " + host);
    if (status.isDirectory()) admittedHosts.push({ host, files: files(hostDir) });
  }
  const assetsSource = join(args.export, "assets");
  if (existsSync(assetsSource)) {
    if (lstatSync(assetsSource).isSymbolicLink()) throw Error("public asset directory cannot be a symlink");
    files(assetsSource);
  }
  rmSync(args.out, { recursive: true, force: true });
  mkdirSync(join(args.out, "h"), { recursive: true });
  let pages = 0, shared = 0;
  for (const {host,files:hostFiles} of admittedHosts) {
    for (const file of hostFiles) {
      if (!file.endsWith(".html") && !file.includes("/_search/")) continue;
      const rel = relative(join(hosts, host), file);
      // "/route.html" is served at /route; an index stays an index.
      const target = rel.endsWith("index.html") || rel.includes("_search/") ? rel : rel.replace(/\.html$/, "/index.html");
      mkdirSync(join(args.out, "h", host, target, ".."), { recursive: true });
      cpSync(file, join(args.out, "h", host, target));
      pages++;
    }
  }
  if (existsSync(join(args.export, "assets"))) {
    cpSync(join(args.export, "assets"), join(args.out, "assets"), { recursive: true });
    shared = files(join(args.out, "assets")).length;
  }
  const closure = Bun.spawnSync([process.env.BONFYRE_WEB_ADDRESS || join(root, "cmd/BonfyreWebAddress/bonfyre-web-address"), "publish-assets", "--site-root", join(root, "site/matrix"), "--out", args.out], {env:{...process.env,BONFYRE_WORKSPACE_CLIENT:process.env.BONFYRE_WORKSPACE_CLIENT || join(root,"integrations/browser")},stdout:"pipe",stderr:"pipe"});
  if (closure.exitCode !== 0) throw new Error("Native public asset closure refused: " + closure.stderr.toString());
  const total = files(args.out).length; shared = total - pages;
  console.log(closure.stdout.toString().trim());
  if (total > args.maxFiles) throw new Error(`${total} files exceed the ${args.maxFiles}-file limit of one Worker version; publish the rest through the object store`);
  writeFileSync(join(args.out, ".assetsignore"), ".DS_Store\n");
  console.log(JSON.stringify({ hosts: readdirSync(join(args.out, "h")).length, pages, shared_assets: shared, files: total }));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
