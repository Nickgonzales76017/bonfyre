#!/usr/bin/env bun
// Marketplace addresses establish observed product/listing stages, never offers or inventory.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const lotPaths = new Map([
  ["bringatrailer.com", /\/listing\//i], ["ebay.com", /\/itm\//i],
  ["etsy.com", /\/listing\//i], ["carsandbids.com", /\/auctions\//i],
  ["hemmings.com", /\/classified/i],
]);
const cannotInfer = "this estate observed a marketplace publish a lot; it is not a claim that the thing exists, is in stock, is theirs to sell, is still obtainable, or has any price this estate has seen, and this estate performs no part of the exchange";

function argsOf(argv) {
  const args = { estate: join(homedir(), ".bonfyre/estate-local"), repo: join(homedir(), "Developer/Bonfyre"),
    source: "", limit: 60_000, cms: "", apply: false };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--apply") { args.apply = true; continue; }
    if (!["--estate", "--repo", "--source", "--limit", "--cms"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2)] = argv[++i];
  }
  args.limit = Number(args.limit);
  if (!Number.isInteger(args.limit) || args.limit < 0) throw new Error("--limit must be a nonnegative integer");
  return args;
}

function hostOf(href) {
  const match = /^https?:\/\/([^/]+)/.exec(href || "");
  const host = match ? match[1].toLowerCase() : "";
  return host.startsWith("www.") ? host.slice(4) : host;
}

function run(binary, argv) {
  return spawnSync(binary, argv, { encoding: "utf8" });
}

function clip(value, length) { return Array.from(String(value || "")).slice(0, length).join(""); }

function report(value) { console.log(JSON.stringify(value, null, 1)); }

export function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const commerce = join(args.repo, "cmd/BonfyreCommerce/bonfyre-commerce");
  const cmsBin = args.cms || join(args.repo, "cmd/BonfyreCMS/bonfyre-cms");
  const schemas = join(args.repo, "cmd/BonfyreCMS/content-types");
  const store = join(args.estate, "commerce.db");
  const cmsPath = join(args.estate, "cms.db");
  const archive = new Database(join(args.estate, "items.db"), { readonly: true });
  let lots = [], editorial = 0;
  const bySource = new Map();
  try {
    const rows = archive.query("SELECT source, key, title, COALESCE(href,'') AS href, COALESCE(publisher,'') AS publisher, COALESCE(at,0) AS at FROM item WHERE COALESCE(href,'') <> '' AND (?1 = '' OR source = ?1) ORDER BY at DESC LIMIT ?2").iterate(args.source, args.limit);
    for (const row of rows) {
      const marketplace = hostOf(row.href);
      const pattern = lotPaths.get(marketplace);
      if (!pattern) continue;
      if (!pattern.test(row.href)) { editorial++; continue; }
      lots.push({ ...row, marketplace });
      bySource.set(row.source, (bySource.get(row.source) || 0) + 1);
    }
  } finally { archive.close(); }
  if (!args.apply) {
    report({ lots: lots.length, editorial_in_the_same_feeds: editorial,
      by_source: Object.fromEntries(bySource), marketplaces_this_estate_can_read: [...lotPaths.keys()].sort(),
      sample: lots.slice(0, 5).map(lot => clip(lot.title, 60)) });
    return 0;
  }
  if (!existsSync(commerce) || !existsSync(cmsBin)) {
    report({ error: "a required owner is not built", commerce, cms: cmsBin });
    return 2;
  }
  const applied = run(cmsBin, ["schema", "apply", join(schemas, "service_offer.json"),
    "--db", cmsPath, "--schemas", schemas]);
  if (applied.status !== 0) {
    report({ error: "service_offer schema refused", detail: (applied.stderr || "").slice(-600) });
    return 2;
  }
  const cms = new Database(cmsPath);
  const findOffer = cms.query("SELECT id FROM service_offer WHERE namespace=? AND offer_id=?");
  let established = 0, refused = 0, ownerWritten = 0, ownerRefused = 0;
  try {
    for (const lot of lots) {
      const exchange = `lot:${lot.marketplace}:${lot.key}`;
      const when = lot.at ? new Date(Number(lot.at) * 1000).toISOString().replace(/\.\d{3}Z$/, "Z") : "";
      const receipt = `foreign/receipts/${lot.source}.json`;
      for (const [stage, owner, detail] of [
        ["product", "ObjectContract / catalogue", clip(lot.title, 200)],
        ["listing", "merchant or marketplace ForeignTwin", `${clip(lot.href, 120)} -- observed in ${lot.source}, receipt ${receipt}`],
      ]) {
        const result = run(commerce, ["record", "--db", store, "--exchange", exchange,
          "--stage", stage, "--owner", owner, "--detail", detail, "--at", when]);
        if (result.status === 0) established++; else refused++;
      }
      if (!when) continue;
      const offer = { offer_id: exchange, source_ref: lot.source, marketplace: lot.marketplace,
        canonical_href: lot.href, title: clip(lot.title, 400), observed_at: when,
        receipt_ref: receipt, assertion_state: "OBSERVED", offer_status: "observed",
        cannot_infer: cannotInfer, status: "published" };
      const existing = findOffer.get("root", exchange);
      const command = ["entry", existing ? "update" : "create", "service_offer"];
      if (existing) command.push(String(existing.id));
      command.push(JSON.stringify(offer), "--ns", "root", "--db", cmsPath, "--schemas", schemas);
      const written = run(cmsBin, command);
      if (written.status === 0) ownerWritten++; else ownerRefused++;
    }
    // Compatibility family refiling remains a local CMS projection; editorial
    // items are never promoted to lots merely because their source is refiled.
    if (lots.length) {
      const update = cms.query("UPDATE internet_source SET family='commerce' WHERE source=? AND status='published'");
      for (const source of [...bySource.keys()].sort()) update.run(source);
    }
  } finally { cms.close(); }
  report({ lots: lots.length, stages_established: established, refused,
    canonical_offers_written: ownerWritten, canonical_offers_refused: ownerRefused,
    editorial_left_alone: editorial, sources_refiled_as_commerce: lots.length ? bySource.size : 0,
    store, cannot_infer: cannotInfer });
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
