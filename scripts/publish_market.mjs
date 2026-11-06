#!/usr/bin/env bun
// Render CMS-owned observed offers through the native CommerceChain owner.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const boundary = "This estate observed a marketplace publish these lots. Nothing here says the thing exists, is in stock, is the seller's to sell, is still obtainable, or carries any price this estate has seen. This estate performs no part of any exchange: it records what it observed and names who would have to act next.";
const esc = value => String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;")
  .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#x27;");

function argsOf(argv) {
  const args = { estate: join(homedir(), ".bonfyre/estate-local"), repo: join(homedir(), "Developer/Bonfyre"), out: "", limit: 60 };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!["--estate", "--repo", "--out", "--limit"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2)] = argv[++i];
  }
  args.limit = Number(args.limit);
  if (!args.out || !Number.isInteger(args.limit) || args.limit < 0) throw new Error("--out and a nonnegative --limit are required");
  return args;
}

function ownerJson(binary, argv) {
  const result = spawnSync(binary, argv, { encoding: "utf8" });
  if (result.status !== 0) return null;
  try { return JSON.parse(result.stdout); } catch (_) { return null; }
}

function lotsFrom(cmsPath, limit) {
  if (!existsSync(cmsPath)) return [];
  const db = new Database(cmsPath, { readonly: true });
  try {
    try {
      return db.query("SELECT offer_id AS exchange, marketplace, title, canonical_href AS href, observed_at, 0 AS price_observed, '' AS currency, cannot_infer FROM service_offer WHERE namespace='root' AND status='published' AND assertion_state='OBSERVED' AND offer_status='observed' ORDER BY observed_at DESC LIMIT ?").all(limit);
    } catch (_) {
      // Only absent owner schema permits the historical compatibility table.
      try { return db.query("SELECT exchange, marketplace, title, href, observed_at, price_observed, currency FROM commerce_lot ORDER BY observed_at DESC LIMIT ?").all(limit); }
      catch (_) { return []; }
    }
  } finally { db.close(); }
}

export function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const commerce = join(args.repo, "cmd/BonfyreCommerce/bonfyre-commerce");
  const store = join(args.estate, "commerce.db");
  const vocab = ownerJson(commerce, ["stages"])?.stages || [];
  const lots = lotsFrom(join(args.estate, "cms.db"), args.limit);
  const now = new Date().toISOString().slice(0, 16).replace("T", " ") + " UTC";
  const out = ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    '<title>Lots this estate has observed · Aurekai</title>',
    '<link rel="stylesheet" href="/assets/tailwind.css">',
    '<link rel="stylesheet" href="/assets/world.css">',
    '<script src="/assets/client-organs.js" defer></script>',
    '<meta name="bonfyre:cannot-infer" content="', esc(boundary), '">',
    '</head><body><main class="wrap">',
    '<header class="region"><h1>Lots this estate has observed</h1>',
    '<p class="src">', esc(boundary), '</p>',
    '<p class="src">Stated ', esc(now), '. What it costs this estate to read anything is at ',
    '<a href="/economics">/economics</a>.</p></header>'];
  if (!lots.length) out.push('<section class="region"><p class="src">No marketplace lot has been observed yet. That is a fact about what this estate has read, not about what is for sale anywhere.</p></section>');
  else {
    out.push(`<section class="region"><h2 class="region-label">${lots.length} lots</h2></section>`);
    for (const lot of lots) {
      const chain = ownerJson(commerce, ["progress", "--db", store, "--exchange", lot.exchange]);
      out.push('<section class="region"><h2>');
      if (String(lot.href || "").startsWith("http"))
        out.push(`<a href="${esc(lot.href)}" rel="noopener nofollow" target="_blank">${esc(lot.title)}</a>`);
      else out.push(esc(lot.title));
      const price = lot.price_observed ? `${lot.price_observed} ${lot.currency} observed` : "price not observed";
      out.push(`</h2><p class="by"><span>${esc(lot.marketplace)}</span><span>observed ${esc(lot.observed_at || "at an unrecorded time")}</span><span>${esc(price)}</span></p>`);
      if (!chain) { out.push('<p class="src">This lot has no chain recorded.</p></section>'); continue; }
      out.push('<h3 class="region-label">Established</h3><ol class="more">');
      for (const row of chain.established || [])
        out.push(`<li><strong>${esc(row.stage)}</strong><span class="by"><span>which does not establish ${esc(row.does_not_establish)}</span></span></li>`);
      out.push('</ol>');
      if (chain.outstanding?.length) {
        out.push('<h3 class="region-label">Not established — and whose move it is</h3><ol class="more">');
        for (const row of chain.outstanding)
          out.push(`<li><strong>${esc(row.stage)}</strong><span class="by"><span>${esc(row.owner)}</span></span></li>`);
        out.push('</ol>');
      }
      out.push(`<p class="src">${esc(lot.cannot_infer || chain.cannot_infer || "")}</p></section>`);
    }
  }
  if (vocab.length) {
    out.push('<section class="region"><h2 class="region-label">The chain itself</h2><p class="src">Fifteen stages, each requiring exactly the one before it. Recording one establishes that stage and nothing later.</p><ol class="more">');
    for (const row of vocab)
      out.push(`<li><strong>${esc(row.stage)}</strong><span class="by"><span>${esc(row.owner)}</span><span>does not establish ${esc(row.does_not_establish)}</span></span></li>`);
    out.push('</ol></section>');
  }
  out.push('</main></body></html>');
  const page = out.join("");
  mkdirSync(args.out, { recursive: true });
  writeFileSync(join(args.out, "market.html"), page);
  console.log(JSON.stringify({ lots: lots.length, stages_in_vocabulary: vocab.length,
    wrote: "market.html", bytes: page.length, cannot_infer: boundary }, null, 1));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
