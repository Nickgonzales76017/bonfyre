#!/usr/bin/env bun
// Publish metered accrual without promoting it to billing or spending authority.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const boundary = "Accrued use is what this estate's reading would cost at rates it has admitted with evidence. It is not money billed, paid or booked, not an amount owed to anyone, and not authority to spend. Use for which no rate has been admitted is counted here and left unpriced, never counted as free.";
const esc = value => String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;")
  .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#x27;");

function argsOf(argv) {
  const args = { estate: join(homedir(), ".bonfyre/estate-local"), out: "" };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!["--estate", "--out"].includes(key) || !argv[i + 1]) throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2)] = argv[++i];
  }
  if (!args.out) throw new Error("--out is required");
  return args;
}

function readMeter(path) {
  if (!existsSync(path)) return null;
  const db = new Database(path, { readonly: true });
  try {
    const byProvider = db.query("SELECT COALESCE(NULLIF(e.provider,''),'(none named)') AS provider, e.op, COALESCE(r.currency,'') AS currency, COUNT(*) AS events, SUM(e.cost IS NULL) AS unpriced, SUM(e.cost IS NOT NULL) AS priced, printf('%.4f', COALESCE(SUM(e.cost),0)) AS accrued, COALESCE(r.unit,'') AS rate_unit FROM events e LEFT JOIN rate r ON r.id=e.rate_id GROUP BY e.provider, e.op, r.currency, r.unit ORDER BY events DESC, provider, e.op").all();
    const byKey = db.query("SELECT e.key_id, COALESCE(r.currency,'') AS currency, COUNT(*) AS events, SUM(e.cost IS NULL) AS unpriced, SUM(e.cost IS NOT NULL) AS priced, printf('%.4f', COALESCE(SUM(cost),0)) AS accrued FROM events e LEFT JOIN rate r ON r.id=e.rate_id GROUP BY e.key_id, r.currency ORDER BY events DESC, e.key_id LIMIT 40").all();
    const rates = db.query("SELECT provider, op, unit, amount, currency, effective_at, evidence_ref FROM rate ORDER BY provider, op").all();
    const totals = db.query("SELECT COUNT(*) AS n, SUM(cost IS NULL) AS unpriced, SUM(trim(receipt_ref) <> '') AS receipted, SUM(trim(physical_attempt_ref) <> '') AS identified_attempts, SUM(trim(realization_ref) <> '') AS identified_realizations FROM events").get();
    const series = [];
    for (const row of db.query("SELECT substr(timestamp,1,10) AS day, COUNT(*) AS events, COALESCE(SUM(cost),0) AS accrued FROM events WHERE timestamp <> '' GROUP BY day ORDER BY day").all()) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(row.day)) continue;
      const when = Date.parse(`${row.day}T00:00:00Z`);
      if (!Number.isNaN(when) && new Date(when).toISOString().slice(0, 10) === row.day)
        series.push([when / 1000, row.events]);
    }
    return { by_provider: byProvider, by_key: byKey, rates, events: totals.n,
      unpriced: totals.unpriced || 0, receipted: totals.receipted || 0,
      identified_attempts: totals.identified_attempts || 0,
      identified_realizations: totals.identified_realizations || 0, series };
  } finally { db.close(); }
}

function readPrograms(path, siteId = null) {
  if (!existsSync(path)) return [];
  const db = new Database(path, { readonly: true });
  try {
    return db.query("SELECT i.instance_id, i.owner_subject, i.site_id, i.archetype, (SELECT COUNT(*) FROM program_requirement q WHERE q.instance_id=i.instance_id AND q.state='open' AND q.status='published') AS open_requirements, (SELECT COUNT(*) FROM funding_work_package p WHERE p.instance_id=i.instance_id AND p.status='published') AS work_packages FROM program_instance i WHERE i.status='published' AND (?1 IS NULL OR i.site_id=?1) ORDER BY i.instance_id").all(siteId);
  } catch (_) { return []; }
  finally { db.close(); }
}

function page(meter, programs, subject) {
  const who = subject || "this estate";
  const now = new Date().toISOString().slice(0, 16).replace("T", " ") + " UTC";
  const out = ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    `<title>What ${esc(who)} spends · Aurekai</title>`,
    '<link rel="stylesheet" href="/assets/tailwind.css">',
    '<link rel="stylesheet" href="/assets/world.css">',
    '<link rel="stylesheet" href="/assets/vendor-uplot.min.css">',
    '<script src="/assets/client-organs.js" defer></script>',
    `<meta name="bonfyre:cannot-infer" content="${esc(boundary)}">`,
    '</head><body><main class="wrap">',
    `<header class="region"><h1>What ${esc(who)} spends</h1>`,
    `<p class="src">${esc(boundary)}</p>`,
    `<p class="src">Stated ${esc(now)}.</p></header>`];
  if (!meter || !meter.events) out.push('<section class="region"><p class="src">Nothing has been metered yet. That is a fact about what this estate has recorded, not a claim that it has spent nothing.</p></section>');
  else {
    if (meter.series.length > 1) {
      const series = JSON.stringify(meter.series).replaceAll(",", ", ");
      out.push(`<section class="region"><h2 class="region-label">What was metered, by day</h2><div data-bf-series='${series}' data-bf-label="metered events" data-bf-height="160" role="img" aria-label="metered events per day, ${meter.series.length} days"></div><p class="src">One point per day this estate metered something. Nothing is interpolated between them: a line drawn between two observations would be a claim nobody made.</p></section>`);
    }
    out.push('<section class="region"><h2 class="region-label">By provider</h2><ol class="more">');
    for (const row of meter.by_provider) {
      const accrued = row.priced ? `${row.accrued} ${row.currency}` : "unpriced";
      out.push(`<li><strong>${esc(row.provider)}</strong> · ${esc(row.op)}<span class="by"><span>${row.events} events</span><span>${esc(accrued)}</span>${row.unpriced ? `<span>${row.unpriced} unpriced</span>` : ""}</span></li>`);
    }
    out.push('</ol></section>');
    out.push('<section class="region"><h2 class="region-label">By what was being done</h2><ol class="more">');
    for (const row of meter.by_key) {
      const accrued = row.priced ? `${row.accrued} ${row.currency} accrued` : "unpriced";
      out.push(`<li><strong>${esc(row.key_id)}</strong><span class="by"><span>${row.events} events</span><span>${esc(accrued)}</span>${row.unpriced ? `<span>${row.unpriced} unpriced</span>` : ""}</span></li>`);
    }
    out.push('</ol></section>');
    out.push('<section class="region"><h2 class="region-label">The rates this estate has admitted</h2><p class="src">A price may only enter this store with the evidence that admits it. Anything not listed here is unpriced wherever it appears above.</p><ol class="more">');
    for (const row of meter.rates)
      out.push(`<li><strong>${esc(row.provider)} · ${esc(row.op)}</strong> — ${esc(row.amount)} ${esc(row.currency)} per ${esc(row.unit)} from ${esc(row.effective_at)}<span class="by"><span>${esc(row.evidence_ref)}</span></span></li>`);
    out.push('</ol></section>');
    if (meter.unpriced) out.push(`<section class="region"><p class="src">${meter.unpriced} of ${meter.events} metered events carry no admitted rate and are counted without a price.</p></section>`);
    out.push(`<section class="region"><h2 class="region-label">Evidence coverage</h2><p class="src">${meter.receipted} of ${meter.events} events retain a receipt reference; ${meter.identified_attempts} bind a stable physical attempt and ${meter.identified_realizations} name a realization. Earlier rows without the newer attempt/realization coordinates remain visible as legacy recorded usage rather than being silently upgraded.</p></section>`);
  }
  if (programs.length) {
    out.push('<section class="region"><h2 class="region-label">Programs running here</h2><p class="src">Each names funding streams it could be paid by, kept as distinct scopes so one effort supported several ways can be told apart from one cost claimed several times. No scope or cost code has been stated for any of them, so nothing can be allocated to any stream.</p><ol class="more">');
    for (const row of programs)
      out.push(`<li><strong>${esc(row.instance_id)}</strong><span class="by"><span>${esc(row.archetype)}</span><span>${row.work_packages} work packages</span><span>${row.open_requirements} open requirements</span></span></li>`);
    out.push('</ol></section>');
  }
  out.push('</main></body></html>');
  return out.join("");
}

export function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const meter = readMeter(join(args.estate, "meter.db"));
  const cmsPath = join(args.estate, "cms.db");
  const written = [];
  mkdirSync(args.out, { recursive: true });
  writeFileSync(join(args.out, "economics.html"), page(meter, readPrograms(cmsPath), null));
  written.push("economics.html");
  if (existsSync(cmsPath)) {
    const db = new Database(cmsPath, { readonly: true });
    let hosts = [];
    try { hosts = db.query("SELECT DISTINCT p.host, p.site_id, p.subject_address FROM site_presentation p JOIN program_instance i ON i.site_id=p.site_id AND i.status='published' WHERE p.status='published' AND p.host LIKE '%.aurekai.%'").all(); }
    catch (_) { /* legacy CMS without the owner tables */ }
    finally { db.close(); }
    for (const row of hosts) {
      const folder = join(args.out, "hosts", row.host);
      mkdirSync(folder, { recursive: true });
      writeFileSync(join(folder, "economics.html"), page(meter, readPrograms(cmsPath, row.site_id), row.subject_address));
      written.push(`hosts/${row.host}/economics.html`);
    }
  }
  console.log(JSON.stringify({ pages: written.length, wrote: written.slice(0, 6),
    metered_events: meter?.events || 0, cannot_infer: boundary }, null, 1));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
