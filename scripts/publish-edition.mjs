#!/usr/bin/env bun
// Read-only public edition compiler over retained items and published CMS design.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const exported = Bun.spawnSync(["cue", "export", join(here, "edition-policy.cue"), "-e", "editionPolicy", "--out", "json"]);
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
export const policy = JSON.parse(exported.stdout.toString());
const css = readFileSync(join(here, "edition.css"), "utf8");
const ways = '<nav class="ways"><a href="/press">the press</a><a href="/edition">the edition</a><a href="/archive">the reading room</a><a href="/field">the field</a></nav>';

export function clean(value) {
  let current = String(value ?? "");
  for (let i = 0; i < 8; i++) {
    const next = current.replace(/\\+(["'/])/g, "$1").replaceAll("\\n", " ").replaceAll("\\t", " ").replace(/\\{2,}/g, "");
    if (next === current) break;
    current = next;
  }
  return current.trim().replace(/\s+/g, " ");
}

const escapeHtml = value => clean(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#x27;");
const safeCss = (value, fallback) => typeof value === "string" && value && !/[;{}<>\x00-\x1f]/.test(value) ? value : fallback;
const title = row => escapeHtml(row?.title ?? "");
const ago = days => Number(days) * 86400;

function pickTemplate(cms, day) {
  const rows = cms.query("SELECT template,reference_name,reference_url,family,hero_mode,typography_mode,density,layout_tokens FROM presentation_template WHERE family=? AND COALESCE(layout_tokens,'')<>'' ORDER BY id").all(policy.family);
  if (!rows.length) return null;
  const selected = rows[day % rows.length];
  try {
    const tokens = JSON.parse(selected.layout_tokens);
    if (!tokens || typeof tokens !== "object" || Array.isArray(tokens)) return null;
    return { template: selected.template, name: selected.reference_name, url: selected.reference_url,
      family: selected.family, hero: selected.hero_mode, type: selected.typography_mode,
      density: selected.density, tokens };
  } catch (_) { return null; }
}

function material(archive) {
  const window = days => `AND at BETWEEN strftime('%s','now') - ${ago(days)} AND strftime('%s','now') + 86400`;
  const cover = archive.query(`SELECT title,body,href,image,publisher,"by",at,source FROM item WHERE COALESCE(image,'') LIKE 'http%' AND length(COALESCE(body,'')) > ? AND COALESCE(publisher,'')<>'' AND length(title) BETWEEN ? AND ? ${window(policy.cover.window_days)} ORDER BY at DESC LIMIT ?`).get(policy.cover.minimum_body_chars, policy.cover.minimum_title_chars, policy.cover.maximum_title_chars, policy.cover.count);
  const strip = archive.query(`SELECT title,href,image,publisher,at FROM item WHERE kind='photo' AND COALESCE(image,'') LIKE 'http%' AND COALESCE(publisher,'')<>'' ${window(policy.strip.window_days)} ORDER BY at DESC LIMIT ?`).all(policy.strip.count);
  const watch = archive.query(`SELECT title,body,href,image,publisher,at FROM item WHERE kind='video' AND COALESCE(image,'') LIKE 'http%' ${window(policy.watch.window_days)} ORDER BY at DESC LIMIT ?`).get(policy.watch.count);
  const listen = archive.query(`SELECT title,body,href,image,publisher,at FROM item WHERE kind='audio' AND COALESCE(image,'') LIKE 'http%' AND COALESCE(publisher,'')<>'' ${window(policy.listen.window_days)} ORDER BY at DESC LIMIT ?`).all(policy.listen.count);
  const reading = archive.query(`SELECT title,MIN(href) AS href,MIN(publisher) AS publisher,MAX(at) AS at FROM item WHERE kind='story' AND COALESCE(publisher,'')<>'' AND length(title) BETWEEN ? AND ? AND title GLOB '*[A-Za-z]*' ${window(policy.reading.window_days)} GROUP BY lower(title) ORDER BY at DESC LIMIT ?`).all(policy.reading.minimum_title_chars, policy.reading.maximum_title_chars, policy.reading.count);
  const kept = archive.query(`SELECT strftime('%Y-%m-%d',at,'unixepoch') AS day,COUNT(*) AS n FROM item WHERE at BETWEEN strftime('%s','now') - ${ago(policy.chart.window_days)} AND strftime('%s','now') + 86400 GROUP BY day ORDER BY day`).all();
  const reach = archive.query("SELECT COUNT(*) AS n,COUNT(DISTINCT publisher) AS publishers FROM item").get();
  return { cover, strip, watch, listen, reading, kept, n: reach.n, publishers: reach.publishers };
}

function dress(tokens = {}) {
  const defaults = { ground: "#0c0d0f", ink: "#f2efe9", display: safeCss(tokens.ink, "#fff"), accent: "#c8a24a",
    muted: "#8a867e", rule: "rgba(0,0,0,.14)", panel: "#fff", face_display: "Georgia,serif",
    face_body: "system-ui,sans-serif", face_mono: "ui-monospace,monospace", display_size: "clamp(2.4rem,6vw,5rem)",
    display_weight: "600", display_tracking: "-.02em", display_leading: "1.04" };
  return `:root{${Object.entries(defaults).map(([key, fallback]) => `--${key.replaceAll("_", "-")}:${safeCss(tokens[key], fallback)};`).join("")}}`;
}

function when(unix) {
  if (!unix) return "";
  const date = new Date(Number(unix) * 1000);
  if (Number.isNaN(date.getTime())) return "";
  const pieces = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "long", hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "UTC" }).formatToParts(date);
  const part = name => pieces.find(item => item.type === name)?.value ?? "";
  return `${part("day")} ${part("month")}, ${part("hour")}:${part("minute")} UTC`;
}

function byline(row) {
  return [row.publisher, row.by, when(row.at)].filter(Boolean).map(part => `<span>${escapeHtml(part)}</span>`).join("");
}

const sourceNote = "This estate read this and kept it. That it is here is this estate's arrangement of its own reading, not the publisher's.";

export function compose(look, items, territory, stamp = new Date()) {
  const section = [];
  section.push('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">');
  section.push('<title>The edition · Aurekai</title>', `<meta name="bonfyre:cannot-infer" content="${escapeHtml(policy.boundary)}">`);
  section.push('<link rel="stylesheet" href="/assets/vendor-uplot.min.css">', `<style>${dress(look?.tokens)}${css}</style>`, '<script src="/assets/client-organs.js" defer></script></head><body><div class="wrap">');
  const printed = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "long", year: "numeric", hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "UTC" }).format(stamp).replace(" at ", " ");
  section.push(`<header class="masthead"><span class="mark">Aurekai · ${escapeHtml(territory)}</span>${ways}<span class="when">${escapeHtml(printed)} UTC</span></header>`);
  const cover = items.cover;
  if (cover) {
    const body = String(cover.body || "").trim();
    section.push(`<section class="cover"><figure><img src="${escapeHtml(cover.image)}" alt="" loading="eager" decoding="async"></figure><h1>`);
    section.push(cover.href ? `<a href="${escapeHtml(cover.href)}" rel="noopener nofollow">${title(cover)}</a>` : title(cover), "</h1>");
    if (body) section.push(`<p class="dek">${escapeHtml(body.slice(0, 420) + (body.length > 420 ? "…" : ""))}</p>`);
    section.push(`<p class="by">${byline(cover)}</p><p class="src">${escapeHtml(sourceNote)}</p></section>`);
  }
  if (items.strip.length) {
    section.push('<hr class="rule"><section><p class="label">Also seen</p><div class="strip">');
    for (const one of items.strip) section.push(`<figure><img src="${escapeHtml(one.image)}" alt="" loading="lazy" decoding="async"><h3>${title(one)}</h3><figcaption>${[one.publisher, when(one.at)].filter(Boolean).map(escapeHtml).join(" · ")}</figcaption></figure>`);
    section.push("</div></section>");
  }
  if (items.watch) {
    const one = items.watch;
    section.push(`<hr class="rule"><section class="watch"><p class="label">Watch</p><a href="${escapeHtml(one.href || "#")}" rel="noopener nofollow"><img src="${escapeHtml(one.image)}" alt="" loading="lazy"></a><h2>${title(one)}</h2><p class="by">${byline(one)}</p><p class="src">A moving picture this estate observed being published. It is played from the publisher, not from here.</p></section>`);
  }
  if (items.listen.length) {
    section.push('<hr class="rule"><section><p class="label">Listen</p><div class="listen">');
    for (const one of items.listen) section.push(`<div class="one"><img src="${escapeHtml(one.image)}" alt="" loading="lazy"><div><h3>${title(one)}</h3><p class="by">${byline(one)}</p></div></div>`);
    section.push("</div></section>");
  }
  if (items.kept.length >= policy.chart.minimum_points) {
    const points = items.kept.map(row => [Date.parse(`${row.day}T00:00:00Z`) / 1000, row.n]);
    section.push(`<hr class="rule"><section><p class="label">What this estate kept, by day</p><div class="kept" data-bf-series='${escapeHtml(JSON.stringify(points))}' data-bf-height="150" data-bf-label="things kept" role="img" aria-label="things kept per day"></div><p class="src">One point per day. Nothing is drawn between them, because a line between two observations is a claim nobody made.</p></section>`);
  }
  if (items.reading.length) {
    section.push('<hr class="rule"><section><p class="label">Reading</p><div class="reading"><ol>');
    for (const one of items.reading) section.push(`<li><h3><a href="${escapeHtml(one.href || "#")}" rel="noopener nofollow">${title(one)}</a></h3><p class="by">${byline(one)}</p></li>`);
    section.push("</ol></div></section>");
  }
  section.push(`<footer class="colophon"><p><span class="big">${Number(items.n).toLocaleString("en-US")}</span>things kept, from <strong>${Number(items.publishers).toLocaleString("en-US")}</strong> publishers.</p>`);
  if (look) section.push(`<p>Dressed by <strong>${escapeHtml(look.name || look.template)}</strong>, one of this estate's own published presentation templates (${escapeHtml(look.family)}, ${escapeHtml(look.hero)} hero, ${escapeHtml(look.type)} typography). The edition changes its dress daily and says which one it is wearing.</p>`);
  section.push(`<p class="src">${escapeHtml(policy.boundary)}</p><p>The whole population this edition is drawn from is at <a href="/field">the field</a>. What reading it costs is at <a href="/economics">economics</a>.</p></footer></div></body></html>`);
  return section.join("");
}

function readEdition(estate, day) {
  const archive = new Database(join(estate, "items.db"), { readonly: true, create: false });
  const cms = new Database(join(estate, "cms.db"), { readonly: true, create: false });
  let look, items;
  try { look = pickTemplate(cms, day); items = material(archive); }
  finally { archive.close(); cms.close(); }
  return { look, items };
}

function writeEdition(out, page) {
  mkdirSync(out, { recursive: true });
  const target = join(out, "edition.html"), temporary = `${target}.partial-${process.pid}-${crypto.randomUUID()}`;
  try { writeFileSync(temporary, page, { flag: "wx" }); renameSync(temporary, target); }
  finally { if (existsSync(temporary)) rmSync(temporary); }
}

function receipt(look, items, page) {
  return { bytes: Buffer.byteLength(page), template: look?.name ?? null, cover: items.cover?.title ?? null,
    strip: items.strip.length, listen: items.listen.length, reading: items.reading.length,
    days_charted: items.kept.length, kept: items.n, publishers: items.publishers, cannot_infer: policy.boundary };
}

export function publish({ estate = join(homedir(), ".bonfyre/estate-local"), out, territory = policy.root_territory, day = Math.floor(Date.now() / 86400000) }) {
  if (!out || !/^[a-z][a-z0-9-]*$/.test(territory)) throw new Error("--out and a valid --territory are required");
  const { look, items } = readEdition(estate, day);
  const page = compose(look, items, territory);
  writeEdition(out, page);
  return receipt(look, items, page);
}

export function publishTerritories({ estate = join(homedir(), ".bonfyre/estate-local"), out, day = Math.floor(Date.now() / 86400000) }) {
  if (!out || !policy.territories.includes(policy.root_territory)) throw new Error("--out and a root territory in edition policy are required");
  const { look, items } = readEdition(estate, day), editions = {};
  let rootPage = null;
  for (const territory of policy.territories) {
    if (!/^[a-z][a-z0-9-]*$/.test(territory)) throw new Error(`invalid authored territory: ${territory}`);
    const page = compose(look, items, territory);
    writeEdition(join(out, "hosts", `${policy.host_root}.${territory}`), page);
    editions[territory] = receipt(look, items, page);
    if (territory === policy.root_territory) rootPage = page;
  }
  writeEdition(out, rootPage);
  return { root_territory: policy.root_territory, editions, source_read_count: 1, cannot_infer: policy.boundary };
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2), args = {};
    while (argv.length) {
      const key = argv.shift();
      if (key === "--all-territories") { args.allTerritories = true; continue; }
      const value = argv.shift();
      if (!value || !["--estate", "--out", "--territory"].includes(key)) throw new Error(`unknown or incomplete argument: ${key}`);
      args[key.slice(2)] = value;
    }
    if (args.allTerritories && args.territory) throw new Error("--all-territories and --territory are mutually exclusive");
    console.log(JSON.stringify(args.allTerritories ? publishTerritories(args) : publish(args), null, 1));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
