#!/usr/bin/env bun
// Compile retained Commons audio through subject, licence, receipt, body and CMS gates.
import { spawnSync } from "node:child_process";
import { existsSync, lstatSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { bodyDigest } from "./columnar_body.mjs";

const apiDefault = "https://commons.wikimedia.org/w/api.php";
const music = /\b(music|musical|song|songs|classical|instrumental|opera|symphon|orchestra|piano|violin|choir|vocal|audio files of music)\b/i;
const denied = /\b(nc|nd|non[- ]commercial|no[- ]derivatives)\b/i;
const mimeExtensions = new Map([["audio/ogg", "ogg"], ["application/ogg", "ogg"], ["audio/flac", "flac"],
  ["audio/mpeg", "mp3"], ["audio/wav", "wav"], ["audio/x-wav", "wav"]]);
const cannot = "a retained Commons file and its declared licence do not establish performer identity, ownership, release order, royalty share, or a delivery grant beyond the stated reusable licence";

export function options(args) {
  const cfg = { cmsBin: "cmd/BonfyreCMS/bonfyre-cms", schemas: "cmd/BonfyreCMS/content-types",
    project: "components/BonfyreProject/bonfyre-project", api: apiDefault, maxBytes: 64 * 1024 * 1024, apply: false };
  const values = new Map([["--estate", "estate"], ["--items", "items"], ["--cms", "cms"],
    ["--cms-bin", "cmsBin"], ["--schemas", "schemas"], ["--project", "project"],
    ["--api", "api"], ["--max-bytes", "maxBytes"], ["--out", "out"]]);
  for (let i = 0; i < args.length; i++) {
    const key = values.get(args[i]);
    if (key) {
      if (!args[i + 1]) throw new Error(`${args[i]} requires a value`);
      cfg[key] = args[++i];
    } else if (args[i] === "--apply") cfg.apply = true;
    else throw new Error(`unknown option: ${args[i]}`);
  }
  if (!cfg.estate) throw new Error("--estate is required");
  cfg.maxBytes = Number(cfg.maxBytes);
  if (!Number.isSafeInteger(cfg.maxBytes) || cfg.maxBytes < 1) throw new Error("invalid --max-bytes");
  cfg.estate = resolve(cfg.estate);
  cfg.items = resolve(cfg.items ?? join(cfg.estate, "items.jsonl"));
  cfg.cms = resolve(cfg.cms ?? join(cfg.estate, "cms.db"));
  cfg.foreign = join(cfg.estate, "foreign");
  cfg.evidence = join(cfg.estate, "media-evidence/commons-recorded-audio");
  return cfg;
}

function decodeHtml(value) {
  const names = { amp: "&", quot: '"', apos: "'", lt: "<", gt: ">", nbsp: " " };
  return String(value ?? "").replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, (entity, code) => {
    if (code.startsWith("#")) {
      const point = code[1]?.toLowerCase() === "x" ? Number.parseInt(code.slice(2), 16) : Number.parseInt(code.slice(1), 10);
      return Number.isInteger(point) && point >= 0 && point <= 0x10ffff ? String.fromCodePoint(point) : entity;
    }
    return names[code.toLowerCase()] ?? entity;
  });
}

function clean(value) { return decodeHtml(value).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim(); }

export function commonsTitle(row) {
  let url;
  try { url = new URL(row.origin_href); } catch { return null; }
  if (url.hostname !== "commons.wikimedia.org") return null;
  let path;
  try { path = decodeURIComponent(url.pathname); } catch { return null; }
  const marker = "/wiki/File:";
  return path.startsWith(marker) ? `File:${path.slice(marker.length)}` : null;
}

export async function metadata(title, api = apiDefault, fetcher = fetch) {
  const url = new URL(api);
  for (const [key, value] of Object.entries({ action: "query", format: "json", formatversion: "2",
    prop: "imageinfo|categories", iiprop: "url|size|mime|extmetadata", cllimit: "500", titles: title }))
    url.searchParams.set(key, value);
  const response = await fetcher(url, { headers: { "User-Agent": "BonfyreMediaCanonicalizer/1.0 (+https://aurekai.work/)",
    Accept: "application/json" }, signal: AbortSignal.timeout(45_000) });
  if (!response.ok) throw new Error(`Commons metadata HTTP ${response.status}`);
  const pages = (await response.json())?.query?.pages;
  if (!Array.isArray(pages) || !pages.length || pages[0].missing) return null;
  const info = pages[0].imageinfo?.[0] ?? {}, meta = info.extmetadata ?? {};
  return { title, page_url: `https://commons.wikimedia.org/wiki/${encodeURIComponent(title.replaceAll(" ", "_"))}`,
    download_url: info.url ?? "", bytes: Number(info.size ?? 0), mime: String(info.mime ?? ""),
    metadata: Object.fromEntries(Object.entries(meta).map(([key, value]) => [key, clean(value?.value)])),
    categories: (pages[0].categories ?? []).map(category => String(category.title ?? "")) };
}

export function licenseLaw(info) {
  const terms = ["LicenseShortName", "UsageTerms", "License", "Copyrighted"]
    .map(key => info.metadata[key] ?? "").join(" | ");
  const normalized = terms.toLowerCase();
  if (denied.test(terms)) return [false, "licence_excludes_public_redistribution", terms];
  if (["cc0", "public domain", "publicdomain"].some(value => normalized.includes(value)))
    return [true, "declared_public_domain_or_cc0", terms];
  if (["cc by", "cc-by", "creativecommons.org/licenses/by"].some(value => normalized.includes(value)))
    return [true, "declared_cc_by_family", terms];
  return [false, "no_explicit_reusable_licence", terms];
}

export function extension(info) {
  const mime = String(info.mime ?? "").toLowerCase();
  if (mimeExtensions.has(mime)) return mimeExtensions.get(mime);
  let path;
  try { path = new URL(info.download_url).pathname.toLowerCase(); } catch { return null; }
  const match = path.match(/\.(ogg|oga|opus|flac|mp3|wav)$/);
  return match ? (["oga", "opus"].includes(match[1]) ? "ogg" : match[1]) : null;
}

export function isMusic(info) {
  return music.test([...info.categories, ...Object.values(info.metadata)].join(" "));
}

function command(args, check = true) {
  const child = spawnSync(args[0], args.slice(1), { encoding: "utf8", timeout: 120_000, maxBuffer: 4 * 1024 * 1024 });
  if (check && (child.error || child.status !== 0))
    throw new Error(`${args.slice(0, 4).join(" ")}: ${(child.error?.message ?? child.stderr ?? "").slice(0, 500)}`);
  return child;
}

function latestReceipt(path) {
  const lines = readFileSync(path, "utf8").split(/\r?\n/).filter(line => line.trimStart().startsWith("{"));
  return lines.length ? JSON.parse(lines.at(-1)) : null;
}

function durationMs(path) {
  const child = command(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", path], false);
  try {
    const duration = Number(JSON.parse(child.stdout).format.duration);
    return Number.isFinite(duration) && duration > 0 ? Math.round(duration * 1000) : null;
  } catch { return null; }
}

function cmsCreate(cfg, kind, value) {
  const child = command([cfg.cmsBin, "entry", "create", kind, JSON.stringify(value), "--db", cfg.cms,
    "--schemas", cfg.schemas], false);
  if (child.error || (child.status !== 0 && !/unique/i.test(`${child.stderr}${child.stdout}`)))
    throw new Error(`CMS ${kind}: ${(child.error?.message ?? child.stderr ?? child.stdout ?? "").slice(0, 500)}`);
}

async function applyOne(plan, cfg) {
  const { row, commons: info } = plan;
  if (!/^[A-Za-z0-9_-]+$/.test(row.key ?? "") || !/^Q[1-9][0-9]*$/.test(row.ident ?? ""))
    throw new Error("retained audio has an unsafe key or Wikidata identifier");
  const twin = `commons-audio-${row.key}`;
  const receipt = join(cfg.foreign, "receipts", `${twin}.json`), body = join(cfg.foreign, "bodies", `${twin}.body`);
  mkdirSync(dirname(receipt), { recursive: true });
  mkdirSync(dirname(body), { recursive: true });
  command([cfg.project, "foreign", "observe", twin, info.download_url, "--receipt", receipt,
    "--body", body, "--observe-right", "public", "--robots", "https://commons.wikimedia.org/robots.txt"]);
  const observed = latestReceipt(receipt);
  if (observed?.policy_decision !== "allow" || !/^[0-9a-f]{64}$/i.test(observed.content_sha256 ?? ""))
    throw new Error("foreign observation did not retain a usable body");
  const actual = resolve(observed.body_path ?? body), root = resolve(cfg.foreign, "bodies");
  if (!actual.startsWith(`${root}${sep}`) || !existsSync(actual) || !lstatSync(actual).isFile() || lstatSync(actual).isSymbolicLink())
    throw new Error("receipted body is unavailable or outside the bounded body root");
  if (await bodyDigest(actual) !== observed.content_sha256)
    throw new Error("receipted body digest does not match its retained bytes");
  const digest = observed.content_sha256, duration = durationMs(actual), extension = plan.extension;
  const evidencePath = join(cfg.evidence, `${digest}.json`);
  mkdirSync(dirname(evidencePath), { recursive: true });
  const evidence = { schema: "bonfyre-commons-recorded-audio-evidence.v1", source_item: row,
    commons: info, licence_decision: plan.licence_decision, receipt: observed, content_sha256: digest,
    generated_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z") };
  writeFileSync(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`);
  const workId = `wikidata-${row.ident.toLowerCase()}`, recordingId = `commons-${digest.slice(0, 24)}`;
  const attribution = `attribution:commons-file:${digest}`, subject = `recording:${recordingId}`;
  cmsCreate(cfg, "music_work", { work_id: workId, title: row.title, evidence_refs: [row.href, info.page_url],
    cannot_infer: cannot, status: "published" });
  const recording = { recording_id: recordingId, work_ref: `work:${workId}`, title: row.title,
    primary_artist_ref: attribution, master_asset_ref: `asset:${digest}`,
    evidence_refs: [evidencePath, row.origin_href], cannot_infer: cannot, status: "published" };
  if (duration) recording.duration_ms = duration;
  cmsCreate(cfg, "music_recording", recording);
  cmsCreate(cfg, "music_right", { right_id: `commons-license-${digest.slice(0, 24)}`, subject_ref: subject,
    right_type: "public_redistribution_under_declared_license", holder_ref: `licensor:commons-file:${digest}`,
    evidence_ref: info.page_url, cannot_infer: cannot, status: "published" });
  cmsCreate(cfg, "music_credit", { credit_id: `commons-attribution-${digest.slice(0, 24)}`, subject_ref: subject,
    actor_ref: attribution, role: "source_attribution",
    credited_as: info.metadata.Artist || info.metadata.Credit || "Commons file attribution retained in evidence",
    evidence_ref: info.page_url,
    cannot_infer: "a Commons attribution string is not a resolved person, ownership claim, employment relation, or royalty share",
    status: "published" });
  const program = { program_id: `${recordingId}:audio`, subject_ref: subject, media_kind: "audio",
    asset_ref: `/assets/twin/${digest}.${extension}`, publication_state: "published",
    cannot_infer: cannot, status: "published" };
  if (duration) program.duration_ms = duration;
  cmsCreate(cfg, "media_program", program);
  return { recording_id: recordingId, work_id: workId, digest,
    asset_ref: program.asset_ref, duration_ms: duration, evidence: evidencePath };
}

function checkpoint(cfg, rows, results) {
  const summary = { schema: "bonfyre-commons-recorded-audio-promotion.v1", candidates: rows.length,
    admitted: results.filter(result => result.state === "ADMITTED").length,
    eligible: results.filter(result => result.state === "ADMIT").length,
    refused: results.filter(result => result.state === "REFUSED").length,
    processed: results.length, results,
    cannot_infer: "a retained row, a Commons file, its licence, a Work, a Recording, a credit, a right, a program, and a playback event are distinct claims" };
  if (cfg.out) {
    const out = resolve(cfg.out), stage = `${out}.staging`;
    mkdirSync(dirname(out), { recursive: true });
    writeFileSync(stage, `${JSON.stringify(summary, null, 2)}\n`);
    renameSync(stage, out);
  }
  return summary;
}

export async function promote(cfg, fetcher = fetch) {
  if (cfg.apply) command([cfg.cmsBin, "schema", "migrate", "--db", cfg.cms, "--schemas", cfg.schemas]);
  const rows = readFileSync(cfg.items, "utf8").split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
    .filter(row => row.source === "wikidata-recorded-music" && row.kind === "audio");
  const results = [];
  for (const row of rows) {
    const result = { key: row.key, title: row.title, state: "REFUSED" };
    if (!row.ident || !row.origin_href) result.reason = "missing_wikidata_subject_or_commons_file";
    else {
      const title = commonsTitle(row);
      if (!title) result.reason = "origin_is_not_a_commons_file_page";
      else {
        let info;
        try { info = await metadata(title, cfg.api, fetcher); }
        catch (error) { result.reason = "commons_metadata_unavailable"; result.detail = error.message.slice(0, 180); }
        if (!result.reason) {
          if (!info?.download_url) result.reason = "commons_file_unavailable";
          else {
            const ext = extension(info);
            if (!ext) result.reason = "not_an_admitted_audio_carrier";
            else if (!Number.isSafeInteger(info.bytes) || info.bytes <= 0 || info.bytes > cfg.maxBytes) {
              result.reason = "outside_physical_carrier_limit"; result.bytes = info.bytes;
            } else if (!isMusic(info)) result.reason = "commons_metadata_does_not_prove_music_subject";
            else {
              const [reusable, law, terms] = licenseLaw(info);
              if (!reusable) { result.reason = law; result.licence = terms; }
              else {
                Object.assign(result, { state: "ADMIT", commons_file: info.page_url,
                  bytes: info.bytes, licence: terms, extension: ext });
                if (cfg.apply) {
                  try {
                    result.admission = await applyOne({ row, commons: info, extension: ext,
                      licence_decision: { law, terms } }, cfg);
                    result.state = "ADMITTED";
                  } catch (error) { result.state = "REFUSED"; result.reason = "promotion_failed";
                    result.detail = error.message.slice(0, 500); }
                }
              }
            }
          }
        }
      }
    }
    results.push(result);
    checkpoint(cfg, rows, results);
  }
  return checkpoint(cfg, rows, results);
}

if (import.meta.main) {
  try { console.log(JSON.stringify(await promote(options(process.argv.slice(2))), null, 2)); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
