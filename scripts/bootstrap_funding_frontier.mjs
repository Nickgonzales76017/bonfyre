#!/usr/bin/env bun
// ProgrammeFabric declarations plus bounded Grants.gov observations; no application effect.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const defaultEndpoint = "https://api.grants.gov/v1/api/search2";
const boundary = "A programme's funding candidate is a declared possible stream. A Grants.gov result is an external search observation. Neither establishes eligibility, an application, an award, available money, authority to submit, or permission to spend.";
const vocabulary = {
  "CDC PHIG": "Public Health Infrastructure Grant", "CDC/health partner": "public health infrastructure",
  "CISA SLCGP": "State Local Cybersecurity Grant Program", "DOE GRIP/SPARK": "Grid Resilience Innovation Partnerships",
  "DOL SAEF": "apprenticeship expansion", "EDA Build to Scale": "Build to Scale",
  "FMPP": "Farmers Market Promotion Program", "HRSA Rural Health Network": "Rural Health Network Development",
  "HUD Automated Permitting": "automated permitting housing", "HUD Lead Hazard Reduction": "Lead Hazard Reduction",
  "HUD PRO Housing": "PRO Housing", "Healthy Homes": "Healthy Homes",
  "IMLS NLG-L": "National Leadership Grants Libraries", "NIA/NIH SBIR": "Small Business Innovation Research health",
  "NSF PESOSE": "Pathways Enable Secure Open Source Ecosystems", "NSF S&CC": "Smart Connected Communities",
  "NSF-SBIR": "NSF Small Business Innovation Research", "PESOSE": "Pathways Enable Secure Open Source Ecosystems",
  "S&CC": "Smart Connected Communities", "SS4A": "Safe Streets Roads for All",
  "USDA DLT": "Distance Learning Telemedicine", "USDA LFPP": "Local Food Promotion Program",
  "USDA-SBIR Rural Development": "Small Business Innovation Research Rural Development",
};
const nowIso = () => new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
const esc = value => String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;")
  .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#x27;");
const text = value => String(value || "");

function argsOf(argv) {
  const args = { cms: join(homedir(), ".bonfyre/estate-local/cms.db"), out: "", rows: 4,
    timeout: 30, offline: false, endpoint: process.env.BONFYRE_GRANTS_SEARCH_URL || defaultEndpoint };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--offline") { args.offline = true; continue; }
    if (!["--cms", "--out", "--rows", "--timeout", "--endpoint"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2)] = argv[++i];
  }
  args.rows = Number(args.rows);
  args.timeout = Number(args.timeout);
  if (!args.out || !Number.isInteger(args.rows) || args.rows < 1 || args.rows > 25 ||
      !Number.isInteger(args.timeout) || args.timeout < 1 || !existsSync(args.cms))
    throw new Error("--out and an existing --cms are required; --rows must be 1..25 and --timeout positive");
  return args;
}

function jsonValue(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === "object") return Object.keys(value);
  try {
    const parsed = JSON.parse(value || "[]");
    return Array.isArray(parsed) ? parsed : parsed && typeof parsed === "object" ? Object.keys(parsed) : [];
  } catch (_) { return []; }
}

function programmesFrom(cmsPath) {
  const db = new Database(cmsPath, { readonly: true });
  try {
    const instances = db.query("SELECT i.instance_id, i.owner_subject, i.site_id, i.archetype, t.name, t.description, t.semantic_slots, t.funding_candidates FROM program_instance i JOIN program_template t ON t.template_id=i.template_id WHERE i.status='published' AND t.status='published' ORDER BY i.instance_id").all();
    const packageRows = db.query("SELECT instance_id, funding_program FROM funding_work_package WHERE status='published' ORDER BY instance_id, funding_program").all();
    const packages = new Map();
    for (const row of packageRows) {
      const name = text(row.funding_program).trim();
      if (!name) continue;
      if (!packages.has(row.instance_id)) packages.set(row.instance_id, []);
      if (!packages.get(row.instance_id).includes(name)) packages.get(row.instance_id).push(name);
    }
    return instances.map(row => {
      const declared = jsonValue(row.funding_candidates).map(value => text(value).trim()).filter(Boolean);
      const work = packages.get(row.instance_id) || [];
      return { instance_id: row.instance_id, owner_subject: row.owner_subject, site_id: row.site_id,
        archetype: row.archetype, name: row.name, description: row.description,
        semantic_slots: jsonValue(row.semantic_slots).map(value => text(value).trim()).filter(Boolean),
        funding_candidates: work, template_candidates: declared, package_count: work.length,
        candidate_contract_matches_packages: JSON.stringify([...work].sort()) === JSON.stringify([...declared].sort()) };
    });
  } finally { db.close(); }
}

function editionContract(programme) {
  const named = programme.semantic_slots.length ? programme.semantic_slots.slice(0, 5).join(", ") :
    "the material this institution chooses to connect";
  return {
    public: { title: "Public edition", description: `A named, useful publication anyone can browse: ${named}.` },
    connected: { title: "Connected edition", description: "People with a real relationship receive their own saved context, relevant updates and lawful actions." },
    operator: { title: "Operator edition", description: "An authorized team works from evidence, exceptions, outcomes and the work that remains." },
  };
}

function pilotRequestUrl(programme) {
  const title = `Aurekai pilot — ${programme.name}`;
  const body = `## Pilot conversation\n\nI want to discuss a living publication for **${programme.name}**.\n\n- Institution / community:\n- My role and authority to explore a pilot:\n- What public material should be useful first:\n- Who needs a connected or operator edition:\n- Relevant deadline, budget, or funding route (if any):\n\nSubmitting this request opens a conversation only. It does not form an agreement, grant application, payment, or authority delegation.`;
  const quotePlus = value => encodeURIComponent(value).replace(/[!'()*]/g,
    char => `%${char.charCodeAt(0).toString(16).toUpperCase()}`).replace(/%20/g, "+");
  return `https://github.com/Nickgonzales76017/bonfyre/issues/new?title=${quotePlus(title)}&body=${quotePlus(body)}&labels=pilot`;
}

export async function observeFundingCandidate(candidate, args, { request = fetch } = {}) {
  const query = vocabulary[candidate] || candidate;
  const observedAt = nowIso();
  const payload = JSON.stringify({ rows: Math.max(args.rows, 25), keyword: query, oppStatuses: "forecasted|posted" });
  try {
    const response = await request(args.endpoint, { method: "POST", signal: AbortSignal.timeout(args.timeout * 1000),
      headers: { "Content-Type": "application/json", Accept: "application/json",
        "User-Agent": "BonfyreFundingFrontier/1.0" }, body: payload });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    // The existing funding owner supplies the HTTP/evidence contract. Bound
    // the provider response before allocating any JSON or retaining its hash.
    const maximum = 2 * 1024 * 1024;
    if (Number(response.headers?.get('content-length')) > maximum) throw new Error('funding response exceeds 2 MiB');
    const reader = response.body?.getReader();
    if (!reader) throw new Error('funding response is not a readable byte stream');
    const parts = []; let size = 0;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > maximum) { await reader.cancel().catch(() => {}); throw new Error('funding response exceeds 2 MiB'); }
        parts.push(Buffer.from(value));
      }
    } finally { reader.releaseLock(); }
    const raw = Buffer.concat(parts, size);
    const digest = "sha256:" + createHash("sha256").update(raw).digest("hex");
    const data = JSON.parse(raw.toString("utf8")).data || {};
    const hits = data.oppHits || [];
    const compact = hits.map(hit => ({ opportunity_id: text(hit.id), number: text(hit.number),
      title: text(hit.title), agency_code: text(hit.agencyCode), agency: text(hit.agency || hit.agencyName),
      open_date: text(hit.openDate), close_date: text(hit.closeDate), provider_status: text(hit.oppStatus),
      document_type: text(hit.docType), assistance_listings: hit.cfdaList || hit.alnist || [] }));
    const terms = new Set(query.replaceAll("/", " ").replaceAll("-", " ").split(/\s+/)
      .map(word => word.toLowerCase()).filter(word => word.length > 2 && !["and", "for", "the", "program"].includes(word)));
    const relevance = hit => {
      const haystack = `${hit.title} ${hit.agency} ${hit.number}`.toLowerCase();
      let count = 0;
      for (const term of terms) if (haystack.includes(term)) count++;
      return count;
    };
    compact.sort((a, b) => relevance(b) - relevance(a) || a.title.toLowerCase().localeCompare(b.title.toLowerCase()));
    return { candidate, query, source: args.endpoint, observed_at: observedAt,
      http_status: response.status, response_digest: digest, provider_hit_count: Number(data.hitCount || 0),
      returned: compact.slice(0, args.rows), observation_state: "observed" };
  } catch (error) {
    return { candidate, query, source: args.endpoint, observed_at: observedAt,
      observation_state: "unavailable", reason: String(error), returned: [] };
  }
}

function renderHtml(report) {
  const out = [
    '<!doctype html><html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    '<title>Funding frontier · Aurekai</title>',
    '<meta name="description" content="Declared programme funding streams matched against current public Grants.gov search observations.">',
    `<meta name="bonfyre:cannot-infer" content="${esc(boundary)}">`,
    '<link rel="stylesheet" href="/assets/tailwind.css"><link rel="stylesheet" href="/assets/world.css">',
    '<style>.funding-grid{display:grid;gap:18px;grid-template-columns:repeat(auto-fit,minmax(290px,1fr))}.funding-card{border:1px solid currentColor;padding:20px;border-radius:12px}.funding-card h2{margin:.2em 0}.rail{display:inline-block;border:1px solid currentColor;border-radius:999px;padding:3px 8px;margin:3px 4px 3px 0;font:12px ui-monospace,monospace}.boundary{border-left:4px solid #bd7b2a;padding:12px 16px;background:#fff8e8}.opportunity{margin:12px 0;padding-top:12px;border-top:1px solid #ccd}.quiet{color:#667085;font-size:.92em}a{overflow-wrap:anywhere}.edition{margin:14px 0;padding:12px 0;border-top:1px solid #d8ddd8}.edition h3{font-size:1rem;margin:0 0 .25rem}.action{display:inline-block;background:#183b2a;color:#fff!important;padding:10px 14px;border-radius:6px;text-decoration:none;font-weight:700}.action:hover{background:#28543d}</style>',
    '</head><body><main class="wrap"><header class="region"><p class="src">Aurekai / living publications</p><h1>Build a publication people can actually use.</h1>',
    `<p class="src">${esc(report.programme_count)} real institutional pilot shapes · ${esc(report.work_package_count)} separately scoped funding work packages · ${esc(report.candidate_count)} declared public funding streams.</p></header>`,
    '<section class="region"><p>Every pilot begins as a living publication: a useful public edition, a relationship-aware edition for the people it serves, and an operator edition for the people responsible for it. The underlying system stays out of the way.</p><p>A city can start with a service guide. A library can start with a local knowledge edition. A creator, school, household, business, place, or community can start with the thing people come to them for.</p></section>',
    `<section class="region boundary"><strong>Funding boundary.</strong> ${esc(boundary)} A pilot request is a conversation, not an application, award, payment, agreement, or authority delegation.</section>`,
    '<section class="region"><h2>Choose a pilot shape</h2><p class="src">These are published institutional programme declarations rendered as human products. Their distinct funding work packages remain visible below, but funding is never a substitute for a partner or a useful first edition.</p><div class="funding-grid">',
  ];
  for (const programme of report.programmes) {
    out.push(`<article class="funding-card"><p class="quiet">${esc(programme.archetype)}</p><h2>${esc(programme.name)}</h2>`);
    if (programme.description) out.push(`<p>${esc(programme.description)}</p>`);
    for (const edition of Object.values(programme.edition_contract))
      out.push(`<section class="edition"><h3>${esc(edition.title)}</h3><p>${esc(edition.description)}</p></section>`);
    out.push('<p class="quiet">Declared funding work packages</p><p>');
    for (const candidate of programme.funding_candidates) out.push(`<span class="rail">${esc(candidate)}</span>`);
    out.push('</p>');
    if (!programme.candidate_contract_matches_packages)
      out.push('<p class="quiet">Template/package declaration mismatch; this is a compiler consistency signal, not a funding conclusion.</p>');
    out.push(`<a class="action" href="${esc(programme.pilot_request_url)}" rel="noopener noreferrer" target="_blank">Start a pilot conversation ↗</a></article>`);
  }
  out.push('</div></section><section class="region"><h2>Observed public opportunity frontier</h2>',
    "<p class=\"src\">Search observations are grouped by declared stream. Provider status is the provider's label and may change; no result has been scored as eligible.</p>");
  for (const candidate of Object.keys(report.observations).sort()) {
    const observation = report.observations[candidate];
    out.push(`<article class="funding-card"><h2>${esc(candidate)}</h2><p class="quiet">${esc(observation.observation_state)} · observed ${esc(observation.observed_at)} · provider reported ${esc(observation.provider_hit_count || 0)} matching records</p>`);
    if (observation.observation_state !== "observed")
      out.push(`<p>Provider observation unavailable: ${esc(observation.reason)}. It is not treated as zero opportunities.</p>`);
    else if (!observation.returned.length)
      out.push('<p>No bounded result was returned for this exact query. That is not a funding verdict.</p>');
    else for (const hit of observation.returned) {
      const number = hit.number || hit.opportunity_id;
      const url = hit.opportunity_id ? "https://www.grants.gov/search-results-detail/" + encodeURIComponent(hit.opportunity_id) :
        "https://www.grants.gov/search-grants";
      out.push(`<div class="opportunity"><strong>${esc(hit.title)}</strong><br><span class="quiet">${esc(hit.agency || hit.agency_code)} · ${esc(number)} · ${esc(hit.provider_status)} · closes ${esc(hit.close_date || "not stated")}</span><br><a href="${esc(url)}" rel="noopener noreferrer" target="_blank">Open primary opportunity ↗</a></div>`);
    }
    out.push('</article>');
  }
  out.push(`</section><footer class="region"><p class="src">Generated ${esc(report.generated_at)}. Source: <a href="https://www.grants.gov/api/api-guide">Grants.gov Applicant API</a>. <a href="/funding-frontier.json">Machine-readable receipt</a> · <a href="/economics">Economics</a> · <a href="/capability">Capabilities</a></p></footer></main></body></html>`);
  return out.join("");
}

export async function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const programmes = programmesFrom(args.cms);
  const candidates = [...new Set(programmes.flatMap(programme => programme.funding_candidates))].sort();
  const observations = {};
  for (const candidate of candidates) {
    observations[candidate] = args.offline ? {
      candidate, query: vocabulary[candidate] || candidate, source: args.endpoint,
      observed_at: nowIso(), observation_state: "not_observed", returned: [],
      reason: "offline compilation requested",
    } : await observeFundingCandidate(candidate, args);
  }
  for (const programme of programmes) {
    programme.edition_contract = editionContract(programme);
    programme.pilot_request_url = pilotRequestUrl(programme);
  }
  const report = {
    schema: "bonfyre-funding-frontier.v1", generated_at: nowIso(),
    effect_class: "READ_ONLY_EXTERNAL_OBSERVATION",
    source: { provider: "Grants.gov", endpoint: args.endpoint, authentication: "none" },
    programme_count: programmes.length,
    work_package_count: programmes.reduce((total, programme) => total + programme.package_count, 0),
    candidate_count: candidates.length, programmes, observations,
    cannot_infer: ["a funding candidate -> programme eligibility",
      "a public opportunity result -> an application, award, available money, or authority to submit",
      "a missing/unavailable provider observation -> no funding exists",
      "a funding prospect -> spend authority"],
    boundary,
  };
  mkdirSync(args.out, { recursive: true });
  writeFileSync(join(args.out, "funding-frontier.json"), JSON.stringify(report, null, 2) + "\n");
  writeFileSync(join(args.out, "funding.html"), renderHtml(report));
  const observed = Object.values(observations).filter(row => row.observation_state === "observed").length;
  console.log(JSON.stringify({ schema: report.schema, programmes: programmes.length,
    work_packages: report.work_package_count, candidates: candidates.length,
    observed, out: args.out, effect_performed: false }, null, 1));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
