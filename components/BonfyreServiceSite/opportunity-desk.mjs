// Requirements read from buyers' own documents, worked two ways. For a sale:
// how well each requirement is covered, how the options compare once the
// buyer's priorities weigh them (and how far a weight would have to move to
// change the winner), and what a proof of concept actually showed. For a
// product: which needs recur across buyers, ranked by reach, impact,
// confidence and effort, with a check of how stable that ranking is. Every
// requirement keeps the exact sentence and where it came from.

export const THEMES = {
  'Editing and publishing': /\b(content management|cms|publish|update|edit|author|workflow|approval)\w*/i,
  'Accessibility': /\b(accessib\w*|ada|wcag|508|screen reader)\b/i,
  'Design and brand': /\b(design|brand|visual|layout|graphic|look and feel)\w*/i,
  'Hosting and support': /\b(host\w*|maintenance|support|uptime|security|backup|patch\w*|plugin)\b/i,
  'Search and discovery': /\b(seo|search|findab\w*|navigation|navigate|find)\b/i,
  'Social and outreach': /\b(social media|post\w*|newsletter|email|campaign|outreach)\b/i,
  'Reporting and measurement': /\b(analytic\w*|report\w*|metric\w*|measure\w*|dashboard)\b/i,
  'Migration and integration': /\b(migrat\w*|integrat\w*|third[- ]party|api|import)\b/i,
  'Training and handover': /\b(train\w*|documentation|handover|empower\w*)\b/i,
};
export const COVERAGE = { covered: 1, partly: 0.5, planned: 0.25, no: 0 };
const PRIORITY_WEIGHT = { must: 3, should: 2, could: 1 };

// ------------------------------------------------------------ reading a document

export function plainText(html) {
  return String(html).replace(/<(script|style|nav|header|footer|noscript)[^>]*>[\s\S]*?<\/\1>/gi, ' ').replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&#8217;|&rsquo;/g, '’').replace(/&#8216;|&lsquo;/g, '‘').replace(/&#8220;|&ldquo;/g, '“').replace(/&#8221;|&rdquo;/g, '”').replace(/&#8211;|&ndash;/g, '–').replace(/&#8212;|&mdash;/g, '—').replace(/&#039;|&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ').trim();
}
export const priorityOf = s => /\b(must|shall|is required|are required|required to|will be required)\b/i.test(s) ? 'must' : /\b(should|will need|expected|requires)\b/i.test(s) ? 'should' : 'could';
export const themeOf = s => Object.entries(THEMES).find(([, re]) => re.test(s))?.[0] ?? 'Other';

// Sentences that ask for something, with their exact offsets in the document's plain text.
export function requirements(text, source) {
  const out = [], re = /[^.;!?]+(?:[.;!?]|$)/g;
  for (let m; (m = re.exec(text));) {
    const raw = m[0], lead = raw.length - raw.trimStart().length, s = raw.trim();
    if (s.length < 40 || s.length > 450 || !/\b(must|shall|should|required|requires|will need|expected to)\b/i.test(s)) continue;
    out.push({ id: `${source}#${m.index + lead}`, source, start: m.index + lead, end: m.index + lead + s.length, text: s, priority: priorityOf(s), theme: themeOf(s), coverage: '', note: '' });
  }
  return out;
}
export const verify = (texts, r) => texts[r.source]?.slice(r.start, r.end) === r.text;

// ------------------------------------------------------------ the sale

export function coverage(reqs) {
  const counted = reqs.filter(r => r.coverage), weight = r => PRIORITY_WEIGHT[r.priority];
  const total = counted.reduce((a, r) => a + weight(r), 0), got = counted.reduce((a, r) => a + weight(r) * COVERAGE[r.coverage], 0);
  const gaps = reqs.filter(r => r.priority === 'must' && (r.coverage === 'no' || r.coverage === 'planned'));
  return { assessed: counted.length, of: reqs.length, score: total ? got / total : 0, gaps, unassessedMust: reqs.filter(r => r.priority === 'must' && !r.coverage).length };
}

// Options scored 0–5 per theme; a theme's weight is the sum of its requirements' priority weights unless overridden.
export function themeWeights(reqs, overrides = {}) {
  const w = {}; for (const r of reqs) w[r.theme] = (w[r.theme] ?? 0) + PRIORITY_WEIGHT[r.priority];
  return { ...w, ...Object.fromEntries(Object.entries(overrides).filter(([, v]) => Number.isFinite(v))) };
}
function finiteNumber(value, label, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (value === null || value === void 0 || value === "")
    return null;
  if (typeof value === "string" && !/^\d+(?:\.\d+)?$/.test(value))
    throw Error(label + " needs a finite number.");
  if (!["number", "string"].includes(typeof value))
    throw Error(label + " needs a number.");
  const n = Number(value);
  if (!Number.isFinite(n) || n < min || n > max)
    throw Error(label + " is outside its allowed range.");
  return n;
}
export function compare(options, weights) {
  if (!Array.isArray(options) || options.length > 20)
    throw Error("Compare at most 20 options.");
  const entries = Object.entries(weights);
  for (const [name, w] of entries)
    if (finiteNumber(w, "Weight for " + name, 0, 1e6) === null)
      throw Error("Enter each comparison weight.");
  const themes = entries.filter(([, w]) => Number(w) > 0).map(([t]) => t), sum = themes.reduce((n, t) => n + Number(weights[t]), 0), rows = options.map((o, index) => {
    if (!o || typeof o.name !== "string" || !o.name.trim() || o.name.length > 80)
      throw Error("Name every comparison option.");
    let known = 0, got = 0, missingWeight = 0;
    for (const t of themes) {
      const score = finiteNumber(Object.hasOwn(o.scores ?? {}, t) ? o.scores[t] : null, o.name + " on " + t, 0, 5);
      if (score === null)
        missingWeight += Number(weights[t]);
      else {
        known++;
        got += Number(weights[t]) * score;
      }
    }
    return { name: o.name, index, total: sum && known === themes.length ? got / sum : null, lower: sum ? got / sum : null, upper: sum ? (got + 5 * missingWeight) / sum : null, assessed: known, missing: themes.length - known };
  }), complete = !!sum && rows.length > 0 && rows.every((r) => r.total !== null), flips = [];
  if (complete)
    rows.sort((a, b) => b.total - a.total || a.index - b.index);
  if (complete && rows.length > 1) {
    const lead = options[rows[0].index], next = options[rows[1].index], base = themes.reduce((a, t) => a + Number(weights[t]) * (Number(lead.scores[t]) - Number(next.scores[t])), 0);
    for (const t of themes) {
      const d = Number(lead.scores[t]) - Number(next.scores[t]);
      if (d < 0)
        flips.push({ theme: t, from: Number(weights[t]), to: Number(weights[t]) + base / -d });
    }
    flips.sort((a, b) => a.to / a.from - b.to / b.from);
  }
  return { rows, themes, flips, complete, margin: complete && rows.length > 1 ? rows[0].total - rows[1].total : null };
}

export function pocOutcome(criteria) {
  const run = criteria.filter(c => c.result), pass = run.filter(c => c.result === 'pass').length, fail = run.filter(c => c.result === 'fail').length;
  const mustFail = criteria.filter(c => c.result === 'fail' && c.priority === 'must');
  return { planned: criteria.length, run: run.length, pass, fail, open: criteria.length - run.length,
    words: !run.length ? 'Nothing has been tried yet' : mustFail.length ? `${mustFail.length} required check${mustFail.length > 1 ? 's' : ''} failed` : run.length < criteria.length ? `${pass} passed, ${fail} failed, ${criteria.length - run.length} not yet tried` : fail ? `All tried: ${pass} passed, ${fail} failed` : 'Every check passed' };
}

// ------------------------------------------------------------ the product

export function opportunities(reqs) {
  const by = new Map();
  for (const r of reqs) { const o = by.get(r.theme) ?? { theme: r.theme, passages: [], sources: new Set(), must: 0 }; o.passages.push(r); o.sources.add(r.source); if (r.priority === 'must') o.must++; by.set(r.theme, o); }
  return [...by.values()].map(o => ({ theme: o.theme, reach: o.sources.size, mentions: o.passages.length, must: o.must, passages: o.passages, sources: [...o.sources] })).sort((a, b) => b.reach - a.reach || b.mentions - a.mentions);
}
export function rice(o) {
  const reach = finiteNumber(o.reach, "Reach", 0, 1e9), impact = finiteNumber(o.impact, "Impact", 0, 3), confidence = finiteNumber(o.confidence, "Confidence", 0, 1), effort = finiteNumber(o.effort, "Effort", 0, 1e9);
  if ([reach, impact, confidence, effort].includes(null))
    return null;
  if (effort === 0)
    throw Error("Effort must be greater than zero.");
  return reach * impact * confidence / effort;
}
export function ranking(items, swing = 0.2) {
  if (!Array.isArray(items) || items.length > 1000 || new Set(items.map((x) => x.theme)).size !== items.length)
    throw Error("Choose distinct opportunity identities.");
  finiteNumber(swing, "Confidence change", 0, 1);
  const scored = items.map((o) => ({ ...o, score: rice(o) }));
  if (scored.some((o) => o.score === null))
    return scored.map((o) => ({ ...o, place: null, best: null, worst: null, stable: !1 }));
  const order = (list) => list.map((o) => ({ ...o, score: rice(o) })).sort((a, b) => b.score - a.score).map((o) => o.theme), base = order(items), places = Object.fromEntries(base.map((t, i) => [t, [i, i]]));
  for (const o of items)
    for (const f of [1 - swing, 1 + swing])
      order(items.map((x) => x.theme === o.theme ? { ...x, confidence: Math.min(1, Number(x.confidence) * f) } : x)).forEach((t, i) => {
        places[t][0] = Math.min(places[t][0], i);
        places[t][1] = Math.max(places[t][1], i);
      });
  return base.map((t, i) => {
    const o = items.find((x) => x.theme === t);
    return { ...o, score: rice(o), place: i + 1, best: places[t][0] + 1, worst: places[t][1] + 1, stable: places[t][0] === places[t][1] };
  });
}

// ------------------------------------------------------------ handing it over

const q = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
export function complianceCsv(reqs, sources) {
  return 'source,priority,theme,requirement,coverage,note,source_url\n' + reqs.map(r => [q(sources[r.source]?.title ?? r.source), r.priority, q(r.theme), q(r.text), r.coverage || 'not assessed', q(r.note), q(sources[r.source]?.url)].join(',')).join('\n') + '\n';
}
export function proposalOutline(reqs, sources, cmp, poc) {
  const by = {}; for (const r of reqs) (by[r.theme] ??= []).push(r);
  const lines = ['# Response outline', ''];
  if (cmp?.complete && cmp?.rows?.length) lines.push(`Compared on the buyer's own priorities, ${cmp.rows[0].name} leads at ${(cmp.rows[0].total).toFixed(2)} of 5${cmp.margin != null ? `, ${cmp.margin.toFixed(2)} ahead` : ''}.`, '');
  if (poc) lines.push(`Proof of concept: ${poc.words}.`, '');
  for (const [theme, rs] of Object.entries(by)) { lines.push(`## ${theme}`, '');
    for (const r of rs) lines.push(`> ${r.text}`, `> (${sources[r.source]?.title ?? r.source}; ${r.priority})`, '', r.coverage ? `Our position: ${({ covered: 'covered', partly: 'partly covered', planned: 'planned, not yet available', no: 'not offered' })[r.coverage]}.${r.note ? ' ' + r.note : ''}` : 'Our position: not yet assessed.', ''); }
  return lines.join('\n');
}
export function roadmapCsv(ranked) {
  return 'place,opportunity,reach,impact,confidence,effort,score,place_range\n' + ranked.map(o => [o.place, q(o.theme), o.reach, o.impact, o.confidence, o.effort, o.score.toFixed(2), o.stable ? 'stable' : `${o.best}-${o.worst}`].join(',')).join('\n') + '\n';
}

// Reviewed requirement operations; source text and native record ownership stay intact.
export function previewRequirementChanges(reqs, ids, changes, texts) {
  if (!Array.isArray(ids) || !ids.length || ids.length > 500 || new Set(ids).size !== ids.length)
    throw Error("Choose 1\u2013500 different requirements.");
  const keys = Object.keys(changes ?? {});
  if (!keys.length || keys.some((k) => !["priority", "coverage"].includes(k)))
    throw Error("Only priority and coverage belong in this review.");
  if ("priority" in changes && !["must", "should", "could"].includes(changes.priority))
    throw Error("Choose a valid priority.");
  if ("coverage" in changes && !["", "covered", "partly", "planned", "no"].includes(changes.coverage))
    throw Error("Choose a valid coverage.");
  const by = new Map(reqs.map((r) => [r.id, r]));
  if (by.size !== reqs.length)
    throw Error("Requirements need unique identities.");
  return ids.map((id) => {
    const r = by.get(id);
    if (!r || !verify(texts, r))
      throw Error("A selected requirement no longer matches its source.");
    return { id, source: r.source, start: r.start, end: r.end, text: r.text, before: Object.fromEntries(keys.map((k) => [k, r[k]])), after: { ...changes } };
  });
}
export function applyRequirementChanges(reqs, review, texts) {
  if (!Array.isArray(review) || !review.length)
    throw Error("Review the selected requirements first.");
  const by = new Map(reqs.map((r) => [r.id, r])), seen = new Set;
  for (const edit of review) {
    if (seen.has(edit.id))
      throw Error("A requirement occurs twice in this review.");
    seen.add(edit.id);
    const r = by.get(edit.id);
    if (!r || r.source !== edit.source || r.start !== edit.start || r.end !== edit.end || r.text !== edit.text || !verify(texts, r) || Object.entries(edit.before).some(([k, v]) => r[k] !== v))
      throw Error("This review is stale. Review the current requirements again.");
    previewRequirementChanges(reqs, [edit.id], edit.after, texts);
  }
  const changes = new Map(review.map((r) => [r.id, r.after]));
  return reqs.map((r) => changes.has(r.id) ? { ...r, ...changes.get(r.id) } : { ...r });
}
export async function importRequirementDocument({ name, text }, existing) {
  if (typeof name !== "string" || !name.trim() || name.length > 255 || typeof text !== "string" || !text.trim() || new TextEncoder().encode(text).length > 2097152)
    throw Error("Use a named, non-empty document up to 2 MiB.");
  const bytes = new TextEncoder().encode(text), digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), sha256 = Array.from(digest, (b) => b.toString(16).padStart(2, "0")).join(""), id = "own-" + sha256;
  if (Object.hasOwn(existing.texts, id)) {
    if (existing.texts[id] !== text)
      throw Error("The document identity conflicts with retained text.");
    return { id, duplicate: !0, requirements: [] };
  }
  if (Object.keys(existing.texts).length >= 100)
    throw Error("Keep at most 100 documents in one review.");
  if (Object.values(existing.texts).reduce((n, t) => n + new TextEncoder().encode(t).length, bytes.length) > 8388608)
    throw Error("The review exceeds its 8 MiB source-text limit.");
  const found = requirements(text, id);
  if (found.length > 2000)
    throw Error("Split documents with more than 2,000 requirements.");
  return { id, duplicate: !1, requirements: found, text, title: name, sha256 };
}
export function deliveryPlan({ requirements: reqs, ids, estimates, start, name, texts, checkpoint }) {
  if (!checkpoint || typeof checkpoint.key !== "string" || !checkpoint.key.startsWith("opportunity:") || !Number.isSafeInteger(checkpoint.revision) || checkpoint.revision < 1)
    throw Error("Save and review the opportunity before preparing its delivery plan.");
  if (typeof name !== "string" || !name.trim() || name.length > 80)
    throw Error("Name this delivery plan (up to 80 characters).");
  if (typeof start !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(start) || !Number.isFinite(Date.parse(start + "T12:00:00Z")) || new Date(start + "T12:00:00Z").toISOString().slice(0, 10) !== start)
    throw Error("Choose an actual start date.");
  if (!Array.isArray(ids) || !ids.length || ids.length > 100 || new Set(ids).size !== ids.length)
    throw Error("Choose 1\u2013100 distinct requirements for a plan.");
  const by = new Map(reqs.map((r) => [r.id, r])), selected = new Set(ids), taskIds = new Map(ids.map((id, i) => [id, "requirement-" + (i + 1)])), tasks = ids.map((id) => {
    const r = by.get(id), e = estimates?.[id];
    if (!r || !verify(texts, r))
      throw Error("A selected requirement no longer matches its retained source.");
    if (!e)
      throw Error("Enter an estimate for every selected requirement.");
    const o = finiteNumber(e.optimistic, "Best duration", 0, 1e4), m = finiteNumber(e.likely, "Likely duration", 0, 1e4), p = finiteNumber(e.pessimistic, "Worst duration", 0, 1e4);
    if (o === null || m === null || p === null || o > m || m > p)
      throw Error("Enter best \u2264 likely \u2264 worst working days for every task.");
    const deps = e.deps ?? [];
    if (!Array.isArray(deps) || new Set(deps).size !== deps.length || deps.some((d) => d === id || !selected.has(d)))
      throw Error("Task dependencies must refer to other selected requirements.");
    const owner = String(e.owner ?? "");
    if (owner.length > 80)
      throw Error("Keep owner labels under 80 characters.");
    return { id: taskIds.get(id), name: r.text, optimistic: o, likely: m, pessimistic: p, owner, deps: deps.map((d) => taskIds.get(d)), requirement: { record: checkpoint.key, revision: checkpoint.revision, id: r.id, source: r.source, start: r.start, end: r.end, quote: r.text } };
  });
  return { name: name.trim(), start, seed: 1, tasks, sourceOpportunity: { key: checkpoint.key, revision: checkpoint.revision }, scope: "Proposed delivery work, not a booking or accepted commercial commitment." };
}
