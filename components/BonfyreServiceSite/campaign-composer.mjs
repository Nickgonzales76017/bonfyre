// One message, several formats. A post is shared content (the quotation, its
// citation, the claim it supports) laid out on linked artboards; each format
// keeps its own adjustments (type scale, alignment, colours) while a change to
// the shared content reaches all of them. Layout is computed here with any text
// measurer, so the same arithmetic runs in a canvas and in a test.

export const FORMATS = {
  square: { label: 'Square post', w: 1080, h: 1080, pad: 96 },
  story: { label: 'Story', w: 1080, h: 1920, pad: 110 },
  card: { label: 'Link card', w: 1200, h: 630, pad: 72 },
};
export const THEMES = {
  night: { ground: '#0d130f', ink: '#f5f7ee', accent: '#b5df32', muted: '#9aa89b' },
  lime: { ground: '#b5df32', ink: '#102107', accent: '#0d130f', muted: '#2f4a12' },
  paper: { ground: '#f5f7ee', ink: '#0d130f', accent: '#3d6b12', muted: '#5d6b60' },
};

// Greedy line breaking by measured width.
export function wrap(text, width, measure) {
  const words = String(text).split(/\s+/).filter(Boolean), lines = [];
  let line = '';
  for (const w of words) { const next = line ? line + ' ' + w : w; if (measure(next) <= width || !line) line = next; else { lines.push(line); line = w; } }
  if (line) lines.push(line);
  return lines;
}
// The largest type size (in whole pixels) at which the text fits the box, between min and max.
export function fit(text, box, measureAt, { min = 24, max = 140, leading = 1.18 } = {}) {
  let lo = min, hi = max, best = { size: min, lines: wrap(text, box.w, s => measureAt(s, min)) };
  while (lo <= hi) { const mid = (lo + hi) >> 1, lines = wrap(text, box.w, s => measureAt(s, mid));
    const height = lines.length * mid * leading, widest = Math.max(...lines.map(l => measureAt(l, mid)));
    if (height <= box.h && widest <= box.w) { best = { size: mid, lines }; lo = mid + 1; } else hi = mid - 1; }
  const height = best.lines.length * best.size * leading, widest = Math.max(0, ...best.lines.map(line => measureAt(line, best.size)));
  return { ...best, height, overflow: height > box.h || widest > box.w };
}

// Where everything goes on one artboard. Overrides belong to the format; content is shared.
export function layout(content, format, override = {}, measureAt) {
  const f = FORMATS[format], o = { scale: 1, align: 'left', theme: 'night', ...override }, pad = f.pad, inner = f.w - pad * 2;
  const top = pad + (format === 'story' ? 220 : 64), citeH = format === 'card' ? 70 : 110, claimH = content.claim ? (format === 'card' ? 48 : 70) : 0;
  const box = { w: inner, h: f.h - top - pad - citeH - claimH };
  const quote = fit(`“${content.quote}”`, box, measureAt, { min: 22, max: Math.round((format === 'card' ? 72 : 120) * o.scale) });
  const x = o.align === 'center' ? f.w / 2 : pad;
  const quoteTop = format === 'story' ? top + (box.h - quote.height) / 2 : top;
  return { ...f, format, theme: THEMES[o.theme] ?? THEMES.night, align: o.align, x, pad,
    claim: content.claim ? { text: content.claim.toUpperCase(), x, y: pad + 20, size: format === 'card' ? 22 : 30 } : null,
    quote: { ...quote, x, y: quoteTop + quote.size, lineHeight: Math.round(quote.size * 1.18) },
    cite: { text: content.cite, x, y: f.h - pad - (format === 'card' ? 20 : 40), size: format === 'card' ? 24 : 32 },
    mark: { text: content.mark ?? 'Aurekai', x: o.align === 'center' ? f.w / 2 : f.w - pad, y: pad + 20, size: format === 'card' ? 20 : 26, anchor: o.align === 'center' ? 'center' : 'right' } };
}

// How much of the quotation is shown at time t (seconds) of a reveal lasting `seconds`, word by word.
export function revealAt(lines, t, seconds = 4) {
  const words = lines.join(' ').split(' '), shown = Math.max(0, Math.min(words.length, Math.ceil(t / (seconds * 0.8) * words.length)));
  let left = shown; return lines.map(line => { const w = line.split(' '), take = Math.max(0, Math.min(w.length, left)); left -= take; return w.slice(0, take).join(' '); });
}

// Whether the post still quotes its source exactly; edited text is allowed but labelled.
export const quotesExactly = (content, sourceText) => content.quote.trim() === String(sourceText).trim();

// Artboards belong to the research project and travel through its existing
// revision-checked save, copy and import/export operations.
export function validateCampaigns(project) {
  if (project.campaigns === undefined) return;
  if (!Array.isArray(project.campaigns) || project.campaigns.length > 1000) throw Error('Invalid saved artboards.');
  const seen = new Set();
  for (const draft of project.campaigns) {
    const ref = draft?.origin;
    if (draft?.version !== 1 || !ref || !['excerpt', 'claim', 'record'].every(k => typeof ref[k] === 'string' && ref[k].length > 0)
      || !Number.isSafeInteger(ref.start) || !Number.isSafeInteger(ref.end) || ref.start < 0 || ref.end <= ref.start
      || !draft.content || !['quote', 'cite', 'claim'].every(k => typeof draft.content[k] === 'string')
      || typeof draft.source !== 'string' || !Object.hasOwn(FORMATS, draft.active)
      || !draft.overrides || typeof draft.overrides !== 'object' || Array.isArray(draft.overrides)) throw Error('Invalid saved artboard.');
    const key = JSON.stringify([ref.excerpt, ref.claim]);
    if (seen.has(key)) throw Error('Duplicate saved artboard identity.');
    seen.add(key);
    if (draft.originalSource !== undefined && typeof draft.originalSource !== 'string') throw Error('Invalid original campaign source.');
    if (draft.sourceRef !== undefined && !validSourceRef(draft.sourceRef)) throw Error('Invalid reviewed campaign source.');
    for (const [format, override] of Object.entries(draft.overrides)) {
      if (!Object.hasOwn(FORMATS, format) || !override || typeof override !== 'object' || Array.isArray(override)
        || Object.keys(override).some(k => !['scale', 'align', 'theme'].includes(k))
        || (override.scale !== undefined && (!Number.isFinite(override.scale) || override.scale < 0.6 || override.scale > 1.3))
        || (override.align !== undefined && !['left', 'center'].includes(override.align))
        || (override.theme !== undefined && !Object.hasOwn(THEMES, override.theme))) throw Error('Invalid saved artboard settings.');
    }
  }
}

export function campaignFor(project, excerptId, claimId, citation) {
  validateCampaigns(project);
  const excerpt = project.excerpts.find(e => e.id === excerptId && e.claim === claimId);
  const record = project.records.find(r => r.id === excerpt?.record);
  const claim = project.claims.find(c => c.id === claimId);
  if (!excerpt || !record || !claim) throw Error('The quotation or claim is no longer in this review.');
  const saved = project.campaigns?.find(d => d.origin.excerpt === excerptId && d.origin.claim === claimId);
  if (saved) return saved;
  if (!Number.isSafeInteger(excerpt.start) || !Number.isSafeInteger(excerpt.end) || excerpt.start < 0
    || excerpt.end <= excerpt.start || excerpt.end > record.abstract.length
    || record.abstract.slice(excerpt.start, excerpt.end).trim() !== excerpt.text) throw Error('The quotation no longer matches its source.');
  const draft = { version: 1, origin: { excerpt: excerptId, claim: claimId, record: record.id, start: excerpt.start, end: excerpt.end },
    content: { quote: excerpt.text, cite: citation, claim: claim.text }, source: excerpt.text, overrides: {}, active: 'square' };
  validateCampaigns({ campaigns: [...(project.campaigns ?? []), draft] });
  (project.campaigns ??= []).push(draft);
  return draft;
}

export function campaignSourceStatus(project, draft) {
  const ref = draft.sourceRef ?? draft.origin;
  const excerpt = project.excerpts.find(e => e.id === ref.excerpt && e.claim === ref.claim);
  const record = project.records.find(r => r.id === ref.record);
  const claim = project.claims.find(c => c.id === ref.claim);
  if (!excerpt || !record || !claim) return 'The source quotation or claim was removed. Your artboard edits are retained.';
  if (excerpt.record !== ref.record || excerpt.start !== ref.start || excerpt.end !== ref.end
    || record.abstract.slice(ref.start, ref.end).trim() !== draft.source || excerpt.text !== draft.source)
    return 'The source quotation changed. Review it before using this artboard; your edits are retained.';
  if (project.decisions[record.id]?.status !== 'include') return 'This source is not included in the review.';
  return '';
}

const validSourceRef = ref => ref && ['excerpt', 'claim', 'record'].every(k => typeof ref[k] === 'string' && ref[k].length > 0)
  && Number.isSafeInteger(ref.start) && Number.isSafeInteger(ref.end) && ref.start >= 0 && ref.end > ref.start;

// A correction reviews one exact filed passage. Original provenance and the
// workspace's saved versions remain intact; unrelated artboards are untouched.
export function prepareCampaignSource(project, draft, excerptId, claimId, citation, wording = 'keep') {
  validateCampaigns(project);
  if (!project.campaigns?.includes(draft)) throw Error('This artboard is not in the current project.');
  if (!['keep', 'source'].includes(wording) || typeof citation !== 'string') throw Error('Choose how to use the reviewed quotation.');
  const excerpt = project.excerpts.find(e => e.id === excerptId && e.claim === claimId);
  const record = project.records.find(r => r.id === excerpt?.record);
  const claim = project.claims.find(c => c.id === claimId);
  if (!excerpt || !record || !claim || project.decisions[record.id]?.status !== 'include') throw Error('Choose a quotation from an included source.');
  const ref = { excerpt: excerpt.id, claim: claim.id, record: record.id, start: excerpt.start, end: excerpt.end };
  if (!validSourceRef(ref) || ref.end > record.abstract.length || record.abstract.slice(ref.start, ref.end).trim() !== excerpt.text)
    throw Error('The replacement quotation no longer matches its source.');
  return structuredClone({ before: { origin: draft.origin, sourceRef: draft.sourceRef ?? null, source: draft.source, content: draft.content },
    target: { ref, text: excerpt.text, citation }, wording });
}

export function applyCampaignSource(project, draft, proposal, currentCitation) {
  const ref = proposal?.target?.ref;
  const current = prepareCampaignSource(project, draft, ref?.excerpt, ref?.claim, currentCitation, proposal?.wording);
  if (JSON.stringify(current) !== JSON.stringify(proposal)) throw Error('The artboard or source changed during review. Review the update again.');
  // Preserve the creation identity even when a removed quotation is replaced.
  draft.originalSource ??= draft.source;
  draft.sourceRef = structuredClone(current.target.ref);
  draft.source = current.target.text;
  draft.content.cite = current.target.citation;
  if (current.wording === 'source') draft.content.quote = current.target.text;
  return draft;
}
