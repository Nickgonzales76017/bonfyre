import { test, expect } from 'bun:test';
import { wrap, fit, layout, revealAt, quotesExactly, FORMATS, campaignFor, validateCampaigns, campaignSourceStatus, prepareCampaignSource, applyCampaignSource } from '../campaign-composer.mjs';

// A measurer with fixed advance: 0.55 of the type size per character.
const measureAt = (text, size) => text.length * size * 0.55;
const content = { quote: 'Individual and organisational well-being variables reported a positive impact.', cite: 'Antonelli & Luciano, 2025', claim: 'Wellbeing improves after the move to four days' };

test('lines never exceed the width unless a single word does', () => {
  const lines = wrap('one two three four five six seven', 100, s => s.length * 10);
  expect(lines).toEqual(['one two', 'three four', 'five six', 'seven']);
  expect(wrap('incomprehensibilities', 50, s => s.length * 10)).toEqual(['incomprehensibilities']);
});

test('fit finds the largest size that fits, and smaller boxes get smaller type', () => {
  const big = fit(content.quote, { w: 888, h: 600 }, measureAt), small = fit(content.quote, { w: 888, h: 300 }, measureAt);
  expect(big.size).toBeGreaterThan(small.size);
  expect(big.height).toBeLessThanOrEqual(600);
  expect(Math.max(...big.lines.map(l => measureAt(l, big.size)))).toBeLessThanOrEqual(888);
  // One pixel larger would not fit.
  const bigger = wrap(content.quote, 888, s => measureAt(s, big.size + 1));
  expect(bigger.length * (big.size + 1) * 1.18 > 600 || Math.max(...bigger.map(l => measureAt(l, big.size + 1))) > 888).toBe(true);
});

test('shared content reaches every format; overrides stay with their own', () => {
  const sq = layout(content, 'square', { scale: 0.8, align: 'center', theme: 'lime' }, measureAt), st = layout(content, 'story', {}, measureAt), cd = layout(content, 'card', {}, measureAt);
  expect([sq, st, cd].map(l => l.cite.text)).toEqual(Array(3).fill(content.cite));
  expect(sq.align).toBe('center'); expect(st.align).toBe('left'); expect(sq.theme.ground).toBe('#b5df32'); expect(st.theme.ground).toBe('#0d130f');
  for (const l of [sq, st, cd]) { const last = l.quote.y + (l.quote.lines.length - 1) * l.quote.lineHeight; expect(last).toBeLessThan(l.cite.y); expect(l.w).toBe(FORMATS[l.format].w); }
  expect(sq.quote.size).toBeLessThanOrEqual(layout(content, 'square', {}, measureAt).quote.size);
});

test('the reveal shows words in order and the whole quotation by the end', () => {
  const lines = ['a b c', 'd e'];
  expect(revealAt(lines, 0)).toEqual(['', '']);
  expect(revealAt(lines, 1)).toEqual(['a b', '']);
  expect(revealAt(lines, 4)).toEqual(lines);
  expect(quotesExactly(content, content.quote + ' ')).toBe(true);
  expect(quotesExactly({ ...content, quote: 'Well-being improved.' }, content.quote)).toBe(false);
});

function review() {
  return { question: 'How did access change?', records: [{ id: 'r1', abstract: 'Weekend access increased by two hours.' }],
    claims: [{ id: 'c1', text: 'Access increased' }, { id: 'c2', text: 'Weekend provision changed' }],
    decisions: { r1: { status: 'include' } },
    excerpts: ['c1', 'c2'].map(claim => ({ id: 'e1', claim, record: 'r1', start: 0, end: 'Weekend access increased by two hours.'.length, text: 'Weekend access increased by two hours.' })) };
}

test('saved research roundtrip preserves edited content, per-format overrides and selected artboard', () => {
  const project = review(), draft = campaignFor(project, 'e1', 'c1', 'Access study, 2026');
  draft.content.quote = 'An edited summary'; draft.content.claim = 'Reviewed conclusion';
  draft.active = 'story'; draft.overrides.story = { scale: 0.8, align: 'center', theme: 'paper' };
  const reopened = JSON.parse(JSON.stringify(project));
  const restored = campaignFor(reopened, 'e1', 'c1', 'A later citation');
  expect(restored).toEqual(draft); expect(reopened.campaigns).toHaveLength(1);
  expect(restored.overrides.square).toBeUndefined();
  expect(quotesExactly(restored.content, restored.source)).toBe(false);
});

test('one passage under two claims and the same IDs in another project keep independent work', () => {
  const project = review(), first = campaignFor(project, 'e1', 'c1', 'Study');
  first.overrides.card = { theme: 'lime' };
  const otherClaim = campaignFor(project, 'e1', 'c2', 'Study');
  const otherProject = campaignFor(review(), 'e1', 'c1', 'Study');
  expect(otherClaim.overrides).toEqual({}); expect(otherProject.overrides).toEqual({});
  expect(project.campaigns).toHaveLength(2);
});

test('source correction retains independent edits and reports stale provenance', () => {
  const project = review(), draft = campaignFor(project, 'e1', 'c1', 'Study');
  draft.content.quote = 'Editorial wording'; draft.overrides.card = { theme: 'lime' };
  expect(campaignSourceStatus(project, draft)).toBe('');
  project.records[0].abstract = 'Weekend access decreased by two hours.';
  expect(campaignSourceStatus(project, draft)).toContain('changed');
  expect(campaignFor(project, 'e1', 'c1', 'Study')).toBe(draft);
  expect(draft.content.quote).toBe('Editorial wording');
  expect(draft.overrides.card.theme).toBe('lime');
});

test('withdrawn evidence and removed source remain visible without deleting saved edits', () => {
  const project = review(), draft = campaignFor(project, 'e1', 'c1', 'Study');
  project.decisions.r1.status = 'exclude';
  expect(campaignSourceStatus(project, draft)).toContain('not included');
  project.excerpts = [];
  expect(campaignSourceStatus(project, draft)).toContain('removed');
  expect(project.campaigns).toEqual([draft]);
});

test('new artboards refuse a mismatched passage before changing the project', () => {
  const project = review(); project.excerpts[0].text = 'Not in the abstract';
  expect(() => campaignFor(project, 'e1', 'c1', 'Study')).toThrow('no longer matches');
  expect(project.campaigns).toBeUndefined();
});

test('legacy projects load without inventing artboards', () => {
  const project = review(); validateCampaigns(project);
  expect(project.campaigns).toBeUndefined();
});

test('malformed import settings and duplicate identities are rejected without changing input', () => {
  const project = review(); campaignFor(project, 'e1', 'c1', 'Study');
  for (const override of [{ scale: null }, { scale: 10 }, { align: 'right' }, { theme: '__proto__' }, { unknown: true }]) {
    const corrupt = structuredClone(project); corrupt.campaigns[0].overrides.story = override;
    const before = JSON.stringify(corrupt);
    expect(() => validateCampaigns(corrupt)).toThrow('settings'); expect(JSON.stringify(corrupt)).toBe(before);
  }
  project.campaigns.push(structuredClone(project.campaigns[0]));
  expect(() => validateCampaigns(project)).toThrow('Duplicate');
});

test('a saved snapshot is independent of subsequent working edits', () => {
  const project = review(), draft = campaignFor(project, 'e1', 'c1', 'Study');
  const saved = structuredClone(project); draft.content.quote = 'After save';
  expect(saved.campaigns[0].content.quote).toBe('Weekend access increased by two hours.');
});

function withReplacement() {
  const project = review();
  project.records.push({ id: 'r2', abstract: 'Weekend access increased by one hour.' });
  project.decisions.r2 = { status: 'include' };
  project.excerpts.push({ id: 'e2', claim: 'c2', record: 'r2', start: 0, end: project.records[1].abstract.length, text: project.records[1].abstract });
  return project;
}

test('reviewed replacement keeps manual words, creation provenance, layout and another artboard', () => {
  const project = withReplacement(), draft = campaignFor(project, 'e1', 'c1', 'Original study');
  const other = campaignFor(project, 'e1', 'c2', 'Original study'), otherBefore = structuredClone(other);
  draft.content.quote = 'Editorial choice'; draft.content.claim = 'My claim line'; draft.active = 'story';
  const origin = structuredClone(draft.origin), source = draft.source;
  const proposal = prepareCampaignSource(project, draft, 'e2', 'c2', 'Correction study');
  draft.overrides.story = { theme: 'lime', scale: 0.7 }; // Independent work after preview.
  applyCampaignSource(project, draft, proposal, 'Correction study');
  expect(draft.content).toEqual({ quote: 'Editorial choice', claim: 'My claim line', cite: 'Correction study' });
  expect(draft.origin).toEqual(origin); expect(draft.originalSource).toBe(source);
  expect(draft.sourceRef.record).toBe('r2'); expect(draft.source).toBe(project.records[1].abstract);
  expect(draft.overrides.story).toEqual({ theme: 'lime', scale: 0.7 }); expect(draft.active).toBe('story');
  expect(other).toEqual(otherBefore); expect(campaignSourceStatus(project, draft)).toBe('');
  validateCampaigns(JSON.parse(JSON.stringify(project)));
});

test('a removed quotation can be replaced and source wording requires an explicit choice', () => {
  const project = withReplacement(), draft = campaignFor(project, 'e1', 'c1', 'Original study');
  project.excerpts = project.excerpts.filter(e => e.id !== 'e1');
  expect(campaignSourceStatus(project, draft)).toContain('removed');
  const proposal = prepareCampaignSource(project, draft, 'e2', 'c2', 'Correction study', 'source');
  expect(draft.sourceRef).toBeUndefined();
  applyCampaignSource(project, draft, proposal, 'Correction study');
  expect(draft.content.quote).toBe(project.records[1].abstract);
  expect(campaignSourceStatus(project, draft)).toBe('');
});

test('source or wording changes during review refuse adoption without losing work', () => {
  for (const change of ['source', 'wording', 'citation', 'permission']) {
    const project = withReplacement(), draft = campaignFor(project, 'e1', 'c1', 'Original study');
    const proposal = prepareCampaignSource(project, draft, 'e2', 'c2', 'Correction study', 'source');
    if (change === 'source') project.records[1].abstract = 'An unrelated source body.';
    if (change === 'wording') draft.content.quote = 'New editorial work';
    if (change === 'permission') project.decisions.r2.status = 'exclude';
    const before = structuredClone(draft);
    expect(() => applyCampaignSource(project, draft, proposal, change === 'citation' ? 'Changed citation' : 'Correction study')).toThrow();
    expect(draft).toEqual(before);
  }
});

test('source review refuses a different project and malformed reviewed coordinates', () => {
  const project = withReplacement(), draft = campaignFor(project, 'e1', 'c1', 'Original study');
  expect(() => prepareCampaignSource(withReplacement(), draft, 'e2', 'c2', 'Correction study')).toThrow('current project');
  draft.sourceRef = { ...draft.origin, end: -1 };
  expect(() => validateCampaigns(project)).toThrow('reviewed campaign source');
});


test('minimum-size text reports horizontal as well as vertical overflow', () => {
  const measure=(text,size)=>text.length*size;
  expect(fit('W'.repeat(80),{w:100,h:1000},measure,{min:10,max:40}).overflow).toBe(true);
  expect(fit('many repeated words '.repeat(80),{w:100,h:20},measure,{min:10,max:40}).overflow).toBe(true);
  expect(fit('Fits',{w:100,h:100},measure,{min:10,max:40}).overflow).toBe(false);
  for(const format of Object.keys(FORMATS))expect(layout({quote:'W'.repeat(400),cite:'Source',claim:''},format,{},measure).quote.overflow).toBe(true);
});
