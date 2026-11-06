import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { plainText, requirements, verify, priorityOf, themeOf, coverage, themeWeights, compare, pocOutcome, opportunities, rice, ranking, complianceCsv, proposalOutline, roadmapCsv } from '../opportunity-desk.mjs';

const pack = join(import.meta.dir, '../../../packs/aurekai-work-samples');
const docs = ['achievement-first-website-rfp', 'stc-website-rfp', 'tjpdc-website-rfp', 'arc-wordpress-rfp804v2'];
const texts = Object.fromEntries(docs.map(d => [d, plainText(readFileSync(join(pack, d + '.html'), 'utf8'))]));
const reqs = docs.flatMap(d => requirements(texts[d], d));

test('requirements are the buyers\' exact sentences, with priority and theme read from the words', () => {
  expect(reqs.length).toBeGreaterThan(30);
  expect(reqs.every(r => verify(texts, r))).toBe(true);
  expect(new Set(reqs.map(r => r.source)).size).toBeGreaterThanOrEqual(3);
  expect(priorityOf('The consultant shall produce the annual report.')).toBe('must');
  expect(priorityOf('Donors should be able to find information.')).toBe('should');
  expect(themeOf('Staff must publish updates within guardrails for accessibility')).toBe('Editing and publishing');
  expect(themeOf('The site must meet WCAG 2.1 AA')).toBe('Accessibility');
  expect(reqs.some(r => /shall not publish content without prior approval/.test(r.text))).toBe(true);
});

test('coverage weighs must over should and names the must-have gaps', () => {
  const rs = [{ priority: 'must', coverage: 'covered' }, { priority: 'must', coverage: 'no', text: 'x' }, { priority: 'should', coverage: 'partly' }, { priority: 'could', coverage: '' }];
  const c = coverage(rs);
  expect(c.score).toBeCloseTo((3 + 0 + 1) / 8, 9); expect(c.gaps).toHaveLength(1); expect(c.assessed).toBe(3);
});

test('a weighted comparison finds the leader and how far a weight must move to change it', () => {
  const weights = { A: 6, B: 2 }, options = [{ name: 'Us', scores: { A: 5, B: 1 } }, { name: 'Them', scores: { A: 3, B: 5 } }];
  const c = compare(options, weights);
  expect(c.rows[0].name).toBe('Us'); expect(c.rows[0].total).toBeCloseTo((30 + 2) / 8, 9);
  // Lead = 6·2 − 2·4 = 4; B must rise by 4/4 = 1 to 3 for a tie.
  expect(c.flips).toEqual([{ theme: 'B', from: 2, to: 3 }]);
  expect(compare(options, { A: 6, B: 3.01 }).rows[0].name).toBe('Them');
  expect(themeWeights([{ theme: 'A', priority: 'must' }, { theme: 'A', priority: 'could' }], { B: 2 })).toEqual({ A: 4, B: 2 });
});

test('a proof of concept reports a failed required check above everything else', () => {
  expect(pocOutcome([{ priority: 'must', result: 'pass' }, { priority: 'should', result: '' }]).words).toBe('1 passed, 0 failed, 1 not yet tried');
  expect(pocOutcome([{ priority: 'must', result: 'fail' }, { priority: 'should', result: 'pass' }]).words).toBe('1 required check failed');
  expect(pocOutcome([{ priority: 'must', result: 'pass' }]).words).toBe('Every check passed');
});

test('opportunities recur across buyers; RICE ranks them and says which places are stable', () => {
  const ops = opportunities(reqs);
  expect(ops[0].reach).toBeGreaterThanOrEqual(ops.at(-1).reach);
  expect(ops.reduce((a, o) => a + o.mentions, 0)).toBe(reqs.length);
  expect(rice({ reach: 3, impact: 2, confidence: 0.5, effort: 2 })).toBe(1.5);
  const items = [{ theme: 'X', reach: 4, impact: 2, confidence: 0.8, effort: 2 }, { theme: 'Y', reach: 4, impact: 2, confidence: 0.75, effort: 2 }, { theme: 'Z', reach: 1, impact: 1, confidence: 0.5, effort: 5 }];
  const r = ranking(items);
  expect(r.map(o => o.theme)).toEqual(['X', 'Y', 'Z']);
  expect(r[0].stable).toBe(false); expect(r[0].worst).toBe(2); expect(r[2].stable).toBe(true);
});

test('hand-over files carry the exact requirement text', () => {
  const sources = { 'stc-website-rfp': { title: 'STC RFP', url: 'https://example.org/stc' } };
  const some = reqs.filter(r => r.source === 'stc-website-rfp').slice(0, 2).map((r, i) => ({ ...r, coverage: i ? 'planned' : 'covered' }));
  expect(complianceCsv(some, sources)).toContain(some[0].text.replace(/"/g, '""'));
  const md = proposalOutline(some, sources, compare([{ name: 'Us', scores: {} }], { A: 1 }), pocOutcome([]));
  expect(md).toContain('> ' + some[0].text); expect(md).toContain('planned, not yet available');
  expect(roadmapCsv(ranking([{ theme: 'X', reach: 1, impact: 1, confidence: 1, effort: 1 }])).split('\n')[1]).toBe('1,"X",1,1,1,1,1.00,stable');
});

import {previewRequirementChanges,applyRequirementChanges,importRequirementDocument,deliveryPlan} from '../opportunity-desk.mjs';
const exactA='The system must preserve every original request and its source document.';
const exactB='The team should review every proposed delivery estimate before scheduling.';
const exactTexts={a:exactA,b:exactB};const exactReqs=[...requirements(exactA,'a'),...requirements(exactB,'b')];
test("missing option scores remain unknown, not zero and not a leader",() => {
  const c = compare([{ name: "A", scores: { X: 5 } }, { name: "B", scores: {} }], { X: 3, Y: 1 });
  expect(c.complete).toBe(!1);
  expect(c.rows[0].total).toBeNull();
  expect(c.rows[0].lower).toBe(3.75);
  expect(c.rows[0].upper).toBe(5);
  expect(c.rows[1].lower).toBe(0);
  expect(c.rows[1].upper).toBe(5);
  expect(c.flips).toEqual([]);
  expect(c.margin).toBeNull();
});
test("duplicate option labels retain their distinct calculation identities",() => {
  const c = compare([{ name: "Same", scores: { X: 1 } }, { name: "Same", scores: { X: 5 } }], { X: 1 });
  expect(c.rows.map((r) => r.index)).toEqual([1, 0]);
  expect(c.rows.map((r) => r.total)).toEqual([5, 1]);
});
test("invalid and zero-weight comparisons cannot invent a total",() => {
  for (const w of [-1, 1 / 0, NaN, "bad", !0])
    expect(() => compare([{ name: "A", scores: { X: 1 } }], { X: w })).toThrow();
  for (const score of [-1, 6, 1 / 0, "5x", !0])
    expect(() => compare([{ name: "A", scores: { X: score } }], { X: 1 })).toThrow();
  expect(compare([{ name: "A", scores: { X: 5 } }], { X: 0 }).rows[0].total).toBeNull();
});
test("missing prioritization estimates do not produce ranks or implicit effort",() => {
  expect(rice({ reach: 2, impact: null, confidence: 0.8, effort: 1 })).toBeNull();
  expect(rice({ reach: 2, impact: 1, confidence: 0.8 })).toBeNull();
  for (const effort of [0, -2, 1 / 0])
    expect(() => rice({ reach: 2, impact: 1, confidence: 0.8, effort })).toThrow();
  expect(() => rice({ reach: 2, impact: 1, confidence: 2, effort: 1 })).toThrow();
  expect(ranking([{ theme: "A", reach: 1, impact: 1, confidence: 1, effort: 1 }, { theme: "B", reach: 1, impact: null, confidence: null, effort: null }]).every((r) => r.place === null)).toBe(!0);
});
test("the last unterminated requirement retains its exact source coordinates",() => {
  const rows = requirements("  The supplier must retain the complete original document without truncation", "own");
  expect(rows).toHaveLength(1);
  expect(rows[0].text).toBe("  The supplier must retain the complete original document without truncation".trim());
  expect(verify({ own: "  The supplier must retain the complete original document without truncation" }, rows[0])).toBe(!0);
});
test("bulk preview is inert, applies only selected rows and supports exact inverse",() => {
  const review = previewRequirementChanges(exactReqs, [exactReqs[0].id], { coverage: "planned" }, exactTexts);
  expect(exactReqs[0].coverage).toBe("");
  const changed = applyRequirementChanges(exactReqs, review, exactTexts);
  expect(changed[0].coverage).toBe("planned");
  expect(changed[1]).toEqual(exactReqs[1]);
  expect(applyRequirementChanges(changed, review.map((r) => ({ ...r, before: r.after, after: r.before })), exactTexts)).toEqual(exactReqs);
});
test("stale fields or source bytes reject the entire reviewed bulk change",() => {
  const review = previewRequirementChanges(exactReqs, exactReqs.map((r) => r.id), { coverage: "covered" }, exactTexts);
  expect(() => applyRequirementChanges([{ ...exactReqs[0], coverage: "no" }, exactReqs[1]], review, exactTexts)).toThrow("stale");
  expect(() => applyRequirementChanges(exactReqs, review, { ...exactTexts, a: exactA + " revised" })).not.toThrow();
  expect(() => applyRequirementChanges(exactReqs, review, { ...exactTexts, a: "changed source" })).toThrow("stale");
  expect(() => previewRequirementChanges(exactReqs, [exactReqs[0].id], { note: "not a bulk field" }, exactTexts)).toThrow();
});
test("same-name changed documents receive different exact identities; duplicates are not reimported",async () => {
  const existing = { texts: {} }, a = await importRequirementDocument({ name: "rfp.txt", text: exactA }, existing);
  existing.texts[a.id] = a.text;
  const b = await importRequirementDocument({ name: "rfp.txt", text: exactB }, existing);
  expect(a.id).not.toBe(b.id);
  expect(b.requirements.every((r) => verify({ [b.id]: b.text }, r))).toBe(!0);
  expect((await importRequirementDocument({ name: "renamed.txt", text: exactA }, existing)).duplicate).toBe(!0);
  expect(existing.texts[a.id]).toBe(exactA);
});
test("import bounds refuse before altering retained material",async () => {
  const existing = { texts: {} };
  await expect(importRequirementDocument({ name: "x.txt", text: "a".repeat(2097153) }, existing)).rejects.toThrow();
  await expect(importRequirementDocument({ name: "x.txt", text: "  " }, existing)).rejects.toThrow();
  const unstructured = await importRequirementDocument({name:"brief.txt",text:"Turnaround: 48 hours."},existing);
  expect(unstructured.requirements).toEqual([]);
  expect(unstructured.text).toBe("Turnaround: 48 hours.");
  expect(existing.texts).toEqual({});
});
test("prepared delivery tasks preserve source revision and explicit estimate units",() => {
  const ids = exactReqs.map((r) => r.id), estimates = Object.fromEntries(ids.map((id, i) => [id, { optimistic: 1, likely: 2, pessimistic: 3, owner: "Reviewer", deps: i ? [ids[0]] : [] }])), plan = deliveryPlan({ requirements: exactReqs, ids, estimates, start: "2026-11-02", name: "Reviewed delivery", texts: exactTexts, checkpoint: { key: "opportunity:review", revision: 7 } });
  expect(plan.tasks[1].deps).toEqual(["requirement-1"]);
  expect(plan.tasks[0].requirement).toMatchObject({ record: "opportunity:review", revision: 7, quote: exactA });
  expect(plan.tasks[0].likely).toBe(2);
  expect(plan.sourceOpportunity.revision).toBe(7);
});
test("delivery refuses unknown estimates, impossible dates and changed source selections",() => {
  const id = exactReqs[0].id, options = { requirements: exactReqs, ids: [id], estimates: { [id]: { optimistic: 1, likely: 2, pessimistic: 3 } }, start: "2026-11-02", name: "Plan", texts: exactTexts, checkpoint: { key: "opportunity:review", revision: 1 } };
  expect(() => deliveryPlan({ ...options, estimates: { [id]: { optimistic: "", likely: 2, pessimistic: 3 } } })).toThrow();
  expect(() => deliveryPlan({ ...options, start: "2026-02-30" })).toThrow();
  expect(() => deliveryPlan({ ...options, texts: { a: "different" } })).toThrow();
  expect(() => deliveryPlan({ ...options, estimates: { [id]: { optimistic: 1, likely: 2, pessimistic: 3, deps: [id] } } })).toThrow();
  expect(() => deliveryPlan({ ...options, checkpoint: null })).toThrow();
});
