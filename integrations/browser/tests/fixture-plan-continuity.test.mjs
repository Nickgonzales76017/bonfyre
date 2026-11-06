import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { reviewPlanRefresh, useReviewedRefresh, refreshBrief } from '../../../components/BonfyreServiceSite/planner-continuity.mjs';
import { plannerFiles } from '../../../components/BonfyreServiceSite/planner-material.mjs';
import { startingPlan, validatePlan, assessPlan, planRequest, planReviewForm } from '../../../components/BonfyreServiceSite/planner-model.mjs';
import { workerSource } from '../../../components/BonfyreServiceSite/lead-worker.mjs';

const source = JSON.parse(readFileSync(new URL('../../../packs/aurekai-work-samples/site/fixture-planner.json', import.meta.url), 'utf8'));
const owner = { validatePlan, assessPlan }, clone = x => structuredClone(x);
const release = input => plannerFiles(input).catalogue;
const previous = release(source);
const plan = () => startingPlan(previous, 'compact', 'continuity-test-001');
const updated = edit => { const data = clone(source); edit(data); return release(data); };
const review = current => reviewPlanRefresh(plan(), previous, current, owner);

test('unchanged release preserves the complete project without mutating it', () => {
  const original = plan(), saved = clone(original), r = reviewPlanRefresh(original, previous, previous, owner);
  assert.deepEqual(original, saved); assert.deepEqual(r.candidate, saved);
  assert.equal(r.status, 'unchanged'); assert.equal(r.effect, 'none');
  assert.equal(r.approval_carried_forward, false); assert.equal(r.receipt, null);
});

test('one supplier price change affects both placements without discarding customer work', () => {
  const original = plan(); original.items.push({ ...original.items[0], key: 'rack-extra', x: 4 });
  original.notes = 'Keep the customer requirements.'; original.deadline = '2026-12-10';
  const current = updated(data => data.products[0].price_cents += 700);
  const r = reviewPlanRefresh(original, previous, current, owner);
  assert.equal(r.changes.length, 1); assert.equal(r.changes[0].item_keys.length, 2);
  assert.deepEqual(r.changes[0].fields, ['price_cents']); assert.equal(r.subtotal_delta_cents, 1400);
  assert.deepEqual(r.candidate.items, original.items); assert.equal(r.candidate.notes, original.notes);
  assert.equal(r.candidate.deadline, original.deadline); assert.equal(r.candidate.id, original.id);
  assert.equal(r.candidate.catalogue, current.digest); assert.match(refreshBrief(r), /2 placement/);
});

test('new dimensions recalculate geometry with the existing planner owner', () => {
  const current = updated(data => { data.products[0].dimensions.depth = 144; });
  const r = review(current);
  assert.ok(r.changes[0].fields.includes('dimensions'));
  assert.ok(r.new_issues.some(issue => issue.includes('beyond')));
  assert.equal(r.candidate.items[0].y, plan().items[0].y);
});

test('unselected catalogue additions do not manufacture changed selected facts', () => {
  const current = updated(data => data.products.push({ ...clone(data.products[0]), id: 'extra-rack', product_id: '99999', variant_id: '99998', source_url: data.origin + '/retail_fixtures/extra-rack/' }));
  const r = review(current);
  assert.equal(r.status, 'evidence_refresh'); assert.deepEqual(r.changes, []);
  assert.equal(r.subtotal_delta_cents, 0); assert.deepEqual(r.candidate.items, plan().items);
});

test('source re-observation is distinguished from changed product facts', () => {
  const current = updated(data => { for (const p of data.products) { p.observed_at = '2026-10-09T00:00:00Z'; p.source_sha256 = 'f'.repeat(64); } });
  const r = review(current); assert.equal(r.evidence_only.length, 4);
  assert.equal(r.changes.length, 0); assert.equal(r.status, 'evidence_refresh');
  assert.equal(r.receipt, null); assert.equal(r.approval_carried_forward, false);
});

test('a changed display alias still resolves by exact source/product/option identity', () => {
  const current = updated(data => { data.products[0].id = 'new-rack-alias'; });
  const r = review(current);
  assert.equal(r.missing.length, 0); assert.equal(r.candidate.items[0].product, 'new-rack-alias');
  assert.equal(r.candidate.items[0].key, plan().items[0].key);
});

test('same display name with a different option requires a choice instead of substitution', () => {
  const current = updated(data => { data.products[0].variant_id = '9999'; });
  const r = review(current); assert.equal(r.candidate, null); assert.equal(r.status, 'needs_selection');
  assert.equal(r.missing[0].variant_id, source.products[0].variant_id);
  assert.equal(r.original.items.length, 4);
  assert.throws(() => useReviewedRefresh(plan(), previous, current, r, owner), /missing fixture/);
});

test('removed products remain in the original project until the user resolves them', () => {
  const current = updated(data => { data.products.splice(1, 1); });
  const r = review(current); assert.equal(r.missing.length, 1); assert.equal(r.candidate, null);
  assert.equal(r.after, null); assert.equal(r.subtotal_delta_cents, null);
  assert.deepEqual(r.original, plan()); assert.match(refreshBrief(r), /remove it explicitly/);
});

test('cross-merchant and changed-currency migrations refuse', () => {
  assert.throws(() => review(updated(data => { data.id = 'another-merchant'; })), /another merchant/);
  assert.throws(() => review(updated(data => { data.currency = 'EUR'; })), /USD/);
  assert.throws(() => review(updated(data => { delete data.currency; })), /USD/);
});

test('ambiguous source identities and digest disagreement refuse', () => {
  const ambiguous = updated(data => data.products.push({ ...clone(data.products[0]), id: 'duplicate' }));
  assert.throws(() => review(ambiguous), /Ambiguous/);
  const forged = clone(previous); forged.products[0].price_cents += 100;
  assert.throws(() => review(forged), /release digest/);
});

test('reviewed action rechecks both the customer work and the current release', () => {
  const original = plan(), current = updated(data => { data.products[0].price_cents += 1; });
  const r = reviewPlanRefresh(original, previous, current, owner);
  const kept = useReviewedRefresh(original, previous, current, r, owner);
  assert.equal(kept.plan.catalogue, current.digest); assert.equal(kept.receipt, null);
  const edited = clone(original); edited.notes += ' More work since review.';
  assert.throws(() => useReviewedRefresh(edited, previous, current, r, owner), /after review/);
  const later = updated(data => { data.products[0].price_cents += 2; });
  assert.throws(() => useReviewedRefresh(original, previous, later, r, owner), /after review/);
});

test('refreshed candidate enters the existing generated intake, while the old release refuses', async () => {
  const current = updated(data => { data.products[0].price_cents += 100; }), original = plan();
  const r = reviewPlanRefresh(original, previous, current, owner);
  const candidate = useReviewedRefresh(original, previous, current, r, owner).plan;
  const dir = mkdtempSync(join(tmpdir(), 'planner-continuity-'));
  try {
    const form = 'planner-test', page = '/planner/';
    const file = join(dir, 'worker.mjs'); writeFileSync(file, workerSource('test', { [form]: planReviewForm(current, page) }));
    const worker = (await import(file)).default, held = new Map();
    const env = { LEADS: { get: async k => held.get(k) ?? null, put: async (k, v) => held.set(k, v) } };
    const submit = plan => worker.fetch(new Request('https://test.example/api/lead/' + form, { method: 'POST', headers: { origin: 'https://test.example', 'content-type': 'application/json' }, body: planRequest(plan, plan === original ? previous : current, { form, page, name: 'Marked test', email: 'test@example.invalid' }) }), env);
    const stale = await submit(original); assert.equal(stale.status, 400); assert.equal(held.size, 0);
    const accepted = await submit(candidate); assert.equal(accepted.status, 201);
    const receipt = (await accepted.json()).receipt; assert.match(receipt, /^lead:planner:/);
    const stored = JSON.parse(held.get(receipt));
    assert.deepEqual(JSON.parse(stored.fields.find(f => f.label === 'Fixture plan').value), candidate);
    const retry = await submit(candidate); assert.equal((await retry.json()).receipt, receipt);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
