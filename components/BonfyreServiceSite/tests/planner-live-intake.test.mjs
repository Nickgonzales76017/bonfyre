import { test, expect } from 'bun:test';
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { plannerFiles } from '../planner-material.mjs';
import { startingPlan, planRequest, planBrief } from '../planner-model.mjs';

// Explicitly writes one marked synthetic enquiry to our own published site.
// Re-running against the same catalogue uses the same request and receipt.
const live = process.env.PLANNER_VERIFY_LIVE === '1';
const site = join(import.meta.dir, '../../../packs/aurekai-work-samples/site');
const origin = 'https://aurekai-work-samples.pages.dev';
const proof = join(site, 'design/verification-planner-live/intake');
const hash = bytes => createHash('sha256').update(bytes).digest('hex');

test.skipIf(!live)('published planner accepts a complete enquiry and gives its retry the same receipt', async () => {
  const { catalogue } = plannerFiles(JSON.parse(readFileSync(join(site, 'fixture-planner.json'), 'utf8')));
  const plan = startingPlan(catalogue, 'compact', 'aurekai-live-verification-20261008');
  plan.title = 'Aurekai verification only — no customer enquiry';
  plan.notes = 'Synthetic verification: retain this complete plan for native handoff checks. Do not contact the supplier or treat this as a sales lead. Café pop-up.';
  plan.deadline = '2026-12-10';
  const request = planRequest(plan, catalogue, {
    form: 'planner-red3-fixtures', page: '/red3-planner/',
    name: 'Aurekai verification only', email: 'verification@example.invalid'
  });
  mkdirSync(proof, { recursive: true });
  const evidence = { at: new Date().toISOString(), origin, plan, request, expected_brief: planBrief(plan, catalogue), assets: [], responses: [], complete: false };
  const retain = () => writeFileSync(join(proof, 'intake-verification.json'), JSON.stringify(evidence, null, 2) + '\n');
  retain();
  try {
    for (const path of ['/red3-planner/index.html', '/data/red3-fixtures/planner-model.mjs']) {
      const response = await fetch(origin + path, { cache: 'no-store', signal: AbortSignal.timeout(20000) });
      expect(response.status).toBe(200);
      const actual = hash(Buffer.from(await response.arrayBuffer())), expected = hash(readFileSync(join(site, 'dist', path)));
      evidence.assets.push({ path, actual, expected });
      expect(actual).toBe(expected);
    }
    for (let attempt = 0; attempt < 2; attempt++) {
      const response = await fetch(origin + '/api/lead/planner-red3-fixtures', {
        method: 'POST', headers: { origin, 'content-type': 'application/json' }, body: request, signal: AbortSignal.timeout(20000)
      });
      const body = await response.json();
      evidence.responses.push({ status: response.status, body });
      retain();
      expect([200, 201]).toContain(response.status);
      expect(body.ok).toBe(true);
      expect(body.receipt).toMatch(/^lead:planner:[a-f0-9]{64}$/);
    }
    expect(evidence.responses[1].body.receipt).toBe(evidence.responses[0].body.receipt);
    expect(evidence.responses[1].status).toBe(200);
    evidence.complete = true;
  } finally { retain(); }
}, 90000);
