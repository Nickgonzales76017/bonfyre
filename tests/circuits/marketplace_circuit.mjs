#!/usr/bin/env bun
// Operator circuit; requires an explicit isolated target and write-scoped token.
import { options, circuit } from './client.mjs';

export async function run(config, transport = circuit(config)) {
  if (!config.apply || !config.token || !config.allowUntargetedClaim) throw new Error('marketplace circuit requires --apply, BONFYRE_CIRCUIT_TOKEN and --allow-untargeted-claim on an isolated estate');
  const { http, check, finish } = transport;
  const auth = { token: config.token };
  const unique = Date.now().toString(36);
  const offerName = `circuit-estate-coverage-${unique}`;
  const worker = `circuit-worker-${unique}`;

  const offered = await http('/studio/v1/economy/offer', { method: 'POST', body: { name: offerName, buy: '0.40', sell: '3.50', source: 'fleming' }, ...auth });
  check('offer accepted', offered.status === 200, `status ${offered.status}`);
  const catalogue = await http('/capability');
  const capability = (catalogue.data?.capabilities || []).find(item => item.name === offerName) || {};
  check('newly listed capability is now priced in the catalogue', capability.priced && Math.abs(Number(capability.priceUsd) - 3.5) < 1e-9, `priceUsd ${capability.priceUsd}`);
  const quote = await http(`/mpp/quote?capability=${encodeURIComponent(offerName)}`);
  check('quote matches the new listing', Math.abs(Number(quote.data?.amount) - 3.5) < 1e-9, `${quote.data?.amount}`);
  const unpaid = await http(`/capability/${encodeURIComponent(offerName)}`);
  check('newly listed capability now demands payment', unpaid.status === 402, `status ${unpaid.status}`);

  const reso = await http('/reso/Property?limit=200');
  const rows = (reso.data?.value || []).filter(row => row.ListingKey && row.TaxAssessedValue);
  check('projection yields usable parcels', rows.length > 0, `${rows.length} with assessed value`);
  if (!rows.length) return finish();
  const target = rows.reduce((best, row) => Number(row.TaxAssessedValue) > Number(best.TaxAssessedValue) ? row : best);
  const account = String(target.ListingKey).replace(/[^A-Za-z0-9]/g, '').slice(0, 32);
  const opened = await http('/studio/v1/fleming/opportunity', { method: 'POST', body: { account }, ...auth });
  check('opportunity becomes a durable mission', opened.status === 200 && opened.data?.mission_id, opened.data?.mission_id || '');
  if (!opened.data?.mission_id) return finish();

  // claim-next cannot target our mission. This circuit is restricted to an
  // isolated estate because the claim itself can lease unrelated work.
  const claim = await http('/studio/v1/work/claim', { method: 'POST', body: { worker }, ...auth });
  check('claim issues a lease token', claim.status === 200 && String(claim.data?.claim_token || '').length === 64, claim.data?.status || '');
  check('claimed mission is the circuit mission', claim.data?.mission_id === opened.data.mission_id, `${claim.data?.mission_id || 'none'} vs ${opened.data.mission_id}`);
  if (claim.data?.mission_id !== opened.data.mission_id || !claim.data?.claim_token) return finish();
  const fields = { mission: claim.data.mission_id, node: claim.data.node_id, worker };
  const wrong = await http('/studio/v1/work/complete', { method: 'POST', body: { ...fields, token: 'a'.repeat(64) }, ...auth });
  check('a wrong lease cannot complete the work', wrong.data?.result === 'stale_claim' || wrong.status !== 200, `status ${wrong.status}`);
  const done = await http('/studio/v1/work/complete', { method: 'POST', body: { ...fields, token: claim.data.claim_token }, ...auth });
  check('the lease holder completes it', done.data?.status === 'complete', done.data?.status || '');

  const before = await http('/studio/v1/membrane', auth);
  const beforeBook = Array.isArray(before.data) ? before.data[0] : null;
  const cost = await http('/studio/v1/economy/cost', { method: 'POST', body: { recipe: 'fleming-harvest', stage: 'underwrite', model: 'local', usd: '1.25' }, ...auth });
  check('cost recorded', cost.status === 200);
  const after = await http('/studio/v1/membrane', auth);
  const afterBook = Array.isArray(after.data) ? after.data[0] : null;
  check('membrane cost_records advanced', afterBook?.cost_records === beforeBook?.cost_records + 1, `${beforeBook?.cost_records} -> ${afterBook?.cost_records}`);
  check('membrane counts the new service', afterBook?.services >= beforeBook?.services, `${beforeBook?.services} -> ${afterBook?.services}`);
  check('book margin reflects the listing', Number(afterBook?.book_margin_usd) >= 3.10, `margin ${afterBook?.book_margin_usd}`);
  const events = await http('/events?limit=200');
  const ids = new Set(Array.isArray(events.data) ? events.data.map(event => event.id) : []);
  check('the mission just created appears in the event log', ids.has(opened.data.mission_id), `${ids.size} events scanned`);
  return finish();
}

if (import.meta.main) {
  try { process.exitCode = await run(options()); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
