#!/usr/bin/env bun
// Outside-agent read circuit: discovery, quoted price, refusal, source overlap, events.
import { options, circuit } from './client.mjs';

export async function run(config, transport = circuit(config)) {
  const { http, check, finish } = transport;
  const card = await http('/.well-known/agent.json');
  check('agent card advertises skills', card.status === 200 && Array.isArray(card.data?.skills) && card.data.skills.length > 0, `${card.data?.skills?.length || 0} skills`);

  const catalogue = await http('/capability');
  const capabilities = Object.fromEntries((catalogue.data?.capabilities || []).map(capability => [capability.name, capability]));
  check('catalogue lists capabilities', catalogue.status === 200 && Object.keys(capabilities).length > 0, `${Object.keys(capabilities).length} listed`);
  const priced = Object.values(capabilities).filter(capability => capability.priced);
  check('at least one capability is priced', priced.length > 0, `priced: ${priced.map(capability => capability.name).join(', ')}`);
  if (priced.length) {
    const selected = priced[0], quote = await http(`/mpp/quote?capability=${encodeURIComponent(selected.name)}`);
    check('MPP quote equals the catalogue price', quote.status === 200 && Math.abs(Number(quote.data?.amount) - Number(selected.priceUsd)) < 1e-9,
      `${selected.name}: quote ${quote.data?.amount} vs catalogue ${selected.priceUsd}`);
    const path = `/capability/${encodeURIComponent(selected.name)}`;
    const unpaid = await http(path);
    const accepted = unpaid.data?.accepts?.[0] || {};
    check('unpaid request is 402 with x402 requirements', unpaid.status === 402 && accepted.scheme === 'exact', `status ${unpaid.status}, scheme ${accepted.scheme}`);
    const fake = await http(path, { headers: { 'X-PAYMENT': 'eyJmYWtlIjoxfQ==' } });
    check('unverified payment does not deliver goods', fake.status === 402, `status ${fake.status}`);
  }

  const reso = await http('/reso/Property?limit=25');
  const rows = Array.isArray(reso.data?.value) ? reso.data.value : [];
  check('RESO returns property records', reso.status === 200 && rows.length > 0, `${rows.length} rows`);
  const ogc = await http('/ogc/collections/parcels/items?limit=25');
  check('OGC returns a FeatureCollection', ogc.status === 200 && ogc.data?.type === 'FeatureCollection');
  const resoKeys = new Set(rows.map(row => row.ListingKey).filter(Boolean));
  const ogcKeys = new Set((ogc.data?.properties_rows || []).map(row => row?.account).filter(Boolean));
  const overlap = [...resoKeys].filter(key => ogcKeys.has(key));
  check('RESO and OGC describe the same parcels', overlap.length > 0, `${overlap.length} of ${resoKeys.size} keys shared`);
  const assessed = rows.find(row => row.TaxAssessedValue);
  if (assessed) check('assessed value is a real number', typeof assessed.TaxAssessedValue === 'number', `${assessed.ListingKey} = ${assessed.TaxAssessedValue}`);
  const events = await http('/events?limit=5');
  check('CloudEvents batch is spec-shaped', events.status === 200 && Array.isArray(events.data) && events.data.length > 0 && events.data[0].specversion === '1.0', `${Array.isArray(events.data) ? events.data.length : 0} events`);
  return finish();
}

if (import.meta.main) {
  try { process.exitCode = await run(options()); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
