import { test, expect } from 'bun:test';
import { options, circuit } from './client.mjs';
import { run as fleming } from './fleming_close_circuit.mjs';
import { run as marketplace } from './marketplace_circuit.mjs';
import { run as crossProtocol, priceMap } from './cross_protocol_circuit.mjs';
import { run as interactionStack } from './interaction_stack_circuit.mjs';

test('circuits require an explicit origin and gate effects', async () => {
  expect(() => options([])).toThrow('--base is required');
  expect(() => options(['--base', 'https://user:password@example.test'])).toThrow('uncredentialed');
  const readOnly = circuit(options(['--base', 'https://example.test']));
  await expect(readOnly.http('/studio/v1/economy/offer', { method: 'POST' })).rejects.toThrow('--apply');
  await expect(readOnly.http('/make/create?type=Work', { effect: true })).rejects.toThrow('--apply');
  await expect(marketplace(options(['--base', 'https://example.test']))).rejects.toThrow('BONFYRE_CIRCUIT_TOKEN');
  await expect(crossProtocol(options(['--base', 'https://example.test']))).rejects.toThrow('--parcel');
  await expect(interactionStack(options(['--base', 'https://example.test']))).rejects.toThrow('--parcel');
});

test('Beckn and ordinary product price forms use the same numeric comparison', () => {
  expect(priceMap({ products: [{ id: 'one', price: { value: '1.25' } }] })).toEqual({ one: 1.25 });
  expect(priceMap({ message: { catalog: { 'bpp/providers': [{ items: [{ id: 'one', price: { value: '1.25' } }] }] } } }, true)).toEqual({ one: 1.25 });
});

test('outside-agent circuit checks price, refusal, shared parcels and events', async () => {
  const answers = {
    '/.well-known/agent.json': { skills: [{ id: 'one' }] },
    '/capability': { capabilities: [{ name: 'priced', priced: true, priceUsd: 2 }] },
    '/mpp/quote?capability=priced': { amount: 2 },
    '/capability/priced': { accepts: [{ scheme: 'exact' }] },
    '/reso/Property?limit=25': { value: [{ ListingKey: 'parcel', TaxAssessedValue: 50 }] },
    '/ogc/collections/parcels/items?limit=25': { type: 'FeatureCollection', properties_rows: [{ account: 'parcel' }] },
    '/events?limit=5': [{ specversion: '1.0' }],
  };
  const checks = [];
  const transport = {
    http: async (path, opts = {}) => ({ status: path === '/capability/priced' ? 402 : 200, data: answers[path] }),
    check: (name, condition) => { checks.push([name, Boolean(condition)]); return Boolean(condition); },
    finish: () => checks.some(([, ok]) => !ok) ? 1 : 0,
  };
  expect(await fleming({ base: 'https://example.test' }, transport)).toBe(0);
  expect(checks.length).toBeGreaterThan(9);
});
