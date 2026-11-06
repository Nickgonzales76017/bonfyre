import { test, expect } from 'bun:test';
import { liveSurfaceEnabled, surface } from './live-surface.mjs';

const live = test.skipIf(!liveSurfaceEnabled);

async function external() {
  const { status, body } = await surface('/_surface/external-estate');
  expect(status).toBe(200);
  return body;
}

live('catalogue is larger than walked census', async () => {
  const page = await external();
  const census = await surface('/_surface/internet-census');
  expect(census.status).toBe(200);
  expect(page.count).toBeGreaterThan(10 * census.body.count);
});

live('catalogued resource cannot claim observation', async () => {
  const row = (await external()).records[0];
  for (const field of ['tier', 'twin', 'state', 'last_seen', 'extract', 'cadence', 'http_status']) {
    expect(row).not.toHaveProperty(field);
  }
  expect(row.cannot_infer).toBeTruthy();
  expect(row.cannot_infer.toLowerCase()).toContain('not a read one');
});

live('every catalogue row retains identity', async () => {
  for (const row of (await external()).records) {
    expect(row.ext_id.startsWith('EXT-')).toBe(true);
    expect(row.name).toBeTruthy();
    expect(row.estate).toBeTruthy();
  }
});

live('description does not repeat its URL', async () => {
  const repeats = (await external()).records.filter(row => row.url && (row.says || '').startsWith(row.url));
  expect(repeats.map(row => row.ext_id)).toEqual([]);
});

live('catalogue is sliceable', async () => {
  const whole = await external();
  const mcp = await surface('/_surface/external-estate?estate=~MCP');
  expect(mcp.status).toBe(200);
  expect(mcp.body.count).toBeGreaterThan(0);
  expect(mcp.body.count).toBeLessThan(whole.count);
  for (const row of mcp.body.records) expect(row.estate).toContain('MCP');
});

live('catalogue slices compound', async () => {
  const one = await surface('/_surface/external-estate?functions=~Government');
  const two = await surface('/_surface/external-estate?functions=~Government&estate=~Public+API');
  expect(one.status).toBe(200);
  expect(two.status).toBe(200);
  expect(two.body.count).toBeLessThanOrEqual(one.body.count);
});

live('unknown catalogue slice is refused', async () => {
  const { status, body } = await surface('/_surface/external-estate?nope=1');
  expect(status).toBe(400);
  expect(body.error).toBe('unknown_slice');
  expect(body.sliceable).toContain('ext_id');
});

live('census remains a small map of sources actually walked', async () => {
  const { status, body } = await surface('/_surface/internet-census');
  expect(status).toBe(200);
  expect(body.count).toBeLessThan(1000);
  expect(body.records.some(row => row.state === 'observed')).toBe(true);
});
