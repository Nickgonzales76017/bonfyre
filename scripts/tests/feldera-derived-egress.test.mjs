import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { canonical, ingest, occurrence } from '../feldera-derived-egress.mjs';

test('canonical digest matches Python sorted compact Unicode escaping', () => {
  expect(canonical({ z: 'é', a: { y: 2, x: 1 } })).toBe('{"a":{"x":1,"y":2},"z":"\\u00e9"}');
  expect(occurrence('p', 'v', 'insert', { b: 2, a: 1 }, '2026-09-30T00:00:00+00:00').payloadJson).toBe('{"a":1,"b":2}');
});

test('streaming chunks persist only valid operations and preserve derived receipt fields', async () => {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE derived_occurrence(id TEXT PRIMARY KEY, program_id TEXT, view TEXT, op TEXT, digest TEXT, payload TEXT, extra TEXT, stamp TEXT, kind TEXT, created TEXT)');
  const encoder = new TextEncoder();
  const body = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode('[{"insert":{"b":2,"a":1}}]\nnot json\n{"del'));
      controller.enqueue(encoder.encode('ete":{"id":1}}\n'));
      controller.close();
    },
  });
  const fetcher = async (endpoint, options) => {
    expect(endpoint).toBe('http://fixture/v0/pipelines/pipe/egress/view?format=json&backpressure=true');
    expect(options.method).toBe('POST');
    return { ok: true, body };
  };
  const stamp = '2026-09-30T00:00:00+00:00';
  expect(await ingest({ db, url: 'http://fixture/', pipeline: 'pipe', view: 'view', program: 'program', fetcher, now: () => stamp })).toBe(2);
  const rows = db.query('SELECT * FROM derived_occurrence ORDER BY op').all();
  expect(rows).toHaveLength(2);
  expect(rows[1].id).toBe(occurrence('program', 'view', 'insert', { a: 1, b: 2 }, stamp).id);
  expect(rows.every(row => row.kind === 'derived')).toBe(true);
  db.close();
});
