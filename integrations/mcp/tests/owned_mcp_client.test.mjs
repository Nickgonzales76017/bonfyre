import { test, expect } from 'bun:test';
import { createOwnedMcpCall, mcpEndpoint } from '../owned_mcp_client.mjs';

test('outbound owner is explicitly bound, validates identity and never retries effects', async () => {
  let calls = 0;
  const call = createOwnedMcpCall({ endpoint: 'http://127.0.0.1:9999/mcp', token: 'secret', tools: ['owned'], request: async (_url, options) => {
    calls++; expect(options.redirect).toBe('error'); const message = JSON.parse(options.body);
    expect(options.headers.authorization).toBe('Bearer secret');
    return Response.json({ jsonrpc: '2.0', id: message.id, result: { structuredContent: { accepted: true } } });
  } });
  expect(await call('owned', {})).toEqual({ accepted: true });
  await expect(call('unowned', {})).rejects.toThrow('unbound'); expect(calls).toBe(1);
  const uncertain = createOwnedMcpCall({ endpoint: 'http://127.0.0.1:9999/mcp', token: 'secret', tools: ['owned'], request: async () => { calls++; throw new Error('secret connection lost'); } });
  await expect(uncertain('owned')).rejects.toThrow('[redacted] connection lost'); expect(calls).toBe(2);
});

test('outbound HTTP, identity, isError and byte boundaries cannot become success', async () => {
  for (const target of ['http://example.com/mcp', 'https://user:pw@example.com/mcp', 'https://example.com/mcp?token=x']) expect(() => mcpEndpoint(target)).toThrow();
  for (const response of [new Response('', { status: 403 }), Response.json({ jsonrpc: '2.0', id: 'wrong', result: {} }), new Response('x'.repeat(1025))]) {
    const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'], maxBytes: 1024, request: async () => response });
    await expect(call('owned')).rejects.toThrow();
  }
  const failed = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'], request: async (_url, options) =>
    Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id, result: { isError: true, structuredContent: { passed: true } } }) });
  const reported = await failed('owned').catch(error => error);
  expect(reported.code).toBe('MCP_REMOTE_ERROR');
  expect(reported.message).toContain('requires reconciliation');
});

test('shared outbound admission rejects concurrent and byte-pressure overload before fetch, then releases', async () => {
  const pending = [];
  let sent = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    maxBytes: 1024, maxConcurrent: 2, maxInFlightBytes: 1800,
    request: async (_url, options) => {
      sent++;
      return await new Promise(resolve => pending.push(() => resolve(Response.json({ jsonrpc: '2.0',
        id: JSON.parse(options.body).id, result: { structuredContent: { ok: true } } }))));
    } });
  const first = call('owned');
  await Bun.sleep(0);
  expect(sent).toBe(1);
  const refused = await call('owned').catch(error => error);
  expect(refused.code).toBe('MCP_ADMISSION');
  expect(sent).toBe(1);
  pending.shift()();
  expect(await first).toEqual({ ok: true });
  const second = call('owned'); await Bun.sleep(0);
  expect(sent).toBe(2); pending.shift()();
  expect(await second).toEqual({ ok: true });
});

test('maxConcurrent independently rejects a third request without queueing', async () => {
  const pending = [];
  let sent = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    maxBytes: 1024, maxConcurrent: 2, maxInFlightBytes: 4000,
    request: async (_url, options) => {
      sent++;
      return await new Promise(resolve => pending.push(() => resolve(Response.json({ jsonrpc: '2.0',
        id: JSON.parse(options.body).id, result: { structuredContent: { sent } } }))));
    } });
  const first = call('owned'), second = call('owned');
  await Bun.sleep(0);
  expect(sent).toBe(2);
  const rejected = await call('owned').catch(error => error);
  expect(rejected.code).toBe('MCP_ADMISSION'); expect(sent).toBe(2);
  pending.splice(0).forEach(release => release());
  expect((await Promise.all([first, second])).length).toBe(2);
});

test('deadline and caller abort span stalled response body and release admission', async () => {
  let sent = 0, cancelled = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    maxBytes: 1024, maxConcurrent: 1,
    request: async (_url, options) => {
      sent++;
      if (sent < 3) return new Response(new ReadableStream({
        cancel() { cancelled++; },
      }));
      return Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id, result: { structuredContent: { recovered: true } } });
    } });
  const timed = await call('owned', {}, { timeoutMs: 20 }).catch(error => error);
  expect(timed.code).toBe('MCP_OUTCOME_UNCERTAIN'); expect(cancelled).toBe(1);
  const controller = new AbortController(), aborted = call('owned', {}, { signal: controller.signal });
  await Bun.sleep(0); controller.abort();
  const unknown = await aborted.catch(error => error);
  expect(unknown.code).toBe('MCP_OUTCOME_UNCERTAIN'); expect(cancelled).toBe(2);
  expect(await call('owned')).toEqual({ recovered: true }); expect(sent).toBe(3);
});

test('HTTPS owner exact response identity succeeds; uncertain transport errors never retry', async () => {
  let sent = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    request: async (url, options) => {
      expect(url).toBe('https://owner.example/mcp'); sent++;
      return Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id,
        result: { structuredContent: { identity: 'exact' } } });
    } });
  expect(await call('owned')).toEqual({ identity: 'exact' }); expect(sent).toBe(1);
  const uncertain = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    request: async () => { sent++; throw new Error('secret transport failed'); } });
  const error = await uncertain('owned').catch(value => value);
  expect(error.code).toBe('MCP_OUTCOME_UNCERTAIN');
  expect(error.message).toContain('[redacted]'); expect(sent).toBe(2);
});

test('header deadline releases the slot and cancels a late response without replay', async () => {
  let sent = 0, release, cancelled = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    maxConcurrent: 1, request: async (_url, options) => {
      sent++;
      if (sent === 1) return await new Promise(resolve => { release = () => resolve(new Response(new ReadableStream({ cancel() { cancelled++; } }))); });
      return Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id, result: { structuredContent: { later: true } } });
    } });
  const timeout = await call('owned', {}, { timeoutMs: 20 }).catch(error => error);
  expect(timeout.code).toBe('MCP_OUTCOME_UNCERTAIN');
  expect(await call('owned')).toEqual({ later: true });
  release(); await Bun.sleep(0);
  expect(cancelled).toBe(1); expect(sent).toBe(2);
  const controller = new AbortController(); controller.abort();
  const early = await call('owned', {}, { signal: controller.signal }).catch(error => error);
  expect(early.code).toBe('MCP_CANCELLED'); expect(sent).toBe(2);
});

test('invalid declared response length cancels body before releasing admission', async () => {
  let cancelled = 0, sent = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'], maxConcurrent: 1,
    request: async (_url, options) => {
      sent++;
      if (sent === 1) return new Response(new ReadableStream({ cancel() { cancelled++; } }),
        { headers: { 'content-length': 'not-a-number' } });
      return Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id, result: { structuredContent: { ok: true } } });
    } });
  const malformed = await call('owned').catch(error => error);
  expect(malformed.code).toBe('MCP_OUTCOME_UNCERTAIN'); expect(cancelled).toBe(1);
  expect(await call('owned')).toEqual({ ok: true }); expect(sent).toBe(2);
});

test('abort between call return and queued fetch prevents any dispatch', async () => {
  let sent = 0;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['owned'],
    request: async () => { sent++; throw new Error('must not fetch'); } });
  const controller = new AbortController();
  const pending = call('owned', {}, { signal: controller.signal });
  controller.abort();
  const error = await pending.catch(value => value);
  expect(error.code).toBe('MCP_CANCELLED'); expect(sent).toBe(0);
});

test('remote isError after an effect is reported but never assumed effect-free or replayed', async () => {
  let sent = 0, effectCommitted = false;
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'secret', tools: ['write'],
    request: async (_url, options) => {
      sent++; effectCommitted = true;
      return Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id,
        result: { isError: true, structuredContent: { receipt_id: 'attempt-1', error: 'downstream failed after commit' } } });
    } });
  const error = await call('write').catch(value => value);
  expect(error.code).toBe('MCP_REMOTE_ERROR'); expect(error.message).toContain('requires reconciliation');
  expect(error.message).toContain('owner reported: downstream failed after commit');
  expect(effectCommitted).toBe(true); expect(sent).toBe(1);
});

test('owner error details are bounded and redact the dedicated credential', async () => {
  const call = createOwnedMcpCall({ endpoint: 'https://owner.example/mcp', token: 'dedicated-secret', tools: ['owned'],
    request: async (_url, options) => Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id,
      result: { isError: true, structuredContent: { error: `dedicated-secret ${'x'.repeat(3000)}` } } }) });
  const error = await call('owned').catch(value => value);
  expect(error.code).toBe('MCP_REMOTE_ERROR');
  expect(error.message).toContain('[redacted]');
  expect(error.message).not.toContain('dedicated-secret');
  expect(error.message.length).toBeLessThanOrEqual(2000);
});
