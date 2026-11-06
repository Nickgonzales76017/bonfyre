// Explicitly scoped outbound owner calls. No retries: a lost response may have
// followed an accepted effect and requires caller-owned reconciliation.
import { randomUUID } from 'node:crypto';
import { json, object } from './http_boundary.mjs';

const OUTCOME_UNCERTAIN = 'MCP_OUTCOME_UNCERTAIN';
function classified(message, code, token) {
  const error = new Error(String(message).replaceAll(token, '[redacted]').slice(0, 2000));
  error.code = code;
  return error;
}
function until(promise, signal) {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const abort = () => { signal.removeEventListener('abort', abort); reject(signal.reason); };
    signal.addEventListener('abort', abort, { once: true });
    Promise.resolve(promise).then(value => { signal.removeEventListener('abort', abort); resolve(value); },
      error => { signal.removeEventListener('abort', abort); reject(error); });
  });
}
async function boundedResponse(response, maxBytes, signal) {
  const length = response.headers.get('content-length');
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > maxBytes) || !response.body) {
    response.body?.cancel().catch(() => {});
    throw new Error('invalid MCP owner body size');
  }
  const reader = response.body.getReader(), chunks = [];
  let size = 0;
  try {
    for (;;) {
      const { value, done } = await until(reader.read(), signal);
      if (done) break;
      size += value.byteLength;
      if (size > maxBytes) throw new Error('MCP owner body exceeds budget');
      chunks.push(value);
    }
    if (!size) throw new Error('invalid MCP owner body size');
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, size)));
  } finally {
    reader.cancel().catch(() => {});
    try { reader.releaseLock(); } catch { /* An aborted read can still be settling. */ }
  }
}

export function mcpEndpoint(value) {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.hash || url.search
      || url.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname))
    throw new Error('MCP endpoint requires loopback or HTTPS without URL credentials');
  return url.href;
}

export function createOwnedMcpCall({ endpoint, token, tools, request = fetch, maxBytes = 20_000_000,
  maxConcurrent = 16, maxInFlightBytes = 64_000_000 }) {
  endpoint = mcpEndpoint(endpoint);
  if (!token || !Array.isArray(tools) || !tools.length || tools.some(name => typeof name !== 'string' || !name)) throw new Error('invalid MCP owner binding');
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 30_000_000) throw new Error('invalid MCP response budget');
  if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent < 1 || maxConcurrent > 64 ||
      !Number.isSafeInteger(maxInFlightBytes) || maxInFlightBytes < maxBytes + 1 || maxInFlightBytes > 512_000_000)
    throw new Error('invalid MCP in-flight bounds');
  const allowed = new Set(tools);
  let active = 0, reservedBytes = 0;
  return async (name, args = {}, { timeoutMs = 300_000, signal } = {}) => {
    if (!allowed.has(name) || !object(args)) throw new Error('unbound MCP operation');
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 3_600_000) throw new Error('invalid MCP timeout');
    const id = randomUUID();
    const body = json({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } });
    const bodyBytes = Buffer.byteLength(body), reserve = maxBytes + bodyBytes;
    if (bodyBytes > maxBytes || reserve > maxInFlightBytes) throw classified('MCP request exceeds budget', 'MCP_ADMISSION', token);
    if (signal?.aborted) throw classified('MCP owner call cancelled before dispatch', 'MCP_CANCELLED', token);
    if (active >= maxConcurrent || reservedBytes + reserve > maxInFlightBytes)
      throw classified('MCP owner outbound admission limit', 'MCP_ADMISSION', token);
    active++; reservedBytes += reserve;
    const controller = new AbortController();
    const abort = () => controller.abort(signal.reason ?? new Error('MCP owner call cancelled'));
    signal?.addEventListener('abort', abort, { once: true });
    const timer = setTimeout(() => controller.abort(new Error('MCP owner deadline exceeded')), timeoutMs);
    let dispatched = false;
    try {
      const pending = Promise.resolve().then(() => {
        controller.signal.throwIfAborted();
        dispatched = true;
        return request(endpoint, { method: 'POST', redirect: 'error', signal: controller.signal,
          headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'MCP-Protocol-Version': '2025-06-18' }, body });
      });
      let claimed = false;
      pending.then(response => { if (controller.signal.aborted && !claimed) response?.body?.cancel().catch(() => {}); }).catch(() => {});
      const res = await until(pending, controller.signal); claimed = true;
      if (!res.ok) { res.body?.cancel().catch(() => {}); throw new Error(`MCP owner HTTP ${res.status}`); }
      const value = await boundedResponse(res, maxBytes, controller.signal);
      if (!object(value) || value.jsonrpc !== '2.0' || value.id !== id || value.error || !object(value.result))
        throw classified('MCP owner returned invalid response identity', OUTCOME_UNCERTAIN, token);
      // A structured isError is a reported remote error, not proof that the
      // owner performed no effect. The caller must reconcile its receipt.
      if (value.result.isError === true) {
        const reason = value.result.structuredContent?.error;
        const detail = typeof reason === 'string' ? `; owner reported: ${reason}` : '';
        throw classified(`MCP owner reported error; effect outcome requires reconciliation${detail}`, 'MCP_REMOTE_ERROR', token);
      }
      if (Object.hasOwn(value.result, 'structuredContent')) return value.result.structuredContent;
      for (const part of value.result.content || []) if (part.type === 'text') {
        try { return JSON.parse(part.text); } catch { return { text: part.text }; }
      }
      return value.result;
    } catch (error) {
      if (error?.code === 'MCP_REMOTE_ERROR' || error?.code === 'MCP_ADMISSION') throw error;
      if (!dispatched) throw classified('MCP owner call cancelled before dispatch', 'MCP_CANCELLED', token);
      throw classified(error?.message || 'MCP owner call failed', OUTCOME_UNCERTAIN, token);
    } finally {
      clearTimeout(timer); signal?.removeEventListener('abort', abort);
      active--; reservedBytes -= reserve;
    }
  };
}
