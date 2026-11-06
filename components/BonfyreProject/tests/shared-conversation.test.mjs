import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { extractSharedConversation, readSharedConversation, selectMessages } from '../tools/shared-conversation.mjs';

const source = 'https://chatgpt.com/share/abc-123';
function fixture(messages) {
  const table = [];
  const put = value => {
    if (value === null) return -5;
    const index = table.length;
    table.push(null);
    table[index] = Array.isArray(value) ? value.map(put) : value && typeof value === 'object'
      ? Object.fromEntries(Object.entries(value).map(([k, v]) => [`_${put(k)}`, put(v)])) : value;
    return index;
  };
  put({ loaderData: { 'routes/share.$shareId.($action)': { serverResponse: { data: {
    title: 'A shared strategy', conversation_id: 'abc-123', linear_conversation: messages.map(message => ({ message }))
  } } } } });
  return `<script>window.__reactRouterContext.streamController.enqueue(${JSON.stringify(JSON.stringify(table) + '\n')});globalThis.shouldNeverRun = true;</script>`;
}
const msg = (id, role, text, extra = {}) => ({ id, author: { role }, content: { content_type: 'text', parts: [text] }, ...extra });

test('extracts visible text without executing scripts or mixing roles and channels', () => {
  const html = fixture([null, msg('system', 'system', 'hidden'), msg('u', 'user', 'funding "quoted" \\ path'),
    msg('analysis', 'assistant', 'hidden', { metadata: { channel: 'analysis' } }),
    msg('call', 'assistant', 'tool input', { recipient: 'browser' }), msg('result', 'tool', 'tool output'),
    msg('hidden', 'user', 'hidden', { metadata: { is_visually_hidden_from_conversation: true } }),
    msg('a', 'assistant', 'visible answer', { channel: 'final', recipient: 'all' }),
    msg('image', 'user', '', { content: { content_type: 'multimodal_text', parts: ['image caption', { asset_pointer: 'private' }] } })]);
  const doc = extractSharedConversation(html, source);
  expect(doc.messages.map(m => m.id)).toEqual(['u', 'a', 'image']);
  expect(doc.messages[0].text).toBe('funding "quoted" \\ path');
  expect(doc.messages[2].omitted_nontext_parts).toBe(1);
  expect(doc.omitted).toEqual({ other_roles: 2, nonvisible_channels: 2, tool_calls: 1, nontext: 0, empty_nodes: 1 });
  expect(globalThis.shouldNeverRun).toBeUndefined();
});

test('literal query pagination preserves source position and full single-message reads', () => {
  const doc = extractSharedConversation(fixture([msg('1', 'user', 'funding '.repeat(200)), msg('2', 'assistant', 'funding too'), msg('3', 'user', 'Funding next')]), source);
  const first = selectMessages(doc, { role: 'user', query: 'FUNDING', limit: 1 });
  expect(first.matched).toBe(2);
  expect(first.next_offset).toBe(1);
  expect(first.messages[0].text.length).toBe(900);
  expect(selectMessages(doc, { role: 'user', offset: 1 }).messages[0].id).toBe('3');
  expect(selectMessages(doc, { message: '1' }).messages[0].text).toBe('funding '.repeat(200));
  expect(() => selectMessages(doc, { limit: 101 })).toThrow();
  expect(() => selectMessages(doc, { offset: -1 })).toThrow();
});

test('rejects login pages, source identity mismatch, malformed data and duplicate messages', () => {
  const html = fixture([msg('1', 'user', 'ok')]);
  expect(() => extractSharedConversation('<html>Login</html>', source)).toThrow('absent');
  expect(() => extractSharedConversation(html, 'https://chatgpt.com/share/abc-456')).toThrow('identity');
  expect(() => extractSharedConversation(html, 'https://example.com/share/abc-123')).toThrow('source');
  expect(() => extractSharedConversation(html.replace('enqueue("', 'enqueue(run("'), source)).toThrow('JSON string');
  expect(() => extractSharedConversation(fixture([msg('1', 'user', 'a'), msg('1', 'assistant', 'b')]), source)).toThrow('duplicate');
});

test('receipt digest binds parsing to retained bytes and refuses denied observations', () => {
  const dir = mkdtempSync(join(tmpdir(), 'shared-conversation-'));
  try {
    const bodyPath = join(dir, 'page'), receiptPath = join(dir, 'receipt');
    const body = fixture([msg('1', 'user', 'ok')]);
    writeFileSync(bodyPath, body);
    const receipt = { schema: 'bonfyre-foreign-observation.v1', observation_kind: 'source_observed', http_status: 200,
      policy_decision: 'allow', body_path: bodyPath, source_url: source, content_sha256: createHash('sha256').update(body).digest('hex') };
    writeFileSync(receiptPath, JSON.stringify(receipt));
    expect(readSharedConversation(receiptPath).messages[0].text).toBe('ok');
    writeFileSync(bodyPath, `${body}changed`);
    expect(() => readSharedConversation(receiptPath)).toThrow('digest mismatch');
    writeFileSync(receiptPath, JSON.stringify({ ...receipt, policy_decision: 'deny' }));
    expect(() => readSharedConversation(receiptPath)).toThrow('allowed successful');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
