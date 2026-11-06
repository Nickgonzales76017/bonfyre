import { test, expect } from 'bun:test';
import { createWorkbookMcpController, workbookCorrection } from '../workbook-mcp-controller.mjs';
const read = { binding: 'ws-' + 'a'.repeat(40), key: 'finance-workbook:test', revision: 1, sha256: 'b'.repeat(64), rows: [], offset: 0, release: 'c'.repeat(64), saved_work_url: 'https://example.test/apps/test/_work' };
const result = value => ({ structuredContent: value, content: [] });
const preview = { ...read, preview_sha256: 'd'.repeat(64), review_time: '2026-10-17T00:00:00.000Z' };

test('account correction leaves untouched amount formulas and shortened fields out of the proposal', () => {
  const row = { row: 1, amount: '20', account: 'Office' };
  expect(workbookCorrection(row, { amount: '20', account: 'Travel' })).toEqual({ edits: [{ row: 1, role: 'account', value: 'Travel' }] });
  expect(workbookCorrection({ ...row, account: 'a'.repeat(500) }, { amount: '25', account: 'a'.repeat(500) }).edits).toEqual([{ row: 1, role: 'amount', value: '25' }]);
  expect(() => workbookCorrection(row, row)).toThrow('Change an amount');
});

test('host refresh preserves an unfinished correction until explicit reload', async () => {
  const c = createWorkbookMcpController({ call: async () => result({ ...read, revision: 2 }) });
  c.receive(result(read)); c.draftChanged(); c.receive(result({ ...read, revision: 2 }));
  expect(c.state.read.revision).toBe(1); expect(c.state.dirty).toBe(true);
  await c.load(); expect(c.state.read.revision).toBe(2); expect(c.state.dirty).toBe(false);
});

test('UI confirmation binds immutable changes; saving consumes confirmation and refreshes the native record', async () => {
  const calls = [], changes = { edits: [{ row: 1, role: 'account', value: 'Travel' }] };
  const c = createWorkbookMcpController({ requestId: () => 'review-save', call: async request => {
    calls.push(request); return result(request.name === 'workbook_preview' ? preview : request.name === 'workbook_update' ? { state: 'workbook_saved' } : { ...read, revision: 2 });
  } });
  c.receive(result(read)); await c.preview(changes); changes.edits[0].value = 'Changed outside preview';
  c.state.preview.preview_sha256 = 'altered'; await c.save();
  expect(calls[1].arguments).toMatchObject({ edits: [{ value: 'Travel' }], preview_sha256: preview.preview_sha256, request_id: 'review-save' });
  expect(c.state.read.revision).toBe(2); expect(c.state.preview).toBeNull();
  await expect(c.save()).rejects.toThrow('Preview'); expect(calls.filter(c => c.name === 'workbook_update')).toHaveLength(1);
});

test('lost save response blocks writes until an explicit successful reload and never replays the request', async () => {
  const calls = []; const c = createWorkbookMcpController({ call: async request => {
    calls.push(request); if (request.name === 'workbook_update') throw Error('Connection lost');
    return result(request.name === 'workbook_preview' ? preview : { ...read, revision: 2 });
  } });
  c.receive(result(read)); await c.preview({ reopen: [1] }); await expect(c.save()).rejects.toThrow('Connection lost');
  expect(c.state.uncertain.request_id).toBeTruthy();
  await expect(c.preview({ reopen: [1] })).rejects.toThrow('Reload');
  c.receive(result({ ...read, revision: 2 })); expect(c.state.uncertain).not.toBeNull();
  await c.load(); expect(c.state.uncertain).toBeNull(); expect(c.state.read.revision).toBe(2);
  expect(calls.filter(c => c.name === 'workbook_update')).toHaveLength(1);
});

test('host notifications cannot replace a confirmed preview or switch its account and workbook', async () => {
  let writes = 0; const c = createWorkbookMcpController({ call: async request => { if (request.name === 'workbook_update') writes++; return result(preview); } });
  c.receive(result(read)); c.receive(result({ ...read, binding: 'ws-' + 'e'.repeat(40) }));
  expect(c.state.error).toContain('separate review'); expect(c.state.read.binding).toBe(read.binding);
  await c.preview({ reopen: [1] }); c.receive(result({ ...read, revision: 2 }));
  expect(c.state.read.revision).toBe(1); await expect(c.save()).rejects.toThrow('Reload'); expect(writes).toBe(0);
});

test('concurrent clicks and tool errors cannot create overlapping saves or keep a stale preview', async () => {
  let finish; const pending = new Promise(resolve => { finish = resolve; });
  const c = createWorkbookMcpController({ call: async () => { await pending; return { isError: true, structuredContent: { error: 'Changed', status: 409 } }; } });
  c.receive(result(read)); const first = c.preview({ reopen: [1] });
  await expect(c.load()).rejects.toThrow('Wait'); finish(); await expect(first).rejects.toThrow('Changed');
  expect(c.state.preview).toBeNull(); expect(c.state.externalChange).toBe(true); expect(c.state.busy).toBe(false);
});

test('page continuation carries revision, digest, filter and release; editor link must resolve the same saved record', async () => {
  const calls = []; const c = createWorkbookMcpController({ call: async request => {
    calls.push(request);
    return result(request.name === 'workspace_views' ? { ...read, views: [{ view: 'workbook', url: 'https://unrelated.test/apps/a/_open' }] } : { ...read, offset: 50, status: 'pending' });
  } });
  c.receive(result(read)); await c.load({ offset: 50, status: 'pending' });
  expect(calls[0].arguments).toMatchObject({ expected_revision: 1, sha256: read.sha256, release: read.release, offset: 50, status: 'pending' });
  await expect(c.editor()).rejects.toThrow('Invalid workbook continuation');
  await expect(c.preview({ binding: 'ws-' + 'f'.repeat(40) })).rejects.toThrow('Unsupported');
});
