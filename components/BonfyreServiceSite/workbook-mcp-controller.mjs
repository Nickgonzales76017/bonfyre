// Both the embedded view and the full editor operate on the native workbook.
// This controller holds only the current page and an uncommitted preview.
export function toolValue(result) {
  const value = result?.structuredContent;
  if (result?.isError) throw Object.assign(Error(value?.error || 'The workbook operation failed.'), value);
  if (!value || typeof value !== 'object') throw Error('The workbook service returned no structured result.');
  return value;
}
const identity = value => ({ binding: value.binding, key: value.key });
const version = value => ({ ...identity(value), expected_revision: value.revision, sha256: value.sha256 });
const sameRecord = (a, b) => a.binding === b.binding && a.key === b.key;
export function workbookCorrection(row, fields) {
  const edits = ['amount', 'account'].filter(role => String(fields[role]) !== String(row[role] ?? '')).map(role => ({ row: row.row, role, value: fields[role] }));
  if (!edits.length) throw Error('Change an amount or account before previewing.');
  return { edits };
}
export function createWorkbookMcpController({ call, changed = () => {}, requestId = () => crypto.randomUUID() }) {
  let state = { read: null, preview: null, busy: false, dirty: false, error: '', uncertain: null, externalChange: false, status: 'all' };
  let proposal = null;
  const notify = () => changed(structuredClone(state));
  function assertRead(value) {
    if (!/^ws-[a-f0-9]{40}$/.test(value?.binding) || !/^finance-workbook:[a-zA-Z0-9_-]{1,100}$/.test(value?.key) || !Number.isSafeInteger(value.revision) || value.revision < 1 || !/^[a-f0-9]{64}$/.test(value.sha256) || !Array.isArray(value.rows)) throw Error('Invalid workbook review result.');
    if (state.read && !sameRecord(state.read, value)) throw Error('Open the other workbook in a separate review.');
  }
  async function invoke(name, args) { return toolValue(await call({ name, arguments: args })); }
  async function run(fn) {
    if (state.busy) throw Error('Wait for the current workbook operation.');
    state.busy = true; state.error = ''; notify();
    try { return await fn(); }
    catch (error) { state.error = error.message; if (error.status === 409) { proposal = null; state.preview = null; state.externalChange = true; } throw error; }
    finally { state.busy = false; notify(); }
  }
  function ready() {
    if (!state.read) throw Error('Open a saved workbook first.');
    if (state.uncertain || state.externalChange) throw Error('Reload the saved workbook before continuing.');
  }
  function accept(value) {
    assertRead(value); state.read = structuredClone(value); state.status = value.status ?? state.status; state.preview = null; proposal = null;
    state.uncertain = null; state.externalChange = false; state.dirty = false;
  }
  return {
    get state() { return structuredClone(state); },
    draftChanged() { if (!state.busy) { state.dirty = true; notify(); } },
    receive(result) {
      try {
        const value = toolValue(result); assertRead(value);
        if (state.busy || state.preview || state.uncertain || state.dirty) { state.externalChange = true; notify(); return; }
        accept(value); state.error = ''; notify();
      } catch (error) { state.error = error.message; notify(); }
    },
    load({ status = state.status, offset = 0 } = {}) {
      return run(async () => {
        if (!state.read) throw Error('Open a saved workbook first.');
        const args = { ...identity(state.read), status, offset, limit: 50, ...(state.read.release ? { release: state.read.release } : {}), ...(offset ? version(state.read) : {}) };
        const value = await invoke('workbook_review', args); accept(value); state.status = status; return value;
      });
    },
    inspect(row, column, columns = 1) {
      return run(async () => { ready(); return invoke('workbook_review', { ...version(state.read), limit: 1, inspect: { sheetId: state.read.table.sheetId, row, column, rows: 1, columns } }); });
    },
    preview(changes) {
      return run(async () => {
        ready(); state.preview = null; proposal = null;
        if (Object.keys(changes).some(key => !['table', 'edits', 'reviewed', 'reopen', 'note'].includes(key))) throw Error('Unsupported review change.');
        const args = structuredClone({ ...changes, ...version(state.read) });
        const result = await invoke('workbook_preview', args);
        if (!sameRecord(state.read, result) || result.revision !== state.read.revision || result.sha256 !== state.read.sha256 || !/^[a-f0-9]{64}$/.test(result.preview_sha256) || !result.review_time) throw Error('The preview does not match this workbook revision.');
        if (state.externalChange) throw Error('The workbook changed during preview. Reload before continuing.');
        proposal = { ...args, review_time: result.review_time, preview_sha256: result.preview_sha256 };
        state.preview = structuredClone(result); state.dirty = false; return result;
      });
    },
    discard() { if (state.busy) return; proposal = null; state.preview = null; notify(); },
    save() {
      return run(async () => {
        ready(); if (!proposal || !state.preview) throw Error('Preview the changes before saving.');
        const args = { ...proposal, request_id: requestId() };
        // Consume confirmation before dispatch. A lost reply must never replay it.
        proposal = null; state.preview = null; state.uncertain = { request_id: args.request_id };
        const saved = await invoke('workbook_update', args);
        state.uncertain.saved = true;
        const value = await invoke('workbook_review', { ...identity(state.read), status: state.status, limit: 50, ...(state.read.release ? { release: state.read.release } : {}) });
        accept(value); return saved;
      });
    },
    editor() {
      return run(async () => {
        ready(); if (!state.read.release) throw Error('Open this review with the installed workspace release to continue in its editor.');
        const result = await invoke('workspace_views', { ...identity(state.read), revision: state.read.revision, release: state.read.release });
        if (!sameRecord(state.read, result) || result.sha256 !== state.read.sha256) throw Error('The workbook changed. Reload before opening the editor.');
        const view = result.views?.find(view => view.view === 'workbook');
        if (!view) throw Error('A compatible workbook editor is unavailable.');
        const url = new URL(view.url), origin = new URL(state.read.saved_work_url).origin;
        if (url.origin !== origin || !url.pathname.startsWith('/apps/') || !url.pathname.endsWith('/_open')) throw Error('Invalid workbook continuation.');
        return url.href;
      });
    }
  };
}
