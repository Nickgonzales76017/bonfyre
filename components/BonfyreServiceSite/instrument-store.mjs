// Shared instrument lifecycle. Native work stays with its destination owner;
// public drafts use the existing structured browser record owner.
const meta = name => globalThis.document?.querySelector(`meta[name="${name}"]`)?.content ?? '';
export function instrumentEndpoint() {
  const endpoint = meta('bonfyre-workspace-store');
  if (endpoint && !/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint)) throw Error('The instrument has an invalid account binding.');
  return endpoint;
}
const copy = value => structuredClone(value);
const localOwner = () => import('./instrument-local-store.mjs');
const announce = (key, message) => globalThis.window?.dispatchEvent(new CustomEvent('instrument:storage', {detail:{key,message}}));

// Installed working copies never read or write an origin-wide browser draft.
export async function openInstrumentScratch(key, fallback = null) {
  if (!/^[a-z0-9-]{1,100}$/.test(key)) throw Error('Use a bounded instrument scratch identity.');
  const native = !!instrumentEndpoint();
  let value = copy(fallback), revision = 0, chain = Promise.resolve(), failed = null;
  let description = native ? 'Unsaved changes in this tab; use Save to keep them in this account.' : 'No browser changes yet.';
  let owner;
  if (!native) {
    owner = await localOwner();
    const record = 'draft:instrument-scratch:' + key;
    const rows = await owner.readLocal(record), exact = rows.find(row => row.key === record);
    if (exact) { value = copy(exact.value); revision = exact.revision; description = 'Kept in this browser.'; }
    else {
      // Migrate an old public draft once; retain its original bytes.
      let legacy; try { legacy = localStorage.getItem(key); } catch {}
      if (legacy) {
        if (legacy.length > 2 * 1024 * 1024) throw Error('The previous browser draft needs a bounded import. Its original was kept.');
        value = JSON.parse(legacy);
        const saved = await owner.saveLocal(record, value, 0); revision = saved.revision;
        description = 'Previous browser draft retained in shared structured storage.';
      }
    }
  }
  return {
    native, get description() { return description; }, get() { return copy(value); },
    set(next) {
      value = copy(next);
      if (native) return true;
      if (failed) return false;
      const snapshot = copy(value);
      description = 'Keeping changes in this browser…';
      chain = chain.then(async () => {
        if (failed) return;
        try { const saved = await owner.saveLocal('draft:instrument-scratch:' + key, snapshot, revision); revision = saved.revision; description = 'Kept in this browser.'; }
        catch (error) { failed = error; description = 'Browser draft was not saved: ' + error.message + '. Your current edits remain in this tab.'; }
        announce(key, description);
      });
      return true;
    },
    async flush() { await chain; if (failed) throw failed; }
  };
}

async function sameValue(a, b, depth = 0) {
  if (Object.is(a, b)) return true;
  if (depth > 32 || a == null || b == null || typeof a !== typeof b) return false;
  if (a instanceof Blob || b instanceof Blob) {
    if (!(a instanceof Blob && b instanceof Blob) || a.size !== b.size || a.type !== b.type) return false;
    if ((a.name ?? null) !== (b.name ?? null) || (a.lastModified ?? null) !== (b.lastModified ?? null)) return false;
    return sameValue(new Uint8Array(await a.arrayBuffer()), new Uint8Array(await b.arrayBuffer()), depth + 1);
  }
  if (a instanceof ArrayBuffer || b instanceof ArrayBuffer) return a instanceof ArrayBuffer && b instanceof ArrayBuffer && sameValue(new Uint8Array(a), new Uint8Array(b), depth + 1);
  if (ArrayBuffer.isView(a) || ArrayBuffer.isView(b)) {
    if (!(ArrayBuffer.isView(a) && ArrayBuffer.isView(b)) || a.constructor !== b.constructor || a.byteLength !== b.byteLength) return false;
    const x = new Uint8Array(a.buffer, a.byteOffset, a.byteLength), y = new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
    return x.every((v, i) => v === y[i]);
  }
  if (typeof a !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a), other = Object.keys(b);
  if (keys.length !== other.length) return false;
  for (const key of keys) if (!Object.hasOwn(b, key) || !await sameValue(a[key], b[key], depth + 1)) return false;
  return true;
}

export async function attachInstrumentStore({ prefix, label, get, set, mount }) {
  if (!/^[a-z][a-z0-9-]*:$/.test(prefix) || typeof get !== 'function' || typeof set !== 'function' || !mount) throw Error('An instrument needs its own record prefix and working editor.');
  const endpoint = instrumentEndpoint(), native = !!endpoint;
  const store = native ? (await import(new URL('../bridge/workspace-store.mjs', import.meta.url).href)).openDestinationStore(endpoint) : await localOwner();
  const make = (tag, text) => { const n = document.createElement(tag); if (text) n.textContent = text; return n; };
  const bar = make('div'); bar.className = 'instrument-store'; bar.setAttribute('role','region'); bar.setAttribute('aria-label','Saved work');
  const where = make('span', native ? 'Saving to this Aurekai destination' : 'Saving in this browser'); where.className = 'is-where';
  const nameLabel = make('label','Name'), name = make('input'); name.dataset.is = 'name'; name.maxLength = 80; name.placeholder = 'Name this ' + label; nameLabel.append(name);
  const button = (text, id) => { const b = make('button', text); b.type = 'button'; b.dataset.is = id; return b; };
  const saveButton = button('Save','save'), copyButton = button('Save a copy','copy'), refreshButton = button('Refresh saved work','refresh');
  const select = make('select'); select.dataset.is = 'list'; select.setAttribute('aria-label','Open saved work');
  const status = make('span'); status.dataset.is = 'said'; status.setAttribute('role','status'); status.setAttribute('aria-live','polite');
  const checkButton = button('Check saved result','check'), retryButton = button('Retry this save','retry'); checkButton.hidden = retryButton.hidden = true;
  bar.append(where, nameLabel, saveButton, copyButton, select, refreshButton, checkButton, retryButton, status);
  const style = make('style'); style.textContent = '.instrument-store{display:flex;flex-wrap:wrap;align-items:end;gap:10px;padding:16px;border:1px solid currentColor;border-radius:12px;margin:0 0 18px}.instrument-store label{display:grid;gap:4px}.instrument-store input,.instrument-store select,.instrument-store button{min-height:36px;max-width:100%;font:inherit}.instrument-store .is-where{flex-basis:100%;font-size:12px}.instrument-store [data-is=said]{flex-basis:100%}.instrument-store [hidden]{display:none!important}.instrument-store [data-save-state=conflict]{font-weight:600}.instrument-discard{max-width:450px;padding:24px;border-radius:12px}.instrument-discard button{margin:6px;min-height:40px}@media(max-width:640px){.instrument-store>*{max-width:100%}.instrument-store select{width:100%}}';
  mount.prepend(style, bar);
  let active = null, pending = null, busy = false, dirty = false, dirtyEpoch = 0;
  let baseline = copy(await get()), baselineName = '', listEpoch = 0, listed = [];
  const said = (text, state = 'ready') => { status.textContent = text; bar.dataset.saveState = state; };
  const canKey = key => typeof key === 'string' && key.startsWith(prefix) && key.length < 512 && !/[\u0000-\u001f\u007f]/.test(key);
  const exact = async key => native ? store.readVersion(key) : (await store.readLocal(key)).find(row => row.key === key) ?? null;
  function controls() {
    saveButton.disabled = copyButton.disabled = busy || !!pending;
    select.disabled = busy || !!pending; refreshButton.disabled = busy;
    checkButton.hidden = !pending; checkButton.disabled = busy;
    retryButton.hidden = !pending?.retryable; retryButton.disabled = busy;
  }
  function address(key) { const u = new URL(location.href); u.searchParams.set('record', key); history.replaceState(null,'',u); }
  async function isDirty() { return name.value.trim() !== baselineName || !await sameValue(await get(), baseline); }
  // Pointer gestures and dialog fields are not necessarily edits to this record.
  // Be conservative until a snapshot comparison finishes; old comparisons may
  // never clear edits made after them or after a new saved baseline is opened.
  function reconcileDirty() {
    const ticket = ++dirtyEpoch; dirty = true;
    queueMicrotask(async () => {
      const prior = baseline, priorName = baselineName;
      try {
        const snapshot = copy(await get()), title = name.value.trim();
        const changed = title !== priorName || !await sameValue(snapshot, prior);
        if (ticket === dirtyEpoch && prior === baseline && priorName === baselineName) dirty = changed;
      } catch { /* A failed comparison keeps the leave-page guard. */ }
    });
  }
  const mark = event => { if (!bar.contains(event.target)) reconcileDirty(); };
  for (const event of ['input','change','pointerup','drop']) mount.addEventListener(event, mark);
  name.addEventListener('input', reconcileDirty);
  window.addEventListener('beforeunload', event => { if (dirty || pending) { event.preventDefault(); event.returnValue = ''; } });
  window.addEventListener('instrument:storage', event => {
    if (!native) { const node = mount.querySelector('[data-ui=save-state]'); if (node) node.textContent = event.detail.message; }
  });
  function drawList() {
    const option = make('option', listed.length ? 'Open saved work…' : 'Nothing saved yet'); option.value = ''; select.replaceChildren(option);
    for (const row of listed) {
      const o = make('option', String(row.value?.name || row.key).slice(0,160)); o.value = row.key; select.append(o);
    }
    if (active && listed.some(row => row.key === active.key)) select.value = active.key;
  }
  async function list() {
    const ticket = ++listEpoch;
    let rows;
    if (native) {
      rows = []; let cursor = '', epoch = -1;
      for (let n = 0; n < 10; n++) {
        const result = await store.readMetadata(prefix, {cursor,epoch}); rows.push(...result.rows);
        if (rows.length > 1000) throw Error('Use the native saved-work page for more than 1,000 records.');
        if (!result.more) break;
        if (n === 9) throw Error('The saved-work list exceeds this instrument’s bound.');
        cursor = result.cursor; epoch = result.epoch;
      }
    } else { rows = await store.readLocal(prefix); if (rows.length > 1000) throw Error('The browser has more than 1,000 saved records for this instrument.'); }
    if (ticket !== listEpoch) return [];
    if (rows.some(row => !canKey(row.key))) throw Error('The saved list changed instrument identity.');
    listed = rows.sort((a,b) => String(b.value?.saved_at ?? '').localeCompare(String(a.value?.saved_at ?? '')));
    drawList(); return rows;
    // Listing metadata never advances the revision of the state in the editor.
  }
  async function confirmOpen() {
    if (!await isDirty()) return true;
    return new Promise(resolve => {
      const old = document.activeElement, dialog = make('dialog'); dialog.className = 'instrument-discard'; dialog.setAttribute('aria-label','Unsaved instrument changes');
      dialog.append(make('h2','Open another saved record?'),make('p','Your current edits have not been saved. Keep editing, or discard them and open the selected record.'));
      const keep = button('Keep editing','keep'), discard = button('Discard changes and open','discard'); dialog.append(keep,discard); document.body.append(dialog);
      let done = false; const finish = yes => { if(done)return; done=true; dialog.close(); dialog.remove(); old?.focus(); resolve(yes); };
      keep.onclick = () => finish(false); discard.onclick = () => finish(true); dialog.addEventListener('cancel',e=>{e.preventDefault();finish(false);}); dialog.showModal(); keep.focus();
    });
  }
  async function open(key, {initial = false} = {}) {
    if (busy || pending) return false;
    if (!canKey(key)) { said('This link names work outside this instrument.','error'); return false; }
    busy = true; controls();
    try {
      const before = copy(await get());
      if (!initial && !await confirmOpen()) { drawList(); return false; }
      const row = await exact(key);
      if (!row || row.deleted) throw Error('That saved work is not here any more. Your current work was kept.');
      if (!row.value || typeof row.value.name !== 'string' || !Object.hasOwn(row.value,'state')) throw Error('The selected record is not an instrument draft.');
      try { await set(copy(row.value.state)); } catch(error) { await set(before); throw error; }
      active = {key,revision:row.revision}; baseline = copy(await get()); baselineName = row.value.name; name.value = baselineName; dirtyEpoch++; dirty = false;
      address(key); drawList(); said(`Opened "${baselineName}" (version ${row.revision}).`); return true;
    } catch(error) { said(error.message,'error'); drawList(); return false; }
    finally { busy = false; controls(); }
  }
  async function confirmed(row, operation) {
    active = {key:row.key,revision:row.revision}; baseline = copy(operation.value.state); baselineName = operation.value.name;
    pending = null; const ticket = ++dirtyEpoch, changed = await isDirty(); if (ticket === dirtyEpoch) dirty = changed; address(row.key);
    said(`Saved "${baselineName}" (version ${row.revision}).${dirty ? ' Newer edits are still unsaved.' : ''}`,'saved');
    // A verified commit is usable now; optional metadata must not hold the
    // saving lock or turn a later editor's state into the older save result.
    const savedKey = row.key, savedRevision = row.revision, savedName = operation.value.name;
    void list().catch(() => {
      if (active?.key === savedKey && active.revision === savedRevision && !pending)
        said('Saved "' + savedName + '" (version ' + savedRevision + '); the saved-work list could not be refreshed.','saved');
    });
  }
  async function commit(operation) {
    try {
      const row = await store.saveLocal(operation.key, operation.value, operation.revision);
      await confirmed(row,operation); return true;
    } catch(error) {
      if (error.status === 403 || error.status === 401) { pending = null; said('This account cannot save here. Your current edits remain in this tab.','refused'); }
      else if (error.status === 409 || /changed in another tab|draft changed/i.test(error.message)) { pending = null; said('A newer saved version exists. Keep your edits as a copy, or open that version after reviewing the discard prompt.','conflict'); }
      else { pending = operation; operation.retryable = false; said('The save could not be confirmed. Check the saved result before retrying; your edits are still here.','uncertain'); }
      return false;
    }
  }
  async function save(asCopy = false) {
    if (busy || pending) return false;
    busy = true; controls();
    try {
      const title = name.value.trim() || label; name.value = title;
      const operation = {key:active && !asCopy ? active.key : prefix + crypto.randomUUID(), revision:active && !asCopy ? active.revision : 0,
        value:{name:title,saved_at:new Date().toISOString(),save_id:crypto.randomUUID(),state:copy(await get())},retryable:false};
      return await commit(operation);
    } catch(error) { said('Not saved: ' + error.message,'error'); return false; }
    finally { busy = false; controls(); }
  }
  async function check() {
    if (busy || !pending) return false;
    busy = true; controls(); const operation = pending;
    try {
      const row = await exact(operation.key);
      if (row && !row.deleted && row.revision === operation.revision + 1 && await sameValue(row.value,operation.value)) { await confirmed(row,operation); return true; }
      if ((!row && operation.revision === 0) || row && !row.deleted && row.revision === operation.revision) {
        operation.retryable = true; said('This exact save is not present. Retry the reviewed save.','uncertain'); return false;
      }
      pending = null; said('The destination changed. Your edits are kept; save a separate copy or review the current version.','conflict'); return false;
    } catch(error) { said('Could not check the saved result: ' + error.message,'uncertain'); return false; }
    finally { busy = false; controls(); }
  }
  async function retry() {
    if (busy || !pending?.retryable) return false;
    busy = true; controls(); try { return await commit(pending); } finally { busy = false; controls(); }
  }
  saveButton.onclick = () => void save(); copyButton.onclick = () => void save(true);
  checkButton.onclick = () => void check(); retryButton.onclick = () => void retry();
  refreshButton.onclick = () => void list().catch(error=>said('Saved work could not be listed: '+error.message,'error'));
  select.onchange = () => { if(select.value)void open(select.value); };
  if (!native) {
    try {
      const legacy = localStorage.getItem('aurekai-instrument:' + prefix);
      if (legacy) {
        if (legacy.length > 2 * 1024 * 1024) throw Error('The older named-work collection exceeds its import limit. Its originals were retained.');
        const values = JSON.parse(legacy), existing = new Set((await store.readLocal(prefix)).map(row => row.key));
        if (!values || Array.isArray(values) || typeof values !== 'object') throw Error('The older named-work collection is not a record map.');
        const entries = Object.entries(values);
        if (entries.length > 1000) throw Error('Import at most 1,000 older named drafts.');
        const changes = [];
        for (const [key,value] of entries) {
          if (!canKey(key) || !value || typeof value.name !== 'string' || !Object.hasOwn(value,'state')) throw Error('An older named draft needs review before import. Its original was retained.');
          if (!existing.has(key)) changes.push({key,value,expectedRevision:0});
        }
        if (changes.length) { await store.saveLocalBatch(changes); said('Older named drafts retained in shared structured storage.'); }
      }
    } catch(error) { said('Older browser drafts were not imported: ' + error.message,'error'); }
  }
  await list().catch(error=>said('Saved work could not be listed: '+error.message,'error'));
  const wanted = new URL(location.href).searchParams.get('record');
  if (wanted) await open(wanted,{initial:true});
  async function checkpoint(){if(busy||pending||!active||await isDirty())throw Error('Save the current review before preparing its delivery plan.');return {key:active.key,revision:active.revision,name:baselineName};}
  controls(); return {native,save,open,list,check,retry,checkpoint};
}
