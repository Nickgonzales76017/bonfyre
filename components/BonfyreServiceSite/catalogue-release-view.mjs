import { loadGrid, catalogueGridTheme } from './catalogue-grid.mjs';
import { releasePrefix, prepareCatalogueRelease, reviewCatalogueRelease, saveCatalogueRelease, verifyCatalogueRelease, releaseDigest } from './catalogue-release.mjs';
import { draftKey } from './catalogue-model.mjs';
import { escapeHTML as esc } from './workspace-navigation.mjs';
const usd = cents => cents === null ? 'Needs source check' : new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(cents / 100);

export function catalogueReleases(host, { config, fetchProduct, storage, openProduct, onSelect = () => {}, lifetime }) {
  let active = false, busy = false, api, store, rows = [], selected = null, current = null, serial = 0, stale = false, noteDirty = false, pending = null;
  const $ = name => host.querySelector(`[data-release="${name}"]`);
  const closed=()=>lifetime?.closed??false,assertOpen=()=>lifetime?.assert();
  const notice = message => { if(closed())return; $('notice').textContent = message; };
  function controls() {
    if(closed())return;
    host.setAttribute('aria-busy', String(busy));
    for (const control of host.querySelectorAll('button,input,textarea')) control.disabled = busy || !!pending && control.matches('input,textarea');
    $('stage').disabled = busy || !!pending || !(api?.getSelectedRows().length);
    $('review').disabled = busy || !!pending || !selected || stale || !current || current.impact.unresolved.length > 0 || current.impact.impacted.some(room => room.blockers.length);
    $('refresh').disabled = busy || !!pending || !selected; $('download').disabled = busy || !selected; $('retry').hidden = !pending;
  }
  async function run(fn) {
    if (closed()||busy) return; busy = true; notice(''); controls();
    try { await fn(); } catch (error) { notice(error.message + (pending ? ' Save status is unknown. Reload saved releases or retry the same save.' : '')); } finally { busy = false; controls(); }
  }
  async function prepare(keys, title, existing) {
    assertOpen();
    const products = [];
    for (const key of keys) { const entry = config.entries.find(product => draftKey(product) === key); if (!entry) throw Error('This release includes products outside the current catalogue.'); products.push(await fetchProduct(entry)); }
    const result=await prepareCatalogueRelease({ keys, title, products, records: rows, catalogues: config.fixture_catalogues ?? [], ...(existing ? { id: existing.id, createdAt: existing.created_at } : {}) });assertOpen();return result;
  }
  function list() {
    if(closed())return;
    const releases = rows.filter(row => row.key.startsWith(releasePrefix) && row.value?.schema === 'service-site.catalogue-release.v1' && Array.isArray(row.value.entries) && row.value.entries.some(entry => entry?.draft?.store === store));
    $('list').innerHTML = releases.map(row => `<button type="button" class="cr-release ${selected?.key === row.key ? 'is-selected' : ''}" aria-pressed="${selected?.key === row.key}" data-open-release="${esc(row.key)}"><strong>${esc(row.value.title)}</strong><span>${row.value.state === 'reviewed' ? 'Review complete' : 'Staged'} · ${row.value.entries.length} products</span></button>`).join('') || '<p>No staged releases.</p>';
    $('release-count').textContent = releases.length;
  }
  async function open(row) {
    selected = null; current = null; stale = false; noteDirty = false; draw();
    if (!row) throw Error('The release is no longer available. Reload saved releases.');
    await verifyCatalogueRelease(row.value);assertOpen();
    if (row.key !== releasePrefix + row.value.id) throw Error('The release identity does not match its record.');
    if (row.value.store !== store) throw Error('This release belongs to another merchant. Select that store first.');
    selected = row; host.querySelector('.cr-new').open = false;
    try { current = await prepare(row.value.entries.map(entry => entry.key), row.value.title, row.value); stale = current.sha256 !== row.value.sha256; }
    catch (error) { stale = true; notice(error.message); }
    assertOpen();draw(); list(); onSelect(row.key);
  }
  function draw() {
    if(closed())return;
    $('detail').hidden = !selected; if (!selected) return;
    const value = selected.value;
    $('title').textContent = value.title;
    $('state').textContent = stale ? 'Sources changed · refresh required' : value.state === 'reviewed' ? 'Review complete' : 'Staged';
    $('summary').textContent = `${value.entries.length} products · ${value.entries.reduce((n, entry) => n + entry.draft.changes.length, 0)} changes · ${value.impact.impacted.length} affected rooms`;
    $('changes').innerHTML = value.entries.flatMap(entry => entry.draft.changes.map(change => `<tr><th><button type="button" class="cw-text-button" data-release-product="${esc(entry.key)}">${esc(entry.title)}</button></th><td>${esc(change.label ?? change.field)}</td><td>${esc(change.before || 'Empty')}</td><td>${esc(change.after || 'Empty')}</td></tr>`)).join('');
    const fieldLabels = item => item.fields.map(field => { const entry=value.entries.find(entry=>entry.draft.product_id===item.product_id&&entry.draft.source_url===item.source_url); return esc(entry?.draft.changes.find(change=>change.field===field)?.label ?? (field.startsWith('price:')?'Listed price':field.startsWith('sku:')?'SKU':'Product field')); }).join(', ');
    $('impact').innerHTML = value.impact.impacted.map(room => `<article class="cr-room"><h3>${esc(room.title)}</h3><div class="cr-totals"><span>Listed<strong>${usd(room.before_cents)}</strong></span><span>Proposed<strong>${usd(room.proposed_cents)}</strong></span><span>Difference<strong>${usd(room.delta_cents)}</strong></span></div>${room.exceeds_budget ? `<p class="bad">${usd(room.proposed_cents - room.budget_cents)} over the fixture budget</p>` : ''}<details class="cr-placements"><summary>${room.items.length} affected placements</summary><ul>${room.items.map(item => `<li>${esc(item.label)} · ${esc(item.placement)} · ${fieldLabels(item)}</li>`).join('')}</ul></details>${room.layout_issues.map(message => `<p>${esc(message)}</p>`).join('')}${room.blockers.map(message => `<p class="bad">${esc(message)}</p>`).join('')}</article>`).join('') + value.impact.unresolved.map(row => `<article class="cr-room"><h3>Room source check</h3><code>${esc(row.key)}</code><p class="bad">${esc(row.reason)}</p></article>`).join('') || `<p>${value.impact.checked} saved rooms checked. No matching selected products.</p>`;
    $('coverage').textContent = value.impact.coverage;
    $('note').value = value.review?.note ?? '';
    $('review').textContent = value.state === 'reviewed' ? 'Save review note' : 'Finish review';
    controls();
  }
  async function reload() {
    rows = await storage.readLocal();assertOpen();
    if (pending) {
      const kept = rows.find(row => row.key === releasePrefix + pending.value.id);
      if (kept && kept.revision > pending.revision) {
        const exact = kept.revision === pending.revision + 1 && await releaseDigest(kept.value) === await releaseDigest(pending.value);
        assertOpen();selected = kept; pending = null;
        notice(exact ? 'Saved release recovered.' : 'A newer release is saved. Review that version before making changes.');
      }
    }
    const drafts = rows.filter(row => row.key.startsWith('draft:catalogue:') && row.value?.store === store);
    api.setGridOption('rowData', drafts.map(row => ({ key: row.key, title: (Array.isArray(row.value.changes) ? row.value.changes.find(change => change?.field === 'title')?.after : null) ?? config.entries.find(product => draftKey(product) === row.key)?.title ?? row.value.product_id,
      changes: row.value.changes?.length ?? 0, revision: row.revision, source: row.value.source_url })));
    if (selected) { const latest = rows.find(row => row.key === selected.key); if (latest) await open(latest); else { selected = null; draw(); } }
    list(); draw();
  }
  async function save(value, revision = 0) {
    assertOpen();
    pending = { value: structuredClone(value), revision };
    try { const saved = await saveCatalogueRelease(storage, pending.value, revision);assertOpen(); pending = null; return saved; }
    catch (error) { if (error.status >= 400 && error.status < 500) { pending = null; stale = true; } throw error; }
  }
  const discardNote = () => !closed() && (!noteDirty || confirm('Discard your unsaved review note?'));
  host.innerHTML = `<div class="cw-desk-heading"><div><h1>Releases <span data-release="release-count">0</span></h1></div><button type="button" class="cw-button" data-release="reload">Reload saved</button></div>
    <div class="cr-layout"><aside class="cr-sidebar" aria-label="Saved releases" data-release="list"></aside><div class="cr-content">
    <details class="cr-new" open><summary>Stage saved product drafts</summary><div class="cr-create"><label>Release name<input data-release="name" maxlength="120" placeholder="October collection update"></label><button type="button" class="cw-button cw-button-primary" data-release="stage">Stage selected drafts</button></div><div class="cr-grid" data-release="grid" aria-label="Saved product drafts"></div></details>
    <section data-release="detail" hidden><div class="cr-heading"><h2 data-release="title"></h2><span class="cw-tag" data-release="state"></span></div><p data-release="summary"></p><div class="cr-buttons"><button type="button" class="cw-button" data-release="refresh">Refresh sources</button><button type="button" class="cw-button" data-release="download">Download review</button></div>
    <details open><summary>Product changes</summary><div class="cr-table"><table><thead><tr><th>Product</th><th>Field</th><th>Original</th><th>Proposed</th></tr></thead><tbody data-release="changes"></tbody></table></div></details>
    <details open><summary>Room impact</summary><div data-release="impact" class="cr-rooms"></div><p data-release="coverage" class="cr-caption"></p></details>
    <label>Review note<textarea data-release="note" maxlength="2000" rows="3"></textarea></label><button type="button" class="cw-button cw-button-primary" data-release="review">Finish review</button><p class="cr-caption">Review records readiness. Merchant publication is not connected.</p></section>
    <button type="button" class="cw-button" data-release="retry" hidden>Retry same save</button><p data-release="notice" role="status" aria-live="polite"></p></div></div>`;
  $('stage').onclick = () => { if (!discardNote()) return; return run(async () => {
    const keys = api.getSelectedRows().map(row => row.key); rows = await storage.readLocal();
    const value = await prepare(keys, $('name').value);
    const saved = await save(value); selected = saved; await reload();
    $('name').value = ''; host.querySelector('.cr-new').open = false; notice('Release staged.');
  }); };
  $('reload').onclick = () => { if (discardNote()) run(reload); };
  $('retry').onclick = () => run(async () => { if (!pending) return; const attempt = pending; selected = await save(attempt.value, attempt.revision); await reload(); notice('Release save confirmed.'); });
  $('refresh').onclick = () => { if (!discardNote()) return; return run(async () => {
    const previous = selected; rows = await storage.readLocal();
    const value = await prepare(previous.value.entries.map(entry => entry.key), previous.value.title, previous.value);
    selected = await save(value, previous.revision); await reload(); notice('Sources refreshed. Review the updated release.');
  }); };
  $('review').onclick = () => run(async () => {
    const previous = selected, note = $('note').value; rows = await storage.readLocal();
    const now = await prepare(previous.value.entries.map(entry => entry.key), previous.value.title, previous.value);
    const value = await reviewCatalogueRelease(previous.value, now, note);
    selected = await save(value, previous.revision); await reload(); notice('Review saved.');
  });
  $('download').onclick = () => {
    if (closed()||!selected || busy) return;
    const url = URL.createObjectURL(new Blob([JSON.stringify(selected.value, null, 2)], { type: 'application/json' }));
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = `catalogue-release-${selected.value.id}.json`; anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  $('note').oninput = () => { noteDirty = true; };
  host.addEventListener('click', event => {
    const button = event.target.closest('[data-open-release],[data-release-product]'); if (closed()||!button || busy || pending) return;
    if (button.dataset.openRelease) { if (!discardNote()) return; run(() => open(rows.find(row => row.key === button.dataset.openRelease))); }
    else { const product = config.entries.find(product => draftKey(product) === button.dataset.releaseProduct); if (product) openProduct(product); }
  });
  lifetime?.onClose(()=>{active=false;serial++;api?.destroy();api=null;rows=[];selected=null;current=null;pending=null;noteDirty=false;});
  return {
    busy: () => busy || !!pending, dirty: () => active && (noteDirty || !!pending),
    close() { active = false; serial++; },
    async activate(nextStore, key = null) {
      if(closed())return;
      active = true; store = nextStore; selected = null; current = null; noteDirty = false; const generation = ++serial;
      await run(async () => {
        const library = await loadGrid(); if (!active || generation !== serial) return;
        if (!api) api = library.createGrid($('grid'), { theme: catalogueGridTheme(library,44), rowData: [], rowSelection: { mode: 'multiRow' }, getRowId: params => params.data.key,
          defaultColDef: { sortable: true, resizable: true }, columnDefs: [{ field: 'title', headerName: 'Saved product', flex: 1, minWidth: 180 }, { field: 'changes', headerName: 'Changes', width: 100 }, { field: 'revision', headerName: 'Revision', width: 100 }], onSelectionChanged: controls });
        await reload(); if (key) await open(rows.find(row => row.key === key)); draw();
      });
    }
  };
}
