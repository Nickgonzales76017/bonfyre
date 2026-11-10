import { createReview, detectReview, projectReview, markRowsReviewed, reopenReviewRows, reviewRoles } from './workbook-review.mjs';
import { validateWorkbook } from './workbook-model.mjs';

let library;
function loadGrid() {
  if (!library) library = new Promise((resolve, reject) => {
    const script = document.createElement('script'); script.src = new URL('./ag-grid-community.min.js', import.meta.url);
    script.onload = () => resolve(globalThis.agGrid);
    script.onerror = () => { library = null; script.remove(); reject(Error('The review table could not load.')); }; document.head.append(script);
  }); return library;
}
const node = (tag, text) => { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; return el; };

export function mountWorkbookReview(host, { getSnapshot, getSource, getReview, setReview, editCell, openCell, recalculate, runTask, changed }) {
  let api, busy = false, operation = false, externalBusy = false, active = false, serial = 0, inspectorSerial = 0, selectedRow = null, baseline, baselineSource;
  host.className = 'finance-review';
  host.innerHTML = `<style>.finance-review [hidden]{display:none!important}
    .finance-review .fr-stage:has(.fr-inspector[hidden]){grid-template-columns:minmax(0,1fr)}
    .finance-review[hidden]{display:none}.finance-review{padding:12px 0}.fr-toolbar,.fr-metrics,.fr-bulk{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin:12px 0}.fr-metrics strong{font-size:24px}.fr-metrics span{padding:12px 18px;background:#eaf1ec;border:1px solid #cbd8d0;border-radius:8px}.fr-stage{display:grid;grid-template-columns:minmax(0,1fr) 310px;gap:16px}.fr-grid{height:65vh;min-height:420px}.fr-inspector{border:1px solid #cbd8d0;border-radius:8px;padding:16px;overflow-wrap:anywhere}.fr-inspector table{border-collapse:collapse;width:100%;font-size:13px}.fr-inspector th,.fr-inspector td{padding:8px 4px;border-bottom:1px solid #dbe3dc;text-align:left}.fr-mapping{display:flex;gap:12px;flex-wrap:wrap;padding:16px;border:1px solid #cbd8d0;margin:12px 0}.fr-mapping label{display:grid;gap:6px;max-width:200px}.finance-review button,.finance-review input,.finance-review select{min-height:44px;font:inherit}.finance-review input{max-width:100%;box-sizing:border-box}.fr-bulk input{flex:1;min-width:200px}.fr-toolbar input{flex:1;min-width:150px}.fr-inspector button{margin:8px 8px 0 0}.fr-note{font-size:13px;color:#53665a}@media(max-width:850px){.fr-stage{grid-template-columns:1fr}.fr-inspector{order:-1}.fr-grid{height:60vh}.fr-metrics{gap:6px}.fr-metrics span{padding:8px}.fr-mapping label{max-width:100%}}
  </style><div class="fr-toolbar"><input data-review-search type="search" placeholder="Search transactions" aria-label="Search transactions"><select data-review-filter aria-label="Review status"><option value="pending">Needs review</option><option value="all">All rows</option><option value="reviewed">Reviewed</option><option value="duplicates">Possible duplicates</option></select><button data-review-map type="button">Columns</button></div>
  <form class="fr-mapping" data-review-mapping hidden></form><div class="fr-metrics" data-review-metrics aria-live="polite"></div>
  <div class="fr-bulk"><strong data-review-count>0 selected</strong><input data-review-note maxlength="1000" aria-label="Review note" placeholder="Review note"><button data-review-accept type="button">Mark reviewed</button><button data-review-reopen type="button">Reopen review</button></div>
  <div class="fr-stage"><div class="fr-grid" data-review-grid aria-label="Transaction review"></div><aside class="fr-inspector" data-review-inspector hidden></aside></div><details class="fr-account-summary"><summary>Amounts by account</summary><div data-review-accounts></div></details><p data-review-status role="status"></p>`;
  const $ = key => host.querySelector(`[data-review-${key}]`);
  function notice(message) { const target=$('status');if(target)target.textContent = message; }
  function updateState() { busy = operation || externalBusy; host.setAttribute('aria-busy', String(busy)); for (const el of host.querySelectorAll('button,input,select')) el.disabled = busy; }
  function state(value) { operation = value; updateState(); }
  function selections() { const n = api?.getSelectedRows().length ?? 0; $('count').textContent = `${n} selected`; }
  async function sourceSnapshot() {
    const source = getSource(); if (source === baselineSource) return baseline;
    baselineSource = source; baseline = null;
    if (source?.baseline instanceof Blob && source.baseline.size <= 12 * 1024 * 1024) baseline = source.baseline.text().then(text => validateWorkbook(JSON.parse(text)));
    return baseline;
  }
  async function inspect(row) {
    const version = ++inspectorSerial; selectedRow = row?.row ?? null; const panel = $('inspector'); panel.hidden = !row; panel.replaceChildren(); if (!row) return;
    panel.append(node('h2', row.description || 'Transaction'), node('p', row.address), node('p', row.status));
    const table = node('table'), heading = node('tr'); ['Field', 'Imported', 'Current'].forEach(label => heading.append(node('th', label))); table.append(heading);
    const before = await sourceSnapshot(); if (version !== inspectorSerial || !active) return;
    const review = getReview(), current = getSnapshot(), originalSheet = before?.sheets[review.table.sheetId];
    for (const role of ['date', 'description', 'amount', 'account', 'suggested']) {
      const col = review.table.columns[role]; if (col === undefined) continue;
      const line = node('tr'); line.append(node('th', reviewRoles[role]), node('td', String(originalSheet?.cellData?.[row.row]?.[col]?.v ?? 'Unavailable')), node('td', String(current.sheets[review.table.sheetId]?.cellData?.[row.row]?.[col]?.v ?? ''))); table.append(line);
    }
    panel.append(table); const addressNote = node('p', before ? 'Imported values show this address in the original workbook. Inserted or moved rows require a source check.' : 'Original cell preview is unavailable for this earlier review.'); addressNote.className = 'fr-note'; panel.append(addressNote);
    if (row.source.sheet || row.source.row || row.source.cell) { panel.append(node('h3', 'Reported source'), node('p', `${row.source.sheet} · row ${row.source.row} · ${row.source.cell}`)); if (row.source.sha256) { const hash = node('small', row.source.sha256); panel.append(hash); } }
    const open = node('button', 'Open workbook row'); open.type = 'button'; open.onclick = () => { if (!busy) openCell(review.table.sheetId, row.row, review.table.columns.amount); }; panel.append(open);
    if (row.suggested.trim()) { const use = node('button', 'Use suggested account'); use.type = 'button'; use.onclick = () => run(async () => { try { await editCell(row.row, 'account', row.suggested); } finally { await refresh(); } }); panel.append(use); }
    if (row.note) panel.append(node('h3', 'Review note'), node('p', row.note));
    updateState();
  }
  async function refresh() {
    if (!active || !api) return; const version = ++serial, review = getReview();
    if (!review) { api.setGridOption('rowData', []); return; }
    const next = await projectReview(getSnapshot(), review); if (version !== serial || !active) return;
    const selected = new Set(api.getSelectedRows().map(row => row.id));
    api.setGridOption('rowData', next.rows); api.forEachNode(n => n.setSelected(selected.has(n.data.id))); api.onFilterChanged(); selections();
    $('metrics').replaceChildren(...[[next.pending, 'Need review'], [next.reviewed, 'Reviewed'], [next.missingAmounts, 'Missing amounts'], [next.possibleDuplicates, 'Possible duplicates']].map(([count, label]) => { const item = node('span'); const caption=node('small',label);caption.className='fr-metric-label';item.append(node('strong', String(count)),caption); return item; }));
    const accountTable=node('table'),head=node('thead'),heading=node('tr'),body=node('tbody');for(const label of ['Account','Rows','Reviewed','Known amount','Missing amounts']){const th=node('th',label);th.scope='col';heading.append(th);}head.append(heading);
    for(const account of next.accounts){const tr=node('tr'),raw=BigInt(account.knownCents),absolute=raw<0n?-raw:raw,amount=(raw<0n?'-':'')+(absolute/100n).toLocaleString('en-US')+'.'+String(absolute%100n).padStart(2,'0');for(const value of [account.account||'Unassigned',account.rows,account.reviewed,amount,account.missingAmounts])tr.append(node('td',String(value)));body.append(tr);}
    accountTable.append(head,body);const note=node('p','Amounts include all mapped rows, including those awaiting review. Missing amounts are excluded; currency is not inferred.');$('accounts').replaceChildren(accountTable,note);
    if (selectedRow !== null) await inspect(next.rows.find(row => row.row === selectedRow));
  }
  async function run(fn) {
    if (busy) return; state(true); try { await runTask(fn); notice(''); } catch (error) { notice(error.message); } finally { state(false); }
  }
  function mapping() {
    const form = $('mapping'), snapshot = getSnapshot(), existing = getReview()?.table;
    form.hidden = false; form.replaceChildren();
    const label = (text, input) => { const el = node('label', text); el.append(input); form.append(el); };
    const sheet = node('select'); sheet.name = 'sheet';
    for (const id of snapshot.sheetOrder) { const option = node('option', snapshot.sheets[id].name); option.value = id; sheet.append(option); } sheet.value = existing?.sheetId ?? snapshot.sheetOrder[0]; label('Worksheet', sheet);
    const header = node('input'); header.type = 'number'; header.min = '1'; header.max = '4999'; header.value = String((existing?.headerRow ?? 0) + 1); label('Header row', header);
    const selectors = node('div'); selectors.className = 'fr-mapping'; form.append(selectors);
    function columns() {
      selectors.replaceChildren(); const selected = snapshot.sheets[sheet.value], row = Number(header.value) - 1;
      for (const role of Object.keys(reviewRoles)) {
        const wrap = node('label', reviewRoles[role]), select = node('select'); select.dataset.role = role;
        const none = node('option', 'Choose column'); none.value = ''; select.append(none);
        for (let col = 0; col < selected.columnCount; col++) { const option = node('option', `${col + 1}: ${selected.cellData?.[row]?.[col]?.v ?? 'Untitled'}`); option.value = String(col); select.append(option); }
        const value = sheet.value === existing?.sheetId && row === existing?.headerRow ? existing.columns[role] : undefined;
        select.value = value === undefined ? '' : String(value); wrap.append(select); selectors.append(wrap);
      }
    }
    sheet.onchange = columns; header.onchange = columns; columns();
    const apply = node('button', 'Use columns'); apply.type = 'submit'; form.append(apply);
    form.onsubmit = event => { event.preventDefault(); run(async () => {
      const table = { sheetId: sheet.value, headerRow: Number(header.value) - 1, columns: Object.fromEntries([...selectors.querySelectorAll('select')].filter(s => s.value !== '').map(s => [s.dataset.role, Number(s.value)])) };
      const next = createReview(table, getSnapshot()), previous = getReview()?.table;
      const same = previous?.sheetId === table.sheetId && previous?.headerRow === table.headerRow && Object.keys(reviewRoles).every(role => previous.columns[role] === table.columns[role]);
      if (!same) {
        if (getReview()?.decisions.length && !confirm('Changing review columns clears the current row decisions. Continue?')) return;
        setReview(next); changed();
      }
      form.hidden = true; await refresh();
    }); };
  }
  $('map').onclick = mapping;
  $('search').oninput = () => api?.setGridOption('quickFilterText', $('search').value);
  $('filter').onchange = () => api?.onFilterChanged();
  $('accept').onclick = () => run(async () => {
    const selected = api.getSelectedRows().map(({ row, fingerprint }) => ({ row, fingerprint }));
    await recalculate(); setReview(await markRowsReviewed(getSnapshot(), getReview(), selected, $('note').value)); changed(); await refresh();
  });
  $('reopen').onclick = () => run(async () => { const rows = api.getSelectedRows().map(row => row.row); if (!rows.length) throw Error('Select rows to reopen.'); setReview(reopenReviewRows(getReview(), rows)); changed(); await refresh(); });
  return {
    async activate() {
      active = true; state(true);
      try {
        await runTask(async () => {
        if (!api) {
          const lib = await loadGrid(); if (!active) return;
          const render = params => {const n=node('span', params.value ?? '');if(params.colDef.field==='status'){n.className='fr-row-status';n.dataset.reviewed=String(params.data?.reviewed===true);}return n;};
          const setter = role => params => { if (busy) return false; run(async () => {
            const value = role === 'amount' ? String(params.newValue).trim() : String(params.newValue ?? '');
            if (role === 'amount' && !/^-?\d+(?:\.\d{1,2})?$/.test(value)) throw Error('Enter an amount with at most two decimal places.');
            if (role === 'amount' && !Number.isSafeInteger(Math.round(Number(value) * 100))) throw Error('Amount exceeds the supported range.');
            try { await editCell(params.data.row, role, role === 'amount' ? Number(value) : value); } finally { await refresh(); }
          }); return false; };
          api = lib.createGrid($('grid'), { theme: lib.themeQuartz.withParams({ accentColor: '#416b36', fontFamily: 'DM Sans, system-ui', rowHeight: 50, headerHeight: 46, backgroundColor: '#fffef8', headerBackgroundColor: '#eaf1df', foregroundColor: '#263e2b', borderColor: '#cedbbe', rowHoverColor: '#f0f5e8', selectedRowBackgroundColor: '#e4efd4', borderRadius: 12 }), rowData: [], getRowId: p => p.data.id,
            defaultColDef: { sortable: true, resizable: true, filter: true, minWidth: 110, cellRenderer: render }, animateRows: false,
            rowSelection: { mode: 'multiRow', selectAll: 'filtered' },
            columnDefs: [{ field: 'date', width: 130 }, { field: 'description', width: 260 }, { field: 'amount', width: 140, editable: () => !busy, valueSetter: setter('amount') },
              { field: 'account', width: 190, editable: () => !busy, valueSetter: setter('account') }, { field: 'suggested', headerName: 'Suggested account', width: 180 }, { field: 'status', width: 260 }],
            onSelectionChanged: selections, onCellClicked: params => inspect(params.data).catch(error => notice(error.message)),
            isExternalFilterPresent: () => $('filter').value !== 'all', doesExternalFilterPass: ({ data }) => $('filter').value === 'reviewed' ? data.reviewed : $('filter').value === 'duplicates' ? data.issues.includes('Possible duplicate') : !data.reviewed
          });
        }
        if (!getReview()) { const detected = detectReview(getSnapshot()); if (detected) { setReview(detected); changed(); } else { mapping(); notice('Choose the transaction sheet and columns.'); return; } }
        try { await recalculate(); } catch (error) { notice(error.message); }
        await refresh();
        });
      } catch (error) { notice(error.message); }
      finally { state(false); }
    },
    refresh: () => refresh().catch(error => notice(error.message)),
    deactivate() { active = false; serial++; inspectorSerial++; },
    dispose() { active = false; serial++; inspectorSerial++; api?.destroy(); host.replaceChildren(); },
    setBusy(value) { externalBusy = value; updateState(); }
  };
}
