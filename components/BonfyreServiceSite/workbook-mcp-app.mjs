import { App } from '@modelcontextprotocol/ext-apps';
import { createWorkbookMcpController, workbookCorrection } from './workbook-mcp-controller.mjs';

const app = new App({ name: 'Aurekai financial review', version: '1.0.0' });
const $ = name => document.querySelector(`[data-${name}]`);
const node = (tag, text) => { const el = document.createElement(tag); if (text !== undefined) el.textContent = String(text); return el; };
const columnName = column => { let label = ''; for (let n = column + 1; n; n = Math.floor((n - 1) / 26)) label = String.fromCharCode(65 + (n - 1) % 26) + label; return label; };
const address = cell => `${columnName(cell.column)}${cell.row + 1}`;
const value = cell => cell ? `${cell.value ?? 'Empty'}${cell.formula ? ` · ${cell.formula}` : ''}${cell.truncated ? ' (truncated)' : ''}` : 'Unavailable';
let readKey, previewKey, selection = [];
const grid = globalThis.agGrid.createGrid($('grid'), {
  theme: globalThis.agGrid.themeQuartz.withParams({ fontFamily: 'system-ui', fontSize: 13, spacing: 7, accentColor: '#22634a' }),
  columnDefs: [
    { field: 'address', headerName: 'Row', width: 85 },
    { field: 'description', headerName: 'Transaction', flex: 2, minWidth: 180 },
    { field: 'amount', headerName: 'Amount', width: 125, type: 'rightAligned' },
    { field: 'account', headerName: 'Account', flex: 1, minWidth: 130 },
    { field: 'status', headerName: 'Review', width: 160 },
  ], defaultColDef: { sortable: true, resizable: true }, rowData: [],
  rowSelection: { mode: 'multiRow' }, getRowId: params => String(params.data.row),
  onSelectionChanged() { selection = grid.getSelectedRows(); $('selected').textContent = `${selection.length} selected`; syncControls(); },
  onRowClicked(event) { if (event.data) inspect(event.data); }
});
const controller = createWorkbookMcpController({ call: params => app.callServerTool(params), changed: render });
function syncControls() {
  const s = controller.state, locked = s.busy || !!s.preview, blocked = !!s.uncertain || s.externalChange;
  for (const el of document.querySelectorAll('button,input,select')) el.disabled = locked;
  $('reload').disabled = s.busy || !s.read;
  $('save').disabled = s.busy || !s.preview || blocked;
  $('discard').disabled = s.busy;
  for (const key of ['review', 'reopen']) $(key).disabled = locked || blocked || !selection.length;
  $('previous').disabled = locked || blocked || !s.read?.offset;
  $('next').disabled = locked || blocked || !s.read?.more;
  $('editor').disabled = s.busy || blocked || !s.read?.release;
  if (blocked) for (const key of ['mapping', 'edit-preview']) { const el = $(key); if (el) el.disabled = true; }
}
function render(s) {
  document.body.setAttribute('aria-busy', String(s.busy));
  $('status').textContent = s.error || (s.busy ? 'Working…' : '');
  $('recovery').hidden = !s.uncertain && !s.externalChange;
  $('recovery').textContent = s.uncertain ? `${s.uncertain.saved ? 'Saved. Reload to see the current workbook.' : 'Save outcome needs checking. Reload before making another change.'} Request: ${s.uncertain.request_id}` : 'Another result arrived or this revision changed. Reload before continuing.';
  if (s.read) {
    const r = s.read;
    $('title').textContent = r.title || 'Financial review';
    $('revision').textContent = `Revision ${r.revision}`;
    $('metrics').replaceChildren(...[[r.summary?.pending, 'Need review'], [r.summary?.reviewed, 'Reviewed'], [r.summary?.possible_duplicates, 'Possible duplicates'], [r.summary?.missing_amounts, 'Missing amounts']].map(([count, title]) => { const el = node('div'); el.append(node('strong', count ?? '—'), node('span', title)); return el; }));
    $('calculation').textContent = r.calculation?.error || (r.calculation?.errors?.length ? `${r.calculation.errors.length} calculation errors` : '');
    $('filter').value = s.status;
    $('page').textContent = `${r.matched ? r.offset + 1 : 0}–${r.offset + r.rows.length} of ${r.matched} transactions`;
    const key = `${r.sha256}:${r.offset}:${s.status}`;
    if (key !== readKey) {
      readKey = key; grid.setGridOption('rowData', r.rows); selection = []; $('selected').textContent = '0 selected';
      $('inspector').hidden = true; $('mapping-form').hidden = true;
      if (r.mapping_required) mapping();
    }
  }
  $('preview').hidden = !s.preview;
  if (s.preview && s.preview.preview_sha256 !== previewKey) {
    previewKey = s.preview.preview_sha256; const p = s.preview;
    $('preview-summary').textContent = `${p.changed_cell_count} changed cells · ${p.reviewed_after} reviewed · ${p.pending_after} remaining${p.decisions_cleared ? ` · ${p.decisions_cleared} decisions removed` : ''}`;
    const body = $('changes'); body.replaceChildren();
    for (const cell of p.changed_cells) { const tr = node('tr'); tr.append(node('th', `${cell.sheetId}!${address(cell)}`), node('td', value(cell.before)), node('td', value(cell.after))); body.append(tr); }
    for (const decision of p.decisions) { const tr = node('tr'); tr.append(node('th', `Row ${decision.row + 1}`), node('td', decision.reviewed ? 'Mark reviewed' : 'Reopen'), node('td', decision.note || '')); body.append(tr); }
    if (p.changed_cells_truncated) { const tr = node('tr'), td = node('td', 'First 100 changed cells shown. Open the workbook for the complete review.'); td.colSpan = 3; tr.append(td); body.append(tr); }
  }
  syncControls();
}
async function action(fn) { try { await fn(); } catch (error) { $('status').textContent = error.message; } }
async function inspect(row) {
  if (controller.state.busy || controller.state.preview || controller.state.uncertain) return;
  if (controller.state.dirty) { $('status').textContent = 'Preview your current changes or reload before selecting another transaction.'; return; }
  await action(async () => {
    const r = controller.state.read, columns = r.table.columns, first = Math.min(columns.description, columns.amount, columns.account), last = Math.max(columns.description, columns.amount, columns.account);
    // A bounded range is an address comparison, never inferred source lineage.
    const result = await controller.inspect(row.row, first, Math.min(12, last - first + 1));
    const panel = $('inspector'); panel.replaceChildren(node('h2', row.description || 'Transaction'), node('p', `${row.address} · ${row.status}`)); panel.hidden = false;
    const table = node('table'), head = node('tr'); ['Cell', 'Imported', 'Current'].forEach(text => head.append(node('th', text))); table.append(head);
    for (const cell of result.cells ?? []) { const tr = node('tr'); tr.append(node('th', address(cell)), node('td', value(cell.imported)), node('td', value(cell.current))); table.append(tr); } panel.append(table, node('p', result.source_comparison));
    if (last - first >= 12) panel.append(node('p', 'First 12 columns shown. Open the workbook to inspect the remaining columns.'));
    if (row.source?.cell || row.source?.sheet) panel.append(node('p', `Reported source: ${row.source.sheet || ''} ${row.source.cell || ''} ${row.source.sha256 || ''}`));
    if (row.truncated_fields?.length) panel.append(node('p', `Shortened fields: ${row.truncated_fields.join(', ')}. Open the workbook for full values.`));
    const form = node('form'), amount = node('input'), account = node('input');
    amount.value = String(row.amount ?? ''); amount.inputMode = 'decimal'; account.value = row.account; account.maxLength = 1000;
    for (const [title, input] of [['Amount', amount], ['Account', account]]) { const label = node('label', title); label.append(input); form.append(label); }
    const submit = node('button', 'Preview correction'); submit.type = 'submit'; submit.dataset.editPreview = ''; form.append(submit);
    form.oninput = () => controller.draftChanged();
    form.onsubmit = event => { event.preventDefault(); action(() => controller.preview(workbookCorrection(row, { amount: amount.value, account: account.value }))); };
    panel.append(form); syncControls();
  });
}
function mapping() {
  const r = controller.state.read; if (!r) return;
  const form = $('mapping-form'); form.hidden = false; form.replaceChildren();
  const sheet = node('select'); for (const item of r.sheets) { const option = node('option', item.name); option.value = item.id; sheet.append(option); }
  sheet.value = r.table?.sheetId || r.sheets[0].id;
  const header = node('input'); header.type = 'number'; header.min = '1'; header.max = '4999'; header.value = String((r.table?.headerRow ?? 0) + 1);
  const add = (title, input, target = form) => { const label = node('label', title); label.append(input); target.append(label); };
  add('Worksheet', sheet); add('Header row', header);
  const fields = node('div'); fields.className = 'mapping-fields'; form.append(fields);
  function choices() {
    fields.replaceChildren(); const count = r.sheets.find(item => item.id === sheet.value).columns;
    for (const role of ['description', 'amount', 'account', 'date', 'suggested', 'review', 'sourceSheet', 'sourceRow', 'sourceCell', 'sourceHash']) {
      const select = node('select'); select.name = role; select.required = ['description', 'amount', 'account'].includes(role);
      const none = node('option', 'Choose column'); none.value = ''; select.append(none);
      for (let i = 0; i < count; i++) { const option = node('option', columnName(i)); option.value = String(i); select.append(option); }
      select.value = sheet.value === r.table?.sheetId ? String(r.table.columns[role] ?? '') : ''; add(role, select, fields);
    }
  }
  sheet.onchange = () => { choices(); controller.draftChanged(); }; choices();
  form.oninput = () => controller.draftChanged();
  const submit = node('button', 'Preview column mapping'); submit.type = 'submit'; form.append(submit);
  form.onsubmit = event => { event.preventDefault(); action(() => controller.preview({ table: { sheetId: sheet.value, headerRow: Number(header.value) - 1, columns: Object.fromEntries([...fields.querySelectorAll('select')].filter(input => input.value !== '').map(input => [input.name, Number(input.value)])) } })); };
}
$('reload').onclick = () => action(() => controller.load());
$('filter').onchange = event => action(() => controller.load({ status: event.target.value }));
$('previous').onclick = () => action(() => controller.load({ offset: Math.max(0, controller.state.read.offset - 50) }));
$('next').onclick = () => action(() => controller.load({ offset: controller.state.read.next_offset }));
$('review').onclick = () => action(() => controller.preview({ reviewed: selection.map(row => ({ row: row.row, fingerprint: row.fingerprint })), note: $('note').value }));
$('reopen').onclick = () => action(() => controller.preview({ reopen: selection.map(row => row.row) }));
$('save').onclick = () => action(() => controller.save());
$('discard').onclick = () => controller.discard();
$('mapping').onclick = mapping;
$('note').oninput = () => controller.draftChanged();
$('editor').onclick = () => action(async () => { const url = await controller.editor(); await app.openLink({ url }); });
app.ontoolresult = result => controller.receive(result);
app.ontoolcancelled = () => { $('status').textContent = 'The host cancelled this operation. Reload to check the saved workbook.'; };
render(controller.state);
app.connect().catch(error => { $('status').textContent = `Could not connect to the host: ${error.message}`; });
