// The schema workspace. A SQLite file opens in this browser (nothing is
// uploaded); its tables are drawn with their references; changes are staged,
// checked against the data, turned into a migration and tried on a copy.
// The original file is never changed: the result is a download.
import * as lab from './schema-lab.mjs';
import { attachInstrumentStore, openInstrumentScratch } from './instrument-store.mjs';

const root = document.querySelector('[data-schema]');
const $ = name => root.querySelector(`[data-ui="${name}"]`);
const here = new URL('.', import.meta.url), NS = 'http://www.w3.org/2000/svg', KEY = 'aurekai-schema-lab-v1';
const esc = t => String(t ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const say = (text, bad) => { const m = $('message'); m.textContent = text; m.dataset.tone = bad ? 'bad' : ''; };
const store = await openInstrumentScratch(KEY, {});

let SQL, S = { name: '', bytes: null, db: null, before: null, changes: [], after: null, layout: {}, selected: null, trialResult: null, copy: null, rowsOf: 'before' };
const fingerprint = () => `${S.name}:${S.bytes?.length}`;

async function engine() {
  if (SQL) return SQL;
  await new Promise((ok, bad) => { const s = document.createElement('script'); s.src = new URL('vendor/sql.js-1.12.0/sql-wasm.js', here); s.onload = ok; s.onerror = () => bad(Error('SQLite could not load')); document.head.append(s); });
  SQL = await window.initSqlJs({ locateFile: f => new URL('vendor/sql.js-1.12.0/' + f, here).href });
  return SQL;
}
async function openBytes(name, bytes) {
  await engine();
  const db = new SQL.Database(bytes), h = lab.sqlJsHandle(db);
  try { h.q('SELECT count(*) FROM sqlite_master'); } catch { db.close(); throw Error(`${name} is not a SQLite database`); }
  h.run('PRAGMA foreign_keys = ON');
  S.db?.close?.(); S.copy?.close?.();
  S = { ...S, name, bytes, db, handle: h, before: lab.inspect(h), changes: [], trialResult: null, copy: null, rowsOf: 'before' };
  const kept = store.get()[fingerprint()]; S.layout = kept?.layout ?? {}; S.changes = kept?.changes ?? [];
  autoLayout(); S.selected = S.before.tables[0]?.name ?? null; recompute();
  $('file-name').textContent = `${name} · ${(bytes.length / 1024).toFixed(0)} KB · ${S.before.tables.length} tables`;
  if (kept?.changes?.length) say(`Opened ${name}. ${kept.changes.length} staged change${kept.changes.length > 1 ? 's' : ''} from before came back with it.`);
}
function persist() { const all = store.get(); all[fingerprint()] = { layout: S.layout, changes: S.changes }; store.set(all); }
function autoLayout() {
  const tables = (S.after ?? S.before).tables, cols = Math.max(1, Math.ceil(Math.sqrt(tables.length)));
  tables.forEach((t, i) => { S.layout[t.name] ??= { x: 30 + (i % cols) * 280, y: 30 + Math.floor(i / cols) * 230 }; });
}

// ------------------------------------------------------------ staging

function recompute() {
  try { S.after = lab.stage(S.before, S.changes); S.stageError = null; }
  catch (e) { S.stageError = e.message; S.after = lab.stage(S.before, S.changes.slice(0, -1)); S.changes = S.changes.slice(0, -1); say(e.message, true); }
  for (const t of S.after.tables) if (t.renamedFrom && S.layout[t.renamedFrom] && !S.layout[t.name]) S.layout[t.name] = S.layout[t.renamedFrom];
  autoLayout(); S.trialResult = null; persist(); render();
}
function add(change) { S.changes.push(change); recompute(); if (!S.stageError) say(describe(change) + '. Staged; nothing has changed yet.'); }
function describe(c) {
  return { addTable: () => `Add table ${c.name}`, renameTable: () => `Rename ${c.table} to ${c.to}`, dropTable: () => `Remove table ${c.table}`, addColumn: () => `Add ${c.table}.${c.name} (${c.type}${c.notnull ? ', required' : ''})`,
    dropColumn: () => `Remove ${c.table}.${c.column}`, renameColumn: () => `Rename ${c.table}.${c.column} to ${c.to}`, changeType: () => `Make ${c.table}.${c.column} ${c.type}`,
    setRequired: () => c.required ? `Require ${c.table}.${c.column}${c.fill != null ? `, filling empty ones with "${c.fill}"` : ''}` : `Allow ${c.table}.${c.column} to be empty`,
    addForeignKey: () => `${c.table}.${c.column} refers to ${c.refTable}.${c.refColumn}`, dropForeignKey: () => `Stop ${c.table}.${c.column} referring to ${c.refTable}`,
    addIndex: () => `${c.unique ? 'Unique index' : 'Index'} on ${c.table} (${c.columns.join(', ')})`, dropIndex: () => `Remove index ${c.name}` }[c.kind]?.() ?? c.kind;
}

// ------------------------------------------------------------ drawing

const W = 220, ROW = 20, HEAD = 34;
function draw() {
  const svg = $('diagram'), m = S.after; svg.replaceChildren();
  const el = (tag, attrs, parent = svg) => { const n = document.createElementNS(NS, tag); for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v); parent.append(n); return n; };
  let maxX = 600, maxY = 400;
  const at = (t, col) => { const p = S.layout[t.name], i = t.columns.findIndex(c => c.name === col); return { x: p.x, y: p.y + HEAD + (i < 0 ? 0 : i) * ROW + ROW / 2 }; };
  for (const t of m.tables) for (const f of t.fks) { const r = m.tables.find(x => x.name === f.table); if (!r) continue;
    const a = at(t, f.from[0]), b = at(r, f.to[0] ?? r.columns.find(c => c.pk)?.name), ax = a.x + (b.x > a.x ? W : 0), bx = b.x + (b.x > a.x ? 0 : W), mid = (ax + bx) / 2;
    el('path', { d: `M${ax} ${a.y}C${mid} ${a.y} ${mid} ${b.y} ${bx} ${b.y}`, class: 'sl-ref' + (f.added ? ' is-new' : '') }); el('circle', { cx: bx, cy: b.y, r: 4, class: 'sl-ref-end' }); }
  for (const t of m.tables) {
    const p = S.layout[t.name], g = el('g', { class: 'sl-table' + (S.selected === t.name ? ' is-selected' : '') + (t.rebuild || t.created ? ' is-changed' : ''), transform: `translate(${p.x} ${p.y})`, 'data-table': t.name, tabindex: 0, role: 'button', 'aria-label': `Table ${t.name}, ${t.columns.length} columns. Drag to move.` });
    el('rect', { x: 0, y: 0, width: W, height: 26, rx: 13, class: 'sl-head' }, g);
    const title = el('text', { x: 14, y: 18, class: 'sl-title' }, g); title.textContent = t.name.length > 22 ? t.name.slice(0, 21) + '…' : t.name;
    const rows = el('text', { x: W - 12, y: 18, class: 'sl-count', 'text-anchor': 'end' }, g); rows.textContent = t.created ? 'new' : (t.rows ?? 0).toLocaleString();
    t.columns.forEach((c, i) => { const y = HEAD + i * ROW + 14;
      el('line', { x1: 0, x2: W, y1: HEAD + i * ROW - 3, y2: HEAD + i * ROW - 3, class: 'sl-rule' }, g);
      const n = el('text', { x: 14, y, class: 'sl-col' + (c.added || c.renamedFrom || c.retyped ? ' is-new' : '') }, g); n.textContent = (c.pk ? '⚿ ' : '') + c.name + (c.notnull && !c.pk ? ' *' : '');
      const ty = el('text', { x: W - 12, y, class: 'sl-type', 'text-anchor': 'end' }, g); ty.textContent = c.type || 'any'; });
    maxX = Math.max(maxX, p.x + W + 30); maxY = Math.max(maxY, p.y + HEAD + t.columns.length * ROW + 30);
  }
  svg.setAttribute('viewBox', `0 0 ${maxX} ${maxY}`); svg.style.aspectRatio = `${maxX} / ${maxY}`;
}
let drag = null;
const pt = e => { const r = $('diagram').getBoundingClientRect(), vb = $('diagram').viewBox.baseVal; return { x: (e.clientX - r.left) / r.width * vb.width, y: (e.clientY - r.top) / r.height * vb.height }; };
$('diagram').addEventListener('pointerdown', e => { const g = e.target.closest('[data-table]'); if (!g) return; const p = pt(e), at = S.layout[g.dataset.table]; drag = { name: g.dataset.table, dx: p.x - at.x, dy: p.y - at.y, moved: false }; $('diagram').setPointerCapture(e.pointerId); select(g.dataset.table); });
$('diagram').addEventListener('pointermove', e => { if (!drag) return; const p = pt(e); drag.moved = true; S.layout[drag.name] = { x: Math.max(0, p.x - drag.dx), y: Math.max(0, p.y - drag.dy) }; draw(); });
$('diagram').addEventListener('pointerup', () => { if (drag?.moved) persist(); drag = null; });
$('diagram').addEventListener('keydown', e => { const g = e.target.closest?.('[data-table]'); if (g && (e.key === 'Enter' || e.key === ' ')) { select(g.dataset.table); e.preventDefault(); } });
function select(name) { S.selected = name; render(); }

// ------------------------------------------------------------ the selected table

function inspector() {
  const t = S.after.tables.find(x => x.name === S.selected), box = $('inspector');
  if (!t) { box.innerHTML = '<p class="sl-hint">Select a table in the drawing.</p>'; return; }
  const others = S.after.tables.filter(x => x !== t), opt = (v, l = v, sel) => `<option value="${esc(v)}"${sel ? ' selected' : ''}>${esc(l)}</option>`;
  box.innerHTML = `<h2>${esc(t.name)}</h2><p class="sl-hint">${t.created ? 'New table' : `${(t.rows ?? 0).toLocaleString()} rows`}${t.fks.length ? ` · refers to ${[...new Set(t.fks.map(f => f.table))].map(esc).join(', ')}` : ''}${t.checks.length ? ` · ${t.checks.length} check${t.checks.length > 1 ? 's' : ''} kept on rebuild` : ''}</p>
  <table class="sl-cols"><thead><tr><th>Column</th><th>Type</th><th>Required</th><th></th></tr></thead><tbody>${t.columns.map(c => `<tr>
    <td><input data-rename="${esc(c.name)}" value="${esc(c.name)}" aria-label="Name of ${esc(c.name)}" ${c.pk ? 'title="Key column"' : ''}></td>
    <td><select data-type="${esc(c.name)}" aria-label="Type of ${esc(c.name)}">${(c.type && !lab.TYPES.includes(c.type) ? [c.type] : []).concat(lab.TYPES).map(x => opt(x, x, x === c.type)).join('')}</select></td>
    <td><input type="checkbox" data-required="${esc(c.name)}" ${c.notnull ? 'checked' : ''} ${c.pk ? 'disabled' : ''} aria-label="${esc(c.name)} required"></td>
    <td>${c.pk ? '<span class="sl-hint">key</span>' : `<button type="button" class="sl-quiet" data-drop="${esc(c.name)}" aria-label="Remove ${esc(c.name)}">✕</button>`}</td></tr>`).join('')}</tbody></table>
  <form data-ui="add-column" class="sl-row"><input name="name" placeholder="new column" required maxlength="60"><select name="type">${lab.TYPES.map(x => opt(x)).join('')}</select><input name="default" placeholder="value for existing rows"><label class="sl-check"><input type="checkbox" name="notnull"> required</label><button type="submit">Add column</button></form>
  ${others.length ? `<form data-ui="add-ref" class="sl-row"><select name="column">${t.columns.map(c => opt(c.name)).join('')}</select><span>refers to</span><select name="target">${others.flatMap(o => o.columns.filter(c => (c.pk && o.columns.filter(x => x.pk).length === 1) || o.indexes.some(i => i.unique && i.columns.length === 1 && i.columns[0] === c.name)).map(c => opt(`${o.name}.${c.name}`))).join('')}</select><select name="onDelete">${['NO ACTION', 'CASCADE', 'SET NULL', 'RESTRICT'].map(x => opt(x, 'on delete: ' + x.toLowerCase())).join('')}</select><button type="submit">Add reference</button></form>` : ''}
  <form data-ui="add-index" class="sl-row"><select name="columns" multiple size="${Math.min(4, t.columns.length)}" aria-label="Columns to index">${t.columns.map(c => opt(c.name)).join('')}</select><label class="sl-check"><input type="checkbox" name="unique"> unique</label><button type="submit">Add index</button></form>
  ${t.indexes.length ? `<p class="sl-hint">Indexes: ${t.indexes.map(i => `${esc(i.columns.join(' + '))}${i.unique ? ' (unique)' : ''}${i.origin === 'c' ? ` <button type="button" class="sl-quiet" data-drop-index="${esc(i.name)}">✕</button>` : ''}`).join(' · ')}</p>` : ''}
  ${t.fks.length ? `<p class="sl-hint">References: ${t.fks.map(f => `${esc(f.from.join())} → ${esc(f.table)} <button type="button" class="sl-quiet" data-drop-ref="${esc(f.from.join())}|${esc(f.table)}">✕</button>`).join(' · ')}</p>` : ''}
  <form data-ui="rename-table" class="sl-row"><input name="to" placeholder="rename table to" maxlength="60" required><button type="submit">Rename table</button><button type="button" data-ui="drop-table" class="sl-quiet">Remove table</button></form>`;
}
$('inspector').addEventListener('change', e => {
  const t = S.selected, x = e.target;
  if (x.dataset.rename != null && x.value.trim() && x.value.trim() !== x.dataset.rename) add({ kind: 'renameColumn', table: t, column: x.dataset.rename, to: x.value.trim() });
  if (x.dataset.type != null) add({ kind: 'changeType', table: t, column: x.dataset.type, type: x.value });
  // A value for empty rows is asked for on the staged change itself, where the count of empty rows is shown.
  if (x.dataset.required != null) add({ kind: 'setRequired', table: t, column: x.dataset.required, required: x.checked });
});
$('inspector').addEventListener('click', e => {
  const x = e.target, t = S.selected;
  if (x.dataset.drop) add({ kind: 'dropColumn', table: t, column: x.dataset.drop });
  if (x.dataset.dropIndex) add({ kind: 'dropIndex', table: t, name: x.dataset.dropIndex });
  if (x.dataset.dropRef) { const [column, refTable] = x.dataset.dropRef.split('|'); add({ kind: 'dropForeignKey', table: t, column, refTable }); }
  if (x.dataset.ui === 'drop-table') add({ kind: 'dropTable', table: t });
});
$('inspector').addEventListener('submit', e => {
  e.preventDefault(); const f = new FormData(e.target), t = S.selected, form = e.target.dataset.ui;
  if (form === 'add-column') add({ kind: 'addColumn', table: t, name: f.get('name').trim(), type: f.get('type'), notnull: !!f.get('notnull'), default: f.get('default') === '' ? null : f.get('default') });
  if (form === 'add-ref') { const [refTable, refColumn] = f.get('target').split('.'); add({ kind: 'addForeignKey', table: t, column: f.get('column'), refTable, refColumn, onDelete: f.get('onDelete') }); }
  if (form === 'add-index') { const columns = f.getAll('columns'); if (!columns.length) return say('Choose the columns to index.'); add({ kind: 'addIndex', table: t, columns, unique: !!f.get('unique') }); }
  if (form === 'rename-table') { const to = f.get('to').trim(); add({ kind: 'renameTable', table: t, to }); if (!S.stageError) S.selected = to, render(); }
});
$('new-table').addEventListener('submit', e => { e.preventDefault(); const name = new FormData(e.target).get('name').trim(); if (!name) return; add({ kind: 'addTable', name, columns: [{ name: 'id', type: 'INTEGER', pk: 1 }] }); if (!S.stageError) { S.selected = name; render(); } e.target.reset(); });

// ------------------------------------------------------------ staged list, checks, migration, trial

function staged() {
  const issues = lab.precheck(S.handle, S.before, S.changes);
  $('changes').innerHTML = S.changes.length ? S.changes.map((c, i) => { const mine = issues.filter(x => x.change === c);
    return `<li><span>${esc(describe(c))}</span>${c.kind === 'setRequired' && c.required ? `<input data-fill="${i}" value="${esc(c.fill ?? '')}" placeholder="value for empty rows" aria-label="Value for empty rows">` : ''}<button type="button" class="sl-quiet" data-unstage="${i}" aria-label="Remove this change">✕</button>
    ${mine.map(x => `<p class="sl-issue${x.blocking ? ' is-blocking' : ''}">${esc(x.words)}${x.examples?.length ? ` Examples: ${x.examples.map(v => `“${esc(v)}”`).join(', ')}.` : ''}</p>`).join('')}</li>`; }).join('')
    : '<li class="sl-hint">Change a name, type or requirement on the right, or add a column, reference or index. Changes wait here until you try them.</li>';
  S.blocking = issues.filter(x => x.blocking).length;
  $('sql').textContent = S.changes.length ? lab.migration(S.before, S.after).join('\n\n') : '';
  $('try').disabled = !S.changes.length;
  $('try-note').textContent = S.blocking ? `${S.blocking} change${S.blocking > 1 ? 's' : ''} will stop or break references as things stand. Trying shows exactly where.` : '';
}
$('changes').addEventListener('click', e => { const i = e.target.dataset.unstage; if (i == null) return; const removed = S.changes.splice(+i, 1)[0]; recompute(); say(`Removed: ${describe(removed)}.`); });
$('changes').addEventListener('change', e => { const i = e.target.dataset.fill; if (i == null) return; const v = e.target.value; S.changes[+i] = { ...S.changes[+i], ...(v === '' ? { fill: undefined } : { fill: v }) }; recompute(); });
$('try').addEventListener('click', async () => {
  S.copy?.close?.(); S.copy = new SQL.Database(S.db.export()); const h = lab.sqlJsHandle(S.copy); h.run('PRAGMA foreign_keys = ON');
  const r = lab.trial(h, S.before, S.after, lab.migration(S.before, S.after)); S.trialResult = r; S.copyHandle = h; S.rowsOf = r.ok ? 'after' : 'before';
  renderTrial(); renderRows();
  say(!r.ok ? 'The migration stopped on the copy; nothing was kept from it.' : r.lost.length || r.brokenCount ? 'It ran on the copy, with problems listed below.' : 'It ran on the copy: every row kept, every reference holds.', !r.ok);
});
function renderTrial() {
  const r = S.trialResult, box = $('trial');
  if (!r) { box.innerHTML = ''; $('download-db').disabled = true; return; }
  if (!r.ok) { box.innerHTML = `<p class="sl-issue is-blocking">${esc(r.error)}</p><pre>${esc(r.step)}</pre>`; $('download-db').disabled = true; return; }
  box.innerHTML = `<table class="sl-cols"><thead><tr><th>Table</th><th>Rows before</th><th>After</th></tr></thead><tbody>${r.tables.map(t => `<tr class="${t.after < t.before ? 'is-lost' : ''}"><td>${esc(t.name)}</td><td>${t.before.toLocaleString()}</td><td>${t.after.toLocaleString()}</td></tr>`).join('')}</tbody></table>
  <p class="${r.brokenCount ? 'sl-issue is-blocking' : 'sl-ok'}">${r.brokenCount ? `${r.brokenCount.toLocaleString()} rows refer to something that is not there: ${r.broken.slice(0, 5).map(b => `${esc(b.table)} row ${b.rowid} → ${esc(b.parent)}`).join(', ')}${r.brokenCount > 5 ? '…' : ''}` : 'Every reference holds.'} ${r.integrity.join(' ') === 'ok' ? 'The file passes SQLite\'s integrity check.' : esc(r.integrity.join('; '))}</p>`;
  $('download-db').disabled = false;
}

function renderRows() {
  const t = (S.rowsOf === 'after' && S.trialResult?.ok ? S.trialResult.model : S.before).tables.find(x => x.name === (S.rowsOf === 'after' ? S.selected : (S.after.tables.find(y => y.name === S.selected)?.renamedFrom ?? S.selected)));
  const h = S.rowsOf === 'after' && S.trialResult?.ok ? S.copyHandle : S.handle;
  $('rows-which').innerHTML = `<button type="button" data-rows="before" aria-pressed="${S.rowsOf === 'before'}">As it is</button><button type="button" data-rows="after" aria-pressed="${S.rowsOf === 'after'}" ${S.trialResult?.ok ? '' : 'disabled'}>After the change</button>`;
  if (!t) { $('rows').innerHTML = '<p class="sl-hint">This table is new; it has no rows yet.</p>'; return; }
  const rows = h.q(`SELECT * FROM "${t.name.replace(/"/g, '""')}" LIMIT 60`), cols = t.columns.map(c => c.name);
  $('rows').innerHTML = `<table class="sl-grid"><thead><tr>${cols.map(c => `<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>${rows.map(r => `<tr>${cols.map(c => `<td title="${esc(typeof r[c])}">${r[c] == null ? '<em>empty</em>' : esc(String(r[c]).slice(0, 80))}</td>`).join('')}</tr>`).join('')}</tbody></table><p class="sl-hint">First ${rows.length} of ${(t.rows ?? 0).toLocaleString()} rows.</p>`;
}
$('rows-which').addEventListener('click', e => { const w = e.target.dataset.rows; if (w) { S.rowsOf = w; renderRows(); } });

function render() { draw(); inspector(); staged(); renderTrial(); renderRows(); }

const download = (name, type, data) => { const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([data], { type })); a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); };
const base = () => S.name.replace(/\.(sqlite3?|db)$/i, '');
$('download-sql').addEventListener('click', () => S.changes.length && download(base() + '-migration.sql', 'application/sql', lab.migration(S.before, S.after).join('\n\n') + '\n'));
$('download-db').addEventListener('click', () => S.trialResult?.ok && download(base() + '-changed.sqlite', 'application/vnd.sqlite3', S.copy.export()));
$('download-diagram').addEventListener('click', () => { const svg = $('diagram').cloneNode(true); svg.setAttribute('xmlns', NS); const style = document.createElementNS(NS, 'style'); style.textContent = [...document.styleSheets].flatMap(s => { try { return [...s.cssRules].map(r => r.cssText).filter(t => t.includes('.sl-')); } catch { return []; } }).join('\n'); svg.prepend(style); download(base() + '-diagram.svg', 'image/svg+xml', svg.outerHTML); });
$('open').addEventListener('change', async e => { const f = e.target.files[0]; if (!f) return; try { await openBytes(f.name, new Uint8Array(await f.arrayBuffer())); say(`Opened ${f.name} in this browser. The file itself is not changed.`); } catch (err) { say(err.message, true); } });
$('sample').addEventListener('click', loadSample);
async function loadSample() { const bytes = new Uint8Array(await (await fetch(new URL('sample.sqlite', here))).arrayBuffer()); await openBytes('research-and-311.sqlite', bytes); }

try { await loadSample(); $('loading').hidden = true; $('ready').hidden = false;
  await attachInstrumentStore({ prefix: 'schema-plan:', label: 'database', mount: $('ready'),
    get: () => ({ file: S.name, changes: S.changes, layout: S.layout, database: new Blob([S.bytes], { type: 'application/vnd.sqlite3' }) }),
    set: async v => { const bytes = v.database ? new Uint8Array(await v.database.arrayBuffer()) : S.bytes; await openBytes(v.file ?? S.name, bytes); S.changes = v.changes ?? []; S.layout = v.layout ?? S.layout; recompute(); } }); if (!S.changes.length) say('This sample holds the research desk\'s 87 papers and the simulator\'s 2,225 NYC 311 requests. Try making works.year a number.'); }
catch (e) { $('loading').textContent = `The schema workspace could not start: ${e.message}`; }
