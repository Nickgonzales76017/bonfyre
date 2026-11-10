// The shipment check in the browser: files are read, hashed and checked on
// this device; nothing is uploaded. A person's corrections are kept in this
// browser by file content, so they survive reloading the same files.
import { DOCUMENT_TYPES, readShipments, checkBatch, checkedCsv, missingCsv, pdfText, jpegText, jpegPhotoDate, storedZip } from './shipment-check.mjs';

const root = document.querySelector('[data-shipments]');
const base = new URL('.', import.meta.url);
const $ = name => root.querySelector(`[data-ui="${name}"]`);
const KEY = 'aurekai.shipment-check.v1';
const state = { shipments: [], shipmentsText: '', files: [], chosen: {}, filter: 'all', result: null, urls: new Map() };
const LABEL = { ready: 'Ready', duplicate: 'Duplicate', unmatched: 'No shipment', 'date-needs-review': 'Date to check', 'type-needs-review': 'Type to check' };
const TYPE_LABEL = { 'delivery-proof': 'Delivery proof', 'bill-of-lading': 'Bill of lading', 'shipping-label': 'Shipping label', invoice: 'Invoice', 'cargo-photo': 'Cargo photo', other: 'Other' };

function load() {
  try { const saved = JSON.parse(localStorage.getItem(KEY) || '{}'); state.shipmentsText = saved.shipmentsText || ''; state.chosen = saved.chosen || {}; } catch {}
}
function save() {
  try { localStorage.setItem(KEY, JSON.stringify({ shipmentsText: state.shipmentsText, chosen: state.chosen })); } catch {}
}
const say = (name, text, bad = false) => { const el = $(name); el.textContent = text; el.classList.toggle('sc-bad', bad); };

// Keeps what was decoded when a stream ends badly, as many real PDFs do.
async function inflate(bytes) {
  const reader = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('deflate')).getReader(), parts = [];
  try { for (;;) { const { done, value } = await reader.read(); if (done) break; parts.push(value); } }
  catch (error) { if (!parts.length) throw error; }
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0; for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}
async function readFile(file) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  const sha256 = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(b => b.toString(16).padStart(2, '0')).join('');
  const pdf = /\.pdf$/i.test(file.name) || (bytes[0] === 0x25 && bytes[1] === 0x50);
  let text = '';
  try { text = pdf ? await pdfText(bytes, inflate) : jpegText(bytes); } catch { text = ''; }
  return { key: sha256 + ':' + file.name, name: file.name, size: bytes.length, sha256, text: text.slice(0, 20000), photo_date: pdf ? '' : jpegPhotoDate(bytes), bytes, type: file.type };
}

function setShipments(text) {
  state.shipmentsText = text;
  try {
    state.shipments = text.trim() ? readShipments(text) : [];
    say('shipments-status', state.shipments.length ? `${state.shipments.length} shipments` : 'No shipment list yet.');
  } catch (error) { state.shipments = []; say('shipments-status', error.message, true); }
  save(); run();
}

async function addFiles(list) {
  const incoming = [...list].filter(file => file.size <= 50 * 1024 * 1024);
  if (incoming.length < list.length) say('files-status', 'Files over 50 MB were left out.', true);
  say('files-status', `Reading ${incoming.length} files…`);
  const read = await Promise.all(incoming.map(readFile));
  const known = new Set(state.files.map(file => file.key));
  for (const file of read) if (!known.has(file.key)) state.files.push(file);
  say('files-status', `${state.files.length} files in this check. They stay on this device.`);
  run();
}

function run() {
  const ready = state.shipments.length && state.files.length;
  $('results').hidden = !ready;
  $('empty').hidden = !!ready;
  if (!ready) return;
  state.result = checkBatch(state.shipments, state.files, state.chosen);
  const { counts, missing, rows } = state.result;
  const tiles = [['all', 'All files', rows.length], ['ready', LABEL.ready, counts.ready], ['duplicate', LABEL.duplicate, counts.duplicate],
    ['unmatched', LABEL.unmatched, counts.unmatched], ['date-needs-review', LABEL['date-needs-review'], counts['date-needs-review']],
    ['type-needs-review', LABEL['type-needs-review'], counts['type-needs-review']]];
  $('filters').replaceChildren(...tiles.map(([key, label, n]) => {
    const b = document.createElement('button');
    b.type = 'button'; b.dataset.filter = key; b.setAttribute('aria-pressed', String(state.filter === key));
    b.innerHTML = `<span class="sc-n"></span><span></span>`; b.children[0].textContent = n; b.children[1].textContent = label;
    return b;
  }));
  const shown = rows.filter(row => state.filter === 'all' || row.disposition === state.filter);
  $('rows').replaceChildren(...shown.map(rowView));
  $('missing-count').textContent = missing.length ? `${missing.length} shipments have no delivery proof in this batch` : 'Every shipment has a delivery proof in this batch';
  $('missing').replaceChildren(...missing.map(row => {
    const li = document.createElement('li');
    li.textContent = `${row.shipment_id}${row.destination ? ' · ' + row.destination : ''}${row.ship_date ? ' · shipped ' + row.ship_date : ''}`;
    return li;
  }));
  $('zip').disabled = !counts.ready;
  $('zip').textContent = counts.ready ? `Download ${counts.ready} renamed files (ZIP)` : 'No ready files to rename';
}

function select(options, value, label, onChange) {
  const el = document.createElement('select');
  el.setAttribute('aria-label', label);
  for (const [v, text] of options) { const o = document.createElement('option'); o.value = v; o.textContent = text; el.append(o); }
  el.value = value; el.addEventListener('change', () => onChange(el.value));
  return el;
}
function rowView(row) {
  const tr = document.createElement('tr'), file = state.files.find(f => f.key === row.key);
  const choose = patch => { state.chosen[row.key] = { ...(state.chosen[row.key] || {}), ...patch }; save(); run(); };
  const name = document.createElement('td');
  const link = document.createElement('a');
  if (!state.urls.has(row.key)) state.urls.set(row.key, URL.createObjectURL(new Blob([file.bytes], { type: file.type || (/\.pdf$/i.test(file.name) ? 'application/pdf' : '') })));
  link.href = state.urls.get(row.key); link.target = '_blank'; link.rel = 'noopener'; link.textContent = row.file_name;
  const meta = document.createElement('small'); meta.textContent = `${Math.max(1, Math.round(row.bytes / 1024))} KB · ${row.sha256.slice(0, 8)}`;
  name.append(link, meta);
  const shipment = document.createElement('td');
  shipment.append(select([['', '— choose —'], ...state.shipments.map(s => [s.shipment_id, s.shipment_id])], state.shipments.some(s => s.shipment_id === row.shipment_id) ? row.shipment_id : '',
    `Shipment for ${row.file_name}`, value => choose({ shipment_id: value || undefined })));
  const type = document.createElement('td');
  type.append(select([['', '— choose —'], ...DOCUMENT_TYPES.map(t => [t, TYPE_LABEL[t]])], row.document_type, `Document type for ${row.file_name}`, value => choose({ document_type: value || undefined })));
  const date = document.createElement('td'), input = document.createElement('input');
  input.type = 'date'; input.value = row.date; input.setAttribute('aria-label', `Date for ${row.file_name}`);
  input.addEventListener('change', () => choose({ date: input.value || undefined }));
  const from = document.createElement('small'); from.textContent = row.date_from ? `from ${row.date_from}` : '';
  date.append(input, from);
  const result = document.createElement('td'), badge = document.createElement('span');
  badge.className = `sc-badge sc-${row.disposition}`; badge.textContent = LABEL[row.disposition];
  result.append(badge);
  for (const reason of row.reasons) { const r = document.createElement('small'); r.textContent = reason; result.append(r); }
  const proposed = document.createElement('td'); proposed.className = 'sc-name'; proposed.textContent = row.proposed_name || '—';
  [[shipment, 'Shipment'], [type, 'Document'], [date, 'Date'], [result, 'Result'], [proposed, 'Proposed name']].forEach(([cell, label]) => { cell.dataset.label = label; });
  tr.append(name, shipment, type, date, result, proposed);
  return tr;
}

function download(name, blob) {
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

async function loadSample() {
  say('files-status', 'Loading the sample…');
  const sample = await (await fetch(new URL('sample.json', base))).json();
  setShipments(await (await fetch(new URL(sample.shipments, base))).text());
  $('shipments-text').value = state.shipmentsText;
  const files = await Promise.all(sample.files.map(async item => new File([await (await fetch(new URL('sample/' + item.name, base))).arrayBuffer()], item.name)));
  state.files = [];
  await addFiles(files);
}

load();
$('shipments-text').value = state.shipmentsText;
if (state.shipmentsText) setShipments(state.shipmentsText);
$('shipments-text').addEventListener('change', event => setShipments(event.target.value));
$('shipments-file').addEventListener('change', async event => { const f = event.target.files[0]; if (f) { const t = await f.text(); $('shipments-text').value = t; setShipments(t); } });
$('files').addEventListener('change', event => addFiles(event.target.files));
const drop = $('drop');
drop.addEventListener('dragover', event => { event.preventDefault(); drop.classList.add('sc-over'); });
drop.addEventListener('dragleave', () => drop.classList.remove('sc-over'));
drop.addEventListener('drop', event => { event.preventDefault(); drop.classList.remove('sc-over'); addFiles(event.dataTransfer.files); });
$('sample').addEventListener('click', loadSample);
$('clear').addEventListener('click', () => { for (const url of state.urls.values()) URL.revokeObjectURL(url); state.urls.clear(); state.files = []; say('files-status', 'No files yet.'); run(); });
$('filters').addEventListener('click', event => { const b = event.target.closest('[data-filter]'); if (b) { state.filter = b.dataset.filter; run(); } });
$('csv').addEventListener('click', () => download('shipment-check.csv', new Blob([checkedCsv(state.result)], { type: 'text/csv' })));
$('missing-csv').addEventListener('click', () => download('missing-delivery-proof.csv', new Blob([missingCsv(state.result)], { type: 'text/csv' })));
$('zip').addEventListener('click', () => {
  const entries = state.result.rows.filter(row => row.disposition === 'ready').map(row => ({ name: row.proposed_name, bytes: state.files.find(f => f.key === row.key).bytes }));
  download('renamed-shipment-files.zip', storedZip(entries));
});
say('files-status', 'No files yet.');
run();
