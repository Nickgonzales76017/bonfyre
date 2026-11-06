// Shipment document check: the rules, with no browser or file system in them,
// so the page and the tests run the same code. A batch of files is matched to
// a list of shipments: exact duplicates by content hash, the shipment by a
// reference found in the file name or text, the date and document type from
// what the file says, and every shipment is checked for a delivery proof.
// Nothing is renamed or sent; the answer is a proposal to review.

export const DOCUMENT_TYPES = ['delivery-proof', 'bill-of-lading', 'shipping-label', 'invoice', 'cargo-photo', 'other'];
const TYPE_WORDS = [
  ['delivery-proof', /\b(pod|proof[\s_-]*of[\s_-]*delivery|delivery[\s_-]*(receipt|proof|confirmation)|signed[\s_-]*for|delivered)\b/i],
  ['bill-of-lading', /\b(bol|b[\s/._-]?o[\s/._-]?l|bill[\s_-]*of[\s_-]*lading)\b/i],
  ['shipping-label', /\b(label|shipping[\s_-]*label|tracking)\b/i],
  ['invoice', /\b(invoice|inv|freight[\s_-]*bill)\b/i],
];
const IMAGE = /\.(jpe?g|png|heic|webp|gif|tiff?)$/i;

// A shipment list as CSV text: a shipment column is required; date, origin and
// destination are kept when present.
export function readShipments(text) {
  const rows = parseCsv(text);
  if (!rows.length) throw new Error('the shipment list is empty');
  const header = rows[0].map(cell => cell.trim().toLowerCase());
  const at = names => header.findIndex(cell => names.some(name => cell === name || cell.replace(/[\s_-]+/g, '') === name.replace(/[\s_-]+/g, '')));
  const id = at(['shipment', 'shipment id', 'shipment_id', 'load', 'load id', 'reference', 'pro', 'pro number']);
  if (id < 0) throw new Error('the shipment list needs a shipment (or load, reference, PRO) column');
  const date = at(['ship date', 'ship_date', 'date', 'pickup date']), origin = at(['origin', 'from', 'pickup']), destination = at(['destination', 'to', 'delivery', 'consignee']);
  const seen = new Set(), shipments = [];
  for (const cells of rows.slice(1)) {
    const value = String(cells[id] ?? '').trim();
    if (!value) continue;
    if (seen.has(value.toUpperCase())) throw new Error(`shipment ${value} is listed twice`);
    seen.add(value.toUpperCase());
    shipments.push({ shipment_id: value, ship_date: normalDate(cells[date] ?? '') || String(cells[date] ?? '').trim(),
      origin: String(cells[origin] ?? '').trim(), destination: String(cells[destination] ?? '').trim() });
  }
  if (!shipments.length) throw new Error('the shipment list has no shipments');
  return shipments;
}

// Dates as written on documents: ISO, US numeric, and "Oct 1, 2026".
const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
export function normalDate(text) {
  const s = String(text || '').trim();
  let m = /\b(20\d{2})[-/._](\d{1,2})[-/._](\d{1,2})\b/.exec(s) || /\b(20\d{2})(\d{2})(\d{2})\b/.exec(s);
  if (m) return valid(+m[1], +m[2], +m[3]);
  m = /\b(\d{1,2})[/.-](\d{1,2})[/.-](20\d{2}|\d{2})\b/.exec(s);
  if (m) return valid(m[3].length === 2 ? 2000 + +m[3] : +m[3], +m[1], +m[2]);
  m = /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2}),?\s+(20\d{2})\b/i.exec(s);
  if (m) return valid(+m[3], MONTHS.indexOf(m[1].toLowerCase()) + 1, +m[2]);
  return '';
}
function valid(y, mo, d) {
  const date = new Date(Date.UTC(y, mo - 1, d));
  return date.getUTCFullYear() === y && date.getUTCMonth() === mo - 1 && date.getUTCDate() === d ? date.toISOString().slice(0, 10) : '';
}

// The shipment a file belongs to: a listed reference in its name or text,
// ignoring case and separators. Two different references is ambiguous.
export function findShipment(shipments, ...texts) {
  const squash = value => String(value).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const haystack = texts.map(squash).join(' ');
  const hits = shipments.filter(row => { const key = squash(row.shipment_id); return key.length >= 3 && haystack.includes(key); });
  if (hits.length === 1) return { shipment_id: hits[0].shipment_id };
  if (hits.length > 1) return { ambiguous: hits.map(row => row.shipment_id) };
  // A reference shaped like the listed ones (same letters, then digits) that
  // is not in the list is named, so the gap can be traced.
  const prefixes = [...new Set(shipments.map(row => /^[A-Za-z]{1,6}/.exec(row.shipment_id)?.[0]).filter(Boolean))];
  for (const prefix of prefixes) {
    const seen = new RegExp(`\\b${prefix}[\\s_-]?(\\d{3,})\\b`, 'i').exec(texts.join(' '));
    if (seen) return { unlisted: `${prefix.toUpperCase()}-${seen[1]}` };
  }
  return {};
}

export function documentType(name, text = '') {
  for (const [type, pattern] of TYPE_WORDS) if (pattern.test(name) || pattern.test(text)) return type;
  return IMAGE.test(name) ? 'cargo-photo' : '';
}

// One row per file. `facts` is what the page read from each file: its hash,
// text (PDF), photo date (EXIF) and file timestamp. A person's choice in the
// table (`chosen`) always wins over what was read.
export function checkBatch(shipments, facts, chosen = {}) {
  const byId = new Map(shipments.map(row => [row.shipment_id.toUpperCase(), row]));
  const firstByHash = new Map();
  const rows = facts.map(file => {
    const pick = chosen[file.key] || {};
    const found = findShipment(shipments, file.name, file.text || '');
    const shipment_id = pick.shipment_id ?? found.shipment_id ?? found.unlisted ?? '';
    const type = pick.document_type ?? documentType(file.name, file.text);
    const read = normalDate(file.text || '') || normalDate(file.name) || file.photo_date || '';
    const date = pick.date ?? read;
    const date_from = pick.date ? 'person' : normalDate(file.text || '') ? 'document text' : normalDate(file.name) ? 'file name' : file.photo_date ? 'photo' : '';
    const shipment = byId.get(String(shipment_id).toUpperCase());
    const reasons = [];
    const earlier = firstByHash.get(file.sha256);
    if (earlier) reasons.push(`same content as ${earlier}`);
    else firstByHash.set(file.sha256, file.name);
    if (found.ambiguous && !pick.shipment_id) reasons.push(`names more than one shipment: ${found.ambiguous.join(', ')}`);
    else if (!shipment) reasons.push(shipment_id ? `shipment ${shipment_id} is not in the list` : 'no listed shipment found');
    if (!date) reasons.push('no readable date');
    if (!type) reasons.push('document type unknown');
    const disposition = earlier ? 'duplicate' : !shipment ? 'unmatched' : !date ? 'date-needs-review' : !type ? 'type-needs-review' : 'ready';
    const extension = (/\.([A-Za-z0-9]{1,8})$/.exec(file.name)?.[1] || 'bin').toLowerCase();
    return { key: file.key, file_name: file.name, bytes: file.size, sha256: file.sha256, shipment_id: shipment?.shipment_id || shipment_id,
      document_type: type, date, date_from, disposition, reasons,
      proposed_name: disposition === 'ready' ? `${shipment.shipment_id}_${date}_${type}_${file.sha256.slice(0, 8)}.${extension}` : '' };
  });
  const missing = shipments.filter(row => !rows.some(file => file.disposition === 'ready' && file.shipment_id === row.shipment_id && file.document_type === 'delivery-proof'))
    .map(row => ({ ...row, issue: 'No delivery proof in this batch' }));
  const counts = Object.fromEntries(['ready', 'duplicate', 'unmatched', 'date-needs-review', 'type-needs-review'].map(name => [name, rows.filter(row => row.disposition === name).length]));
  return { rows, missing, counts };
}

export function checkedCsv(result) {
  const head = ['file_name', 'shipment_id', 'document_type', 'date', 'date_from', 'disposition', 'reasons', 'proposed_name', 'sha256', 'bytes'];
  return [head.join(','), ...result.rows.map(row => [row.file_name, row.shipment_id, row.document_type, row.date, row.date_from, row.disposition,
    row.reasons.join('; '), row.proposed_name, row.sha256, row.bytes].map(csvCell).join(','))].join('\n') + '\n';
}
export function missingCsv(result) {
  return ['shipment_id,ship_date,origin,destination,issue', ...result.missing.map(row =>
    [row.shipment_id, row.ship_date, row.origin, row.destination, row.issue].map(csvCell).join(','))].join('\n') + '\n';
}

// Text a PDF shows, from its content streams: literal strings shown with Tj,
// TJ and ' operators. `inflate` turns FlateDecode bytes into bytes (the page
// passes DecompressionStream, tests pass zlib). Scanned PDFs have no text.
export async function pdfText(bytes, inflate) {
  const latin = new TextDecoder('latin1').decode(bytes), out = [];
  const streams = /<<(.*?)>>\s*stream\r?\n/gs;
  let match;
  while ((match = streams.exec(latin))) {
    const start = match.index + match[0].length, end = latin.indexOf('endstream', start);
    if (end < 0) break;
    // The line break before endstream is not part of the data; a strict
    // decompressor (the browser's) refuses bytes after the end of it.
    let stop = end;
    while (stop > start && (bytes[stop - 1] === 0x0a || bytes[stop - 1] === 0x0d)) stop--;
    let body = bytes.subarray(start, stop);
    if (/\/FlateDecode/.test(match[1])) { try { body = await inflate(body); } catch { continue; } }
    else if (/\/Filter/.test(match[1])) continue;
    out.push(showText(new TextDecoder('latin1').decode(body)));
    streams.lastIndex = end;
  }
  return out.join(' ').replace(/\s+/g, ' ').trim();
}
function showText(content) {
  const parts = [];
  for (const block of content.match(/BT[\s\S]*?ET/g) || []) {
    for (const m of block.matchAll(/\((?:\\.|[^\\)])*\)/g)) parts.push(m[0].slice(1, -1).replace(/\\([nrtbf()\\])/g, (_, c) => ({ n: '\n', r: '\r', t: '\t', b: '', f: '' })[c] ?? c).replace(/\\(\d{1,3})/g, (_, o) => String.fromCharCode(parseInt(o, 8))), ' ');
    parts.push(' ');
  }
  return parts.join('');
}

// Readable text a JPEG carries in its EXIF segment (description, caption),
// which phone and scanner apps often fill with the job or load reference.
export function jpegText(bytes) {
  if (bytes[0] !== 0xff || bytes[1] !== 0xd8) return '';
  for (let i = 2; i + 4 < bytes.length && bytes[i] === 0xff;) {
    const marker = bytes[i + 1], length = (bytes[i + 2] << 8) | bytes[i + 3];
    if (marker === 0xe1) return (new TextDecoder('latin1').decode(bytes.subarray(i + 4, i + 2 + length)).match(/[ -~]{4,}/g) || [])
      .filter(run => !/^(Exif|MM|II)$/.test(run)).join(' ');
    if (marker === 0xda) break;
    i += 2 + length;
  }
  return '';
}

// The date a camera wrote (EXIF DateTimeOriginal) in a JPEG, as YYYY-MM-DD.
export function jpegPhotoDate(bytes) {
  if (bytes[0] !== 0xff || bytes[1] !== 0xd8) return '';
  const text = new TextDecoder('latin1').decode(bytes.subarray(0, Math.min(bytes.length, 131072)));
  const m = /(20\d{2}):(\d{2}):(\d{2}) \d{2}:\d{2}:\d{2}/.exec(text);
  return m ? valid(+m[1], +m[2], +m[3]) : '';
}

// A ZIP of the ready files under their proposed names, stored without
// compression (documents and photos are already compressed).
export function storedZip(entries) {
  const encoder = new TextEncoder(), local = [], central = [];
  let offset = 0;
  for (const { name, bytes } of entries) {
    const nameBytes = encoder.encode(name), crc = crc32(bytes);
    const header = new DataView(new ArrayBuffer(30));
    header.setUint32(0, 0x04034b50, true); header.setUint16(4, 20, true); header.setUint16(8, 0, true);
    header.setUint32(14, crc, true); header.setUint32(18, bytes.length, true); header.setUint32(22, bytes.length, true); header.setUint16(26, nameBytes.length, true);
    local.push(new Uint8Array(header.buffer), nameBytes, bytes);
    const record = new DataView(new ArrayBuffer(46));
    record.setUint32(0, 0x02014b50, true); record.setUint16(4, 20, true); record.setUint16(6, 20, true);
    record.setUint32(16, crc, true); record.setUint32(20, bytes.length, true); record.setUint32(24, bytes.length, true); record.setUint16(28, nameBytes.length, true);
    record.setUint32(42, offset, true);
    central.push(new Uint8Array(record.buffer), nameBytes);
    offset += 30 + nameBytes.length + bytes.length;
  }
  const size = central.reduce((sum, part) => sum + part.length, 0), end = new DataView(new ArrayBuffer(22));
  end.setUint32(0, 0x06054b50, true); end.setUint16(8, entries.length, true); end.setUint16(10, entries.length, true);
  end.setUint32(12, size, true); end.setUint32(16, offset, true);
  return new Blob([...local, ...central, new Uint8Array(end.buffer)], { type: 'application/zip' });
}
const CRC_TABLE = Array.from({ length: 256 }, (_, n) => { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; return c >>> 0; });
export function crc32(bytes) { let c = 0xffffffff; for (const b of bytes) c = CRC_TABLE[(c ^ b) & 0xff] ^ (c >>> 8); return (c ^ 0xffffffff) >>> 0; }

function parseCsv(text) {
  const rows = []; let row = [], field = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) { if (c === '"' && text[i + 1] === '"') { field += '"'; i++; } else if (c === '"') quoted = false; else field += c; }
    else if (c === '"') quoted = true;
    else if (c === ',' || c === '\t') { row.push(field); field = ''; }
    else if (c === '\n' || c === '\r') { if (c === '\r' && text[i + 1] === '\n') i++; row.push(field); field = ''; if (row.some(cell => cell.trim())) rows.push(row); row = []; }
    else field += c;
  }
  row.push(field); if (row.some(cell => cell.trim())) rows.push(row);
  return rows;
}
const csvCell = value => /[",\n]/.test(String(value)) ? `"${String(value).replaceAll('"', '""')}"` : String(value ?? '');
