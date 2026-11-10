import * as XLSX from '../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { workbookSnapshot } from './workbook-model.mjs';
const maxFile = 8 * 1024 * 1024, maxExpanded = 32 * 1024 * 1024;

// Browser worker import: bound actual decompression before SheetJS parses XML.
// Neither the compressed file length nor declared ZIP sizes bound expansion.
export async function importWorkbook(input, { name = 'Imported review' } = {}) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  if (bytes.length < 22 || bytes.length > maxFile) throw Error('Choose an XLSX workbook of at most 8 MiB.');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), u16 = p => view.getUint16(p, true), u32 = p => view.getUint32(p, true);
  if (u32(0) !== 0x04034b50) throw Error('Choose an XLSX workbook.');
  let end = -1;
  for (let p = bytes.length - 22; p >= Math.max(0, bytes.length - 65557); p--)
    if (u32(p) === 0x06054b50 && p + 22 + u16(p + 20) === bytes.length) { end = p; break; }
  if (end < 0 || u16(end + 4) || u16(end + 6)) throw Error('Unsupported XLSX archive.');
  const count = u16(end + 10), directory = u32(end + 16), names = new Set(), decoder = new TextDecoder('utf-8', { fatal: true });
  if (!count || count > 2000 || count !== u16(end + 8) || directory + u32(end + 12) !== end) throw Error('Invalid XLSX directory.');
  let at = directory, expanded = 0;
  for (let i = 0; i < count; i++) {
    if (at + 46 > end || u32(at) !== 0x02014b50) throw Error('Invalid XLSX member.');
    const flags = u16(at + 8), method = u16(at + 10), size = u32(at + 20), raw = u32(at + 24), length = u16(at + 28), local = u32(at + 42);
    const next = at + 46 + length + u16(at + 30) + u16(at + 32);
    if (next > end || flags & 1 || ![0, 8].includes(method) || raw > maxExpanded - expanded || local + 30 > directory) throw Error('Unsupported or oversized XLSX member.');
    const name = decoder.decode(bytes.subarray(at + 46, at + 46 + length));
    if (names.has(name) || /(^\/|\\|(^|\/)\.\.($|\/)|vbaProject|macrosheets)/i.test(name)) throw Error('Duplicate, unsafe, or macro workbook member.');
    names.add(name);
    if (u32(local) !== 0x04034b50 || u16(local + 8) !== method || u16(local + 6) & 1) throw Error('Invalid XLSX member header.');
    const localLength = u16(local + 26), start = local + 30 + localLength + u16(local + 28);
    if (start + size > directory || decoder.decode(bytes.subarray(local + 30, local + 30 + localLength)) !== name) throw Error('Invalid XLSX member location.');
    let actual = 0;
    if (method === 0) actual = size;
    else {
      const reader = new Blob([bytes.subarray(start, start + size)]).stream().pipeThrough(new DecompressionStream('deflate-raw')).getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read(); if (done) break;
          actual += value.length;
          if (actual > raw || actual > maxExpanded - expanded) throw Error('XLSX expansion exceeds its declared size or limit.');
        }
      } catch (error) { await reader.cancel().catch(() => {}); throw error; }
      finally { reader.releaseLock(); }
    }
    if (actual !== raw || actual > maxExpanded - expanded) throw Error('XLSX member size does not match.');
    expanded += actual; at = next;
  }
  if (at !== end || !names.has('xl/workbook.xml') || !names.has('[Content_Types].xml')) throw Error('Choose an XLSX workbook.');
  const workbook = XLSX.read(bytes, { type: 'array', cellFormula: true, cellNF: true, cellStyles: true, sheetStubs: true });
  const snapshot = workbookSnapshot(workbook, { name });
  const sha256 = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(x => x.toString(16).padStart(2, '0')).join('');
  return { snapshot, source: { name, sha256, bytes: bytes.length,
    baseline: new Blob([JSON.stringify(snapshot)], { type: 'application/json' }),
    original: new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }) } };
}
