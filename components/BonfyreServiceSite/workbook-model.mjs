import * as XLSX from '../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';

const types = { s: 1, n: 2, b: 3 };
const functions = new Set('SUM SUMIF SUMIFS COUNT COUNTA COUNTIF COUNTIFS MIN MAX AVERAGE IF IFERROR AND OR NOT ROUND ROUNDUP ROUNDDOWN ABS SUBSTITUTE LEN NPV IRR PMT POWER SQRT'.split(' '));
export function validateWorkbook(snapshot) {
  if (!snapshot || typeof snapshot !== 'object' || !Array.isArray(snapshot.sheetOrder) || !snapshot.sheetOrder.length || snapshot.sheetOrder.length > 32 || !snapshot.sheets)
    throw Error('Choose a workbook with one to 32 sheets.');
  if (new TextEncoder().encode(JSON.stringify(snapshot)).length > 12 * 1024 * 1024) throw Error('Workbook exceeds 12 MiB.');
  if (typeof snapshot.id !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(snapshot.id) || typeof snapshot.name !== 'string' || !snapshot.name.trim() || snapshot.name.length > 200) throw Error('Invalid workbook identity or title.');
  const ids = new Set(), names = new Set(); let count = 0;
  for (const id of snapshot.sheetOrder) {
    const sheet = snapshot.sheets[id];
    if (typeof id !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(id) || ids.has(id) || !sheet || sheet.id !== id) throw Error('Invalid sheet identity.');
    ids.add(id);
    if (typeof sheet.name !== 'string' || !sheet.name || sheet.name.length > 31 || /[\\/\[\]*?:]/.test(sheet.name) || names.has(sheet.name.toLowerCase())) throw Error('Sheet names must be valid and unique.');
    names.add(sheet.name.toLowerCase());
    if (!Number.isInteger(sheet.rowCount) || sheet.rowCount < 1 || sheet.rowCount > 5000 || !Number.isInteger(sheet.columnCount) || sheet.columnCount < 1 || sheet.columnCount > 128) throw Error('A sheet supports up to 5,000 rows and 128 columns.');
    for (const [r, row] of Object.entries(sheet.cellData ?? {})) for (const [c, cell] of Object.entries(row)) {
      if (!/^\d+$/.test(r) || !/^\d+$/.test(c) || +r >= sheet.rowCount || +c >= sheet.columnCount || ++count > 200000) throw Error('Workbook cell bounds exceeded.');
      if (!cell || typeof cell !== 'object' || cell.p || cell.si) throw Error('Rich text and shared formula cells need conversion before editing.');
      if (cell.v !== null && cell.v !== undefined && !['string', 'number', 'boolean'].includes(typeof cell.v) || typeof cell.v === 'number' && !Number.isFinite(cell.v)) throw Error('Invalid cell value.');
      if (typeof cell.v === 'string' && cell.v.length > 32767) throw Error('Cell text exceeds 32,767 characters.');
      if (cell.f) {
        if (typeof cell.f !== 'string' || cell.f.length > 8192 || !cell.f.startsWith('=')) throw Error('Invalid formula.');
        const withoutStrings = cell.f.replace(/"(?:[^"]|"")*"/g, '""');
        const code = withoutStrings.replace(/'(?:[^']|'')*'/g, "''");
        if (/[\[\]{}]/.test(withoutStrings)) throw Error('External references, table formulas and array constants need a separate workbook review.');
        for (const match of code.matchAll(/([A-Z_][A-Z0-9_.]*)\s*\(/gi)) if (!functions.has(match[1].toUpperCase())) throw Error(`Formula function ${match[1]} is not enabled in this workspace.`);
        for (const match of code.matchAll(/\$?([A-Z]{1,3})\$?(\d+)\b(?!\s*\()/gi)) {
          const coordinate = XLSX.utils.decode_cell(match[0].replaceAll('$', ''));
          if (coordinate.r >= 5000 || coordinate.c >= 128) throw Error('Formula reference exceeds the workbook bounds.');
        }
      }
    }
  }
  return snapshot;
}

// This exchange preserves the review model's scalar cells, formulas, number
// formats, dimensions and visibility. It is not a lossless Excel file editor.
export function workbookSnapshot(workbook, { id = crypto.randomUUID(), name = 'Financial review' } = {}) {
  const snapshot = { id, name, appVersion: '1.1.0', locale: 'enUS', sheetOrder: [], sheets: {}, styles: {} };
  for (let i = 0; i < workbook.SheetNames.length; i++) {
    const title = workbook.SheetNames[i], input = workbook.Sheets[title], sheetId = `sheet-${i}`;
    const range = XLSX.utils.decode_range(input['!ref'] ?? 'A1');
    if (range.e.r >= 5000 || range.e.c >= 128) throw Error('A sheet supports up to 5,000 rows and 128 columns.');
    const sheet = { id: sheetId, name: title, rowCount: Math.min(5000, Math.max(100, range.e.r + 20)), columnCount: Math.max(16, range.e.c + 1), cellData: {}, rowData: {}, columnData: {}, hidden: workbook.Workbook?.Sheets?.[i]?.Hidden ? 1 : 0 };
    if (input['!merges']?.length) throw Error('Unmerge cells before opening this review model.');
    for (const [address, cell] of Object.entries(input)) {
      if (address.startsWith('!') || cell.t === 'z') continue;
      if (!Object.hasOwn(types, cell.t) || cell.F) throw Error(`Unsupported cell at ${title}!${address}.`);
      const { r, c } = XLSX.utils.decode_cell(address);
      const value = { t: types[cell.t], ...(cell.v === undefined ? {} : { v: cell.v }), ...(cell.f ? { f: `=${cell.f}` } : {}), ...(cell.z ? { s: { n: { pattern: cell.z } } } : {}) };
      (sheet.cellData[r] ??= {})[c] = value;
    }
    for (const [r, data] of (input['!rows'] ?? []).entries()) if (data) sheet.rowData[r] = { ...(data.hidden ? { hd: 1 } : {}), ...(data.hpx ? { h: data.hpx } : {}) };
    for (const [c, data] of (input['!cols'] ?? []).entries()) if (data) sheet.columnData[c] = { ...(data.hidden ? { hd: 1 } : {}), w: data.wpx ?? (data.wch ?? 12) * 7 + 10 };
    snapshot.sheetOrder.push(sheetId); snapshot.sheets[sheetId] = sheet;
  }
  return validateWorkbook(snapshot);
}

export function exportWorkbookSnapshot(snapshot) {
  validateWorkbook(snapshot);
  const workbook = XLSX.utils.book_new();
  for (const id of snapshot.sheetOrder) {
    const input = snapshot.sheets[id], sheet = {}; let lastRow = 0, lastColumn = 0;
    for (const [r, row] of Object.entries(input.cellData)) for (const [c, cell] of Object.entries(row)) {
      if (cell.v === undefined && !cell.f) continue;
      const type = typeof cell.v === 'number' ? 'n' : typeof cell.v === 'boolean' ? 'b' : 's';
      const style = typeof cell.s === 'string' ? snapshot.styles?.[cell.s] : cell.s;
      sheet[XLSX.utils.encode_cell({ r: +r, c: +c })] = { t: type, ...(cell.v === undefined ? {} : { v: cell.v }), ...(cell.f ? { f: cell.f.slice(1) } : {}), ...(style?.n?.pattern ? { z: style.n.pattern } : {}) };
      lastRow = Math.max(lastRow, +r); lastColumn = Math.max(lastColumn, +c);
    }
    sheet['!ref'] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: lastRow, c: lastColumn } });
    sheet['!cols'] = Array.from({ length: lastColumn + 1 }, (_, c) => ({ wpx: input.columnData?.[c]?.w ?? 100, hidden: !!input.columnData?.[c]?.hd }));
    sheet['!rows'] = Array.from({ length: lastRow + 1 }, (_, r) => ({ hidden: !!input.rowData?.[r]?.hd }));
    XLSX.utils.book_append_sheet(workbook, sheet, input.name);
  }
  workbook.Workbook = { Sheets: snapshot.sheetOrder.map(id => ({ name: snapshot.sheets[id].name, Hidden: snapshot.sheets[id].hidden ? 1 : 0 })) };
  return XLSX.write(workbook, { type: 'array', bookType: 'xlsx', compression: true });
}

export function calculationErrors(snapshot) {
  const errors = [];
  for (const id of snapshot.sheetOrder) for (const [r, row] of Object.entries(snapshot.sheets[id].cellData ?? {})) for (const [c, cell] of Object.entries(row)) {
    if (cell.f && (cell.v === undefined || cell.v === null || /^#(REF!|DIV\/0!|VALUE!|NAME\?|N\/A|NULL!|NUM!|SPILL!|CALC!|CYCLE!|ERROR!)/.test(String(cell.v))))
      errors.push({ sheet: snapshot.sheets[id].name, cell: XLSX.utils.encode_cell({ r: +r, c: +c }), formula: cell.f, error: cell.v ?? 'No result' });
  }
  return errors;
}

export function hasFormulas(snapshot) {
  return snapshot.sheetOrder.some(id => Object.values(snapshot.sheets[id].cellData ?? {}).some(row => Object.values(row).some(cell => cell.f)));
}
