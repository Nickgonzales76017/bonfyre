import * as XLSX from '../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { validateWorkbook } from './workbook-model.mjs';

export const reviewRoles = { description: 'Description', amount: 'Amount', account: 'Account', date: 'Date', suggested: 'Suggested account', review: 'Review', sourceSheet: 'Source sheet', sourceRow: 'Source row', sourceCell: 'Source amount cell', sourceHash: 'Source SHA-256' };
export const reviewBodyLimit = 8 * 1024 * 1024;
const required = ['description', 'amount', 'account'], hashPattern = /^[a-f0-9]{64}$/;
const sha = async value => [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value)))].map(n => n.toString(16).padStart(2, '0')).join('');
const plain = value => value && !Array.isArray(value) && typeof value === 'object';
const text = cell => cell?.v === null || cell?.v === undefined ? '' : String(cell.v);
const cellError = cell => cell?.t === 4 || cell?.f && (cell.v === null || cell.v === undefined || /^#(?:REF!|DIV\/0!|VALUE!|NAME\?|N\/A|NULL!|NUM!|SPILL!|CALC!|CYCLE!|ERROR!)/.test(text(cell)));
function cents(cell) {
  if (!cell || cellError(cell) || typeof cell.v === 'boolean') return null;
  const raw = text(cell).trim(); if (!/^-?\d+(?:\.\d{1,2})?$/.test(raw)) return null;
  const negative = raw.startsWith('-'), [whole, decimal = ''] = raw.replace(/^-/, '').split('.');
  const value = (BigInt(whole) * 100n + BigInt(decimal.padEnd(2, '0'))) * (negative ? -1n : 1n);
  return value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER) ? Number(value) : null;
}
export function validateReview(review, snapshot) {
  validateWorkbook(snapshot);
  if (!plain(review) || review.schema !== 'bonfyre.workbook-review.v1' || !plain(review.table) || !Array.isArray(review.decisions) || review.decisions.length > 5000) throw Error('Invalid transaction review.');
  const table = review.table, sheet = snapshot.sheets[table.sheetId], columns = table.columns;
  if (!snapshot.sheetOrder.includes(table.sheetId) || !Number.isInteger(table.headerRow) || table.headerRow < 0 || table.headerRow >= sheet.rowCount - 1 || !plain(columns)) throw Error('Choose a worksheet and a header row followed by transactions.');
  const used = new Set();
  for (const [role, col] of Object.entries(columns)) {
    if (!Object.hasOwn(reviewRoles, role) || !Number.isInteger(col) || col < 0 || col >= sheet.columnCount || used.has(col)) throw Error('Choose a different valid column for each review field.');
    used.add(col);
  }
  if (required.some(role => !Object.hasOwn(columns, role))) throw Error('Choose description, amount and account columns.');
  const seen = new Set();
  for (const decision of review.decisions) {
    if (!plain(decision) || !Number.isInteger(decision.row) || decision.row <= table.headerRow || decision.row >= 5000 || seen.has(decision.row) || !hashPattern.test(decision.fingerprint ?? '') || typeof decision.note !== 'string' || decision.note.length > 1000 || typeof decision.at !== 'string' || !Number.isFinite(Date.parse(decision.at))) throw Error('Invalid reviewed-row decision.');
    seen.add(decision.row);
  }
  if (new TextEncoder().encode(JSON.stringify(review)).length > reviewBodyLimit) throw Error('Review decisions exceed the 8 MiB storage limit. Shorten the review notes.');
  return review;
}
export function createReview(table, snapshot) {
  return validateReview({ schema: 'bonfyre.workbook-review.v1', table: structuredClone(table), decisions: [] }, snapshot);
}
export function setReviewCell(workbook, review, row, role, value) {
  const snapshot = workbook.save(); validateReview(review, snapshot);
  const { sheetId, headerRow, columns } = review.table;
  if (!['amount', 'account'].includes(role) || !Number.isInteger(row) || row <= headerRow || row >= snapshot.sheets[sheetId].rowCount) throw Error('Choose a transaction amount or account to edit.');
  if (role === 'amount' && (typeof value !== 'number' || cents({ v: value }) === null)) throw Error('Enter a supported amount with at most two decimal places.');
  if (role === 'account' && (typeof value !== 'string' || value.length > 1000)) throw Error('Enter an account of at most 1,000 characters.');
  workbook.getSheetBySheetId(sheetId).getRange(row, columns[role]).setValue({ v: value, t: typeof value === 'number' ? 2 : 1, f: null });
}
export function detectReview(snapshot) {
  validateWorkbook(snapshot);
  const matches = [];
  for (const sheetId of snapshot.sheetOrder) {
    const sheet = snapshot.sheets[sheetId];
    for (let headerRow = 0; headerRow < Math.min(10, sheet.rowCount - 1); headerRow++) {
      const columns = {}, seen = new Set(); let ambiguous = false;
      for (const [col, cell] of Object.entries(sheet.cellData?.[headerRow] ?? {})) {
        const role = Object.keys(reviewRoles).find(role => reviewRoles[role].toLowerCase() === text(cell).trim().toLowerCase());
        if (role) { if (seen.has(role)) ambiguous = true; seen.add(role); columns[role] = Number(col); }
      }
      if (!ambiguous && required.every(role => Object.hasOwn(columns, role))) matches.push({ sheetId, headerRow, columns });
    }
  }
  return matches.length === 1 ? createReview(matches[0], snapshot) : null;
}
export async function projectReview(snapshot, review) {
  validateReview(review, snapshot);
  const { sheetId, headerRow, columns } = review.table, sheet = snapshot.sheets[sheetId], decisions = new Map(review.decisions.map(d => [d.row, d]));
  const rows = [], duplicates = new Map();
  for (const row of Object.keys(sheet.cellData ?? {}).map(Number).sort((a, b) => a - b)) {
    if (row <= headerRow) continue;
    const cells = sheet.cellData[row], value = role => text(cells[columns[role]]);
    if (!Object.values(columns).some(col => cells[col]?.f || text(cells[col]).trim())) continue;
    const amount = cents(cells[columns.amount]), issues = [];
    if (amount === null) issues.push('Amount needs review');
    if (!value('account').trim()) issues.push('Missing account');
    if (!value('description').trim()) issues.push('Missing description');
    if (Object.values(cells).some(cellError)) issues.push('Formula error');
    const duplicateKey = JSON.stringify([value('date').trim(), value('description').trim().toLowerCase(), amount]);
    if (amount !== null && value('description').trim()) { if (!duplicates.has(duplicateKey)) duplicates.set(duplicateKey, []); duplicates.get(duplicateKey).push(row); }
    const content = Object.entries(cells).sort(([a], [b]) => +a - +b).map(([col, cell]) => [Number(col), cell.v ?? null, cell.f ?? null, cell.t ?? null]);
    rows.push({ id: String(row), row, address: `${sheet.name}!${XLSX.utils.encode_cell({ r: row, c: columns.amount })}`, description: value('description'), date: value('date'), amount_cents: amount,
      amount: value('amount'), account: value('account'), suggested: value('suggested'), originStatus: value('review'), issues, duplicateKey, content,
      source: { sheet: value('sourceSheet'), row: value('sourceRow'), cell: value('sourceCell'), sha256: value('sourceHash') } });
  }
  await Promise.all(rows.map(async row => {
    const group = duplicates.get(row.duplicateKey) ?? [];
    if (group.length > 1) row.issues.push('Possible duplicate');
    row.fingerprint = await sha(JSON.stringify([review.table, row.row, row.content, group]));
    const decision = decisions.get(row.row);
    row.reviewed = decision?.fingerprint === row.fingerprint;
    row.changed = !!decision && !row.reviewed;
    row.note = decision?.note ?? ''; row.reviewedAt = row.reviewed ? decision.at : null;
    row.status = row.reviewed ? 'Reviewed' : row.changed ? 'Changed since review' : row.issues.length ? row.issues.join(' · ') : 'Needs review';
    delete row.content; delete row.duplicateKey;
  }));
  const accounts = new Map();
  for (const row of rows) {
    const account = row.account.trim(); if (!accounts.has(account)) accounts.set(account, { account, rows: 0, reviewed: 0, missingAmounts: 0, knownCents: 0n });
    const summary = accounts.get(account); summary.rows++; if (row.reviewed) summary.reviewed++;
    if (row.amount_cents === null) summary.missingAmounts++; else summary.knownCents += BigInt(row.amount_cents);
  }
  return { rows, accounts: [...accounts.values()].sort((a, b) => a.account.localeCompare(b.account)).map(row => ({ ...row, knownCents: row.knownCents.toString() })), total: rows.length, reviewed: rows.filter(r => r.reviewed).length, pending: rows.filter(r => !r.reviewed).length,
    missingAmounts: rows.filter(r => r.amount_cents === null).length, possibleDuplicates: rows.filter(r => r.issues.includes('Possible duplicate')).length };
}
export async function markRowsReviewed(snapshot, review, selected, note = '', { at = new Date().toISOString() } = {}) {
  if (typeof at !== 'string' || !Number.isFinite(Date.parse(at)) || new Date(at).toISOString() !== at) throw Error('Invalid review time.');
  if (!Array.isArray(selected) || !selected.length || selected.length > 5000 || selected.some(row => !Number.isInteger(row?.row) || !hashPattern.test(row.fingerprint ?? '')) || new Set(selected.map(row => row.row)).size !== selected.length || typeof note !== 'string' || note.length > 1000) throw Error('Choose current rows and a review note of at most 1,000 characters.');
  const projection = await projectReview(snapshot, review), rows = new Map(projection.rows.map(row => [row.row, row])), next = structuredClone(review);
  const decisions = new Map(next.decisions.map(d => [d.row, d]));
  for (const { row: rowId, fingerprint } of selected) {
    const row = rows.get(rowId); if (!row) throw Error('A selected row is no longer in this review.');
    if (row.fingerprint !== fingerprint) throw Error('A selected row changed. Refresh and review its current values.');
    const blocked = row.issues.filter(issue => issue !== 'Possible duplicate');
    if (blocked.length) throw Error(`${row.address}: ${blocked.join(', ')}.`);
    if (row.issues.includes('Possible duplicate') && !note.trim()) throw Error('Add a note explaining the possible duplicate before marking it reviewed.');
    decisions.set(rowId, { row: rowId, fingerprint: row.fingerprint, note: note.trim(), at });
  }
  next.decisions = [...decisions.values()].sort((a, b) => a.row - b.row); return validateReview(next, snapshot);
}
export function reopenReviewRows(review, selected) {
  const keys = new Set(selected); return { ...review, decisions: review.decisions.filter(d => !keys.has(d.row)) };
}
export async function withReviewSheet(snapshot, review) {
  if (!review) return snapshot;
  const projection = await projectReview(snapshot, review);
  if (snapshot.sheetOrder.length > 30) throw Error('Leave room for the review decisions and account summary sheets before exporting.');
  const next = structuredClone(snapshot), names = new Set(Object.values(next.sheets).map(s => s.name.toLowerCase()));
  const values = [['Worksheet cell', 'Description', 'Amount', 'Account', 'Review status', 'Issues', 'Note', 'Reviewed at'],
    ...projection.rows.map(row => [row.address, row.description, row.amount_cents === null ? '' : row.amount_cents / 100, row.account, row.reviewed ? 'Reviewed' : 'Needs review', row.issues.join('; '), row.note, row.reviewedAt ?? ''])];
  const add = (title, data) => {
    const id = 'review-' + crypto.randomUUID(); let name = title; for (let n = 2; names.has(name.toLowerCase()); n++) name = `${title} (${n})`; names.add(name.toLowerCase());
    next.sheetOrder.push(id); next.sheets[id] = { id, name, rowCount: Math.max(100, data.length), columnCount: 16,
      cellData: Object.fromEntries(data.map((row, r) => [r, Object.fromEntries(row.map((v, c) => [c, { v, t: typeof v === 'number' ? 2 : 1 }]))])) };
  };
  add('Review decisions', values);
  add('Review accounts', [['Account', 'Rows', 'Reviewed rows', 'Known amount', 'Missing amounts'], ...projection.accounts.map(row => {
    const total = Number(row.knownCents); if (!Number.isSafeInteger(total)) throw Error('Account total exceeds the supported amount range.');
    return [row.account || 'Unassigned', row.rows, row.reviewed, total / 100, row.missingAmounts];
  })]);
  return validateWorkbook(next);
}
