import { test, expect } from 'bun:test';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { booksReviewWorkbook } from './books-review-native.mjs';
import { importWorkbook } from '../workbook-import.mjs';
import { exportWorkbookSnapshot } from '../workbook-model.mjs';
import { calculateWorkbook } from '../workbook-calculation.mjs';
import { workbookReviewValue, readWorkbookReview } from '../workbook-record.mjs';
import { createReview, detectReview, projectReview, markRowsReviewed, reopenReviewRows, withReviewSheet } from '../workbook-review.mjs';

async function fixture() {
  const bytes = booksReviewWorkbook({ transactions: [
    { date: '2026-10-01', description: 'Rent', amount_cents: -125050, amount_text: '-1250.50', account: 'Office*', suggested: 'Office*', review: false, confidence: 0.98, line: 2 },
    { date: '2026-10-02', description: 'Supplies', amount_cents: -2749, amount_text: '-27.49', account: 'Office*', suggested: 'Office*', review: false, confidence: 0.95, line: 3 },
    { date: '2026-10-03', description: 'Unclear receipt', amount_cents: null, amount_text: 'pending', account: '', suggested: 'Office*', review: true, confidence: null, line: 4 },
  ] });
  const imported = await importWorkbook(bytes, { name: 'Books source.xlsx' });
  return { ...imported, review: detectReview(imported.snapshot) };
}
const select = (projection, ...rows) => projection.rows.filter(row => rows.includes(row.row)).map(({ row, fingerprint }) => ({ row, fingerprint }));

test('Books review detects its columns and retains missing amounts and exact account totals', async () => {
  const { snapshot, review } = await fixture(), projected = await projectReview(snapshot, review);
  expect(review.table).toMatchObject({ sheetId: 'sheet-0', headerRow: 0, columns: { description: 1, amount: 2, account: 4 } });
  expect(projected).toMatchObject({ total: 3, reviewed: 0, pending: 3, missingAmounts: 1 });
  expect(projected.rows[2].amount_cents).toBeNull();
  expect(projected.rows[2].issues).toEqual(['Amount needs review', 'Missing account']);
  expect(projected.accounts.find(a => a.account === 'Office*').knownCents).toBe('-127799');
  expect(projected.accounts.find(a => a.account === '').missingAmounts).toBe(1);
});

test('content changes invalidate decisions and stale selections cannot mark changed rows reviewed', async () => {
  const { snapshot, review } = await fixture(), projected = await projectReview(snapshot, review);
  const accepted = await markRowsReviewed(snapshot, review, select(projected, 1), 'Compared with source');
  expect((await projectReview(snapshot, accepted)).reviewed).toBe(1);
  const edited = structuredClone(snapshot); edited.sheets['sheet-0'].cellData[1][4].v = 'Facilities';
  expect((await projectReview(edited, accepted)).rows[0]).toMatchObject({ reviewed: false, changed: true, status: 'Changed since review' });
  await expect(markRowsReviewed(edited, accepted, select(projected, 1))).rejects.toThrow('changed');
  expect(review.decisions).toEqual([]);
  expect(reopenReviewRows(accepted, [1]).decisions).toEqual([]);
});

test('duplicate group changes invalidate earlier review and a duplicate needs an explanation', async () => {
  const { snapshot, review } = await fixture(), projected = await projectReview(snapshot, review);
  const accepted = await markRowsReviewed(snapshot, review, select(projected, 1));
  snapshot.sheets['sheet-0'].cellData[4] = structuredClone(snapshot.sheets['sheet-0'].cellData[1]);
  const duplicates = await projectReview(snapshot, accepted);
  expect(duplicates.possibleDuplicates).toBe(2);
  expect(duplicates.rows[0]).toMatchObject({ reviewed: false, changed: true });
  await expect(markRowsReviewed(snapshot, accepted, select(duplicates, 1, 4), '  ')).rejects.toThrow('note');
  const confirmed = await markRowsReviewed(snapshot, accepted, select(duplicates, 1, 4), 'Separate invoices verified');
  expect((await projectReview(snapshot, confirmed)).reviewed).toBe(2);
  delete snapshot.sheets['sheet-0'].cellData[4];
  expect((await projectReview(snapshot, confirmed)).rows[0].reviewed).toBe(false);
});

test('batch review refuses missing data and formula errors without partially accepting rows', async () => {
  const { snapshot, review } = await fixture();
  let projected = await projectReview(snapshot, review);
  await expect(markRowsReviewed(snapshot, review, select(projected, 1, 3))).rejects.toThrow('Amount needs review');
  expect(review.decisions).toEqual([]);
  snapshot.sheets['sheet-0'].cellData[1][12] = { v: '#DIV/0!', f: '=1/0', t: 4 };
  projected = await projectReview(snapshot, review);
  await expect(markRowsReviewed(snapshot, review, select(projected, 1))).rejects.toThrow('Formula error');
  snapshot.sheets['sheet-0'].cellData[2][2].v = 1.005;
  expect((await projectReview(snapshot, review)).rows[1].amount_cents).toBeNull();
});

test('arbitrary layouts can be mapped and ambiguous headers require an explicit choice', async () => {
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, XLSX.utils.aoa_to_sheet([['Operations'], [], ['GL', 'Value', 'Memo'], ['Travel', -12.34, 'Train']]), 'Ledger');
  const { snapshot } = await importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx' }));
  expect(detectReview(snapshot)).toBeNull();
  const table = { sheetId: 'sheet-0', headerRow: 2, columns: { account: 0, amount: 1, description: 2 } };
  const projection = await projectReview(snapshot, createReview(table, snapshot));
  expect(projection.rows[0]).toMatchObject({ address: 'Ledger!B4', amount_cents: -1234, account: 'Travel', description: 'Train' });
  expect(() => createReview({ ...table, columns: { account: 0, amount: 1, description: 1 } }, snapshot)).toThrow('different');
  const books = await fixture();
  books.snapshot.sheets['sheet-0'].cellData[0][12] = { v: 'Amount', t: 1 };
  expect(detectReview(books.snapshot)).toBeNull();
});

test('review decisions, unchanged source bytes and imported cell baseline survive record exchange', async () => {
  const { snapshot, review, source } = await fixture(), originalBytes = await source.original.arrayBuffer(), originalBaseline = await source.baseline.text();
  snapshot.sheets['sheet-0'].cellData[1][4].v = 'Facilities';
  const accepted = await markRowsReviewed(snapshot, review, select(await projectReview(snapshot, review), 1));
  const retained = await readWorkbookReview({ key: 'finance-workbook:decisions', revision: 2, value: workbookReviewValue(snapshot, source, accepted) });
  expect((await projectReview(retained.snapshot, retained.review)).reviewed).toBe(1);
  expect(await retained.source.original.arrayBuffer()).toEqual(originalBytes);
  expect(await retained.source.baseline.text()).toBe(originalBaseline);
  expect(JSON.parse(originalBaseline).sheets['sheet-0'].cellData[1][4].v).toBe('Office*');
});

test('UTF-8 review size is bounded before saving a body that the reader would refuse', async () => {
  const { snapshot, review, source } = await fixture(); snapshot.sheets['sheet-0'].rowCount = 4000;
  review.decisions = Array.from({ length: 3000 }, (_, n) => ({ row: n + 1, fingerprint: 'a'.repeat(64), note: '界'.repeat(1000), at: '2026-10-17T00:00:00.000Z' }));
  expect(() => workbookReviewValue(snapshot, source, review)).toThrow('8 MiB');
  review.decisions = review.decisions.slice(0, 1);
  const kept = workbookReviewValue(snapshot, source, review);
  expect((await readWorkbookReview({ key: 'finance-workbook:unicode', revision: 1, value: kept })).review.decisions[0].note).toBe(review.decisions[0].note);
});

test('export includes current decisions and all accounts, retaining formulas and literal notes', async () => {
  const { snapshot, review } = await fixture();
  snapshot.sheets['sheet-0'].cellData[1][4].v = 'Facilities';
  const calculated = (await calculateWorkbook(snapshot)).snapshot;
  const accepted = await markRowsReviewed(calculated, review, select(await projectReview(calculated, review), 1), '=HYPERLINK("https://example.com")');
  const enhanced = await withReviewSheet(calculated, accepted);
  expect(calculated.sheetOrder).toHaveLength(4);
  const exported = XLSX.read(exportWorkbookSnapshot(enhanced), { type: 'array' });
  const decisions = exported.Sheets['Review decisions'], accounts = XLSX.utils.sheet_to_json(exported.Sheets['Review accounts']);
  expect(decisions.E2.v).toBe('Reviewed'); expect(decisions.G2.t).toBe('s'); expect(decisions.G2.f).toBeUndefined();
  expect(decisions.G2.v).toBe('=HYPERLINK("https://example.com")');
  expect(decisions.C4?.v ?? '').toBe('');
  expect(accounts.find(a => a.Account === 'Facilities')).toMatchObject({ Rows: 1, 'Reviewed rows': 1, 'Known amount': -1250.5 });
  expect(accounts.find(a => a.Account === 'Unassigned')['Missing amounts']).toBe(1);
  expect(exported.Sheets.Accounts.C2.f).toContain('SUMIF');
  expect(exported.Sheets.Accounts.C2.v).toBeCloseTo(-27.49, 8);
  const reimported = await importWorkbook(exportWorkbookSnapshot(enhanced));
  expect((await calculateWorkbook(reimported.snapshot)).errors).toEqual([]);
});
