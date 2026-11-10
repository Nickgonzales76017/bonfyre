import { test, expect } from 'bun:test';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { booksReviewWorkbook } from './books-review-native.mjs';
import { workbookSnapshot, exportWorkbookSnapshot, validateWorkbook } from '../workbook-model.mjs';
import { calculateWorkbook } from '../workbook-calculation.mjs';

const example = () => {
  const report = { transactions: [
    { date: '2026-10-01', description: 'Rent', amount_cents: -125050, amount_text: '-1250.50', account: 'Office*', suggested: 'Office*', review: false, confidence: 0.98, line: 2 },
    { date: '2026-10-02', description: 'Supplies', amount_cents: -2749, amount_text: '-27.49', account: 'Office*', suggested: 'Office*', review: false, confidence: 0.95, line: 3 },
    { date: '2026-10-03', description: 'Unclear receipt', amount_cents: null, amount_text: 'pending', account: '', suggested: 'Office*', review: true, confidence: null, line: 4 },
  ] };
  return workbookSnapshot(XLSX.read(booksReviewWorkbook(report), { type: 'buffer', cellNF: true, cellStyles: true }), { id: 'review-test' });
};

test('real Univer calculation independently verifies Books export formulas and invalidates stale caches', async () => {
  const snapshot = example();
  snapshot.sheets['sheet-1'].cellData[1][2].v = 999999;
  const result = await calculateWorkbook(snapshot);
  expect(result.errors).toEqual([]);
  const accounts = result.snapshot.sheets['sheet-1'].cellData;
  expect(accounts[1][1].v).toBe(2);
  expect(accounts[1][2].v).toBeCloseTo(-1277.99, 8);
  expect(accounts[1][3].v).toBe(0);
  expect(snapshot.sheets['sheet-1'].cellData[1][2].v).toBe(999999);
});

test('editing an assumption recalculates dependent cells and survives XLSX export and reopen', async () => {
  const snapshot = example();
  snapshot.sheets['sheet-0'].cellData[1][2].v = -1000;
  const result = await calculateWorkbook(snapshot);
  expect(result.errors).toEqual([]);
  expect(result.snapshot.sheets['sheet-1'].cellData[1][2].v).toBeCloseTo(-1027.49, 8);
  const exported = XLSX.read(exportWorkbookSnapshot(result.snapshot), { type: 'array', cellNF: true });
  expect(exported.Sheets.Accounts.C2.f).toContain('SUMIF');
  expect(exported.Sheets.Accounts.C2.v).toBeCloseTo(-1027.49, 8);
  expect(exported.Sheets.Transactions.C2.z).toContain('#,##0.00');
  expect(exported.Sheets.Transactions.D4.v).toBe('pending');
  const reopened = await calculateWorkbook(workbookSnapshot(exported, { id: 'reopened' }));
  expect(reopened.snapshot.sheets['sheet-1'].cellData[1][2].v).toBeCloseTo(-1027.49, 8);
});

test('formula failures are visible and circular references cannot reuse cached success', async () => {
  const broken = example();
  broken.sheets['sheet-1'].cellData[1][2].f = '=1/0';
  const result = await calculateWorkbook(broken);
  expect(result.errors[0]).toMatchObject({ sheet: 'Accounts', cell: 'C2', error: '#DIV/0!' });
  const circular = example();
  circular.sheets['sheet-1'].cellData[1][2] = { f: '=C2+1', v: 1234, t: 2 };
  await expect(calculateWorkbook(circular)).rejects.toThrow('Circular');
});

test('unsupported functions, external files and excessive coordinates are refused', () => {
  for (const formula of ['=WEBSERVICE("https://example.com")', '=SEQUENCE(1000000)', '=[Other.xlsx]Sheet1!A1', "='[Other.xlsx]Sheet1'!A1", '=SUM(A1:A900000)']) {
    const snapshot = example(); snapshot.sheets['sheet-1'].cellData[1][2].f = formula;
    expect(() => validateWorkbook(snapshot)).toThrow();
  }
});

test('workbooks without formulas return without waiting for a calculation event', async () => {
  const snapshot = example();
  for (const sheet of Object.values(snapshot.sheets)) for (const row of Object.values(sheet.cellData)) for (const cell of Object.values(row)) delete cell.f;
  const result = await calculateWorkbook(snapshot);
  expect(result.formulas).toBe(0);
  expect(result.snapshot).toEqual(snapshot);
});

test('calculation deadline terminates the worker without accepting cached values', async () => {
  await expect(calculateWorkbook(example(), { timeout: 1 })).rejects.toThrow('deadline');
});
