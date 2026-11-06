import { test, expect } from 'bun:test';
import { Univer, LocaleType } from '@univerjs/core';
import { FUniver } from '@univerjs/core/facade';
import { UniverSheetsPlugin } from '@univerjs/sheets';
import sheetsEnUS from '@univerjs/sheets/locale/en-US';
import '@univerjs/sheets/facade';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { workbookSnapshot } from '../workbook-model.mjs';
import { detectReview, setReviewCell } from '../workbook-review.mjs';
import { calculateWorkbook } from '../workbook-calculation.mjs';

test('review edits through the real Univer facade clear the replaced formula, retain formatting and recalculate dependents', async () => {
  const book = XLSX.utils.book_new(), sheet = XLSX.utils.aoa_to_sheet([
    ['Description', 'Amount', 'Account', 'Tax'],
    ['Supplies', { t: 'n', v: 20, f: '10*2', z: '#,##0.00' }, 'Office', { t: 'n', v: 2, f: 'B2*0.1' }],
  ]);
  XLSX.utils.book_append_sheet(book, sheet, 'Transactions');
  const snapshot = workbookSnapshot(book), review = detectReview(snapshot);
  const univer = new Univer({ locale: LocaleType.EN_US, locales: { enUS: sheetsEnUS } }); univer.registerPlugin(UniverSheetsPlugin);
  try {
    const workbook = FUniver.newAPI(univer).createWorkbook(snapshot), before = workbook.save().sheets['sheet-0'].cellData[1][1];
    setReviewCell(workbook, review, 1, 'amount', 35.5);
    setReviewCell(workbook, review, 1, 'account', '=literal account');
    const after = workbook.save(), cells = after.sheets['sheet-0'].cellData[1];
    expect(cells[1].v).toBe(35.5); expect(cells[1].f ?? null).toBeNull(); expect(cells[1].s).toEqual(before.s);
    expect(cells[2].v).toBe('=literal account'); expect(cells[2].f ?? null).toBeNull();
    expect(() => setReviewCell(workbook, review, 0, 'amount', 10)).toThrow('transaction');
    expect(() => setReviewCell(workbook, review, 1, 'amount', 1.005)).toThrow('amount');
    expect(() => setReviewCell(workbook, review, 1, 'description', 'Changed')).toThrow('transaction');
    const calculated = await calculateWorkbook(after);
    expect(calculated.errors).toEqual([]); expect(calculated.snapshot.sheets['sheet-0'].cellData[1][3].v).toBeCloseTo(3.55, 8);
  } finally { univer.dispose(); }
});
