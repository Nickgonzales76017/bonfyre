import { test, expect } from 'bun:test';
import { deflateRawSync } from 'node:zlib';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { importWorkbook } from '../workbook-import.mjs';
import { workbookReviewValue, readWorkbookReview } from '../workbook-record.mjs';
import { calculateWorkbook } from '../workbook-calculation.mjs';

export function fixtureWorkbook() {
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, XLSX.utils.aoa_to_sheet([['Amount', 'Multiplier', 'Total'], [120.5, 2, { t: 'n', f: 'A2*B2', v: 9999 }]]), 'Review');
  return XLSX.write(book, { type: 'buffer', bookType: 'xlsx', compression: true });
}
test('browser-compatible import retains original XLSX bytes and real calculation replaces cached results', async () => {
  const bytes = fixtureWorkbook(), imported = await importWorkbook(bytes, { name: 'Original review.xlsx' });
  expect(new Uint8Array(await imported.source.original.arrayBuffer())).toEqual(new Uint8Array(bytes));
  expect(imported.source.sha256).toMatch(/^[a-f0-9]{64}$/);
  const calculated = await calculateWorkbook(imported.snapshot);
  expect(calculated.snapshot.sheets['sheet-0'].cellData[1][2].v).toBe(241);
  const value = workbookReviewValue(calculated.snapshot, imported.source);
  expect(value.snapshot).toBeInstanceOf(Blob);
  const reopened = await readWorkbookReview({ key: 'finance-workbook:roundtrip', revision: 1, value });
  expect(reopened.snapshot).toEqual(calculated.snapshot);
  expect(reopened.source.sha256).toBe(imported.source.sha256);
});
test('old browser draft records remain readable, and calculation errors cannot become saved reviews', async () => {
  const { snapshot } = await importWorkbook(fixtureWorkbook());
  expect((await readWorkbookReview({ key: 'finance-workbook:old', revision: 3, value: { schema: 'bonfyre.finance-workbook.v1', snapshot } })).revision).toBe(3);
  snapshot.sheets['sheet-0'].cellData[1][2].v = '#NULL!';
  expect(() => workbookReviewValue(snapshot)).toThrow('calculation');
  await expect(readWorkbookReview({ key: 'draft:other', revision: 1, value: {} })).rejects.toThrow('identity');
});
test('the last supported row imports without adding rows past the editor limit', async () => {
  const book = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(book, { '!ref': 'A5000', A5000: { t: 'n', v: 1 } }, 'Edge');
  const result = await importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx' }));
  expect(result.snapshot.sheets['sheet-0'].rowCount).toBe(5000);
  expect(result.snapshot.sheets['sheet-0'].cellData[4999][0].v).toBe(1);
});
test('invalid files, unsupported formulas, merged cells and hidden macro content refuse import', async () => {
  await expect(importWorkbook(new Uint8Array(22))).rejects.toThrow('XLSX');
  const book = XLSX.utils.book_new(), sheet = XLSX.utils.aoa_to_sheet([[{ t: 'n', f: 'WEBSERVICE("https://example.com")', v: 1 }]]);
  XLSX.utils.book_append_sheet(book, sheet, 'Sheet1');
  await expect(importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx' }))).rejects.toThrow('not enabled');
  delete sheet.A1.f; sheet['!merges'] = [{ s: { r: 0, c: 0 }, e: { r: 0, c: 1 } }];
  await expect(importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx' }))).rejects.toThrow('Unmerge');
  delete sheet['!merges']; book.vbaraw = new Uint8Array([1, 2, 3]);
  await expect(importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsm' }))).rejects.toThrow('macro');
});
test('lying ZIP size cannot hide actual deflate expansion', async () => {
  const name = Buffer.from('xl/workbook.xml'), compressed = deflateRawSync(Buffer.alloc(1024 * 1024, 65));
  const local = Buffer.alloc(30), directory = Buffer.alloc(46), end = Buffer.alloc(22);
  local.writeUInt32LE(0x04034b50); local.writeUInt16LE(8, 8); local.writeUInt32LE(compressed.length, 18); local.writeUInt32LE(1, 22); local.writeUInt16LE(name.length, 26);
  directory.writeUInt32LE(0x02014b50); directory.writeUInt16LE(8, 10); directory.writeUInt32LE(compressed.length, 20); directory.writeUInt32LE(1, 24); directory.writeUInt16LE(name.length, 28);
  end.writeUInt32LE(0x06054b50); end.writeUInt16LE(1, 8); end.writeUInt16LE(1, 10); end.writeUInt32LE(directory.length + name.length, 12); end.writeUInt32LE(local.length + name.length + compressed.length, 16);
  await expect(importWorkbook(Buffer.concat([local, name, compressed, directory, name, end]))).rejects.toThrow('expansion');
});
