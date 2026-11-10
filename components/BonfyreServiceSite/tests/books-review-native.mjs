// The Books review workbook as the native program writes it (cmd/BonfyreDecide).
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const books = resolve(import.meta.dir, '../../../cmd/BonfyreDecide/bonfyre-books');
export function booksReviewWorkbook(report) {
  const dir = mkdtempSync(join(tmpdir(), 'books-review-'));
  try {
    writeFileSync(join(dir, 'report.json'), JSON.stringify(report));
    const run = spawnSync(books, ['review', '--report', join(dir, 'report.json'), '--out', join(dir, 'review.xlsx')], { encoding: 'utf8' });
    if (run.status !== 0) throw new Error(run.stderr.trim() || 'bonfyre-books review failed');
    return readFileSync(join(dir, 'review.xlsx'));
  } finally { rmSync(dir, { recursive: true, force: true }); }
}
