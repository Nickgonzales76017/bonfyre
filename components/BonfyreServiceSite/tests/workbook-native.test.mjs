import { test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import * as XLSX from '../../BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
import { workspaceFixture } from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import { projectFinanceWorkspaceSetup, publishSetups } from '../../../scripts/publish_experience_setups.mjs';
import { readWorkspaceFile } from '../../../scripts/materialize_workspace_package.mjs';
import { workbookPackage } from '../workbook-package.mjs';
import { importWorkbook } from '../workbook-import.mjs';
import { workbookReviewValue, readWorkbookReview } from '../workbook-record.mjs';
import {validateCrmSource,estimatedCents,crmFinanceDraft,validateCrmFinanceDraft} from '../../../integrations/browser/crm-finance-handoff.mjs';
import { calculateWorkbook } from '../workbook-calculation.mjs';
import { exportWorkbookSnapshot } from '../workbook-model.mjs';
import { detectReview, projectReview, markRowsReviewed, withReviewSheet } from '../workbook-review.mjs';

let temp, pack, f, binding, original, snapshot;
const verified = new Set();
const key = 'finance-workbook:native-review', packName = 'experience.finance-workbooks.workspace';
const previous = Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE', 'BONFYRE_WORKSPACE_PACKAGES'].map(k => [k, process.env[k]]));
beforeAll(async () => {
  temp = mkdtempSync(join(tmpdir(), 'finance-workbook-')); const packageRoot = join(temp, 'packages');
  pack = await workbookPackage({ packageRoot });
  process.env.BONFYRE_WORKSPACE_RELEASE = pack.id; process.env.BONFYRE_WORKSPACE_PACKAGES = packageRoot;
  f = await workspaceFixture();
  const definition = { records: projectFinanceWorkspaceSetup(pack.id), binary: f.cms, db: f.db, schemas: join(f.home, 'schemas'), apply: true };
  expect(publishSetups(definition).created).toBe(7); expect(publishSetups(definition).created).toBe(0);
  binding = await f.install(packName, 'institution:workspace-team');
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, XLSX.utils.aoa_to_sheet([['Amount', 'Multiplier', 'Total'], [120.5, 2, { t: 'n', f: 'A2*B2', v: 9999 }]]), 'Review');
  original = await importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx', compression: true }), { name: 'Native review.xlsx' });
  snapshot = (await calculateWorkbook(original.snapshot)).snapshot;
}, 60000);
afterAll(async () => {
  if (verified.size === 6 && process.env.WORKBOOK_NATIVE_PROOF) {
    const out = process.env.WORKBOOK_NATIVE_PROOF; mkdirSync(out, { recursive: true });
    writeFileSync(join(out, 'native.json'), JSON.stringify({ package: pack.id, family: pack.family, browser_verified: false,
      scope: 'isolated native Auth, CMS and WebAddress; no live accounts or provider calls',
      binaries: Object.fromEntries(['web', 'cms', 'auth'].map(name => [name, createHash('sha256').update(readFileSync(f[name])).digest('hex')])),
      checks: [...verified] }, null, 2) + '\n');
  }
  await f?.close(); if (temp) rmSync(temp, { recursive: true, force: true });
  for (const [key, value] of Object.entries(previous)) if (value === undefined) delete process.env[key]; else process.env[key] = value;
});
test('the installed finance workspace serves its editor, import worker, styles and native continuation contract', async () => {
  const response = await f.raw(binding.path), html = await response.text();
  expect(response.status).toBe(200); expect(html).toContain(`content="${binding.endpoint}"`);
  expect(html).not.toContain('__WORKSPACE__'); expect(html).toContain('data-workbook-open');
  const script = await f.raw(binding.path + 'workbook-workspace-client.js'); expect(script.status).toBe(200);
  expect(await script.text()).toContain('../bridge/workspace-store.mjs');
  for (const path of ['workbook-workspace-client.css', 'workbook-import-worker.js', 'ag-grid-community.min.js', 'ag-grid-LICENSE.txt']) expect((await f.raw(binding.path + path)).status).toBe(200);
  const manifest = await readWorkspaceFile(process.env.BONFYRE_WORKSPACE_PACKAGES, pack.id, 'CONTINUATIONS.tsv');
  expect(manifest.toString()).toContain('workbook=$key');
  verified.add('installed editor assets, import worker and native continuation manifest');
});
test('two authorized people reopen, edit, recalculate and export the same organization workbook', async () => {
  const owner = f.store(binding), writer = f.store(binding, f.writer);
  const saved = await owner.saveLocal(key, workbookReviewValue(snapshot, original.source), 0); expect(saved.revision).toBe(1);
  const read = await readWorkbookReview((await writer.readLocal(key)).find(row => row.key === key));
  expect(read.snapshot.sheets['sheet-0'].cellData[1][2].v).toBe(241);
  expect(await read.source.original.arrayBuffer()).toEqual(await original.source.original.arrayBuffer());
  read.snapshot.sheets['sheet-0'].cellData[1][1].v = 3;
  const revised = (await calculateWorkbook(read.snapshot)).snapshot;
  expect(revised.sheets['sheet-0'].cellData[1][2].v).toBe(361.5);
  await writer.saveLocal(key, workbookReviewValue(revised, read.source), read.revision);
  const reopened = await readWorkbookReview((await f.store(binding, f.reader).readLocal(key))[0]); expect(reopened.revision).toBe(2);
  const exchanged = XLSX.read(exportWorkbookSnapshot(reopened.snapshot), { type: 'array' });
  expect(exchanged.Sheets.Review.C2.f).toBe('A2*B2'); expect(exchanged.Sheets.Review.C2.v).toBe(361.5);
  const views = await f.raw(`/apps/${binding.id}/_views?` + new URLSearchParams({ key, revision: '2', release: pack.id }));
  expect(views.status).toBe(200);
  const target = (await views.text()).match(/data-compatible-view href="([^"]+)"/)?.[1].replaceAll('&amp;', '&');
  expect(target).toBeDefined();
  const continued = await f.raw(target); expect(continued.status).toBe(303);
  const address = new URL(continued.headers.get('location'), f.base);
  expect(address.pathname).toBe(binding.path); expect(address.searchParams.get('workbook')).toBe(key);
  verified.add('shared organization review, formula recalculation, XLSX round trip and exact native continuation');
});
test('large workbook bodies use native files without increasing metadata limits', async () => {
  const large = structuredClone(snapshot), cells = large.sheets['sheet-0'].cellData;
  large.sheets['sheet-0'].rowCount = 1200;
  for (let r = 2; r < 1002; r++) cells[r] = Object.fromEntries(Array.from({ length: 10 }, (_, c) => [c, { v: `Evidence ${r}:${c} ` + 'x'.repeat(32), t: 1 }]));
  const value = workbookReviewValue(large, original.source); expect(value.snapshot.size).toBeGreaterThan(256 * 1024);
  await f.store(binding).saveLocal('finance-workbook:large', value, 0);
  const reopened = await readWorkbookReview((await f.store(binding, f.writer).readLocal('finance-workbook:large'))[0]);
  expect(reopened.snapshot.sheets['sheet-0'].cellData[1001][9].v).toBe(cells[1001][9].v);
  expect(f.sql.query('SELECT MAX(length(body)) AS bytes FROM _workspace_body').get().bytes).toBeLessThan(256 * 1024);
  verified.add('large workbook body retained through existing immutable file storage');
});
test('organization members continue row decisions and source evidence through the native owners', async () => {
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, XLSX.utils.aoa_to_sheet([
    ['Description', 'Amount', 'Account'], ['Train', -12.34, 'Travel'], ['Supplies', -35.5, 'Office'], ['Receipt pending', '', ''],
  ]), 'Transactions');
  const imported = await importWorkbook(XLSX.write(book, { type: 'buffer', bookType: 'xlsx' }));
  const reviewKey = 'finance-workbook:row-decisions', review = detectReview(imported.snapshot);
  const projected = await projectReview(imported.snapshot, review);
  const selected = row => [{ row: row.row, fingerprint: row.fingerprint }];
  const accepted = await markRowsReviewed(imported.snapshot, review, selected(projected.rows[0]), 'Source checked');
  await f.store(binding).saveLocal(reviewKey, workbookReviewValue(imported.snapshot, imported.source, accepted), 0);
  const writer = f.store(binding, f.writer), read = await readWorkbookReview((await writer.readLocal(reviewKey))[0]);
  expect(read.review).toEqual(accepted);
  expect(await read.source.baseline.text()).toBe(await imported.source.baseline.text());
  const next = await markRowsReviewed(read.snapshot, read.review, selected((await projectReview(read.snapshot, read.review)).rows[1]));
  await writer.saveLocal(reviewKey, workbookReviewValue(read.snapshot, read.source, next), read.revision);
  const reopened = await readWorkbookReview((await f.store(binding, f.reader).readLocal(reviewKey))[0]);
  expect(reopened.revision).toBe(2); expect((await projectReview(reopened.snapshot, reopened.review)).reviewed).toBe(2);
  const exported = XLSX.read(exportWorkbookSnapshot(await withReviewSheet(reopened.snapshot, reopened.review)), { type: 'array' });
  expect(exported.Sheets['Review decisions'].E2.v).toBe('Reviewed');
  expect(exported.Sheets['Review decisions'].E3.v).toBe('Reviewed');
  expect(exported.Sheets['Review decisions'].E4.v).toBe('Needs review');
  expect(await reopened.source.original.arrayBuffer()).toEqual(await imported.source.original.arrayBuffer());
  reopened.snapshot.sheets['sheet-0'].cellData[1][1].v = -13;
  await f.store(binding).saveLocal(reviewKey, workbookReviewValue(reopened.snapshot, reopened.source, reopened.review), reopened.revision);
  const edited = await readWorkbookReview((await writer.readLocal(reviewKey))[0]);
  expect((await projectReview(edited.snapshot, edited.review)).rows[0]).toMatchObject({ reviewed: false, changed: true });
  await expect(writer.saveLocal(reviewKey, workbookReviewValue(read.snapshot, read.source, next), read.revision)).rejects.toMatchObject({ status: 409 });
  verified.add('row decisions, original source and baseline survive shared native revisions; edits invalidate decisions and stale saves refuse');
});
test('a retained CRM opportunity can enter a reviewed native Finance draft without economic effects', async () => {
  const sourceKey='opportunity:feed:'+'a'.repeat(64),sourceRevision=2,crmBinding='ws-'+'c'.repeat(40);
  const relationship={schema:'bonfyre.relationship.v1',id:'b'.repeat(64),state:'observed',label:'Lithic: Senior Software Engineer, Product Platform',actor:{state:'unresolved',subject:null},
    source:{itemRef:'feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa',snapshot:'d'.repeat(64),publicPath:'/o/feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa'},opportunity:{kind:'job-opening',stage:'observed'}};
  const source={key:sourceKey,revision:sourceRevision,value:{state:{relationship}}};
  expect(validateCrmSource(source,sourceKey,sourceRevision)).toBe(relationship);
  expect(()=>validateCrmSource(source,sourceKey,sourceRevision+1)).toThrow('changed');
  expect(estimatedCents('1200.25')).toBe(120025);
  expect(()=>estimatedCents('NaN')).toThrow();
  const draft=crmFinanceDraft(crmBinding,sourceKey,sourceRevision,relationship,'1200.25','Estimate subject to review');
  const reviewKey='crm-finance:'+'e'.repeat(64);
  expect(draft).toMatchObject({schema:'bonfyre.crm-finance-review.v1',status:'review-draft',estimate:{amount_cents:120025,currency:'USD',approval:'not-requested'},effects:{invoice:false,ledger_posted:false,payment:false,contract:false,commitment:false}});
  expect((await f.store(binding).saveLocal(reviewKey,draft,0)).revision).toBe(1);
  const reopened=(await f.store(binding,f.writer).readLocal(reviewKey))[0];
  expect(validateCrmFinanceDraft(reopened,reviewKey).source).toMatchObject({key:sourceKey,revision:sourceRevision,binding:crmBinding,snapshot:relationship.source.snapshot});
  await expect(f.store(binding).saveLocal(reviewKey,draft,0)).rejects.toMatchObject({status:409});
  await expect(f.store(binding,f.other).readLocal('crm-finance:')).rejects.toMatchObject({status:403});
});
test('stale writes, readers, unrelated accounts and foreign record families cannot overwrite reviews', async () => {
  const owner = f.store(binding), current = (await owner.readLocal(key))[0];
  await expect(owner.saveLocal(key, workbookReviewValue(snapshot, original.source), current.revision - 1)).rejects.toMatchObject({ status: 409 });
  await expect(f.store(binding, f.reader).saveLocal(key, workbookReviewValue(snapshot), current.revision)).rejects.toMatchObject({ status: 403 });
  expect((await f.raw(binding.path, f.other)).status).toBe(403);
  await expect(owner.saveLocal('draft:catalogue:foreign', {}, 0)).rejects.toThrow();
  const personal = await f.install(packName, 'self', f.other);
  expect(await f.store(personal, f.other).readLocal('finance-workbook:')).toEqual([]);
  expect((await owner.readLocal(key))[0].revision).toBe(current.revision);
  f.grant(f.writer.id, 'workspace-team', 'read');
  await expect(f.store(binding, f.writer).saveLocal(key, workbookReviewValue(snapshot), current.revision)).rejects.toMatchObject({ status: 403 });
  verified.add('stale write, read role, revoked write role, unrelated account and foreign family refusal');
});
test('deactivation removes access and reactivation restores the retained review', async () => {
  await f.action('deactivate', packName, 'institution:workspace-team');
  expect((await f.raw(binding.path)).status).toBe(404);
  await f.action('activate', packName, 'institution:workspace-team');
  expect((await f.store(binding).readLocal(key))[0].revision).toBe(2);
  verified.add('deactivation removes access and reactivation preserves the saved revision');
});
