import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { workbookMaterial } from '../workbook-material.mjs';
import { renderSite } from '../build.mjs';
import { checkLinks } from '../navigation.mjs';

test('existing site renderer publishes a workbook workspace with its actual JS and CSS bundle', async () => {
  const snapshot = { id: 'example', name: 'Example review', sheetOrder: ['sheet'], sheets: {
    sheet: { id: 'sheet', name: 'Budget', rowCount: 100, columnCount: 16, cellData: { 0: { 0: { v: 20, t: 2 }, 1: { f: '=A1*2', t: 2 } } } }
  } };
  const material = await workbookMaterial({ key: 'finance-workbook:example', snapshot });
  expect(Object.keys(material.files).sort()).toEqual(['ag-grid-LICENSE.txt', 'ag-grid-community.min.js', 'review.json', 'workbook-client.css', 'workbook-client.js', 'workbook-scenario-worker.js']);
  expect(JSON.parse(material.files['review.json']).snapshot).toEqual(snapshot);
  const content = JSON.parse(readFileSync(new URL('./fixtures/sample-lawn/content.json', import.meta.url)));
  content.pages = [{ path: 'financial-review', title: 'Financial review', menu: 'Review', layout: 'workspace',
    sections: [{ kind: 'workbook', id: 'finance', title: 'October review', data: '/data/finance/' }] }];
  const page = renderSite(content).pages.find(page => page.path === 'financial-review');
  expect(page.html).toContain('data-workbook-config="/data/finance/review.json"');
  expect(page.html).toContain('/data/finance/workbook-client.js');
  expect(page.html).toContain('/data/finance/workbook-client.css');
});

test('navigation checks anchors without treating workbook stylesheets as page routes', () => {
  expect(() => checkLinks([{ path: '', html: '<link rel="stylesheet" href="/data/review/workbook-client.css"><a href="/">Home</a>' }])).not.toThrow();
  expect(() => checkLinks([{ path: '', html: '<a href="/missing/">Missing</a>' }])).toThrow('Dead site link');
});
