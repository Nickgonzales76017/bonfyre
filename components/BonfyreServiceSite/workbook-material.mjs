import { basename, join } from 'node:path';
import { readFile } from 'node:fs/promises';
import { validateWorkbook } from './workbook-model.mjs';

export async function workbookMaterial({ key, snapshot, source = null }) {
  validateWorkbook(snapshot);
  if (!/^finance-workbook:[a-zA-Z0-9_-]{1,100}$/.test(key)) throw Error('Invalid workbook record identity.');
  const build = await Bun.build({ entrypoints: ['workbook-client.mjs','workbook-scenario-worker.mjs'].map(name=>join(import.meta.dir,name)), target: 'browser', minify: true,
    external: ['/apps/workspace-store.mjs'] });
  if (!build.success) throw new AggregateError(build.logs, 'Workbook editor could not build.');
  const files = { 'review.json': JSON.stringify({ key, snapshot, source }) };
  for (const output of build.outputs) files[basename(output.path)] = new Uint8Array(await output.arrayBuffer());
  for (const [name, source] of [['ag-grid-community.min.js', 'ag-grid-community.min.js'], ['ag-grid-LICENSE.txt', 'LICENSE.txt']]) files[name] = new Uint8Array(await readFile(join(import.meta.dir, 'vendor/ag-grid-community-36.2.0', source)));
  return { files };
}
