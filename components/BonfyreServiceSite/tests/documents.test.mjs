import { test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, copyFileSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { prepareDocuments, readCollection, createDocumentHandler, createDocumentOwner } from '../document-workspace.mjs';

const dir = mkdtempSync(join(tmpdir(),'document-workspace-test-'));
const file = join(dir,'documents.json'), db = join(dir,'.documents/index.db');
const sample = resolve(import.meta.dir,'../../../packs/aurekai-work-samples/household/documents.json');
let owner, handler;
const origin = 'http://127.0.0.1:4177', token = 'test-session';
beforeAll(async () => {
  copyFileSync(sample,file); owner = await prepareDocuments(dir);
  handler = createDocumentHandler(owner,{origin,token});
},60000);
afterAll(() => rmSync(dir,{recursive:true,force:true}));
const request = (path,body,headers={}) => handler(new Request(origin+path,{
  method:body ? 'POST':'GET',headers:{'X-Document-Session':token,...(body?{'Content-Type':'application/json'}:{}),...headers},
  ...(body?{body:JSON.stringify(body)}:{})}));

test('native search returns the retained source, unsupported phrases stay empty', async () => {
  const found = await owner.search('Where is the dishwasher receipt?');
  expect(found.results[0].document_id).toBe('appliance-receipts');
  expect(found.results[0].paragraph).toBe(2);
  const source = owner.source(found.results[0].document_id);
  expect(source.text).toContain(found.results[0].text);
  expect(found).not.toHaveProperty('answer');
  expect((await owner.search('submarine insurance policy','phrase')).results).toEqual([]);
  await expect(owner.search('','phrase')).rejects.toThrow('1 to 500');
},60000);

test('local API rejects missing session, foreign origins, rebinding and invalid requests',async () => {
  expect((await request('/api/documents',null,{'X-Document-Session':''})).status).toBe(403);
  expect((await request('/api/documents',null,{'Origin':'https://example.invalid'})).status).toBe(403);
  expect((await request('/api/documents',null,{'Host':'example.invalid:4177'})).status).toBe(403);
  expect((await request('/api/search',{query:''})).status).toBe(400);
  expect((await request('/api/search',{query:'a'.repeat(501)})).status).toBe(400);
  expect((await request('/api/search',{query:'a'.repeat(9000)})).status).toBe(413);
  expect((await request('/api/remove',{id:'appliance-receipts',digest:'wrong'})).status).toBe(400);
  expect(owner.list().documents.every(d=>d.indexed)).toBe(true);
  const page = await request('/'); expect(page.headers.get('content-security-policy')).toContain("connect-src 'self'");
  expect((await request('/documents.mjs')).status).toBe(200);
  expect((await request('/store.mjs')).status).toBe(200);
});

test('failed removal retains the source and changed collections are refused',async () => {
  const collection = readCollection(file);
  const failing = createDocumentOwner({file,db,collection,run:async()=>{throw Error('Native operation unavailable');}});
  const doc = failing.source('garden-project');
  await expect(failing.remove(doc.id,doc.digest)).rejects.toThrow('unavailable');
  expect(failing.source(doc.id).text).toBe(doc.text);
  const original = readFileSync(file); writeFileSync(file,original.toString()+' ');
  expect(()=>owner.source(doc.id)).toThrow('changed');
  await expect(prepareDocuments(dir)).rejects.toThrow('changed');
  writeFileSync(file,original);
});

test('all passages disappear from both search modes and stay removed after reopening',async () => {
  const source = owner.source('appliance-receipts');
  const removed = await owner.remove(source.id,source.digest);
  expect(removed.removed.deleted_artifacts).toBe(source.passages.length);
  expect(removed.removed.deleted_vectors).toBe(source.passages.length);
  owner = await prepareDocuments(dir);
  expect(owner.list().documents.find(d=>d.id===source.id).indexed).toBe(false);
  expect(()=>owner.source(source.id)).toThrow('no longer');
  expect((await owner.search('dishwasher','phrase')).results).toEqual([]);
  expect((await owner.search('Where is the dishwasher receipt?')).results.some(r=>r.document_id===source.id)).toBe(false);
  expect(readFileSync(file,'utf8')).toContain('dishwasher');
},60000);
