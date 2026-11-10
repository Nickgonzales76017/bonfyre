#!/usr/bin/env bun
// Structural contract plus executable source-selection tests; no retired JSON export.
import {readFileSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import vm from 'node:vm';
const page=readFileSync(join(dirname(fileURLToPath(import.meta.url)),'../link.html'),'utf8');
for(const token of ['Find · Understand · Distribute · Connect','/foreign/twins','K.foreignEdges()','K.observeForeign',
'openSourcePublication','/apps/source-publication.mjs','requestId:recovering[1]','source:B(selectedSource).source',
'subject:selectedSource.subject_id','Review collection + World','source-comparison','source-destinations',
'catalogue_generation','snapshot_mismatch','Retry next page','Inspect known authorization endpoints'])
 if(!page.includes(token))throw Error('Missing source-workbench contract: '+token);
for(const old of ["localStorage.setItem(key,JSON.stringify(x))","foreign-twin-plan.json","async function cmsDraft("])
 if(page.includes(old))throw Error('Retired browser plan or two-write publisher returned: '+old);
for(const script of page.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))
 if(script[1].trim())new vm.Script(script[1],{filename:'link.html'});
console.log('ForeignTwin carrier and native publication contract pass.');
