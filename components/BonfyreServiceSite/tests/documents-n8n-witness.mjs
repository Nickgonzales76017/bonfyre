// Opt-in runtime witness; not yet executed successfully. Requires pinned n8n.
import { mkdtempSync, mkdirSync, copyFileSync, writeFileSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { prepareDocuments, createDocumentHandler } from '../document-workspace.mjs';
import { householdWorkflow } from '../../../packs/aurekai-work-samples/household/n8n/workflow.mjs';

const repo=resolve(import.meta.dir,'../../..');
const installation=process.env.BONFYRE_N8N_INSTALL||'/private/tmp/bonfyre-n8n-household-20261008';
const cli=join(installation,'node_modules/n8n/bin/n8n');
assert.ok(existsSync(cli),'Install the pinned local n8n runtime first.');
const version=JSON.parse(readFileSync(join(installation,'node_modules/n8n/package.json'))).version;
assert.equal(version,'2.42.5','This witness pins the executed n8n version.');
const dir=mkdtempSync(join(tmpdir(),'household-n8n-test-'));
const proof=join(repo,'packs/aurekai-work-samples/site/design/verification-documents-local');
mkdirSync(proof,{recursive:true});
copyFileSync(join(repo,'packs/aurekai-work-samples/household/documents.json'),join(dir,'documents.json'));
const env={...process.env,N8N_USER_FOLDER:join(dir,'n8n-state'),N8N_DIAGNOSTICS_ENABLED:'false',
  N8N_VERSION_NOTIFICATIONS_ENABLED:'false',N8N_TEMPLATES_ENABLED:'false',N8N_COMMUNITY_PACKAGES_ENABLED:'false',
  N8N_LICENSE_AUTO_RENEW_ENABLED:'false',N8N_LOG_LEVEL:'error',N8N_RUNNERS_BROKER_LISTEN_ADDRESS:'127.0.0.1'};
const checks=[],cases=[];let server;
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
async function run(args) {
  const child=Bun.spawn(['node',cli,...args],{env,stdout:'pipe',stderr:'pipe'});
  const timer=setTimeout(()=>child.kill(),90000);
  try { const [stdout,stderr,code]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);return {stdout,stderr,code}; }
  finally {clearTimeout(timer);}
}
async function execute(options) {
  const workflow=householdWorkflow({id:randomUUID().replaceAll('-','').slice(0,16),...options});
  const path=join(dir,'workflow.json');writeFileSync(path,JSON.stringify(workflow));
  const imported=await run(['import:workflow','--input='+path]);
  assert.equal(imported.code,0,'n8n import failed: '+imported.stderr+' '+imported.stdout);
  const result=await run(['execute','--id='+workflow.id,'--rawOutput']);
  let execution;
  // The pinned CLI prints indented JSON, sometimes after startup diagnostics.
  for(const match of result.stdout.matchAll(/^\{/gm)) { try {
    const parsed=JSON.parse(result.stdout.slice(match.index,result.stdout.lastIndexOf('}')+1));
    if(parsed.data?.resultData)execution=parsed;
  }catch{} }
  if(result.code===0&&!execution)throw Error('No n8n execution result: '+result.stdout.slice(-4000)+result.stderr.slice(-2000));
  const outcome=execution?.data.resultData;
  const final=outcome?.runData?.['Source review']?.[0]?.data?.main?.[0]?.[0]?.json||outcome?.runData?.['No matching passage']?.[0]?.data?.main?.[0]?.[0]?.json;
  cases.push({query:options.query,mode:options.mode||'semantic',exit_code:result.code,last_node:outcome?.lastNodeExecuted??null,output:final??null});
  return {...result,execution,final};
}
try {
  const owner=await prepareDocuments(dir);
  let handler;
  server=Bun.serve({hostname:'127.0.0.1',port:0,idleTimeout:90,fetch:request=>handler(request)});
  const origin=`http://127.0.0.1:${server.port}`;handler=createDocumentHandler(owner,{origin});
  const good=await execute({origin,query:'Where is the dishwasher receipt?'});
  assert.equal(good.code,0,good.stderr+' '+good.stdout.slice(-3000));
  check('Actual n8n retrieves the expected document',good.final?.document_id==='appliance-receipts');
  check('Source identity and original text survive the workflow',good.final.source_digest===owner.source('appliance-receipts').digest&&good.final.original_text===owner.source('appliance-receipts').text&&good.final.paragraph===2);
  check('Output requires human review and contains no cloud handoff',good.final.review_required===true&&good.final.cloud_handoff===false&&!('answer' in good.final));
  const none=await execute({origin,query:'submarine insurance policy',mode:'phrase'});
  check('Unsupported exact phrase reaches the empty-result branch',none.code===0&&none.final?.status==='no_matching_passage');
  const invalid=await execute({origin,query:'',mode:'phrase'});
  check('Invalid input fails execution instead of becoming an empty answer',invalid.code!==0&&!invalid.final);
  const doc=owner.source('appliance-receipts');await owner.remove(doc.id,doc.digest);
  const removed=await execute({origin,query:'dishwasher',mode:'phrase'});
  check('A new n8n process sees persistent document removal',removed.code===0&&removed.final?.status==='no_matching_passage');
  server.stop(true);server=null;
  const unavailable=await execute({origin,query:'Where is the dishwasher receipt?'});
  check('Unavailable local service fails without an answer',unavailable.code!==0&&!unavailable.final);
  writeFileSync(join(proof,'n8n-verification.json'),JSON.stringify({at:new Date().toISOString(),version,checks,cases,synthetic:true,workflow_published:false,credentials_imported:false},null,2)+'\n');
} finally {server?.stop(true);rmSync(dir,{recursive:true,force:true});}
