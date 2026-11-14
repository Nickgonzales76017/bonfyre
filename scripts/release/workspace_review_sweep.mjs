// Bounded review of existing workspace upgrade owners. Never applies, replays
// a write, or treats a local package as already served.
import {resolve,join} from 'node:path';
import {readFileSync,existsSync} from 'node:fs';
const root=resolve(import.meta.dir,'../..');
const plans=[
 ['connections','scripts/release/connection_studio_upgrade.mjs','review'],
 ['source-library','scripts/release/source_library_known_upgrade.mjs','review'],
 ['finance-crm','scripts/release/crm_finance_workspace_upgrade.mjs','review'],
 ['research','scripts/release/research_workspace_upgrade.mjs','review'],
 ['publication-picker','scripts/release/publication_picker_upgrade.mjs','review'],
 ['procurement-room','scripts/release/procurement_workspace_upgrade.mjs','review','room'],
 ['procurement-catalogue','scripts/release/procurement_workspace_upgrade.mjs','review','catalogue'],
 ['procurement-downstream-finance','scripts/release/procurement_downstream_workspace_upgrade.mjs','review','finance'],
 ['procurement-downstream-shipment','scripts/release/procurement_downstream_workspace_upgrade.mjs','review','shipment']
];
export function reviewCandidates({selection}={}){
 const names=selection?new Set(selection):null,results=[];
 for(const [name,script,...args] of plans){
  if(names&&!names.has(name))continue;
  if(!existsSync(join(root,script))){results.push({name,state:'missing_source'});continue;}
  const proc=Bun.spawnSync([process.execPath,script,...args],{
    cwd:root,stdout:'pipe',stderr:'pipe',timeout:140000,maxBuffer:1024*1024});
  const stdout=proc.stdout.toString().trim(),stderr=proc.stderr.toString().trim();
  let packet=null;
  try{packet=JSON.parse(stdout.split('\n').at(-1));}catch{}
  results.push({name,exit:proc.exitCode,signal:proc.signalCode??null,
   ...packet&&typeof packet==='object'?{
    state:packet.state||'unknown',candidate:packet.candidate||null,
    previousRelease:packet.previousRelease||null,version:packet.version||null,
    reason:packet.reason||null,journal:packet.journal||null}:{
    state:'error',reason:(stderr||stdout||'no native receipt').slice(-900)}});
 }
 return {schema:'bonfyre.workspace-upgrade-review-sweep.v1',total:results.length,
  reviewed:results.filter(r=>r.state==='reviewed'||r.state==='review_retained').length,
  installed:results.filter(r=>r.state==='already_verified').length,results};
}
if(import.meta.main){const selected=process.argv.slice(2);console.log(JSON.stringify(reviewCandidates({selection:selected.length?selected:null})));}
