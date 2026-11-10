#!/usr/bin/env bun
import {existsSync,readFileSync,writeFileSync} from 'node:fs';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url)),role=/^[a-z][a-z0-9-]{0,63}$/,pack=/^experience\.[a-z0-9][a-z0-9.-]{0,94}$/,grammarSourceDefault=join(here,'../cmd/BonfyreInteractionGrammar/src/main.c');
const route=value=>typeof value==='string'&&value.length>0&&value.length<=192&&value[0]==='/'&&!value.includes('..')&&!/\s/.test(value);
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const canonical=value=>Array.isArray(value)?value.map(canonical):object(value)?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const bounded=(value,max)=>typeof value==='string'&&value.length>0&&value.length<=max&&!/[\x00-\x1f\x7f]/.test(value);
const fail=message=>{throw Error('Program service policy: '+message);};
export function interactionGrammarApplications(sourcePath=grammarSourceDefault){
 const source=readFileSync(sourcePath,'utf8'),start=source.indexOf('static const Grammar GRAMMARS[] = {'),end=source.indexOf('\n};',start);
 if(start<0||end<0)fail('native InteractionGrammar catalog is unavailable.');
 const rows=new Map(),pattern=/\{"[^"]+","[^"]+","[^"]+","([^"]+)","[^"]+","[^"]+","[^"]+","[^"]+","([^"]+)"\}/g,grammarRows=[...source.slice(start,end).matchAll(pattern)];
 if(grammarRows.length!==20)fail('native InteractionGrammar catalog must contain exactly 20 grammars.');
 for(const match of grammarRows){const application=match[1],owner=match[2];if(!role.test(application)||!bounded(owner,96))fail('native InteractionGrammar row is malformed.');const owners=rows.get(application)||new Set();owners.add(owner);rows.set(application,owners);}
 return Object.fromEntries([...rows].sort(([a],[b])=>a.localeCompare(b)).map(([application,owners])=>[application,[...owners].sort()]));
}
export function programServicePolicy(policy,{grammarSource=grammarSourceDefault}={}){
 if(!object(policy))fail('ProgramFabric policy is missing.');
 const packs=policy.service_packs,aliases=policy.continuation_aliases,continuations=policy.continuations,basis=policy.interaction_grammar_basis;
 if(!object(packs)||Object.keys(packs).length>64)fail('service_packs is invalid.');
 for(const [name,value] of Object.entries(packs))if(!/^[A-Za-z][A-Za-z0-9]{0,63}$/.test(name)||!pack.test(value))fail('invalid workspace pack declaration.');
 const knownPacks=new Set(Object.values(packs));
 if(!object(aliases)||Object.keys(aliases).length>128)fail('continuation_aliases is invalid.');
 if(!object(continuations)||Object.keys(continuations).length>128)fail('continuations is invalid.');
 for(const [name,target] of Object.entries(aliases))if(!role.test(name)||!role.test(target)||!Object.hasOwn(continuations,target))fail('alias target is not a declared continuation.');
 const nativeBasis=interactionGrammarApplications(grammarSource);
 if(!object(basis)||Object.keys(basis).length!==Object.keys(nativeBasis).length)fail('interaction_grammar_basis does not cover the native grammar applications.');
 for(const [application,owners] of Object.entries(nativeBasis)){
  const entry=basis[application];
  if(!object(entry)||!['typed','pending_exact_owner'].includes(entry.state)||!Array.isArray(entry.owner_families)||entry.owner_families.length<1||entry.owner_families.length>16)fail('interaction grammar basis entry is invalid.');
  if(JSON.stringify([...entry.owner_families].sort())!==JSON.stringify(owners))fail('interaction grammar owner families drifted for '+application+'.');
  if(entry.state==='typed'){if(!role.test(entry.continuation)||!Object.hasOwn(continuations,entry.continuation)||Object.keys(entry).some(key=>!['state','continuation','owner_families'].includes(key)))fail('typed interaction grammar basis is invalid.');}
  else if(Object.hasOwn(entry,'continuation')||Object.keys(entry).some(key=>!['state','owner_families'].includes(key)))fail('pending interaction grammar basis is invalid.');
 }
 for(const application of Object.keys(basis))if(!Object.hasOwn(nativeBasis,application))fail('interaction grammar basis contains an unknown application.');
 for(const [name,specs] of Object.entries(continuations)){
  if(!role.test(name)||!Array.isArray(specs)||!specs.length||specs.length>16)fail('invalid continuation role.');
  const identities=new Set();
  for(const spec of specs){
   if(!object(spec)||!['workspace','surface','account'].includes(spec.kind)||!bounded(spec.label,128)||!bounded(spec.description,600))fail('invalid continuation specification.');
   let identity;
   if(spec.kind==='workspace'){if(!pack.test(spec.pack)||!knownPacks.has(spec.pack)||Object.keys(spec).some(key=>!['kind','pack','label','description'].includes(key)))fail('workspace continuation is invalid.');identity='workspace:'+spec.pack;}
   else if(spec.kind==='surface'){if(!role.test(spec.id)||!route(spec.href)||Object.keys(spec).some(key=>!['kind','id','label','href','description'].includes(key)))fail('surface continuation is invalid.');identity='surface:'+spec.id;}
   else {if(!role.test(spec.id)||!route(spec.path)||Object.keys(spec).some(key=>!['kind','id','label','path','description'].includes(key)))fail('account continuation is invalid.');identity='account:'+spec.id;}
   if(identities.has(identity))fail('duplicate continuation identity for '+name+'.');identities.add(identity);
  }
 }
 return canonical({packs,aliases,continuations,grammar_basis:basis});
}
export function readProgramServicePolicy(cuePath=join(here,'program-fabric.cue'),{cue='cue',grammarSource=grammarSourceDefault}={}){
 const result=Bun.spawnSync([cue,'export',cuePath,'-e','programFabric','--out','json'],{stdout:'pipe',stderr:'pipe'});
 if(result.exitCode!==0)throw Error(result.stderr.toString()||'CUE export failed.');
 let value;try{value=JSON.parse(result.stdout.toString());}catch{fail('CUE export did not return JSON.');}
 return programServicePolicy(value,{grammarSource});
}
export function renderProgramServicePolicy(value,{grammarSource=grammarSourceDefault}={}){const selected=value&&Object.hasOwn(value,'service_packs')?programServicePolicy(value,{grammarSource}):canonical(value);return '// Generated by scripts/project-program-service-policy.mjs from scripts/program-fabric.cue. DO NOT EDIT.\nexport const programServicePolicy = '+JSON.stringify(selected,null,2)+';\n';}
export function projectProgramServicePolicy({cuePath=join(here,'program-fabric.cue'),outPath=join(here,'../integrations/browser/program-service-policy.mjs'),cue='cue',grammarSource=grammarSourceDefault,write=false,check=false}={}){
 if(write===check)fail('choose exactly one of write or check.');
 const rendered=renderProgramServicePolicy(readProgramServicePolicy(cuePath,{cue,grammarSource}));
 if(write){writeFileSync(outPath,rendered);return{state:'written',outPath};}
 if(!existsSync(outPath)||readFileSync(outPath,'utf8')!==rendered)fail('generated browser policy is stale; run project-program-service-policy.mjs --write.');
 return{state:'current',outPath};
}
const arg=name=>{const i=process.argv.indexOf(name);return i>=0?process.argv[i+1]:'';};
if(import.meta.main){try{const write=process.argv.includes('--write'),check=process.argv.includes('--check'),result=projectProgramServicePolicy({write,check,cuePath:resolve(arg('--cue')||join(here,'program-fabric.cue')),outPath:resolve(arg('--out')||join(here,'../integrations/browser/program-service-policy.mjs')),cue:arg('--cue-binary')||'cue',grammarSource:resolve(arg('--grammar-source')||grammarSourceDefault)});console.log((result.state==='written'?'WROTE ':'CURRENT ')+result.outPath);}catch(error){console.error(error.message);process.exit(1);}}
