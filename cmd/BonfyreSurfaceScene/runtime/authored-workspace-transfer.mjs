// A reviewed copy between existing native destinations. This module owns only
// the transient plan; each destination keeps its own records and authority.
import {createAuthoredWorkspace,validateAuthoredWorkspace,productionPrefix} from './authored-workspace.mjs';
const storyFields=['id','title','epic','reach','impact','confidence','effort','release','requires','criteria','evidence','researchTitle','origin'];
const incomingPrefix='draft:instruments:incoming:',hashPattern=/^[a-f0-9]{64}$/;
const requireValue=(ok,message)=>{if(!ok)throw Error(message);};
const revision=n=>Number.isSafeInteger(n)&&n>0&&n<2147483647;
const coordinate=s=>typeof s==='string'&&/^[A-Za-z0-9:._-]{1,240}$/.test(s);
const fields={research:['kind','title','sources','findings','selectedSource','selectedFinding','tab'],product:['kind','title','stories','releases','selected'],traces:['kind','title','text','mapping','selectedCase','selectedEvent'],schema:['kind','title','bytes','selected','positions','acceptedSQL'],checks:['kind','title','contract','tables','selected']};
const select=(value,names)=>Object.fromEntries(names.filter(k=>Object.hasOwn(value,k)).map(k=>[k,structuredClone(value[k])]));
function targetIdentity(target){
  requireValue(target&&typeof target.context==='string'&&/^(self|[A-Za-z0-9][A-Za-z0-9:._-]{0,199})$/.test(target.context)&&/^ws-[a-f0-9]{40}$/.test(target.id)&&hashPattern.test(target.release)&&target.entry==='instruments/production/','Choose a currently admitted Production Studio destination.');
  return select(target,['context','id','release','entry']);
}
// Conservative upper bounds for the existing MessagePack value and batch
// contracts. Files, ArrayBuffers and typed views of at least 64 KiB use native
// asset references, matching the workspace-value carrier.
function boundedSize(value){
  let nodes=0,assets=0;const utf8=new TextEncoder();
  function walk(v,depth=0,pathBytes=0){
    requireValue(depth<=30&&++nodes<=45000,'The production material is too deeply nested.');
    if(typeof SharedArrayBuffer!=='undefined'&&(v instanceof SharedArrayBuffer||ArrayBuffer.isView(v)&&v.buffer instanceof SharedArrayBuffer))throw Error('Shared mutable memory cannot enter a reviewed production.');
    if(v instanceof ArrayBuffer||v instanceof Blob||v instanceof Uint8Array&&v.byteLength>=64*1024){const size=v instanceof Blob?v.size:v.byteLength;requireValue(size<=24*1024*1024&&++assets<=64,'A production asset exceeds the native file limit.');return 2048+pathBytes;}
    if(v instanceof Uint8Array)return v.byteLength+5;
    if(v===null||typeof v==='boolean')return 1;
    if(typeof v==='number'){requireValue(Number.isFinite(v),'Production numbers must be finite.');return 9;}
    if(typeof v==='string')return utf8.encode(v).length+5;
    if(Array.isArray(v)){requireValue(v.length<=20000,'The production contains too many items.');return 5+v.reduce((n,x)=>n+walk(x,depth+1,pathBytes+9),0);}
    requireValue(v&&[Object.prototype,null].includes(Object.getPrototypeOf(v)),'Unsupported production value.');
    const keys=Object.keys(v),descriptors=Object.getOwnPropertyDescriptors(v);requireValue(keys.length<=4096&&Reflect.ownKeys(v).length===keys.length,'Unsupported production properties.');
    let size=5;for(const key of keys){const bytes=utf8.encode(key).length;requireValue(bytes<=512&&Object.hasOwn(descriptors[key],'value'),'Unsupported production property.');size+=bytes+5+walk(descriptors[key].value,depth+1,pathBytes+bytes+5);}return size;
  }
  const size=walk(value)+32;requireValue(size<=256*1024,'This production record exceeds the native value limit.');return size;
}
async function hash(value,digestValue){
  requireValue(typeof digestValue==='function','Use the native byte-complete value digest.');
  boundedSize(value);const result=await digestValue(value);requireValue(hashPattern.test(result),'The native value digest is invalid.');return result;
}
async function exact(store,key,expected=null){
  requireValue(typeof store?.readVersion==='function','Use the existing native record store.');
  const result=await store.readVersion(key),row=result&&structuredClone(result);
  requireValue(row?.key===key&&!row.deleted&&revision(row.revision)&&row.value,'A required saved record is missing: '+key);
  requireValue(expected===null||row.revision===expected,'A source revision changed. Save and prepare the production again: '+key);return row;
}
async function evidence(source,candidate,validateResearchEvidence){
  requireValue(typeof validateResearchEvidence==='function'&&Array.isArray(candidate?.evidence)&&candidate.evidence.length>0&&candidate.evidence.length<=100,'Review the adopted research evidence before copying this production.');
  requireValue(await validateResearchEvidence(structuredClone(source),structuredClone(candidate))===true,'A copied opportunity no longer matches its retained research evidence.');
}
async function planDigest(plan,digestValue){
  const changes=[];let bytes=128;
  requireValue(Array.isArray(plan.changes)&&plan.changes.length>0&&plan.changes.length<=1000,'The transfer exceeds the native batch limit.');
  for(const change of plan.changes){bytes+=boundedSize(change.value)+new TextEncoder().encode(change.key).length+32;changes.push({key:change.key,expectedRevision:change.expectedRevision,digest:await hash(change.value,digestValue)});}
  requireValue(bytes<=512*1024,'The complete production exceeds the native batch limit.');
  return hash({schema:plan.schema,sourceEndpoint:plan.sourceEndpoint,sourceRelease:plan.sourceRelease,target:plan.target,productionKey:plan.productionKey,productionDigest:await hash(plan.production,digestValue),sources:plan.sources,summary:plan.summary,changes},digestValue);
}
async function checkSources(plan,sourceStore,digestValue){
  for(const source of plan.sources){const row=await exact(sourceStore,source.key,source.revision);requireValue(await hash(row.value,digestValue)===source.digest,'A source changed after review. Prepare the production again: '+source.key);}
}
export async function prepareProductionTransfer({sourceEndpoint,sourceRelease,productionKey,productionRevision,sourceStore,target,digestValue,validateResearchEvidence,validateInstrument}){
  // Capture caller-owned coordinates before the first asynchronous operation.
  target=targetIdentity(structuredClone(target));
  requireValue(/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(sourceEndpoint)&&hashPattern.test(sourceRelease)&&coordinate(productionKey)&&productionKey.startsWith(productionPrefix)&&revision(productionRevision),'Use the exact saved production and release.');
  const root=await exact(sourceStore,productionKey,productionRevision),original=validateAuthoredWorkspace(root.value,{saved:true});
  requireValue(productionKey===productionPrefix+original.id,'The saved production identity changed.');
  const sources=[],rows=new Map(),incoming=new Map();
  async function retain(row){requireValue(rows.size<1000,'The source closure exceeds the native batch limit.');rows.set(row.key,row);sources.push({key:row.key,revision:row.revision,digest:await hash(row.value,digestValue)});}
  await retain(root);
  for(const panel of original.panels){const row=await exact(sourceStore,panel.recordKey,panel.revision);requireValue(row.value.kind===panel.kind,'A bound editor returned a different kind.');if(panel.kind==='checks')requireValue(typeof validateInstrument==='function'&&await validateInstrument('checks',structuredClone(row.value))===true,'Review retained Data Checks inputs with the current native validator before copying.');await retain(row);}
  const next=createAuthoredWorkspace(original.title),panelIds=new Map(),recordKeys=new Map(),storyMaps=new Map(),findingMaps=new Map();
  next.description=original.description;next.layout=original.layout;next.appearance=original.appearance;
  if(original.recoveryOrigin)next.recoveryOrigin=structuredClone(original.recoveryOrigin);
  for(const panel of original.panels){const id=crypto.randomUUID(),key='draft:instruments:'+panel.kind+':'+crypto.randomUUID();panelIds.set(panel.id,id);recordKeys.set(panel.recordKey,key);next.panels.push({id,kind:panel.kind,label:panel.label,role:panel.role,recordKey:key,revision:1});}
  next.connections=original.connections.map(edge=>({from:panelIds.get(edge.from),to:panelIds.get(edge.to),operation:edge.operation}));next.focus=panelIds.get(original.focus)??null;
  for(const panel of original.panels.filter(p=>p.kind==='research')){
    const research=rows.get(panel.recordKey).value,ids=new Map();
    requireValue(Array.isArray(research.findings),'The research findings are missing.');
    for(const finding of research.findings){requireValue(typeof finding.id==='string'&&finding.id&&!ids.has(finding.id),'Review repeated or missing research finding identities before copying.');ids.set(finding.id,crypto.randomUUID());}
    requireValue(research.selectedFinding===null||research.selectedFinding===undefined||ids.has(research.selectedFinding),'The selected research finding is missing.');findingMaps.set(panel.recordKey,ids);
  }
  for(const panel of original.panels.filter(p=>p.kind==='product')){
    const product=rows.get(panel.recordKey).value;requireValue(Array.isArray(product.stories)&&product.stories.length<=1000,'The product map has unsupported stories.');
    const ids=new Map();storyMaps.set(panel.recordKey,ids);
    for(const story of product.stories){
      requireValue(typeof story.id==='string'&&story.id&&!ids.has(story.id)&&Array.isArray(story.requires),'Review the product map identities and dependencies before copying.');
      let id=crypto.randomUUID();
      if(story.origin){
        const origin=story.origin;requireValue(coordinate(origin.record)&&recordKeys.has(origin.record)&&rows.get(origin.record).value.kind==='research','Add the adopted opportunity’s research record to this production before preparing its copy.');
        requireValue(typeof origin.incoming==='string'&&/^draft:instruments:incoming:[A-Za-z0-9._-]{1,180}$/.test(origin.incoming)&&revision(origin.incomingRevision)&&revision(origin.revision),'The adopted opportunity has incomplete native origins.');
        let kept=incoming.get(origin.incoming);
        if(!kept){
          const row=await exact(sourceStore,origin.incoming,origin.incomingRevision);await retain(row);
          const oldId=row.value.candidate?.id,findingId=typeof oldId==='string'&&oldId.startsWith('research-')?oldId.slice('research-'.length):null,newFinding=findingMaps.get(origin.record)?.get(findingId);
          requireValue(newFinding,'Restore the incoming opportunity’s original research finding before preparing this copy.');
          kept={row,id:'research-'+newFinding};incoming.set(origin.incoming,kept);
        }
        const value=kept.row.value,source=rows.get(origin.record);
        requireValue(kept.row.revision===origin.incomingRevision&&value.sourceRecord===origin.record&&value.sourceRevision===origin.revision&&source.revision>=value.sourceRevision&&value.candidate?.id===story.id&&origin.incoming===incomingPrefix+story.id,'An adopted opportunity differs from its retained incoming record.');
        await evidence(source.value,value.candidate,validateResearchEvidence);await evidence(source.value,story,validateResearchEvidence);id=kept.id;
      }
      ids.set(story.id,id);
    }
    for(const story of product.stories)requireValue(story.requires.every(id=>ids.has(id)&&id!==story.id)&&new Set(story.requires).size===story.requires.length,'A product dependency is missing or repeated.');
    requireValue(product.selected===null||product.selected===undefined||ids.has(product.selected),'The product selection is missing.');
  }
  const changes=[];
  for(const panel of original.panels){
    const value=select(rows.get(panel.recordKey).value,fields[panel.kind]);
    if(panel.kind==='research'){
      const ids=findingMaps.get(panel.recordKey);value.findings=value.findings.map(finding=>({...finding,id:ids.get(finding.id)}));
      if(value.selectedFinding!==null&&value.selectedFinding!==undefined)value.selectedFinding=ids.get(value.selectedFinding);
    }
    if(panel.kind==='product'){
      const ids=storyMaps.get(panel.recordKey);value.stories=value.stories.map(story=>{
        const result={...select(story,storyFields),id:ids.get(story.id),requires:story.requires.map(id=>ids.get(id))};
        if(story.origin){const kept=incoming.get(story.origin.incoming);result.origin={record:recordKeys.get(story.origin.record),revision:1,incoming:incomingPrefix+kept.id,incomingRevision:1};}return result;
      });if(value.selected!==null&&value.selected!==undefined)value.selected=ids.get(value.selected);
    }
    changes.push({key:recordKeys.get(panel.recordKey),expectedRevision:0,value});
  }
  for(const kept of incoming.values()){
    const value=kept.row.value,candidate=select(value.candidate,storyFields);candidate.id=kept.id;
    // Incoming candidates normally have no dependencies. If authored dependencies
    // exist, every adopting map must agree on their receiving identities.
    requireValue(Array.isArray(candidate.requires),'The retained candidate dependencies are missing.');
    candidate.requires=candidate.requires.map(oldId=>{
      const mapped=[...storyMaps.entries()].filter(([key])=>rows.get(key).value.stories.some(s=>s.origin?.incoming===kept.row.key)).map(([,ids])=>ids.get(oldId));
      requireValue(mapped.length&&mapped.every(id=>id&&id===mapped[0]),'A candidate dependency cannot be resolved in its receiving maps.');return mapped[0];
    });
    // The incoming record owns source coordinates; only an adopted story owns origin.
    delete candidate.origin;
    changes.push({key:incomingPrefix+kept.id,expectedRevision:0,value:{candidate,sourceRecord:recordKeys.get(value.sourceRecord),sourceRevision:1}});
  }
  next.transferFrom={schema:'bonfyre.production-transfer-provenance.v1',endpoint:sourceEndpoint,release:sourceRelease,productionKey,productionRevision,records:structuredClone(sources)};
  validateAuthoredWorkspace(next,{saved:true});
  const targetKey=productionPrefix+next.id;changes.push({key:targetKey,expectedRevision:0,value:next});
  const plan={schema:'bonfyre.production-transfer.v1',sourceEndpoint,sourceRelease,target,productionKey:targetKey,production:next,sources,changes,summary:{title:next.title,panels:next.panels.map(p=>({kind:p.kind,label:p.label,role:p.role})),incomingCount:incoming.size,recordCount:changes.length,sourceProductionKey:productionKey,sourceProductionRevision:productionRevision}};
  plan.digest=await planDigest(plan,digestValue);
  // Sequential native reads are not an atomic cross-owner snapshot. Rechecking
  // catches movement during preparation; commit checks this reviewed snapshot again.
  await checkSources(plan,sourceStore,digestValue);return plan;
}
async function checkReceipt(plan,rows,digestValue){
  requireValue(Array.isArray(rows)&&rows.length===plan.changes.length,'The destination receipt is incomplete. Keep the review and reconcile before retrying.');
  for(const change of plan.changes){const matches=rows.filter(row=>row?.key===change.key);requireValue(matches.length===1&&matches[0].revision===1&&!matches[0].deleted&&await hash(matches[0].value,digestValue)===await hash(change.value,digestValue),'The destination content differs from the reviewed copy. Reconcile before retrying.');}
  return structuredClone(rows.find(row=>row.key===plan.productionKey));
}
async function retainedCopy(plan,targetStore,digestValue){
  const rows=[];for(const change of plan.changes){const row=await targetStore.readVersion(change.key);if(row)rows.push(structuredClone(row));}
  if(!rows.length)return null;
  requireValue(rows.length===plan.changes.length,'Only part of the destination copy exists. Reconcile it before preparing another copy.');return checkReceipt(plan,rows,digestValue);
}
export async function commitProductionTransfer(plan,{sourceStore,targetStore,verifyTarget,digestValue,validateInstrument}){
  // Isolate the reviewed bytes before hashing, callbacks or native reads can yield.
  const kept=structuredClone(plan);
  requireValue(kept?.schema==='bonfyre.production-transfer.v1'&&hashPattern.test(kept.digest)&&await planDigest(kept,digestValue)===kept.digest,'The reviewed copy changed. Prepare it again before writing.');
  const target=targetIdentity(kept.target);validateAuthoredWorkspace(kept.production,{saved:true});
  requireValue(typeof targetStore?.readVersion==='function'&&typeof targetStore?.saveLocalBatch==='function'&&typeof verifyTarget==='function','Use the admitted native destination and record store.');
  requireValue(kept.productionKey===productionPrefix+kept.production.id&&new Set(kept.changes.map(c=>c.key)).size===kept.changes.length&&kept.changes.every(c=>coordinate(c.key)&&c.expectedRevision===0&&!Object.hasOwn(c,'remove')&&!Object.hasOwn(c,'checkOnly')),'A production copy can only create its reviewed new records.');
  for(const panel of kept.production.panels.filter(p=>p.kind==='checks')){const change=kept.changes.find(c=>c.key===panel.recordKey);requireValue(change?.value?.kind==='checks'&&typeof validateInstrument==='function'&&await validateInstrument('checks',structuredClone(change.value))===true,'Review retained Data Checks inputs with the current native validator before copying.');}
  const prior=await retainedCopy(kept,targetStore,digestValue);if(prior)return prior;
  await checkSources(kept,sourceStore,digestValue);
  const admission=await verifyTarget(structuredClone(target));
  requireValue(admission===true||admission&&['context','id','release','entry'].every(key=>admission[key]===target[key]),'The selected destination or its write admission changed. Refresh the destination list.');
  try{return await checkReceipt(kept,await targetStore.saveLocalBatch(structuredClone(kept.changes)),digestValue);}
  catch(error){
    // An acknowledgement can disappear after the native atomic batch commits.
    // Only complete revision-one byte equality admits the retained result.
    const retained=await retainedCopy(kept,targetStore,digestValue);if(retained)return retained;throw error;
  }
}
