// Historical requirements are research material, never executable instructions.
// This adapter prepares the existing Research editor's records. It neither
// installs an application nor creates tasks, grants, findings or estimates.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {encode,decode} from '../integrations/browser/workspace-msgpack.mjs';

const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const valueHash=value=>hash(encode(value,{maxBytes:256*1024}));
const PREFIX='draft:instruments:research:history-';
const utf8=new TextEncoder();
export const recoveryBoundary='Historical source requirements for review. These records are not completed workflows, current provider claims, execution authority, or implementation findings. Existing native owners retain all effects and progress.';
const clientExcluded=item=>item.lane==='upper90'||['upper90-receiver','operating-circuit'].includes(item.kind)||/upper90|jrss|ai[-_ ]?loss/i.test(item.key+' '+item.source);

// Split only at complete Unicode characters, preferring line boundaries. Exact
// UTF-16 offsets and the full original digest allow reconstruction without loss.
export function splitRetainedText(body,maxBytes=64*1024){
  assert(typeof body==='string'&&body.length&&Number.isSafeInteger(maxBytes)&&maxBytes>=64);
  assert(!body.includes('\0'),'NUL in historical text');
  const parts=[];let start=0;
  while(start<body.length){
    let at=start,bytes=0,lastLine=-1;
    while(at<body.length){
      const point=body.codePointAt(at),s=String.fromCodePoint(point),n=utf8.encode(s).length;
      assert(!(point>=0xd800&&point<=0xdfff),'Invalid Unicode in historical text');
      if(bytes+n>maxBytes)break;
      bytes+=n;at+=s.length;if(s==='\n')lastLine=at;
    }
    if(at<body.length&&lastLine>start)at=lastLine;
    assert(at>start,'A source character exceeds the part bound');
    parts.push({start,end:at,body:body.slice(start,at)});start=at;
  }
  assert.equal(parts.map(p=>p.body).join(''),body);return parts;
}

function groupOf(item){
  if(item.kind==='skill')return 'skills / '+item.key.split('/')[0];
  if(item.kind==='historical-workflow')return 'historical brief / '+item.source;
  return item.kind;
}
function labelOf(group,first){
  if(group.startsWith('historical brief / '))return first.goal.title.split(' · ')[0];
  return group.replaceAll('-',' ');
}
function targetValue(target){
  assert(target&&/^ws-[a-f0-9]{40}$/.test(target.binding));
  assert(/^[a-f0-9]{64}$/.test(target.release));
  assert(/^user:[1-9][0-9]*$/.test(target.subject)&&target.scope==='self','Recovery intake is actor-private only');
  assert.equal(target.pack,'experience.connected-instruments.workspace');
  return {binding:target.binding,release:target.release,subject:target.subject,scope:'self',pack:target.pack};
}
function reviewedDigest(plan){
  return hash(encode([plan.schema,plan.target,plan.packets,plan.held,plan.obligations,plan.sourceParts,plan.groups,plan.entries.map(e=>[e.key,valueHash(e.value)])],{maxBytes:512*1024}));
}

export async function prepareHistoryResearch({packets,target,makeSource,maxSources=24,maxValueBytes=180*1024}){
  target=targetValue(target);assert(typeof makeSource==='function','Use the installed research source owner');
  assert(Array.isArray(packets)&&packets.length>0&&packets.length<=8);
  assert(Number.isSafeInteger(maxSources)&&maxSources>=1&&maxSources<=40);
  assert(Number.isSafeInteger(maxValueBytes)&&maxValueBytes>=8192&&maxValueBytes<=200*1024);
  const groups=new Map(),held=[],packetIds=[],seen=new Set(),contractEntries=[];
  let obligationCount=0,sourceParts=0;
  for(const supplied of packets){
    assert(supplied.bytes instanceof Uint8Array&&/^[a-f0-9]{64}$/.test(supplied.sha256));
    assert.equal(hash(supplied.bytes),supplied.sha256,'Retained packet digest changed');
    assert(!packetIds.includes(supplied.sha256),'Repeated historical packet');packetIds.push(supplied.sha256);
    const packet=decode(supplied.bytes,{maxBytes:16*1024*1024});
    assert(Array.isArray(packet.items)&&packet.items.length<=2048&&Array.isArray(packet.sources));
    for(const source of packet.sources)assert.equal(hash(source.text),source.sha256,'Retained original source changed');
    const contract=packet.contracts??packet.shared;
    assert(typeof contract==='string'&&hash(contract)===packet.contractHash,'The full shared contract is missing');
    const contractKey=PREFIX+'contract-'+packet.contractHash.slice(0,32);
    contractEntries.push({key:contractKey,parts:splitRetainedText(contract,Math.min(64*1024,Math.floor(maxValueBytes/2))),digest:packet.contractHash,packet:supplied.sha256,label:packet.contracts?'Original delivery contract':'Supplemental institutional contract'});
    for(const item of packet.items){
      assert(item.goal&&typeof item.goal.dedupe_key==='string'&&!seen.has(item.goal.dedupe_key),'Repeated historical obligation');seen.add(item.goal.dedupe_key);
      assert(typeof item.goal.details==='string'&&typeof item.goal.title==='string'&&/^[a-f0-9]{64}$/.test(item.source_sha256)&&/^[a-f0-9]{64}$/.test(item.unit_sha256));
      assert(packet.sources.some(s=>s.label===item.source&&s.sha256===item.source_sha256),'Original source identity is absent from its packet');
      if(clientExcluded(item)){held.push({key:item.goal.dedupe_key,kind:item.kind,reason:'Dedicated client scope retained with its current owner; no Aurekai connection or operation.'});continue;}
      const group=groupOf(item);if(!groups.has(group))groups.set(group,{label:labelOf(group,item),sources:[],contractKey});
      const fullDigest=hash(item.goal.details),parts=splitRetainedText(item.goal.details,Math.min(64*1024,Math.floor(maxValueBytes/2)));
      for(let part=0;part<parts.length;part++){
        const selected=parts[part],id='history-'+hash(item.goal.dedupe_key).slice(0,40)+'-'+part;
        const title=(item.goal.title+(parts.length>1?' · part '+(part+1)+'/'+parts.length:'')).slice(0,500);
        const source=await makeSource({id,title,body:selected.body,url:''});
        assert(source.id===id&&source.title===title&&source.body===selected.body&&source.sha256===hash(selected.body),'The research owner changed retained text');
        source.history={packet:supplied.sha256,goalKey:item.goal.dedupe_key,kind:item.kind,originalKey:item.key,source:item.source,sourceSha256:item.source_sha256,unitSha256:item.unit_sha256,detailSha256:fullDigest,detailStart:selected.start,detailEnd:selected.end,part,parts:parts.length,contractKey};
        groups.get(group).sources.push(source);sourceParts++;
      }
      obligationCount++;
    }
  }
  const entries=[];
  function makeRecord(group,label,sources,index,contractKeys=[]){
    const identity=hash(encode([group,sources.map(s=>s.id)])),key=PREFIX+identity.slice(0,40);
    const inputDigest=hash(encode(sources,{maxBytes:512*1024}));
    const value={kind:'research',title:('Recovered · '+label+' · '+String(index+1).padStart(2,'0')).slice(0,200),sources,findings:[],selectedSource:sources[0].id,selectedFinding:null,tab:'sources',recovery:{schema:'bonfyre.history-research.v1',group,inputDigest,contracts:contractKeys,boundary:recoveryBoundary}};
    return {key,value};
  }
  for(const[group,data]of groups){
    let batch=[],index=0;
    for(const source of data.sources){
      const proposed=makeRecord(group,data.label,[...batch,source],index,[data.contractKey]);
      if(batch.length&&(batch.length>=maxSources||encode([2,proposed.value,[]],{maxBytes:512*1024}).length>maxValueBytes)){entries.push(makeRecord(group,data.label,batch,index++,[data.contractKey]));batch=[];}
      batch.push(source);assert(encode([2,makeRecord(group,data.label,batch,index,[data.contractKey]).value,[]],{maxBytes:256*1024}).length<=maxValueBytes,'One source part exceeds the native record budget');
    }
    if(batch.length)entries.push(makeRecord(group,data.label,batch,index,[data.contractKey]));
  }
  for(const contract of contractEntries){
    for(let n=0;n<contract.parts.length;n++){
      const part=contract.parts[n],source=await makeSource({id:'contract-'+contract.digest+'-'+n,title:'Complete production contract · part '+(n+1)+'/'+contract.parts.length,body:part.body,url:''});
      assert.equal(source.sha256,hash(part.body));
      source.history={packet:contract.packet,detailSha256:contract.digest,detailStart:part.start,detailEnd:part.end,part:n,parts:contract.parts.length};
      const record=makeRecord('contract / '+contract.digest,contract.label,[source],n);record.key=contract.key+(n?'-'+n:'');entries.push(record);
    }
  }
  assert(entries.length<=256&&new Set(entries.map(e=>e.key)).size===entries.length,'The prepared review set exceeds its bound');
  const plan={schema:'bonfyre.history-research-preparation.v1',target,packets:packetIds,entries,held,obligations:obligationCount,sourceParts,groups:groups.size};plan.digest=reviewedDigest(plan);return plan;
}

export async function applyReviewedHistoryResearch(plan,{expectedDigest,store,verifyTarget}){
  const kept=structuredClone(plan);assert.equal(kept.schema,'bonfyre.history-research-preparation.v1');assert.equal(kept.digest,expectedDigest,'Review this exact intake first');assert.equal(reviewedDigest(kept),expectedDigest,'Prepared source material changed');targetValue(kept.target);
  assert(typeof verifyTarget==='function'&&typeof store.readVersion==='function'&&typeof store.saveLocalBatch==='function');
  assert.equal(await verifyTarget(kept.target),true,'Current private receiving workspace changed');
  const pending=[],preserved=[];
  // Reconcile the whole set before creating the first record. An edited draft
  // is never overwritten; changed input identities require another review.
  for(const entry of kept.entries){
    const row=await store.readVersion(entry.key);
    if(!row){pending.push(entry);continue;}
    assert(!row.deleted&&row.value?.recovery?.inputDigest===entry.value.recovery.inputDigest,'Existing recovered draft was removed or its source identity changed');
    preserved.push({key:entry.key,revision:row.revision,edited:valueHash(row.value)!==valueHash(entry.value)});
  }
  const created=[];let batch=[];
  async function commit(){
    if(!batch.length)return;
    assert.equal(await verifyTarget(kept.target),true,'Receiving release or account changed');const changes=batch.map(e=>({key:e.key,value:e.value,expectedRevision:0}));
    try{await store.saveLocalBatch(changes);}catch(error){
      const rows=await Promise.all(batch.map(e=>store.readVersion(e.key)));
      if(!rows.every((r,i)=>r&&!r.deleted&&r.revision===1&&valueHash(r.value)===valueHash(batch[i].value)))throw error;
    }
    for(const entry of batch){const row=await store.readVersion(entry.key);assert(row&&!row.deleted&&row.revision===1&&valueHash(row.value)===valueHash(entry.value),'Native record readback differs');created.push({key:entry.key,revision:row.revision});}
    batch=[];
  }
  for(const entry of pending){const trial=[...batch,entry];if(batch.length&&encode([1,'history-review',trial.map(e=>[e.key,0,0,encode([2,e.value,[]],{maxBytes:256*1024})])]).length>450*1024)await commit();batch.push(entry);if(batch.length===2)await commit();}await commit();
  return {digest:kept.digest,created,preserved,obligations:kept.obligations,held:kept.held.length,boundary:recoveryBoundary};
}
