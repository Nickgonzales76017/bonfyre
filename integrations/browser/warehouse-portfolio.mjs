// Aurekai presentation of a read-only Bonfyre Warehouse Portfolio.
// Never converts source material, query candidates, or installed apps into
// authority, persisted data products, published material, or executed queries.
import {programServiceContinuations} from './program-service-continuations.mjs';

const contextPattern=/^(self|(?:institution|household):[A-Za-z0-9][A-Za-z0-9._-]{0,95})$/;
const semantic=/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,239}$/;
const bindingPattern=/^ws-[a-f0-9]{40}$/;
const releasePattern=/^[a-f0-9]{64}$/;
const digestPattern=/^[a-f0-9]{64}$/;
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const validCount=(n,maximum=Number.MAX_SAFE_INTEGER)=>Number.isSafeInteger(n)&&n>=0&&n<=maximum;
const requireValue=(v,reason)=>{if(!v)throw Error(reason);};
const allowedRoles=new Set(['sources','research','analysis','finance','connections','programs','campaigns','commerce','creator','logistics']);
const expectedKeys=new Set(['semantic_kind','state','representation_kind','generation',
  'representation_id','digest','realization_count','max_realization_bytes']);
const untrustedKeys=new Set(['source_db','physical_path','path','url','query','sql','token',
  'credentials','actor','authority','body','original','access']);

export function warehousePortfolioPage(page,{offset=0,total}={}){
 requireValue(object(page)&&page.schema==='bonfyre.warehouse.portfolio.v1'&&
   ['READ_ONLY','NOT_INSTALLED'].includes(page.state),'This is not a native warehouse portfolio.');
 requireValue(page.read_only===true&&page.execution_authority===false&&
   page.physical_bytes_verified===false&&page.tenant_access_verified===false,
   'The imported portfolio does not declare a read-only boundary.');
 requireValue(Array.isArray(page.rows)&&page.rows.length<=100,
   'The portfolio page exceeds the bounded read contract.');
 if(page.state==='NOT_INSTALLED'){
   requireValue(page.rows.length===0&&page.next_offset===null,
     'An uninstalled portfolio must not contain data.');
   return {state:'NOT_INSTALLED',rows:[],next:null,total:0,offset:0};
 }
 requireValue(validCount(page.total,1000000)&&validCount(page.offset,100000000)&&
   validCount(page.limit,100)&&page.limit>=1&&page.offset===offset&&
   (total===undefined||page.total===total),
   'A portfolio page is from a different or incomplete read.');
 requireValue(page.offset<=page.total&&
   page.rows.length===Math.min(page.limit,page.total-page.offset),
   'The portfolio page has an incomplete range.');
 const expectedNext=page.offset+page.limit<page.total?page.offset+page.limit:null;
 requireValue(page.next_offset===expectedNext,'A portfolio cursor changed unexpectedly.');
 const seen=new Set(),rows=page.rows.map(row=>{
   requireValue(object(row)&&semantic.test(row.semantic_kind)&&
     ['PINNED_METADATA','UNAVAILABLE'].includes(row.state)&&
     ![...Object.keys(row)].some(k=>untrustedKeys.has(k)||!expectedKeys.has(k))&&
     !seen.has(row.semantic_kind),
     'A portfolio row has a missing, unsafe, or duplicate source identity.');
   seen.add(row.semantic_kind);
   if(row.state==='UNAVAILABLE'){
     requireValue(Object.keys(row).length===2,'Unavailable sources must not expose stale versions.');
     return Object.freeze({semanticKind:row.semantic_kind,state:'unavailable'});
   }
   requireValue(row.representation_kind==='parquet'&&
     validCount(row.generation)&&row.generation>0&&
     typeof row.representation_id==='string'&&semantic.test(row.representation_id)&&
     digestPattern.test(row.digest)&&
     validCount(row.realization_count)&&row.realization_count>0&&
     validCount(row.max_realization_bytes)&&row.max_realization_bytes>0,
     'The pinned warehouse metadata is incomplete or invalid.');
   return Object.freeze({semanticKind:row.semantic_kind,state:'pinned',
     representationKind:'parquet',generation:row.generation,
     representationId:row.representation_id,digest:row.digest,
     realizationCount:row.realization_count,observedBytes:row.max_realization_bytes});
 });
 requireValue(rows.every((row,i)=>i===0||row.semanticKind>rows[i-1].semanticKind),
   'The portfolio is not in canonical source order.');
 return Object.freeze({state:'READ_ONLY',rows,next:expectedNext,total:page.total,offset});
}

// Each separately opened native page is a SQLite snapshot, NOT a database-wide
// cross-page transaction. Rows remain individually pinned; a multi-page
// collection is a candidate inventory, not a current all-source census.
export function collectWarehousePortfolio(pages){
 requireValue(Array.isArray(pages)&&pages.length>0&&pages.length<=100,
   'Choose between one and 100 bounded native portfolio pages.');
 const rows=[],seen=new Set();let cursor=0,total;
 let complete=false;
 for(const raw of pages){
   const page=warehousePortfolioPage(raw,{offset:cursor,total});
   if(page.state==='NOT_INSTALLED'){
     requireValue(pages.length===1,'An uninstalled portfolio cannot be mixed with data.');
     return {state:'not_installed',rows:[],total:0,complete:false,scope:'metadata-only'};
   }
   total=page.total;
   for(const row of page.rows){
     requireValue(!seen.has(row.semanticKind)&&
       (!rows.length||row.semanticKind>rows[rows.length-1].semanticKind),
       'The source changed or overlapped between imported pages.');
     seen.add(row.semanticKind);rows.push(row);
   }
   complete=page.next===null;
   cursor=page.next??cursor+page.rows.length;
   if(complete&&raw!==pages.at(-1))throw Error('Additional pages follow the final cursor.');
 }
 return Object.freeze({state:'review',rows,total,complete,
   scope:'metadata-only',crossPageSnapshotVerified:false,
   tenantAccessVerified:false,physicalBytesVerified:false});
}

export function warehouseInstalledTools(packet,context){
 requireValue(contextPattern.test(context),'Choose a valid current account destination.');
 const scope=context==='self'?'self':context.split(':')[0];
 requireValue(Array.isArray(packet)&&packet.length===4&&packet[0]===1&&
   packet[2]===scope&&Array.isArray(packet[3])&&packet[3].length<=64&&
   (context==='self'?/^user:[1-9]\d*$/.test(packet[1]):packet[1]===context),
   'The installed-app list belongs to another destination.');
 const seen=new Set();
 return packet[3].map(v=>{
   requireValue(Array.isArray(v)&&v.length===6&&bindingPattern.test(v[0])&&
     typeof v[1]==='string'&&v[1].length<=128&&
     typeof v[2]==='string'&&/^([a-z0-9-]+\/)+$/.test(v[2])&&
     releasePattern.test(v[3])&&typeof v[4]==='string'&&v[4].length<=256&&
     typeof v[5]==='boolean'&&!seen.has(v[0]),
     'An installed app has an incomplete binding.');
   seen.add(v[0]);
   return Object.freeze({id:v[0],pack:v[1],entry:v[2],release:v[3],
     label:v[4],ready:v[5],href:'/apps/'+v[0]+'/'+v[2]});
 });
}

export function warehouseContinuations(row,installed,{context='self',roles=['sources','analysis']}={}){
 requireValue(row?.state==='pinned'&&semantic.test(row.semanticKind)&&
   digestPattern.test(row.digest)&&validCount(row.generation)&&row.generation>0,
   'Select one current pinned metadata candidate.');
 requireValue(contextPattern.test(context)&&Array.isArray(roles)&&roles.length>0&&
   roles.length<=allowedRoles.size&&roles.every(role=>allowedRoles.has(role)),
   'Choose declared Aurekai continuations.');
 const proposals=programServiceContinuations([...new Set(roles)],installed,{context});
 return Object.freeze(proposals.map(p=>Object.freeze({
   ...p,sourceCoordinate:Object.freeze({
     semanticKind:row.semanticKind,representationKind:'parquet',generation:row.generation,
     representationId:row.representationId,digest:row.digest
   }),
   state:p.state,boundary:'Open the installed owner for a reviewed continuation. '+
     'No snapshot bytes, query results, record writes, account access or publication are transferred.',
   dataTransferAuthorized:false,queryExecuted:false,recordCreated:false
 })));
}
