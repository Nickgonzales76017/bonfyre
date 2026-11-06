// Source Library owns the original and revisions; RepositoryFabric owns cut identity.
// No write occurs until a caller explicitly invokes saveReviewedRepositoryCut.
import {readRepositoryCutReceipt} from './repository-cuts.mjs';
const fail=(value,message)=>{if(!value)throw Error(message);};
const hex=/^[0-9a-f]{64}$/;

export async function prepareRepositoryCutSource(file,cut,prepareFile){
 fail(file instanceof Blob&&typeof file.name==='string'&&file.name.length>0&&
   file.size>0&&file.size<=2048&&/\.yaff$/i.test(file.name)&&
   cut?.kind==='repository_cut'&&cut.state==='receipt_self_consistent'&&
   hex.test(cut.cutDigest)&&typeof prepareFile==='function',
   'Choose the exact verified native YAFF source-cut original.');
 const current=await readRepositoryCutReceipt(await file.text());
 fail(current.cutDigest===cut.cutDigest&&current.virtualCommit===cut.virtualCommit&&
   current.virtualTree===cut.virtualTree&&current.filter===cut.filter,
   'The source-cut file changed after its review.');
 const changes=await prepareFile(file,{
   title:'Repository cut · '+current.org+'/'+current.repository+' · '+current.filter,
   collection:'Repository cuts',
   tags:['git','repository-cut',...(current.filter==='none'?[]:['josh'])],
   notes:'Immutable virtual commit/tree and cut digest. Canonical mapping: '+
     current.canonicalMapping+'. Repository access and downstream effects require separate approval.'
 });
 fail(changes.expectedRevision===0&&changes.value?.kind==='file'&&
   changes.value.textLike===true&&changes.value.original===file&&
   changes.value.bytes===file.size&&hex.test(changes.value.contentSHA256),
   'The receiving Source Library cannot preserve the original as reviewable text.');
 return Object.freeze({cut:current,change:changes});
}

export async function inspectRepositoryCutSource(store,prepared){
 fail(store&&typeof store.readVersion==='function'&&
   prepared?.change?.key?.startsWith('source-material:')&&
   prepared.cut?.kind==='repository_cut',
   'Choose the current native Source Library owner.');
 const row=await store.readVersion(prepared.change.key);
 if(!row)return Object.freeze({state:'absent'});
 if(row.deleted||!Number.isSafeInteger(row.revision)||row.revision<1||
    row.value?.kind!=='file'||row.value?.schema!=='bonfyre.source-material.v1'||
    row.value?.filename!==prepared.change.value.filename||
    row.value?.contentSHA256!==prepared.change.value.contentSHA256||
    !(row.value.original instanceof Blob))return Object.freeze({state:'conflict'});
 const native=await readRepositoryCutReceipt(await row.value.original.text());
 if(native.cutDigest!==prepared.cut.cutDigest)return Object.freeze({state:'conflict'});
 return Object.freeze({state:'retained',revision:row.revision,key:row.key||prepared.change.key});
}

export async function saveReviewedRepositoryCut(store,prepared){
 fail(store&&typeof store.saveLocal==='function'&&prepared?.change?.expectedRevision===0,
   'Choose the exact reviewed addition and destination-owned store.');
 const prior=await inspectRepositoryCutSource(store,prepared);
 if(prior.state==='retained')return {...prior,outcome:'already_retained'};
 fail(prior.state==='absent','A saved source conflicts with this exact repository-cut identity.');
 try{await store.saveLocal(prepared.change.key,prepared.change.value,0);}
 catch(error){
   const readback=await inspectRepositoryCutSource(store,prepared).catch(()=>({state:'unknown'}));
   if(readback.state==='retained')return {...readback,outcome:'recovered'};
   throw Object.assign(Error('Source Library save acknowledgement is uncertain. '+
     'Check the exact saved source before any new review. Do not retry the write automatically.'),
     {code:'source_save_uncertain',cause:error});
 }
 const after=await inspectRepositoryCutSource(store,prepared).catch(()=>({state:'unknown'}));
 fail(after.state==='retained','The native save did not yield a verifiable exact source record. Do not replay this write.');
 return {...after,outcome:'saved'};
}
