// Exact, browser-side read-only representation of RepositoryFabric cut receipts.
// A self-consistent digest is not provider verification, a canonical JOSH mapping,
// a PackGraph installation, or authority to reuse source bytes.
import {programServiceContinuations} from './program-service-continuations.mjs';

const names=[
 'schema','source_provider','source_org','source_repository','requested_ref',
 'josh_filter','virtual_commit','virtual_tree','cut_digest','canonical_mapping',
 'pack_root','install_authority','publication_authority'
];
const expected=new Set(names);
const hex40or64=/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const sha256=/^[0-9a-f]{64}$/;
const refPattern=/^[A-Za-z0-9][A-Za-z0-9._/-]{0,159}$/;
const namePattern=/^[A-Za-z0-9][A-Za-z0-9_.-]{0,94}$/;
const filterPattern=/^:[A-Za-z0-9_.\/:=,\-]{1,199}$/;
const contextPattern=/^(self|(?:institution|household):[A-Za-z0-9][A-Za-z0-9._-]{0,95})$/;
const roleSet=new Set(['sources','research','analysis','finance','connections','programs','campaigns','commerce','creator','logistics']);
const requireValue=(truth,message)=>{if(!truth)throw Error(message);};
const toHex=bytes=>Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');

export async function readRepositoryCutReceipt(body,{digest=globalThis.crypto?.subtle}={}){
 requireValue(typeof body==='string'&&body.length>0&&
   new TextEncoder().encode(body).length<=2048&&
   body.endsWith('\n')&&!body.includes('\r')&&!body.includes('\0'),
   'Use one bounded native repository-cut YAFF receipt.');
 requireValue(digest&&typeof digest.digest==='function',
   'SHA-256 verification is unavailable; this cut cannot be admitted.');
 const lines=body.slice(0,-1).split('\n');
 requireValue(lines.length===names.length,'This is not an exact RepositoryFabric cut receipt.');
 const fields=Object.create(null);
 for(let i=0;i<lines.length;i++){
   const match=/^([a-z_]+): ([^\n]*)$/.exec(lines[i]);
   requireValue(match&&expected.has(match[1])&&!Object.hasOwn(fields,match[1]),
     'Repository cut includes an unknown or duplicate field.');
   fields[match[1]]=match[2];
 }
 requireValue(names.every(name=>Object.hasOwn(fields,name)),
   'The repository cut is missing an expected field.');
 requireValue(fields.schema==='bonfyre.repository-cut.v1'&&
   fields.source_provider==='forgejo'&&
   namePattern.test(fields.source_org)&&namePattern.test(fields.source_repository)&&
   refPattern.test(fields.requested_ref)&&!fields.requested_ref.includes('..')&&
   (fields.josh_filter==='none'||filterPattern.test(fields.josh_filter)&&!fields.josh_filter.includes('..'))&&
   hex40or64.test(fields.virtual_commit)&&hex40or64.test(fields.virtual_tree)&&
   sha256.test(fields.cut_digest)&&
   fields.canonical_mapping===(fields.josh_filter==='none'?'direct-git':'unverified')&&
   fields.pack_root==='uncompiled'&&
   fields.install_authority==='none'&&
   fields.publication_authority==='none',
   'The repository cut is malformed or claims authority it does not have.');
 const binding=['bonfyre.repository-cut.v1',fields.source_provider,fields.source_org,
   fields.source_repository,fields.requested_ref,
   fields.josh_filter==='none'?'':fields.josh_filter,
   fields.virtual_commit,fields.virtual_tree,''].join('\n');
 const calculated=toHex(new Uint8Array(await digest.digest('SHA-256',new TextEncoder().encode(binding))));
 requireValue(calculated===fields.cut_digest,
   'Repository cut digest differs from its immutable source coordinates.');
 return Object.freeze({
   kind:'repository_cut',state:'receipt_self_consistent',sourceOwner:'BonfyreRepositoryFabric',
   provider:'forgejo',org:fields.source_org,repository:fields.source_repository,
   requestedRef:fields.requested_ref,
   filter:fields.josh_filter,virtualCommit:fields.virtual_commit,
   virtualTree:fields.virtual_tree,cutDigest:fields.cut_digest,
   canonicalMapping:fields.canonical_mapping,
   dataBytesVerified:false,remoteRepositoryVerified:false,
   sourceReadAuthorized:false,installAuthorized:false,publicationAuthorized:false
 });
}

export async function readRepositoryCutCollection(files,{max=24}={}){
 requireValue(Array.isArray(files)&&files.length>0&&files.length<=max&&max<=24,
   'Select one to 24 bounded native repository-cut receipts.');
 const result=[],seen=new Set();
 for(const body of files){
   const row=await readRepositoryCutReceipt(body);
   requireValue(!seen.has(row.cutDigest),'A repository-cut digest was repeated.');
   seen.add(row.cutDigest);result.push(row);
 }
 return Object.freeze(result);
}

export function repositoryCutContinuations(cut,tools,{context='self',roles=['sources','analysis']}={}){
 requireValue(cut?.kind==='repository_cut'&&cut.state==='receipt_self_consistent'&&
   sha256.test(cut.cutDigest)&&hex40or64.test(cut.virtualCommit)&&
   hex40or64.test(cut.virtualTree)&&cut.provider==='forgejo'&&
   ['direct-git','unverified'].includes(cut.canonicalMapping)&&
   cut.canonicalMapping===(cut.filter==='none'?'direct-git':'unverified')&&
   cut.sourceReadAuthorized===false&&cut.installAuthorized===false&&
   cut.publicationAuthorized===false&&
   contextPattern.test(context)&&Array.isArray(roles)&&roles.length>0&&
   roles.length<=roleSet.size&&roles.every(role=>roleSet.has(role)),
   'Choose a verified read-only source cut and declared receiving owner.');
 const coordinate=Object.freeze({
   kind:'repository-cut',provider:cut.provider,org:cut.org,repository:cut.repository,
   requestedRef:cut.requestedRef,joshFilter:cut.filter,
   virtualCommit:cut.virtualCommit,virtualTree:cut.virtualTree,
   cutDigest:cut.cutDigest,canonicalMapping:cut.canonicalMapping
 });
 return Object.freeze(programServiceContinuations([...new Set(roles)],tools,{context})
   .map(row=>Object.freeze({...row,sourceCoordinate:coordinate,
     boundary:'Open the owning Aurekai application for review only. A JOSH virtual commit '+
       'does not prove a canonical upstream mapping. Source reads, repository changes, '+
       'PackGraph installation, analysis execution and publication require separate authority.',
     dataTransferAuthorized:false,queryExecuted:false,recordCreated:false,
     sourceReadAuthorized:false,gitWriteAuthorized:false,sourceBytesVerified:false
   })));
}
