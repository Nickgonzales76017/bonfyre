#!/usr/bin/env bun
// A read projection after CMS has admitted the exact account record. Package
// identity and view grammar stay with their existing shared implementations.
import {readSync,writeSync} from 'node:fs';
import {isAbsolute} from 'node:path';
import {createHash} from 'node:crypto';
import {readWorkspaceFile} from './workspace_material.mjs';
import {parseContinuationManifest,projectSavedView} from '../integrations/browser/workspace-continuation.mjs';
import {encode,decode,maxPacket} from '../integrations/browser/workspace-msgpack.mjs';

export async function inspectProgramWorkReference(root,packet) {
  if(typeof root!=='string'||!isAbsolute(root)||!Array.isArray(packet)||packet.length!==5||packet[0]!==1)throw Error('Invalid saved-work projection.');
  const [,release,key,revision,body]=packet;
  if(typeof release!=='string'||!/^[a-f0-9]{64}$/.test(release)||typeof key!=='string'||!key||Buffer.byteLength(key)>=512
    ||/[\u0000-\u001f\u007f]/.test(key)||!Number.isSafeInteger(revision)||revision<1||revision>=2147483647
    ||!(body instanceof Uint8Array)||body.byteLength>256*1024)throw Error('Invalid saved-work projection.');
  const record=decode(body,{maxBytes:256*1024});
  if(!Array.isArray(record)||record.length!==3||![2,3,4].includes(record[0])||!Array.isArray(record[2])||record[2].length>64)throw Error('Invalid saved record.');
  const manifest=await readWorkspaceFile(root,release,'CONTINUATIONS.tsv');
  const views=parseContinuationManifest(new TextDecoder('utf-8',{fatal:true}).decode(manifest));
  // Native _views validates every declared editor member before using this
  // manifest. Match that requirement, including views for other record keys.
  const inspected=new Set();
  for(const view of views)if(!inspected.has(view.entry)) {
    const page=await readWorkspaceFile(root,release,view.entry+'index.html');
    if(!Buffer.from(page).includes(Buffer.from('bonfyre-workspace-store')))throw Error('The retained editor is unavailable.');
    inspected.add(view.entry);
  }
  const matches=views.map(view=>projectSavedView(view,{key,revision,value:record[1]})).filter(Boolean);
  if(!matches.length)throw Error('This saved record has no declared continuation.');
  return {release,key,revision,record_digest:createHash('sha256').update(body).digest('hex'),views:matches.map(view=>view.view)};
}
function inputPacket() {
  const parts=[];let total=0;
  for(;;) {
    const part=Buffer.alloc(Math.min(65536,maxPacket+1-total)),read=readSync(0,part,0,part.length,null);
    if(!read)break;total+=read;if(total>maxPacket)throw Error('The projection exceeds its bound.');parts.push(part.subarray(0,read));
  }
  return decode(Buffer.concat(parts));
}
function output(packet) {
  const bytes=encode(packet);let offset=0;
  while(offset<bytes.length){const written=writeSync(1,bytes,offset,bytes.length-offset);if(!written)throw Error('Cannot write projection.');offset+=written;}
}
if(import.meta.main) {
  try {
    const args=process.argv.slice(2);
    if(args.length!==2||args[0]!=='--root')throw Error('Use the configured workspace root.');
    output([1,true,await inspectProgramWorkReference(args[1],inputPacket())]);
  } catch {
    output([1,false,{code:'work_view_unavailable',message:'The saved record has no available verified editor view.'}]);
    process.exitCode=4;
  }
}
