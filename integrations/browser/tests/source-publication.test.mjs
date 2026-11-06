import {test,expect} from 'bun:test';
import {encode,decode} from '../workspace-msgpack.mjs';
import {sourcePublicationInput,sourcePublicationReceipt,sourcePublicationSession,selectedPublicationSource,publicationRecoveryId} from '../source-publication.mjs';
import {publicationPacket} from '../publication-research.mjs';
const id='a'.repeat(64),tail='b'.repeat(64),input=()=>[1,id,'twin:workshop','workshop-source','Repair notes','Read the organizer’s retained notices.','atlas'];
const reply=(state,p=null)=>[1,state,id,'foreign-collection-'+tail,'foreign-world-'+tail,p?1:0,p?2:0,p,p?'DISABLED':'NOT_REQUESTED'];
const response=(p,status=200)=>new Response(encode(p),{status,headers:{'Content-Type':'application/msgpack'}});
function owner({lose=false,before=false,unreadable=false}={}){let saved=null,writes=0,reads=0;return {get writes(){return writes;},get reads(){return reads;},get saved(){return saved;},set(value){saved=value;},async request(url,options){if(options.method==='POST'){writes++;if(before){before=false;throw Error('Transport lost before commit');}saved=decode(options.body);if(lose){lose=false;throw Error('Post-commit acknowledgement lost');}return response(reply('committed',saved));}reads++;if(unreadable)throw Error('Read unavailable');return response(reply(saved?'committed':'absent',saved));}};}
test('one observed source is explicit; mixed or missing subject coordinates do not infer one',()=>{
 const a={sourceRef:'workshop-source',sourceSubject:'twin:workshop',title:'First'},b={...a,title:'Second'};
 expect(selectedPublicationSource([a,b])).toEqual({source:'workshop-source',subject:'twin:workshop',label:'First'});
 expect(selectedPublicationSource([{...a,sourceSubject:''}])).toBeNull();expect(selectedPublicationSource([a,{...b,sourceRef:'other'}])).toBeNull();expect(selectedPublicationSource([])).toBeNull();
});
test('the original publication protocol remains compatible while V2 carries an exact source subject',()=>{
 const row=['one','Title','Excerpt','https://example.test/a','feed','receipt','Terms','excerpt',''];
 expect(publicationPacket([1,[row],0]).inputs[0].sourceSubject).toBeUndefined();expect(publicationPacket([2,[[...row,'twin:one']],0]).inputs[0].sourceSubject).toBe('twin:one');
 expect(()=>publicationPacket([2,[row],0])).toThrow();expect(()=>publicationPacket([2,[[...row,'bad\u0000subject']],0])).toThrow();expect(()=>publicationPacket([3,[],0])).toThrow();
});
test('only one bounded typed request and supported presentation can be published',()=>{
 const p=input();expect(sourcePublicationInput(p)).toEqual(p);expect(sourcePublicationInput(p)).not.toBe(p);
 for(const change of [p=>p.push('extra'),p=>p[1]='bad',p=>p[4]=' ',p=>p[5]='x'.repeat(7201),p=>p[2]='https://user:secret@foreign/path?x=1',p=>p[6]='run',p=>p[4]='bad\u0000text']){const v=input();change(v);expect(()=>sourcePublicationInput(v)).toThrow();}
 expect(publicationRecoveryId('#source-publication='+id)).toBe(id);expect(publicationRecoveryId('#source-publication='+id+'&actor=other')).toBeNull();
});
test('recovered receipt must contain the complete matching request and pair',()=>{
 expect(sourcePublicationReceipt(reply('committed',input()),id).worldId).toBe(2);
 for(const change of [p=>p[2]='c'.repeat(64),p=>p[5]=0,p=>p[4]='foreign-world-'+'c'.repeat(64),p=>p[7]=null,p=>p[8]='probably_saved',p=>p.push('extra')]){const p=reply('committed',input());change(p);expect(()=>sourcePublicationReceipt(p,id)).toThrow();}
 expect(()=>sourcePublicationReceipt(reply('absent',input()),id)).toThrow();
});
test('lost post-commit acknowledgement settles by native readback without replay',async()=>{
 const f=owner({lose:true}),s=sourcePublicationSession(id,{request:f.request});const r=await s.publish(input());expect(r.state).toBe('committed');expect(f.writes).toBe(1);expect(f.reads).toBe(2);expect(s.pending).toBe(false);
 expect((await s.publish(input())).state).toBe('committed');expect(f.writes).toBe(1);const detached=s.input;detached[4]='tampered';expect(s.input[4]).toBe(input()[4]);
});
test('absent readback after failed send requires an explicit identical retry',async()=>{
 const f=owner({before:true}),s=sourcePublicationSession(id,{request:f.request});expect((await s.publish(input())).state).toBe('absent');expect(f.saved).toBeNull();expect(f.writes).toBe(1);
 const changed=input();changed[4]='Another title';await expect(s.publish(changed)).rejects.toThrow('cannot be changed');expect(f.writes).toBe(1);
 expect((await s.publish(input())).state).toBe('committed');expect(f.writes).toBe(2);
});
test('unavailable preflight never sends a write and refuses duplicate simultaneous calls',async()=>{
 const f=owner({unreadable:true}),s=sourcePublicationSession(id,{request:f.request});await expect(s.publish(input())).rejects.toThrow('Read unavailable');expect(f.writes).toBe(0);expect(s.pending).toBe(false);
 let release;const waiting=new Promise(r=>release=r);const gate=sourcePublicationSession(id,{request:async()=>{await waiting;return response(reply('absent'));}});const read=gate.check();await expect(gate.publish(input())).rejects.toThrow('Finish');release();await read;
});
test('foreign or changed saved material is never accepted as the reviewed publication',async()=>{
 const f=owner(),other=input();other[4]='Changed';f.set(other);const s=sourcePublicationSession(id,{request:f.request});await expect(s.publish(input())).rejects.toThrow('different saved publication');expect(f.writes).toBe(0);
 const restored=sourcePublicationSession(id,{request:f.request});expect((await restored.check()).input).toEqual(other);expect(f.writes).toBe(0);
});
test('permission refusals and oversized response bodies cannot become receipts',async()=>{
 const denied=sourcePublicationSession(id,{request:async()=>new Response('',{status:403})});await expect(denied.publish(input())).rejects.toThrow('grant');
 const large=sourcePublicationSession(id,{request:async()=>new Response(new Uint8Array(17000),{headers:{'Content-Type':'application/msgpack'}})});await expect(large.check()).rejects.toThrow('bound');
 const misleading=sourcePublicationSession(id,{request:async()=>response(reply('committed',input()),409)});await expect(misleading.check()).rejects.toThrow('contradicts');
});
