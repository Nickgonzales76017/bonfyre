import {afterEach,expect,test} from 'bun:test';
import {Database} from 'bun:sqlite';
import {createDecipheriv,createECDH,createPublicKey,hkdfSync,verify} from 'node:crypto';
import {chmodSync,existsSync,mkdirSync,mkdtempSync,readFileSync,rmSync,statSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createFabricStore} from '../fabric_store.mjs';
import {createFabricPushService} from '../fabric_push.mjs';
import {createWebPushCarrier,encryptWebPush,loadPushIdentity,pushEndpoint,validatePushSubscription,vapidIdentity} from '../fabric_push_carrier.mjs';
import {accountAuthorizerFromEnvironment,createAccountAuthorizer} from '../account_authority.mjs';
import {encode,decode} from '../../browser/workspace-msgpack.mjs';

const uaPrivate=Buffer.from('q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94','base64url');
const ua=createECDH('prime256v1');ua.setPrivateKey(uaPrivate);
const subscription={endpoint:'https://fcm.googleapis.com/fcm/send/local-fixture',expirationTime:null,
  keys:{p256dh:ua.getPublicKey().toString('base64url'),auth:'BTBZMqHH6r4Tts7J_aSIgg'}};
const binding={actor:'user:1',subject:'institution:upper90',scope:'institution'};
const identity=vapidIdentity(Buffer.from('yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw','base64url'),'https://aurekai.example');
const cleanups=[];
afterEach(async()=>{for(const cleanup of cleanups.splice(0).reverse())await cleanup();});
function decrypt(body) {
  body=Buffer.from(body);expect(body.readUInt32BE(16)).toBe(4096);expect(body[20]).toBe(65);
  const salt=body.subarray(0,16),server=body.subarray(21,86),shared=ua.computeSecret(server);
  const ikm=Buffer.from(hkdfSync('sha256',shared,Buffer.from(subscription.keys.auth,'base64url'),Buffer.concat([Buffer.from('WebPush: info\0'),ua.getPublicKey(),server]),32));
  const cek=Buffer.from(hkdfSync('sha256',ikm,salt,Buffer.from('Content-Encoding: aes128gcm\0'),16));
  const nonce=Buffer.from(hkdfSync('sha256',ikm,salt,Buffer.from('Content-Encoding: nonce\0'),12));
  const decipher=createDecipheriv('aes-128-gcm',cek,nonce);decipher.setAuthTag(body.subarray(-16));
  const plain=Buffer.concat([decipher.update(body.subarray(86,-16)),decipher.final()]);expect(plain.at(-1)).toBe(2);return plain.subarray(0,-1);
}
function fixture(options={}) {
  let at=1000,paused=false,calls=0;const root=mkdtempSync(join(tmpdir(),'fabric-push-test-')),projectRoot=join(root,'project');mkdirSync(projectRoot);
  const store=createFabricStore({home:join(root,'owner'),projectRoot,now:()=>at});
  const sent=[];
  let allow=true;
  const service=createFabricPushService({store,origin:identity.origin,identity,now:()=>at,stopped:()=>paused,
    authorize:async()=>{if(allow instanceof Error)throw allow;return allow;},request:async(endpoint,request)=>{calls++;sent.push(decode(decrypt(request.body)));return new Response(null,{status:201});},...options});
  cleanups.push(()=>{store.close();rmSync(root,{recursive:true,force:true});},()=>service.close());
  let sequence=0;
  const notify=(who=binding,kind='claude')=>{const task=store.createScopedTask({kind,title:'Private customer task',prompt:'Private task prompt',requestId:'notice-request-'+(++sequence)},who);store.updateTask(task.task_id,{state:'completed',completed_at:at});return task.task_id;};
  const enable=(who=binding,sub=subscription)=>service.dispatch(who,'push-subscribe',{expectedBinding:who,...sub});
  return {store,service,root,sent,notify,enable,advance:n=>{at+=n;},pause:value=>{paused=value;},allow:value=>{allow=value;},calls:()=>calls,now:()=>at};
}

test('RFC 8291 published encryption vector matches byte for byte',()=>{
  const body=encryptWebPush(Buffer.from('When I grow up, I want to be a watermelon'),subscription.keys,
    {salt:Buffer.from('DGv6ra1nlYgDCS1FRnbzlw','base64url'),privateKey:Buffer.from('yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw','base64url')});
  expect(body.toString('base64url')).toBe('DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPTpK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN');
});

test('actual local HTTP receiver verifies VAPID ES256 and decrypts the native MessagePack envelope',async()=>{
  let received;const server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
    const auth=request.headers.get('authorization'),match=/^vapid t=([^,]+), k=([A-Za-z0-9_-]+)$/.exec(auth);expect(match).not.toBeNull();
    const [head,body,signature]=match[1].split('.'),claims=JSON.parse(Buffer.from(body,'base64url'));
    expect(JSON.parse(Buffer.from(head,'base64url'))).toEqual({typ:'JWT',alg:'ES256'});expect(claims).toEqual({aud:'https://fcm.googleapis.com',exp:4600,sub:'https://aurekai.example/apps'});
    const pub=Buffer.from(match[2],'base64url');const key=createPublicKey({key:{kty:'EC',crv:'P-256',x:pub.subarray(1,33).toString('base64url'),y:pub.subarray(33).toString('base64url')},format:'jwk'});
    expect(verify('sha256',Buffer.from(head+'.'+body),{key,dsaEncoding:'ieee-p1363'},Buffer.from(signature,'base64url'))).toBe(true);
    expect(request.headers.get('content-encoding')).toBe('aes128gcm');expect(request.headers.get('ttl')).toBe('60');
    received=decode(decrypt(new Uint8Array(await request.arrayBuffer())));return new Response(null,{status:201});
  }});cleanups.push(()=>server.stop(true));
  const send=createWebPushCarrier({identity,now:()=>1000,request:(url,init)=>{expect(url).toBe(subscription.endpoint);expect(init.redirect).toBe('error');return fetch(`http://127.0.0.1:${server.port}/fixture`,init);}});
  const payload=[1,{deliveryId:'a'.repeat(24),binding,context:binding.subject,expiresAt:1060000}];
  expect(await send(subscription,encode(payload),{ttl:60,topic:'a'.repeat(24)})).toEqual({state:'accepted',status:201});expect(received).toEqual(payload);
});

test('subscription validation rejects internal addresses, redirects, credentials and invalid curve points before transport',async()=>{
  for(const endpoint of ['https://127.0.0.1/private','http://fcm.googleapis.com/fcm/send/id','https://fcm.googleapis.com:444/fcm/send/id',
    'https://fcm.googleapis.com.evil.invalid/push','https://u:p@fcm.googleapis.com/fcm/send/id','https://fcm.googleapis.com/fcm/send/id#fragment','https://anything.invalid/push'])
    expect(()=>pushEndpoint(endpoint)).toThrow();
  expect(()=>validatePushSubscription({...subscription,keys:{...subscription.keys,p256dh:Buffer.alloc(65,4).toString('base64url')}})).toThrow('P-256');
  let calls=0;const send=createWebPushCarrier({identity,request:async()=>{calls++;return new Response(null,{status:302,headers:{location:'https://127.0.0.1/private'}});}});
  expect(await send(subscription,encode([1]),{ttl:30,topic:'fixture'})).toEqual({state:'failed',status:302});expect(calls).toBe(1);
});

test('native VAPID identity persists privately and refuses a changed origin or permissive key mode',()=>{
  const root=mkdtempSync(join(tmpdir(),'push-key-test-'));cleanups.push(()=>rmSync(root,{recursive:true,force:true}));
  const first=loadPushIdentity(root,identity.origin),second=loadPushIdentity(root,identity.origin);expect(first.publicKey).toBe(second.publicKey);
  const path=join(root,'push/vapid.msgpack');expect(statSync(path).mode&0o777).toBe(0o600);
  expect(()=>loadPushIdentity(root,'https://other.example')).toThrow('another origin');chmodSync(path,0o644);
  expect(()=>loadPushIdentity(root,identity.origin)).toThrow('private');
});

test('STOP prevents delivery identity provisioning until the owner is resumed',async()=>{
  const f=fixture({identity:undefined}),key=join(f.store.home,'push/vapid.msgpack');f.pause(true);f.service.start();
  expect(await f.service.dispatch(binding,'push-status')).toMatchObject({configured:false,paused:true,publicKey:null});
  expect(await f.service.pump()).toMatchObject({planned:0,attempted:0});expect(existsSync(key)).toBe(false);
  f.pause(false);expect(await f.service.dispatch(binding,'push-status')).toMatchObject({configured:true,paused:false});
  expect(existsSync(key)).toBe(true);expect(f.calls()).toBe(0);
});

test('only post-opt-in unread recipient work is queued, with no private task body in the push',async()=>{
  const f=fixture();f.notify();await f.enable();const taskId=f.notify();
  const other={actor:'user:2',subject:binding.subject,scope:binding.scope};f.notify(other);
  const legacy=f.store.createTask({kind:'claude',title:'operator only',prompt:'private'});f.store.updateTask(legacy,{state:'completed'});
  expect(await f.service.pump()).toMatchObject({planned:1,attempted:1,accepted:1});expect(f.calls()).toBe(1);
  expect(JSON.stringify(f.sent)).not.toContain('Private');expect(JSON.stringify(f.sent)).not.toContain(taskId);
  expect(f.sent[0][1].binding).toEqual(binding);
  expect(await f.service.pump()).toMatchObject({planned:0,attempted:0});expect(f.calls()).toBe(1);
});

test('accepted, shown and opened are distinct; acknowledgements do not read or cancel work',async()=>{
  const f=fixture();await f.enable();const task=f.notify();await f.service.pump();const deliveryId=f.sent[0][1].deliveryId;
  expect((await f.service.dispatch(binding,'push-status')).counts).toEqual({queued:0,accepted:1,shown:0,opened:0,uncertain:0});
  const body={expectedBinding:binding,deliveryId};
  await f.service.dispatch(binding,'push-ack',{...body,stage:'shown'});await f.service.dispatch(binding,'push-ack',{...body,stage:'shown'});
  await f.service.dispatch(binding,'push-ack',{...body,stage:'opened'});
  expect((await f.service.dispatch(binding,'push-status')).counts).toEqual({queued:0,accepted:1,shown:1,opened:1,uncertain:0});
  expect(await f.service.dispatch(binding,'push-resolve',body)).toMatchObject({taskId:task,notificationState:'unread',href:'/apps?context=institution%3Aupper90#fabric-task-'+task});
  expect(f.store.task(task).state).toBe('completed');
});

test('a different account in the same organization cannot register stale UI, acknowledge or resolve a delivery',async()=>{
  const f=fixture();await f.enable();f.notify();await f.service.pump();const other={...binding,actor:'user:2'},deliveryId=f.sent[0][1].deliveryId;
  await expect(f.service.dispatch(other,'push-subscribe',{expectedBinding:binding,...subscription})).rejects.toMatchObject({code:'conflict'});
  await expect(f.service.dispatch(other,'push-resolve',{expectedBinding:other,deliveryId})).rejects.toMatchObject({code:'not_found'});
  await expect(f.service.dispatch(other,'push-ack',{expectedBinding:binding,deliveryId,stage:'opened'})).rejects.toMatchObject({code:'conflict'});
  await expect(f.enable(other)).rejects.toMatchObject({code:'device_account_conflict'});
});

test('read and dismiss suppress unsent alerts without changing work state; disabling a device revokes its pending deliveries',async()=>{
  const f=fixture();const enabled=await f.enable();const task=f.notify();
  const notice=f.store.activityRead(binding,{execution:()=>({owned:false,queued:false})}).notifications[0];
  f.store.notificationAction(binding,{id:notice.id,revision:notice.revision,action:'dismiss',requestId:'dismiss-before-push'});
  await f.service.pump();expect(f.calls()).toBe(0);expect(f.store.task(task).state).toBe('completed');
  f.notify();f.store.push.plan(identity.keyId);
  await f.service.dispatch(binding,'push-unsubscribe',{expectedBinding:binding,subscriptionId:enabled.subscription.id});await f.service.pump();expect(f.calls()).toBe(0);
});

test('fresh revoked or unavailable account authority prevents delivery and is not accepted as success',async()=>{
  const f=fixture();await f.enable();f.notify();f.allow(new Error('owner offline'));await f.service.pump();expect(f.calls()).toBe(0);
  f.advance(61);f.allow({allowed:true});await f.service.pump();expect(f.calls()).toBe(0);
  f.allow(true);expect((await f.service.dispatch(binding,'push-status')).subscriptions[0].state).toBe('revoked');
});

test('STOP prevents scheduling and sending, including a STOP raised during current authorization',async()=>{
  let paused=false,checks=0;
  const f=fixture({stopped:()=>paused,authorize:async()=>{checks++;if(checks===2)paused=true;return true;}});
  await f.enable();f.notify();await f.service.pump();expect(f.calls()).toBe(0);
  expect((await f.service.dispatch(binding,'push-status')).paused).toBe(true);
  await expect(f.enable()).rejects.toMatchObject({code:'paused'});
});

test('retry-after, endpoint expiry and network uncertainty have distinct durable outcomes',async()=>{
  let count=0;const f=fixture({request:async()=>{count++;if(count===1)return new Response(null,{status:429,headers:{'retry-after':'120'}});return new Response(null,{status:410});}});
  await f.enable();f.notify();await f.service.pump();expect(count).toBe(1);f.advance(119);await f.service.pump();expect(count).toBe(1);
  f.advance(2);await f.service.pump();expect(count).toBe(2);expect((await f.service.dispatch(binding,'push-status')).subscriptions[0].state).toBe('revoked');
  const uncertain=fixture({request:async()=>{throw new Error('connection reset after write');}});await uncertain.enable();uncertain.notify();
  expect(await uncertain.service.pump()).toMatchObject({attempted:1,uncertain:1});uncertain.advance(1000);
  expect(await uncertain.service.pump()).toMatchObject({attempted:0});expect((await uncertain.service.dispatch(binding,'push-status')).counts.uncertain).toBe(1);
});

test('crashed pre-send claims may retry; crashed sending leases remain uncertain across owner restart',async()=>{
  const f=fixture();await f.enable();f.notify();f.store.push.plan(identity.keyId);f.store.push.claim();f.advance(31);
  expect(await f.service.pump()).toMatchObject({attempted:1,accepted:1});
  f.notify();f.store.push.plan(identity.keyId);const claim=f.store.push.claim();f.store.push.beginSend(claim,identity.keyId);f.advance(31);
  expect(await f.service.pump()).toMatchObject({attempted:0});expect((await f.service.dispatch(binding,'push-status')).counts.uncertain).toBe(1);
});

test('delivery expiry and consent expiry do not resurrect old notifications',async()=>{
  const f=fixture();await f.enable();f.notify();f.store.push.plan(identity.keyId);f.advance(86401);await f.service.pump();expect(f.calls()).toBe(0);
  f.advance(30*86400);expect((await f.service.dispatch(binding,'push-status')).subscriptions[0].state).toBe('expired');
  await f.enable();await f.service.pump();expect(f.calls()).toBe(0);
});

test('model MessagePack receipts remain in existing owner, verify bytes and project model work kind',()=>{
  const f=fixture();const task=f.notify(binding,'model');const value={input:'retained input',reservation:'reservation-1'};
  const receipt=f.store.saveReceipt(task,'model-request',value,'msgpack');expect(decode(readFileSync(receipt.path))).toEqual(value);
  expect(f.store.readReceipt(task,'model-request').value).toEqual(value);expect(f.store.taskScope(task)).toEqual(binding);
  const textOutput='Retained result — \"exact\"\nsecond line',output=f.store.saveReceipt(task,'model-output',textOutput,'txt');
  expect(f.store.readReceipt(task,'model-output')).toEqual({value:textOutput,receipt:output});expect(readFileSync(output.path,'utf8')).toBe(textOutput);
  const patch='diff --git a/a b/a\n+retained\n',patchReceipt=f.store.saveReceipt(task,'model-patch',patch,'patch');
  expect(f.store.readReceipt(task,'model-patch')).toEqual({value:patch,receipt:patchReceipt});
  const projection=f.store.activityRead(binding,{execution:()=>({owned:false,queued:false})});expect(projection.tasks[0].workKind).toBe('model');expect(projection.outputs[0].title).toBe('Model output');
  writeFileSync(receipt.path,encode({input:'tampered'}));expect(()=>f.store.readReceipt(task,'model-request')).toThrow('integrity');
});

test('account adapter requires exact fresh native binding, real boolean allow and explicit owner paths',async()=>{
  const input={...binding,permission:'write'},config={authBinary:'/trusted/bonfyre-auth',authDb:'/trusted/auth.db',cmsDb:'/trusted/cms.db',now:()=>1000};
  let command;const run=async argv=>{command=argv;return {exitCode:0,stdout:encode([1,true,{...input,allowed:true,checked_at:1000}])};};
  expect(await createAccountAuthorizer({...config,run})(input)).toBe(true);expect(command).toContain('background-access');expect(command.join(' ')).not.toContain('--token');
  for(const row of [{...input,actor:'user:2',allowed:true,checked_at:1000},{...input,allowed:'yes',checked_at:1000},{...input,allowed:true,checked_at:900}])
    await expect(createAccountAuthorizer({...config,run:async()=>({exitCode:0,stdout:encode([1,true,row])})})(input)).rejects.toMatchObject({code:'authority_unavailable'});
  await expect(accountAuthorizerFromEnvironment({})(input)).rejects.toMatchObject({code:'authority_unavailable'});
  expect(await createAccountAuthorizer({...config,run:async()=>({exitCode:0,stdout:encode([1,true,{...input,allowed:false,checked_at:1000}])})})(input)).toBe(false);
});
