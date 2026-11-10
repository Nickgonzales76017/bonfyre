// Explicit verification of an owner-admitted output between two local native
// Swarm processes. This is not recipient pairing, remote delivery or inference.
import {spawn} from 'node:child_process';
import {createHash,randomBytes} from 'node:crypto';
import {closeSync,constants,existsSync,fstatSync,fsyncSync,lstatSync,mkdirSync,openSync,
  readSync,readdirSync,unlinkSync,writeFileSync} from 'node:fs';
import {dirname,isAbsolute,join} from 'node:path';
import {encode,decode} from '../browser/workspace-msgpack.mjs';
import {FabricActivityError,fabricActivityBinding} from './fabric_store.mjs';

const fail=(ok,code,message)=>{if(!ok)throw new FabricActivityError(code,message);};
const sha=value=>createHash('sha256').update(value).digest('hex');
const hash=value=>sha(encode(value));
const equal=(a,b)=>a.actor===b.actor&&a.subject===b.subject&&a.scope===b.scope;
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===keys.length&&Object.keys(value).every(key=>keys.includes(key));
const absolute=value=>typeof value==='string'&&isAbsolute(value)&&Buffer.byteLength(value)<4096&&!/[\x00-\x1f\x7f]/.test(value);
const identity=value=>typeof value==='string'&&/^[a-f0-9]{24}$/.test(value);
const maxBytes=64*1024*1024;
const message='The local transfer needs reconciliation; this request will not be transmitted again.';

function privateDirectory(path) {
  if(!existsSync(path))mkdirSync(path,{mode:0o700});
  const stat=lstatSync(path);
  fail(stat.isDirectory()&&!stat.isSymbolicLink()&&(stat.mode&0o077)===0&&
    (typeof process.getuid!=='function'||stat.uid===process.getuid()),'unavailable','The local verification owner directory is unavailable.');
}
function readBounded(path,maximum) {
  const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try {
    const before=fstatSync(fd,{bigint:true});
    fail(before.isFile()&&before.size<=BigInt(maximum),'unavailable','The retained local verification evidence is invalid.');
    const bytes=Buffer.alloc(Number(before.size)+1);let used=0;
    while(used<bytes.length){const n=readSync(fd,bytes,used,bytes.length-used,null);if(!n)break;used+=n;}
    const after=fstatSync(fd,{bigint:true}),named=lstatSync(path,{bigint:true});
    const key=stat=>[stat.dev,stat.ino,stat.size,stat.mtimeNs,stat.ctimeNs].join(':');
    fail(used===Number(before.size)&&key(before)===key(after)&&key(after)===key(named),
      'unavailable','The retained local verification evidence changed during its read.');
    return bytes.subarray(0,used);
  } finally {closeSync(fd);}
}
function exclusive(path,bytes) {
  const fd=openSync(path,'wx',0o600);try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}
  const owner=openSync(dirname(path),'r');try{fsyncSync(owner);}finally{closeSync(owner);}
}

export function createFabricOutputTransfer({store,outputArtifact,authorizeAccount,admit,binary,index,spawnImpl=spawn}={}) {
  fail(store&&absolute(store.home)&&absolute(binary)&&absolute(index)&&typeof spawnImpl==='function'&&
    [outputArtifact,authorizeAccount,admit].every(value=>typeof value==='function'),
    'unavailable','The local native output verification owners are unavailable.');
  const directory=join(store.home,'local-transfer-verifications'),active=new Map(),children=new Set();let closed=false;

  async function authority(binding) {
    fail(!closed,'unavailable','The local output verification owner is closed.');
    const allowed=await Promise.allSettled(['read','write'].map(permission=>authorizeAccount({...binding,permission})));
    fail(allowed.every(item=>item.status==='fulfilled'&&item.value===true),'admission','Current native read and write access is required for local output verification.');
    fail(!closed,'unavailable','The local output verification owner is closed.');admit();
  }
  function owned(binding,taskId) {
    const scope=store.taskScope(taskId);
    fail(scope&&equal(scope,binding),'not_found','Only the initiating account can verify this destination’s output.');
  }
  function start(args,{ready=false}={}) {
    fail(!closed,'unavailable','The local output verification owner is closed.');
    const child=spawnImpl(binary,args,{stdio:['ignore','pipe','pipe'],env:{PATH:'/usr/bin:/bin:/usr/sbin:/sbin',LANG:'C'}});
    children.add(child);let stdout=[],stderr='',count=0,problem=false,settled=false,readyResolve,readyReject;
    const readyPromise=ready?new Promise((resolve,reject)=>{readyResolve=resolve;readyReject=reject;}):null;
    // A rejected readiness is also observed by completion/cleanup when an
    // abort races before its caller starts awaiting it.
    readyPromise?.catch(()=>{});
    let completionResolve;
    const completion=new Promise(resolve=>{completionResolve=resolve;});
    const stop=()=>{problem=true;try{child.kill('SIGKILL');}catch{}};
    const timer=setTimeout(stop,15000);
    function done(exitCode) {
      if(settled)return;settled=true;clearTimeout(timer);children.delete(child);
      readyReject?.(new FabricActivityError('unavailable','The local native sender did not become ready.'));
      completionResolve({exitCode,stdout:Buffer.concat(stdout),problem});
    }
    child.stdout.on('data',chunk=>{count+=chunk.length;if(count>16384){stop();return;}stdout.push(Buffer.from(chunk));});
    child.stderr.on('data',chunk=>{
      count+=chunk.length;if(count>16384){stop();return;}stderr+=chunk.toString('utf8');
      if(readyResolve){const match=stderr.match(/^SWARM_READY 127\.0\.0\.1 ([1-9][0-9]{0,4})\n$/);
        if(match&&Number(match[1])<=65535){readyResolve(Number(match[1]));readyResolve=null;readyReject=null;}}
    });
    child.on('error',()=>{problem=true;done(null);});child.on('close',code=>done(code));
    return {completion,ready:readyPromise,stop};
  }
  function nativeReceipt(result,grant,direction,state,port) {
    fail(result&&!result.problem&&result.exitCode===0,'unavailable','The native local transfer did not confirm completion.');
    const packet=decode(result.stdout,{maxBytes:16384}),keys=['schema','direction','state','transport','transfer_id','scope','recipient','sha256','size','expires','port','exit_status'];
    fail(Array.isArray(packet)&&packet.length===3&&packet[0]===1&&packet[1]===true&&exact(packet[2],keys),
      'unavailable','The native local transfer receipt is incomplete.');
    const row=packet[2];
    fail(row.schema==='bonfyre.swarm.transfer.v1'&&row.direction===direction&&row.state===state&&row.transport==='loopback-tcp'&&
      row.transfer_id===grant.id&&row.scope===grant.scope&&row.recipient===grant.recipient&&row.sha256===grant.digest&&
      row.size===grant.bytes&&row.expires===grant.expires&&row.port===port&&row.exit_status===0,
      'unavailable','The native local transfer receipt does not match the admitted output.');
    return row;
  }
  function retained(binding,input,key,claim) {
    const saved=store.readReceipt(input.taskId,'model-local-transfer-'+key);
    if(saved){
      const value=saved.value;
      fail(value?.schema===1&&value.requestDigest===claim.requestDigest&&value.transfer?.id===claim.transferId&&equal(value.binding,binding),
        'unavailable','The retained local transfer receipt does not match this request.');
      return {binding,taskId:input.taskId,outputId:input.outputId,requestId:input.requestId,
        transfer:{...value.transfer,receiptId:saved.receipt.receipt_id},replayed:true};
    }
    return {binding,taskId:input.taskId,outputId:input.outputId,requestId:input.requestId,replayed:true,
      transfer:{state:'uncertain',id:claim.transferId,transport:'loopback-tcp',localOnly:true,verificationOnly:true,
        sha256:null,bytes:null,senderConfirmed:false,receiverConfirmed:false,receiptId:null,message}};
  }
  async function perform(binding,input,key,claim,artifact,keyPath,outPath) {
    let seed,fetcher,transfer;
    const grant={id:claim.transferId,scope:hash([1,'fabric-output',binding.subject,binding.scope,input.taskId,input.outputId]),
      recipient:hash([1,'same-destination-local-verifier',binding.actor,binding.subject,binding.scope]),
      digest:artifact.digest,bytes:artifact.bytes,expires:Math.floor(Date.now()/1000)+30};
    try {
      await authority(binding);owned(binding,input.taskId);
      exclusive(keyPath,randomBytes(32));
      const grantArgs=['--scope',grant.scope,'--recipient',grant.recipient,'--sha256',grant.digest,'--size',String(grant.bytes),
        '--transfer-id',grant.id,'--key-file',keyPath,'--expires',String(grant.expires),'--index',index];
      seed=start(['scoped-seed','--artifact',artifact.path,'--port','0',...grantArgs],{ready:true});
      const port=await seed.ready;
      await authority(binding);owned(binding,input.taskId);
      const current=await outputArtifact(binding,input.taskId,input.outputId);
      fail(current.path===artifact.path&&current.digest===artifact.digest&&current.bytes===artifact.bytes,
        'conflict','The admitted output changed before local verification.');
      fail(!closed,'unavailable','The local output verification owner is closed.');
      fetcher=start(['scoped-fetch','--out',outPath,'--port',String(port),...grantArgs]);
      // Observe either failed side immediately so its peer is revoked and
      // joined without waiting for the listener's entire deadline.
      await Promise.all([
        seed.completion.then(result=>nativeReceipt(result,grant,'send','receiver_confirmed',port)),
        fetcher.completion.then(result=>nativeReceipt(result,grant,'receive','stored_confirmed',port))
      ]);
      const receivedBytes=readBounded(outPath,maxBytes);
      fail(receivedBytes.length===artifact.bytes&&sha(receivedBytes)===artifact.digest,
        'unavailable','The local received output did not match its owner receipt.');
      transfer={state:'verified',id:grant.id,transport:'loopback-tcp',localOnly:true,verificationOnly:true,
        sha256:artifact.digest,bytes:artifact.bytes,senderConfirmed:true,receiverConfirmed:true};
    } catch {
      transfer={state:'uncertain',id:grant.id,transport:'loopback-tcp',localOnly:true,verificationOnly:true,
        sha256:artifact.digest,bytes:artifact.bytes,senderConfirmed:false,receiverConfirmed:false,message};
    } finally {
      // Revocation precedes child cleanup. Private transport material and the
      // verification-only received copy never become customer-visible outputs.
      try{unlinkSync(keyPath);}catch{}
      seed?.stop();fetcher?.stop();
      await Promise.allSettled([seed?.completion,fetcher?.completion].filter(Boolean));
      try{unlinkSync(outPath);}catch{}
    }
    const receipt=store.saveReceipt(input.taskId,'model-local-transfer-'+key,
      {schema:1,binding,requestDigest:claim.requestDigest,transfer},'msgpack');
    await authority(binding);owned(binding,input.taskId);
    return {binding,taskId:input.taskId,outputId:input.outputId,requestId:input.requestId,
      transfer:{...transfer,receiptId:receipt.receipt_id},replayed:false};
  }
  async function verify(authorityBinding,input) {
    const binding=fabricActivityBinding(authorityBinding);
    fail(exact(input,['expectedBinding','requestId','taskId','outputId'])&&
      typeof input.requestId==='string'&&/^[A-Za-z0-9_.:-]{8,100}$/.test(input.requestId)&&identity(input.taskId)&&identity(input.outputId),
      'invalid','Use the exact retained output and local verification request.');
    fail(equal(fabricActivityBinding(input.expectedBinding),binding),'conflict','The active destination changed. Reopen the output.');
    await authority(binding);owned(binding,input.taskId);
    const key=hash([1,'local-transfer',binding.actor,binding.subject,binding.scope,input.requestId]).slice(0,48),
      requestDigest=hash([1,input.taskId,input.outputId]);
    privateDirectory(directory);
    const claimPath=join(directory,key+'.msgpack'),keyPath=join(directory,key+'.key'),outPath=join(directory,key+'.received');
    if(existsSync(claimPath)) {
      const claim=decode(readBounded(claimPath,4096),{maxBytes:4096});
      fail(claim?.schema===1&&claim.requestDigest===requestDigest&&/^[a-f0-9]{32}$/.test(claim.transferId),
        'conflict','This local verification identity already names another output.');
      if(active.has(key))await active.get(key);
      await authority(binding);owned(binding,input.taskId);return retained(binding,input,key,claim);
    }
    const artifact=await outputArtifact(binding,input.taskId,input.outputId);
    fail(exact(artifact,['path','digest','bytes'])&&absolute(artifact.path)&&/^[a-f0-9]{64}$/.test(artifact.digest)&&
      Number.isSafeInteger(artifact.bytes)&&artifact.bytes>=0&&artifact.bytes<=maxBytes,
      'unavailable','The retained output owner did not supply an immutable artifact.');
    await authority(binding);owned(binding,input.taskId);
    fail(readdirSync(directory).length<2000,'capacity','The retained local verification limit has been reached.');
    const claim={schema:1,requestDigest,transferId:randomBytes(16).toString('hex')};
    try{exclusive(claimPath,encode(claim));}catch(error){if(error.code==='EEXIST')return verify(binding,input);throw error;}
    const pending=perform(binding,input,key,claim,artifact,keyPath,outPath);active.set(key,pending);
    try{return await pending;}finally{if(active.get(key)===pending)active.delete(key);}
  }
  async function close() {
    closed=true;for(const child of children){try{child.kill('SIGKILL');}catch{}}
    await Promise.allSettled([...active.values()]);
  }
  return Object.freeze({configured:true,verify,close});
}

export function outputTransferFromEnvironment(env=process.env,{root,store,outputArtifact,authorizeAccount,admit,spawnImpl}={}) {
  try {
    const binary=env.BONFYRE_SWARM_BINARY||(root&&join(root,'cmd/BonfyreSwarm/bonfyre-swarm')),
      index=env.BONFYRE_SWARM_INDEX||(env.HOME&&join(env.HOME,'.local/share/bonfyre/swarm.db'));
    fail(absolute(binary)&&absolute(index)&&existsSync(binary)&&lstatSync(binary).isFile()&&
      !lstatSync(binary).isSymbolicLink()&&existsSync(dirname(index))&&lstatSync(dirname(index)).isDirectory(),
      'unavailable','The native local transfer carrier is unavailable.');
    return createFabricOutputTransfer({store,outputArtifact,authorizeAccount,admit,binary,index,...(spawnImpl?{spawnImpl}:{})});
  } catch {
    return Object.freeze({configured:false,verify:async()=>{throw new FabricActivityError('unavailable','Local output verification has no configured native carrier.');},close:async()=>{}});
  }
}
