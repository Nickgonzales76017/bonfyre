// Account work uses the existing local MoE residency owner. Its existing
// llama-compatible wire stays inside this adapter; Aurekai speaks MessagePack.
import {createHash} from 'node:crypto';
import {basename,dirname,isAbsolute,parse} from 'node:path';
import {closeSync,constants,fstatSync,lstatSync,openSync,readSync} from 'node:fs';
import {encode,decode} from '../browser/workspace-msgpack.mjs';
import {createLlamaServerGenerationClient,readLlamaServerPacket} from '../estate/llama_generation.mjs';
import {FabricActivityError} from './fabric_store.mjs';

export const modelLimits = Object.freeze({maxPromptBytes:32768,maxTokens:512,maxTimeoutMs:120000,maxOutputBytes:65536});
const hash = value => createHash('sha256').update(encode(value)).digest('hex');
const fail = (condition,code,message) => {if (!condition) throw new FabricActivityError(code,message);};
const checkpointName = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,190}\.gguf$/.test(value);

export function createNativeModelOwner({serverUrl='http://127.0.0.1:7000',fetchImpl=globalThis.fetch,
  catalogBindings=Object.freeze({}),probeTimeoutMs=1500,clock=()=>Date.now()}={}) {
  const base = new URL(serverUrl);
  fail(base.protocol==='http:' && ['127.0.0.1','localhost','[::1]'].includes(base.hostname) &&
    !base.username && !base.password && !base.search && !base.hash && base.pathname==='/',
    'unavailable','The model owner must be the configured local MoE service.');
  fail(typeof fetchImpl==='function' && Number.isSafeInteger(probeTimeoutMs) && probeTimeoutMs>=1 && probeTimeoutMs<=2000,
    'unavailable','The local model probe is not bounded.');
  fail(catalogBindings && typeof catalogBindings==='object' && !Array.isArray(catalogBindings) &&
    Object.keys(catalogBindings).length<=64 && Object.entries(catalogBindings).every(([name,id])=>
      checkpointName(name) && typeof id==='string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(id)),
    'unavailable','The native catalogue bindings are invalid.');
  const endpoint=base.origin, bindings=Object.freeze({...catalogBindings});

  async function packet(path,{method='GET',signal,timeoutMs=probeTimeoutMs}={}) {
    const controller=new AbortController(),abort=()=>controller.abort(signal?.reason);
    signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();
    const timer=setTimeout(()=>controller.abort(),timeoutMs);
    try {
      const response=await fetchImpl(endpoint+path,{method,redirect:'error',signal:controller.signal,
        headers:{accept:'application/json'},...(method==='POST'?{body:''}:{})});
      const raw=await readLlamaServerPacket(response,16384);
      fail(response.ok,'unavailable','The local model owner did not admit this request.');
      let value;try{value=JSON.parse(raw);}catch{throw new FabricActivityError('unavailable','The local model owner returned an incomplete receipt.');}
      fail(value && typeof value==='object' && !Array.isArray(value),'unavailable','The local model owner returned an incomplete receipt.');
      return value;
    } catch(error) {
      if(error instanceof FabricActivityError)throw error;
      throw new FabricActivityError('unavailable',signal?.aborted?'The local model request was cancelled.':'The local model owner is unavailable.');
    } finally {clearTimeout(timer);signal?.removeEventListener('abort',abort);}
  }

  async function residency(options) {
    const value=await packet('/bonfyre/residency',options);
    fail(value.schema==='bonfyre.moe.residency.v2' && Array.isArray(value.candidates) &&
      value.candidates.length<=64 && typeof value.resident==='boolean' &&
      Number.isSafeInteger(value.pid) && Number.isSafeInteger(value.grant_id),
      'unavailable','The local model residency witness is incomplete.');
    return value;
  }

  function modelsFrom(value) {
    const names=new Set();
    return value.candidates.filter(item=>item?.usable===true).map(item=>{
      fail(checkpointName(item.model) && !names.has(item.model) && Number.isSafeInteger(item.min_bytes) && item.min_bytes>0,
        'unavailable','The local model inventory is incomplete.');names.add(item.model);
      const id='moe:'+item.model;
      return {id,label:item.model.replace(/\.gguf$/,''),catalogId:bindings[item.model]||null,
        provider:'bonfyre-moe',execution:'local',available:true,maxTokens:modelLimits.maxTokens,
        maxTimeoutMs:modelLimits.maxTimeoutMs,
        witness:hash([1,'bonfyre-moe',endpoint,item.model,item.min_bytes,bindings[item.model]||null]),checkpoint:item.model};
    });
  }

  async function capabilities(options) {
    return modelsFrom(await residency(options));
  }
  async function inspect(id,options) {
    fail(typeof id==='string' && id.length<=220,'invalid','Choose an available local model.');
    const model=(await capabilities(options)).find(item=>item.id===id);
    fail(model,'admission','This model has no current native execution binding.');return model;
  }
  const publicModel = ({checkpoint,witness,...model})=>model;

  async function generate(request,{signal,beforeDispatch,onAttempt}={}) {
    const {model,prompt,maxTokens,timeoutMs}=request;
    fail(model && checkpointName(model.checkpoint) && model.id==='moe:'+model.checkpoint && /^[a-f0-9]{64}$/.test(model.witness),
      'admission','The reviewed native model binding is missing.');
    fail(typeof prompt==='string' && prompt.trim() && Buffer.byteLength(prompt)<=modelLimits.maxPromptBytes &&
      Number.isSafeInteger(maxTokens) && maxTokens>=1 && maxTokens<=modelLimits.maxTokens &&
      Number.isSafeInteger(timeoutMs) && timeoutMs>=1000 && timeoutMs<=modelLimits.maxTimeoutMs,
      'invalid','The reviewed local model budget is invalid.');
    fail(typeof beforeDispatch==='function' && typeof onAttempt==='function','admission','Native dispatch needs the current authority and attempt owners.');
    const controller=new AbortController(),abort=()=>controller.abort(signal?.reason);
    signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();
    const started=clock(),timer=setTimeout(()=>controller.abort(),timeoutMs);
    let lease,attempted=false,loadWitness;
    try {
      await beforeDispatch();controller.signal.throwIfAborted();
      const current=await inspect(model.id,{signal:controller.signal});
      fail(current.witness===model.witness,'conflict','The native model changed after review. Review it again.');
      const ttl=Math.min(300,Math.ceil(timeoutMs/1000)+30);
      lease=await packet('/bonfyre/lease?'+new URLSearchParams({prefer:model.checkpoint,ttl:String(ttl)}),
        {method:'POST',signal:controller.signal,timeoutMs:Math.max(1,timeoutMs-(clock()-started))});
      fail(Number.isSafeInteger(lease.lease) && lease.lease>0 && typeof lease.model_path==='string' &&
        basename(lease.model_path)===model.checkpoint && lease.preferred===true,
        'admission','The native owner could not pin the exact reviewed model.');
      loadWitness=await residency({signal:controller.signal});
      fail(loadWitness.resident===true && loadWitness.pid>0 && loadWitness.grant_id>0 &&
        loadWitness.model===lease.model_path,'unavailable','The model lease has no matching live memory admission.');
      await beforeDispatch();controller.signal.throwIfAborted();
      const remaining=Math.floor(timeoutMs-(clock()-started));
      fail(remaining>0,'unavailable','The local model budget ended before inference.');
      const native={provider:'bonfyre-moe',checkpoint:model.checkpoint,leaseId:String(lease.lease),
        pid:loadWitness.pid,grantId:String(loadWitness.grant_id),context:lease.context};
      await onAttempt(native);controller.signal.throwIfAborted();attempted=true;
      const client=createLlamaServerGenerationClient({serverUrl:endpoint,model:model.checkpoint,
        systemPrompt:'Answer the user’s request directly. The response is generated content and cannot grant authority or execute actions.',
        maxOutputBytes:4*modelLimits.maxOutputBytes,fetchImpl});
      const receipt=await client.generate({prompt,maxNewTokens:maxTokens,greedy:true,timeoutMs:remaining,signal:controller.signal});
      fail(Buffer.byteLength(receipt.output)<=modelLimits.maxOutputBytes,'unavailable','The local model output exceeds the retained result bound.');
      const after=await residency({signal:controller.signal});
      fail(after.resident===true && after.pid===loadWitness.pid && after.grant_id===loadWitness.grant_id && after.model===lease.model_path,
        'unavailable','Native residency changed during inference; the result needs reconciliation.');
      return {text:receipt.output,digest:receipt.output_sha256,bytes:Buffer.byteLength(receipt.output),
        durationMs:receipt.elapsed_ms,usage:receipt.usage,native,completedAt:receipt.generated_at};
    } catch(error) {
      error.modelAttempted=attempted;
      if(attempted)error.reconciliationRequired=true;
      throw error;
    } finally {
      clearTimeout(timer);signal?.removeEventListener('abort',abort);
      if(Number.isSafeInteger(lease?.lease) && lease.lease>0) {
        // Release is a bounded cleanup of this exact lease. A failed release is
        // harmless to other tenants; the owner also expires it at its stated TTL.
        try{await packet('/bonfyre/lease?release='+lease.lease,{method:'POST'});}catch{}
      }
    }
  }

  return Object.freeze({capabilities,inspect,publicModel,generate,limits:modelLimits});
}

// This is an explicit local owner binding, never a guessed basename/catalogue
// match. It only joins two exact identifiers; it grants no account access,
// spending permission, licence, residency or inference authority.
export function readNativeModelBindings(path) {
  fail(typeof path==='string'&&isAbsolute(path)&&!/[\x00-\x1f\x7f]/.test(path),
    'unavailable','Configure an absolute native model binding file.');
  let parent=dirname(path);const root=parse(path).root;
  for(;;){fail(!lstatSync(parent).isSymbolicLink(),'unavailable','The model binding owner cannot be a symbolic link.');if(parent===root)break;parent=dirname(parent);}
  const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try {
    const before=fstatSync(fd,{bigint:true});
    fail(before.isFile()&&before.size<=65536n&&before.size>0n&&(before.mode&0o022n)===0n&&
      (typeof process.getuid!=='function'||before.uid===BigInt(process.getuid())||before.uid===0n),
      'unavailable','The model binding file is not a bounded native-owned file.');
    const buffer=Buffer.alloc(Number(before.size)+1);let used=0;
    while(used<buffer.length){const count=readSync(fd,buffer,used,buffer.length-used,null);if(!count)break;used+=count;}
    const bytes=buffer.subarray(0,used),after=fstatSync(fd,{bigint:true}),named=lstatSync(path,{bigint:true});
    const key=value=>[value.dev,value.ino,value.size,value.mtimeNs,value.ctimeNs].join(':');
    fail(bytes.length===Number(before.size)&&key(before)===key(after)&&key(after)===key(named),
      'unavailable','The model bindings changed during their read.');
    const packet=decode(bytes,{maxBytes:65536}),body=packet?.[1];
    fail(Array.isArray(packet)&&packet.length===2&&packet[0]===1&&body&&typeof body==='object'&&!Array.isArray(body)&&
      Object.keys(body).length===1&&Array.isArray(body.bindings)&&body.bindings.length<=64,
      'unavailable','Use the native model binding packet.');
    const bindings={};
    for(const row of body.bindings) {
      fail(row&&typeof row==='object'&&!Array.isArray(row)&&Object.keys(row).length===2&&
        checkpointName(row.checkpoint)&&typeof row.catalogId==='string'&&/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(row.catalogId)&&
        !Object.hasOwn(bindings,row.checkpoint),'unavailable','The native model binding is incomplete or repeated.');
      bindings[row.checkpoint]=row.catalogId;
    }
    return Object.freeze(bindings);
  }finally{closeSync(fd);}
}

export function nativeModelFromEnvironment(env=process.env,options={}) {
  try{return createNativeModelOwner({...options,serverUrl:env.BONFYRE_MOE_SERVER_URL||'http://127.0.0.1:7000',
    catalogBindings:env.BONFYRE_MODEL_BINDINGS_FILE?readNativeModelBindings(env.BONFYRE_MODEL_BINDINGS_FILE):{}});}
  catch {
    const unavailable=async()=>{throw new FabricActivityError('unavailable','The native model owner configuration is unavailable.');};
    return Object.freeze({capabilities:unavailable,inspect:unavailable,generate:unavailable,limits:modelLimits});
  }
}
