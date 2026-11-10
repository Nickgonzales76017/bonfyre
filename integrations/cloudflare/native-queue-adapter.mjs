import {existsSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';

export const nativeMission=({mission,family,digest})=>'edge-'+createHash('sha256').update([family,mission,digest].join('\n')).digest('hex');
export function queueOwner({binary,db,timeoutMs=10000}) {
  if(!binary||!db||!existsSync(binary)||!Number.isSafeInteger(timeoutMs)||timeoutMs<1) throw Error('native queue owner unavailable');
  async function run(args,{missing=false}={}) {
    const result=await new Promise((resolve,reject)=>{
      const child=spawn(binary,[...args,'--db',db],{stdio:['ignore','pipe','pipe'],shell:false});
      let stdout='',stderr='',fault=null;
      const timer=setTimeout(()=>{fault=Error('native queue timed out');child.kill('SIGKILL');},timeoutMs);
      child.stdout.on('data',b=>{stdout+=b;if(Buffer.byteLength(stdout)>262144){fault=Error('native queue output exceeds limit');child.kill('SIGKILL');}});
      child.stderr.on('data',b=>{stderr=(stderr+b).slice(-4096);});
      child.on('error',e=>{clearTimeout(timer);reject(e);});
      child.on('close',code=>{clearTimeout(timer);fault?reject(fault):resolve({code,stdout,stderr});});
    });
    if(missing&&result.code===1&&!result.stdout.trim()) return [];
    if(result.code!==0) throw Error(`native queue ${args[0]} failed (${result.code})`);
    return result.stdout.trim().split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));
  }
  const read=mission=>run(['mission',mission],{missing:true});
  async function enqueue(input) {
    const {mission,family,digest,ifAbsent}=input;
    if(ifAbsent!==true||!/^[-a-z0-9.:]{3,120}$/.test(mission)||family!=='source-sync'||!/^[a-f0-9]{64}$/.test(digest)) throw Error('invalid queue intent');
    const id=nativeMission(input),uri='urn:sha256:'+digest;
    const result=await run(['enqueue','external.source.digest',uri,'--family',family,'--mission',id,'--if-absent']);
    if(result.length!==1||result[0].result!=='ok') throw Error('native queue did not admit work');
    const rows=await read(id),row=rows[0];
    if(rows.length!==1||row.node_id!=='job'||row.family!==family||row.input_uri!==uri||row.operator_id!=='queue.external.source.digest') throw Error('native admission readback mismatch');
    return {accepted:true,receipt:{mission:id,node:'job',sourceMission:mission,digest,status:row.status}};
  }
  return {run,read,enqueue};
}
export const nativeEnqueue=options=>queueOwner(options).enqueue;
