import {spawn} from 'node:child_process';
import {isAbsolute} from 'node:path';
import {decode} from '../browser/workspace-msgpack.mjs';

export class AccountAuthorityUnavailable extends Error {
  constructor() { super('The current native account authority is unavailable.'); this.code='authority_unavailable'; }
}
function binding(value) {
  if (!value || typeof value!=='object' || Array.isArray(value) ||
      Object.keys(value).length!==4 || !['read','write'].includes(value.permission) ||
      typeof value.actor!=='string' || !/^user:[1-9][0-9]{0,9}$/.test(value.actor) || Number(value.actor.slice(5))>2147483647 ||
      typeof value.subject!=='string' || (value.scope==='self' ? value.subject!==value.actor :
        !['institution','household'].includes(value.scope) || !new RegExp(`^${value.scope}:[a-z0-9-]{1,63}$`).test(value.subject)))
    throw new TypeError('Use the admitted native actor, destination and permission.');
  return value;
}
// A private local process boundary. Native authority receives no session token,
// and its fresh projection cannot itself admit work or change a grant.
export function runAccountAuthority(command,{timeoutMs=5000,maxBytes=4096}={}) {
  return new Promise((resolve,reject)=>{
    let child,settled=false,used=0;const chunks=[];
    const finish=(error,result)=>{if(settled)return;settled=true;clearTimeout(timer);error?reject(error):resolve(result);};
    let timer;
    try { child=spawn(command[0],command.slice(1),{stdio:['ignore','pipe','ignore'],env:{PATH:'/usr/bin:/bin'},windowsHide:true}); }
    catch { finish(new AccountAuthorityUnavailable());return; }
    timer=setTimeout(()=>{child.kill('SIGKILL');finish(new AccountAuthorityUnavailable());},timeoutMs);
    child.on('error',()=>finish(new AccountAuthorityUnavailable()));
    child.stdout.on('data',chunk=>{used+=chunk.length;if(used>maxBytes){child.kill('SIGKILL');finish(new AccountAuthorityUnavailable());}else chunks.push(chunk);});
    child.on('close',code=>finish(null,{exitCode:code,stdout:Buffer.concat(chunks)}));
  });
}
export function createAccountAuthorizer({authBinary,authDb,cmsDb,run=runAccountAuthority,now=()=>Date.now()/1000}={}) {
  if (![authBinary,authDb,cmsDb].every(path=>typeof path==='string'&&isAbsolute(path)&&!/[\x00-\x1f\x7f]/.test(path)) || typeof run!=='function')
    throw new TypeError('Configure the trusted native authority paths.');
  return async function authorize(input) {
    const {actor,subject,scope,permission}=binding(input);
    let result,packet;
    try {
      result=await run([authBinary,'--db',authDb,'background-access','--actor',actor,'--subject',subject,'--scope',scope,'--permission',permission,'--cms-db',cmsDb],{timeoutMs:5000,maxBytes:4096});
      if (result?.exitCode!==0 || !(result.stdout instanceof Uint8Array)) throw new AccountAuthorityUnavailable();
      packet=decode(result.stdout,{maxBytes:4096});
    } catch { throw new AccountAuthorityUnavailable(); }
    const row=packet?.[2],at=now();
    if (!Array.isArray(packet)||packet.length!==3||packet[0]!==1||packet[1]!==true||!row||typeof row!=='object'||Array.isArray(row)||
        Object.keys(row).length!==6||row.actor!==actor||row.subject!==subject||row.scope!==scope||row.permission!==permission||typeof row.allowed!=='boolean'||
        !Number.isSafeInteger(row.checked_at)||row.checked_at<at-10||row.checked_at>at+2) throw new AccountAuthorityUnavailable();
    return row.allowed;
  };
}
export function accountAuthorizerFromEnvironment(env=process.env,options={}) {
  try {
    return createAccountAuthorizer({...options,authBinary:env.BONFYRE_AUTH_BINARY,authDb:env.BONFYRE_AUTH_DB,cmsDb:env.BONFYRE_CMS_DB});
  } catch {
    return async()=>{throw new AccountAuthorityUnavailable();};
  }
}
