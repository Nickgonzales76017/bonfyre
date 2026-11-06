import {existsSync,statSync} from 'node:fs';
import {dirname,isAbsolute,join} from 'node:path';
import {decode} from '../browser/workspace-msgpack.mjs';
import {runAccountAuthority} from './account_authority.mjs';

// Same bounded raw-byte subprocess carrier as the current-account check. It
// passes no browser/session credential or ambient loader configuration.
export const runNativeMeter=runAccountAuthority;
export class NativeMeterUnavailable extends Error {
  constructor(code='meter_unavailable',recordingMayHaveSucceeded=false) {
    super(code==='meter_refused'?'The native meter refused this physical-attempt identity.':'The native meter observation is unavailable.');
    this.code=code;this.recordingMayHaveSucceeded=recordingMayHaveSucceeded;
  }
}
const object=value=>value&&typeof value==='object'&&!Array.isArray(value);
const exact=(value,keys)=>object(value)&&Object.keys(value).length===keys.length&&Object.keys(value).every(key=>keys.includes(key));
const text=(value,max=512)=>typeof value==='string'&&value.length>0&&Buffer.byteLength(value)<=max&&!/[\x00-\x1f\x7f]/.test(value);
const utc=value=>typeof value==='string'&&/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(value)&&Number.isFinite(Date.parse(value))&&Date.parse(value)>=0&&new Date(value).toISOString().replace('.000Z','Z')===value;
const bound=(value,max)=>Number.isSafeInteger(value)&&value>=0&&value<=max;
const path=value=>text(value,4095)&&isAbsolute(value);
const reference=value=>text(value)&&!value.startsWith('/')&&!value.includes('\\')&&!/^file:/i.test(value);
const units=new Set(['call','credit','query','byte','megabyte','gigabyte','millisecond','second']);
const quoteKeys=['provider','operation','at'];
const recordKeys=['key','operation','provider','receiptRef','attemptRef','realizationRef','at','bytes','durationMs'];
const priceKeys=['state','rateId','amount','currency','unit','effectiveAt','evidenceRef'];
function valid(ok) {if(!ok)throw new TypeError('Use the admitted native physical-attempt fields.');}
function checkPrice(value,usage=false) {
  if(exact(value,['state'])&&value.state==='unknown')return {state:'unknown'};
  if(!exact(value,usage?[...priceKeys,'cost']:priceKeys)||value.state!=='priced'||!Number.isSafeInteger(value.rateId)||value.rateId<=0||
    !Number.isFinite(value.amount)||value.amount<0||typeof value.currency!=='string'||!/^[A-Z]{3}$/.test(value.currency)||
    !units.has(value.unit)||!utc(value.effectiveAt)||!reference(value.evidenceRef)||usage&&(!Number.isFinite(value.cost)||value.cost<0))
    throw new NativeMeterUnavailable();
  return {...value};
}
export function createNativeMeter({binary,db,run=runNativeMeter}={}) {
  valid(path(binary)&&path(db)&&typeof run==='function');
  async function invoke(mode,args,recording=false) {
    let result,packet;
    try {
      result=await run([binary,mode,'--db',db,...args],{timeoutMs:recording?10000:2000,maxBytes:16384});
      if(!(result?.stdout instanceof Uint8Array))throw new Error('raw packet required');
      packet=decode(result.stdout,{maxBytes:16384});
    }catch{throw new NativeMeterUnavailable('meter_unavailable',recording);}
    if(!Array.isArray(packet)||packet.length!==3||packet[0]!==1||typeof packet[1]!=='boolean'||!object(packet[2]))
      throw new NativeMeterUnavailable('meter_unavailable',recording);
    if(result.exitCode!==0||packet[1]!==true) {
      const refused=result.exitCode===77&&packet[1]===false&&packet[2].code==='record_refused';
      throw new NativeMeterUnavailable(refused?'meter_refused':'meter_unavailable',recording);
    }
    return packet[2];
  }
  async function quote(input) {
    valid(exact(input,quoteKeys)&&text(input.provider)&&text(input.operation)&&utc(input.at));
    const row=await invoke('quote-packet',['--provider',input.provider,'--op',input.operation,'--at',input.at]);
    if(!exact(row,['provider','operation','at','pricing'])||quoteKeys.some(key=>row[key]!==input[key]))throw new NativeMeterUnavailable();
    const pricing=checkPrice(row.pricing);
    if(pricing.state==='priced'&&pricing.effectiveAt>input.at)throw new NativeMeterUnavailable();
    return pricing;
  }
  async function record(input) {
    valid(exact(input,recordKeys)&&recordKeys.filter(key=>!['bytes','durationMs'].includes(key)).every(key=>text(input[key]))&&
      /^(?:user:[1-9][0-9]{0,9}|(?:institution|household):[a-z0-9-]{1,63})$/.test(input.key)&&utc(input.at)&&
      ['receiptRef','attemptRef','realizationRef'].every(key=>reference(input[key]))&&
      bound(input.bytes,64*1024*1024)&&bound(input.durationMs,3600000));
    const row=await invoke('record-packet',[
      '--key',input.key,'--op',input.operation,'--provider',input.provider,
      '--receipt',input.receiptRef,'--attempt-ref',input.attemptRef,'--realization-ref',input.realizationRef,
      '--at',input.at,'--bytes',String(input.bytes),'--duration',String(input.durationMs)
    ],true);
    if(!exact(row,['state','eventId','key','operation','provider','attemptRef','receiptRef','realizationRef','at','outputBytes','durationMs','pricing'])||
      row.state!=='recorded'||!Number.isSafeInteger(row.eventId)||row.eventId<=0||
      ['key','operation','provider','attemptRef','receiptRef','realizationRef','at','durationMs'].some(key=>row[key]!==input[key])||row.outputBytes!==input.bytes)
      throw new NativeMeterUnavailable('meter_unavailable',true);
    let pricing;try{pricing=checkPrice(row.pricing,true);}catch{throw new NativeMeterUnavailable('meter_unavailable',true);}
    if(pricing.state==='priced'&&pricing.effectiveAt>input.at)throw new NativeMeterUnavailable('meter_unavailable',true);
    return {...row,pricing};
  }
  return Object.freeze({configured:true,quote,record});
}

// Follow the native bf_state_path coordinate order; never create a second
// meter under Fabric home just because that directory is convenient.
function meterStore(env) {
  if(env.BONFYRE_METER_DB)return env.BONFYRE_METER_DB;
  const candidates=[env.BONFYRE_STATE_ROOT&&join(env.BONFYRE_STATE_ROOT,'state/bonfyre-meter.db'),
    '/var/lib/bonfyre/state/bonfyre-meter.db',env.HOME&&join(env.HOME,'.local/share/bonfyre/state/bonfyre-meter.db')].filter(Boolean);
  return candidates.find(candidate=>{try{return existsSync(dirname(candidate))&&statSync(dirname(candidate)).isDirectory();}catch{return false;}});
}
export function nativeMeterFromEnvironment(env=process.env,{root,run}={}) {
  try{return createNativeMeter({binary:env.BONFYRE_METER_BINARY||(root&&join(root,'cmd/BonfyreMeter/bonfyre-meter')),db:meterStore(env),...(run?{run}:{})});}
  catch{
    const unavailable=async()=>{throw new NativeMeterUnavailable();};
    return Object.freeze({configured:false,quote:unavailable,record:unavailable});
  }
}
