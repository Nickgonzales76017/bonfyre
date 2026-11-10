// Narrow native carrier for the existing Fabric process. The service credential
// authenticates WebAddress's freshly resolved actor/subject/scope, never a browser.
import {encode,decode,maxPacket} from '../browser/workspace-msgpack.mjs';
import {equal} from './http_boundary.mjs';
import {FabricActivityError,fabricActivityBinding} from './fabric_store.mjs';

export async function readFabricPacket(stream,{limit=maxPacket,timeoutMs=8000}={}) {
  if(!stream?.getReader)throw new FabricActivityError('invalid','A native packet is required.');
  const reader=stream.getReader(),parts=[];let size=0,timer;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>reject(new FabricActivityError('unavailable','The native packet did not finish.')),timeoutMs);});
  try{for(;;){const {value,done}=await Promise.race([reader.read(),deadline]);if(done)break;
    if(!(value instanceof Uint8Array)||(size+=value.length)>limit)throw new FabricActivityError('invalid','The native packet exceeds its bound.');parts.push(value);}
    const bytes=new Uint8Array(size);let at=0;for(const part of parts){bytes.set(part,at);at+=part.length;}
    try{return decode(bytes,{maxBytes:limit});}catch{throw new FabricActivityError('invalid','The native MessagePack packet is incomplete.');}
  }finally{clearTimeout(timer);try{reader.cancel()?.catch(()=>{});}catch{}try{reader.releaseLock();}catch{}}
}
const fields=(value,allowed)=>value && typeof value==='object' && !Array.isArray(value) && Object.keys(value).every(key=>allowed.includes(key));
const reply=(status,packet)=>new Response(encode(packet),{status,headers:{'Content-Type':'application/msgpack','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'}});
export function createFabricActivityBoundary({token,read,notification,push,model,maxConcurrent=4}) {
  if(typeof token!=='string'||!token||typeof read!=='function'||typeof notification!=='function')throw new Error('Fabric activity boundary is incomplete');
  let active=0;
  return async request=>{
    const refuse=(status,code,message)=>reply(status,[1,false,{code,message}]);
    if(!equal(request.headers.get('authorization')||'',`Bearer ${token}`))return refuse(401,'unauthorized','The native Fabric caller is not authenticated.');
    if(request.headers.has('origin')||request.headers.has('cookie')||request.headers.has('sec-fetch-site'))return refuse(403,'forbidden','Use the current native workspace boundary.');
    if(request.method!=='POST'||new URL(request.url).search||request.headers.get('content-type')?.split(';')[0]!=='application/msgpack')return refuse(400,'invalid','Use the declared native activity packet.');
    const length=request.headers.get('content-length');if(length!==null&&(!/^\d+$/.test(length)||Number(length)>65536))return refuse(400,'invalid','The native request exceeds its bound.');
    if(active>=maxConcurrent)return refuse(503,'unavailable','The Fabric activity reader is busy.');active++;
    try{
      const packet=await readFabricPacket(request.body,{limit:65536});
      if(!Array.isArray(packet)||packet.length!==2||packet[0]!==1||!fields(packet[1],['operation','actor','subject','scope','payload','task']))throw new FabricActivityError('invalid','Use the declared native activity coordinates.');
      const input=packet[1],binding=fabricActivityBinding({actor:input.actor,subject:input.subject,scope:input.scope});let value;
      if(input.operation==='read'&&!Object.hasOwn(input,'payload')&&(!Object.hasOwn(input,'task')||typeof input.task==='string'&&/^[a-f0-9]{24}$/.test(input.task)))value=await read(binding,{taskId:input.task||''});
      else if(input.operation==='notification'&&Object.hasOwn(input,'payload')&&!Object.hasOwn(input,'task'))value=await notification(binding,input.payload);
      else if(input.operation==='push-status'&&!Object.hasOwn(input,'payload')&&!Object.hasOwn(input,'task')||
        ['push-subscribe','push-unsubscribe','push-ack','push-resolve'].includes(input.operation)&&Object.hasOwn(input,'payload')&&!Object.hasOwn(input,'task')){
        if(typeof push!=='function')throw new FabricActivityError('unavailable','Browser notification delivery is not connected.');
        value=await push(binding,input.operation,input.payload);
      }
      else if(input.operation==='model-capabilities'&&!Object.hasOwn(input,'payload')&&!Object.hasOwn(input,'task')||
        input.operation==='model-read'&&!Object.hasOwn(input,'payload')&&typeof input.task==='string'&&/^[a-f0-9]{24}$/.test(input.task)||
        ['model-review','model-submit','model-cancel','model-reconcile','model-verify-transfer'].includes(input.operation)&&Object.hasOwn(input,'payload')&&!Object.hasOwn(input,'task')){
        if(typeof model!=='function')throw new FabricActivityError('unavailable','Model execution is not connected.');
        value=await model(binding,input.operation,input.payload,{taskId:input.task});
      }
      else throw new FabricActivityError('invalid','Choose one exact native activity operation.');
      return reply(200,[1,true,value]);
    }catch(error){
      const known=error instanceof FabricActivityError,code=known?error.code:'unavailable';
      return refuse(({invalid:400,not_found:404,conflict:409,capacity:429,admission:403,access_required:403,binding_changed:409,expired:409,paused:423})[code]||503,code,known?error.message:'The Fabric activity owner could not confirm this read or action.');
    }finally{active--;}
  };
}
