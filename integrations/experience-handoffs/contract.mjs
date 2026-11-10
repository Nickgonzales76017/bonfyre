import {createHash} from 'node:crypto';
export const VERSION='aurekai.handoff.v1';
export const sha256=bytes=>createHash('sha256').update(bytes).digest('hex');
export const validId=s=>typeof s==='string'&&/^[a-z0-9][a-z0-9-]{0,62}$/.test(s)&&!s.endsWith('-');
export const validSha=s=>typeof s==='string'&&/^[a-f0-9]{64}$/.test(s);
const forbidden=/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/;
export function atom(value){
  if(typeof value!=='string'&&!Number.isSafeInteger(value))throw Error('Invalid handoff atom.');
  const s=String(value);
  if(s.length>60000||forbidden.test(s))throw Error('Invalid handoff text.');
  return '"'+s.replaceAll('\\','\\\\').replaceAll('"','\\"').replaceAll('\n','\\n').replaceAll('\r','\\r').replaceAll('\t','\\t')+'"';
}
export function line(key,...values){
  if(!/^[a-z][a-z0-9_]{0,63}$/.test(key)||!values.length||values.length>20)throw Error('Invalid field.');
  return key+' '+values.map(atom).join(' ')+'\n';
}
export function decode(text){
  if(typeof text!=='string'||Buffer.byteLength(text)>262144||!text.endsWith('\n'))throw Error('Invalid handoff size.');
  const rows=new Map(),lines=text.trimEnd().split('\n');
  if(lines.shift()!=='handoff '+VERSION)throw Error('Invalid handoff version.');
  for(const raw of lines){
    const i=raw.indexOf(' '),key=raw.slice(0,i),args=raw.slice(i+1);
    if(i<1||!/^[a-z][a-z0-9_]{0,63}$/.test(key))throw Error('Invalid handoff key.');
    const parts=args.match(/"(?:\\.|[^"\\])*"/g)||[];
    if(!parts.length||parts.join(' ')!==args)throw Error('Invalid handoff values.');
    const values=parts.map(v=>{
      const decoded=v.slice(1,-1).replace(/\\([nrt"\\])/g,(_,c)=>({n:'\n',r:'\r',t:'\t','"':'"','\\':'\\'})[c]);
      if(/\\/.test(decoded.replaceAll('\\\\',''))||decoded.length>60000||forbidden.test(decoded))throw Error('Invalid escape.');
      return decoded;
    });
    rows.set(key,[...(rows.get(key)||[]),values]);
    if(rows.size>64||rows.get(key).length>500)throw Error('Handoff exceeds limits.');
  }
  return rows;
}
export function one(rows,key){const a=rows.get(key);if(a?.length!==1||a[0].length!==1)throw Error('Missing or duplicate '+key);return a[0][0];}
