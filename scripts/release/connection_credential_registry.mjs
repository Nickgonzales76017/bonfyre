import assert from 'node:assert/strict';
const providerId=/^[a-z0-9_-]{1,64}$/;
const fieldId=/^[a-z][a-z0-9_]{0,63}$/;
function parse(body,label){
 const value=JSON.parse(Buffer.from(body).toString('utf8'));assert(Array.isArray(value)&&value.length<=512,label+' must be a bounded provider array.');
 for(const p of value){
  assert(p&&providerId.test(p.provider||'')&&typeof p.name==='string'&&p.name.length>0&&p.name.length<=160,label+' has an invalid provider.');
  assert(Array.isArray(p.fields)&&p.fields.length<=32&&p.fields.every(f=>f&&fieldId.test(f.field||'')&&typeof f.kind==='string'&&typeof f.sensitive==='boolean'&&typeof f.required==='boolean'),label+' has an invalid field declaration.');
  assert(p.apply&&Array.isArray(p.apply.headers)&&Array.isArray(p.apply.query)&&p.verify&&typeof p.verify==='object',label+' has an invalid application/verification contract.');
 }
 return value;
}
export function mergeConnectionCredentialRegistries(baseBody,supplementBody){
 const base=parse(baseBody,'Base credential registry'),supplement=parse(supplementBody,'Connection credential supplement'),seen=new Set(base.map(p=>p.provider));
 for(const p of supplement){assert(!seen.has(p.provider),'Connection credential supplement duplicates '+p.provider);seen.add(p.provider);}
 const merged=[...base,...supplement];assert(new Set(merged.map(p=>p.provider)).size===merged.length,'Merged credential registry contains duplicates.');
 return Buffer.from(JSON.stringify(merged,null,4)+'\n');
}
