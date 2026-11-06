import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
export const roomQuantityBase='a1c9968a938333e9c8e10f502d45347d39374e8e9fb6220ecf8858a6d41085f8';
const hash=b=>createHash('sha256').update(b).digest('hex');
function once(text,before,after){assert.equal(text.split(before).length,2,'Quantity preimage changed: '+before);return text.replace(before,after);}
export function roomQuantityRepairFiles(original){
 const files=new Map(original),projections=[];
 for(const [entry,kind,count] of [['catalogue/index.html','catalogue',44],['product-room/index.html','planner',29]]){
  const html=original.get(entry).toString(),prior=html.match(new RegExp('data/ui-[a-f0-9]{64}/'+kind+'/'))?.[0];assert(prior);
  const members=new Map([...original].filter(([n])=>n.startsWith(prior)).map(([n,b])=>[n.slice(prior.length),b]));assert.equal(members.size,count);
  members.set('catalogue-room-model.mjs',readFileSync(new URL('../components/BonfyreServiceSite/catalogue-room-model.mjs',import.meta.url)));
  for(const name of ['catalogue-product-scene.mjs','planner-product-scene.mjs']){
   let t=members.get(name).toString();t=once(t,"import {validatePlan,","import {planItemLimit,validatePlan,");t=once(t,'max:12','max:planItemLimit');
   if(name==='catalogue-product-scene.mjs'){t=once(t,'n>12','n>planItemLimit');t=once(t,'.cr-heading button{','.cr-controls input,.cr-controls select{width:100%;min-width:0;box-sizing:border-box}.cr-heading button{');}
   members.set(name,Buffer.from(t));
  }
  const identity=Object.fromEntries([...members].sort(([a],[b])=>a.localeCompare(b,'en')).map(([n,b])=>[n,hash(b)])),prefix='data/ui-'+hash(JSON.stringify(identity))+'/'+kind+'/';
  for(const[n,b]of members){assert(!original.has(prefix+n));assert(!b.includes(Buffer.from(prior)));files.set(prefix+n,b);}
  files.set(entry,Buffer.from(html.replaceAll(prior,prefix)));projections.push({entry,prior,prefix,files:members.size});
 }
 for(const[n,b]of original)if(!projections.some(p=>p.entry===n))assert(b.equals(files.get(n)),'Unrelated package member changed: '+n);
 return {files,projections};
}
export async function materializeRoomQuantityRepair({root,base=roomQuantityBase}){
 assert.equal(base,roomQuantityBase);const {files,projections}=roomQuantityRepairFiles(await loadWorkspaceFiles(root,base));
 return {...await retainWorkspaceMaterial(files,{root}),base,projections};
}
