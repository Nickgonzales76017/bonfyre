// Narrow repair of the retained spaces package; historical asset URLs remain immutable.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
export const roomScaleBase='a5b379ab187e523c3d1eb090a33d014287491c4d37594520bbb7b4c151183e5a';
const hash=body=>createHash('sha256').update(body).digest('hex');
const allowed={
 'planner-spatial.mjs':['65f5a3d5f3beb2e372b343ed878599e76b78c5dd4ec7312e9ea0f292c28cc897'],
 'planner-model.mjs':['dd0717ae279e10282ba51d4ebe5d8b5baee84583a15e91907c5e5a8712807c04'],
 'planner-spatial-model.mjs':['6a014df44f766b00966883d7480b2fa9af23b80152e16aa1d8292a45c5b94487'],
 'planner-client.mjs':['3fb13ff63afaf908acea1cbd2a3cd97b19bca358eda0e129ba62faa5501a80bc','89438c3c101992d779e7744c4bfcc87a2aacd19d672dc6d9a29d03fd70839b24']
};
function replaceOnce(text,before,after){assert.equal(text.split(before).length,2,'Repair preimage is ambiguous or missing.');return text.replace(before,after);}
function repair(name,bytes){
 assert(allowed[name].includes(hash(bytes)),'Unreviewed installed module: '+name);
 let text=bytes.toString();
 if(name==='planner-model.mjs'){
  text='export const planItemLimit = 64;\n'+text;
  text=replaceOnce(text,"plan.items.length > 12) fail('Choose between one and twelve fixtures.');","plan.items.length > planItemLimit) fail(\x60Choose between one and \x24{planItemLimit} fixtures.\x60);");
  text=replaceOnce(text,"const subtotal = lines.reduce((sum,item)=>sum+item.product.price_cents,0), issues=[], questions=new Set();","const subtotal = Number(lines.reduce((sum,item)=>sum+BigInt(item.product.price_cents),0n)), issues=[], questions=new Set();\n  if (!Number.isSafeInteger(subtotal) || subtotal < 0) throw Error('The fixture subtotal exceeds supported precision.');");
 }else if(name==='planner-spatial-model.mjs'){
  text=replaceOnce(text,'{validatePlan}','{validatePlan,planItemLimit}');
  text=replaceOnce(text,"> 12) throw Error('This plan holds up to twelve fixtures.');","> planItemLimit) throw Error(\x60This plan holds up to \x24{planItemLimit} fixtures.\x60);");
  text=replaceOnce(text,'const size=product.allowance;','const size=product.dimensions?.width&&product.dimensions?.depth?product.dimensions:product.allowance;');
 }else if(name==='planner-spatial.mjs'){
  text=replaceOnce(text,"g.add(rect);g.add(text(0,Math.max(0,(dim.h-12/scale)/2),String(index+1),{width:dim.w,align:'center',fontStyle:'bold',fill:p.allowance?'#344334':'white'}));","const labelWidth=Math.max(dim.w,24/scale);\n      g.add(rect);g.add(text((dim.w-labelWidth)/2,Math.max(0,(dim.h-12/scale)/2),String(index+1),{name:'fixture-number',width:labelWidth,wrap:'none',align:'center',fontStyle:'bold',fill:dim.known||p.allowance?'#344334':'white'}));");
  text=replaceOnce(text,"if(p.allowance)g.add(text(-.5,dim.h+5/scale,p.use,{width:dim.w+1,align:'center',fontSize:9/scale}));","if(p.allowance&&(dim.w+1)*scale>=60)g.add(text(-.5,dim.h+5/scale,p.use,{name:'fixture-use',width:dim.w+1,wrap:'none',ellipsis:true,align:'center',fontSize:9/scale}));");
 }else{
  text=replaceOnce(text,'{startingPlan,','{planItemLimit,startingPlan,');
  text=replaceOnce(text,"plan.items.length>=12){error('This plan holds up to twelve fixtures. Remove one before adding another.');","plan.items.length>=planItemLimit){error(\x60This plan holds up to \x24{planItemLimit} fixtures. Remove one before adding another.\x60);");
  text=replaceOnce(text,'dim=p.allowance;','dim=known?p.dimensions:p.allowance;');
 }
 return Buffer.from(text);
}
export function repairRoomScaleFiles(original){
 assert(original instanceof Map);const files=new Map(original),projections=[];
 for(const [role,entry]of [['planner','product-room/index.html'],['catalogue','catalogue/index.html']]){
  const html=original.get(entry)?.toString();assert(html,'Missing installed entry.');
  const prefixes=[...new Set(html.match(new RegExp('data/ui-[a-f0-9]{64}/'+role+'/','g')))];assert.equal(prefixes.length,1,'Choose one exact current editor.');
  const prior=prefixes[0],members=new Map([...original].filter(([path])=>path.startsWith(prior)).map(([path,body])=>[path.slice(prior.length),body]));
  assert(members.size>10&&members.size<100,'Unexpected editor closure.');
  for(const name of Object.keys(allowed)){assert(members.has(name));members.set(name,repair(name,members.get(name)));}
  const identity=Object.fromEntries([...members].sort(([a],[b])=>a.localeCompare(b,'en')).map(([name,body])=>[name,hash(body)]));
  const prefix='data/ui-'+hash(JSON.stringify(identity))+'/'+role+'/';
  assert(![...original.keys()].some(path=>path.startsWith(prefix)),'Candidate closure already exists.');
  for(const[name,body]of members){assert(!body.includes(Buffer.from(prior)),'Editor contains an absolute self reference.');files.set(prefix+name,body);}
  let next=html.replaceAll(prior,prefix);
  if(role==='planner')next=replaceOnce(next,'</head>','<style data-room-scale>.fixture-planner .fp-selection{max-height:360px;overflow:auto;overscroll-behavior:contain;padding:3px;scrollbar-gutter:stable}</style></head>');
  files.set(entry,Buffer.from(next));projections.push({role,entry,prior,prefix,files:members.size});
 }
 for(const[name,body]of original)if(!projections.some(p=>p.entry===name))assert(body.equals(files.get(name)),'Existing material changed: '+name);
 return {files,projections};
}
export async function materializeRoomScaleRepair({root,base=roomScaleBase}){
 assert.equal(base,roomScaleBase,'Only the reviewed installed package is supported.');
 const original=await loadWorkspaceFiles(root,base),{files,projections}=repairRoomScaleFiles(original);
 return {...await retainWorkspaceMaterial(files,{root}),base,projections};
}
