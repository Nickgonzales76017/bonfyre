import {validatePlan,startingPlan,assessPlan} from './planner-model.mjs';
import {priceCents} from './catalogue-model.mjs';

/** Resolve an explicitly compiled join. An equal label, brand or model color is insufficient. */
export function resolveRoomOption(product,variantId,routes) {
 const option=product.variants?.find(v=>v.id===variantId);
 if(!option)throw Error('The selected source option is absent.');
 const matches=[];
 for(const route of routes??[]){
  if(!/^\/[a-z0-9/_-]+\/$/.test(route.path)||!/^planner-[a-z0-9-]+$/.test(route.form))throw Error('Invalid declared room route.');
  for(const binding of route.bindings??[]){
   if(binding.store!==product.store||binding.productId!==product.id||binding.variantId!==variantId||binding.sourceDigest!==product.source_sha256)continue;
   const fixture=route.catalogue.products.find(p=>p.id===binding.fixtureId);
   if(!fixture||fixture.product_id!==product.id||fixture.variant_id!==variantId||fixture.source_url!==product.source_url||fixture.source_sha256!==binding.fixtureSourceDigest||priceCents(option.price)!==BigInt(fixture.price_cents))throw Error('The catalogue and room source disagree. Refresh the source before placing this option.');
   matches.push({route,binding,fixture,option});
  }
 }
 if(matches.length>1)throw Error('This option has more than one room destination. A source-scoped route must be selected explicitly.');
 return matches[0]??null;
}
export function addRoomUnits(plan,catalogue,fixtureId,quantity,{makeId=()=>crypto.randomUUID()}={}) {
 validatePlan(plan,catalogue);
 if(!Number.isInteger(quantity)||quantity<1||quantity>12||plan.items.length+quantity>12)throw Error('Choose a whole quantity that keeps the space within twelve fixtures.');
 if(!catalogue.products.some(p=>p.id===fixtureId))throw Error('This option does not belong to the room source.');
 const next=structuredClone(plan);
 for(let i=0;i<quantity;i++)next.items.push({key:makeId(),product:fixtureId,x:1+(i%4)*.25,y:1+Math.floor(i/4)*.25,rotation:0});
 return validatePlan(next,catalogue);
}
export function newRoomPlan(catalogue,fixtureId,quantity,id=crypto.randomUUID()) {
 const plan=startingPlan(catalogue,Object.keys(catalogue.presets)[0],id);
 // A deliberate new plan contains only the chosen units, never hidden preset items.
 plan.items=[];
 if(!Number.isInteger(quantity)||quantity<1||quantity>12||!catalogue.products.some(p=>p.id===fixtureId))throw Error('Choose an existing option and a quantity from one to twelve.');
 for(let i=0;i<quantity;i++)plan.items.push({key:crypto.randomUUID(),product:fixtureId,x:1+(i%4),y:1+Math.floor(i/4),rotation:0});
 return validatePlan(plan,catalogue);
}
export function replaceRoomOption(plan,catalogue,key,fixtureId) {
 validatePlan(plan,catalogue);
 const item=plan.items.find(i=>i.key===key),before=catalogue.products.find(p=>p.id===item?.product),after=catalogue.products.find(p=>p.id===fixtureId);
 if(!before||!after||before.product_id!==after.product_id||before.source_url!==after.source_url)throw Error('Choose an observed option of this exact product.');
 const next=structuredClone(plan);next.items.find(i=>i.key===key).product=after.id;return validatePlan(next,catalogue);
}
export function changeRoomQuantity(plan,catalogue,fixtureId,quantity) {
 validatePlan(plan,catalogue);
 if(!Number.isInteger(quantity)||quantity<0||quantity>12)throw Error('Quantity must be a whole number from zero to twelve.');
 const existing=plan.items.filter(i=>i.product===fixtureId);
 if(quantity>existing.length)return addRoomUnits(plan,catalogue,fixtureId,quantity-existing.length);
 const removed=new Set(existing.slice(quantity).map(i=>i.key)),next={...structuredClone(plan),items:plan.items.filter(i=>!removed.has(i.key)).map(i=>({...i}))};
 return validatePlan(next,catalogue);
}
export function roomQuantityRows(plan,catalogue){
 const a=assessPlan(plan,catalogue),counts=new Map();for(const i of plan.items)counts.set(i.product,(counts.get(i.product)??0)+1);
 return [...counts].map(([id,quantity])=>{const product=catalogue.products.find(p=>p.id===id);return {product,quantity,total:quantity*product.price_cents};});
}
