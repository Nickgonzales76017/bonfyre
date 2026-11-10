// Fictional merchant source for deterministic native account and release checks.
import { catalogueFiles } from '../catalogue-material.mjs';
import { plannerFiles } from '../planner-material.mjs';
import { baseline, makeDraft, draftKey } from '../catalogue-model.mjs';
import { startingPlan } from '../planner-model.mjs';

export const product = {id:'101',store:'fixture-store',title:'Display table',description_text:'Oak display table.',tags:['Display'],
  source_url:'https://fixture.example/products/table',source_sha256:'a'.repeat(64),observed_at:'2026-10-17T10:00:00Z',
  image:'https://fixture.example/images/table.jpg',kind:'goods',variants:[{id:'201',label:'Oak',sku:'TABLE-OAK',price:'300.00',available:true}]};
export const products = [product];
export const stores = [{id:product.store,name:'Fixture store',origin:'https://fixture.example',image_path:'/images/',sha256:product.source_sha256}];
export const fixtureInput = {schema:'service-site.fixture-catalogue.v1',id:'release-fixtures',title:'Display fixtures',origin:stores[0].origin,currency:'USD',
  products:[{id:'table',product_id:product.id,variant_id:'201',label:product.title,option:'Oak',source_url:product.source_url,source_sha256:product.source_sha256,
    observed_at:product.observed_at,image:product.image,price_cents:30000,dimensions:{width:36,depth:24},questions:[]}],
  defaults:{title:'Showroom',width:20,depth:20,budget:75000,deadline:'',notes:''},
  presets:{compact:[{key:'table-one',product:'table',x:1,y:1,rotation:0}]}};
export const fixtureCatalogue = plannerFiles(fixtureInput).catalogue;
export const material = catalogueFiles(products,stores,{fixtureCatalogues:[fixtureCatalogue]});
export const changedDraft = (amount='400.00') => {
  const original=baseline(product),edited=structuredClone(original);edited.variants[0].price=amount;
  return makeDraft(product,original,edited,'2026-10-17T12:00:00Z');
};
export const releaseFixture = () => {
  const plan=startingPlan(fixtureCatalogue,'compact','release-room-001');
  plan.items.push({...plan.items[0],key:'second-placement',x:5});
  return {keys:[draftKey(product)],products,catalogues:[fixtureCatalogue],records:[
    {key:draftKey(product),revision:1,value:changedDraft()},
    {key:`fixture-plan:planner-rooms:${plan.id}`,revision:1,value:{plan,receipt:null}}]};
};
