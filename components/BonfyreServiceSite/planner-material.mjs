import {validateFixtureCatalogue} from './planner-catalogue.mjs';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';

export function plannerFiles(input,{previousCatalogues=[]}={}) {
  validateFixtureCatalogue(input);
  const catalogue={...input,digest:createHash('sha256').update(JSON.stringify(input)).digest('hex')};
  if(!Array.isArray(previousCatalogues)||previousCatalogues.length>20)throw Error('Too many retained fixture catalogues.');
  const retained=new Set([catalogue.digest]);
  for(const previous of previousCatalogues){const {digest,...prior}=previous;if(retained.has(digest))throw Error('Duplicate retained fixture generation.');retained.add(digest);validateFixtureCatalogue(prior);if(prior.id!==input.id||prior.origin!==input.origin||createHash('sha256').update(JSON.stringify(prior)).digest('hex')!==digest)throw Error('Invalid retained fixture catalogue.');}
  const files={};
  for(const name of ['planner-client.mjs','planner-spatial.mjs','planner-spatial-model.mjs','vendor/konva-10.9.0/konva.min.js','vendor/konva-10.9.0/LICENSE','planner-model.mjs','planner-catalogue.mjs','planner-continuity.mjs','planner-projects.mjs','planner-project-client.mjs','workspace-navigation.mjs','opportunity-selection.mjs'])files[name]=readFileSync(join(import.meta.dir,name));
  files['store.mjs']=readFileSync(join(import.meta.dir,'../../integrations/browser/extension/store.mjs'));
  const hash=body=>createHash('sha256').update(body).digest('hex');
  const identity=Object.fromEntries(Object.keys(files).sort().map(name=>[name,hash(files[name])]));
  const asset_prefix=`v-${hash(JSON.stringify(identity))}/`;
  // Keep the existing flat addresses available for already-open planner tabs.
  return {catalogue,previous_catalogues:previousCatalogues,asset_prefix,files:{...files,...Object.fromEntries(Object.entries(files).map(([name,body])=>[asset_prefix+name,body]))}};
}
