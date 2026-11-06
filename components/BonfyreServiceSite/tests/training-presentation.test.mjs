import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,writeFileSync,readdirSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {Database} from 'bun:sqlite';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
import {presentationStore,presentationInstitution} from '../training-presentation.mjs';
import {creatorAuth} from '../training-creator.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio');
const png='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7i8AAAAASUVORK5CYII=';
async function isolated(fn){const home=mkdtempSync(join(tmpdir(),'training-presentation-'));try{await fn({home,store:presentationStore(home,material)});}finally{rmSync(home,{recursive:true,force:true});}}
test('shared appearance uses native template roles and separate bounded image artifacts',()=>isolated(({home,store})=>{
  const original=structuredClone(material),brand={...store.read('forge').brand,name:'Forge Strength',font:'serif',density:'compact',logo:{dataUrl:'data:image/png;base64,'+png,width:1,height:1}};
  const result=store.save('forge','actor:designer',{brand,expectedRevision:0});expect(result.revision).toBe(1);expect(result.brand.logo).toEqual(brand.logo);
  expect(presentationStore(home,material).read('forge')).toEqual(result);expect(store.read('still').brand.name).toBe('Still Studio');expect(material).toEqual(original);
  const db=new Database(join(home,'catalogue.db'),{readonly:true}),row=db.query('select layout_tokens,density from presentation_template').get(),identity=db.query('select logo from training_studio_presentation').get();db.close();
  expect(JSON.parse(row.layout_tokens).face_display).toBe('"Instrument Serif", ui-serif, serif');expect(row.density).toBe('tight');expect(identity.logo).not.toContain('data:image');expect(readdirSync(join(home,'brand-assets'))).toHaveLength(1);
  expect(store.save('forge','actor:designer',{brand,expectedRevision:0})).toEqual(result);
  const next=store.save('forge','actor:designer',{brand:{...brand,logo:null,accent:'#ffaa66'},expectedRevision:1});expect(next.revision).toBe(2);expect(next.brand.logo).toBeNull();
  expect(()=>store.save('forge','actor:designer',{brand:{...brand,name:'Stale'},expectedRevision:1})).toThrow('appearance changed');expect(store.read('forge')).toEqual(next);
}));
test('invalid styles cannot mutate the current studio and altered native templates are refused',()=>isolated(({home,store})=>{
  const brand=store.read('forge').brand;
  for(const invalid of [{...brand,ink:brand.ground},{...brand,accent:'red;display:none'},{...brand,css:'body{}'},{...brand,font:'remote-font'},{...brand,logo:{dataUrl:'https://example.test/logo.png',width:1,height:1}}])expect(()=>store.save('forge','actor:designer',{brand:invalid,expectedRevision:0})).toThrow();
  expect(store.read('forge').revision).toBe(0);
  store.save('forge','actor:designer',{brand,expectedRevision:0});const db=new Database(join(home,'catalogue.db'));db.run('update presentation_template set density=?',['loud']);db.close();expect(()=>store.read('forge')).toThrow('template changed');expect(store.read('still').revision).toBe(0);
}));
test('corrupted shared images fail readback instead of becoming new brand content',()=>isolated(({home,store})=>{
  const brand={...store.read('forge').brand,icon:{dataUrl:'data:image/png;base64,'+png,width:1,height:1}};store.save('forge','actor:designer',{brand,expectedRevision:0});
  writeFileSync(join(home,'brand-assets',readdirSync(join(home,'brand-assets'))[0]),'changed');expect(()=>store.read('forge')).toThrow('image changed');
}));
test('appearance access is independent from programs, rechecked after the request body and public only after save',()=>isolated(async({home,store})=>{
  const db=join(home,'auth.db'),native=(command,args=[],input='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input,encoding:'utf8',stdio:'pipe'}));
  native('http-signup',[],JSON.stringify({email:'designer@example.test',password:'appearance-test-password',name:'Designer'}));native('actor-bind',['--user-id','1','--actor','actor:designer']);native('institution-grant',['--user-id','1','--institution','training-forge-programs','--role','write']);
  const handler=trainingHandler(material,4190,{requests:{},auth:creatorAuth(db),presentation:store}),origin='http://127.0.0.1:4190';let cookie='';
  const call=(path,body,headers={})=>handler(new Request(origin+'/api/training/creator'+path,{method:body===undefined?'GET':'POST',headers:{host:'127.0.0.1:4190',origin,cookie,'content-type':'application/json',...headers},body:body===undefined?undefined:JSON.stringify(body)}));
  const login=await call('/login',{email:'designer@example.test',password:'appearance-test-password'});cookie=login.headers.get('set-cookie').split(';')[0];
  const path='/studios/forge/appearance',body={brand:{...store.read('forge').brand,name:'Forge Shared'},expectedRevision:0};expect((await call(path,body)).status).toBe(403);
  native('institution-grant',['--user-id','1','--institution',presentationInstitution('forge'),'--role','read']);expect((await call(path)).status).toBe(200);expect((await call(path,body)).status).toBe(403);
  native('institution-grant',['--user-id','1','--institution',presentationInstitution('forge'),'--role','write']);expect((await call(path,body,{origin:'https://elsewhere.test'})).status).toBe(403);expect((await call('/studios/still/appearance',body)).status).toBe(403);expect((await call(path,body)).status).toBe(200);
  const publicResponse=await handler(new Request(origin+'/api/training/appearance/forge',{headers:{host:'127.0.0.1:4190'}})),publicText=await publicResponse.text();expect(publicText).toContain('Forge Shared');expect(publicText).not.toContain('actor:designer');expect(publicText).not.toContain('editor_actor');
  let send;const stream=new ReadableStream({start(controller){send=()=>{native('institution-revoke',['--user-id','1','--institution',presentationInstitution('forge')]);controller.enqueue(new TextEncoder().encode(JSON.stringify({...body,expectedRevision:1})));controller.close();};}});
  const pending=handler(new Request(origin+'/api/training/creator'+path,{method:'POST',headers:{host:'127.0.0.1:4190',origin,cookie,'content-type':'application/json'},body:stream,duplex:'half'}));send();expect((await pending).status).toBe(403);expect(store.read('forge').revision).toBe(1);
  const content=new Database(join(home,'catalogue.db'));content.run('update presentation_template set density=?',['broken']);content.close();
  const session=await call('/session');expect(session.status).toBe(200);expect((await session.json()).studios[0].name).toBe('Forge Studio');
}));
