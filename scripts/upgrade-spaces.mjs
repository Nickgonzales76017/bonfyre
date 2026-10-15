// Integrate helper-backed views without changing catalogue or saved-plan identities.
import {readFile,writeFile,readdir,mkdir,cp,stat} from 'node:fs/promises';
import {resolve,join,dirname,relative} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import * as vm from 'node:vm';
if(!vm.SourceTextModule){const r=spawnSync(process.execPath,['--experimental-vm-modules',process.argv[1],...process.argv.slice(2)],{stdio:'inherit'});process.exit(r.status??1);}
const root=resolve(process.argv[2]??'site/spaces'),inputs=resolve(process.argv[3]??'studio-inputs');
const tools=resolve('components/BonfyreServiceSite/product-scene-tools.mjs'),hash=b=>createHash('sha256').update(b).digest('hex');
const code=await readFile(tools,'utf8'),version=hash(code+await readFile(process.argv[1],'utf8'));
const marker=join(root,'STUDIO-V2.txt');if(await readFile(marker,'utf8').catch(()=>'')===version+'\n'){console.log('Studio release already matches its source.');process.exit(0);}
const vendor=resolve('vendor/spaces-helpers-20261015'),threeDest=join(root,'assets/product-scene/three-0.180.0');
const exists=async p=>!!await stat(p).catch(()=>null);
async function put(path,body){await mkdir(dirname(path),{recursive:true});await writeFile(path,body);}
async function walk(dir){const result=[];for(const e of await readdir(dir,{withFileTypes:true})){const path=join(dir,e.name);if(e.isDirectory())result.push(...await walk(path));else result.push(path);}return result;}
if(!await exists(join(vendor,'SOURCE.txt'))){
 const camera=join(inputs,'camera/package'),drei=join(inputs,'drei/package'),three=join(inputs,'three/package');
 for(const [dir,name,ver] of [[camera,'camera-controls','3.1.2'],[drei,'@pmndrs/vanilla','1.25.0'],[three,'three','0.180.0']]){const p=JSON.parse(await readFile(join(dir,'package.json'),'utf8'));if(p.name!==name||p.version!==ver)throw Error('Unexpected helper package '+name);}
 await put(join(vendor,'camera-controls.module.js'),await readFile(join(camera,'dist/camera-controls.module.js')));
 await put(join(vendor,'CAMERA-LICENSE'),await readFile(join(camera,'LICENSE')));
 for(const name of ['Grid.js','shaderMaterial.js','pcss.js']){let text=await readFile(join(drei,'core',name),'utf8');text=text.replaceAll("from 'three'","from '../three-0.180.0/build/three.module.js'");await put(join(vendor,name),text);}
 await put(join(vendor,'DREI-LICENSE'),await readFile(join(drei,'LICENSE')));
 const queue=['examples/jsm/postprocessing/EffectComposer.js','examples/jsm/postprocessing/RenderPass.js','examples/jsm/postprocessing/OutlinePass.js','examples/jsm/postprocessing/OutputPass.js','examples/jsm/renderers/CSS2DRenderer.js','examples/jsm/helpers/ViewHelper.js'],seen=new Set();
 while(queue.length){const file=queue.shift();if(seen.has(file))continue;seen.add(file);if(seen.size>90||file.startsWith('..')||file.startsWith('/'))throw Error('Helper closure escaped its bounds.');let text=await readFile(join(three,file),'utf8');
  for(const spec of new vm.SourceTextModule(text).dependencySpecifiers){if(spec==='three'){let rel=relative(dirname(join(threeDest,file)),join(threeDest,'build/three.module.js'));if(!rel.startsWith('.'))rel='./'+rel;text=text.replace(/(\bfrom\s*|\bimport\s*)(['"])three\2/g,(_,p,q)=>p+q+rel+q);}else if(spec.startsWith('.'))queue.push(relative(three,resolve(three,dirname(file),spec)));else throw Error('Unadmitted helper import '+spec);}
  const out=join(threeDest,file);if(await exists(out)){if(await readFile(out,'utf8')!==text)throw Error('Refusing to rewrite an existing Three module: '+file);}else await put(out,text);
 }
 await put(join(vendor,'SOURCE.txt'),'camera-controls 3.1.2 / MIT / https://github.com/yomotsu/camera-controls\n@pmndrs/vanilla 1.25.0 / MIT / https://github.com/pmndrs/drei-vanilla\nThree.js r180 addons / existing MIT release.\nRegistry archives verified before extraction. Bare imports use the existing shared Three instance.\n');
}
const helperFiles=await walk(vendor),helperId=hash(code+(await Promise.all(helperFiles.sort().map(async p=>relative(vendor,p)+hash(await readFile(p))))).join('\n'));
const helperURL='/bonfyre/spaces/assets/product-scene/helpers-'+helperId+'/',helperDest=join(root,'assets/product-scene/helpers-'+helperId);await cp(vendor,helperDest,{recursive:true});await put(join(helperDest,'product-scene-tools.mjs'),code);
const catalogBase=join(root,'data/product-workspace'),plannerCandidates=(await readdir(join(root,'data/red3-fixtures'))).filter(x=>/^v-/.test(x));
let plannerBase;for(const name of plannerCandidates){const p=join(root,'data/red3-fixtures',name);if(!String(await readFile(join(p,'product-scene.mjs'),'utf8').catch(()=>'')).includes('createStudioStage')){plannerBase=p;break;}}
if(!plannerBase)throw Error('Original source-bound planner package is missing.');
function once(text,before,after){if(text.split(before).length!==2)throw Error('Source drift at '+before.slice(0,90));return text.replace(before,after);}
const dirs=new Map();
for(const [kind,base] of [['catalogue',catalogBase],['planner',plannerBase]]){
 const files=new Map();for(const p of await walk(base))files.set(relative(base,p),await readFile(p));
 let scene=files.get('product-scene.mjs').toString();
 scene="import {createStudioStage,studioCSS} from '"+helperURL+"product-scene-tools.mjs';\n"+scene;
 scene=once(scene,"el('style',sceneCSS)","el('style',sceneCSS+studioCSS)");
 scene=once(scene,'const bindings=new Map()',"let studio=null,lastRenderAt=0,builtSignature='',instanceBuilds=0;\n  const bindings=new Map()");
 scene=once(scene,'frame=requestAnimationFrame(()=>{frame=0;renderer.render(scene,camera);});',"frame=requestAnimationFrame(now=>{frame=0;const delta=(now-lastRenderAt)/1000;lastRenderAt=now;if(studio){if(studio.render(delta))invalidate();}else renderer.render(scene,camera);});");
 scene=once(scene,'camera.updateProjectionMatrix();invalidate();}', 'camera.updateProjectionMatrix();studio?.resize();invalidate();}');
 scene=once(scene,"root.dataset.selected=selectedProduct;cleanInstances();const T=engine.THREE;",`root.dataset.selected=selectedProduct;const T=engine.THREE;
    const signature=[mode,mode==='product'?selectedProduct:'',plan?.id,plan?.width,plan?.depth,...(mode==='room'?plan?.items??[]:[]).map(i=>i.key+':'+i.product),...[...bindings].map(([id,b])=>id+':'+b.asset.digest+':'+b.unit+':'+b.variant)].join('|');
    if(signature===builtSignature){if(mode==='room')for(const item of plan.items){const obj=sceneObjects.get(item.key),p=catalogue.products.find(p=>p.id===item.product);if(obj){const place=sourcePlacement(item,p,obj.userData.size);obj.position.set(place.x,0,place.z);obj.rotation.y=place.rotationY;}}refreshControls();studio?.sync();renderInspector();invalidate();return;}
    cleanInstances();builtSignature=signature;instanceBuilds++;root.dataset.instanceBuilds=String(instanceBuilds);`);
 scene=once(scene,"if(mode==='room'&&selectedKey&&sceneObjects.has(selectedKey)){dimensionHelper=", "if(!studio&&mode==='room'&&selectedKey&&sceneObjects.has(selectedKey)){dimensionHelper=");
 scene=once(scene,'refreshControls();renderInspector();invalidate();\n  }','refreshControls();studio?.sync();renderInspector();invalidate();\n  }');
 scene=once(scene,"function fitCamera(view='perspective',scope='all'){if(!engine)return;", "function fitCamera(view='perspective',scope='all'){if(!engine)return;if(studio)return studio.fit(view,scope);");
 scene=once(scene,"b.themedScene=null;b.variant=null;build();", "b.themedScene=null;b.variant=null;builtSignature='';build();");
 scene=once(scene,"orbit=new engine.OrbitControls(camera,renderer.domElement);orbit.enableDamping=false;orbit.addEventListener('change',invalidate);",`engine.CameraControls.install({THREE:T});orbit=new engine.CameraControls(camera,renderer.domElement);orbit.smoothTime=.22;orbit.dollyToCursor=true;orbit.minDistance=.08;orbit.maxDistance=100;
      studio=createStudioStage({engine,renderer,scene,camera,controls:orbit,root,viewport,objects,sceneObjects,readState:()=>({mode,plan,selectedKey,currentMesh,moveMode}),onSelect,onMove,canEdit,invalidate,say});`);
 scene=once(scene,"renderer.shadowMap.type=T.PCFSoftShadowMap", "renderer.shadowMap.type=T.PCFShadowMap");
 scene=once(scene,'cleanInstances();for(const b of bindings.values())','studio?.dispose();cleanInstances();for(const b of bindings.values())');
 scene=once(scene,'inspect(){return {mode,selectedProduct','inspect(){return {studio:studio?.inspect(),instanceBuilds,mode,selectedProduct');
 files.set('product-scene.mjs',Buffer.from(scene));
 let engine=files.get('product-scene-engine.mjs').toString();engine='let shadowHelpersInstalled=false;\n'+engine;
 engine=once(engine,'let draco=null,ktx=null;',`const helper=path=>import(new URL(path,new URL('${helperURL}',location.origin)).href);
 const [{default:CameraControls},{Grid},{pcss},{ViewHelper},{EffectComposer},{RenderPass},{OutlinePass},{OutputPass},{CSS2DRenderer,CSS2DObject}]=await Promise.all([
  helper('camera-controls.module.js'),helper('Grid.js'),helper('pcss.js'),load('examples/jsm/helpers/ViewHelper.js'),load('examples/jsm/postprocessing/EffectComposer.js'),load('examples/jsm/postprocessing/RenderPass.js'),load('examples/jsm/postprocessing/OutlinePass.js'),load('examples/jsm/postprocessing/OutputPass.js'),load('examples/jsm/renderers/CSS2DRenderer.js')]);
 if(!shadowHelpersInstalled){pcss({size:8,samples:12,focus:0});shadowHelpersInstalled=true;}
 let draco=null,ktx=null;`);
 engine=once(engine,'return {THREE,OrbitControls','return {CameraControls,Grid,ViewHelper,EffectComposer,RenderPass,OutlinePass,OutputPass,CSS2DRenderer,CSS2DObject,THREE,OrbitControls');files.set('product-scene-engine.mjs',Buffer.from(engine));
 const id=hash([...files].sort(([a],[b])=>a.localeCompare(b)).map(([p,b])=>p+':'+hash(b)).join('\n')),url='/bonfyre/spaces/data/ui-'+id+'/'+kind+'/',dest=join(root,'data/ui-'+id,kind);
 for(const [p,b] of files)await put(join(dest,p),b);dirs.set(kind,url);
}
for(const [page,kind,entry] of [['catalogue','catalogue','catalogue-client.mjs'],['red3-planner','planner','planner-client.mjs'],['product-room','planner','planner-client.mjs']]){const p=join(root,page,'index.html');let html=await readFile(p,'utf8');const pattern=new RegExp('(<script[^>]*src=")[^"]*'+entry.replace('.','\\.')+'(")');if(!pattern.test(html))throw Error('Missing application entry '+page);html=html.replace(pattern,(_,a,b)=>a+dirs.get(kind)+entry+b);await writeFile(p,html);}
await writeFile(marker,version+'\n');
const entries=[];for(const p of await walk(root)){if(p.endsWith('/RELEASE.tsv'))continue;const b=await readFile(p);entries.push([relative(root,p),hash(b),b.length].join('\t'));}await writeFile(join(root,'RELEASE.tsv'),'path\tsha256\tbytes\n'+entries.sort().join('\n')+'\n');
console.log('Installed helper-backed studio into immutable UI packages; catalogue and saved-plan identities preserved.');
