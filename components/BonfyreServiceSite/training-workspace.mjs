import {readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {workerSource} from './training-offline.mjs';
import {checkRoutine} from './training-model.mjs';
import {checkSettings} from './training-settings.mjs';
import {configuredModel,modelExecutor,boundedText} from './training-assist-server.mjs';
import {requestStore} from './training-requests.mjs';
import {creatorAuth,creatorHandler} from './training-creator.mjs';
import {creatorHTML} from './training-creator-view.mjs';
import {publishingStore} from './training-publishing.mjs';
import {presentationStore} from './training-presentation.mjs';
import {planStore} from './training-plan-store.mjs';

export function trainingMaterial(dir) {
  const data=JSON.parse(readFileSync(join(resolve(dir),'training.json'),'utf8'));
  if(data.schema!=='service-site.training.v1' || !Array.isArray(data.creators)||!data.creators.length||data.creators.length>20) throw Error('Invalid training collection.');
  const ids=new Set();
  for(const c of data.creators) {
    if(!/^[a-z0-9-]{1,64}$/.test(c.id)||ids.has(c.id)||typeof c.description!=='string'||c.description.length>500) throw Error('Invalid creator.'); ids.add(c.id);
    checkSettings(c.settings);
    if(!Array.isArray(c.routines)||c.routines.length>100) throw Error('Invalid programs.');
    const routines=new Set();
    c.routines.forEach(r=>{checkRoutine(r);if(routines.has(r.id)||r.source?.creator!==c.id) throw Error('Program creator or ID does not match.');routines.add(r.id);});
  }
  return data;
}
const html=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#141812"><title>Your training</title><link rel="manifest" href="/training.webmanifest"><link rel="icon" href="/training-icon.svg" type="image/svg+xml"><link rel="stylesheet" href="/training.css"></head><body>
<div class="app"><aside class="sidebar"><a class="wordmark" href="#today"><span class="mark" aria-hidden="true">↗</span><span id="brand-name">Training</span></a><label class="studio-label" for="creator">Your studios</label><select id="creator"></select><nav aria-label="Main"><a href="#today">Today</a><a href="#train">Train</a><a href="#programs">Programs</a><a href="#activity">Activity</a><a href="#devices">Devices</a><a href="#requests">Requests</a><a href="#settings">Settings</a></nav><div class="sidebar-foot">Your training, across studios.<br><small>Saved on this device</small></div></aside>
<main><header class="topbar"><span id="date"></span><div><span id="save-state" role="status">Opening your training…</span><br><span id="offline-state" role="status">Preparing offline access…</span></div></header><div id="notice" role="alert" hidden></div><section id="view"></section></main></div><script type="module" src="/training-client.mjs"></script></body></html>`;
export function trainingHandler(material,port,{modelRoute=null,modelOptions={},requests=null,auth=null,publishing=null,presentation=null,plans=null}={}) {
  const executeModel=modelExecutor(modelRoute,modelOptions);
  const creatorAPI=creatorHandler(material,{auth,requests,publishing,presentation,plans});
  const files={'/training-progression.mjs':'training-progression.mjs','/training-weight.mjs':'training-weight.mjs','/training-offline.mjs':'training-offline.mjs','/training-import.mjs':'training-import.mjs','/training-editor.mjs':'training-editor.mjs','/training-client.mjs':'training-client.mjs','/training-model.mjs':'training-model.mjs','/training-settings.mjs':'training-settings.mjs','/training.css':'training-workspace.css','/store.mjs':'../../integrations/browser/extension/store.mjs'};
  files['/training-assist.mjs']='training-assist.mjs';
  files['/training-assist-view.mjs']='training-assist-view.mjs';
  files['/training-brand-assets.mjs']='training-brand-assets.mjs';
  files['/training-request-view.mjs']='training-request-view.mjs';
  files['/training-catalogue.mjs']='training-catalogue.mjs';
  files['/training-creator-client.mjs']='training-creator-client.mjs';
  files['/training-publication-view.mjs']='training-publication-view.mjs';
  files['/training-appearance.mjs']='training-appearance.mjs';
  for(const name of ['training-devices.mjs','training-device-files.mjs','training-device-view.mjs'])files['/'+name]=name;
  files['/training-plan.mjs']='training-plan.mjs';
  files['/training-plan-view.mjs']='training-plan-view.mjs';
  files['/training-plan-editor.mjs']='training-plan-editor.mjs';
  for(const name of ['workspace-navigation.mjs','workspace-queue.mjs','training-workspace-views.mjs'])files['/'+name]=name;
  files['/training-creator.css']='training-creator.css';
  const brand=material.creators[0].settings.brand;
  const assets=new Map([['/',[html,'text/html']],['/creator',[creatorHTML,'text/html']],['/training.json',[JSON.stringify(material),'application/json']],
    ['/training-icon.svg',[`<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512"><rect width="512" height="512" rx="110" fill="${brand.ground}"/><path d="M145 367L367 145M170 145h197v197" fill="none" stroke="${brand.accent}" stroke-width="52" stroke-linecap="round" stroke-linejoin="round"/></svg>`,'image/svg+xml']],
    ['/training.webmanifest',[JSON.stringify({id:'/',name:`${brand.name} Training`,short_name:'Training',start_url:'/',scope:'/',display:'standalone',background_color:brand.ground,theme_color:brand.ground,icons:[{src:'/training-icon.svg',sizes:'any',type:'image/svg+xml',purpose:'any maskable'}]}),'application/manifest+json']]]);
  for(const [path,file] of Object.entries(files))assets.set(path,[readFileSync(join(import.meta.dir,file),'utf8'),path.endsWith('.css')?'text/css':'text/javascript']);
  for(const name of ['dm-sans','dm-mono','dm-mono-medium','instrument-serif','instrument-serif-italic'])assets.set(`/fonts/${name}.woff2`,[readFileSync(join(import.meta.dir,'fonts',name+'.woff2')),'font/woff2']);
  const version=createHash('sha256').update(JSON.stringify([...assets])).digest('hex');
  const worker=workerSource(version,[...assets.keys()]);
  return request=>{
    const url=new URL(request.url),host=request.headers.get('host');
    const headers={'X-Training-Version':version,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; worker-src 'self'; manifest-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"};
    const response=(body,status=200,type='text/plain')=>new Response(body,{status,headers:{...headers,'Content-Type':type.startsWith('font/')?type:`${type}; charset=utf-8`}});
    if(host!==`127.0.0.1:${port}`&&host!==`localhost:${port}`) return response('Unknown host',403);
    if(url.pathname.startsWith('/api/training/creator/'))return creatorAPI(request);
    if(url.pathname.startsWith('/api/training/appearance/')&&request.method==='GET'){
      try{const studio=url.pathname.slice('/api/training/appearance/'.length),base=material.creators.find(c=>c.id===studio);if(!base)throw Error('Unknown studio.');return response(JSON.stringify(presentation?.read(studio)??{revision:0,brand:base.settings.brand}),200,'application/json');}
      catch(error){return response(JSON.stringify({error:error.message}),400,'application/json');}
    }
    if(url.pathname.startsWith('/api/training/programs/')&&request.method==='GET'){
      try{const studio=url.pathname.slice('/api/training/programs/'.length);if(!material.creators.some(c=>c.id===studio))throw Error('Unknown studio.');return response(JSON.stringify({available:!!publishing,programs:publishing?.list(studio,{publicOnly:true})??[],plans:plans?.list(studio,{publicOnly:true})??[]}),200,'application/json');}
      catch(error){return response(JSON.stringify({error:error.message}),400,'application/json');}
    }
    if(url.pathname==='/api/training/requests'&&request.method==='POST'){
      if(request.headers.get('origin')!==url.origin)return response('Request origin does not match this training app.',403);
      if(request.headers.get('content-type')?.split(';')[0]!=='application/json')return response('Use application/json.',415);
      if(!requests)return response(JSON.stringify({error:'Creator requests are not connected to this server.'}),503,'application/json');
      return (async()=>{try{return response(JSON.stringify(requests.submit(JSON.parse(await boundedText(request,16384)))),200,'application/json');}catch(error){return response(JSON.stringify({error:error.message}),400,'application/json');}})();
    }
    if(url.pathname.startsWith('/api/training/requests/')&&request.method==='GET'){
      if(!requests)return response(JSON.stringify({error:'Creator requests are not connected to this server.'}),503,'application/json');
      try{return response(JSON.stringify(requests.read(url.pathname.slice('/api/training/requests/'.length),request.headers.get('x-request-receipt'))),200,'application/json');}
      catch{return response(JSON.stringify({error:'Request or receipt not found.'}),404,'application/json');}
    }
    if(url.pathname==='/api/training/model-route'&&request.method==='GET')return response(JSON.stringify(modelRoute),200,'application/json');
    if(url.pathname==='/api/training/propose'&&request.method==='POST'){
      if(request.headers.get('origin')!==url.origin)return response('Request origin does not match this training app.',403);
      if(request.headers.get('content-type')?.split(';')[0]!=='application/json')return response('Use application/json.',415);
      return (async()=>{try{
        const raw=await boundedText(request,32*1024),result=await executeModel(JSON.parse(raw),request.signal);
        return response(JSON.stringify(result),200,'application/json');
      }catch(error){return response(JSON.stringify({error:error.name==='TimeoutError'?'The model request timed out.':error instanceof SyntaxError?'The model returned invalid JSON.':error.message}),400,'application/json');}})();
    }
    if(request.method!=='GET') return response('Method not allowed',405);
    if(url.pathname==='/training-sw.js')return response(worker,200,'text/javascript');
    if(/^\/creator\/[a-z0-9-]+\/(requests|programs|appearance|team|account)$/.test(url.pathname))return response(creatorHTML,200,'text/html');
    if(assets.has(url.pathname)){const [body,type]=assets.get(url.pathname);return response(body,200,type);}
    return response('Not found',404);
  };
}
export function serveTraining(dir,port=4190) {
  if(!Number.isInteger(port)||port<1||port>65535) throw Error('Invalid port.');
  const material=trainingMaterial(dir);let publishing=null;
  const requests=process.env.BONFYRE_TRAINING_REQUEST_HOME?requestStore(process.env.BONFYRE_TRAINING_REQUEST_HOME,material,{programResolver:(studio,id)=>publishing?.list(studio,{publicOnly:true}).find(p=>p.routine.id===id)?.routine}):null;
  if(process.env.BONFYRE_TRAINING_CMS_HOME&&requests)publishing=publishingStore(process.env.BONFYRE_TRAINING_CMS_HOME,material,requests);
  const auth=process.env.BONFYRE_TRAINING_AUTH_DB?creatorAuth(process.env.BONFYRE_TRAINING_AUTH_DB):null;
  const presentation=process.env.BONFYRE_TRAINING_CMS_HOME?presentationStore(process.env.BONFYRE_TRAINING_CMS_HOME,material):null;
  const plans=publishing?planStore(process.env.BONFYRE_TRAINING_CMS_HOME,material,publishing):null;
  const server=Bun.serve({hostname:'127.0.0.1',port,idleTimeout:120,fetch:trainingHandler(material,port,{modelRoute:configuredModel(),requests,auth,publishing,presentation,plans})});
  console.log(`Training: http://127.0.0.1:${server.port}`);return server;
}
