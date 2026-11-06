import {test,expect} from 'bun:test';
import {defaultSettings,checkSettings,exportPrograms} from '../training-settings.mjs';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio');
const bridge={id:'wordpress',name:'Studio',kind:'wordpress',endpoint:'https://studio.example',scope:'programs',direction:'export'};
test('settings reject injected colors, credential URLs, unknown scopes and unbounded model sharing',()=>{
  const base=defaultSettings();expect(checkSettings(base)).toEqual(base);
  for(const mutate of [s=>s.brand.accent='red;display:none',s=>s.model={provider:'local',model:'test',endpoint:'http://remote.example',data:'program-only'},
    s=>s.model={provider:'local',model:'test',endpoint:'http://127.0.0.1:11434',data:'all-history'},
    s=>s.bridges=[{...bridge,endpoint:'https://user:password@studio.example'}],s=>s.bridges=[{...bridge,scope:'training-history'}],
    s=>s.bridges=[{...bridge,apiKey:'secret'}],s=>s.model.apiKey='secret',s=>s.bridges=[bridge,bridge]]){
    const bad=structuredClone(base);mutate(bad);expect(()=>checkSettings(bad)).toThrow();
  }
  expect(checkSettings({...base,model:{provider:'local',model:'local-route',endpoint:'http://127.0.0.1:11434',data:'program-only'}}).model.provider).toBe('local');
});
test('program exports preserve version, escape markup and omit completion flags',()=>{
  const routines=structuredClone(material.creators[0].routines);routines[0].name='A < B & C';routines[0].exercises[0].sets[0].done=true;
  const xml=exportPrograms(routines,bridge);expect(xml).toContain('A &lt; B &amp; C');expect(xml).toContain('<wp:status>draft</wp:status>');expect(xml).not.toContain('"done"');
  const docs=JSON.parse(exportPrograms(routines,{...bridge,kind:'frappe'}));expect(docs[0].doctype).toBe('Website Article');expect(docs[0].published).toBe(0);
  expect(JSON.parse(docs[0].content).source.version).toBe('1');expect(JSON.parse(docs[0].content).exercises[0].sets[0].done).toBeUndefined();
});
test('local training service serves only exact assets and never accepts mutations or foreign hosts',async()=>{
  const handle=trainingHandler(material,4190),request=(path,options={})=>new Request(`http://127.0.0.1:4190${path}`,{headers:{host:'127.0.0.1:4190'},...options});
  expect(handle(request('/')).status).toBe(200);expect(handle(request('/training-client.mjs')).headers.get('content-type')).toContain('text/javascript');
  expect(handle(request('/training.json',{method:'POST'})).status).toBe(405);
  expect(handle(request('/',{headers:{host:'elsewhere.example'}})).status).toBe(403);
  expect(handle(request('/cms.db')).status).toBe(404);
});
