import {test,expect} from 'bun:test';
import {continuationManifest} from '../workspace_continuation.mjs';
const view={view:'room',label:'Room plan',entry:'room/',prefix:'fixture-plan:',title:'plan.title',query:'project=plan.id&scene=@room'};
const files=()=>new Map([['room/index.html',Buffer.from('<meta name="bonfyre-workspace-store">')]]);
test('a continuation names only retained editor paths and record selectors',()=>{
 const result=continuationManifest([view],files()).toString();expect(result).toBe('view\tlabel\tentry\tprefix\ttitle\tquery\nroom\tRoom plan\troom/\tfixture-plan:\tplan.title\tproject=plan.id&scene=@room\n');
 expect(continuationManifest([{...view,prefix:'=draft:creator:one:imports',title:'$key',query:'-'}],files()).toString()).toContain('=draft:creator:one:imports');
 expect(continuationManifest([{...view,title:'@Untitled room'}],files()).toString()).toContain('@Untitled room');
 expect(view).toEqual({view:'room',label:'Room plan',entry:'room/',prefix:'fixture-plan:',title:'plan.title',query:'project=plan.id&scene=@room'});
});
test('external, traversal, unbound and absent destinations cannot become continuation views',()=>{
 for(const entry of ['https://other.test/','//other.test/','../room/','room/../other/','room//','/room/','room/?q=1','room/#x','room'])expect(()=>continuationManifest([{...view,entry}],files())).toThrow();
 expect(()=>continuationManifest([view],new Map())).toThrow();expect(()=>continuationManifest([view],new Map([['room/index.html',Buffer.from('unbound document')]]))).toThrow();
});
test('unknown fields, duplicated views, excess views and control bytes are rejected',()=>{
 for(const row of [{...view,extra:true},{...view,view:'a b'},{...view,label:'bad\nrow'},{...view,prefix:'a\tfield'},{...view,label:'\0'},{...view,label:'é'.repeat(80)}])expect(()=>continuationManifest([row],files())).toThrow();
 expect(()=>continuationManifest([view,view],files())).toThrow();expect(()=>continuationManifest([],files())).toThrow();expect(()=>continuationManifest(Array.from({length:17},(_,i)=>({...view,view:'v'+i})),files())).toThrow();
});
test('query selectors cannot inject duplicate coordinates, arbitrary expressions or malformed paths',()=>{
 for(const query of ['project=plan.id&project=@other','project=','project=@','project=plan..id','project=.plan','project=plan.','project=$unknown','project=plan[id]','bad name=plan.id','project=plan.id&','project=plan.id\nnext=x',Array.from({length:9},(_,i)=>'q'+i+'=$key').join('&')])expect(()=>continuationManifest([{...view,query}],files())).toThrow();
 for(const title of ['','plan..title','plan[title]','$unknown','@'])expect(()=>continuationManifest([{...view,title}],files())).toThrow();
});
test('the projection is deterministic and cannot mutate its retained input files',()=>{
 const retained=files(),before=Buffer.from(retained.get('room/index.html'));expect(continuationManifest([view],retained)).toEqual(continuationManifest([view],retained));expect(retained.size).toBe(1);expect(retained.get('room/index.html')).toEqual(before);
});

import {parseContinuationManifest,createContinuationProjection,projectSavedView} from '../../integrations/browser/workspace-continuation.mjs';
const endpoint='/apps/ws-'+ 'a'.repeat(40)+'/_records',release='b'.repeat(64),base=endpoint.slice(0,-'_records'.length);
const row=(id='one',revision=1)=>({key:'fixture-plan:'+id,revision,value:{plan:{id,title:'Room '+id},models:[new Blob(['private model bytes'])]}});
const url=(id='one')=>new URL('https://aurekai.work'+base+'room/?'+new URLSearchParams({project:id,scene:'room'}));
test('compiler and browser share the same bounded physical view grammar',()=>{
 const text=continuationManifest([view],files()).toString();expect(parseContinuationManifest(text)).toEqual([view]);
 for(const bad of [text.replace('view\t','unknown\t'),text+'extra\tcolumns\n',text.repeat(2000)])expect(()=>parseContinuationManifest(bad)).toThrow();
});
test('the current editor is resolved from exact saved identity and named view coordinates',()=>{
 const p=createContinuationProjection([view],endpoint,release);p.observe([row('café & +')]);const current=p.current(url('café & +'));
 expect(current.key).toBe('fixture-plan:café & +');expect(new URL(current.href,'https://aurekai.work').searchParams.get('revision')).toBe('1');
 expect(new URL(current.href,'https://aurekai.work').searchParams.get('release')).toBe(release);expect(p.current(url('elsewhere'))).toBe(null);
 const duplicate=url('café & +');duplicate.searchParams.append('project','wrong');expect(p.current(duplicate)).toBe(null);
});
test('unrelated equal titles cannot select a record and ambiguous identities stay unselected',()=>{
 const p=createContinuationProjection([view],endpoint,release);p.observe([row(),{...row(),key:'fixture-plan:other-key'}]);expect(p.current(url())).toBe(null);
 p.observe([],{removed:['fixture-plan:other-key']});expect(p.current(url()).key).toBe('fixture-plan:one');
});
test('projection keeps saved selectors rather than mutable working bodies or files',()=>{
 const draft=row(),p=createContinuationProjection([view],endpoint,release);p.observe([draft]);draft.value.plan.title='Unsaved title';draft.value.plan.id='changed';
 expect(p.current(url()).title).toBe('Room one');expect(Object.keys(projectSavedView(view,row())).sort()).toEqual(['entry','key','label','parameters','revision','title','view']);
});
test('only confirmed saves or reads advance the projected revision',()=>{
 const p=createContinuationProjection([view],endpoint,release);p.observe([row()]);expect(p.current(url()).revision).toBe(1);
 p.observe([row('one',2)]);expect(p.current(url()).revision).toBe(2);p.observe([],{prefix:'fixture-plan:'});expect(p.current(url())).toBe(null);
});
test('exact-key audio declarations do not select adjacent sessions or personal training',()=>{
 const audio={...view,entry:'audio/',prefix:'=draft:creator:sample:imports',title:'title',query:'workspace=@imports&view=@timeline'};
 expect(projectSavedView(audio,{key:'draft:creator:sample:imports-extra',revision:1,value:{title:'Other'}})).toBe(null);
 expect(projectSavedView(audio,{key:'training:member:primary',revision:1,value:{title:'Private'}})).toBe(null);
 expect(projectSavedView(audio,{key:'draft:creator:sample:imports',revision:1,value:{title:'Recording'}}).title).toBe('Recording');
});
