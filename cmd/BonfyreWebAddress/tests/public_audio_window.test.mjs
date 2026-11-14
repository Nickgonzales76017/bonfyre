import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';

// Exercise the exact functions emitted in the native player. Explicit ranges
// describe browser media state; this witness does not claim delivered live media.
const header=readFileSync(join(import.meta.dir,'../src/public_audio_deck.h'),'utf8');
const emitted=header.split('\n').filter(line=>line.startsWith('"')).map(line=>JSON.parse(line.replace(/;$/,''))).join('');
const helpers=emitted.slice(emitted.indexOf('function bounds(){'),emitted.indexOf('function controls(){'));
function player(audio,initialResume=0){
 let updates=0;
 const api=new Function('audio','controls','initialResume','"use strict";let resumePosition=initialResume;'+helpers+'return {bounds,seek,restorePosition,pendingResume:()=>resumePosition};')(audio,()=>updates++,initialResume);
 return {...api,updates:()=>updates};
}
function stream(start,end){
 const state={start,end};
 const audio={duration:Infinity,currentTime:start,seekable:{length:1,start:()=>state.start,end:()=>state.end}};
 return {audio,state,...player(audio)};
}

test('finite recordings retain absolute time and clamp seeks to the duration',()=>{
 const audio={duration:12,currentTime:3},p=player(audio);
 expect(p.bounds()).toEqual([0,12]);expect(p.seek(90)).toBe(true);expect(audio.currentTime).toBe(12);
 expect(p.seek(-5)).toBe(true);expect(audio.currentTime).toBe(0);expect(p.updates()).toBe(2);
});

test('a stream uses its newest valid window and rechecks it for every seek',()=>{
 const p=stream(600,900);p.audio.seekable.length=2;
 p.audio.seekable.start=i=>i===1?p.state.start:100;p.audio.seekable.end=i=>i===1?p.state.end:200;
 expect(p.bounds()).toEqual([600,900]);expect(p.seek(750)).toBe(true);expect(p.audio.currentTime).toBe(750);
 p.state.start=810;p.state.end=1110;
 expect(p.seek(750)).toBe(true);expect(p.audio.currentTime).toBe(810);
 expect(p.seek(2000)).toBe(true);expect(p.audio.currentTime).toBe(1110);
});

test('missing, closed, malformed and disappearing ranges never seek',()=>{
 for(const audio of [
  {duration:NaN,currentTime:4},{duration:0,currentTime:4},
  {duration:Infinity,currentTime:4,seekable:{length:0}},
  {duration:Infinity,currentTime:4,seekable:{length:1,start:()=>10,end:()=>10}},
  {duration:Infinity,currentTime:4,seekable:{length:1,start:()=>-1,end:()=>10}},
  {duration:Infinity,currentTime:4,seekable:{length:1,start:()=>10,end:()=>Infinity}},
  {duration:Infinity,currentTime:4,seekable:{length:1,start:()=>{throw Error('range moved');},end:()=>20}},
 ]){
  const p=player(audio);expect(p.bounds()).toBeNull();expect(p.seek(5)).toBe(false);
  expect(audio.currentTime).toBe(4);expect(p.updates()).toBe(0);
 }
});

test('invalid requested times and refused browser seeks leave playback unchanged',()=>{
 const p=stream(10,40);for(const value of [NaN,Infinity,-Infinity,'20'])expect(p.seek(value)).toBe(false);
 expect(p.audio.currentTime).toBe(10);expect(p.updates()).toBe(0);
 Object.defineProperty(p.audio,'currentTime',{get:()=>10,set:()=>{throw Error('not ready');}});
 expect(p.seek(20)).toBe(false);expect(p.updates()).toBe(0);
});

test('a successful user seek supersedes a deferred history position when a stream range arrives late',()=>{
 const state={available:false,start:600,end:900};
 const audio={duration:Infinity,currentTime:600,seekable:{get length(){return state.available?1:0;},start:()=>state.start,end:()=>state.end}};
 const p=player(audio,750);p.restorePosition();expect(p.pendingResume()).toBe(750);expect(audio.currentTime).toBe(600);
 state.available=true;expect(p.seek(899.9)).toBe(true);expect(p.pendingResume()).toBe(0);
 p.restorePosition();expect(audio.currentTime).toBe(899.9);
 const refused=player(audio,750);Object.defineProperty(audio,'currentTime',{get:()=>899.9,set:()=>{throw Error('not ready');}});
 expect(refused.seek(850)).toBe(false);expect(refused.pendingResume()).toBe(750);
});

test('device controls and the visible player share one range and seek implementation',()=>{
 const device=readFileSync(join(import.meta.dir,'../src/public_media_session.h'),'utf8');
 expect(device).not.toContain('function bounds(');expect(device).not.toContain('function seek(');
 expect(device).toContain("action('seekto',bounds()?details=>seek(details.seekTime):null)");
});

// The spoken reader must work in the actual browser script, not merely emit a
// button or an ornamental waveform. Run its real script against a bounded DOM
// and explicit speech engine, so no operating-system voice or network is needed.
function articleScript(){
 const source=readFileSync(join(import.meta.dir,'../src/article_listen.h'),'utf8');
 const html=source.split('\n').map(line=>line.trim()).filter(line=>line.startsWith('"')).map(line=>{
   if(line.endsWith(',out);'))line=line.slice(0,-6);
   return JSON.parse(line);
 }).join('');
 const script=html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
 expect(script).toBeTruthy();
 expect(()=>new Function(script)).not.toThrow();
 return {html,script};
}
function readerHarness(script,site=false){
 const calls=[],events={};
 const element=()=>({textContent:'',dataset:{},style:{setProperty(){}},attrs:{},handlers:{},setAttribute(k,v){this.attrs[k]=v;},removeAttribute(k){delete this.attrs[k];},addEventListener(k,fn){this.handlers[k]=fn;},click(){this.handlers.click?.();}});
 const controls=Object.fromEntries(['play','back','next','speed','progress','status','length','timeline'].map(k=>[k,element()]));
 controls.speed.value='1';const timeline=controls.timeline;timeline.children=[];timeline.append=el=>timeline.children.push(el);
 const paragraph=element();paragraph.textContent='The retained text available on the article page.';
 const title=element();title.textContent='A real published heading';
 const heading=element();
  const queried=[];const article={querySelector:k=>k==='h1'?title:null,querySelectorAll:k=>{queried.push(k);return[paragraph];}};
 const scope={dataset:{articleListenScope:'site'}};
 const root={hidden:true,matches:()=>true,contains:()=>false,closest:k=>k.includes('data-article-listen-scope')?(site?scope:null):article,
  querySelector:k=>k==='h2'?heading:controls[k.match(/data-listen-(\w+)/)?.[1]],addEventListener(k,fn){events[k]=fn;}};
 const speech={speak(u){calls.push(u);u.onstart?.();},cancel(){},pause(){events.paused=true;},resume(){events.resumed=true;}};
 function Utterance(text){this.text=text;}
 const doc={currentScript:{previousElementSibling:root},readyState:'complete',documentElement:{lang:'en'},
   createElement:element,addEventListener(k,fn){events[k]=fn;},dispatchEvent(e){events.dispatched=e;events[e.type]?.(e);}};
  const win={speechSynthesis:speech,SpeechSynthesisUtterance:Utterance,CustomEvent:class{constructor(type,init){this.type=type;this.detail=init.detail;}},addEventListener(k,fn){events[k]=fn;}};
 new Function('document','window','SpeechSynthesisUtterance',script)(doc,win,Utterance);
  return {calls,controls,root,heading,paragraph,events,queried};
}
test('article listening starts only on request, follows published text, and coordinates with recorded audio',()=>{
 const {html,script}=articleScript(),item=readerHarness(script),site=readerHarness(script,true);
 expect(html).toContain('data-listen-timeline');
 expect(html).toContain('not a publisher-recorded narration');
 expect(item.root.hidden).toBe(false);
 expect(site.root.hidden).toBe(false);
 expect(site.heading.textContent).toBe('Listen to this article');
 expect(item.calls).toHaveLength(0);
 expect(item.controls.timeline.value).toBe(0);
 item.controls.play.click();
 expect(item.calls.map(x=>x.text)).toEqual(['A real published heading']);
 expect(item.events.dispatched?.type).toBe('bonfyre:public-audio-play');
 expect(item.events.dispatched?.detail).toBe(item.root);
 expect(site.queried[0]).toContain(':not(.bf-article-recording)');
 expect(site.queried[0]).toContain('.site-page > p.lede');
 expect(site.queried[0]).toContain(':not(.site-article-sources)');
 expect(item.controls.play.textContent).toContain('Pause');
 item.controls.play.click();expect(item.events.paused).toBe(true);
 item.controls.play.click();expect(item.events.resumed).toBe(true);
 item.calls[0].onend();
 expect(item.calls.map(x=>x.text)).toEqual(['A real published heading','The retained text available on the article page.']);
 expect(item.controls.timeline.value).toBeGreaterThan(0);
 expect(item.paragraph.attrs).toHaveProperty('data-listen-active');
 item.controls.speed.value='1.5';item.controls.speed.handlers.change();
 expect(item.calls.at(-1).rate).toBe(1.5);
 item.events['bonfyre:public-audio-play']({detail:{}});
 expect(item.controls.status.textContent).toContain('Stopped');
 site.controls.play.click();expect(site.calls[0].text).toBe('A real published heading');
 site.calls[0].onend();expect(site.calls[1].text).toBe('The retained text available on the article page.');
 site.events.pagehide();
 expect(site.paragraph.attrs).not.toHaveProperty('data-listen-active');
});
test('article listening is wired to both independent sites and retained source pieces',()=>{
 const item=readFileSync(join(import.meta.dir,'../src/page_routes_and_emitters.h'),'utf8');
 const site=readFileSync(join(import.meta.dir,'../src/site_mcp.c'),'utf8');
 expect(item).toContain('bf_article_listen_emit(stdout)');
 expect(site.match(/bf_article_listen_emit\(out\)/g)).toHaveLength(1);
 expect(site.match(/site_article_listening\(out,db,site,/g)).toHaveLength(2);
 expect(site).toContain('data-article-listen-scope=');
});

test('audio waveform is measured-data-only and has a bounded parser with a standard seek fallback',()=>{
 const script=emitted.match(/<script>([\s\S]*?)<\/script>/)?.[1];
 expect(script).toBeTruthy();expect(()=>new Function(script)).not.toThrow();
 const begin=script.indexOf('function waveformBins(text){'),end=script.indexOf('function setWaveform(row){');
 expect(begin).toBeGreaterThan(0);expect(end).toBeGreaterThan(begin);
 const parse=new Function(script.slice(begin,end)+'return waveformBins;')();
 const valid=Array.from({length:64},(_,i)=>String(i%101)).join(',');
 expect(parse(valid)).toEqual(Array.from({length:64},(_,i)=>i%101));
 for(const bad of [undefined,'',Array(63).fill('20').join(','),Array(65).fill('20').join(','),valid.replace('0,1,','200,1,'),Array(64).fill('-1').join(','),Array(64).fill('01').join(',')])expect(parse(bad)).toBeNull();
 expect(emitted).toContain('data-deck-waveform hidden aria-hidden=');
 expect(emitted).toContain('.bf-audio-waveform[hidden]{display:none!important}');
 expect(script).toContain("const bins=waveformBins(row?.dataset?.peaks)");
 expect(script).toContain("waveBars[i].dataset.played");
 const native=readFileSync(join(import.meta.dir,'../src/site_mcp.c'),'utf8');
 expect(native).toContain('site_article_waveform_valid(a,w,digest)');
 expect(native).toContain('data-peaks=');
});

test('a published VTT transcript is bounded, timed and safe to render as text',()=>{
 const script=emitted.match(/<script>([\s\S]*?)<\/script>/)?.[1];
 expect(script).toBeTruthy();
 expect(()=>new Function(script)).not.toThrow();
 const start=script.indexOf('function transcriptTime(stamp){');
 const end=script.indexOf('function resetTranscript(row){');
 expect(start).toBeGreaterThan(0);expect(end).toBeGreaterThan(start);
 const parse=new Function(script.slice(start,end)+'return transcriptEntries;')();
 const valid='WEBVTT\n\n00:00:00.000 --> 00:00:01.200\nHello <b>world</b>\n\n00:00:01.200 --> 00:00:03.000\nA second passage.';
 expect(parse(valid)).toEqual([{start:0,end:1.2,text:'Hello world'},{start:1.2,end:3,text:'A second passage.'}]);
 for(const bad of ['bad','WEBVTT\n\n00:00:03.000 --> 00:00:02.000\nBackwards','WEBVTT\n\nbogus --> 00:00:02.000\nNonsense'])expect(parse(bad)).toEqual([]);
 expect(parse('WEBVTT\n\n'+'x'.repeat(300000))).toEqual([]);
 expect(emitted).toContain('data-deck-transcript hidden');
 expect(script).toContain("credentials:'omit'");
 expect(script).toContain('transcriptController?.abort()');
 expect(script).toContain('words.textContent=cue.text');
 expect(script).toContain('transcriptSync();');
 const site=readFileSync(join(import.meta.dir,'../src/site_mcp.c'),'utf8');
 expect(site).toContain('data-transcript=');
});
