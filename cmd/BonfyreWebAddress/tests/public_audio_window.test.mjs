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
