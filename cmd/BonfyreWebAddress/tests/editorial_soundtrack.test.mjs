import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';

const src=name=>readFileSync(join(import.meta.dir,'../src',name),'utf8');
const decoded=line=>{
  let text=line.trim();
  if(!text.startsWith('"'))return '';
  text=text.replace(/\s*\\\s*$/,'');
  if(text.endsWith(',out);'))text=text.slice(0,-6);
  else if(text.endsWith(';'))text=text.slice(0,-1);
  return JSON.parse(text);
};
const helper=src('editorial_soundtrack.h').split('\n').map(decoded).join('');
function emitted(name){
  const html=src(name).split('\n').map(line=>
    line.trim()==='BF_EDITORIAL_SCORE_JS'?helper:decoded(line)).join('');
  return {html,script:html.match(/<script>([\s\S]*?)<\/script>/)?.[1]};
}

function harness(title='An editorial story'){
  const notes=[],levels=[],timers=new Map(),choice={value:'auto'};
  let interval=0,contexts=0,closed=0;
  class AudioContext{
    constructor(){contexts++;this.currentTime=4;this.state='running';this.destination={};}
    createGain(){return {gain:{value:0,setValueAtTime(){},exponentialRampToValueAtTime(){},
      setTargetAtTime(value){levels.push(value);}},connect(){}};}
    createOscillator(){
      const wave={type:'sine',hz:0,frequency:{setValueAtTime(value){wave.hz=value;}},
        connect(){},start(){notes.push([wave.hz,wave.type]);},stop(){}};
      return wave;
    }
    resume(){return Promise.resolve();}
    close(){closed++;return Promise.resolve();}
  }
  const music=new Function('root','heading','window','setInterval','clearInterval',
    helper+'return bfEditorialSoundtrack(root,heading);')(
      {querySelector:()=>choice},title,{AudioContext},
      fn=>{const id=++interval;timers.set(id,fn);return id;},id=>timers.delete(id));
  return {music,choice,notes,levels,timers,stats:()=>({contexts,closed})};
}

test('shared original score is compiled into both recorded-article and browser-voice paths',()=>{
  expect(()=>new Function(helper)).not.toThrow();
  for(const name of ['article_listen.h','public_audio_deck.h']){
    const {html,script}=emitted(name);
    expect(script).toContain('function bfEditorialSoundtrack');
    expect(()=>new Function(script)).not.toThrow();
    expect(html).toContain(name==='article_listen.h'?'data-listen-music':'data-deck-music');
  }
  const recorded=src('public_audio_deck.h');
  expect(recorded).toContain("deck.closest?.('[data-article-recording]')");
  expect(recorded).toContain('score?.end()');
  expect(recorded).toContain('score?.pause()');
  expect(recorded).toContain('score?.dispose()');
  expect(recorded).toContain('on(audio,');
});

test('original score is user-started, voices remain foreground, Off mutes, and disposal stops notes',async()=>{
  const h=harness('Aurekai newsroom · a report');
  expect(h.stats().contexts).toBe(0);
  expect(h.timers.size).toBe(0);
  h.music.start();h.music.duck(true);await Promise.resolve();
  for(const tick of h.timers.values())tick();
  expect(h.notes.length).toBeGreaterThan(0);
  expect(h.levels.at(-1)).toBeCloseTo(.075);
  expect(h.timers.size).toBe(1);
  h.music.volume(.5);
  expect(h.levels.at(-1)).toBeCloseTo(.0375);
  h.choice.value='off';h.music.change(true);
  expect(h.levels.at(-1)).toBe(0);
  expect(h.timers.size).toBe(0);
  const before=h.notes.length;
  h.choice.value='3';h.music.change(true);await Promise.resolve();
  for(const tick of h.timers.values())tick();
  expect(h.notes.length).toBeGreaterThan(before);
  h.music.pause();expect(h.levels.at(-1)).toBe(0);
  h.music.end();expect(h.timers.size).toBe(0);
  h.music.dispose();expect(h.stats().closed).toBe(1);
});

test('different article identities derive different musical notes without a network asset',async()=>{
  const a=harness('Quiet studio'),b=harness('Field report');
  for(const h of [a,b]){h.music.start();await Promise.resolve();for(const tick of h.timers.values())tick();}
  expect(a.notes).not.toEqual(b.notes);
  a.music.dispose();b.music.dispose();
});

test('voice reading continues when Web Audio is unavailable',()=>{
  const choice={value:'auto'};
  const music=new Function('root','heading','window','setInterval','clearInterval',
    helper+'return bfEditorialSoundtrack(root,heading);')(
      {querySelector:()=>choice},'Unvoiced device',{},{},()=>{});
  expect(()=>{music.start();music.duck(true);music.end();music.dispose();}).not.toThrow();
});
