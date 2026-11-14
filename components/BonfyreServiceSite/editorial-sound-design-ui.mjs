/* Native editorial profiles + account-owned, digest-deduplicated music stems.
 * This is an authoring arrangement and a bounded browser preview. Final M4A,
 * automatic ducking and publication remain with the native Broadcast owner.
 */
import {productionDigest,validateProduction,mixerSession} from '../../bridge/narration-production.mjs';
import {suggestedEditorialTrack} from "./editorial-music-catalogue.mjs";
const stemPrefix='music:stem:',soundSchema='aurekai.editorial-sound-design.v1',stemSchema='aurekai.music-stem.v1';
const hash=/^[a-f0-9]{64}$/;
const licences=new Set(['CC0-1.0','CC-BY-4.0','original-owned','licensed-commercial']);
let activePreview=null,previewGeneration=0,musicListener=null;
const node=(tag,text)=>{const element=document.createElement(tag);if(text!==undefined)element.textContent=text;return element;};
const urlOK=value=>{
 try{const u=new URL(value);return u.protocol==='https:'&&!u.username&&!u.password&&value.length<=600;}catch{return false;}
};
export function readNativeEditorialProfiles(tsv) {
 const rows=typeof tsv==='string'?tsv.trim().split(/\r?\n/):[];
 if(rows.shift()!=='profile\tintro_ms\ttail_ms\tbed_gain\tduck_release_ms'||rows.length<5||rows.length>20)
  throw Error('The native sound profile catalogue is missing or unsupported.');
 const catalog=new Map();
 for(const row of rows) {
  const [name,opening,closing,level,release,...extra]=row.split('\t');
  const introMs=Number(opening),tailMs=Number(closing),gain=Number(level),releaseMs=Number(release);
  if(extra.length||!/^[a-z]{3,30}$/.test(name)||catalog.has(name)||
     !Number.isInteger(introMs)||introMs<500||introMs>10000||
     !Number.isInteger(tailMs)||tailMs<500||tailMs>10000||
     !Number.isFinite(gain)||gain<0.01||gain>0.3||
     !Number.isInteger(releaseMs)||releaseMs<100||releaseMs>5000)
   throw Error('The native sound profile catalogue has an invalid preset.');
  catalog.set(name,{name,introMs,tailMs,gain,releaseMs});
 }
 return catalog;
}
export function musicStemRecord({sha256,title,bytes,licence,credit,sourceUrl,durationMs}){
 if(!hash.test(sha256)||!bytes||typeof bytes.arrayBuffer!=='function'||
    !Number.isInteger(bytes.size)||bytes.size<1024||bytes.size>24*1024*1024||
    !licences.has(licence)||!urlOK(sourceUrl)||typeof credit!=='string'||
    !credit.trim()||credit.length>240||typeof title!=='string'||title.length>140||
    !Number.isInteger(durationMs)||durationMs<5000||durationMs>7200000)
   throw Error('Choose a measured, locally retained music stem with complete attribution and explicit terms.');
 return {schema:stemSchema,title,sha256,bytes,licence,credit,sourceUrl,durationMs,
   rightsReview:'pending',cannotInfer:'An uploader credit or license label is not independent clearance of musical rights.'};
}
const rowMeta=async(store,prefix)=>{
 const records=[];let cursor='',epoch=-1;
 for(let n=0;n<20;n++){
  const page=await store.readMetadata(prefix,{cursor,epoch});
  records.push(...page.rows);if(!page.more)return records;
  cursor=page.cursor;epoch=page.epoch;
 }
 throw Error('Music library has more than 1,000 stems. Narrow the collection before opening it.');
};
const textField=(caption,input)=>{const label=node('label',caption);label.append(input);return label;};
const action=(caption,fn)=>{
 const button=node('button',caption);button.type='button';
 button.addEventListener('click',async()=>{
  button.disabled=true;try{await fn();}catch(error){const status=button.closest('[data-sound-design]')?.querySelector('[data-sound-status]');if(status)status.textContent=error.message;}
  finally{button.disabled=false;}
 });return button;
};
const stop=async()=>{
 previewGeneration++;if(!activePreview)return;
 const {context,sources}=activePreview;activePreview=null;
 for(const source of sources)try{source.stop();}catch{}
 await context.close().catch(()=>{});
};
export async function mountEditorialSoundDesign({host,production,store,save}) {
 validateProduction(production);
 if(musicListener)document.removeEventListener?.('bonfyre:music-retained',musicListener);
 await stop();
 const root=node('section');root.dataset.soundDesign='';root.className='editorial-sound-design';
 const title=node('h4','Editorial sound design'),description=node('p',
  'Choose a distinct production sound profile and reuse a licensed stem across articles. This preview is not the approved final master.');
 const status=node('p','Music use requires editorial and rights review before publication.');
 status.dataset.soundStatus='';status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 const profile=node('select');profile.setAttribute('aria-label','Narration sound profile');
 const response=await fetch(new URL('./editorial-profiles.tsv',import.meta.url),{cache:'no-store',credentials:'same-origin'});
 if(!response.ok)throw Error('Native editorial sound profiles are not installed in this Audio Studio.');
 const catalog=readNativeEditorialProfiles(await response.text());
 for(const p of catalog.values()){
  const option=node('option',p.name[0].toUpperCase()+p.name.slice(1));
  option.value=p.name;profile.append(option);
 }
 profile.value=production.soundDesign?.profile||'feature';
 const previewDetail=node('p'),setDetail=()=>{
  const p=catalog.get(profile.value);
  previewDetail.textContent=p?('Opening '+(p.introMs/1000).toFixed(1)+'s · closing '+(p.tailMs/1000).toFixed(1)+
    's · automatic speech ducking · final master: −16 LUFS target'):'';
 };
 profile.addEventListener('change',setDetail);setDetail();
 const stem=node('select');stem.setAttribute('aria-label','Reusable licensed music stem');
 const placeholder=node('option','Choose retained music');placeholder.value='';stem.append(placeholder);
 const refreshStems=async()=>{
  const selected=stem.value||production.soundDesign?.musicStem||'';stem.replaceChildren(placeholder);
  for(const row of await rowMeta(store,stemPrefix)){
   const meta=row.value;if(!hash.test(row.key.slice(stemPrefix.length))||meta?.schema!==stemSchema||meta.sha256!==row.key.slice(stemPrefix.length)||!licences.has(meta.licence))continue;
   const option=node('option',meta.title+' · '+meta.credit+' · '+meta.licence);option.value=row.key;stem.append(option);
  }
  if(selected&&[...stem.options].some(o=>o.value===selected))stem.value=selected;
 };
 await refreshStems();const current=production.soundDesign?.musicStem||'';
 const suggestion=node('p');suggestion.className='editorial-music-suggestion';
 const sourceIdentity=production.id+':'+(production.items[0]?.source?.sha256||'');
 let autoSelected='';
 const updateSuggestion=()=>{
  const candidate=suggestedEditorialTrack(profile.value,sourceIdentity);
  if(!candidate){suggestion.textContent='No recorded track suggestion for this treatment.';return;}
  const key=stemPrefix+candidate.sha256;
  const available=[...stem.options].some(option=>option.value===key);
  suggestion.textContent='Suggested for this article: '+candidate.title+' — '+candidate.artist+
   (available?' · retained in your library.':' · audition and retain from the Recorded music library above.');
  if(!current&&(stem.value===''||stem.value===autoSelected)){
   stem.value=available?key:'';autoSelected=available?key:'';
  }
 };
 profile.addEventListener('change',updateSuggestion);
 updateSuggestion();
 musicListener=()=>{if(root.isConnected===false){document.removeEventListener?.('bonfyre:music-retained',musicListener);return;}void refreshStems().then(updateSuggestion).catch(error=>{status.textContent=error.message;});};document.addEventListener?.('bonfyre:music-retained',musicListener);
 const controls=node('div');controls.className='editorial-sound-fields';
 controls.append(textField('Sound treatment',profile),previewDetail,textField('Music library',stem));
 controls.append(suggestion);
 const buttonRow=node('div');buttonRow.className='editorial-sound-actions';
 const selection=()=>({schema:soundSchema,profile:profile.value,musicStem:stem.value,rightsReview:'pending'});
 buttonRow.append(action('Save sound arrangement',async()=>{
  if(!catalog.has(profile.value)||!/^music:stem:[a-f0-9]{64}$/.test(stem.value))
   throw Error('Choose a native profile and a retained music stem.');
  const next=structuredClone(production);
  next.soundDesign=selection();next.version++;
  validateProduction(next);
  await save(next);
 }));
 const language=node('select');language.setAttribute('aria-label','Narration language');
 for(const edition of production.languages){const option=node('option',edition.label||edition.id);option.value=edition.id;language.append(option);}controls.prepend(textField('Language',language));
 const previewButton=action('Play complete arrangement',async()=>{
  if(activePreview){await stop();status.textContent='Preview stopped.';return;}const ticket=++previewGeneration;
  const p=catalog.get(profile.value);
  if(!p||!/^music:stem:[a-f0-9]{64}$/.test(stem.value))throw Error('Choose a sound profile and a retained music stem first.');
  const arrangement=await mixerSession(production,language.value);
  const record=await store.readVersion(stem.value);
  if(!record||record.deleted||record.value?.schema!==stemSchema||record.value.sha256!==stem.value.slice(stemPrefix.length))throw Error('The music stem changed or is unavailable.');
  const value=record.value,musicBytes=await value.bytes.arrayBuffer();
  if(await productionDigest(musicBytes)!==value.sha256)throw Error('The music stem no longer matches its retained digest.');
  const Audio=window.AudioContext||window.webkitAudioContext;
  if(!Audio)throw Error('This browser cannot preview an audio arrangement.');
  const context=new Audio(),sources=[];if(ticket!==previewGeneration){await context.close();return;}activePreview={context,sources};
  try{
   if(!Number.isFinite(value.durationMs)||value.durationMs<5000||value.durationMs>600000||
      (arrangement.measured.seconds+value.durationMs/1000)*context.sampleRate*8>256*1024*1024)
    throw Error('This arrangement is too large to preview in the browser.');
   await context.resume();
   const music=await context.decodeAudioData(musicBytes.slice(0)),spoken=[];
   for(const file of arrangement.files)spoken.push(await context.decodeAudioData(file.bytes.slice(0)));if(ticket!==previewGeneration)return;
   if(music.duration<5||spoken.some((buffer,i)=>Math.abs(buffer.duration-arrangement.clips[i].frames/arrangement.measured.sampleRate)>0.1))
    throw Error('The decoded recording does not match the retained take.');
   const begin=context.currentTime+0.15,intro=p.introMs/1000,tail=p.tailMs/1000,
    end=begin+intro+arrangement.measured.seconds+tail,master=context.createGain(),bedGain=context.createGain();
   master.gain.value=0.86;master.connect(context.destination);bedGain.connect(master);
   const musicSource=context.createBufferSource();musicSource.buffer=music;musicSource.loop=true;musicSource.connect(bedGain);sources.push(musicSource);
   const envelope=bedGain.gain;envelope.setValueAtTime(0,begin);envelope.linearRampToValueAtTime(p.gain,begin+0.6);
   arrangement.clips.forEach((clip,i)=>{
    const start=begin+intro+clip.at/arrangement.measured.sampleRate,duration=clip.frames/arrangement.measured.sampleRate,
     voice=context.createBufferSource(),gain=context.createGain();
    voice.buffer=spoken[i];gain.gain.value=clip.gain;voice.connect(gain);gain.connect(master);sources.push(voice);
    envelope.linearRampToValueAtTime(p.gain*0.075,Math.max(begin+0.6,start-0.05));
    envelope.setTargetAtTime(p.gain,start+duration,p.releaseMs/1000);voice.start(start,0,duration);
   });
   envelope.setValueAtTime(p.gain,end-tail+0.7);envelope.linearRampToValueAtTime(0,end);
   musicSource.start(begin);musicSource.stop(end);
   musicSource.onended=()=>{if(activePreview?.context===context)void stop();};
   status.textContent='Playing the complete narration with music. The final master is rendered separately.';
  }catch(error){if(activePreview?.context===context)activePreview=null;await context.close().catch(()=>{});throw error;}
 });
 buttonRow.append(previewButton,action('Stop preview',()=>stop()));
 const importer=node('details'),summary=node('summary','Add a reusable music stem');
 importer.append(summary);
 const file=node('input');file.type='file';file.accept='audio/wav,audio/mpeg,audio/mp4,audio/x-m4a,audio/aac';file.setAttribute('aria-label','Music audio file');
 const credit=node('input');credit.maxLength=240;credit.placeholder='Composer, artist or owner';
 const url=node('input');url.type='url';url.placeholder='https://... original music or licence page';url.maxLength=600;
 const licence=node('select');
 for(const id of licences){const opt=node('option',id);opt.value=id;licence.append(opt);}
 importer.append(textField('Retained music file',file),textField('Artist / rights credit',credit),
   textField('Source or rights reference',url),textField('Declared usage terms',licence));
 importer.append(node('p','Import retains one content-addressed stem in this account. This does not confirm license clearance or publish the music.'));
 importer.append(action('Retain music in this workspace',async()=>{
  const selected=file.files?.[0];
  if(!selected||selected.size<1024||selected.size>24*1024*1024)throw Error('Choose one supported music recording within 24 MiB.');
  const bytes=await selected.arrayBuffer(),sha256=await productionDigest(bytes),key=stemPrefix+sha256;
  const Audio=window.AudioContext||window.webkitAudioContext;
  if(!Audio)throw Error('This browser cannot validate music playback.');
  const context=new Audio();let durationMs;
  try{durationMs=Math.round((await context.decodeAudioData(bytes.slice(0))).duration*1000);}
  finally{await context.close().catch(()=>{});}
  const value=musicStemRecord({sha256,title:selected.name.slice(0,140),bytes:new Blob([bytes],{type:selected.type}),
    credit:credit.value.trim(),licence:licence.value,sourceUrl:url.value.trim(),durationMs});
  const existing=await store.readVersion(key);
  if(existing){
   if(existing.deleted||existing.value?.sha256!==sha256||
      existing.value.credit!==value.credit||existing.value.licence!==value.licence||
      existing.value.sourceUrl!==value.sourceUrl)
    throw Error('This music is already retained with different attribution. Review the existing rights record.');
  }else await store.saveLocal(key,value,0);
  const retained=await store.readVersion(key);
  if(!retained||retained.deleted||retained.value?.schema!==stemSchema||
     retained.value.sha256!==sha256||retained.value.credit!==value.credit||
     retained.value.licence!==value.licence||retained.value.sourceUrl!==value.sourceUrl)
   throw Error('The music library did not confirm this recording. Reopen the library before retrying.');
  await refreshStems();
  if(![...stem.options].some(option=>option.value===key))
   throw Error('The retained recording is not yet available in the music library.');
  stem.value=key;
  status.textContent='Music retained and selected. Rights review is still pending.';
 }));
 root.append(title,description,controls,buttonRow,importer,status);
 host.append(root);
 return root;
}
