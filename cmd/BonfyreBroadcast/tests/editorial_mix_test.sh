#!/bin/sh
set -eu
bin=$1
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT INT TERM
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=900:duration=5" -ar 44100 -ac 2 -y "$t/voice.wav"
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=220:duration=7" -ar 44100 -ac 2 -y "$t/bed.wav"
printf 'WEBVTT\n\n00:00:00.000 --> 00:00:01.500\nFirst passage\n\n00:00:02.000 --> 00:00:04.600\nSecond passage\n' > "$t/voice.vtt"
voice=$(shasum -a 256 "$t/voice.wav" | awk '{print $1}')
music=$(shasum -a 256 "$t/bed.wav" | awk '{print $1}')
render() {
 "$bin" mix-article --voice "$t/voice.wav" --voice-sha256 "$voice" \
   --bed "$t/bed.wav" --bed-sha256 "$music" \
   --bed-license CC0-1.0 --bed-credit "Synthetic original test tone" \
   --bed-source-url "https://aurekai.work/testing/editorial-bed" \
   --transcript "$t/voice.vtt" --profile "$1" --program "test-$1" \
   --out "$t/$1" --source-sha256 "$voice"
}
render essay
jq -e --arg music "$music" --arg voice "$voice" \
  '.schema=="bonfyre.editorial-audio.v1" and .editorial_profile=="essay" and
   .voice_sha256==$voice and .narration_source_sha256==$voice and
   .credits[0].audio_sha256==$music and .credits[0].licence=="CC0-1.0" and
   .narration_start_ms==2500 and
   (.waveform_peaks | split(",") | length)==64 and
   .waveform_audio_sha256==.audio_sha256' "$t/essay/manifest.json" >/dev/null ||
   { echo "FAIL: profile manifest lost source identity, credit or waveform" >&2; exit 1; }
grep -q '00:00:02.500 --> 00:00:04.000' "$t/essay/transcript.vtt" ||
   { echo "FAIL: transcript not aligned with narration entry" >&2; exit 1; }
jq -e '.chapters | length==3 and .[1].startTime==2.5' "$t/essay/chapters.json" >/dev/null ||
   { echo "FAIL: editorial chapters not at real audio offsets" >&2; exit 1; }
actual=$(shasum -a 256 "$t/essay/program.m4a" | awk '{print $1}')
expected=$(jq -r .audio_sha256 "$t/essay/manifest.json")
[ "$actual" = "$expected" ] || { echo "FAIL: final audio digest incorrect" >&2; exit 1; }
duration=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$t/essay/program.m4a")
awk -v n="$duration" 'BEGIN {exit !(n>10.7 && n<11.1)}' ||
   { echo "FAIL: intro, spoken track, tail duration incorrect" >&2; exit 1; }
if render essay >/dev/null 2>&1; then echo "FAIL: existing reviewed edition overwritten" >&2; exit 1; fi
if "$bin" mix-article --voice "$t/voice.wav" --voice-sha256 "$voice" \
  --bed "$t/bed.wav" --bed-sha256 "$(printf '%064d' 0)" \
  --bed-license CC0-1.0 --bed-credit "Synthetic fixture" \
  --bed-source-url "https://aurekai.work/testing" \
  --transcript "$t/voice.vtt" --program bad --out "$t/bad" >/dev/null 2>&1; then
 echo "FAIL: unverified bed admitted" >&2; exit 1
fi
printf 'WEBVTT\n\n00:00:03.000 --> 00:00:09.000\nBeyond the take\n' > "$t/bad.vtt"
if "$bin" mix-article --voice "$t/voice.wav" --voice-sha256 "$voice" \
  --bed "$t/bed.wav" --bed-sha256 "$music" \
  --bed-license CC0-1.0 --bed-credit "Synthetic fixture" \
  --bed-source-url "https://aurekai.work/testing" \
  --transcript "$t/bad.vtt" --program invalid-vtt --out "$t/invalid" >/dev/null 2>&1; then
 echo "FAIL: misaligned transcript accepted" >&2; exit 1
fi
[ ! -e "$t/invalid/program.m4a" ] || { echo "FAIL: partial audio remained" >&2; exit 1; }
for profile in newsroom feature conversation documentary; do
  render "$profile" >/dev/null
  jq -e --arg name "$profile" '.editorial_profile==$name and .narration_start_ms>0 and
    .credits[0].creator=="Synthetic original test tone"' "$t/$profile/manifest.json" >/dev/null ||
  { echo "FAIL: profile $profile dropped credit or structure" >&2; exit 1; }
done
[ "$(jq -r .narration_start_ms "$t/newsroom/manifest.json")" != "$(jq -r .narration_start_ms "$t/documentary/manifest.json")" ] ||
 { echo "FAIL: profiles have identical timing" >&2; exit 1; }
mkdir -p "$t/assets"
sqlite3 "$t/cms.db" <<'SQL'
CREATE TABLE site_page(id INTEGER PRIMARY KEY,namespace TEXT,route TEXT,title TEXT,heading TEXT,lede TEXT,media_subject_ref TEXT,editorial_kind TEXT,status TEXT);
CREATE TABLE site_fragment(id INTEGER PRIMARY KEY,namespace TEXT,page TEXT,heading TEXT,body TEXT,kind TEXT,status TEXT,audience TEXT,position INTEGER,semantic_tags TEXT,coordinate TEXT,slot TEXT);
CREATE TABLE community_post(id INTEGER PRIMARY KEY,namespace TEXT,post TEXT,title TEXT,body TEXT,visibility TEXT,moderation_state TEXT,status TEXT);
CREATE TABLE media_program(id INTEGER PRIMARY KEY,namespace TEXT,created_at TEXT,updated_at TEXT,status TEXT,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,duration_ms INTEGER,transcript_ref TEXT,chapters TEXT,captions TEXT,publication_state TEXT,cannot_infer TEXT,waveform_peaks TEXT,waveform_audio_sha256 TEXT,narration_source_sha256 TEXT,editorial_profile TEXT,music_credit TEXT,music_license TEXT,music_source_url TEXT,music_source_sha256 TEXT,narration_start_ms INTEGER,narration_article_revision_sha256 TEXT);
CREATE UNIQUE INDEX unique_media_program ON media_program(namespace,program_id);
INSERT INTO site_page VALUES(1,'acme','/article','Native article','Native article','A published essay about the restoration of a community arts venue.','article:music-story','article','published');
INSERT INTO site_fragment VALUES(1,'acme','/article','The restored venue','The project has reopened the old community arts venue with a program of readings and local performances.','prose','published','public',1,'{}','intro','showcase');
SQL
source_rev=$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:music-story | cut -f4)
cc -O2 -std=c11 -I../../lib/libbonfyre/include -I../../engine/core/include -o "$t/article-revision" tests/article_revision_test.c -L../../lib/libbonfyre -lbonfyre -lsqlite3
article_rev=$("$t/article-revision" "$t/cms.db" acme article:music-story)
"$bin" mix-article --voice "$t/voice.wav" --voice-sha256 "$voice" \
 --bed "$t/bed.wav" --bed-sha256 "$music" --bed-license CC0-1.0 --bed-credit "Synthetic original test tone" \
 --bed-source-url "https://aurekai.work/testing/editorial-bed" --transcript "$t/voice.vtt" \
 --profile essay --program v2-article --out "$t/v2" --article-revision-sha256 "$article_rev" >/dev/null
"$bin" publish "$t/v2" --cms "$t/cms.db" --subject article:music-story \
 --source-namespace acme --article-revision-sha256 "$article_rev" --asset-root "$t/assets" >/dev/null
[ "$(sqlite3 "$t/cms.db" "SELECT narration_article_revision_sha256 FROM media_program WHERE program_id LIKE 'broadcast:v2-article:%'")" = "$article_rev" ] || { echo "FAIL: article revision not retained" >&2; exit 1; }
[ "$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:music-story | cut -f5)" = awaiting-review ] || { echo "FAIL: v2 draft review state" >&2; exit 1; }
sqlite3 "$t/cms.db" "UPDATE site_fragment SET semantic_tags=json_set('{}','$.authoring_v1.editorial',json('[]')) WHERE id=1"
[ "$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:music-story | cut -f5)" = stale-draft ] || { echo "FAIL: v2 structured edit not marked stale" >&2; exit 1; }
if "$bin" publish "$t/v2" --cms "$t/cms.db" --subject article:music-story \
 --source-namespace acme --article-revision-sha256 "$article_rev" --asset-root "$t/assets" >/dev/null 2>&1; then
 echo "FAIL: structured article change retained old mix" >&2; exit 1
fi
sqlite3 "$t/cms.db" "UPDATE site_fragment SET semantic_tags='{}' WHERE id=1; DELETE FROM media_program WHERE program_id LIKE 'broadcast:v2-article:%'"
"$bin" mix-article --voice "$t/voice.wav" --voice-sha256 "$voice" \
    --bed "$t/bed.wav" --bed-sha256 "$music" \
    --bed-license CC0-1.0 --bed-credit "Synthetic original test tone" \
    --bed-source-url "https://aurekai.work/testing/editorial-bed" \
    --transcript "$t/voice.vtt" --profile feature --program "music-published-article" \
    --out "$t/reviewed" --source-sha256 "$source_rev" >/dev/null
if "$bin" publish "$t/reviewed" --cms "$t/cms.db" --subject article:music-story \
    --asset-root "$t/assets" >/dev/null 2>&1; then
 echo "FAIL: editorial edition bypassed current-source review" >&2; exit 1
fi
"$bin" publish "$t/reviewed" --cms "$t/cms.db" --subject article:music-story \
    --source-namespace acme --source-sha256 "$source_rev" --asset-root "$t/assets" >/dev/null
row=$(sqlite3 -tabs "$t/cms.db" "SELECT editorial_profile,music_credit,music_license,music_source_sha256,narration_start_ms,narration_source_sha256,status FROM media_program WHERE subject_ref='article:music-story'")
printf '%s' "$row" | grep -q "feature	Synthetic original test tone	CC0-1.0	$music	3400	$source_rev	draft" ||
 { echo "FAIL: canonical CMS lost source-bound editorial attribution" >&2; exit 1; }
sqlite3 "$t/cms.db" "UPDATE site_fragment SET body='The revised article now includes a second renovated performance space with a different timeline.' WHERE id=1"
if "$bin" publish "$t/reviewed" --cms "$t/cms.db" --subject article:music-story \
    --source-namespace acme --source-sha256 "$source_rev" --asset-root "$t/assets" >/dev/null 2>&1; then
 echo "FAIL: older licensed mix republished against edited article" >&2; exit 1
fi
sqlite3 "$t/cms.db" "UPDATE media_program SET status='published',publication_state='published' WHERE subject_ref='article:music-story'"
if "$bin" publish "$t/reviewed" --cms "$t/cms.db" --subject article:music-story \
    --source-namespace acme --source-sha256 "$source_rev" --asset-root "$t/assets" >/dev/null 2>&1; then
 echo "FAIL: published music edition overwritten by draft" >&2; exit 1
fi
bun --input-type=module - "$bin" "$t/pack" <<'JS'
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {readFile,writeFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {readWorkspaceFile} from '../../scripts/workspace_material.mjs';
import {materializeCreatorWorkspace} from '../../scripts/materialize_creator_workspace.mjs';
import {createProduction,validateProduction,cellFingerprint,productionDigest} from '../../integrations/browser/narration-production.mjs';
const [binary,root]=process.argv.slice(2);
const modulePath=new URL('../../components/BonfyreServiceSite/editorial-sound-design-ui.mjs',import.meta.url);
const bridge=new URL('../../integrations/browser/narration-production.mjs',import.meta.url).href;
const catalogueBridge=new URL('../../components/BonfyreServiceSite/editorial-music-catalogue.mjs',import.meta.url).href;
const source=(await readFile(modulePath,'utf8'))
 .replace('../../bridge/narration-production.mjs',bridge)
 .replace('./editorial-music-catalogue.mjs',catalogueBridge);
const moduleCopy=root+'-editor.mjs';
await writeFile(moduleCopy,source);
const {readNativeEditorialProfiles,musicStemRecord,mountEditorialSoundDesign}=await import(pathToFileURL(moduleCopy).href);
const result=spawnSync(binary,['profiles'],{encoding:'utf8'});
assert.equal(result.status,0);
const profiles=readNativeEditorialProfiles(result.stdout);
assert.equal(profiles.size,5);
assert.equal(profiles.get('essay').introMs,2500);
assert.equal(profiles.get('documentary').tailMs,4600);
assert.throws(()=>readNativeEditorialProfiles(result.stdout.replace('0.110','4.500')));
const digest='a'.repeat(64),key='music:stem:'+digest;
const make=id=>createProduction({id,title:'Reviewed article '+id,items:[{id:'article',title:'Article',source:{href:'https://aurekai.work/article',sha256:digest},segments:[{id:'passage',text:'The story describes an independently authored local community publication.'}]}]});
const a=make('first'),b=make('second');
for(const record of [a,b]){
 record.soundDesign={schema:'aurekai.editorial-sound-design.v1',profile:'essay',musicStem:key,rightsReview:'pending'};
 validateProduction(record);
}
assert.equal(a.soundDesign.musicStem,b.soundDesign.musicStem);
assert.throws(()=>validateProduction({...a,soundDesign:{...a.soundDesign,rightsReview:'approved'}}));
assert.throws(()=>musicStemRecord({sha256:digest,title:'Bad terms',bytes:new Blob([new Uint8Array(2048)]),
 licence:'random-licence',credit:'Unknown',sourceUrl:'https://aurekai.work/rights',durationMs:10000}));
const record=musicStemRecord({sha256:digest,title:'Native audio bed',bytes:new Blob([new Uint8Array(2048)]),
 licence:'CC0-1.0',credit:'Synthetic fixture',sourceUrl:'https://aurekai.work/testing',durationMs:10000});
assert.equal(record.rightsReview,'pending');
const built=await materializeCreatorWorkspace({root});
const file=(await readWorkspaceFile(root,built.id,'data/creator/editorial-profiles.tsv')).toString();
const inputModule=(await readWorkspaceFile(root,built.id,'data/creator/creator-import.mjs')).toString();
assert.match(inputModule,/export function decodeWav/);
assert.equal(file,result.stdout);
const ui=(await readWorkspaceFile(root,built.id,'data/creator/editorial-sound-design-ui.mjs')).toString();
assert.match(ui,/music:stem:/);
const html=(await readWorkspaceFile(root,built.id,'audio/index.html')).toString();
assert.match(html,/data-narration-workspace/);
assert.match(html,/data\/creator\/narration-workspace-client.mjs/);
const catalogue=(await readWorkspaceFile(root,built.id,'data/creator/editorial-music-catalogue.mjs')).toString();
const listening=(await readWorkspaceFile(root,built.id,'data/creator/editorial-music-library-client.mjs')).toString();
assert.match(catalogue,/Mellow Cafe Vibe/);
assert.match(catalogue,/rightsReview/);
assert.match(listening,/verifiedEditorialMusicBytes/);
assert.match(listening,/SHA-256/);
assert.match(html,/data-editorial-library/);
assert.match(html,/Technical mixer lab/);
assert(html.indexOf('data-editorial-library')<html.indexOf('class="audio-demo-lab"'));
assert(!html.includes('/apps/__WORKSPACE__/apps/__WORKSPACE__/'));
{
 const originalDocument=globalThis.document,originalWindow=globalThis.window,originalFetch=globalThis.fetch;
 const started=[];let lastContext,decodeGate=null;
 class Element {constructor(tag){this.tag=tag;this.children=[];this.dataset={};this.attributes={};this.value='';this.textContent='';this.listeners={};}
  append(...nodes){for(const child of nodes){child.parent=this;this.children.push(child);if(this.tag==='select'&&this.children.length===1)this.value=child.value;}}
  prepend(...nodes){for(const child of nodes)child.parent=this;this.children.unshift(...nodes);} replaceChildren(...nodes){this.children=[];this.append(...nodes);}
  setAttribute(key,value){this.attributes[key]=value;} addEventListener(type,fn){this.listeners[type]=fn;}
  get options(){return this.children;} closest(){return this.parent?.closest?.()||this;}
  querySelector(){return null;} async click(){await this.listeners.click?.();}
 }
 class Audio {constructor(){this.sampleRate=8000;this.currentTime=0;this.destination={};this.nodes=[];lastContext=this;}
  async resume(){} async close(){this.closed=true;} createGain(){return {gain:{value:0,setValueAtTime(){},linearRampToValueAtTime(){},setTargetAtTime(){}},connect(){}};}
  createBufferSource(){const n={connect(){},start(...args){started.push(args);},stop(){},buffer:null};this.nodes.push(n);return n;}
  async decodeAudioData(bytes){if(decodeGate)await decodeGate;const n=new Uint8Array(bytes)[0];return {duration:n===3?10:25,sampleRate:8000};}
 }
 try{
  const listeners=new Map();globalThis.document={createElement:tag=>new Element(tag),addEventListener:(key,fn)=>listeners.set(key,fn),removeEventListener:key=>listeners.delete(key),dispatchEvent:event=>listeners.get(event.type)?.(event)};globalThis.window={AudioContext:Audio};
  globalThis.fetch=async()=>({ok:true,text:async()=>result.stdout});
  let production=createProduction({id:'long-article',title:'Long article',items:[{id:'article',title:'Article',source:{href:'https://aurekai.work/article',sha256:digest},segments:[{id:'first',text:'The first passage needs a reviewed recording.'},{id:'second',text:'The second passage also needs a reviewed recording.'}]}]});
  const music=new Uint8Array(2048);music[0]=3;const musicHash=await productionDigest(music.buffer),musicKey='music:stem:'+musicHash;
  for(const [index,cell] of production.cells.entries()){const bytes=new Uint8Array(80);bytes[0]=index+1;const fingerprint=await cellFingerprint(production,cell);cell.reviewed=fingerprint;cell.takes=[{id:'take-'+index,fingerprint,sha256:await productionDigest(bytes.buffer),bytes:bytes.buffer,frames:200000,sampleRate:8000}];cell.activeTake='take-'+index;}
  production.soundDesign={schema:'aurekai.editorial-sound-design.v1',profile:'essay',musicStem:musicKey,rightsReview:'pending'};
  const musicRecord={schema:'aurekai.music-stem.v1',sha256:musicHash,title:'Bed',credit:'Composer',licence:'CC0-1.0',bytes:new Blob([music]),durationMs:10000};
  let musicVisible=false;const musicRecords=new Map([[musicKey,musicRecord]]);
  const store={readMetadata:async()=>({rows:musicVisible?[...musicRecords].map(([key,value])=>({key,value})):[],more:false}),
   readVersion:async key=>musicRecords.has(key)?({value:musicRecords.get(key),deleted:false}):null,
   saveLocal:async(key,value)=>{musicRecords.set(key,value);return {revision:1};}};
  const host=new Element('main');await mountEditorialSoundDesign({host,production,store,save:async()=>{}});
  musicVisible=true;document.dispatchEvent({type:'bonfyre:music-retained'});await new Promise(resolve=>setTimeout(resolve,0));
  const descend=node=>[node,...node.children.flatMap(descend)];const all=descend(host),play=all.find(node=>node.tag==='button'&&node.textContent==='Play complete arrangement');
  assert.ok(all.some(node=>node.tag==='option'&&node.value===musicKey));
  assert.ok(play);await play.click();assert.equal(started.length,3);assert.equal(lastContext.nodes.length,3);
  assert.equal(started[0][2],25);assert.equal(started[1][2],25);assert.ok(started[1][0]>started[0][0]+25);
  assert.equal(all.find(node=>Object.hasOwn(node.dataset,'soundStatus')).textContent,'Playing the complete narration with music. The final master is rendered separately.');
  await all.find(node=>node.tag==='button'&&node.textContent==='Stop preview').click();assert.equal(lastContext.closed,true);
  const input=all.find(node=>node.tag==='input'&&node.type==='file');
  const credit=all.find(node=>node.placeholder==='Composer, artist or owner'),url=all.find(node=>node.type==='url');
  const license=all.find(node=>node.tag==='select'&&node.options.some(option=>option.value==='CC0-1.0'));
  const imported=new Uint8Array(2048);imported[0]=3;imported[1]=1;const file=new Blob([imported],{type:'audio/wav'});file.name='Second recording.wav';
  input.files=[file];credit.value='Another composer';url.value='https://example.org/music';license.value='CC0-1.0';
  await all.find(node=>node.tag==='button'&&node.textContent==='Retain music in this workspace').click();
  const importedKey='music:stem:'+await productionDigest(imported.buffer);
  const musicSelect=all.find(node=>node.attributes['aria-label']==='Reusable licensed music stem');
  assert.equal(musicSelect.value,importedKey);assert(musicRecords.has(importedKey));
  assert.equal(all.find(node=>Object.hasOwn(node.dataset,'soundStatus')).textContent,'Music retained and selected. Rights review is still pending.');
  musicSelect.value=musicKey;
  const prior=lastContext;let release;decodeGate=new Promise(resolve=>{release=resolve;});started.length=0;const pending=play.click();
  for(let n=0;n<100&&lastContext===prior;n++)await new Promise(resolve=>setTimeout(resolve,1));assert.notEqual(lastContext,prior);await all.find(node=>node.tag==='button'&&node.textContent==='Stop preview').click();release();await pending;assert.equal(started.length,0);
 }finally{globalThis.document=originalDocument;globalThis.window=originalWindow;globalThis.fetch=originalFetch;}
}
console.log('Bonfyre Audio Studio: one native profile catalogue, reusable music stem refs, review gate and packaged editor: PASS');
JS
echo "Bonfyre editorial audio: five mixes, attribution, aligned captions, measured waveform, immutable output and digest refusal: PASS"
