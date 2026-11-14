#!/bin/sh
set -eu
bin=$1
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
t=$(mktemp -d); trap 'rm -rf "$t"' EXIT INT TERM
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=440:duration=2" -ar 44100 -ac 1 -y "$t/one.wav"
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=660:duration=2" -ar 44100 -ac 1 -y "$t/two.wav"
one=$(shasum -a 256 "$t/one.wav" | awk '{print $1}');two=$(shasum -a 256 "$t/two.wav" | awk '{print $1}')
jq -n --arg one "$t/one.wav" --arg two "$t/two.wav" --arg a "$one" --arg b "$two" '{schema:"bonfyre.narration-assembly.v1",sample_rate:44100,clips:[{path:$one,sha256:$a,at:0,frames:88200,gain:0.5,script:"First <passage> & more."},{path:$two,sha256:$b,at:132300,frames:88200,gain:1,script:"Second passage."}]}' > "$t/plan.json"
"$bin" assemble-narration --plan "$t/plan.json" --out "$t/voice.wav" --transcript "$t/voice.vtt" > "$t/result.json"
jq -e '.schema=="bonfyre.narration-assembly-result.v1" and .frames==220500 and .passages==2' "$t/result.json" >/dev/null
grep -q '00:00:03.000 --> 00:00:05.000' "$t/voice.vtt"
grep -q 'First &lt;passage&gt; &amp; more.' "$t/voice.vtt"
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=220:duration=7" -ar 44100 -ac 2 -y "$t/bed.wav"
bed=$(shasum -a 256 "$t/bed.wav" | awk '{print $1}');assembled=$(jq -r .audio_sha256 "$t/result.json")
"$bin" mix-article --voice "$t/voice.wav" --voice-sha256 "$assembled" --bed "$t/bed.wav" --bed-sha256 "$bed" \
 --bed-license CC0-1.0 --bed-credit "Synthetic fixture" --bed-source-url https://example.org/bed \
 --transcript "$t/voice.vtt" --program combined --profile essay --out "$t/master" --article-revision-sha256 "$one" >/dev/null
jq -e --arg revision "$one" '.narration_article_revision_sha256==$revision and .editorial_profile=="essay"' "$t/master/manifest.json" >/dev/null
grep -q '00:00:05.500 --> 00:00:07.500' "$t/master/transcript.vtt"
python3 - "$t/voice.wav" <<'PY'
import sys,wave,struct,math
with wave.open(sys.argv[1],'rb') as f:
 assert f.getframerate()==44100 and f.getnframes()==220500 and f.getnchannels()==2
 data=struct.unpack('<'+str(f.getnframes()*2)+'h',f.readframes(f.getnframes()))
 def rms(a,b):
  x=data[a*44100*2:b*44100*2];return math.sqrt(sum(v*v for v in x)/len(x))
 first,gap,second=rms(0,1),rms(2,3),rms(3,4)
 assert first>100 and gap<1 and 1.9<second/first<2.1,(first,gap,second)
PY
bun --input-type=module - "$t" <<'JS'
import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import {createProduction,cellFingerprint,productionDigest,narrationAssemblyPlan} from '../../integrations/browser/narration-production.mjs';
const dir=process.argv[2],p=createProduction({id:'article-proof',title:'Article',items:[{id:'article',title:'Article',source:{href:'https://example.org/article',sha256:'a'.repeat(64)},segments:[{id:'one',text:'First reviewed passage.'},{id:'two',text:'Second reviewed passage.'}]}]});
for(const [i,cell] of p.cells.entries()){
 const bytes=await readFile(dir+'/'+(i?'two':'one')+'.wav'),audio=bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength);
 cell.overrides={gapMs:i?0:1000,gain:i?1:0.5};const fingerprint=await cellFingerprint(p,cell),sha256=await productionDigest(audio);
 cell.reviewed=fingerprint;cell.activeTake='take';cell.takes=[{id:'take',fingerprint,sha256,bytes:audio,frames:88200,sampleRate:44100}];
}
const prepared=await narrationAssemblyPlan(p,'en');await mkdir(dir+'/sources');
const retained=await productionDigest(prepared.files[0].bytes);new Uint8Array(p.cells[0].takes[0].bytes)[50]^=1;
if(await productionDigest(prepared.files[0].bytes)!==retained)throw Error('Assembly bytes changed after preparation.');
new Uint8Array(p.cells[0].takes[0].bytes)[50]^=1;
for(let i=0;i<prepared.files.length;i++)await copyFile(dir+'/'+(i?'two':'one')+'.wav',dir+'/sources/'+prepared.files[i].sha256+'.wav');
await writeFile(dir+'/workspace-plan.json',JSON.stringify(prepared.plan));
p.cells[0].script='Changed after review.';
let refused=false;try{await narrationAssemblyPlan(p,'en');}catch{refused=true;}
if(!refused)throw Error('Changed script was admitted to assembly.');
JS
cue vet -c ../../schemas/control/narration-assembly.cue "$t/workspace-plan.json" -d '#NarrationAssembly'
"$bin" assemble-narration --plan "$t/workspace-plan.json" --source-dir "$t/sources" --out "$t/workspace.wav" --transcript "$t/workspace.vtt" > "$t/workspace-result.json"
jq -e '.passages==2 and .frames==220500' "$t/workspace-result.json" >/dev/null
grep -q 'Second reviewed passage.' "$t/workspace.vtt"
jq '.clips[0].path="../escape.wav"' "$t/workspace-plan.json" > "$t/escape.json"
if "$bin" assemble-narration --plan "$t/escape.json" --source-dir "$t/sources" --out "$t/escape.wav" --transcript "$t/escape.vtt" >/dev/null 2>&1; then echo 'FAIL: source path escaped its identity' >&2; exit 1; fi
jq '.clips[1].at=1' "$t/plan.json" > "$t/overlap.json"
if "$bin" assemble-narration --plan "$t/overlap.json" --out "$t/bad.wav" --transcript "$t/bad.vtt" >/dev/null 2>&1; then echo 'FAIL: overlap accepted' >&2; exit 1; fi
[ ! -e "$t/bad.wav" ] && [ ! -e "$t/bad.vtt" ]
jq -n --arg path "$t/one.wav" --arg sha "$one" '{schema:"bonfyre.narration-assembly.v1",sample_rate:44100,clips:[range(0;64)|{path:$path,sha256:$sha,at:(.*88200),frames:88200,gain:0.7,script:("Passage "+(tostring))}]}' > "$t/scale.json"
"$bin" assemble-narration --plan "$t/scale.json" --out "$t/scale.wav" --transcript "$t/scale.vtt" > "$t/scale-result.json"
jq -e '.passages==64 and .frames==5644800' "$t/scale-result.json" >/dev/null
[ "$(grep -c -- '-->' "$t/scale.vtt")" = 64 ]
printf 'altered' >> "$t/two.wav"
if "$bin" assemble-narration --plan "$t/plan.json" --out "$t/stale.wav" --transcript "$t/stale.vtt" >/dev/null 2>&1; then echo 'FAIL: changed take accepted' >&2; exit 1; fi
[ ! -e "$t/stale.wav" ] && [ ! -e "$t/stale.vtt" ]
echo 'Narration assembly: 64 takes, timing, gain, music mix and stale-source refusal: PASS'
