import { readFileSync, writeFileSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync, renameSync, realpathSync, lstatSync } from 'node:fs';
import { join, resolve, dirname, extname } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';

const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const types={'.mp4':'video/mp4','.wav':'audio/wav','.mp3':'audio/mpeg','.m4a':'audio/mp4','.webm':'video/webm'};
const limit=25*1024*1024;
function mediaBytes(root,file) {
  if(!/^media\/[a-z0-9-]+\.(mp4|wav|mp3|m4a|webm)$/.test(file)) throw Error('Invalid local recording path.');
  const path=join(root,file), actual=realpathSync(path);
  if(actual!==join(realpathSync(root),file)||!lstatSync(path).isFile()||lstatSync(path).isSymbolicLink()) throw Error('The recording must be a regular file inside this collection.');
  if(lstatSync(path).size>limit) throw Error('Use a recording smaller than 25 MB.');
  return readFileSync(path);
}
export function checkRecording(root,media,paragraphs) {
  if(!media||!types[extname(media.file??'')]||media.content_type!==types[extname(media.file)]||
    !/^[a-f0-9]{64}$/.test(media.sha256??'')||!/^[a-f0-9]{64}$/.test(media.transcript_sha256??'')||
    media.review!=='unreviewed'||!Number.isFinite(media.duration_ms)||media.duration_ms<=0||media.duration_ms>600000||
    !Array.isArray(media.segments)||media.segments.length!==paragraphs.length) throw Error('Invalid recording metadata.');
  if(media.source_url!==undefined) {
    let url;try{url=new URL(media.source_url);}catch{throw Error('Use an HTTP or HTTPS source address.');}
    if(!['http:','https:'].includes(url.protocol)||url.username||url.password||media.source_url.length>2048)
      throw Error('Use an HTTP or HTTPS source address.');
  }
  if(media.credit!==undefined&&(typeof media.credit!=='string'||media.credit.length>500))throw Error('Keep the recording credit within 500 characters.');
  let previous=-1;
  for(const segment of media.segments) {
    if(!Number.isFinite(segment.start_ms)||!Number.isFinite(segment.end_ms)||segment.start_ms<0||
      segment.start_ms<previous||segment.end_ms<=segment.start_ms||segment.end_ms>media.duration_ms)
      throw Error('Transcript times must be ordered and within the recording.');
    previous=segment.start_ms;
  }
  if(hash(mediaBytes(root,media.file))!==media.sha256) throw Error('The recording changed. Import it again before using its transcript.');
  return media;
}
export function readRecording(root,media) {
  const bytes=mediaBytes(root,media.file);
  if(hash(bytes)!==media.sha256) throw Error('The recording changed. Import it again before using its transcript.');
  return bytes;
}
async function native(argv) {
  const child=Bun.spawn(argv,{stdout:'pipe',stderr:'pipe'}), timer=setTimeout(()=>child.kill(),180000);
  try {
    const [out,err,code]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);
    if(code!==0)throw Error('Recording preparation failed: '+err.slice(-1800));
    return out;
  } finally {clearTimeout(timer);}
}
export function transcriptPassages(segments,duration=Infinity) {
  const passages=[];let previousStart=-1;
  for(const segment of segments) {
    if(typeof segment.text!=='string'||!segment.text.trim())throw Error('The transcript contains an empty segment.');
    if(!Number.isFinite(segment.t0)||!Number.isFinite(segment.t1)||segment.t0<0||segment.t0<previousStart||segment.t1<=segment.t0||segment.t1>duration)throw Error('Transcript times must be ordered and within the recording.');
    previousStart=segment.t0;
    const next={text:segment.text.trim().replace(/\s+/g,' '),start_ms:segment.t0,end_ms:segment.t1};
    const previous=passages.at(-1);
    if(previous&&!/[.!?]["'”’)]?$/.test(previous.text)&&next.start_ms>=previous.end_ms&&next.start_ms-previous.end_ms<1500&&previous.text.length+next.text.length<700) {
      previous.text+=' '+next.text;previous.end_ms=next.end_ms;
    } else passages.push(next);
  }
  return passages;
}
export async function importRecording(input,output,{title='Local recording',sourceUrl,credit,nativeDirectory=join(homedir(),'.bonfyre/bin'),run=native}={}) {
  const destination=resolve(output), source=resolve(input), extension=extname(source).toLowerCase();
  if(existsSync(destination))throw Error('Choose a new collection folder; existing work is never overwritten.');
  if(!types[extension])throw Error('Use an MP4, WAV, MP3, M4A or WebM recording.');
  if(typeof title!=='string'||!title.trim()||title.length>200)throw Error('Use a title of 1 to 200 characters.');
  if(!lstatSync(source).isFile()||lstatSync(source).size>limit)throw Error('Use a regular recording file smaller than 25 MB.');
  mkdirSync(dirname(destination),{recursive:true});
  const staging=mkdtempSync(join(dirname(destination),'.recording-import-'));
  try {
    mkdirSync(join(staging,'media'));const file='media/recording'+extension,copied=join(staging,file);
    copyFileSync(source,copied);
    const originalHash=hash(readFileSync(copied));
    const inspect=JSON.parse(await run([join(nativeDirectory,'bonfyre-media-prep'),'inspect',copied]));
    const duration=Number(inspect.format?.duration)*1000;
    if(!Number.isFinite(duration)||duration<=0||duration>600000||!inspect.streams?.some(s=>s.codec_type==='audio'))throw Error('Use a recording with audio, no longer than ten minutes.');
    const transcription=join(staging,'.transcription');
    await run([join(nativeDirectory,'bonfyre-transcribe'),copied,transcription,'--model','base','--language','en','--no-vocab']);
    if(hash(readFileSync(copied))!==originalHash)throw Error('The recording changed during transcription.');
    const raw=readFileSync(join(transcription,'transcript.json')), transcript=JSON.parse(raw);
    const meta=JSON.parse(readFileSync(join(transcription,'meta.json')));
    if(transcript.sourceSystem!=='BonfyreTranscribe'||!Array.isArray(transcript.segments)||!transcript.segments.length||
      !Number.isFinite(meta.audio_duration_s)||Math.abs(meta.audio_duration_s*1000-duration)>250)
      throw Error('The transcription does not match the recording duration.');
    const segments=transcriptPassages(transcript.segments,duration);
    const media={file,sha256:originalHash,content_type:types[extension],duration_ms:duration,
      transcript_sha256:hash(raw),review:'unreviewed',segments:segments.map(({start_ms,end_ms})=>({start_ms,end_ms})),
      ...(sourceUrl?{source_url:sourceUrl}:{}),...(credit?{credit}:{})};
    checkRecording(staging,media,segments.map(s=>s.text));
    const collection={schema:'service-site.documents.v1',title,synthetic:false,
      documents:[{id:'recording',title,text:segments.map(s=>s.text).join('\n\n'),media}]};
    if(Buffer.byteLength(JSON.stringify(collection))>1000000||collection.documents[0].text.length>100000)throw Error('The transcript is too large for this workspace.');
    writeFileSync(join(staging,'documents.json'),JSON.stringify(collection,null,2)+'\n',{mode:0o600});
    const receipt={schema:'bonfyre.recording-import.v1',created_at:new Date().toISOString(),recording_sha256:originalHash,
      transcript_sha256:media.transcript_sha256,segments:segments.length,recognized_segments:transcript.segments.length,duration_ms:duration,review:'unreviewed',
      language:'en',model:meta.model,source_system:'BonfyreTranscribe',text_accuracy_verified:false};
    writeFileSync(join(staging,'recording-import.json'),JSON.stringify(receipt,null,2)+'\n',{mode:0o600});
    if(existsSync(destination))throw Error('The output folder appeared during import; it was not overwritten.');
    renameSync(staging,destination);
    return {...receipt,collection:destination};
  } finally {if(existsSync(staging))rmSync(staging,{recursive:true,force:true});}
}

