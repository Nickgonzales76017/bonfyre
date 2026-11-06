import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync, readdirSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { checkRecording, importRecording, transcriptPassages } from '../document-recording.mjs';
import { readCollection, prepareDocuments, createDocumentHandler } from '../document-workspace.mjs';

const sha=b=>createHash('sha256').update(b).digest('hex');
test('recording passages preserve words and span complete sentences without bridging long gaps',()=>{
  const rows=[{t0:0,t1:2000,text:'The image contains'},{t0:2000,t1:4000,text:' infrared light.'},{t0:4000,t1:5000,text:'Another thought'},{t0:8000,t1:9000,text:'after a pause.'}];
  const passages=transcriptPassages(rows);
  expect(passages).toEqual([{text:'The image contains infrared light.',start_ms:0,end_ms:4000},{text:'Another thought',start_ms:4000,end_ms:5000},{text:'after a pause.',start_ms:8000,end_ms:9000}]);
  expect(passages.map(p=>p.text).join(' ')).toBe(rows.map(s=>s.text.trim()).join(' '));
});
function fixture(dir) {
  mkdirSync(join(dir,'media'),{recursive:true});
  const bytes=Buffer.from('bounded recording fixture');
  writeFileSync(join(dir,'media/recording.wav'),bytes);
  const media={file:'media/recording.wav',content_type:'audio/wav',sha256:sha(bytes),transcript_sha256:'a'.repeat(64),duration_ms:2000,review:'unreviewed',segments:[{start_ms:0,end_ms:1000},{start_ms:1000,end_ms:2000}]};
  const doc={id:'recording',title:'Recorded instructions',text:'The garden hose is in the shed.\n\nTurn off the outdoor tap in winter.',media};
  const collection={schema:'service-site.documents.v1',title:'Test collection',documents:[doc]};
  writeFileSync(join(dir,'documents.json'),JSON.stringify(collection));
  return {media,doc,collection,bytes};
}
test('recording identity covers media and times; paths, symlinks and invalid times are rejected',()=>{
  const dir=mkdtempSync(join(tmpdir(),'recording-shape-'));
  try {
    const {media,collection}=fixture(dir),file=join(dir,'documents.json'),before=readCollection(file).documents[0];
    expect(before.passages[1].recording).toEqual({start_ms:1000,end_ms:2000});
    expect(before.passages[0].review).toBe('unreviewed');
    collection.documents[0].media.segments[0].end_ms=900;writeFileSync(file,JSON.stringify(collection));
    expect(readCollection(file).documents[0].digest).not.toBe(before.digest);
    expect(()=>checkRecording(dir,{...media,file:'../elsewhere.wav'},['a','b'])).toThrow();
    expect(()=>checkRecording(dir,{...media,segments:[{start_ms:0,end_ms:3000},{start_ms:1000,end_ms:2000}]},['a','b'])).toThrow('within');
    expect(()=>checkRecording(dir,{...media,review:'verified'},['a','b'])).toThrow('metadata');
    expect(()=>checkRecording(dir,{...media,source_url:'javascript:alert(1)'},['a','b'])).toThrow('HTTP');
    expect(()=>checkRecording(dir,{...media,source_url:'https://user:pass@example.org/'},['a','b'])).toThrow('HTTP');
    expect(checkRecording(dir,{...media,source_url:'https://example.org/video',credit:'Original publisher'},['a','b']).credit).toBe('Original publisher');
    symlinkSync(join(dir,'media/recording.wav'),join(dir,'media/linked.wav'));
    expect(()=>checkRecording(dir,{...media,file:'media/linked.wav'},['a','b'])).toThrow('regular');
    writeFileSync(join(dir,'media/recording.wav'),'changed');
    expect(()=>readCollection(file)).toThrow('recording changed');
  } finally {rmSync(dir,{recursive:true,force:true});}
});
test('local media API requires session and source presence, and changed bytes stop citation use',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'recording-owner-'));
  try {
    const {bytes}=fixture(dir),owner=await prepareDocuments(dir),origin='http://127.0.0.1:4177';
    expect(owner.list().documents[0].duration_ms).toBe(2000);
    const handler=createDocumentHandler(owner,{origin,token:'recording-test'});
    const get=(path,token='recording-test')=>handler(new Request(origin+path,{headers:{'x-document-session':token}}));
    expect((await get('/api/media?id=recording','wrong')).status).toBe(403);
    const response=await get('/api/media?id=recording');
    expect(response.status).toBe(200);expect(Buffer.from(await response.arrayBuffer())).toEqual(bytes);
    const page=await handler(new Request(origin+'/'));
    const cookie=page.headers.get('set-cookie').split(';')[0];
    expect(page.headers.get('set-cookie')).toContain('HttpOnly; SameSite=Strict; Path=/api/media');
    const part=await handler(new Request(origin+'/api/media?id=recording',{headers:{cookie,range:'bytes=1-3'}}));
    expect(part.status).toBe(206);expect(part.headers.get('content-range')).toBe(`bytes 1-3/${bytes.length}`);
    expect(Buffer.from(await part.arrayBuffer())).toEqual(bytes.subarray(1,4));
    const suffix=await handler(new Request(origin+'/api/media?id=recording',{headers:{cookie,range:'bytes=-3'}}));
    expect(Buffer.from(await suffix.arrayBuffer())).toEqual(bytes.subarray(-3));
    const invalid=await handler(new Request(origin+'/api/media?id=recording',{headers:{cookie,range:'bytes=99999-'}}));
    expect(invalid.status).toBe(416);expect(invalid.headers.get('content-range')).toBe(`bytes */${bytes.length}`);
    expect((await handler(new Request(origin+'/api/source?id=recording',{headers:{cookie}}))).status).toBe(403);
    expect((await handler(new Request(origin+'/api/media?id=recording',{headers:{cookie,origin:'https://elsewhere.test'}}))).status).toBe(403);
    const result=await owner.search('winter','phrase');expect(result.results[0].recording.start_ms).toBe(1000);
    const source=owner.source('recording');
    writeFileSync(join(dir,'media/recording.wav'),'changed');
    expect(()=>owner.source('recording')).toThrow('recording changed');
    expect((await get('/api/media?id=recording')).status).toBe(400);
    writeFileSync(join(dir,'media/recording.wav'),bytes);
    await owner.remove('recording',source.digest);
    expect((await get('/api/media?id=recording')).status).toBe(400);
    expect(()=>owner.source('recording')).toThrow('no longer');
    expect((await prepareDocuments(dir)).list().documents[0].indexed).toBe(false);
    expect(readFileSync(join(dir,'media/recording.wav'))).toEqual(bytes);
  } finally {rmSync(dir,{recursive:true,force:true});}
},60000);
test('a failed or mismatched native import leaves no partial collection and preserves input',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'recording-failure-'));
  try {
    const input=join(dir,'sample.mp4');writeFileSync(input,'original');
    await expect(importRecording(input,join(dir,'new'),{run:async()=>{throw Error('unavailable');}})).rejects.toThrow('unavailable');
    expect(existsSync(join(dir,'new'))).toBe(false);expect(readdirSync(dir)).toEqual(['sample.mp4']);
    const run=async args=>{
      if(args[1]==='inspect')return JSON.stringify({format:{duration:'2'},streams:[{codec_type:'audio'}]});
      mkdirSync(args[2]);writeFileSync(join(args[2],'meta.json'),JSON.stringify({audio_duration_s:2,model:'base'}));
      writeFileSync(join(args[2],'transcript.json'),JSON.stringify({sourceSystem:'BonfyreTranscribe',segments:[{t0:0,t1:3000,text:'outside recording'}]}));return '';
    };
    await expect(importRecording(input,join(dir,'new'),{run})).rejects.toThrow('within');
    expect(existsSync(join(dir,'new'))).toBe(false);expect(readFileSync(input,'utf8')).toBe('original');
    mkdirSync(join(dir,'existing'));await expect(importRecording(input,join(dir,'existing'),{run})).rejects.toThrow('never overwritten');
  } finally {rmSync(dir,{recursive:true,force:true});}
});

