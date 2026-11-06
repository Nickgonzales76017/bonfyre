import {test,expect} from 'bun:test';
import {reconcileMeeting} from '../tools/civic_sync.mjs';
import {mkdtempSync,writeFileSync,rmSync,existsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';

const pair={collection:'source-correction-fixture',
  agenda:{source:'dallas-council-20261015-legistar',url:'https://cityofdallas.legistar.com/MeetingDetail.aspx?ID=123'},
  video:{source:'dallas-council-20261015-swagit',url:'https://dallastx.new.swagit.com/videos/123'}};
const agenda=[{file:'26-2200A',agenda_number:'1',title:'Allocation one'},{file:'26-2201A',agenda_number:'2',title:'Allocation two'}];
const chapters=agenda.map((a,i)=>({file:a.file,title:a.file+' official chapter',start_seconds:100+i*100,end_seconds:200+i*100,href:'https://dallastx.new.swagit.com/play/123/'+(100+i*100)}));
const meta={observedAt:'2026-10-15T12:00:00Z',agendaDigest:'a'.repeat(64),videoDigest:'b'.repeat(64)};
const snapshot=()=>reconcileMeeting(pair,agenda,chapters,[],meta).new_rows;
const after={...meta,observedAt:'2026-10-15T13:00:00Z',agendaDigest:'c'.repeat(64),videoDigest:'d'.repeat(64)};

test('revalidation and unrelated source byte changes do not invalidate unchanged records',()=>{
  const rows=snapshot(),result=reconcileMeeting(pair,agenda,chapters,rows,after);
  expect(result).toMatchObject({unchanged:4,changed:0,incomplete:0,missing:0});
  expect(result.new_rows).toEqual([]);expect(result.review).toEqual([]);
  expect(rows).toEqual(snapshot());
});
test('a corrected agenda title emits one source-scoped field delta, not false reuse',()=>{
  const rows=snapshot(),input=[{...agenda[0],title:'Corrected allocation'},agenda[1]];
  const result=reconcileMeeting(pair,input,chapters,rows,after);
  expect(result).toMatchObject({unchanged:3,changed:1});expect(result.new_rows).toEqual([]);
  expect(result.review).toHaveLength(1);
  expect(result.review[0]).toMatchObject({reason:'agenda_metadata_changed',file:'26-2200A',source_ref:pair.agenda.source,
    previous_receipt_ref:'foreign:sha256:'+meta.agendaDigest,observed_receipt_ref:'foreign:sha256:'+after.agendaDigest,
    changed_fields:['title'],expected_fields:{title:'26-2200A Allocation one'},proposed_fields:{title:'26-2200A Corrected allocation'},automatic_update:false});
  expect(rows).toEqual(snapshot());
});
test('agenda order changes compare the source summary without changing file identity',()=>{
  const result=reconcileMeeting(pair,[{...agenda[0],agenda_number:'9'},agenda[1]],chapters,snapshot(),after);
  expect(result.changed).toBe(1);expect(result.review[0].changed_fields).toEqual(['summary']);
  expect(result.review[0].occurrence_id).toBe('dallas-civic-20261015-26-2200A');
});
test('chapter title and end-time corrections are detected even when the player URL is unchanged',()=>{
  const changed=[{...chapters[0],title:'Corrected chapter',end_seconds:190},chapters[1]];
  const result=reconcileMeeting(pair,agenda,changed,snapshot(),after);
  expect(result).toMatchObject({unchanged:3,changed:1});
  expect(result.review[0]).toMatchObject({reason:'video_metadata_changed',changed_fields:['title','summary'],source_ref:pair.video.source});
  expect(result.review[0].proposed_fields.canonical_href).toBeUndefined();
});
test('a changed chapter start retains the existing time-change review contract',()=>{
  const changed=[{...chapters[0],href:'https://dallastx.new.swagit.com/play/123/110',start_seconds:110},chapters[1]];
  const result=reconcileMeeting(pair,agenda,changed,snapshot(),after);
  expect(result.review[0]).toMatchObject({reason:'video_index_time_changed',previous_href:chapters[0].href,current_href:changed[0].href,automatic_update:false});
});
test('missing agenda and chapter records are both surfaced without inferring deletion authority',()=>{
  const rows=snapshot(),result=reconcileMeeting(pair,[agenda[1]],[chapters[1]],rows,after);
  expect(result).toMatchObject({unchanged:2,missing:2,changed:0});expect(result.new_rows).toEqual([]);
  expect(result.review.map(r=>r.reason)).toEqual(['previous_agenda_no_longer_advertised','previous_video_index_no_longer_advertised']);
  expect(result.review.every(r=>r.automatic_delete===false&&r.requires_review===true)).toBe(true);
  expect(rows).toEqual(snapshot());
});
test('an empty parsed source is reviewable missing coverage, not an automatic wipe',()=>{
  const result=reconcileMeeting(pair,[],[],snapshot(),after);
  expect(result).toMatchObject({unchanged:0,missing:4});expect(result.new_rows).toEqual([]);
  expect(result.review.every(r=>r.automatic_delete===false)).toBe(true);
});
test('matching file numbers from other sources neither satisfy nor invalidate this pair',()=>{
  const unrelated=snapshot().map(r=>({...r,source_ref:'another-council'}));
  const result=reconcileMeeting(pair,agenda,chapters,unrelated,after);
  expect(result).toMatchObject({unchanged:0,changed:0,missing:0});expect(result.new_rows).toHaveLength(4);expect(result.review).toEqual([]);
});
test('identity-only legacy fixtures cannot certify metadata equality',()=>{
  const partial=snapshot().map(({occurrence_id,source_ref,canonical_href})=>({occurrence_id,source_ref,canonical_href}));
  const result=reconcileMeeting(pair,agenda,chapters,partial,after);
  expect(result).toMatchObject({unchanged:0,incomplete:4,changed:0});expect(result.new_rows).toEqual([]);
  expect(result.review.every(r=>r.unverified_fields.includes('title')&&r.automatic_update===false)).toBe(true);
});
test('local editorial and rights differences stay reviewable and preserve unrelated fields',()=>{
  const rows=snapshot();rows[0]={...rows[0],visibility:'private',status:'draft',reviewer_note:'Keep my decision'};
  const result=reconcileMeeting(pair,agenda,chapters,rows,after);
  expect(result).toMatchObject({unchanged:3,changed:1});expect(result.new_rows).toEqual([]);
  expect(result.review[0].expected_fields).toEqual({visibility:'private',status:'draft'});
  expect(rows[0]).toMatchObject({visibility:'private',status:'draft',reviewer_note:'Keep my decision'});
});
test('a reviewed corrected projection converges without creating another occurrence',()=>{
  const rows=snapshot(),corrected=[{...agenda[0],title:'Corrected allocation'},agenda[1]];
  const first=reconcileMeeting(pair,corrected,chapters,rows,after),delta=first.review[0];
  const reviewed=rows.map(r=>r.occurrence_id===delta.occurrence_id?{...r,...delta.proposed_fields,receipt_ref:delta.observed_receipt_ref}:r);
  const again=reconcileMeeting(pair,corrected,chapters,reviewed,after);
  expect(again).toMatchObject({unchanged:4,changed:0,missing:0});expect(again.new_rows).toEqual([]);expect(again.review).toEqual([]);
});
test('real native CMS import and readback preserve the metadata used for delta comparison',()=>{
  const root=resolve(import.meta.dir,'../../..'),cms=join(root,'cmd/BonfyreCMS/bonfyre-cms');
  expect(existsSync(cms)).toBe(true);
  const dir=mkdtempSync(join(tmpdir(),'civic-native-correction-')),db=join(dir,'cms.db'),input=join(dir,'rows.jsonl');
  const run=args=>{const r=Bun.spawnSync([cms,...args,'--db',db,'--schemas',join(root,'cmd/BonfyreCMS/content-types')],{cwd:root,stdout:'pipe',stderr:'pipe',timeout:20000});if(r.exitCode!==0)throw Error(r.stderr.toString()||r.stdout.toString());return r.stdout.toString();};
  try{
    run(['schema','apply',join(root,'cmd/BonfyreCMS/content-types/publication_occurrence.json')]);
    writeFileSync(input,snapshot().map(r=>JSON.stringify(r)).join('\n')+'\n');
    const imported=JSON.parse(run(['entry','import-jsonl','publication_occurrence',input,'--ns','civic-test']));expect(imported.imported).toBe(4);
    const read=new Database(db,{readonly:true});let rows;try{rows=read.query('SELECT * FROM publication_occurrence WHERE namespace=?').all('civic-test');}finally{read.close();}
    expect(reconcileMeeting(pair,agenda,chapters,rows,after).unchanged).toBe(4);
    const corrected=[{...agenda[0],title:'Updated after import'},agenda[1]],result=reconcileMeeting(pair,corrected,chapters,rows,after);
    expect(result).toMatchObject({unchanged:3,changed:1});expect(result.new_rows).toEqual([]);
    expect(result.review[0].expected_fields.title).toBe('26-2200A Allocation one');
  }finally{rmSync(dir,{recursive:true,force:true});}
});
