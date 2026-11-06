import { test, expect } from 'bun:test';
import { discoverCivicPairs, parseSwagitIndex, reconcileMeeting, verifyNativeObservation } from '../tools/civic_sync.mjs';
const agendaUrl='https://cityofdallas.legistar.com/MeetingDetail.aspx?GUID=C4FABFDE-D9D8-493D-A742-4D392E055CA8&ID=1362423&Search=';
const videoUrl='https://dallastx.new.swagit.com/videos/402001';
const pair={collection:'civic-dallas-council-20260923',stamp:'20260923',agenda:{source:'dallas-council-20260923-legistar',url:agendaUrl, family:'civic-meeting'},video:{source:'dallas-council-20260923-swagit',url:videoUrl,family:'civic-meeting-video'}};
const file=i=>`26-${String(2200+i).padStart(4,'0')}A`;
const agenda=Array.from({length:70},(_,i)=>({file:file(i),agenda_number:String(i+1),title:`Item ${i+1}, ${file(i)} Budget $120,000 and address 321 Elm Street` }));
const tags=Array.from({length:26},(_,i)=>{const f=i<22?file(i):null;return `<a class="btn playerControl label" data-title="${f?'Agenda '+i+' '+f:'Introduction '+i}" data-ts="${44+i*173}" data-end-ts="${44+(i+1)*173}" href="/play/402001/${44+i*173}">Watch</a>`;});
// Real CMS reads retain source-owned metadata; an identity-only fixture cannot prove it unchanged.
const existing=[...agenda.map(row=>({occurrence_id:'dallas-civic-20260923-'+row.file,
 source_twin:pair.agenda.source,source_ref:pair.agenda.source,canonical_href:agendaUrl,
 title:row.file+' '+row.title,summary:'Official agenda item '+row.agenda_number+'; item action and votes separately unverified.',
 receipt_ref:'foreign:sha256:'+'a'.repeat(64),rights_basis:'official_public_agenda_reference_only',
 retention:'metadata_only',visibility:'public',assertion_state:'official_agenda_item_observed_action_unverified',
 cannot_infer:'Agenda item not a vote, grant award, parcel, or cleared media license.',status:'published'})),
 ...Array.from({length:21},(_,i)=>({occurrence_id:'dallas-civic-video-20260923-'+file(i),source_ref:pair.video.source,
 source_twin:pair.video.source,canonical_href:`https://dallastx.new.swagit.com/play/402001/${44+i*173}`,
 title:'Swagit index · '+file(i)+' · Agenda '+i+' '+file(i),
 summary:'Official index chapter '+file(i)+'; start '+(44+i*173)+'s, end '+(44+(i+1)*173)+'s; meeting '+videoUrl+'; source agenda '+agendaUrl+'. Chapter is not statement-level verification.',
 receipt_ref:'foreign:sha256:'+'b'.repeat(64),rights_basis:'swagit_public_player_reference_only_no_download_or_derivative_license',retention:'metadata_only',visibility:'public',assertion_state:'official_media_index_observed_transcript_not_verified',
 cannot_infer:'Video index start is not an approved action, transcript, media ownership or clip license.',status:'published'}))];
const meta={observedAt:'2026-10-14T17:00:00Z',agendaDigest:'a'.repeat(64),videoDigest:'b'.repeat(64)};
test('published CMS SourceCollection discovers exact meeting pair without per-item specification',()=>{
 const collections=[{collection_id:pair.collection,source_selector:JSON.stringify({source_refs:[pair.agenda.source,pair.video.source]})}, {collection_id:'unrelated',source_selector:JSON.stringify({source_refs:['other']})}];
 const result=discoverCivicPairs(collections,[pair.agenda,pair.video],{limit:12});
 expect(result.pairs).toHaveLength(1);expect(result.held).toEqual([]);
 expect(result.pairs[0]).toMatchObject({event_id:1362423,video_id:'402001',stamp:'20260923'});
 expect(discoverCivicPairs(collections,[pair.agenda],{limit:12}).held).toHaveLength(1);
 expect(()=>discoverCivicPairs(collections,[pair.agenda,pair.video],{limit:1000})).toThrow();
});
test('26 Swagit chapters, 22 official file numbers, one exact new row from 70 agenda + 21 video',()=>{
 const parsed=parseSwagitIndex(tags.join(''),'402001');
 expect(parsed.conflicts).toEqual([]);expect(parsed.chapters).toHaveLength(26);
 expect(parsed.chapters.filter(x=>x.identity_state==='exact_file_number')).toHaveLength(22);
 const result=reconcileMeeting(pair,agenda,parsed.chapters,existing,meta);
 expect(result.agenda_items).toBe(70);expect(result.index_entries).toBe(26);
 expect(result.file_index_entries).toBe(22);expect(result.exact_joins).toBe(22);
 expect(result.unindexed_chapters).toHaveLength(4);expect(result.unchanged).toBe(91);
 expect(result.new_rows).toHaveLength(1);
 expect(result.new_rows[0]).toMatchObject({
  occurrence_id:'dallas-civic-video-20260923-'+file(21),source_ref:pair.video.source,
  rights_basis:'swagit_public_player_reference_only_no_download_or_derivative_license',
  retention:'metadata_only',assertion_state:'official_media_index_observed_transcript_not_verified',
  canonical_href:`https://dallastx.new.swagit.com/play/402001/${44+21*173}`
 });
 expect(result.review).toEqual([]);
 const replay=reconcileMeeting(pair,agenda,parsed.chapters,[...existing,...result.new_rows],meta);
 expect(replay.new_rows).toHaveLength(0);expect(replay.unchanged).toBe(92);
 console.log(JSON.stringify({schema:'bonfyre.civic.synthetic-batch.v1',agenda:70,chapters:26,exact_joins:22,
   already_persisted:91,creates:1,first_run_native_cms_batches:1,second_run_creates:0,synthetic:true}));
});
test('ambiguous agenda indices, duplicate file and drift are held instead of overwritten',()=>{
 const parsed=parseSwagitIndex([...tags,`<a class="playerControl" data-ts="44" data-title="DIFFERENT ${file(0)}" href="/play/402001/44">X</a>`].join(''),'402001');
 expect(parsed.conflicts[0].state).toBe('conflicting_chapter_start');
 const dup=parseSwagitIndex(`<a class="playerControl" data-ts="23" data-title="${file(1)} ${file(2)}" href="/play/402001/23">X</a>`,'402001');
 expect(dup.chapters[0].identity_state).toBe('ambiguous_file_numbers');
 const timeline=parseSwagitIndex(tags.join(''),'402001').chapters;
 const drift=[...existing];drift[70]={...drift[70],canonical_href:'https://dallastx.new.swagit.com/play/402001/98765'};
 const result=reconcileMeeting(pair,agenda,timeline,drift,meta);
 expect(result.new_rows).toHaveLength(1);
 expect(result.review.some(x=>x.reason==='video_index_time_changed'&&x.file===file(0))).toBe(true);
 const extra=[...agenda,{...agenda[0],agenda_number:'999'}];
 const a=reconcileMeeting(pair,extra,timeline,existing,meta);
 expect(a.review.some(x=>x.reason==='duplicate_agenda_file')).toBe(true);
});
test('bad Swagit playback destination is refused even when a file number matches',()=>{
 const html=`<a class="playerControl" data-ts="2838" data-title="${file(0)}" href="https://evil.example/play/402001/2838">Wrong</a>`;
 const x=parseSwagitIndex(html,'402001');expect(x.chapters).toHaveLength(0);
 expect(x.conflicts.map(y=>y.state)).toEqual(['invalid_official_chapter_link']);
 expect(()=>parseSwagitIndex(html,'../../etc')).toThrow();
});
test('ResourceWeather selection reaches late source collections and coalesces repeated ownership',()=>{
  const sources=[],collections=[];
  for(let i=0;i<150;i++){
    const name='dallas-council-20260923-source-'+i;
    const a={source:name+'-legistar',family:'civic-meeting',
      url:'https://cityofdallas.legistar.com/MeetingDetail.aspx?ID='+(1362400+i)};
    const v={source:name+'-swagit',family:'civic-meeting-video',
      url:'https://dallastx.new.swagit.com/videos/'+(402000+i)};
    sources.push(a,v);
    collections.push({collection_id:'meeting-'+i,source_selector:JSON.stringify({source_refs:[a.source,v.source]})});
  }
  collections.push({collection_id:'repeated-collection',source_selector:collections[149].source_selector});
  const onlyLast=discoverCivicPairs(collections,sources,{limit:8,dueSources:new Set([sources[298].source])});
  expect(onlyLast.considered).toBe(151);
  expect(onlyLast.pairs).toHaveLength(1);
  expect(onlyLast.pairs[0].collection).toBe('meeting-149');
  expect(onlyLast.pairs[0].collections).toEqual(['meeting-149','repeated-collection']);
  expect(onlyLast.deferred).toBe(0);
  const budget=discoverCivicPairs(collections,sources,{limit:8});
  expect(budget.pairs).toHaveLength(8);
  expect(budget.deferred).toBe(142);
  expect(budget.held.filter(r=>r.reason==='budget_deferred').length).toBeLessThanOrEqual(128);
  expect(budget.considered).toBe(151);
  console.log(JSON.stringify({
    schema:'bonfyre.civic.source-selection.v1',source_collections:151,
    native_selected_source_position:150,unique_pairs_fetched:1,
    due_pair_found:true,duplicate_collection_fetches_avoided:1,
    budget_deferred_pairs:142
  }));
});
test('native observation revalidation preserves provider freshness without inventing new media bytes',()=>{
  const source={source:'dallas-council-20260923-swagit',twin:'dallas-council-20260923-swagit',
    url:'https://dallastx.new.swagit.com/videos/402001'};
  const digest='f'.repeat(64);
  const base={schema:'bonfyre-foreign-observation.v1',twin_id:source.twin,
    source_url:source.url,observe_right:'public',policy_decision:'allow',
    observed_at:'2026-10-14T17:00:00Z',content_sha256:digest};
  const fresh=verifyNativeObservation({...base,observation_kind:'source_observed',http_status:200},0,source,digest);
  expect(fresh.revalidated).toBe(false);
  const unchanged=verifyNativeObservation({...base,observation_kind:'repeat_observed',
    http_status:304,conditional:true,unchanged_from_content_sha256:digest},3,source,digest);
  expect(unchanged.revalidated).toBe(true);
  expect(()=>verifyNativeObservation({...base,policy_decision:'robots_disallow',observation_kind:'source_observed',http_status:200},0,source,digest)).toThrow();
  expect(()=>verifyNativeObservation({...base,observation_kind:'repeat_observed',
    http_status:304,conditional:false,unchanged_from_content_sha256:digest},3,source,digest)).toThrow();
  expect(()=>verifyNativeObservation({...base,observation_kind:'source_observed',http_status:200},0,source,'e'.repeat(64))).toThrow();
  expect(()=>verifyNativeObservation({...base,observation_kind:'source_observed',http_status:200,
    source_url:'https://other.example/'},0,source,digest)).toThrow();
});
