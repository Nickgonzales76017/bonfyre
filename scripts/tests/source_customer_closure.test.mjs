import {test} from 'node:test';
import assert from 'node:assert/strict';
import {analyze,attributedTitles,canonical,parseIcs,PUBLIC} from '../source_customer_closure.mjs';
const ics=[
 'BEGIN:VCALENDAR','VERSION:2.0','BEGIN:VEVENT',
 'UID:one@example.com','SUMMARY:Rain\\, Wind & Tea','END:VEVENT',
 'BEGIN:VEVENT','UID:two@example.com','SUMMARY:Pottery and ',' Friends',
 'END:VEVENT','END:VCALENDAR',''].join('\n');
const page=rows=>'<html><main><ol>'+rows.join('')+
  '</ol><script type="application/json">{"hidden":"not user-visible"}</script></main></html>';
const li=(name,publisher='wanderatlasguides.com')=>'<li><b>'+name+'</b>Today '+publisher+'</li>';
const pages={
 [PUBLIC.surfaces[0]]:page([li('Rain, Wind &amp; Tea')]),
 [PUBLIC.surfaces[1]]:page([li('Rain, Wind &amp; Tea'),li('Pottery and Friends'),
   li('Other feed','example.org')])
};
test('unfolds iCalendar lines and preserves publisher event UIDs',()=>{
 const events=parseIcs(ics);assert.equal(events.length,2);
 assert.deepEqual(events.map(x=>x.summary),['rain, wind & tea','pottery and friends']);
 assert.equal(new Set(events.map(x=>x.uid)).size,2);
});
test('attributes visible rows and excludes unrelated publishers and scripts',()=>{
 assert.deepEqual(attributedTitles(pages[PUBLIC.surfaces[1]]),
   ['rain, wind & tea','pottery and friends']);
 assert.equal(canonical('Rain\\, Wind &amp; Tea'),'rain, wind & tea');
});
test('measures shared title evidence without invented resource or financial savings',()=>{
 const report=analyze(ics,pages);
 assert.equal(report.verdict,'source_titles_observed_on_customer_surfaces');
 assert.deepEqual(report.consumers.map(x=>x.source_matched_titles),[1,2]);
 assert.equal(report.overlap.exact_titles_visible_on_both_surfaces,1);
 assert.equal(report.source.vevents,2);
 assert.equal(report.observations.public_get_invocations,3);
 assert.equal(report.observations.avoided_fetches,null);
 assert.equal(report.observations.provider_cost_usd,null);
 assert.equal(report.overlap.physical_reuse_proven,false);
});
test('outdated source material is reported as unmatched rather than silently accepted',()=>{
 const changed={...pages,[PUBLIC.surfaces[0]]:page([li('Absent from source')])};
 const report=analyze(ics,changed);
 assert.equal(report.verdict,'insufficient_current_source_overlap');
 assert.deepEqual(report.consumers[0].not_in_current_feed,['absent from source']);
});
test('rejects malformed feeds and hidden content',()=>{
 assert.throws(()=>parseIcs('junk'),/complete iCalendar/);
 assert.throws(()=>parseIcs('BEGIN:VCALENDAR\nBEGIN:VEVENT\nSUMMARY:A\nEND:VCALENDAR'),
   /unterminated VEVENT/);
 assert.throws(()=>attributedTitles('<script>hidden</script>'),/lacks main content/);
});
test('featured event articles are rendered content, not invisible scene metadata',()=>{
 const markup='<main><article><h2>Rain, Wind &amp; Tea</h2><p>wanderatlasguides.com</p></article></main>';
 assert.deepEqual(attributedTitles(markup),['rain, wind & tea']);
});
