import {test,expect} from 'bun:test';
import {cataloguePage,visibleSourceCatalogueRows} from '../source-desk.mjs';
const twin=(source,url)=>({subject_id:'foreign_twin:long-shared-prefix',url,release:{source_contract:{source,family:'archive'},stages:{archive:{items:2}}}});
const packet=twins=>({schema:'bonfyre.foreign.twins.v2',state:'current',catalogue_generation:'foreign-v1-a',total:twins.length,next_offset:null,twins});
test('distinct source contracts sharing a truncated foreign twin remain separately inspectable',()=>{
 const rows=cataloguePage(packet([twin('archive-films','https://archive.org/advancedsearch?q=films'),twin('archive-music','https://archive.org/advancedsearch?q=music')])).rows;
 expect(rows.map(r=>r.id)).toEqual(['archive-films','archive-music']);expect(rows[0].twin.subject_id).toBe(rows[1].twin.subject_id);expect(rows[0].twin.url).not.toBe(rows[1].twin.url);
});
test('duplicate source contracts cannot overwrite the earlier source row',()=>{expect(()=>cataloguePage(packet([twin('a','https://example.org/a'),twin('a','https://example.org/b')]))).toThrow('repeated');});
test('native snapshot mismatch preserves an explicit reload boundary',()=>{expect(()=>cataloguePage({...packet([]),state:'snapshot_mismatch'})).toThrow('changed');});
test('Stitch source catalogue applies real family/retention/recent/order cuts without changing identities',()=>{
 const first=twin('archive-old','https://archive.org/first');first.release.stages.archive.items=12;first.release.source_contract.family='archive';first.release.source_contract.last_seen='2024-01-01';
 const second=twin('archive-new','https://archive.org/second');second.release.stages.archive.items=1;second.release.source_contract.family='archive';second.release.source_contract.last_seen='2026-01-01';
 const third=twin('journal-one','https://example.org/journal');third.release.stages.archive.items=4;third.release.source_contract.family='journal';third.release.source_contract.last_seen='2025-01-01';
 const original=cataloguePage(packet([first,second,third])).rows;
 expect(visibleSourceCatalogueRows(original).map(x=>x.id)).toEqual(['archive-old','journal-one','archive-new']);
 expect(visibleSourceCatalogueRows(original,{family:'archive',order:'recent'}).map(x=>x.id)).toEqual(['archive-new','archive-old']);
 expect(visibleSourceCatalogueRows(original,{order:'alphabetical'}).map(x=>x.title)).toEqual(['archive.org · /first','archive.org · /second','example.org · /journal']);
 expect(visibleSourceCatalogueRows(original,{family:'unknown'})).toEqual([]);
 expect(original.map(x=>x.id)).toEqual(['archive-old','archive-new','journal-one']);
 expect(()=>visibleSourceCatalogueRows(original,{order:'machine-grants'})).toThrow('Unsupported');
});
