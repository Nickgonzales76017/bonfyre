import {test,expect} from 'bun:test';
import {collectWarehousePortfolio,warehousePortfolioPage,
 warehouseInstalledTools,warehouseContinuations} from '../warehouse-portfolio.mjs';
const dig=c=>c.repeat(64);
const binding=n=>'ws-'+n.repeat(40);
const rel=n=>n.repeat(64);
const page=(rows,{total=rows.length,offset=0,limit=100,next_offset=null,state='READ_ONLY'}={})=>
 ({schema:'bonfyre.warehouse.portfolio.v1',state,total,offset,limit,rows,
   next_offset,read_only:true,execution_authority:false,
   physical_bytes_verified:false,tenant_access_verified:false});
const pinned=(kind='news',id='id')=>({semantic_kind:kind,state:'PINNED_METADATA',
 representation_kind:'parquet',generation:1,representation_id:id,digest:dig('a'),
 realization_count:1,max_realization_bytes:44});
const installed=[
 [binding('a'),'experience.source-library.workspace','sources/',rel('b'),'Source library',true],
 [binding('b'),'experience.connected-instruments.workspace','instruments/',rel('c'),'Research',true],
 [binding('c'),'experience.skill-instruments.workspace','instruments/',rel('d'),'Analysis',true],
 [binding('d'),'experience.finance-workbooks.workspace','finance/',rel('e'),'Finance',false],
 [binding('e'),'experience.connection-studio.workspace','connections/',rel('f'),'Connections',true]
];
test('one current native portfolio yields a safe pinned immutable candidate',()=>{
 const row=pinned(),value=warehousePortfolioPage(page([row],{limit:1}));
 expect(value).toMatchObject({total:1,rows:[{semanticKind:'news',state:'pinned',
  generation:1,digest:dig('a'),observedBytes:44}]});
 expect(Object.isFrozen(value.rows[0])).toBe(true);
});
test('stale and uninstalled portfolios never fabricate usable data products',()=>{
 const unavailable=warehousePortfolioPage(page([{semantic_kind:'news',state:'UNAVAILABLE'}],{limit:1}));
 expect(unavailable.rows).toEqual([{semanticKind:'news',state:'unavailable'}]);
 expect(()=>warehouseContinuations(unavailable.rows[0],[],{})).toThrow();
 const missing=page([],{state:'NOT_INSTALLED'});
 delete missing.total;delete missing.offset;delete missing.limit;
 expect(collectWarehousePortfolio([missing])).toMatchObject({state:'not_installed',complete:false});
});
test('paginated products preserve order, detect overlaps, and never assert cross-page snapshot',()=>{
 const first=page([pinned('a'),pinned('b')],{total:3,limit:2,next_offset:2});
 const second=page([pinned('c')],{total:3,offset:2,limit:2});
 const value=collectWarehousePortfolio([first,second]);
 expect(value).toMatchObject({total:3,complete:true,crossPageSnapshotVerified:false,
   tenantAccessVerified:false,physicalBytesVerified:false});
 expect(value.rows.map(x=>x.semanticKind)).toEqual(['a','b','c']);
 expect(()=>collectWarehousePortfolio([first,page([pinned('b')],{total:3,offset:2,limit:2})]))
   .toThrow();
 expect(()=>collectWarehousePortfolio([first,page([pinned('c')],{total:4,offset:2,limit:2})]))
   .toThrow();
});
test('rejects unexpected fields, SQL, credentials, invalid digest and cursor',()=>{
 for(const row of [
   {...pinned(),source_db:'/private'},
   {...pinned(),sql:'DROP TABLE'},
   {...pinned(),digest:'abc'},
   {...pinned(),generation:0},
   {...pinned(),representation_id:'../../etc/passwd'},
   {...pinned(),max_realization_bytes:-1},
   {...pinned(),state:'AUTHORIZED'},
 ])
   expect(()=>warehousePortfolioPage(page([row],{limit:1}))).toThrow();
 expect(()=>warehousePortfolioPage({...page([pinned()],{limit:1}),
   execution_authority:true})).toThrow();
 expect(()=>warehousePortfolioPage(page([pinned()],{limit:1,next_offset:1}))).toThrow();
});
test('uses current installed account apps and never invents a destination or data transfer',()=>{
 const tools=warehouseInstalledTools([1,'user:19','self',installed],'self');
 const selected=warehousePortfolioPage(page([pinned()],{limit:1})).rows[0];
 const routes=warehouseContinuations(selected,tools,{context:'self',roles:[
   'sources','analysis','finance','connections']});
 expect(routes.some(r=>r.pack==='experience.source-library.workspace'&&r.state==='ready')).toBe(true);
 expect(routes.some(r=>r.pack==='experience.skill-instruments.workspace'&&r.state==='ready')).toBe(true);
 expect(routes.some(r=>r.pack==='experience.finance-workbooks.workspace'&&r.state==='unready')).toBe(true);
 expect(routes.some(r=>r.kind==='account'&&r.label==='Connections'&&
   r.href==='/apps/connections?context=self')).toBe(true);
 expect(routes.some(r=>r.pack==='experience.connection-studio.workspace')).toBe(false);
 for(const r of routes){
   expect(r.dataTransferAuthorized).toBe(false);
   expect(r.queryExecuted).toBe(false);
   expect(r.recordCreated).toBe(false);
   expect(r.sourceCoordinate).toMatchObject({generation:1,digest:dig('a')});
 }
 expect(()=>warehouseInstalledTools([1,'institution:other','institution',installed],'self')).toThrow();
 expect(()=>warehouseContinuations(selected,tools,{context:'self',roles:['all-powerful']})).toThrow();
});


test('one pinned data product offers reviewed continuations across all 11 installed Aurekai apps',()=>{
 const apps=[
  ['1','experience.source-library.workspace','sources/'],
  ['2','experience.connected-instruments.workspace','instruments/'],
  ['3','experience.skill-instruments.workspace','instruments/'],
  ['4','experience.finance-workbooks.workspace','finance/'],
  ['5','experience.native-creator-studio.workspace','audio/'],
  ['6','experience.multi-merchant-catalogue.workspace','catalogue/'],
  ['7','experience.retail-procurement.workspace','product-room/'],
  ['8','experience.shipment-archive.workspace','shipments/'],
  ['9','experience.source-programs.workspace','programs/'],
  ['a','experience.source-campaign.workspace','campaigns/'],
  ['b','experience.connection-studio.workspace','connections/']
 ];
 const packet=[1,'user:19','self',apps.map(([n,pack,entry])=>
   [binding(n),pack,entry,rel('f'),pack,true])];
 const installed=warehouseInstalledTools(packet,'self');
 const row=warehousePortfolioPage(page([pinned('civic-2026')],{limit:1})).rows[0];
 const routes=warehouseContinuations(row,installed,{context:'self',
   roles:['sources','research','analysis','finance','connections',
     'programs','campaigns','commerce','creator','logistics']});
 const ready=new Set(routes.filter(r=>r.state==='ready'&&r.kind==='workspace').map(r=>r.pack));
 for(const [n,pack] of apps)if(pack!=='experience.connection-studio.workspace')
   expect(ready.has(pack)).toBe(true);
 expect(routes.find(r=>r.kind==='account'&&r.label==='Connections')?.href)
   .toBe('/apps/connections?context=self');
 expect(routes.every(r=>!r.queryExecuted&&!r.recordCreated&&!r.dataTransferAuthorized))
   .toBe(true);
 expect(new Set(routes.map(r=>r.sourceCoordinate.digest))).toEqual(new Set([dig('a')]));
});
