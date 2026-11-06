import {test,expect} from 'bun:test';
import {programServiceContinuations,programToolChoices,programServicePacks as P} from '../program-service-continuations.mjs';
import {programServicePolicy} from '../program-service-policy.mjs';

const tool=(pack,label,id)=>({id:'ws-'+id.repeat(40).slice(0,40),pack,entry:'editor/',release:'a'.repeat(64),label,ready:true,href:'/apps/ws-'+id.repeat(40).slice(0,40)+'/editor/'});

test('generated policy is the runtime source and keeps every ProgramFabric lowering addressable',()=>{
 expect(programServicePolicy.packs).toEqual(P);
 for(const role of ['notices','newsroom','sources','evidence','board','work','case','community'])expect(programServicePolicy.continuations[role]?.length).toBeGreaterThan(0);
 expect(programServicePolicy.aliases.provider).toBe('connections');
 expect(Object.keys(programServicePolicy.grammar_basis)).toEqual(['artifact','collection','commerce','community','correspondence','knowledge','media','reservation','work','workspace']);
 expect(programServicePolicy.grammar_basis.media).toEqual({continuation:'media',owner_families:['CreatorChannel','MediaLibrary','RelationshipGraph','Stage'],state:'typed'});
});

test('one program can continue laterally into workspace, surface and account owners without collapsing them',()=>{
 const tools=[tool(P.sourceLibrary,'Source library','1'),tool(P.catalogue,'Catalogue','2')];
 const rows=programServiceContinuations(['sources','community','connections','commerce','mentoring'],tools,{context:'institution:studio'});
 expect(rows.find(row=>row.pack===P.sourceLibrary)).toMatchObject({kind:'workspace',state:'ready',label:'Source library'});
 expect(rows.find(row=>row.pack===P.catalogue)).toMatchObject({kind:'workspace',state:'ready',label:'Catalogue'});
 expect(rows.find(row=>row.pack===P.procurement)).toMatchObject({kind:'workspace',state:'not_installed'});
 expect(rows.find(row=>row.kind==='surface'&&row.id==='community')).toMatchObject({href:'/community',state:'available'});
 expect(rows.find(row=>row.kind==='surface'&&row.id==='listings')).toMatchObject({href:'/listings',state:'available'});
 expect(rows.find(row=>row.kind==='account'&&row.id==='connections')).toMatchObject({href:'/apps/connections?context=institution%3Astudio',state:'available'});
 expect(rows.find(row=>row.kind==='unresolved')).toMatchObject({label:'mentoring',state:'unresolved',href:null});
 expect(rows.every(row=>typeof row.boundary==='string'&&row.boundary.length>0)).toBe(true);
});

test('architecture-known grammar families stay visible when their exact Program owner is still pending',()=>{
 const rows=programServiceContinuations(['correspondence','knowledge','artifact','reservation','mentoring'],[]);
 for(const [role,owner] of [['correspondence','Correspondence'],['knowledge','KnowledgeGraph'],['artifact','ArtifactGraph'],['reservation','ProtocolContract']]){
  expect(rows.find(row=>row.roles.includes(role))).toMatchObject({kind:'pending',state:'pending_exact_owner',href:null,owner_families:[owner]});
 }
 expect(rows.find(row=>row.roles.includes('mentoring'))).toMatchObject({kind:'unresolved',state:'unresolved'});
});

test('native grammar applications with typed continuations reuse the declared Program service family',()=>{
 const rows=programServiceContinuations(['workspace','collection'],[]);
 expect(rows.some(row=>row.pack===P.instruments)).toBe(true);
 expect(rows.some(row=>row.kind==='surface'&&row.id==='board')).toBe(true);
});

test('shared destinations dedupe across semantic roles while retaining why they were offered',()=>{
 const source=tool(P.sourceLibrary,'Source library','3');
 const rows=programServiceContinuations(['sources','research','evidence'],[source]);
 const retained=rows.find(row=>row.pack===P.sourceLibrary);
 expect(rows.filter(row=>row.pack===P.sourceLibrary)).toHaveLength(1);
 expect(retained.roles).toEqual(['sources','research','evidence']);
 expect(programToolChoices(['sources','research','evidence'],[source])).toEqual([source]);
});

test('unknown installed-tool state never pretends an editor is absent',()=>{
 const rows=programServiceContinuations(['sources','community'],[],{context:'self',toolsAvailable:false});
 expect(rows.filter(row=>row.kind==='workspace').every(row=>row.state==='unknown'&&row.href==='/apps?context=self')).toBe(true);
 expect(rows.find(row=>row.id==='community')).toMatchObject({kind:'surface',state:'available',href:'/community'});
});

test('aliases retain service diversity instead of routing every facet into one product family',()=>{
 const rows=programServiceContinuations(['source','provider','budget','product','delivery','data'],[]);
 expect(rows.some(row=>row.pack===P.sourceLibrary)).toBe(true);
 expect(rows.some(row=>row.kind==='account'&&row.id==='connections')).toBe(true);
 expect(rows.some(row=>row.pack===P.finance)).toBe(true);
 expect(rows.some(row=>row.pack===P.catalogue)).toBe(true);
 expect(rows.some(row=>row.pack===P.procurement)).toBe(true);
 expect(rows.some(row=>row.pack===P.shipments)).toBe(true);
 expect(rows.some(row=>row.pack===P.analysis)).toBe(true);
});
