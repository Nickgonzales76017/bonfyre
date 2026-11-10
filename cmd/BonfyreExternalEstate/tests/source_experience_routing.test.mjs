import {test,expect} from 'bun:test';
import {readRouteContracts} from '../tools/source_experience_routing.mjs';

test('authored source semantics bind only declared ExperienceStage programs',()=>{
  expect(readRouteContracts().map(route=>route.destination)).toContain('dallas-civic-media');
});
import {Database} from 'bun:sqlite';
import {readImportedRecords,planSourceRoutes,sourceFingerprint} from '../tools/source_experience_routing.mjs';

const publication=overrides=>({
  kind:'publication_occurrence',namespace:'root',id:'council-2026-15',
  semantic:'civic-meeting',source_ref:'dallas-council',receipt_ref:'receipt-216',
  observed_at:'2026-10-15T10:00:00Z',rights_basis:'public-agenda',
  visibility:'public',assertion_state:'observed',canonical_href:'https://city.test/agenda',
  title:'Purchase offer signed today',summary:'Not a source classifier',
  status:'published',...overrides
});
const auth={
  principal:()=>({actor:'user:fixture-reviewer',userId:7}),
  allowed:(_token,institution,role)=>role==='write'&&
    ['bonfyre-civic','bonfyre-research'].includes(institution)
};

test('source family, not persuasive titles or URL substrings, controls experience ranking',()=>{
  const item=planSourceRoutes([publication()],{auth,token:'test',hasCapability:()=>true}).items[0];
  expect(item.chosen.route).toBe('civic-agenda-review');
  expect(item.chosen.destination).toBe('dallas-civic-media');
  expect(item.candidates.map(x=>x.route)).toEqual([
    'civic-agenda-review','civic-research','civic-town-operations'
  ]);
  expect(item.state).toBe('ready_for_native_review_handoff');
});
test('reviewer can select a different eligible destination only with a reason',()=>{
  const override=planSourceRoutes([publication()],{auth,token:'test',hasCapability:()=>true,
    select:'civic-research',reason:'Needs a research memo before publication'});
  expect(override.items[0].chosen.destination).toBe('research-knowledge-operations');
  expect(override.decision).toBe('reviewer_override');
  expect(override.reason).toContain('research memo');
  expect(()=>planSourceRoutes([publication()],{auth,token:'test',hasCapability:()=>true,
    select:'civic-video-review',reason:'Different output please'})).toThrow('selected destination');
  expect(()=>planSourceRoutes([publication()],{auth,token:'test',hasCapability:()=>true,
    select:'civic-research'})).toThrow('reason required');
});
test('missing binaries, invalid publication scope, and permission denial never become routes',()=>{
  const none=planSourceRoutes([publication()],{auth,token:'test',hasCapability:()=>false}).items[0];
  expect(none.chosen).toBeNull();
  expect(none.candidates[0].state).toBe('capability_unavailable');
  const hidden=planSourceRoutes([publication({visibility:'private'})],{
    auth,token:'test',hasCapability:()=>true}).items[0];
  expect(hidden.state).toBe('source_not_admissible');
  const denied=planSourceRoutes([publication()],{
    auth:{...auth,allowed:()=>false},token:'test',hasCapability:()=>true}).items[0];
  expect(denied.candidates[0].state).toBe('institution_permission_refused');
  expect(denied.chosen).toBeNull();
});
test('source digest changes when the owner receipt or visibility changes',()=>{
  expect(sourceFingerprint(publication())).not.toBe(sourceFingerprint(
    publication({receipt_ref:'receipt-217'})));
  expect(sourceFingerprint(publication())).not.toBe(sourceFingerprint(
    publication({visibility:'private'})));
});
test('CMS selection reads exact published owner rows, using authored source family',()=>{
  const db=new Database(':memory:');
  try{
    db.run('CREATE TABLE internet_source(namespace TEXT,source TEXT,status TEXT,family TEXT)');
    db.run('CREATE TABLE publication_occurrence(namespace TEXT,occurrence_id TEXT,source_ref TEXT,receipt_ref TEXT,observed_at TEXT,rights_basis TEXT,visibility TEXT,assertion_state TEXT,canonical_href TEXT,title TEXT,summary TEXT,status TEXT)');
    db.run("INSERT INTO internet_source VALUES('root','dallas-council','published','civic-meeting')");
    db.run("INSERT INTO publication_occurrence VALUES('root','meeting-1','dallas-council','receipt-1','2026-10-15T10:00:00Z','agenda','public','observed','https://city.test/a','Equipment auction','agenda summary','published')");
    db.run("INSERT INTO publication_occurrence VALUES('root','draft-1','dallas-council','receipt-2','2026-10-15T10:00:00Z','agenda','public','observed','https://city.test/b','Draft','summary','draft')");
    const rows=readImportedRecords(db,{namespace:'root',kind:'publication_occurrence',limit:20});
    expect(rows.map(row=>[row.id,row.semantic])).toEqual([['meeting-1','civic-meeting']]);
    expect(readImportedRecords(db,{namespace:'root',kind:'publication_occurrence',ids:['draft-1']})).toEqual([]);
    expect(planSourceRoutes(rows,{auth,token:'test',hasCapability:()=>true}).items[0].chosen.route).toBe('civic-agenda-review');
  }finally{db.close();}
});
