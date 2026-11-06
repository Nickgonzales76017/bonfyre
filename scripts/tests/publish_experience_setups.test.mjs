import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {portfolio,projectSetups,publishSetups,projectInstrumentsWorkspaceSetup,setupOptions,setupRecords} from '../publish_experience_setups.mjs';
import {packSetupFixture,root} from '../../cmd/BonfyreWebAddress/tests/pack_setup_fixture.mjs';
const f=await packSetupFixture(),checks=[];
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
try {
  const text=readFileSync(join(root,'cmd/BonfyreExperienceStage/experience-maturities.yaff'),'utf8');
  const rows=portfolio(text),records=projectSetups(rows,[1,2,5,6,17,18]);
  const options={records,binary:f.cms,db:f.db,schemas:join(f.home,'schemas')};
  const count=()=>f.sql.query('SELECT count(*) AS n FROM pack_artifact').get().n;
  const before=count(),planned=publishSetups(options);
  check('publication review does not create any configuration',planned.created===0&&count()===before);
  check('six selected products include their declared prerequisite closure',planned.configurations>6);
  const applied=publishSetups({...options,apply:true});
  check('the native CMS retains every projected configuration',count()===before+applied.configurations);
  check('publication replay creates no duplicate definitions',publishSetups({...options,apply:true}).created===0);
  const pack='experience.multi-merchant-catalogue',path='/packs/setup?pack='+pack;
  let response=await f.request(path);
  check('catalogue setup renders its existing published demo',response.status===200&&response.body.includes('data-pack-preview-link')&&response.body.includes('https://aurekai-work-samples.pages.dev/catalogue/'));
  check('demo launch does not claim shared drafts were transferred',response.body.includes('does not transfer its drafts or connect shared editing'));
  response=await f.request('/packs/setup?pack=experience.studio-member-operations');
  check('training remains a guide rather than a hosted application claim',response.body.includes('Application guide')&&response.body.includes('not an installed customer workspace'));
  for(const [id,href] of [['acquisition-brief-studio','https://brief-studio-upgrade.aurekai-work-samples.pages.dev/brief-studio/'],['itad-disposition-exchange','https://itad-desk-upgrade.aurekai-work-samples.pages.dev/itad-exchange/']]){
    response=await f.request('/packs/setup?pack=experience.'+id);
    check(id+' reaches its current public preview',response.status===200&&response.body.includes(href));
  }
  f.add('pack_demand',{pack,demand_kind:'public-preview',needs:'https://user:secret@example.test/',preferred_sources:'Forbidden credential link',fields:'demo',status:'published'});
  response=await f.request(path);
  check('credential-bearing preview links never render',!response.body.includes('Forbidden credential link')&&!response.body.includes('user:secret'));
  const account=await f.login('setup-projection-one@example.test');
  response=await f.request(path,{cookie:account.cookie});
  let sent=await f.request('/packs/setup',{cookie:account.cookie,form:f.form(response.body,'install')});
  check('a new account installs the real projected catalogue configuration',sent.status===303);
  response=await f.request(path,{cookie:account.cookie});
  sent=await f.request('/packs/setup',{cookie:account.cookie,form:f.form(response.body,'activate')});
  check('explicit activation succeeds through native owners',sent.status===303);
  response=await f.request('/packs/closure?pack='+pack+'&context=self',{cookie:account.cookie});
  check('the resulting private configuration is readable',response.status===200&&response.body.includes('Active configuration'));
  const other=await f.login('setup-projection-two@example.test');
  response=await f.request(path,{cookie:other.cookie});
  check('the recipient starts with independent configuration state',response.body.includes('data-pack-state="absent"'));
  check('publishing and adoption grant no institutional roles',f.authsql.query('SELECT count(*) AS n FROM institution_grants').get().n===0);
  const changed=records.map(r=>r.type==='pack_artifact'?{...r,value:{...r.value,purpose:'Unreviewed change'}}:r);
  assert.throws(()=>publishSetups({...options,records:changed,apply:true}),/Existing definition differs/);
  check('unreviewed definition replacement is refused',count()===before+applied.configurations);
  const release='a'.repeat(64),instrumentOptions=setupOptions(['--instruments-workspace',release]);
  const instrumentRecords=setupRecords(instrumentOptions);
  check('the instruments selector remains plan-only by default',instrumentOptions.apply!==true);
  check('six instrument record families retain reviewed jobs and people capability routing',instrumentRecords.filter(r=>r.type==='pack_demand'&&r.value.demand_kind==='workspace-record').map(r=>r.value.fields).join(',')==='process-model:,review-project:,schema-plan:,plan:,price-work:,opportunity:'&&instrumentRecords.some(r=>r.type==='capability_program'&&r.value.program_id==='feed.jobs.relationship'&&r.value.application_kind==='skill-instruments'&&r.value.produces.mode==='relationship'&&r.value.produces.prefix==='opportunity:'&&r.value.consumes.join(',')==='jobs,people'&&r.value.transition_conditions.source_fingerprint_required===true));
  check('instrument setup retains one exact material release',instrumentRecords.find(r=>r.type==='pack_demand'&&r.value.demand_kind==='workspace').value.preferred_sources===release);
  check('instrument setup exposes the native editor entry',instrumentRecords.find(r=>r.type==='pack_demand'&&r.value.demand_kind==='workspace').value.fields==='instruments/');
  check('instrument purpose retains the literal apostrophe',instrumentRecords.find(r=>r.type==='pack_artifact').value.purpose.includes("buyers' requirements"));
  const instrumentOwner={...options,records:instrumentRecords},beforeInstruments=count();
  check('instrument review writes no native records',publishSetups(instrumentOwner).created===0&&count()===beforeInstruments);
  check('native CMS retains all eleven instrument setup rows',publishSetups({...instrumentOwner,apply:true}).created===11);
  check('instrument publication replay is idempotent',publishSetups({...instrumentOwner,apply:true}).created===0);
  const wrongRelease=projectInstrumentsWorkspaceSetup('b'.repeat(64));
  assert.throws(()=>publishSetups({...instrumentOwner,records:wrongRelease,apply:true}),/Existing definition differs/);
  check('a changed instrument release needs an explicit upgrade',count()===beforeInstruments+1);
  for(const bad of [
    ['--instruments-workspace',release,'--finance-workspace',release],
    ['--workspace',release,'--instruments-workspace',release],
    ['--instruments-workspace',release,'--ids','1'],
    ['--instruments-workspace',release,'--portfolio','unused'],
    ['--apply','--apply'],['--db','--apply'],['--db','one','--db','two'],['--unknown','x']
  ])assert.throws(()=>setupOptions(bad));
  check('ambiguous, duplicate and missing options refuse before owner writes',count()===beforeInstruments+1);
  assert.throws(()=>setupRecords(setupOptions(['--instruments-workspace','not-a-digest'])));
  check('instrument selection rejects an unverified release identifier',count()===beforeInstruments+1);
  check('instrument setup creates no institutional roles',f.authsql.query('SELECT count(*) AS n FROM institution_grants').get().n===0);
  console.log('EXPERIENCE_SETUP_PUBLICATION_PASS\t'+checks.length+'\tconfigurations\t'+applied.configurations+'\trows\t'+records.length);
} finally {await f.close();}
