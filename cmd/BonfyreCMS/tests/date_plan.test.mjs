import {test,expect} from 'bun:test';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(import.meta.dir,'../bonfyre-cms');
const schemas=resolve(import.meta.dir,'../content-types');
test('date plans persist with receipts and accept conditionally',()=>{
  const home=mkdtempSync(join(tmpdir(),'cms-dateplan-')),db=join(home,'cms.db');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  const run=(...a)=>{const output=execFileSync(binary,[...a,'--db',db,'--schemas',schemas],{encoding:'utf8',env,stdio:'pipe'});return output.trim()?JSON.parse(output):null;};
  const plan=(over={})=>['entry','create','date_plan',JSON.stringify({plan:'plan-1',actor_subject:'person:a',location:'Dallas, TX',intent:'first date',budget_min:'0',budget_max:'60',interests:'hiking',plan_json:'{"plans":[]}',costs_admitted:'0-15',accepted:'false',generation_model:'qwen-test',prompt_sha256:'p'.repeat(64),output_sha256:'o'.repeat(64),external_id:'dp-1',created_at:'2026-11-10T00:00:00Z',cannot_infer:'Admitted output.',status:'published',...over}),'--ns','root'];
  try{
    const a=run(...plan());
    expect(a.id).toBeGreaterThan(0);
    expect(()=>run(...plan())).toThrow();
    expect(run('entry','get','date_plan',String(a.id),'--ns','root').data.attributes.accepted).toBe('false');
    run('entry','update','date_plan',String(a.id),'{"accepted":"true"}','--ns','root','--if-field','accepted','--if-value','false');
    expect(run('entry','get','date_plan',String(a.id),'--ns','root').data.attributes.accepted).toBe('true');
    expect(()=>run('entry','update','date_plan',String(a.id),'{"accepted":"false"}','--ns','root','--if-field','accepted','--if-value','false')).toThrow();
  }finally{rmSync(home,{recursive:true,force:true});}
});
