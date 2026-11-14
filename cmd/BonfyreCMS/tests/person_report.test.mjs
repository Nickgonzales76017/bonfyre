import {test,expect} from 'bun:test';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(import.meta.dir,'../bonfyre-cms');
const schemas=resolve(import.meta.dir,'../content-types');
test('person reports file once per identity and resolve conditionally',()=>{
  const home=mkdtempSync(join(tmpdir(),'cms-report-')),db=join(home,'cms.db');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  const run=(...a)=>{const output=execFileSync(binary,[...a,'--db',db,'--schemas',schemas],{encoding:'utf8',env,stdio:'pipe'});return output.trim()?JSON.parse(output):null;};
  const report=(id,actor,target,reason)=>['entry','create','person_report',JSON.stringify({report:id,actor_subject:actor,target_subject:target,reason,details:'',created_at:'2026-11-10T00:00:00Z',cannot_infer:'An observation, not a verdict.',status:'published'}),'--ns','root'];
  try{
    const a=run(...report('rep-1','person:a','person:b','spam'));
    expect(a.id).toBeGreaterThan(0);
    expect(()=>run(...report('rep-1','person:a','person:b','spam'))).toThrow();
    expect(run('entry','get','person_report',String(a.id),'--ns','root').data.attributes.reason).toBe('spam');
    run('entry','update','person_report',String(a.id),'{"status":"archived"}','--ns','root','--if-field','status','--if-value','published');
    expect(run('entry','get','person_report',String(a.id),'--ns','root').data.attributes.status).toBe('archived');
  }finally{rmSync(home,{recursive:true,force:true});}
});
