import {test,expect} from 'bun:test';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(import.meta.dir,'../bonfyre-cms');
const schemas=resolve(import.meta.dir,'../content-types');
test('person interests create once per identity and archive conditionally',()=>{
  const home=mkdtempSync(join(tmpdir(),'cms-interest-')),db=join(home,'cms.db');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  const run=(...a)=>{const output=execFileSync(binary,[...a,'--db',db,'--schemas',schemas],{encoding:'utf8',env,stdio:'pipe'});return output.trim()?JSON.parse(output):null;};
  const edge=(interest,actor,label)=>['entry','create','person_interest',JSON.stringify({interest,actor_subject:actor,label,weight:'1',created_at:'2026-11-10T00:00:00Z',cannot_infer:'A stated interest label.',status:'published'}),'--ns','root'];
  try{
    const a=run(...edge('int-abc','person:a','hiking'));
    expect(a.id).toBeGreaterThan(0);
    run(...edge('int-def','person:a','cycling'));
    expect(()=>run(...edge('int-abc','person:a','hiking'))).toThrow();
    const row=run('entry','get','person_interest',String(a.id),'--ns','root');
    expect(row.data.attributes.label).toBe('hiking');
    run('entry','update','person_interest',String(a.id),'{"status":"archived"}','--ns','root','--if-field','status','--if-value','published');
    expect(run('entry','get','person_interest',String(a.id),'--ns','root').data.attributes.status).toBe('archived');
    expect(()=>run('entry','update','person_interest',String(a.id),'{"status":"published"}','--ns','root','--if-field','status','--if-value','published')).toThrow();
  }finally{rmSync(home,{recursive:true,force:true});}
});
