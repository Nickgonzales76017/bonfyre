import {test,expect} from 'bun:test';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(import.meta.dir,'../bonfyre-cms');
const schemas=resolve(import.meta.dir,'../content-types');
test('person quiz answers create once per identity and archive conditionally',()=>{
  const home=mkdtempSync(join(tmpdir(),'cms-quiz-')),db=join(home,'cms.db');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  const run=(...a)=>{const output=execFileSync(binary,[...a,'--db',db,'--schemas',schemas],{encoding:'utf8',env,stdio:'pipe'});return output.trim()?JSON.parse(output):null;};
  const edge=(answer,actor,question,choice,weight)=>['entry','create','person_quiz_answer',JSON.stringify({answer,actor_subject:actor,question,choice,weight,created_at:'2026-11-11T00:00:00Z',cannot_infer:'A stated quiz answer.',status:'published'}),'--ns','root'];
  try{
    const a=run(...edge('qa-abc','person:a','q-weekend','a','2'));
    expect(a.id).toBeGreaterThan(0);
    run(...edge('qa-def','person:a','q-morning','b','1'));
    expect(()=>run(...edge('qa-abc','person:a','q-weekend','a','2'))).toThrow();
    const row=run('entry','get','person_quiz_answer',String(a.id),'--ns','root');
    expect(row.data.attributes.choice).toBe('a');
    run('entry','update','person_quiz_answer',String(a.id),'{"status":"archived"}','--ns','root','--if-field','status','--if-value','published');
    expect(run('entry','get','person_quiz_answer',String(a.id),'--ns','root').data.attributes.status).toBe('archived');
    expect(()=>run('entry','update','person_quiz_answer',String(a.id),'{"status":"published"}','--ns','root','--if-field','status','--if-value','published')).toThrow();
  }finally{rmSync(home,{recursive:true,force:true});}
});
