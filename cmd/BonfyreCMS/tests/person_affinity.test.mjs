import {test,expect} from 'bun:test';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(import.meta.dir,'../bonfyre-cms');
const schemas=resolve(import.meta.dir,'../content-types');
test('person affinity edges create once, withdraw conditionally, and hold uniqueness',()=>{
  const home=mkdtempSync(join(tmpdir(),'cms-affinity-')),db=join(home,'cms.db');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  const run=(...a)=>{const output=execFileSync(binary,[...a,'--db',db,'--schemas',schemas],{encoding:'utf8',env,stdio:'pipe'});return output.trim()?JSON.parse(output):null;};
  const edge=(affinity,actor,target,kind,state='active')=>['entry','create','person_affinity',JSON.stringify({affinity,actor_subject:actor,target_subject:target,kind,state,created_at:'2026-11-10T00:00:00Z',cannot_infer:'A directed affinity edge. Private until mutual; a pass only excludes discovery.',status:'published'}),'--ns','dating'];
  try{
    const a=run(...edge('a1','person:a','person:b','like'));
    expect(a.id).toBeGreaterThan(0);
    run(...edge('a2','person:b','person:a','superlike'));
    run(...edge('a3','person:a','person:c','pass'));
    expect(()=>run(...edge('a1','person:x','person:y','like'))).toThrow();
    const row=run('entry','get','person_affinity',String(a.id),'--ns','dating');
    expect(row.data.attributes.kind).toBe('like');
    run('entry','update','person_affinity',String(a.id),'{"state":"withdrawn"}','--ns','dating','--if-field','state','--if-value','active');
    expect(run('entry','get','person_affinity',String(a.id),'--ns','dating').data.attributes.state).toBe('withdrawn');
    expect(()=>run('entry','update','person_affinity',String(a.id),'{"state":"active"}','--ns','dating','--if-field','state','--if-value','active')).toThrow();
  }finally{rmSync(home,{recursive:true,force:true});}
});
