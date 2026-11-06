import {test,expect} from 'bun:test';
import {execFileSync,spawn} from 'node:child_process';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(import.meta.dir,'../bonfyre-cms');
test('native conditional updates admit one writer and preserve history on refusal',async()=>{
  const home=mkdtempSync(join(tmpdir(),'cms-conditional-')),db=join(home,'cms.db'),schema=join(home,'draft.json');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  const args=a=>[...a,'--db',db,'--schemas',home];
  const run=(...a)=>{const output=execFileSync(binary,args(a),{encoding:'utf8',env,stdio:'pipe'});return output.trim()?JSON.parse(output):null;};
  const update=(body,extra=[])=>['entry','update','test_draft','1',JSON.stringify(body),'--ns','forge','--if-field','revision_key','--if-value','v1',...extra];
  try{
    writeFileSync(schema,JSON.stringify({name:'test_draft',attributes:{revision_key:{type:'string',required:true},title:{type:'string',required:true}}}));
    run('schema','apply',schema);run('entry','create','test_draft',JSON.stringify({revision_key:'v1',title:'First'}),'--ns','forge');
    const before=run('ops','count','test_draft','1').ops;
    const race=await Promise.all(['A','B'].map(name=>new Promise((resolve,reject)=>{const p=spawn(binary,args(update({revision_key:name,title:name})),{env,stdio:'ignore'});p.on('error',reject);p.on('exit',code=>resolve(code));})));
    expect(race.filter(code=>code===0)).toHaveLength(1);
    const after=run('entry','get','test_draft','1','--ns','forge');expect(['A','B']).toContain(after.data.attributes.title);
    expect(run('ops','count','test_draft','1').ops).toBe(before+1);
    expect(()=>run(...update({revision_key:'v3',title:'Stale'}))).toThrow();
    expect(()=>run('entry','update','test_draft','1','{"title":"Wrong namespace"}','--ns','still','--if-field','revision_key','--if-value',after.data.attributes.revision_key)).toThrow();
    expect(()=>run('entry','update','test_draft','1','{"title":"Bad field"}','--ns','forge','--if-field','missing','--if-value','v1')).toThrow();
    expect(()=>run('entry','update','test_draft','1','{"title":"Half condition"}','--ns','forge','--if-field','revision_key')).toThrow();
    expect(run('ops','count','test_draft','1').ops).toBe(before+1);
    const file=join(home,'body.json');writeFileSync(file,JSON.stringify({title:'Quotes " and\nnewlines',revision_key:'v3'}));
    run('entry','update','test_draft','1','@'+file,'--ns','forge','--if-field','revision_key','--if-value',after.data.attributes.revision_key);
    expect(run('entry','get','test_draft','1','--ns','forge').data.attributes.title).toBe('Quotes " and\nnewlines');
  }finally{rmSync(home,{recursive:true,force:true});}
});
