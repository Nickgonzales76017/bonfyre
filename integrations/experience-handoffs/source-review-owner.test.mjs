import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {sourceReviewOwner} from './source-review-owner.mjs';

test('published CMS occurrence enters native WorkGraph review without an effect',()=>{
  const home=mkdtempSync(join(tmpdir(),'bonfyre-source-review-'));
  try{
    const cms=join(home,'cms.db'),db=new Database(cms);
    db.exec('CREATE TABLE internet_source(namespace TEXT,source TEXT,status TEXT,family TEXT);');
    db.exec('CREATE TABLE publication_occurrence(namespace TEXT,occurrence_id TEXT,source_ref TEXT,receipt_ref TEXT,observed_at TEXT,rights_basis TEXT,visibility TEXT,assertion_state TEXT,canonical_href TEXT,title TEXT,summary TEXT,status TEXT);');
    db.exec("INSERT INTO internet_source VALUES('root','council','published','civic-meeting');");
    db.exec("INSERT INTO publication_occurrence VALUES('root','agenda-17','council','receipt-17','2026-10-15T10:00:00Z','public','public','observed','https://city.test/agenda','Agenda','Source','published');");
    db.close();
    const auth={principal:()=>({actor:'actor:test-reviewer',userId:1}),
      allowed:(_token,institution,role)=>institution==='bonfyre-civic'&&role==='write'};
    const binary=join(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue');
    const owner=sourceReviewOwner({cmsDb:cms,home,auth,binary,hasCapability:()=>true});
    const staged=owner.stage('session',{ids:['agenda-17'],kind:'publication_occurrence'});
    expect(staged.items).toHaveLength(1);
    expect(staged.items[0].state).toBe('admitted_for_native_review');
  }finally{rmSync(home,{recursive:true,force:true});}
});
