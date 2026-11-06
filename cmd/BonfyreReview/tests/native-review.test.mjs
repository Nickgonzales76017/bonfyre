import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { nativeReview } from '../review.mjs';
import { queue as nativeQueue } from '../../../components/BonfyreControlPlane/obligation-work.mjs';

const hash=v=>createHash('sha256').update(v).digest('hex');
function fixture(run) {
  const home=mkdtempSync(join(tmpdir(),'bonfyre-review-'));
  const sourceRoot=join(home,'source');mkdirSync(sourceRoot);
  const policy={experience:'itad-acquisition',institution:'acquisition-north',
    approvedFamily:'itad-review-admitted',sourceRoot};
  const grants=new Map([[1,'write'],[2,'write'],[3,'admin'],[4,'read']]);
  const tokens=new Map([['author',1],['reviewer',2],['admin',3],['reader',4]]);
  const auth={
    principal(token){const userId=tokens.get(token);if(!userId)throw Error('invalid session');return {userId,actor:'actor:'+token};},
    allowed(token,institution,role){
      if(institution!==policy.institution)return false;
      const own=grants.get(tokens.get(token)),levels={read:1,write:2,admin:3};
      return (levels[own]??0)>=levels[role];
    },
    members(token,institution,after=0){
      if(!this.allowed(token,institution,'admin'))throw Error('no directory authority');
      const members=[{email:'reviewer@example.test',user_id:2,effective:true,role:grants.get(2)}];
      return {members:members.filter(row=>row.user_id>after),next_user_id:null};
    }
  };
  const source=(content)=>{
    const file=join(sourceRoot,'artifact.yaff');writeFileSync(file,content);
    return {sourceUri:pathToFileURL(file).href,sourceSha256:hash(content)};
  };
  const engine=()=>nativeReview({home:join(home,'reviews'),auth,experiences:[policy],queue:nativeQueue});
  try{return run({home,policy,auth,grants,tokens,source,engine});}
  finally{rmSync(home,{recursive:true,force:true});}
}
test('authorized review approval emits a durable native WorkGraph mission',()=>fixture(({home,source,engine})=>{
  const review=engine();
  const first=review.submit({token:'author',experience:'itad-acquisition',reviewId:'test-case',
    requestKey:'submit-one',...source('source_record v1\n  lot sample\n')});
  expect(first.state).toBe('pending');expect(first.pending).toEqual([]);
  expect(()=>review.decide({token:'author',reviewId:'test-case',action:'approve',
    expectedRevision:1,requestKey:'self'})).toThrow();
  const done=review.decide({token:'reviewer',reviewId:'test-case',action:'approve',
    expectedRevision:1,reason:'Verified source',requestKey:'approve-one'});
  expect(done.state).toBe('approved');expect(done.pending).toEqual([]);
  const node=nativeQueue(['mission',done.downstream[1].mission],join(home,'reviews','work.db'));
  expect(node.family).toBe('itad-review-admitted');
  expect(node.input_uri).toBe(done.history[1].artifact);
  review.close();
  const reopened=engine();
  expect(reopened.read({token:'reader',reviewId:'test-case'}).history.map(e=>e.action)).toEqual(['submit','approve']);
  reopened.close();
}));
