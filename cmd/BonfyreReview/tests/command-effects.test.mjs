import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, copyFileSync, writeFileSync, rmSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { packFixture, repo } from '../../BonfyreCMS/tests/pack_lifecycle_fixture.mjs';
import { nativeReview } from '../review.mjs';
import { commandBindings, reviewedCommandEffects, readCommandIntent } from '../command_effects.mjs';
import { queue as nativeQueue } from '../../../components/BonfyreControlPlane/obligation-work.mjs';

const sha=v=>createHash('sha256').update(v).digest('hex');
const program=(command,pack='media-pack')=>({
  program_id:'media-'+command, application_kind:'native-command',action:command,
  consumes:{kind:'source-library'},produces:{kind:'workspace-record'},
  carrier_candidates:[{kind:'native-workgraph',type:command,operator_id:'queue.'+command}],
  capability_requirements:{installed_pack:pack,workspace_family:'media-studio'},
  authority_requirement:'actor+pack+independent-review',
  transition_conditions:{review:{experience:'media-review',institution:'media-team',
    approved_family:'media-reviewed',executor:'BonfyreQueue',
    artifact_kind:'immutable-review-decision'}},
  failure_projection:'refuse-with-owner',status:'published',
});
const content=fields=>'command_effect_intent v1\n'+[
  'command','program','pack','pack_digest','subject','workspace_binding',
  'workspace_release','operator','input_sha256'
].map(key=>'  '+key+' '+fields[key]).join('\n')+'\n';

function fixture(run) {
  const f=packFixture();
  let review;
  try {
    for(const type of ['application_binding','capability_program'])
      copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),
        join(f.schemas,type+'.json'));
    const migrated=spawnSync(f.binary,['schema','migrate','--db',f.db,
      '--schemas',f.schemas],{encoding:'utf8'});
    if(migrated.status!==0)throw Error('CMS schema migration: '+migrated.stderr);
    f.pack('media-pack');
    const release=sha('media-workspace-release');
    f.add('pack_demand',{pack:'media-pack',demand_kind:'workspace',needs:'media-studio',
      preferred_sources:release,fields:'media-editor/',status:'published'});
    f.add('capability_program',program('transcribe'));
    const plan=f.call('plan','media-pack',{actor:'user:1'});
    if(plan.code!==0)throw Error('plan: '+plan.raw+' '+plan.stderr);
    const installed=f.call('install','media-pack',{actor:'user:1',digest:plan.digest});
    if(installed.code!==0)throw Error('install: '+installed.raw+' '+installed.stderr);
    const activated=f.call('activate','media-pack',{actor:'user:1',
      revision:Number(installed.revision),digest:installed.pinned_digest});
    if(activated.code!==0||activated.state!=='active')
      throw Error('activate: '+activated.raw+' '+activated.stderr);
    const sourceRoot=join(f.home,'source');mkdirSync(sourceRoot);
    const grants=new Map([['author',{userId:1,actor:'actor:author'}],
      ['reviewer',{userId:2,actor:'actor:reviewer'}]]);
    const auth={
      principal:token=>{const user=grants.get(token);if(!user)throw Error('revoked ActorClosure');return user;},
      allowed:(token,institution,role)=>grants.has(token)&&institution==='media-team'&&
        ['read','write'].includes(role),
      members:()=>({members:[],next_user_id:null})
    };
    const home=join(f.home,'reviews');
    review=nativeReview({home,auth,experiences:[{
      experience:'media-review',institution:'media-team',approvedFamily:'media-reviewed',
      sourceRoot}],queue:nativeQueue});
    const workspaceCurrent=({pack,subject})=>{
      const row=f.sql.query(`SELECT w.binding_id AS binding,w.release,a.status
         FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id
         JOIN pack_installation i ON i.id=w.installation_id
         WHERE i.pack=? AND i.installed_for=?`).get(pack,subject);
      return row??null;
    };
    const packCurrent=({pack,subject,scope,actor})=>{
      const result=f.call('current',pack,{subject,scope,actor});
      if(result.code!==0)return null;
      return {state:result.state,digest:result.digest,
        pinned_digest:result.pinned_digest};
    };
    const programs=f.sql.query('SELECT * FROM capability_program').all().map(row=>({
      ...row,carrier_candidates:JSON.parse(row.carrier_candidates),
      capability_requirements:JSON.parse(row.capability_requirements),
      transition_conditions:JSON.parse(row.transition_conditions)
    }));
    const workerState={state:'ready'};
    const workerCurrent=({operator,program,packDigest,workspaceBinding})=>({
      state:workerState.state,operator_id:operator,program_id:program,
      workspace_binding:workspaceBinding,
      pack_digest:plan.digest,binary_sha256:sha('native-worker-fixture')
    });
    const binder=()=>reviewedCommandEffects({programs,review,auth,packCurrent,
      workspaceCurrent,workerCurrent,queue:nativeQueue,queueDb:join(home,'work.db')});
    return run({f,auth,grants,review,binder,programs,workerState,
      workspaceCurrent,packCurrent,sourceRoot,release,plan,content});
  }finally{review?.close();f.close();}
}
test('CMS capability + PackGraph + independent approval admit the exact native Queue operator',()=>fixture(
  ({f,review,binder,workspaceCurrent,packCurrent,sourceRoot,release,plan,workerState,grants})=>{
    const current=packCurrent({pack:'media-pack',subject:'user:1',scope:'self',actor:'user:1'});
    const workspace=workspaceCurrent({pack:'media-pack',subject:'user:1'});
    expect(current.state).toBe('active');
    expect(workspace.status).toBe('active');
    expect(workspace.release).toBe(release);
    const fields={command:'transcribe',program:'media-transcribe',pack:'media-pack',
      pack_digest:current.digest,subject:'user:1',workspace_binding:workspace.binding,
      workspace_release:workspace.release,operator:'queue.transcribe',
      input_sha256:sha('source-library-input')};
    const original=content(fields),file=join(sourceRoot,'intent.yaff');
    writeFileSync(file,original);
    const submitted=review.submit({token:'author',experience:'media-review',
      reviewId:'media-case',sourceUri:pathToFileURL(file).href,
      sourceSha256:sha(original),requestKey:'submit-media'});
    expect(submitted.state).toBe('pending');
    expect(()=>binder().dispatch({token:'author',command:'transcribe',
      program:'media-transcribe',context:'self',reviewId:'media-case',
      sourceSha256:sha(original)})).toThrow();
    expect(()=>review.decide({token:'author',reviewId:'media-case',action:'approve',
      expectedRevision:1,requestKey:'self-approval'})).toThrow();
    const approved=review.decide({token:'reviewer',reviewId:'media-case',
      action:'approve',expectedRevision:1,requestKey:'reviewer-approval'});
    expect(approved.state).toBe('approved');
    expect(approved.downstream.at(-1).admitted).toBe(true);
    const request={token:'author',command:'transcribe',program:'media-transcribe',
      context:'self',reviewId:'media-case',sourceSha256:sha(original)};
    const admitted=binder().dispatch(request);
    expect(admitted).toMatchObject({admitted:true,executed:false,
      command:'transcribe',program:'media-transcribe',pack:'media-pack',
      subject:'user:1',operator_id:'queue.transcribe',
      pack_digest:current.digest,workspace_binding:workspace.binding});
    expect(admitted.review_mission).toBe(approved.downstream.at(-1).mission);
    expect(binder().dispatch(request).mission_id).toBe(admitted.mission_id);
    // No CLI lookup, random argv or caller's own approval can redirect the job.
    expect(()=>binder().dispatch({...request,command:'pay'})).toThrow();
    expect(()=>binder().dispatch({...request,program:'media-narrate'})).toThrow();
    expect(()=>binder().dispatch({...request,context:'institution:other'})).toThrow();
    expect(()=>binder().dispatch({...request,sourceSha256:sha('other')})).toThrow();
    workerState.state='offline';
    expect(()=>binder().dispatch(request)).toThrow();
    workerState.state='ready';
    grants.delete('author');
    expect(()=>binder().dispatch(request)).toThrow();
    grants.set('author',{userId:1,actor:'actor:author'});
    // Native CMS retained-source digests invalidate installed capability.
    f.sql.query("UPDATE pack_demand SET needs='changed' WHERE pack='media-pack'").run();
    expect(()=>binder().dispatch(request)).toThrow();
    f.sql.query("UPDATE pack_demand SET needs='media-studio' WHERE pack='media-pack'").run();
    // An old approval does not authorize a changed source intent.
    writeFileSync(file,original.replace('queue.transcribe','queue.narrate'));
    expect(()=>binder().dispatch(request)).toThrow();
  }
));
test('declarations must be unique, typed and independently reviewed',()=>{
  const base=program('transcribe');
  expect(commandBindings([base]).get('transcribe')).toMatchObject({
    command:'transcribe',pack:'media-pack',operator:'queue.transcribe',
    executor:'BonfyreQueue',approvedFamily:'media-reviewed'});
  expect(()=>commandBindings([base,base])).toThrow(/Ambiguous/);
  expect(()=>commandBindings([{...base,authority_requirement:'api-key'}])).toThrow();
  expect(()=>commandBindings([{...base,carrier_candidates:[{kind:'native-workgraph',
    type:'pay',operator_id:'queue.pay'}]}])).toThrow();
  expect(()=>commandBindings([{...base,transition_conditions:{review:{
    ...base.transition_conditions.review,executor:'local-shell'}}}])).toThrow();
  expect(commandBindings([{...base,status:'draft'}]).size).toBe(0);
});
test('source proof rejects aliases, reordered fields and unpinned bytes',()=>fixture(
  ({sourceRoot,content})=>{
    const candidate={command:'transcribe',program:'media-transcribe',
      pack:'media-pack',pack_digest:sha('pack'),subject:'user:1',
      workspace_binding:'ws-'+sha('workspace').slice(0,40),
      workspace_release:sha('release'),operator:'queue.transcribe',
      input_sha256:sha('source')};
    const file=join(sourceRoot,'standalone.yaff'),bytes=content(candidate),uri=pathToFileURL(file).href;
    writeFileSync(file,bytes);
    expect(readCommandIntent(uri,sha(bytes))).toEqual(candidate);
    expect(()=>readCommandIntent(uri,sha('different'))).toThrow();
    writeFileSync(file,bytes.replace('  operator','  obsolete'));
    expect(()=>readCommandIntent(uri,sha(bytes))).toThrow();
    const rearranged=bytes.replace('  command transcribe\n  program media-transcribe\n',
      '  program media-transcribe\n  command transcribe\n');
    writeFileSync(file,rearranged);
    expect(()=>readCommandIntent(uri,sha(rearranged))).toThrow();
  }
));
