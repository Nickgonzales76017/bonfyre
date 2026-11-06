import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,rmSync,copyFileSync,chmodSync,writeFileSync} from 'node:fs';
import {Database} from 'bun:sqlite';
import {tmpdir} from 'node:os';
import {resolve,join,dirname} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createProjectNative} from '../project_native.mjs';
import {createProjectNode,CATALOG} from '../project_node.mjs';
import {trainingHandler,trainingMaterial} from '../../../components/BonfyreServiceSite/training-workspace.mjs';
import {requestStore} from '../../../components/BonfyreServiceSite/training-requests.mjs';
import {publishingStore} from '../../../components/BonfyreServiceSite/training-publishing.mjs';
import {creatorAuth,creatorInstitution} from '../../../components/BonfyreServiceSite/training-creator.mjs';
import {programInstitution} from '../../../components/BonfyreServiceSite/training-publishing.mjs';
import {newTraining,trainingCommand} from '../../../components/BonfyreServiceSite/training-model.mjs';

const root=resolve('.');
const material=trainingMaterial('packs/aurekai-fitness-studio');
const origin='http://127.0.0.1:4190';

function fixture(){
  const home=mkdtempSync(join(tmpdir(),'bonfyre-training-mcp-'));
  const cmsHome=join(home,'publications'),authDb=join(home,'auth.db');
  const queue=requestStore(join(home,'requests'),material);
  const publishing=publishingStore(cmsHome,material,queue);
  const auth=creatorAuth(authDb);
  const handler=trainingHandler(material,4190,{requests:queue,publishing,auth});
  const executable=resolve('cmd/BonfyreCMS/bonfyre-cms');
  const managed=join(home,'.bonfyre/bin/bonfyre-cms');
  mkdirSync(dirname(managed),{recursive:true});copyFileSync(executable,managed);chmodSync(managed,0o755);
  writeFileSync(join(home,'.bonfyre/bin/.families.json'),JSON.stringify({'bonfyre-cms':'BonfyreCMS'}));
  const calls=[];
  const run=async(argv,options)=>{
    calls.push({argv,options});
    try{
      const stdout=execFileSync(argv[0],argv.slice(1),{cwd:options.cwd,env:options.env,
        encoding:'utf8',stdio:['pipe','pipe','pipe'],timeout:15000,maxBuffer:16*1024*1024});
      return {exit_code:0,stdout,stderr:'',duration_ms:0};
    }catch(error){
      return {exit_code:error.status??1,stdout:String(error.stdout??''),
        stderr:String(error.stderr??'native command refused'),duration_ms:0};
    }
  };
  const env={BONFYRE_TRAINING_CMS_HOME:cmsHome,BONFYRE_TRAINING_MCP_PUBLIC_STUDIOS:'forge'};
  const mcp=createProjectNative({root,home,env,run,request:async()=>{throw Error('Unexpected remote provider')}});
  const nativeAuth=(cmd,args=[],input='')=>JSON.parse(execFileSync(resolve('cmd/BonfyreAuth/bonfyre-auth'),
    ['--db',authDb,cmd,...args],{input,encoding:'utf8',stdio:'pipe',timeout:10000}));
  const ask=async(route,body,cookie='',headers={})=>handler(new Request(origin+route,{
    method:body===undefined?'GET':'POST',
    headers:{host:'127.0.0.1:4190',origin,'content-type':'application/json',cookie,...headers},
    body:body===undefined?undefined:JSON.stringify(body)
  }));
  return {home,publishing,queue,auth,nativeAuth,ask,mcp,calls,env,run,cleanup:()=>rmSync(home,{recursive:true,force:true})};
}

test('one member request reaches native WorkGraph, actor-authorized CMS publication, member adoption and read-only MCP',async()=>{
  const f=fixture();
  try{
    const original=material.creators.find(c=>c.id==='forge').routines[0],id=crypto.randomUUID();
    const brief={id,creatorId:'forge',programId:original.id,
      title:'Private member: travel dumbbells',details:'Private workout needs a 30-minute adaptation.'};
    const posted=await f.ask('/api/training/requests',brief);
    expect(posted.status).toBe(200);
    const submitted=await posted.json();
    expect(submitted.state).toBe('ready');
    expect(f.queue.list('still')).toEqual([]);
    const nativeStatus=f.queue.read(id,submitted.accessToken);
    expect(nativeStatus.brief.programId).toBe(original.id);

    const email='coach@fixture.test',password='isolated-creator-test-password';
    const signup=f.nativeAuth('http-signup',[],JSON.stringify({email,password,name:'Fixture coach'}));
    const userId=String(signup.user?.id??1);
    f.nativeAuth('actor-bind',['--user-id',userId,'--actor','actor:fixture-coach']);
    f.nativeAuth('institution-grant',['--user-id',userId,
      '--institution',creatorInstitution('forge'),'--role','write']);
    const login=await f.ask('/api/training/creator/login',{email,password});
    expect(login.status).toBe(200);
    const cookie=login.headers.get('set-cookie').split(';')[0];
    const base='/api/training/creator/studios/forge';
    expect((await f.ask(base+'/programs',{publicationId:crypto.randomUUID()},cookie)).status).toBe(403);
    const claim=await f.ask(base+'/requests/'+id+'/accept',{},cookie);
    expect(claim.status).toBe(200);
    expect((await claim.json()).state).toBe('running');
    const adapted=structuredClone(original);
    adapted.name='Thirty-minute dumbbells';
    adapted.exercises.forEach(e=>{e.restSeconds=45;e.sets=e.sets.slice(0,2);});
    const draftResponse=await f.ask(base+'/requests/'+id+'/draft',
      {routine:adapted,expectedRevision:0},cookie);
    expect(draftResponse.status).toBe(200);
    const draft=await draftResponse.json();
    expect(draft.revision).toBe(1);
    const delivered=await f.ask(base+'/requests/'+id+'/deliver',{revision:1},cookie);
    expect(delivered.status).toBe(200);
    const delivery=await delivered.json();
    expect(delivery.state).toBe('complete');
    expect(delivery.delivery.sha256).toBe(f.queue.read(id,submitted.accessToken).delivery.sha256);

    f.nativeAuth('institution-grant',['--user-id',userId,
      '--institution',programInstitution('forge'),'--role','write']);
    const proposal={publicationId:crypto.randomUUID(),programId:crypto.randomUUID(),
      expectedVersion:0,requestId:id,title:'Travel strength: 30 minutes',
      description:'A reviewed public short workout using dumbbells.'};
    const publication=await f.ask(base+'/programs',proposal,cookie);
    expect(publication.status).toBe(200);
    const published=await publication.json();
    expect(published.version).toBe(1);
    expect(published.status).toBe('published');
    const appResponse=await f.ask('/api/training/programs/forge');
    expect(appResponse.status).toBe(200);
    const app=(await appResponse.json()).programs;
    expect(app).toHaveLength(1);

    const mcpResult=await f.mcp.call('training_public_programs',{studio:'forge'});
    expect(mcpResult.programs).toHaveLength(1);
    expect(mcpResult.programs[0].routine).toEqual(app[0].routine);
    expect(mcpResult.programs[0].key).toBe(app[0].key);
    expect(mcpResult.programs[0].version).toBe(1);
    expect(mcpResult.programs[0].contentSha256).toMatch(/^[a-f0-9]{64}$/);
    const serialized=JSON.stringify(mcpResult);
    for(const secret of [id,brief.title,brief.details,submitted.accessToken,
      'actor:fixture-coach','origin_request','publisher_actor'])
      expect(serialized).not.toContain(secret);
    const member=trainingCommand(newTraining('fixture-member'),
      {type:'routine.save',routine:mcpResult.programs[0].routine});
    expect(member.routines).toHaveLength(1);
    expect(member.routines[0].source.creator).toBe('forge');
    expect(member.routines[0].source.version).toBe('1');
    expect(member.history).toEqual([]);

    const node=await createProjectNode({root,home:f.home,token:'isolated-mcp-token',
      env:f.env,owners:{reads:async()=>({}),operator:async()=>({}),
        native:(name,args)=>f.mcp.call(name,args),spines:async()=>({}),v6:async()=>({})}});
    const exposed=await node.call('training_public_programs',{studio:'forge'});
    expect(exposed.programs[0].routine).toEqual(member.routines[0]);
    expect(CATALOG.tools.find(t=>t.name==='training_public_programs')?.annotations.readOnlyHint).toBe(true);
    const reads=f.calls.filter(c=>c.argv.some(a=>a==='training_program_release'));
    expect(reads.length).toBeGreaterThan(0);
    expect(reads.every(c=>c.argv.includes('list')&&!c.argv.includes('update')&&!c.argv.includes('create'))).toBe(true);
    await expect(f.mcp.call('training_public_programs',{studio:'still'})).rejects.toThrow('not enabled');

    const withdrawn=await f.ask(base+'/programs/'+published.releaseId+'/withdraw',{},cookie);
    expect(withdrawn.status).toBe(200);
    expect((await f.ask('/api/training/programs/forge').then(r=>r.json())).programs).toEqual([]);
    expect((await f.mcp.call('training_public_programs',{studio:'forge'})).programs).toEqual([]);
    expect(member.routines).toHaveLength(1);
    expect(member.history).toEqual([]);
  } finally {f.cleanup();}
});

test('MCP public catalogue requires explicit configuration and returns an empty unconfigured catalogue',async()=>{
  const f=fixture();
  try{
    const noStore=createProjectNative({root,home:f.home,env:{},run:f.run,request:async()=>{}});
    await expect(noStore.call('training_public_programs',{studio:'forge'})).rejects.toThrow('not enabled');
    const noSite=createProjectNative({root,home:f.home,
      env:{...f.env,BONFYRE_TRAINING_MCP_PUBLIC_STUDIOS:'still'},run:f.run,request:async()=>{}});
    await expect(noSite.call('training_public_programs',{studio:'forge'})).rejects.toThrow('not enabled');
    const empty=await f.mcp.call('training_public_programs',{studio:'forge'});
    expect(empty.programs).toEqual([]);
  }finally{f.cleanup();}
});

test('MCP matches the public latest-version rule and refuses a changed native release',async()=>{
  const f=fixture();
  try{
    const original=material.creators.find(c=>c.id==='forge').routines[0],id=crypto.randomUUID();
    f.queue.submit({id,creatorId:'forge',programId:original.id,
      title:'Private member request',details:'Do not publish the member briefing.'});
    f.queue.accept(id,'fixture-coach');f.queue.deliver(id,original);
    const key=crypto.randomUUID(),base={programId:key,requestId:id,description:'Public routine only.'};
    const first=f.publishing.publish('forge','actor:fixture-coach',{...base,
      publicationId:crypto.randomUUID(),expectedVersion:0,title:'Version one'});
    expect((await f.mcp.call('training_public_programs',{studio:'forge'})).programs[0].routine.id)
      .toBe(first.routine.id);
    const second=f.publishing.publish('forge','actor:fixture-coach',{...base,
      publicationId:crypto.randomUUID(),expectedVersion:1,title:'Version two'});
    expect((await f.mcp.call('training_public_programs',{studio:'forge'})).programs[0].version).toBe(2);
    f.publishing.withdraw('forge',second.releaseId);
    expect((await f.mcp.call('training_public_programs',{studio:'forge'})).programs).toEqual([]);
    const db=new Database(join(f.home,'publications','catalogue.db'));
    db.run('UPDATE training_program_release SET title=? WHERE id=?',['Unreviewed mutation',first.releaseId]);
    db.close();
    await expect(f.mcp.call('training_public_programs',{studio:'forge'}))
      .rejects.toThrow('changed after native CMS approval');
  }finally{f.cleanup();}
});
