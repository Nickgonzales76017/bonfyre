import {afterEach,expect,test} from 'bun:test';
import {copyFileSync,mkdirSync,mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync} from 'node:fs';
import {homedir,tmpdir} from 'node:os';
import {join} from 'node:path';
import {createModelEngine} from '../spines_model.mjs';
import {createSpinesStore} from '../spines_store.mjs';
import {projectRun} from '../project_runtime.mjs';
import {parsedDocument} from '../canonical_json.mjs';
const probe=process.env.BONFYRE_KERNEL_PROBE??join(homedir(),'.local/bin/bonfyre-kernel-probe');
const roots=[],stores=[];
function fixture({run,system='linux'}={}){
  const root=realpathSync(mkdtempSync(join(tmpdir(),'spines-model-')));roots.push(root);
  const projectRoot=join(root,'project');mkdirSync(projectRoot);
  const store=createSpinesStore({home:join(root,'owner'),projectRoot,deltaKernel:probe,helixVm:probe,kernelProbe:probe,
    env:{PATH:process.env.PATH},now:()=>1800000000,...(run?{run}: {})});stores.push(store);
  return {root,store,model:createModelEngine(store,{userHome:join(root,'user'),pathValue:'',system,environment:{}})};
}
afterEach(()=>{for(const store of stores.splice(0))store.close();for(const root of roots.splice(0))rmSync(root,{recursive:true,force:true});});

test('native CPU probe verifies real dot/variance and layernorm bytes without claiming GPU inference',async()=>{
  const {store,model}=fixture();const result=await model.kernelParity();
  expect(result.state).toBe('passed');expect(result.candidate.count).toBe(8);expect(result.reference.dot).toBe(120);
  expect(result.probe_identity.sha256).toHaveLength(64);expect(result.evidence_scope).toBe('eight-element-native-cpu-reference-only');
  expect(store.db.query('SELECT state FROM kernel_parity_runs').get().state).toBe('passed');expect(store.verifyReceipts().verified).toBe(true);
});

test('another backend cannot relabel CPU parity as Metal or CUDA proof',async()=>{
  let calls=0;const {model}=fixture({run:async()=>{calls++;throw new Error('must not execute');}});
  const result=await model.kernelParity('Metal');expect(result.state).toBe('blocked');expect(result.probe).toBeNull();expect(calls).toBe(0);
});

test('valid-looking native numbers with failing exit or incorrect count cannot pass',async()=>{
  for(const variant of ['exit','count','layernorm','timed_out','cancelled']){
    const {model}=fixture({run:async(argv,options)=>{
      const result=await projectRun(argv,options),body=JSON.parse(result.stdout);
      if(variant==='count')body.count=9;if(variant==='layernorm')body.layernorm_sha256='0'.repeat(64);
      return {...result,exit_code:variant==='exit'?1:0,stdout:JSON.stringify(body),...(variant==='timed_out'?{timeout:true}:{}),...(variant==='cancelled'?{cancelled:true}:{})};
    }});expect((await model.kernelParity()).state).toBe('failed');
  }
});

test('inventory bounds discovery, refuses script execution and never verifies model weights',async()=>{
  let calls=0;const {store,model}=fixture({run:async(argv,options)=>{calls++;return projectRun(argv,options);}});
  const cmd=join(store.projectRoot,'cmd','BonfyreQwenFPQ');mkdirSync(cmd,{recursive:true});
  copyFileSync(probe,join(cmd,'bonfyre-qwen-fpq'));
  const bin=join(store.projectRoot,'bin');mkdirSync(bin);
  writeFileSync(join(bin,'llama-cli'),'#!/usr/bin/env python3\nraise Exception("never execute")\n',{mode:0o700});
  const result=await model.discoverModels();expect(result.count).toBe(2);expect(calls).toBe(1);
  expect(result.models.find(x=>x.name==='llama-cli').runtime_boundary).toBe('unverified-script-refused');
  expect(result.models.find(x=>x.name==='BonfyreQwenFPQ').path).toBe(realpathSync(join(cmd,'bonfyre-qwen-fpq')));
  expect(model.inventory()).toHaveLength(2);
  const phenotype=model.compilePhenotype({model_family:'Qwen',token_roots:['t'],token_count:123,context_window:2048});
  expect(phenotype.weights.representation).toBe('unresolved');expect(phenotype.weights.inventory_hint).toBe('FPQ');expect(phenotype.weights.verified).toBe(false);expect(phenotype.authoritative).toBe(false);
});

test('native executable mutated during version probe cannot become durable inventory',async()=>{
  const {store,model}=fixture({run:async argv=>{writeFileSync(argv[0],'changed');return {exit_code:0,stdout:'version',stderr:'',duration_ms:0};}});
  const dir=join(store.projectRoot,'bin');mkdirSync(dir);copyFileSync(probe,join(dir,'llama-cli'));
  await expect(model.discoverModels()).rejects.toThrow('changed');expect(model.inventory()).toHaveLength(0);
});

test('hardware census uses bounded argv without shells and reports missing GPU probe as degraded',async()=>{
  const calls=[];const {model}=fixture({run:async(argv,options)=>{calls.push({argv,options});return {exit_code:argv[0]==='nvidia-smi'?127:0,stdout:'observed',stderr:'',duration_ms:0};}});
  const result=await model.hardwareCensus();expect(calls).toHaveLength(3);expect(calls.every(x=>!x.argv.includes('sh'))).toBe(true);
  expect(result.state).toBe('degraded');expect(result.python_architecture).toBeNull();expect(result.environment.metal_verified).toBe(false);
});

test('no denied or unhealthy node becomes selected merely by winning a negative score',()=>{
  const {store,model}=fixture();store.db.query('DELETE FROM placement_nodes').run();
  expect(()=>model.place({privacy_class:'internal',effect_class:'READ_ONLY'})).toThrow('no eligible');
  model.registerNode({node_id:'other-cloud',kind:'remote-server',trust:'provider',authority_zone:'candidate',capabilities:{gpu:true},health:'healthy'});
  expect(()=>model.place({privacy_class:'restricted',effect_class:'READ_ONLY',remote_allowed:false})).toThrow('no eligible');
  expect(()=>model.place({privacy_class:'public',effect_class:'EXTERNAL_REVERSIBLE_EFFECT'})).toThrow('no eligible');
  expect(()=>model.place({privacy_class:'public',effect_class:'READ_ONLY',requires_gpu:true})).toThrow('no eligible');
  model.registerNode({node_id:'local',kind:'workstation',trust:'owner',authority_zone:'authoritative',capabilities:{},health:'failed'});
  expect(()=>model.place({privacy_class:'public',effect_class:'EXTERNAL_REVERSIBLE_EFFECT',remote_allowed:false})).toThrow('no eligible');
  expect(store.db.query('SELECT count(*) n FROM placement_plans').get().n).toBe(0);
});

test('placement and fuse retain float digests, candidate authority gates and source binding',()=>{
  const {store,model}=fixture();const phenotype=model.compilePhenotype({mission_id:'mission',model_family:'qwen',token_roots:['root']});
  const placement=model.place({mission_id:'mission',phenotype_id:phenotype.phenotype_id,privacy_class:'internal',effect_class:'READ_ONLY'});
  expect(placement.selected_node).toBe('local');expect(placement.authoritative).toBe(false);
  const row=store.db.query('SELECT * FROM placement_plans WHERE id=?').get(placement.placement_id);
  expect(row.alternatives_json).toContain('10.0');
  const fused=model.fuse({mission_id:'mission',phenotype_id:phenotype.phenotype_id,placement_id:placement.placement_id});
  expect(fused.fuse.activation).toBe('candidate');expect(fused.fuse.requires_authority).toBe(true);
  expect(fused.fuse.distributed_ir_digest).toBe(parsedDocument(row.distributed_ir_json).calculated);
  expect(fused.axon.token_roots).toEqual(['root']);expect(store.verifyReceipts().verified).toBe(true);
  expect(()=>model.fuse({mission_id:'other',phenotype_id:phenotype.phenotype_id,placement_id:placement.placement_id})).toThrow('identity');
  store.db.query("UPDATE placement_plans SET selected_node='forged' WHERE id=?").run(placement.placement_id);
  expect(()=>model.fuse({mission_id:'mission',phenotype_id:phenotype.phenotype_id,placement_id:placement.placement_id})).toThrow('digest');
});

test('bounded planner inputs reject coercion, unknown effects and oversized roots',()=>{
  const {model}=fixture();expect(()=>model.compilePhenotype({model_family:'qwen',token_count:'12'})).toThrow();
  expect(()=>model.compilePhenotype({model_family:'qwen',token_roots:Array(10001).fill('root')})).toThrow();
  expect(()=>model.place({privacy_class:'internal',effect_class:'INVENTED'})).toThrow();
  expect(()=>model.registerNode({node_id:'x',kind:'x',trust:'x',authority_zone:'x',capabilities:[]})).toThrow();
});

test('phenotype corruption cannot be laundered into a fuse passport',()=>{
  const {store,model}=fixture();const p=model.compilePhenotype({model_family:'qwen'});
  const plan=model.place({phenotype_id:p.phenotype_id,privacy_class:'public',effect_class:'READ_ONLY'});
  const original=JSON.parse(readFileSync(p.path,'utf8'));expect(original.digest).toBe(p.digest);
  store.db.query('UPDATE phenotype_plans SET payload_json=? WHERE id=?').run('{"model_family":"forged"}',p.phenotype_id);
  expect(()=>model.fuse({phenotype_id:p.phenotype_id,placement_id:plan.placement_id})).toThrow('digest mismatch');
});
