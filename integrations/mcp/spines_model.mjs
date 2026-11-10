import { createHash } from 'node:crypto';
import { accessSync, closeSync, constants, fstatSync, lstatSync, openSync, opendirSync, readSync, realpathSync } from 'node:fs';
import { arch, cpus, endianness, homedir, platform } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { jsonFloat, parsedDocument } from './canonical_json.mjs';

const MODELS = Object.freeze({
  BonfyreQwenFPQ:{kind:'local-llm',executables:['BonfyreQwenFPQ','bonfyre-qwen-fpq'],capabilities:['generation','fpq','kv-cache','structured-inference']},
  BonfyreMoE:{kind:'local-llm',capabilities:['generation','mixture-of-experts']},
  BonfyreEmbed:{kind:'embedding',capabilities:['embedding','vector']},
  BonfyreTranscribe:{kind:'speech',capabilities:['transcription','timestamps']},
  bonfyred:{kind:'runtime',capabilities:['native-command-fabric','authority-boundary']},
  'llama-server':{kind:'local-llm-server',capabilities:['generation','http']},
  'llama-cli':{kind:'local-llm-cli',capabilities:['generation']},
});
const EFFECTS = new Set(['PURE','READ_ONLY','CANDIDATE_WRITE','REVERSIBLE_INTERNAL_WRITE','AUTHORITATIVE_INTERNAL_WRITE','EXTERNAL_REVERSIBLE_EFFECT','EXTERNAL_IRREVERSIBLE_EFFECT']);
const sha = value => createHash('sha256').update(value).digest('hex');
const plain = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function string(value,label,nullable=false) {
  if(nullable && value===null)return value;
  if(typeof value!=='string'||!value||value.includes('\0')||Buffer.byteLength(value)>16384)throw new Error(`invalid ${label}`);return value;
}
function number(value,label,{integer=false,nullable=false,min=0,max=Number.MAX_SAFE_INTEGER}={}) {
  if(nullable&&value===null)return value;
  if(!Number.isFinite(value)||value<min||value>max||(integer&&!Number.isSafeInteger(value)))throw new Error(`invalid ${label}`);return value;
}
function roots(value) {
  if(!Array.isArray(value)||value.length>10000)throw new Error('invalid token roots');for(const id of value)string(id,'token root');return [...value];
}
function representationCut(value) {
  if(value==null)return null;
  if(!plain(value)||value.selected!==false||!Array.isArray(value.candidates)||value.candidates.length>256)throw new Error('invalid representation cut');
  const subject_family=string(value.subject_family,'representation subject family');
  const operation=string(value.operation,'representation operation');
  const law=string(value.law,'representation law');
  if(typeof value.requires_exact!=='boolean')throw new Error('invalid representation exactness requirement');
  const candidates=value.candidates.map((candidate,index)=>{
    if(!plain(candidate))throw new Error('invalid representation candidate '+index);
    const handle=number(candidate.handle,'representation candidate handle',{integer:true,min:1});
    const loss_budget=number(candidate.loss_budget,'representation candidate loss budget');
    const physical_bytes=number(candidate.physical_bytes,'representation candidate bytes',{integer:true});
    return {handle,
      identity:string(candidate.identity,'representation candidate identity'),
      representation_family:string(candidate.representation_family,'representation candidate family'),
      exactness:string(candidate.exactness,'representation candidate exactness'),
      measurement:string(candidate.measurement,'representation candidate measurement'),
      loss_budget,physical_bytes,
      lift_ref:string(candidate.lift_ref,'representation candidate lift ref')};
  });
  return {subject_family,operation,requires_exact:value.requires_exact,selected:false,law,candidates};
}
function passport(path) {
  path=realpathSync(path);const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try{
    const before=fstatSync(fd,{bigint:true});if(!before.isFile()||before.size>536870912n)throw new Error('executable exceeds bounded regular-file contract');
    const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let count=0,magic='';
    for(;;){const n=readSync(fd,buffer,0,buffer.length,null);if(!n)break;if(!count)magic=buffer.subarray(0,4).toString('hex');count+=n;if(count>536870912)throw new Error('executable grew beyond budget');hash.update(buffer.subarray(0,n));}
    const after=fstatSync(fd,{bigint:true}),named=lstatSync(path,{bigint:true});const key=s=>[s.dev,s.ino,s.size,s.mtimeNs,s.ctimeNs].join(':');
    if(count!==Number(before.size)||key(before)!==key(after)||key(after)!==key(named))throw new Error('executable changed during hashing');
    return {path,bytes:count,sha256:hash.digest('hex'),native:['7f454c46','feedface','feedfacf','cefaedfe','cffaedfe','cafebabe','bebafeca','cafebabf','bfbafeca'].includes(magic)};
  }finally{closeSync(fd);}
}

export function createModelEngine(store,{userHome=homedir(),pathValue=process.env.PATH||'',system=platform(),machine=arch(),environment=process.env}={}) {
  if(!store||typeof store.digest!=='function'||typeof store.canonical!=='function'||typeof store.run!=='function'||typeof store.backendStatus!=='function')throw new Error('invalid model store');
  const canonical=value=>store.canonical(value),digest=value=>store.digest(value),clone=value=>JSON.parse(canonical(value));
  const atomic=fn=>store.db.transaction(fn).immediate();
  const receipt=(kind,subject_kind,subject_id,state,payload)=>store.writeReceipt({release:'R7',kind,subject_kind,subject_id,state,payload});
  const queryRows=(sql)=>{
    const statement=store.db.prepare(sql),rows=[];let size=0;
    try{for(const row of statement.iterate()){size+=Object.values(row).reduce((n,v)=>n+(typeof v==='string'?Buffer.byteLength(v):16),0);if(rows.length>=10000||size>8*1024*1024)throw new Error('model query budget exceeded');rows.push(row);}}finally{statement.finalize();}return rows;
  };
  function executable(name,candidates=[]) {
    for(const path of [...candidates,...pathValue.split(':').filter(p=>p&&isAbsolute(p)).map(p=>join(p,name))]){
      try{accessSync(path,constants.X_OK);if(lstatSync(realpathSync(path)).isFile())return realpathSync(path);}catch{/* Absent candidates are not execution evidence. */}
    }return name;
  }
  async function run(argv,timeout) {
    const value=await store.run(argv,{timeout});
    if(!plain(value)||!Number.isInteger(value.exit_code)||typeof value.stdout!=='string'||typeof value.stderr!=='string'||Buffer.byteLength(value.stdout)+Buffer.byteLength(value.stderr)>2*1024*1024)throw new Error('invalid or excessive process receipt');
    return clone(value);
  }
  async function hardwareCensus(node_id='local') {
    string(node_id,'node');const commands=[];
    if(system==='darwin'){
      const sysctl=executable('sysctl',['/usr/sbin/sysctl','/sbin/sysctl']),profiler=executable('system_profiler',['/usr/sbin/system_profiler']);
      for(const [argv,timeout]of [[[sysctl,'-n','hw.model'],10],[[sysctl,'-n','hw.memsize'],10],[[profiler,'SPDisplaysDataType','-json'],30],[[profiler,'SPHardwareDataType','-json'],30]])commands.push(await run(argv,timeout));
    }else{
      commands.push(await run([executable('uname',['/usr/bin/uname']),'-a'],10));
      commands.push(await run([executable('head',['/usr/bin/head']),'-20','/proc/meminfo'],10));
      commands.push(await run([executable('nvidia-smi'),'-L'],15));
    }
    const state=commands.every(p=>p.exit_code===0&&!p.timeout&&!p.timed_out&&!p.cancelled&&!p.overflow)?'passed':'degraded';
    const body={schema:'bonfyre.hardware-census.v1',node_id,system:system==='darwin'?'Darwin':system==='linux'?'Linux':system,
      machine,processor:cpus()[0]?.model??'',python_architecture:null,runtime_architecture:[machine,'bun'],commands,
      environment:{metal:system==='darwin',metal_verified:false,cuda_visible_devices:environment.CUDA_VISIBLE_DEVICES??null,rocr_visible_devices:environment.ROCR_VISIBLE_DEVICES??null}};
    const hash=digest(body),id=sha(`${node_id}\0${hash}`);
    return atomic(()=>{store.db.query('INSERT OR REPLACE INTO hardware_census VALUES(?,?,?,?,?)').run(id,node_id,canonical(body),hash,store.now());
      return {census_id:id,digest:hash,...body,state,receipt:receipt('hardware.census','node',node_id,state,body)};});
  }
  function candidatePaths() {
    const found=new Map(Object.keys(MODELS).map(name=>[name,new Set()]));let entries=0,dirs=0;
    const aliases=new Map(Object.entries(MODELS).flatMap(([name,spec])=>(spec.executables??[name]).map(executableName=>[executableName,name])));
    const add=(name,path)=>{try{const actual=realpathSync(path);if(lstatSync(actual).isFile())found.get(name).add(actual);}catch{/* Missing path. */}};
    for(const [executableName,name] of aliases){const path=executable(executableName);if(isAbsolute(path))add(name,path);}
    const queue=[join(store.projectRoot,'build'),join(store.projectRoot,'bin'),join(store.projectRoot,'cmd'),join(store.projectRoot,'target/release'),join(store.projectRoot,'lib'),join(userHome,'.local/bin'),join(userHome,'BonfyreModels')].map(path=>({path,depth:0}));
    for(let cursor=0;cursor<queue.length;cursor++){
      const {path,depth}=queue[cursor];let directory;
      try{if(lstatSync(path).isSymbolicLink())continue;directory=opendirSync(path);}catch(error){if(['ENOENT','ENOTDIR'].includes(error.code))continue;throw error;}
      if(++dirs>4096){directory.closeSync();throw new Error('model discovery directory budget exceeded');}
      try{let entry;while((entry=directory.readSync())){if(++entries>20000)throw new Error('model discovery entry budget exceeded');const next=join(path,entry.name);
        const modelName=aliases.get(entry.name);
        if(modelName&&(entry.isFile()||entry.isSymbolicLink()))add(modelName,next);
        if(depth<2&&entry.isDirectory())queue.push({path:next,depth:depth+1});
      }}finally{directory.closeSync();}
    }
    if([...found.values()].reduce((n,set)=>n+set.size,0)>32)throw new Error('model discovery candidate budget exceeded');return found;
  }
  async function discoverModels() {
    const candidates=candidatePaths(),discovered=[];
    for(const [name,paths]of candidates)for(const path of [...paths].sort()){
      const file=passport(path);let probe;
      if(!file.native)probe={argv:[path,'--version'],exit_code:126,stdout:'',stderr:'unverified script is not executed by native inventory',duration_ms:0};
      else{probe=await run([path,'--version'],15);if(probe.exit_code!==0&&!probe.timeout&&!probe.timed_out&&!probe.cancelled&&!probe.overflow)probe=await run([path,'--help'],15);}
      if(passport(path).sha256!==file.sha256)throw new Error('model executable changed during probe');
      const body={name,path,kind:MODELS[name].kind,capabilities:[...MODELS[name].capabilities],version_probe:probe,bytes:file.bytes,file_sha256:file.sha256,
        evidence_scope:'executable-inventory-only',capabilities_verified:false,runtime_boundary:file.native?'native':'unverified-script-refused'};
      discovered.push({inventory_id:sha(`${name}\0${path}\0${file.sha256}`),...body});
    }
    return atomic(()=>{for(const {inventory_id,...body}of discovered)store.db.query('INSERT OR REPLACE INTO model_inventory VALUES(?,?,?,?,?,?,?,?)').run(inventory_id,body.name,body.path,body.kind,(body.version_probe.stdout+body.version_probe.stderr).trim().slice(0,4096),canonical(body.capabilities),digest(body),store.now());
      const state=discovered.some(item=>item.runtime_boundary!=='native'||item.version_probe.exit_code!==0||item.version_probe.timeout||item.version_probe.timed_out||item.version_probe.cancelled||item.version_probe.overflow)?'degraded':'passed';
      return {models:discovered,count:discovered.length,receipt:receipt('model.inventory.discover','model_inventory',null,state,{discovered})};});
  }
  function inventory(){return queryRows('SELECT * FROM model_inventory ORDER BY name,path LIMIT 10001').map(({capabilities_json,...row})=>({...row,capabilities:JSON.parse(capabilities_json)}));}
  function compilePhenotype({mission_id=null,model_family,token_roots=[],token_count=0,privacy_class='internal',latency_target_ms=null,quality_floor=null,context_window=null,representation_cut=null}){
    string(mission_id,'mission',true);string(model_family,'model family');string(privacy_class,'privacy');token_roots=roots(token_roots);number(token_count,'token count',{integer:true});
    number(latency_target_ms,'latency',{nullable:true,integer:true});number(quality_floor,'quality',{nullable:true});number(context_window,'context window',{nullable:true,integer:true,min:1});
    representation_cut=representationCut(representation_cut);
    const items=inventory(),preferred=items.find(item=>item.name.toLowerCase().includes(model_family.toLowerCase()))??items.find(item=>['local-llm','local-llm-server','local-llm-cli'].includes(item.kind))??null;
    const body={schema:'bonfyre.execution-phenotype-ir.v1',mission_id,model_family,selected_inventory:preferred,token_roots,token_count,
      context_plan:{mode:'gigatoken-closure',context_window,prefix_reuse:true,exact_tokenizer_binding_required:true},
      weights:{representation:'unresolved',
        candidate_representation:null,
        representation_cut,
        representation_cut_digest:representation_cut?digest(representation_cut):null,
        inventory_hint:preferred?.capabilities.includes('fpq')?'FPQ':null,
        content_addressed:true,verified:false,
        verification_status:representation_cut?'physical-compiler-candidate-cut-is-not-model-quality-admission':'inventory-capability-is-not-model-representation-evidence',
        representation_owner:'ModelRepresentationPassport',
        physical_owner:'PhysicalCompiler'},
      activations:{representation:'runtime-selected',sae_advisory_only:true},kv_cache:{enabled:true,identity_bound_to_token_roots:true,eviction_policy:'bounded-lru-candidate'},
      attention:{preferred:'FlashQLA',fallback:'reference'},kernel_backends:['bonfyre-reference-cpu',system==='darwin'?'Metal':'CPU','CubeCL-candidate'],privacy_class,latency_target_ms,quality_floor,
      fallback_lanes:['bonfyre-native','local-reference'],authoritative:false};
    const hash=digest(body),id=sha(`${mission_id??''}\0${model_family}\0${hash}`),path=join(store.modelRoot,`${id}.phenotype.json`);
    return atomic(()=>{store.writeJson(path,{...body,id,digest:hash});store.db.query('INSERT OR REPLACE INTO phenotype_plans VALUES(?,?,?,?,?,?,?)').run(id,mission_id,model_family,canonical(body),hash,path,store.now());
      return {phenotype_id:id,path,digest:hash,...body,receipt:receipt('phenotype.compile','phenotype',id,'passed',{path,...body})};});
  }
  async function kernelParity(backend='bonfyre-reference-cpu'){
    string(backend,'backend');const a=[1,2,3,4,5,6,7,8],b=[8,7,6,5,4,3,2,1];
    const dot=a.reduce((n,x,i)=>n+x*b[i],0),mean=a.reduce((n,x)=>n+x,0)/a.length,variance=a.reduce((n,x)=>n+(x-mean)**2,0)/a.length;
    const reference={dot,mean,variance,count:a.length},tolerances={absolute:1e-12,relative:1e-12};let candidate={},probe=null,state='blocked',probe_identity=null;
    if(backend==='bonfyre-reference-cpu'){
      const file=passport(store.kernelProbe);if(!file.native)throw new Error('kernel probe must be a native executable');probe_identity={path:file.path,sha256:file.sha256,bytes:file.bytes};
      probe=await run([file.path],30);if(passport(file.path).sha256!==file.sha256)throw new Error('kernel probe changed during execution');
      try{candidate=JSON.parse(probe.stdout);}catch{/* Invalid output fails parity. */}
      if(!plain(candidate))candidate={};
      const normalized=Buffer.alloc(64),inverse=1/Math.sqrt(variance+1e-5);for(let i=0;i<a.length;i++)normalized[endianness()==='LE'?'writeDoubleLE':'writeDoubleBE']((a[i]-mean)*inverse,i*8);
      state=probe.exit_code===0&&!probe.timeout&&!probe.timed_out&&!probe.cancelled&&!probe.overflow&&candidate.schema==='bonfyre.kernel-parity-sample.v1'&&candidate.count===a.length&&
        ['dot','mean','variance'].every(key=>typeof candidate[key]==='number'&&Number.isFinite(candidate[key])&&Math.abs(candidate[key]-reference[key])<=Math.max(tolerances.absolute,tolerances.relative*Math.max(Math.abs(candidate[key]),Math.abs(reference[key]))))&&candidate.layernorm_sha256===sha(normalized)?'passed':'failed';
    }
    const body={schema:'bonfyre.kernel-parity.v1',backend,state,reference,candidate,tolerances,probe,cubecl_adapter:store.backendStatus().cubecl,
      probe_identity,evidence_scope:'eight-element-native-cpu-reference-only',limitations:['No GPU execution, trained model accuracy, throughput, or general kernel equivalence is established.']};
    const hash=digest(body),id=sha(`${backend}\0${hash}`),path=join(store.modelRoot,`${id}.kernel-parity.json`);
    return atomic(()=>{store.writeJson(path,{...body,id,digest:hash});store.db.query('INSERT OR REPLACE INTO kernel_parity_runs VALUES(?,?,?,?,?,?,?,?,?)').run(id,backend,state,canonical(reference),canonical(candidate),canonical(tolerances),hash,path,store.now());
      return {parity_id:id,path,digest:hash,...body,receipt:receipt('kernel.parity','kernel_parity',id,state,body)};});
  }
  function registerNode({node_id,kind,trust,authority_zone,capabilities,constraints={},health='candidate'}){
    for(const value of [node_id,kind,trust,authority_zone,health])string(value,'node declaration');if(!plain(capabilities)||!plain(constraints))throw new Error('invalid node metadata');
    capabilities=clone(capabilities);constraints=clone(constraints);store.db.query('INSERT OR REPLACE INTO placement_nodes VALUES(?,?,?,?,?,?,?,?)').run(node_id,kind,trust,authority_zone,canonical(capabilities),canonical(constraints),health,store.now());
    return {node_id,kind,trust,authority_zone,capabilities,constraints,health};
  }
  function nodes(){return queryRows('SELECT * FROM placement_nodes ORDER BY id LIMIT 10001').map(({capabilities_json,constraints_json,...row})=>({...row,capabilities:JSON.parse(capabilities_json),constraints:JSON.parse(constraints_json)}));}
  function place({mission_id=null,phenotype_id=null,privacy_class,effect_class,data_bytes=0,requires_gpu=false,remote_allowed=true}){
    string(mission_id,'mission',true);string(phenotype_id,'phenotype',true);string(privacy_class,'privacy');if(!EFFECTS.has(effect_class))throw new Error('invalid effect class');number(data_bytes,'data bytes',{integer:true});
    if(typeof requires_gpu!=='boolean'||typeof remote_allowed!=='boolean')throw new Error('invalid placement flags');
    const scored=nodes().map(node=>{
      let score=0;const reasons=[];let eligible=true;
      if(node.id==='local'){score+=5;reasons.push('owner-controlled');if(['confidential','restricted','internal'].includes(privacy_class)){score+=5;reasons.push('privacy-locality');}}
      const remote=node.id==='oracle'||node.kind==='remote-server'||node.capabilities.remote_execution===true||node.constraints.network_required===true;
      if(remote&&!remote_allowed){score-=100;eligible=false;reasons.push('remote-disallowed');}else if(node.id==='oracle'){score+=3;reasons.push('durable-remote');}
      if(node.id==='browser'){if(data_bytes<=64*1024*1024&&['PURE','READ_ONLY'].includes(effect_class)){score+=2;reasons.push('small-readonly-edge');}else{score-=50;eligible=false;reasons.push('browser-effect-or-size-denied');}}
      if(requires_gpu&&node.capabilities.gpu_verified!==true){score-=10;eligible=false;reasons.push('gpu-unproven');}
      if(effect_class.startsWith('EXTERNAL')&&node.authority_zone!=='authoritative'){score-=100;eligible=false;reasons.push('external-effect-denied');}
      if(['failed','unhealthy'].includes(node.health)){score-=100;eligible=false;reasons.push('unhealthy');}
      return {node,score,reasons,eligible};
    }).sort((a,b)=>b.score-a.score||(a.node.id<b.node.id?-1:a.node.id>b.node.id?1:0));
    const selected=scored.find(item=>item.eligible);if(!selected)throw new Error('no eligible placement candidate');
    const alternatives=scored.map(item=>({node_id:item.node.id,score:jsonFloat(item.score),reasons:item.reasons,eligible:item.eligible}));
    const distributed={schema:'bonfyre.distributed-ir.v1',mission_id,processes:['model-runtime','proof-collector'],artifacts:phenotype_id?[phenotype_id]:[],
      authority_zones:scored.map(item=>item.node.authority_zone),placement_alternatives:alternatives,hydro_adapter:store.backendStatus().hydro};
    const body={mission_id,phenotype_id,selected_node:selected.node.id,alternatives,reason:selected.reasons.join(','),distributed_ir:distributed,authoritative:false};
    const hash=digest(body),id=sha(`${mission_id??''}\0${phenotype_id??''}\0${hash}`),path=join(store.placementRoot,`${id}.placement.json`);
    return atomic(()=>{store.writeJson(path,{...body,id,digest:hash});store.db.query('INSERT OR REPLACE INTO placement_plans VALUES(?,?,?,?,?,?,?,?,?,?)').run(id,mission_id,phenotype_id,selected.node.id,canonical(alternatives),body.reason,canonical(distributed),hash,path,store.now());
      return {placement_id:id,path,digest:hash,...clone(body),receipt:receipt('placement.compile','placement',id,'passed',{path,...body})};});
  }
  function fuse({mission_id=null,phenotype_id,placement_id,token_roots=[]}){
    string(mission_id,'mission',true);string(phenotype_id,'phenotype');string(placement_id,'placement');token_roots=roots(token_roots);
    return atomic(()=>{
    const phenotype=store.db.query('SELECT * FROM phenotype_plans WHERE id=?').get(phenotype_id),placement=store.db.query('SELECT * FROM placement_plans WHERE id=?').get(placement_id);
    if(!phenotype||!placement)throw new Error('phenotype or placement missing');
    if(placement.phenotype_id!==phenotype_id||phenotype.mission_id!==mission_id||placement.mission_id!==mission_id)throw new Error('fuse source identity mismatch');
    const typed=parsedDocument(phenotype.payload_json),distributed=parsedDocument(placement.distributed_ir_json);
    if(typed.calculated!==phenotype.digest)throw new Error('phenotype digest mismatch');
    const source=`{"mission_id":${canonical(placement.mission_id)},"phenotype_id":${canonical(placement.phenotype_id)},"selected_node":${canonical(placement.selected_node)},"alternatives":${placement.alternatives_json},"reason":${canonical(placement.reason)},"distributed_ir":${placement.distributed_ir_json},"authoritative":false}`;
    if(parsedDocument(source).calculated!==placement.digest)throw new Error('placement digest mismatch');
    const p=typed.value;
    const axon={schema:'bonfyre.axon.v1',model_family:p.model_family,
      weight_representation:p.weights.verified?p.weights.representation:'unresolved',
      candidate_weight_representation:p.weights.candidate_representation??null,
      representation_cut_digest:p.weights.representation_cut_digest??null,
      representation_candidate_ids:(p.weights.representation_cut?.candidates??[]).map(candidate=>candidate.identity),
      representation_verified:p.weights.verified===true,token_roots:token_roots.length?token_roots:p.token_roots??[],content_addressed:true,authoritative:false};
    const kore={schema:'bonfyre.kore.v1',kernels:p.kernel_backends,parity_required:true,hardware_target:placement.selected_node,fallback:'bonfyre-reference-cpu'};
    const fuse={schema:'bonfyre.fuse.v1',mission_id,phenotype_id,placement_id,axon_digest:digest(axon),kore_digest:digest(kore),distributed_ir_digest:distributed.calculated,activation:'candidate',requires_authority:true,requires_kernel_parity:true};
    const body={axon,kore,fuse},hash=digest(body),id=sha(`${phenotype_id}\0${placement_id}\0${hash}`),path=join(store.modelRoot,`${id}.fuse.json`);
    store.writeJson(path,{...body,id,digest:hash});store.db.query('INSERT OR REPLACE INTO fuse_plans VALUES(?,?,?,?,?,?,?,?,?,?)').run(id,mission_id,phenotype_id,placement_id,canonical(axon),canonical(kore),canonical(fuse),path,hash,store.now());
      return {fuse_id:id,path,digest:hash,...body,receipt:receipt('fuse.compile','fuse',id,'candidate',{path,...body})};});
  }
  return Object.freeze({hardwareCensus,discoverModels,inventory,compilePhenotype,kernelParity,registerNode,nodes,place,fuse});
}
