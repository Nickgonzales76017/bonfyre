// V6 resource observations and backend activation evidence. Discovery is not an
// authority grant; configured credentials and executable presence prove no capability.
import { createHash } from 'node:crypto';
import { constants, openSync, closeSync, readSync, fstatSync, lstatSync, readdirSync, realpathSync, existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve, sep } from 'node:path';
import { projectRun } from './project_runtime.mjs';
import { createNativeProbe, probeExecutable as executable, findProbeExecutable as which,
  isNativeProbeExecutable as nativeExecutable, nativeProbeReceipt as probeReceipt, nativeProbePassed as passed } from './native_probe.mjs';

export const V6_PROVIDERS = Object.freeze({
  openai: [['OPENAI_API_KEY'], ['openai'], ['model.generate','model.embed','model.audio']],
  anthropic: [['ANTHROPIC_API_KEY','ANTHROPIC_OAUTH_TOKEN'], ['claude'], ['model.generate','agent.code']],
  'google-ai': [['GEMINI_API_KEY','GOOGLE_API_KEY'], ['gcloud'], ['model.generate','model.embed']],
  'azure-openai': [['AZURE_OPENAI_API_KEY','AZURE_OPENAI_BASE_URL'], ['az'], ['model.generate','cloud.azure']],
  mistral: [['MISTRAL_API_KEY'], [], ['model.generate']], groq: [['GROQ_API_KEY'], [], ['model.generate']],
  cohere: [['COHERE_API_KEY'], [], ['model.generate','model.embed','model.rerank']],
  'hugging-face': [['HF_TOKEN','HUGGINGFACEHUB_API_TOKEN'], ['huggingface-cli','hf'], ['model.registry','dataset.registry','model.inference']],
  firecrawl: [['FIRECRAWL_API_KEY'], [], ['web.crawl','web.extract']],
  github: [['GITHUB_PAT','GITHUB_TOKEN','GITHUB_ACCESS_TOKEN'], ['gh'], ['source.git','ci.github','issues','pull_requests']],
  aws: [['AWS_PROFILE','AWS_ACCESS_KEY_ID','AWS_BEARER_TOKEN_BEDROCK'], ['aws'], ['cloud.aws','object.s3','model.bedrock']],
  oracle: [['BONFYRE_ORACLE_HOST','BONFYRE_ORACLE_BASE_URL','BONFYRE_ORACLE_TENANCY_OCID','ORACLE_HOST'], ['oci','ssh'], ['cloud.oracle','deployment.oracle']],
  cloudflare: [['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_ACCESS_KEY_ID'], ['wrangler','cloudflared'], ['cloud.cloudflare','object.r2','network.tunnel']],
  supabase: [['SUPABASE_ACCESS_TOKEN','SUPABASE_URL','SUPABASE_PROJECT_REF','SUPABASE_DB_URL'], ['supabase'], ['database.postgres','auth','storage','realtime']],
  neon: [['NEON_API_KEY','NEON_PROJECT_ID','NEON_DB_URL'], ['neonctl'], ['database.postgres']],
  turso: [['TURSO_API_TOKEN','TURSO_AUTH_TOKEN','TURSO_DATABASE_URL'], ['turso'], ['database.libsql','vector']],
  upstash: [['UPSTASH_PERSONAL_API','UPSTASH_REDIS_REST_URL','UPSTASH_REDIS_REST_TOKEN'], [], ['cache.redis','queue']],
  fly: [['FLY_API_TOKEN','BONFYRE_FLY_APP'], ['flyctl','fly'], ['cloud.fly','deployment']],
  vercel: [['VERCEL_TOKEN'], ['vercel'], ['cloud.vercel','deployment']], railway: [['RAILWAY_TOKEN'], ['railway'], ['cloud.railway','deployment']],
  render: [['RENDER_API_KEY'], [], ['cloud.render','deployment']], posthog: [['POSTHOG_API_KEY','POSTHOG_PROJECT_TOKEN'], [], ['telemetry.product','feature.analytics']],
  sentry: [['SENTRY_AUTH_TOKEN'], ['sentry-cli'], ['telemetry.errors','telemetry.performance']],
  twilio: [['TWILIO_ACCOUNT_SID','TWILIO_AUTH_TOKEN'], ['twilio'], ['communication.sms','communication.voice']],
  sendgrid: [['SENDGRID_API_KEY'], [], ['communication.email']], pexels: [['FFTS_PEXELS_API_KEY','PEXELS_API_KEY'], [], ['media.stock']],
  pixabay: [['FFTS_PIXABAY_API_KEY','PIXABAY_API_KEY'], [], ['media.stock']], giphy: [['FFTS_GIPHY_API_KEY','GIPHY_API_KEY'], [], ['media.gif']],
  runpod: [['RUNPOD_API_KEY'], ['runpodctl'], ['gpu.compute','model.inference']],
  langfuse: [['LANGFUSE_PUBLIC_KEY','LANGFUSE_SECRET_KEY','LANGFUSE_HOST'], [], ['telemetry.llm','trace']],
  stripe: [['STRIPE_SECRET_KEY'], ['stripe'], ['payments']], slack: [['SLACK_BOT_TOKEN','SLACK_APP_TOKEN'], [], ['communication.chat']],
  discord: [['DISCORD_BOT_TOKEN'], [], ['communication.chat']], telegram: [['TELEGRAM_BOT_TOKEN'], [], ['communication.chat']],
  pinecone: [['PINECONE_API_KEY'], [], ['vector.database']], weaviate: [['WEAVIATE_API_KEY','WEAVIATE_URL'], [], ['vector.database']],
  qdrant: [['QDRANT_API_KEY','QDRANT_URL'], [], ['vector.database']], typesense: [['TYPESENSE_API_KEY','TYPESENSE_HOST'], [], ['search']],
  minio: [['MINIO_ROOT_USER','MINIO_ROOT_PASSWORD','MINIO_ENDPOINT'], ['mc'], ['object.storage']],
});
const BACKEND_ROWS = [
  ['feldera',['feldera','pipeline-manager'],['vendor/feldera','integrations/feldera','external/feldera'],'incremental-standing-views','.flux/.delta→.graf'],
  ['egglog',['egglog'],['vendor/egglog','integrations/egglog','external/egglog'],'equivalence-saturation','.graf→selected .helix'],
  ['hvm4',['hvm4','hvm'],['vendor/hvm4','integrations/hvm4','bonfyre-zig/output/hvm4-runtime'],'pure-recursive-reduction','.helix→.crux witness'],
  ['hydro',['hydro','hydro_cli'],['vendor/hydro','integrations/hydro','external/hydro'],'distributed-placement','DistributedIR→placement'],
  ['cubecl',['cubecl'],['vendor/cubecl','integrations/cubecl','external/cubecl'],'portable-kernel-compiler','KernelIR→.kore'],
  ['gigatoken',['gigatoken','bonfyre-gigatoken'],['vendor/gigatoken','Gigatoken'],'exact-token-artifacts','source→TokenArtifact'],
  ['yaff',['yaff'],['vendor/yaff','YaFF','bonfyre-hyper/src/serm/yaff','serm/yaff'],'layout-and-format-compiler','artifacts→layout'],
  ['lance',['lance'],['vendor/lance','integrations/lance','Aurekai/generated/integration-repos/lance'],'multimodal-lakehouse','media/documents→artifact fabric'],
  ['daft',['daft'],['vendor/daft','integrations/daft','Aurekai/generated/integration-repos/daft'],'multimodal-transform','raw sources→typed artifacts'],
  ['automerge',['automerge'],['vendor/automerge','integrations/automerge','Aurekai/generated/integration-repos/automerge'],'candidate-local-first-state','draft changes→candidate .delta'],
  ['restate',['restate'],['vendor/restate','integrations/restate','Aurekai/generated/integration-repos/restate'],'durability-parity','workflow replay/recovery'],
  ['verus',['verus'],['vendor/verus','integrations/verus'],'narrow-formal-verification','reader/validator proofs'],
  ['tract',['tract'],['vendor/tract','integrations/tract','Aurekai/generated/integration-repos/tract'],'model-import-and-edge-inference','ONNX/NNEF→.axon'],
  ['burn',['burn'],['vendor/burn','integrations/burn','Aurekai/generated/integration-repos/burn'],'rust-model-compiler','ModelIR→.axon/.kore'],
];
export const V6_BACKENDS = Object.freeze(Object.fromEntries(BACKEND_ROWS.map(([id,binaries,sources,role,stage]) => [id,{binaries,sources,role,stage}])));
const CORE = new Set(['feldera','egglog','hvm4','hydro','cubecl']);
const MANIFEST_CORE = new Set(Object.keys(V6_BACKENDS).filter(id => id !== 'hvm4'));
const CLI_PROBES = {
  github: ['gh','auth','status'], aws: ['aws','sts','get-caller-identity','--output','json'], cloudflare: ['wrangler','whoami'],
  fly: ['flyctl','auth','whoami'], vercel: ['vercel','whoami'], supabase: ['supabase','projects','list','--output','json'],
  turso: ['turso','auth','whoami'], sentry: ['sentry-cli','info'], runpod: ['runpodctl','config','view'],
};
const SKIP = new Set(['.git','node_modules','.venv','venv','target','__pycache__','.cache']);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const digestPattern = /^[0-9a-f]{64}$/;
const secretLike = name => /KEY|TOKEN|SECRET|PASSWORD|PASS|CREDENTIAL|PRIVATE|AUTH|OCID|FINGERPRINT/.test(name);
const abort = signal => signal?.throwIfAborted();
const identity = info => [info.dev,info.ino,info.size,info.mtimeNs,info.ctimeNs].join(':');

// Bound bytes and bind a named regular file to one descriptor. A changed file
// never receives a digest of a mixture of generations.
function inspectFile(path, { maximum = 8 * 1024 * 1024, content = true } = {}) {
  let fd;
  try {
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const before = fstatSync(fd,{bigint:true});
    if (!before.isFile() || before.size > BigInt(maximum)) throw new Error('file budget or regular-file contract');
    const hash = createHash('sha256'), chunks = [], buffer = Buffer.alloc(Math.min(Number(before.size) || 1, 1024 * 1024));
    let total = 0;
    while (total < Number(before.size)) {
      const size = readSync(fd,buffer,0,Math.min(buffer.length,Number(before.size)-total),null);
      if (!size) throw new Error('truncated file');
      const bytes = buffer.subarray(0,size); hash.update(bytes); if(content) chunks.push(Buffer.from(bytes)); total += size;
    }
    const after = fstatSync(fd,{bigint:true}), named = lstatSync(path,{bigint:true});
    if (!named.isFile() || identity(before)!==identity(after) || identity(after)!==identity(named)) throw new Error('file changed');
    return { bytes: total, sha256: hash.digest('hex'), executable: Boolean(Number(before.mode)&0o111), ...(content ? { data:Buffer.concat(chunks) } : {}) };
  } finally { if(fd!==undefined)closeSync(fd); }
}
function entries(path, maximum = 10000) {
  let names;
  try { names=readdirSync(path,{withFileTypes:true}); } catch(error) { if(error.code==='ENOENT')return []; throw error; }
  if(names.length>maximum)throw new Error('directory entry budget exceeded');
  return names.sort((a,b)=>a.name<b.name?-1:a.name>b.name?1:0);
}

export function createV6Resources({store,root,home=store?.home,userHome=homedir(),env=process.env,run=projectRun,request=fetch,now=()=>Date.now()/1000}={}) {
  if(!store||typeof store.digest!=='function'||!root||!home)throw new Error('v6 resource owner dependencies missing');
  root=resolve(root);home=resolve(home);userHome=resolve(userHome);
  const expand=path=>resolve(path.startsWith('~/')?join(userHome,path.slice(2)):path);
  const sealed=body=>({...body,digest:store.digest(body)});
  const write=(path,body)=>store.writeJson(join(home,path),body);
  function loadValues() {
    const values=Object.assign(Object.create(null),env),sources=Object.fromEntries(Object.keys(values).map(key=>[key,['process']]));
    const paths=new Set(),visited=new Set();let walked=0;
    function walk(path,depth=0) {
      if(depth>12)throw new Error('credential discovery depth budget exceeded');
      if(visited.has(path)||!existsSync(path))return;visited.add(path);
      if(lstatSync(path).isSymbolicLink())return;
      for(const entry of entries(path)) {
        if(++walked>30000)throw new Error('credential discovery entry budget exceeded');
        if(SKIP.has(entry.name)||entry.isSymbolicLink())continue;
        const full=join(path,entry.name);
        if(entry.isDirectory())walk(full,depth+1);
        else if(entry.isFile()&&(entry.name==='.env'||entry.name.startsWith('.env.')||entry.name.endsWith('.env'))) {
          paths.add(full);if(paths.size>250)throw new Error('credential file count exceeded');
        }
      }
    }
    for(const base of [root,join(userHome,'.config'),join(userHome,'.bonfyre')])walk(base);
    for(const entry of entries(userHome))if(entry.isFile()&&entry.name.startsWith('.env'))paths.add(join(userHome,entry.name));
    if(paths.size>250)throw new Error('credential file count exceeded');
    for(const path of [...paths].sort()) {
      const text=inspectFile(path,{maximum:1024*1024}).data.toString('utf8');
      for(let line of text.split(/\r?\n/)) {
        line=line.trim().replace(/^export\s+/,'');
        const match=/^([A-Z][A-Z0-9_]{2,})\s*=(.*)$/.exec(line);if(!match)continue;
        const name=match[1];let value=match[2].trim();
        if(value.length>=2&&['"',"'"].includes(value[0])&&value.at(-1)===value[0])value=value.slice(1,-1);
        if(!value||value.startsWith('$(')||value.startsWith('`'))continue;
        if(!Object.hasOwn(values,name))values[name]=value;
        (sources[name] ||= []).push(path);
      }
    }
    return {values,sources};
  }
  const execute=createNativeProbe({run,env,cwd:root});
  async function discoverResources({probe_external=true,signal}={}) {
    if(typeof probe_external!=='boolean')throw new Error('probe_external must be boolean');abort(signal);
    const {values,sources}=loadValues(),resources=[],edges=[];
    for(const [provider,[names,commands,capabilities]] of Object.entries(V6_PROVIDERS)) {
      abort(signal);const present_env=names.filter(name=>values[name]);
      const command_paths=Object.fromEntries(commands.map(name=>[name,which(name,env)]).filter(([,path])=>path));
      const probe=probe_external&&CLI_PROBES[provider]&&which(CLI_PROBES[provider][0],env)?await execute(CLI_PROBES[provider],{signal}):null;
      // config view proves local configuration, not remote reachability.
      if(probe&&provider==='runpod')probe.semantic_scope='local-configuration-only';
      const reachable=passed(probe)&&provider!=='runpod';
      const doc=sealed({id:`provider:${provider}`,provider,kind:'external-provider',state:reachable?'reachable':present_env.length||Object.keys(command_paths).length?'configured':'missing',
        capabilities,evidence:{env_names:present_env,env_sources:Object.fromEntries(present_env.map(name=>[name,sources[name]||[]])),commands:command_paths,probe,secret_values_recorded:false}});
      resources.push(doc);for(const cap of capabilities)edges.push({source:doc.id,target:`capability:${cap}`,kind:'PROVIDES'});
      for(const name of present_env)edges.push({source:`env:${name}`,target:doc.id,kind:'CONFIGURES'});
    }
    const claimed=new Set(Object.values(V6_PROVIDERS).flatMap(([names])=>names));
    for(const name of Object.keys(values).sort()) {
      if(!values[name]||!/^[A-Z][A-Z0-9_]{2,}$/.test(name))continue;
      const runtime=/^(BONFYRE_|AKAI_|AUREKAI_|FFTS_)/.test(name);
      if(!runtime&&(claimed.has(name)||!secretLike(name)||/^(SSH_|GPG_|XPC_|CAAS_|PYTHON_|PIP_|NPM_)/.test(name)))continue;
      resources.push(sealed({id:`${runtime?'env':'env-unmapped'}:${name}`,provider:runtime?'bonfyre-runtime':name.split('_')[0].toLowerCase(),
        kind:runtime?'environment-contract':'unmapped-environment-contract',state:'present',capabilities:[runtime?'runtime.configuration':'external.configuration.unmapped'],
        evidence:{...(runtime?{}:{env_name:name}),sources:sources[name]||[],secret_like:secretLike(name),value_recorded:false}}));
    }
    const integrationRoot=join(root,'Aurekai/generated/integration-repos');
    const integrations=entries(integrationRoot).filter(entry=>entry.isDirectory());
    for(const entry of integrations){const id=`integration:${entry.name}`;resources.push(sealed({id,provider:entry.name,kind:'aurekai-integration',state:'source-present',capabilities:['integration.adapter'],evidence:{path:join(integrationRoot,entry.name)}}));edges.push({source:id,target:'fabric:aurekai',kind:'MEMBER_OF'});}
    const summary={providers:Object.keys(V6_PROVIDERS).length,configured:resources.filter(r=>r.kind==='external-provider'&&r.state!=='missing').length,
      reachable:resources.filter(r=>r.state==='reachable').length,bonfyre_env_contracts:resources.filter(r=>r.kind==='environment-contract').length,
      unmapped_external_env_contracts:resources.filter(r=>r.kind==='unmapped-environment-contract').length,integrations:integrations.length};
    const graph=sealed({schema:'bonfyre.resource-graph.v1',generated_at:now(),resources,edges,summary});abort(signal);
    for(const doc of resources)store.upsertResource(doc);write('resources/resource-graph.json',graph);store.setMeta('resource_graph_summary',summary);
    store.receipt('v6.1.resource-discovery','passed',{digest:graph.digest,summary});return graph;
  }
  function providerRequest(provider,values) {
    const first=(...names)=>names.map(name=>values[name]).find(Boolean);
    if(provider==='cloudflare'){
      const key=first('CLOUDFLARE_API_TOKEN'),account=first('CLOUDFLARE_ACCOUNT_ID');
      if(!key||(account&&!/^[a-f0-9]{32}$/i.test(account)))return null;
      const url=account?`https://api.cloudflare.com/client/v4/accounts/${account}/tokens/verify`:
        'https://api.cloudflare.com/client/v4/user/tokens/verify';
      return {url,headers:{Authorization:`Bearer ${key}`},require_active_token:true};
    }
    const bearer={openai:['https://api.openai.com/v1/models',['OPENAI_API_KEY']],groq:['https://api.groq.com/openai/v1/models',['GROQ_API_KEY']],
      mistral:['https://api.mistral.ai/v1/models',['MISTRAL_API_KEY']],cohere:['https://api.cohere.com/v1/models',['COHERE_API_KEY']],
      'hugging-face':['https://huggingface.co/api/whoami-v2',['HF_TOKEN','HUGGINGFACEHUB_API_TOKEN']],github:['https://api.github.com/user',['GITHUB_PAT','GITHUB_TOKEN','GITHUB_ACCESS_TOKEN']]};
    if(bearer[provider]){const [url,names]=bearer[provider],key=first(...names);return key?{url,headers:{Authorization:`Bearer ${key}`,...(provider==='github'?{Accept:'application/vnd.github+json'}:{})}}:null;}
    let key;
    if(provider==='anthropic'&&(key=first('ANTHROPIC_API_KEY')))return {url:'https://api.anthropic.com/v1/models?limit=1',headers:{'x-api-key':key,'anthropic-version':'2023-06-01'}};
    if(provider==='google-ai'&&(key=first('GEMINI_API_KEY','GOOGLE_API_KEY')))return {url:`https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(key)}`};
    if(provider==='pexels'&&(key=first('FFTS_PEXELS_API_KEY','PEXELS_API_KEY')))return {url:'https://api.pexels.com/v1/search?query=sky&per_page=1',headers:{Authorization:key}};
    if(provider==='pixabay'&&(key=first('FFTS_PIXABAY_API_KEY','PIXABAY_API_KEY')))return {url:`https://pixabay.com/api/?key=${encodeURIComponent(key)}&q=sky&per_page=3`};
    if(provider==='giphy'&&(key=first('FFTS_GIPHY_API_KEY','GIPHY_API_KEY')))return {url:`https://api.giphy.com/v1/gifs/search?api_key=${encodeURIComponent(key)}&q=sky&limit=1`};
    if(provider==='posthog'&&(key=first('POSTHOG_HOST')))return {url:key.replace(/\/$/,'')+'/health'};
    if(provider==='langfuse'&&(key=first('LANGFUSE_HOST')))return {url:key.replace(/\/$/,'')+'/api/public/health'};
    return null;
  }
  async function httpProbe(spec,{signal}={}) {
    const started=performance.now();let endpoint=null,response,reader;
    try {
      const url=new URL(spec.url);
      if(url.protocol!=='https:'||url.username||url.password||url.hash)throw new Error('unapproved HTTP endpoint');
      endpoint=`${url.origin}${url.pathname}`;abort(signal);
      const deadline=AbortSignal.timeout(20000),combined=signal?AbortSignal.any([deadline,signal]):deadline;
      response=await request(url,{method:'GET',headers:spec.headers||{},redirect:'error',signal:combined});
      const hash=createHash('sha256');let bytes=0;
      const maximum=response.ok?2*1024*1024:128*1024;
      if(Number(response.headers.get('content-length'))>maximum)throw new Error('response exceeds budget');
      const verification=spec.require_active_token&&response.ok?response.clone():null;
      reader=response.body?.getReader();
      if(reader)while(true){combined.throwIfAborted();const item=await reader.read();if(item.done)break;bytes+=item.value.byteLength;if(bytes>maximum)throw new Error('response exceeds budget');hash.update(item.value);}
      if(spec.require_active_token){
        let active=false;
        if(verification)try{const value=await verification.json();
          active=value?.success===true&&value.result?.status==='active';
        }catch{/* never classify malformed or disabled credentials as validated */}
        const ok=response.ok&&active;
        return {endpoint,status:response.status,ok,bytes,body_sha256:hash.digest('hex'),
          duration_ms:Math.round(performance.now()-started),
          ...(ok?{}:{error:response.ok?'TokenInactiveOrMalformed':'HTTPError'})};
      }
      return {endpoint,status:response.status,ok:response.ok,bytes,body_sha256:hash.digest('hex'),duration_ms:Math.round(performance.now()-started),...(response.ok?{}:{error:'HTTPError'})};
    } catch {abort(signal);return {endpoint,status:response?.status??null,ok:false,bytes:0,body_sha256:null,duration_ms:Math.round(performance.now()-started),error:'ProbeRefusedOrFailed'};}
    finally {try{await reader?.cancel();if(!reader)await response?.body?.cancel();}catch{/* no raw diagnostics */}}
  }
  async function activateProviders({graph,signal}={}) {
    abort(signal);graph=graph?structuredClone(graph):JSON.parse(inspectFile(join(home,'resources/resource-graph.json')).data);
    if(!object(graph)||!Array.isArray(graph.resources)||graph.resources.length>10000||!object(graph.summary))throw new Error('invalid resource graph');
    const {values}=loadValues(),results=[];
    for(const resource of graph.resources) {
      if(resource.kind!=='external-provider')continue;
      if(!Object.hasOwn(V6_PROVIDERS,resource.provider))throw new Error('unknown provider in graph');
      const provider=resource.provider,prior=resource.state,cli=resource.evidence?.probe;
      const spec=prior!=='missing'&&env.BONFYRE_V61_DISABLE_HTTP_PROBES!=='1'?providerRequest(provider,values):null;
      const hp=spec?await httpProbe(spec,{signal}):null,cliOk=passed(cli)&&provider!=='runpod';
      // An old validated observation cannot survive a newly failed live probe.
      const state=hp?.ok?'validated':cliOk?'reachable':prior==='missing'?'missing':'configured';
      const result=sealed({provider,prior_state:prior,state,http_probe:hp,cli_probe_ok:cliOk,secret_values_recorded:false});results.push(result);
      resource.state=state;resource.evidence ||= {};resource.evidence.activation={state,http_probe:hp,cli_probe_ok:cliOk,secret_values_recorded:false};
      // Imported historical graph output may contain credentials; discard it.
      if(resource.evidence.probe){const old=resource.evidence.probe;resource.evidence.probe={exit_code:Number.isSafeInteger(old.exit_code)?old.exit_code:127,
        timed_out:Boolean(old.timed_out||old.timeout),overflow:Boolean(old.overflow),cancelled:Boolean(old.cancelled),
        ...(old.refused?{refused:'historical-probe-refused'}:{}),stdout:'',stderr:'',output_redacted:true};}
      delete resource.digest;resource.digest=store.digest(resource);
    }
    graph.summary.validated=results.filter(r=>r.state==='validated').length;graph.summary.reachable_or_validated=results.filter(r=>['reachable','validated'].includes(r.state)).length;
    graph.activation=results;delete graph.digest;graph.digest=store.digest(graph);
    const summary={configured:graph.summary.configured||0,validated:graph.summary.validated,reachable_or_validated:graph.summary.reachable_or_validated,providers_probed:results.filter(r=>r.http_probe||r.cli_probe_ok).length};
    const result={schema:'bonfyre.provider-activation.v1',state:summary.reachable_or_validated?'passed':'partial',summary,results,resource_graph_digest:graph.digest};abort(signal);
    for(const resource of graph.resources.filter(r=>r.kind==='external-provider'))store.upsertResource(resource);
    write('resources/resource-graph.json',graph);write('resources/provider-activation.json',result);store.setMeta('provider_activation_summary',summary);store.setMeta('resource_graph_summary',graph.summary);
    store.receipt('v6.1.provider-activation',result.state,{digest:graph.digest,summary});return result;
  }
  async function verifyActivationManifest(path,{expectedId,signal}={}) {
    const result={valid:false,path,id:expectedId??null,failures:[]},fail=reason=>result.failures.push(reason);abort(signal);
    let manifest,raw;
    try {raw=inspectFile(path).data.toString('utf8');manifest=JSON.parse(raw);}catch{fail('manifest-read');return result;}
    if(!object(manifest)){fail('manifest-shape');return result;}result.manifest=manifest;result.id=manifest.id;
    if(manifest.schema!=='bonfyre.substrate-activation.v1')fail('schema');
    if(expectedId&&manifest.id!==expectedId)fail('id');if(!MANIFEST_CORE.has(manifest.id))fail('unsupported-id');
    if(manifest.state!=='active')fail('state');if(manifest.evidence_class==='synthetic')fail('synthetic-evidence-not-live');
    let calculated;
    try{if(typeof store.parseDocument!=='function')throw new Error('raw canonical parser missing');calculated=store.parseDocument(raw,'manifest_digest').calculated;}
    catch{fail('manifest-canonical-form');return result;}
    result.manifest_digest=calculated;if(manifest.manifest_digest!==calculated)fail('manifest-digest');
    if(!object(manifest.upstream)||typeof manifest.upstream.url!=='string'||!manifest.upstream.url||typeof manifest.upstream.identity!=='string'||!manifest.upstream.identity)fail('upstream-identity');
    else{try{const url=new URL(manifest.upstream.url);if(!['https:','http:'].includes(url.protocol)||url.username||url.password||url.search||url.hash)fail('upstream-url');}catch{fail('upstream-url');}}
    if(!object(manifest.semantic_probe)||manifest.semantic_probe.state!=='passed')fail('semantic-probe');
    else if(typeof manifest.semantic_probe.kind!=='string'||!manifest.semantic_probe.kind||!digestPattern.test(manifest.semantic_probe.output_digest))fail('semantic-probe-evidence');
    if(!Array.isArray(manifest.commands)||!manifest.commands.length||manifest.commands.length>4096)fail('commands');
    else if(!manifest.commands.some(command=>object(command)&&command.exit_code===0&&!command.timed_out&&!command.timeout&&!command.overflow&&!command.cancelled&&!command.refused))fail('no-successful-command');
    if(typeof manifest.execution_mode!=='string'||!manifest.execution_mode||manifest.execution_mode.length>256)fail('execution-mode');
    const base=realpathSync(dirname(path));let total=0;const checked=[],seen=new Set();
    if(!Array.isArray(manifest.artifacts)||!manifest.artifacts.length||manifest.artifacts.length>1024)fail('artifacts');
    else for(const [index,artifact] of manifest.artifacts.entries()) {
      abort(signal);
      try {
        if(!object(artifact)||typeof artifact.path!=='string'||!artifact.path||artifact.path.length>4096||!Number.isSafeInteger(artifact.bytes)||artifact.bytes<0||!digestPattern.test(artifact.sha256))throw new Error('shape');
        const named=resolve(base,artifact.path),actual=realpathSync(named);
        if(!actual.startsWith(base+sep)||lstatSync(named).isSymbolicLink())throw new Error('escape');
        if(seen.has(actual))throw new Error('duplicate');seen.add(actual);
        total+=artifact.bytes;if(total>4*1024**3)throw new Error('total-budget');
        const file=inspectFile(named,{maximum:1024**3,content:false});
        if(realpathSync(named)!==actual)throw new Error('escape');
        if(file.bytes!==artifact.bytes)throw new Error('bytes');if(file.sha256!==artifact.sha256)throw new Error('sha256');
        if(artifact.executable===true&&!file.executable)throw new Error('executable');checked.push({path:actual,bytes:file.bytes,sha256:file.sha256});
      }catch(error){fail(`artifact-${index}:${['shape','escape','duplicate','total-budget','bytes','sha256','executable'].includes(error.message)?error.message:'file-refused'}`);}
    }
    result.artifacts=checked;
    if(typeof manifest.completed_at!=='number'||!Number.isFinite(manifest.completed_at)||manifest.completed_at<=0||manifest.completed_at>now()+300)fail('completed-at');
    // Recheck manifest after hashing its potentially large body set.
    try{if(inspectFile(path).data.toString('utf8')!==raw)fail('manifest-changed');}catch{fail('manifest-changed');}
    result.valid=!result.failures.length;return result;
  }
  function findSources(name,spec) {
    const candidates=[];const override=env[`BONFYRE_${name.toUpperCase()}_SOURCE`];if(override)candidates.push(expand(override));
    candidates.push(...spec.sources.map(path=>join(root,path)));
    for(const base of [root,...['vendor','integrations','external','third_party','Aurekai/generated/integration-repos'].map(p=>join(root,p))])
      for(const entry of entries(base))if(entry.isDirectory()&&[name,name.replaceAll('-','_'),name.replaceAll('_','-')].includes(entry.name.toLowerCase()))candidates.push(join(base,entry.name));
    return [...new Set(candidates.filter(path=>{try{return lstatSync(path).isDirectory();}catch{return false;}}).map(path=>realpathSync(path)))].slice(0,16);
  }
  async function sourceActivate(name,source,{signal}={}) {
    const probes=[],packages=[];let selected=null;
    if(existsSync(join(source,'Cargo.toml'))&&which('cargo',env)) {
      // Keep metadata only in memory; never persist arbitrary build-tool output.
      const cargo=which('cargo',env);let metadata;
      if(!nativeExecutable(cargo))return {source,packages,selected_package:null,probes:[{refused:'native-executable-required'}],executed:false};
      abort(signal);
      try {const result=await run([cargo,'metadata','--offline','--locked','--no-deps','--format-version','1'],{cwd:source,env,signal,timeoutMs:120000,maxOutputBytes:2*1024*1024});
        abort(signal);probes.push(probeReceipt([cargo,'metadata','--offline','--locked','--no-deps','--format-version','1'],result));
        if(passed(probes.at(-1)))metadata=JSON.parse(result.stdout);
      }catch{abort(signal);probes.push({refused:'cargo-metadata-failed'});}
      if(Array.isArray(metadata?.packages)&&metadata.packages.length<=4096)for(const pkg of metadata.packages)if(typeof pkg.name==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(pkg.name))packages.push(pkg.name);
      selected=['hvm4','hvm'].flatMap(prefix=>packages.filter(pkg=>pkg===prefix||pkg.startsWith(prefix+'-')))[0]||null;
      if(selected){probes.push(await execute(['cargo','build','--offline','--release','--locked','-p',selected],{cwd:source,timeoutMs:2400000,signal}));
        if(passed(probes.at(-1)))probes.push(await execute(['cargo','test','--offline','--release','--locked','-p',selected,'--lib','--','--test-threads=1'],{cwd:source,timeoutMs:2400000,signal}));}
      else probes.push({refused:'no-exact-backend-package'});
    }else if(basename(source).toLowerCase()===name&&existsSync(join(source,'build.zig')))probes.push(await execute(['zig','build','-Doptimize=ReleaseSafe'],{cwd:source,timeoutMs:2400000,signal}));
    else if(basename(source).toLowerCase()===name&&existsSync(join(source,'Makefile')))probes.push(await execute(['make','-j2'],{cwd:source,timeoutMs:2400000,signal}));
    else probes.push({refused:'no-exact-backend-build-entrypoint'});
    return {source,packages,selected_package:selected,probes,executed:probes.length>0&&probes.every(passed)};
  }
  function builtBinary(source,names) {
    let walked=0;const found=[];
    function walk(dir,depth=0){if(depth>8)throw new Error('build search depth exceeded');for(const entry of entries(dir)){
      if(++walked>20000)throw new Error('build search entry budget exceeded');const path=join(dir,entry.name);
      if(entry.isDirectory())walk(path,depth+1);else if(entry.isFile()&&names.includes(entry.name)&&executable(path))found.push(path);
    }}
    for(const relative of ['target/release','zig-out/bin','build','bin'])walk(join(source,relative));
    return found.sort((a,b)=>a.length-b.length||a.localeCompare(b))[0]||null;
  }
  async function discoverBackends({build=false,signal}={}) {
    if(typeof build!=='boolean')throw new Error('build must be boolean');abort(signal);
    const activationRoot=expand(env.BONFYRE_V61_SUBSTRATE_ROOT||join(userHome,'.bonfyre/substrates/v6.1')),docs=[];
    for(const [name,spec] of Object.entries(V6_BACKENDS)) {
      abort(signal);const override=env[`BONFYRE_${name.toUpperCase()}_BIN`];
      let binary=override?executable(expand(override)):spec.binaries.map(bin=>which(bin,env)).find(Boolean)||null;
      let sources=findSources(name,spec),activation=null,verification=null,manifest=null,probe=null;
      const manifestPath=join(activationRoot,name,'activation.json');
      if(MANIFEST_CORE.has(name)) {
        verification=await verifyActivationManifest(manifestPath,{expectedId:name,signal});
        if(verification.valid){manifest=verification.manifest;sources=[dirname(manifestPath)];const item=manifest.artifacts.find(a=>a.executable===true);
          if(item)binary=resolve(dirname(manifestPath),item.path);
          probe={state:manifest.semantic_probe.state,kind:manifest.semantic_probe.kind,output_digest:manifest.semantic_probe.output_digest};}
      }else if(!binary&&build&&sources.length){activation=await sourceActivate(name,sources[0],{signal});binary=builtBinary(sources[0],spec.binaries);}
      if(!probe&&binary){probe=await execute([binary,'--version'],{timeoutMs:60000,signal});if(!passed(probe)&&!probe.refused)probe=await execute([binary,'--help'],{timeoutMs:60000,signal});probe.semantic_scope='version-and-executable-only';}
      const nativePresent=passed(probe),sourceActive=activation?.executed===true;
      // HVM --help/version and a generic build are observations, not reduction
      // parity. They remain source-backed until an independently bound proof exists.
      const state=manifest?'active':sources.length||nativePresent||sourceActive?'source-backed':'missing';
      const doc=sealed({id:name,state,role:spec.role,stage:spec.stage,binary,sources,probe,source_activation:activation,
        activation_manifest:MANIFEST_CORE.has(name)?manifestPath:null,
        activation_verification:verification?{valid:verification.valid,failures:verification.failures,manifest_digest:verification.manifest_digest||null}:null,
        upstream:manifest?{url:manifest.upstream.url,identity:manifest.upstream.identity}:null,
        execution_mode:manifest?manifest.execution_mode:nativePresent?'upstream-binary-observed':sourceActive?'upstream-source-built':sources.length?'source-only':'unavailable'});docs.push(doc);
    }
    const summary={total:docs.length,active:docs.filter(d=>d.state==='active').length,source_backed:docs.filter(d=>d.state==='source-backed').length,missing:docs.filter(d=>d.state==='missing').length,
      core_five:Object.fromEntries(docs.filter(d=>CORE.has(d.id)).map(d=>[d.id,d.state])),core_five_modes:Object.fromEntries(docs.filter(d=>CORE.has(d.id)).map(d=>[d.id,d.execution_mode])),
      core_activation_manifests:Object.fromEntries(docs.filter(d=>MANIFEST_CORE.has(d.id)).map(d=>[d.id,d.activation_verification?.valid===true]))};
    const output=sealed({schema:'bonfyre.backend-fabric.v2',generated_at:now(),activation_root:activationRoot,discovery_contract:'Exact source roots; live non-synthetic activation manifests bind semantic evidence and resident artifact digests. Binary/help/build observations alone are not semantic activation.',synthetic_evidence_allowed:false,backends:docs,summary});abort(signal);
    for(const doc of docs)store.upsertBackend(doc);write('backends/backend-fabric.json',output);store.setMeta('backend_summary',summary);
    store.receipt('v6.1.backend-activation',Object.values(summary.core_five).every(s=>s==='active')?'passed':'failed',{digest:output.digest,summary});return output;
  }
  return {discoverResources,activateProviders,discoverBackends,verifyActivationManifest};
}
