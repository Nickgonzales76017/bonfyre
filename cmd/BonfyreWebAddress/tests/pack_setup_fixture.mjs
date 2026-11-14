import {fixture,root,password,decode} from './auth_continuation_fixture.mjs';
import {copyFileSync,existsSync,writeFileSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {createFabricStore} from '../../../integrations/mcp/fabric_store.mjs';
import {createFabricActivityBoundary} from '../../../integrations/mcp/fabric_activity.mjs';
import {Database} from 'bun:sqlite';
export {root,password,decode};
export async function packSetupFixture(options={}){
 let fabricStore=null,fabricServer=null;
 const beforeServe=async context=>{
  if(options.beforeServe)return options.beforeServe(context);
  const {home,env,run}=context;
  const queue=process.env.BONFYRE_TEST_QUEUE??join(root,'cmd/BonfyreQueue/bonfyre-queue');
  if(!existsSync(queue))throw Error('Build the real BonfyreQueue binary before account authoring acceptance.');
  // A suite that seeds its own SourceFrontier passes it here; otherwise an empty one.
  const frontier=options.frontier??join(home,'source-frontier.db'),operator=join(home,'operator.db'),meter=join(home,'meter.db'),index=join(home,'fixture-account-work.db');
  if(!options.frontier)new Database(frontier,{create:true}).close();
  const operations=new Database(operator,{create:true});
  operations.exec("CREATE TABLE goals(id TEXT,title TEXT,state TEXT,created_at REAL,updated_at REAL,source TEXT,details TEXT,last_error TEXT,dedupe_key TEXT,priority INTEGER);CREATE TABLE runs(id TEXT,goal_id TEXT,state TEXT,started_at REAL,completed_at REAL,summary TEXT,usage_json TEXT);CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT,tool TEXT,state TEXT,started_at REAL,duration_ms INTEGER);");
  operations.close();
  const measurements=new Database(meter,{create:true});
  measurements.exec('CREATE TABLE events(key_id TEXT,op TEXT,provider TEXT,session_ref TEXT,counter_basis TEXT,uncached_input_tokens INTEGER,cached_input_tokens INTEGER,cache_write_tokens INTEGER,output_tokens INTEGER)');
  measurements.close();
  fabricStore=createFabricStore({home:join(home,'fabric'),projectRoot:root});
  fabricStore.activityRead({actor:'user:1',subject:'user:1',scope:'self'},{execution:()=>({owned:false,queued:false})});
  const sources=['operator='+operator,'fabric='+fabricStore.dbPath,'meter='+meter];
  run(queue,['account-work','index','--db',index,'--subject','user:1','--scope','self','--owner','user:1',...sources.flatMap(s=>['--source',s])]);
  const token=randomBytes(32).toString('hex'),tokenFile=join(home,'fabric-token');
  writeFileSync(tokenFile,token,{mode:0o600,flag:'wx'});
  const fabricActivity=createFabricActivityBoundary({token,
   read:(binding,options)=>fabricStore.activityRead(binding,{...options,execution:()=>({owned:false,queued:false})}),
   notification:(binding,input)=>fabricStore.notificationAction(binding,input),
   push:(binding,operation)=>{if(operation!=='push-status')throw Error('Fixture has no delivery authority');
    return {source:'fabric',context:binding.scope==='self'?'self':binding.subject,binding,configured:false,paused:true,reason:'Delivery is not installed in this isolated fixture.',publicKey:null,keyId:null,...fabricStore.push.status(binding)};}});
  fabricServer=Bun.serve({hostname:'127.0.0.1',port:0,fetch:request=>new URL(request.url).pathname==='/activity'?fabricActivity(request):new Response('Not found',{status:404})});
  Object.assign(env,{BONFYRE_BIN_DIR:dirname(queue),BONFYRE_FLEMING_ROOT:home,BONFYRE_ACCOUNT_WORK_DB:index,BONFYRE_ACCOUNT_WORK_OWNER:'user:1',BONFYRE_ACCOUNT_WORK_SOURCES:sources.join(';'),
   BONFYRE_BUN_BINARY:process.execPath,BONFYRE_SOURCE_FRONTIER_DB:frontier,BONFYRE_WEB_WORK_OWNER:join(root,'cmd/BonfyreSourceFrontier/tools/web_work.mjs'),
   BONFYRE_ACTIVITY_OWNER:join(root,'integrations/mcp/fabric_activity_cli.mjs'),BONFYRE_MCP_TOKEN_FILE:tokenFile,BONFYRE_FABRIC_HOST:'127.0.0.1',BONFYRE_FABRIC_PORT:String(fabricServer.port),
   BONFYRE_EXTERNAL_READS_ENABLED:'0'});
  if(options.beforeServe)await options.beforeServe(context);
 };
 let f;try{f=await fixture({beforeServe});}catch(error){fabricServer?.stop(true);fabricStore?.close();throw error;}
 const schemas=join(f.home,'schemas');
 for(const type of ['pack_artifact','pack_version','pack_head','pack_dependency','pack_overlay','pack_demand','capability_program','pack_installation','pack_activation'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 const sql=new Database(f.db),authsql=new Database(f.authdb),hash=s=>createHash('sha256').update(s).digest('hex');
 function add(type,value,ns='root'){return f.run(f.cms,['entry','create',type,JSON.stringify(value),'--ns',ns,'--db',f.db,'--schemas',schemas]);}
 function pack(id,name=id,layer='official'){
  add('pack_artifact',{pack:id,canonical_name:name,publisher:'fixture-publisher',layer,kind:'base',purpose:'A fictional reusable configuration for the native setup test.',governed_by:'fixture-owner',cannot_infer:'Configuration fixture, not a complete customer application.',status:'published'});
  add('pack_version',{pack:id,version:'1',root:hash(id+':1'),changed:'Initial fixture',status:'published'});
  add('pack_head',{pack:id,channel:'stable',version:'1',advanced_by:'fixture-owner',status:'published'});
 }
 pack('shared-material','Shared source selection');pack('creator-work','Creator workspace');pack('community-reuse','Community reuse','community');
 for(const name of ['creator-work','community-reuse'])add('pack_dependency',{pack:name,depends_on:'shared-material',channel:'stable',why:'Reuse the same public behavior',status:'published'});
 add('pack_demand',{pack:'creator-work',demand_kind:'renderer',needs:'thread',status:'published'});
 add('pack_overlay',{overlay:'community-reuse',overrides:'shared-material',layer:'community',verb:'tighten',target:'audience',value:'self',why:'Keep member working material private',status:'published'});
 function form(html,operation){const forms=[...html.matchAll(/<form\b[^>]*>([\s\S]*?)<\/form>/g)];const found=forms.find(m=>m[1].includes(`data-pack-action="${operation}"`));if(!found)throw Error('Missing '+operation+' form: '+html.slice(-1200));return Object.fromEntries([...found[1].matchAll(/<input\b[^>]*name="([^"]*)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));}
 async function login(email,{signup=true}={}){if(signup)await f.request('/signup',{form:{name:'Configuration user',email,password}});const r=await f.request('/login',{form:{email,password,next:'/packs/setup'}});const cookie=r.headers.get('set-cookie')?.split(';')[0];if(!cookie)throw Error('Native login failed');return {cookie,id:authsql.query('SELECT id FROM users WHERE email=?').get(email).id};}
 function grant(id,institution,role){return f.run(f.auth,['--db',f.authdb,'institution-grant','--user-id',String(id),'--institution',institution,'--role',role]);}
 const close=f.close;return {...f,sql,authsql,hash,add,pack,form,login,grant,async close(){sql.close();authsql.close();fabricServer?.stop(true);fabricStore?.close();await close();}};
}
