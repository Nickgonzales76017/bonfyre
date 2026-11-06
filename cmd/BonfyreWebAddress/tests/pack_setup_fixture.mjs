import {fixture,root,password,decode} from './auth_continuation_fixture.mjs';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {Database} from 'bun:sqlite';
export {root,password,decode};
export async function packSetupFixture(options={}){
 const f=await fixture(options),schemas=join(f.home,'schemas');
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
 const close=f.close;return {...f,sql,authsql,hash,add,pack,form,login,grant,async close(){sql.close();authsql.close();await close();}};
}
