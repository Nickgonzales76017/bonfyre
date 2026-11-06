import {execFileSync} from 'node:child_process';
import {resolve} from 'node:path';
import {createHash} from 'node:crypto';

const failure=(status,message)=>Object.assign(Error(message),{status});
const institutionName=value=>{if(typeof value!=='string'||!/^[a-z0-9-]{1,63}$/.test(value))throw failure(400,'Invalid organization permission scope.');return value;};
const tokenValue=value=>{if(typeof value!=='string'||!/^bfy_[a-f0-9]{64}$/.test(value))throw failure(401,'Sign in to your account.');return value;};

// Native Auth owns accounts, sessions, actor bindings and institutional grants.
// Product adapters supply their declared permission scope; this adapter grants none.
export function accountAuth(db,{binary=resolve(import.meta.dir,'../../cmd/BonfyreAuth/bonfyre-auth')}={}){
  db=resolve(db);
  function run(command,input,...args){
    try{return JSON.parse(execFileSync(binary,['--db',db,command,...args],{input,encoding:'utf8',stdio:['pipe','pipe','pipe'],timeout:5000,maxBuffer:64*1024}));}
    catch(error){let detail;try{detail=JSON.parse(String(error.stdout));}catch{}if([400,401,403,409,500].includes(detail?.status)&&typeof detail.error==='string')throw failure(detail.status,detail.error);if(command==='actor-closure-stdin'&&detail?.schema==='bonfyre-actor-closure.v1'&&detail.authenticated===false)throw failure(detail.error==='no_active_actor_binding'?403:401,detail.error==='no_active_actor_binding'?'This account needs an actor binding.':'Sign in to your account.');if(command==='institution-authorize-stdin'&&detail?.authorized===false)throw failure(403,'Organization permission was refused.');throw failure(503,'Account services are unavailable.');}
  }
  function principal(token){
    const p=run('actor-closure-stdin',tokenValue(token));
    if(!p.authenticated||typeof p.actor_subject!=='string'||!p.actor_subject||!Number.isSafeInteger(p.auth_principal?.user_id))throw failure(401,'Sign in with a linked account.');
    return {actor:p.actor_subject,userId:p.auth_principal.user_id,worker:'actor-'+createHash('sha256').update(p.actor_subject).digest('hex').slice(0,48)};
  }
  return {
    login(input){
      if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>!['email','password'].includes(k))||typeof input.email!=='string'||!input.email.trim()||input.email.length>300||typeof input.password!=='string'||!input.password||input.password.length>1000)throw failure(400,'Enter your email and password.');
      const result=run('http-login',JSON.stringify(input)),token=result.session?.token;
      if(typeof token!=='string'||!/^bfy_[a-f0-9]{64}$/.test(token))throw failure(403,'Sign-in failed.');
      try{return {token,...principal(token)};}catch(error){try{run('logout-stdin',token);}catch{}throw error;}
    },
    principal,
    allowed(token,institution,role){
      tokenValue(token);institutionName(institution);if(!['read','write','admin'].includes(role))throw failure(400,'Invalid permission level.');
      try{return run('institution-authorize-stdin',token,'--institution',institution,'--role',role).authorized===true;}
      catch(error){if(error.status===401||error.status===403)return false;throw error;}
    },
    profile:token=>run('account-profile-stdin',tokenValue(token)),
    members(token,institution,after=0){institutionName(institution);if(!Number.isSafeInteger(after)||after<0||after>2147483647)throw failure(400,'Invalid member cursor.');return run('institution-members-stdin',tokenValue(token),'--institution',institution,'--after-id',String(after));},
    setMember(token,institution,input){
      institutionName(institution);
      if(!input||typeof input!=='object'||Array.isArray(input)||typeof input.email!=='string'||input.email.length>300||!input.email.includes('@')||!['read','write','admin','none'].includes(input.role)||Object.keys(input).some(k=>!['email','role','expected'].includes(k)))throw failure(400,'Choose an existing account and access level.');
      const before=input.expected;
      if(Object.hasOwn(input,'expected')&&before!==null&&(!before||typeof before!=='object'||Array.isArray(before)||Object.keys(before).sort().join(',')!=='expires_at,role'||!['read','write','admin'].includes(before.role)||(before.expires_at!==null&&(typeof before.expires_at!=='string'||!before.expires_at||before.expires_at.length>63))))throw failure(400,'Invalid expected access state.');
      const expected=Object.hasOwn(input,'expected')?(before===null?['--expected-role','none']:['--expected-role',before.role,'--expected-expiry',before.expires_at??'']):[];
      return run('institution-member-set-stdin',tokenValue(token),'--institution',institution,'--email',input.email,'--role',input.role,...expected);
    },
    logout:token=>run('logout-stdin',tokenValue(token))
  };
}
