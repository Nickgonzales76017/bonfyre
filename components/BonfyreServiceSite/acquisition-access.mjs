const prefix='/api/acquisition',cookieName='aurekai_acquisition';
const failure=(status,message)=>Object.assign(new Error(message),{status});
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const tokenPattern=/^bfy_[a-f0-9]{64}$/;
const idPattern=/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

function organizationRegistry(organizations){
  if(!Array.isArray(organizations))throw Error('Configure an acquisition organization registry.');
  const ids=new Set(),institutions=new Set();
  return organizations.map(org=>{
    if(!object(org)||Object.keys(org).sort().join(',')!=='id,institution,name'||!idPattern.test(org.id??'')||typeof org.id!=='string'||typeof org.institution!=='string'||!idPattern.test(org.institution)||typeof org.name!=='string'||!org.name.trim()||org.name.length>160||/[\u0000-\u001f\u007f]/.test(org.name)||ids.has(org.id)||institutions.has(org.institution))throw Error('Invalid or duplicate acquisition organization.');
    ids.add(org.id);institutions.add(org.institution);
    return Object.freeze({id:org.id,name:org.name,institution:org.institution});
  });
}
function principal(auth,token){
  const value=auth.principal(token);
  if(!value||typeof value.actor!=='string'||!value.actor.trim()||value.actor.length>512||/[\u0000-\u001f\u007f]/.test(value.actor)||!Number.isSafeInteger(value.userId)||value.userId<1)throw failure(401,'Sign in with a linked account.');
  return {actor:value.actor,userId:value.userId};
}
async function bodyJSON(request,maxBytes){
  const length=request.headers.get('content-length');
  if(length!==null&&(!/^\d+$/.test(length)||Number(length)>maxBytes))throw failure(413,'Request body is too large.');
  const reader=request.body?.getReader();if(!reader)throw failure(400,'Provide a JSON request body.');
  let size=0;const chunks=[];
  try{
    for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>maxBytes)throw failure(413,'Request body is too large.');chunks.push(value);}
  }finally{try{await reader.cancel();}catch{}}
  try{return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(chunks)));}catch{throw failure(400,'Invalid JSON.');}
}

/** Native account authority and revisioned storage are supplied by the server. */
export function acquisitionHandler({auth=null,store=null,organizations=[]}={}){
  const registry=organizationRegistry(organizations),byId=new Map(registry.map(org=>[org.id,org]));
  let attempts=[];
  return async request=>{
    const url=new URL(request.url);
    if(url.pathname!==prefix&&!url.pathname.startsWith(prefix+'/'))return null;
    const json=(value,status=200,headers={})=>new Response(JSON.stringify(value),{status,headers:{'Content-Type':'application/json','Cache-Control':'no-store','X-Content-Type-Options':'nosniff',...headers}});
    const cookie=(token,age)=>`${cookieName}=${token}; Path=${prefix}; HttpOnly; SameSite=Strict; Max-Age=${age}${url.protocol==='https:'?'; Secure':''}`;
    try{
      if(!auth||!store)throw failure(503,'Account workspace access is not configured.');
      const route=url.pathname.slice(prefix.length),draft=route.match(/^\/organizations\/([a-z0-9-]+)\/draft$/);
      const methods=route==='/login'||route==='/logout'?['POST']:route==='/session'?['GET']:draft?['GET','POST']:[];
      if(!methods.length)throw failure(404,'Not found.');
      if(!methods.includes(request.method))return json({error:'Method not allowed.'},405,{Allow:methods.join(', ')});
      if(request.method==='POST'){
        if(request.headers.get('origin')!==url.origin)throw failure(403,'Request origin does not match this workspace.');
        if(request.headers.get('content-type')?.split(';')[0].trim().toLowerCase()!=='application/json')throw failure(415,'Use application/json.');
      }
      const cookies=(request.headers.get('cookie')??'').split(';').map(value=>value.trim()).filter(value=>value.startsWith(cookieName+'='));
      if(cookies.length>1)throw failure(401,'Clear the duplicate workspace cookie and sign in again.');
      if(route==='/login'){
        const now=Date.now();attempts=attempts.filter(at=>at>now-60000);
        if(attempts.length>=10)return json({error:'Wait a minute before trying again.'},429,{'Retry-After':'60'});
        attempts.push(now);
        const input=await bodyJSON(request,8192);
        if(!object(input)||Object.keys(input).sort().join(',')!=='email,password'||typeof input.email!=='string'||!input.email.trim()||input.email.length>300||typeof input.password!=='string'||!input.password||input.password.length>1000)throw failure(400,'Enter your email and password.');
        let signed;
        try{signed=auth.login(input);}catch(error){throw failure([400,401,403,429].includes(error.status)?error.status:503,'Sign-in failed.');}
        if(!tokenPattern.test(signed?.token??''))throw failure(503,'Sign-in failed.');
        let current;
        try{current=principal(auth,signed.token);if(current.actor!==signed.actor||current.userId!==signed.userId)throw failure(401,'Sign-in identity changed.');}
        catch(error){try{auth.logout(signed.token);}catch{}throw error;}
        return json(current,200,{'Set-Cookie':cookie(signed.token,604800)});
      }
      const token=cookies.length===1?cookies[0].slice(cookieName.length+1):'';
      if(!tokenPattern.test(token))throw failure(401,'Sign in to your workspace.');
      if(route==='/logout'){
        const input=await bodyJSON(request,8192);
        if(!object(input)||Object.keys(input).length)throw failure(400,'Unexpected sign-out fields.');
        auth.logout(token);
        return json({signedOut:true},200,{'Set-Cookie':cookie('',0)});
      }
      const actor=principal(auth,token);
      if(route==='/session'){
        const profile=auth.profile(token);
        if(!profile||profile.id!==actor.userId||typeof profile.name!=='string'||typeof profile.email!=='string')throw failure(401,'Account profile does not match this session.');
        return json({...actor,profile:{id:profile.id,name:profile.name,email:profile.email},organizations:registry.filter(org=>auth.allowed(token,org.institution,'read')===true).map(org=>({id:org.id,name:org.name,canRead:true,canWrite:auth.allowed(token,org.institution,'write')===true,canAdmin:auth.allowed(token,org.institution,'admin')===true}))});
      }
      const org=byId.get(draft[1]),role=request.method==='POST'?'write':'read';
      if(!org||auth.allowed(token,org.institution,role)!==true)throw failure(403,'You do not have permission for this organization.');
      if(request.method==='GET')return json(await store.read(org.id));
      const input=await bodyJSON(request,2*1024*1024);
      if(!object(input)||Object.keys(input).sort().join(',')!=='expectedRevision,value'||!Number.isSafeInteger(input.expectedRevision)||input.expectedRevision<0||!object(input.value))throw failure(400,'Provide a workspace and its expected revision.');
      const current=principal(auth,token);
      if(current.actor!==actor.actor||current.userId!==actor.userId||auth.allowed(token,org.institution,'write')!==true)throw failure(403,'Your workspace permission changed.');
      return json(await store.write(org.id,actor.actor,input.expectedRevision,input.value));
    }catch(error){
      const status=[400,401,403,404,409,413,415,429,503].includes(error.status)?error.status:500;
      return json({error:status===500?'The workspace operation failed.':error.message},status);
    }
  };
}
