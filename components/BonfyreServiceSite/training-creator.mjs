import {accountAuth} from './account-auth.mjs';
import {boundedText} from './training-assist-server.mjs';
import {programInstitution} from './training-publishing.mjs';
import {presentationInstitution} from './training-presentation.mjs';

const prefix='/api/training/creator',cookieName='training_creator';
const teamScopes=['requests','programs','appearance'];
const scopeInstitution=(id,scope)=>scope==='appearance'?presentationInstitution(id):scope==='programs'?programInstitution(id):creatorInstitution(id);
const failure=(status,message)=>Object.assign(Error(message),{status});
export const creatorInstitution=id=>{const name=`training-${id}-requests`;if(!/^[a-z0-9-]{1,63}$/.test(name))throw Error('Studio ID is too long for a native request grant.');return name;};
export function creatorAuth(db,options={}){
  const native=accountAuth(db,options);
  return {...native,
    principal(token){try{return native.principal(token);}catch(error){if(error.status===401)throw failure(403,'Sign-in or studio permission was refused.');throw error;}},
    allowed(token,id,role,scope='requests'){if(!teamScopes.includes(scope))return false;return native.allowed(token,scopeInstitution(id,scope),role);},
    members(token,id,scope,after=0){if(!teamScopes.includes(scope))throw failure(400,'Invalid studio permission scope.');return native.members(token,scopeInstitution(id,scope),after);},
    setMember(token,id,scope,input){if(!teamScopes.includes(scope))throw failure(400,'Invalid studio permission scope.');return native.setMember(token,scopeInstitution(id,scope),input);}
  };
}
export function creatorHandler(material,{auth,requests,publishing=null,presentation=null,plans=null}){
  for(const c of material.creators)creatorInstitution(c.id);
  let attempts=[];
  return async request=>{
    const url=new URL(request.url);if(!url.pathname.startsWith(prefix+'/'))return null;
    const json=(body,status=200,extra={})=>new Response(JSON.stringify(body),{status,headers:{'Content-Type':'application/json','Cache-Control':'no-store','X-Content-Type-Options':'nosniff',...extra}});
    const cookie=(token,age)=>`${cookieName}=${token}; Path=${prefix}; HttpOnly; SameSite=Strict; Max-Age=${age}${url.protocol==='https:'?'; Secure':''}`;
    try{
      if(!auth||!requests)throw failure(503,'Creator access is not configured.');
      if(!['GET','POST'].includes(request.method))throw failure(405,'Method not allowed.');
      if(request.method==='POST'){
        if(request.headers.get('origin')!==url.origin)throw failure(403,'Request origin does not match this studio.');
        if(request.headers.get('content-type')?.split(';')[0]!=='application/json')throw failure(415,'Use application/json.');
      }
      const route=url.pathname.slice(prefix.length);
      if(route==='/login'&&request.method==='POST'){
        attempts=attempts.filter(at=>at>Date.now()-60000);if(attempts.length>=10)throw failure(429,'Wait a minute before trying again.');attempts.push(Date.now());
        const signed=auth.login(JSON.parse(await boundedText(request,8192)));
        return json({actor:signed.actor},200,{'Set-Cookie':cookie(signed.token,604800)});
      }
      const cookies=(request.headers.get('cookie')??'').split(';').map(s=>s.trim()).filter(s=>s.startsWith(cookieName+'='));
      const token=cookies.length===1?cookies[0].slice(cookieName.length+1):'';
      if(!/^bfy_[a-f0-9]{64}$/.test(token))throw failure(401,'Sign in to your creator studio.');
      if(route==='/logout'&&request.method==='POST'){auth.logout(token);return json({signedOut:true},200,{'Set-Cookie':cookie('',0)});}
      const actor=auth.principal(token);
      if(route==='/session'&&request.method==='GET')return json({...actor,profile:auth.profile(token),plansAvailable:!!plans,studios:material.creators.map(c=>{let brand=c.settings.brand;try{brand=presentation?.read(c.id).brand??brand;}catch{/* A broken public style must not block unrelated creator work. */}return {id:c.id,name:brand.name,brand,adminScopes:teamScopes.filter(scope=>auth.allowed(token,c.id,'admin',scope)),canReadRequests:auth.allowed(token,c.id,'read'),canWrite:auth.allowed(token,c.id,'write'),canReadPrograms:auth.allowed(token,c.id,'read','programs'),canPublishPrograms:auth.allowed(token,c.id,'write','programs'),canReadAppearance:auth.allowed(token,c.id,'read','appearance'),canEditAppearance:auth.allowed(token,c.id,'write','appearance')};}).filter(c=>c.canReadRequests||c.canReadPrograms||c.canReadAppearance)});
      const teamRoute=route.match(/^\/studios\/([a-z0-9-]+)\/team\/(requests|programs|appearance)$/);
      if(teamRoute){
        const [,studio,scope]=teamRoute;
        if(!material.creators.some(c=>c.id===studio)||!auth.allowed(token,studio,'admin',scope))throw failure(403,'You do not administer this part of the studio.');
        if(request.method==='GET'){const after=url.searchParams.get('after')??'0';if(!/^(0|[1-9][0-9]{0,9})$/.test(after)||Number(after)>2147483647)throw failure(400,'Invalid member cursor.');return json(auth.members(token,studio,scope,Number(after)));}
        const input=JSON.parse(await boundedText(request,2048));
        if(!input||Array.isArray(input)||!['email,role','email,expected,role'].includes(Object.keys(input).sort().join(','))||typeof input.email!=='string'||input.email.length>300||!input.email.includes('@')||!['read','write','admin','none'].includes(input.role))throw failure(400,'Choose an existing account email and an access level.');
        if(Object.hasOwn(input,'expected')&&input.expected!==null){const expected=input.expected;if(!expected||Array.isArray(expected)||Object.keys(expected).sort().join(',')!=='expires_at,role'||!['read','write','admin'].includes(expected.role)||(expected.expires_at!==null&&(typeof expected.expires_at!=='string'||!expected.expires_at.length||expected.expires_at.length>63)))throw failure(400,'Invalid expected access state.');}
        const current=auth.principal(token);
        if(current.actor!==actor.actor||!auth.allowed(token,studio,'admin',scope))throw failure(403,'Your administrator permission changed.');
        return json(auth.setMember(token,studio,scope,input));
      }
      const appearance=route.match(/^\/studios\/([a-z0-9-]+)\/appearance$/);
      if(appearance){const studio=appearance[1],role=request.method==='POST'?'write':'read';
        if(!material.creators.some(c=>c.id===studio)||!auth.allowed(token,studio,role,'appearance'))throw failure(403,'You do not have appearance permission for this studio.');
        if(!presentation)throw failure(503,'Studio appearance is not configured.');
        if(request.method==='GET')return json(presentation.read(studio));
        const input=JSON.parse(await boundedText(request,1500*1024)),current=auth.principal(token);
        if(current.actor!==actor.actor||!auth.allowed(token,studio,'write','appearance'))throw failure(403,'Your appearance permission changed.');
        return json(presentation.save(studio,actor.actor,input));
      }
      const planRoute=route.match(/^\/studios\/([a-z0-9-]+)\/(plans|plan-drafts|plan-routines)(?:\/([a-f0-9-]+)(?:\/(withdraw))?)?$/);
      if(planRoute){const [,studio,kind,id,withdraw]=planRoute,role=request.method==='POST'?'write':'read';
        if(!material.creators.some(c=>c.id===studio)||!auth.allowed(token,studio,role,'programs'))throw failure(403,'You do not have program library permission for this studio.');
        if(!plans)throw failure(503,'Training plans are not configured.');
        if(request.method==='GET'&&!withdraw){if(kind==='plan-routines'&&!id)return json(plans.choices(studio));if(kind==='plan-drafts')return json(id?plans.readDraft(studio,id):plans.drafts(studio));if(kind==='plans'&&!id)return json(plans.list(studio));}
        if(request.method!=='POST'||kind==='plan-routines'||(kind==='plan-drafts'&&(!id||withdraw))||(kind==='plans'&&!!id!==!!withdraw))throw failure(405,'Choose a supported plan action.');
        const input=JSON.parse(await boundedText(request,128*1024)),current=auth.principal(token);
        if(current.actor!==actor.actor||!auth.allowed(token,studio,'write','programs'))throw failure(403,'Your program library permission changed.');
        if(kind==='plan-drafts')return json(plans.saveDraft(studio,actor.actor,id,input));
        if(withdraw){if(!input||Object.keys(input).length)throw failure(400,'Unexpected withdrawal fields.');return json(plans.withdraw(studio,Number(id)));}
        return json(plans.publish(studio,actor.actor,input));
      }
      const libraryDraft=route.match(/^\/studios\/([a-z0-9-]+)\/program-drafts(?:\/([a-f0-9-]+))?$/);
      if(libraryDraft){
        const [,studio,id]=libraryDraft,role=request.method==='POST'?'write':'read';
        if(!material.creators.some(c=>c.id===studio)||!auth.allowed(token,studio,role,'programs'))throw failure(403,'You do not have program library permission for this studio.');
        if(!publishing)throw failure(503,'Studio publishing is not configured.');
        if(request.method==='GET')return json(id?publishing.readDraft(studio,id):publishing.drafts(studio));
        if(!id)throw failure(405,'Choose a program draft.');
        const input=JSON.parse(await boundedText(request,128*1024)),current=auth.principal(token);
        if(current.actor!==actor.actor||!auth.allowed(token,studio,'write','programs'))throw failure(403,'Your program library permission changed.');
        return json(publishing.saveDraft(studio,actor.actor,id,input));
      }
      const publication=route.match(/^\/studios\/([a-z0-9-]+)\/programs(?:\/(\d+)\/withdraw)?$/);
      if(publication){
        const [,studio,releaseId]=publication,role=request.method==='POST'?'write':'read';
        if(!material.creators.some(c=>c.id===studio)||!auth.allowed(token,studio,role,'programs'))throw failure(403,'You do not have publishing permission for this studio.');
        if(!publishing)throw failure(503,'Studio publishing is not configured.');
        if(request.method==='GET'&&!releaseId)return json(publishing.list(studio));
        if(request.method!=='POST')throw failure(405,'Method not allowed.');
        const input=JSON.parse(await boundedText(request,8192)),current=auth.principal(token);
        if(current.actor!==actor.actor||!auth.allowed(token,studio,'write','programs'))throw failure(403,'Your publishing permission changed.');
        if(releaseId){if(!input||Array.isArray(input)||Object.keys(input).length)throw failure(400,'Unexpected withdrawal fields.');return json(publishing.withdraw(studio,Number(releaseId)));}
        if(input?.requestId&&!auth.allowed(token,studio,'read'))throw failure(403,'Request access is required to publish its delivered routine.');
        return json(publishing.publish(studio,actor.actor,input));
      }
      const match=route.match(/^\/studios\/([a-z0-9-]+)\/requests(?:\/([a-f0-9-]+)\/(accept|renew|deliver|draft))?$/);
      if(!match)throw failure(404,'Not found.');
      const [,studio,id,action]=match;
      if(!material.creators.some(c=>c.id===studio)||!auth.allowed(token,studio,request.method==='POST'?'write':'read'))throw failure(403,'You do not have permission for this studio.');
      if(!id&&request.method==='GET')return json(requests.list(studio).map(row=>requests.creatorRead(row.id,studio)));
      if(!id)throw failure(405,'Method not allowed.');
      try{requests.creatorRead(id,studio);}catch{throw failure(404,'Request not found.');}
      if(action==='draft'&&request.method==='GET'){
        if(!auth.allowed(token,studio,'write'))throw failure(403,'Draft editing requires studio write access.');
        return json(requests.draftRead(id,actor.worker));
      }
      if(request.method!=='POST')throw failure(405,'Method not allowed.');
      const input=JSON.parse(await boundedText(request,512*1024));
      const current=auth.principal(token);
      if(current.actor!==actor.actor||!auth.allowed(token,studio,'write'))throw failure(403,'Your studio permission changed.');
      const fields=action==='draft'?['routine','expectedRevision']:action==='deliver'?['revision']:[];
      if(!input||Array.isArray(input)||typeof input!=='object'||Object.keys(input).some(k=>!fields.includes(k)))throw failure(400,'Unexpected operation fields.');
      if(action==='accept')requests.accept(id,actor.worker);
      else if(action==='renew')requests.renew(id,actor.worker);
      else if(action==='draft')return json(requests.saveDraft(id,input.routine,actor.worker,input.expectedRevision));
      else requests.deliverDraft(id,actor.worker,input.revision);
      return json(requests.creatorRead(id,studio));
    }catch(error){return json({error:error.status?error.message:error instanceof SyntaxError?'Invalid JSON.':error.message||'The operation failed.'},error.status??400);}
  };
}
