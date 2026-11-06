import { createHash } from 'node:crypto';
import { constants, openSync, closeSync, fstatSync, readFileSync, realpathSync } from 'node:fs';
import { sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { accountAuth } from '../../components/BonfyreServiceSite/account-auth.mjs';

const IDENT=/^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/;
const HASH=/^[a-f0-9]{64}$/;
export const refuse=(status,message)=>Object.assign(new Error(message),{status});
export const validId=(value,label)=>{
  if(typeof value!=='string'||!IDENT.test(value))throw refuse(400,'Invalid '+label);
  return value;
};
export const validDigest=value=>{
  if(typeof value!=='string'||!HASH.test(value))throw refuse(400,'Source SHA-256 is required');
  return value;
};
export const validReason=value=>{
  if(typeof value!=='string'||value.length>1200||/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value))
    throw refuse(400,'Invalid review reason');
  return value.trim();
};
export const intentDigest=parts=>createHash('sha256').update(
  parts.map(part=>{const s=String(part??'');return Buffer.byteLength(s)+':'+s;}).join('')
).digest('hex');

export function reviewAuthority(authDb,experiences,auth=accountAuth(authDb)) {
  if(!Array.isArray(experiences)||!experiences.length||experiences.length>64)throw Error('A bounded review registry is required');
  const policies=new Map();
  for(const entry of experiences) {
    if(!entry||Object.keys(entry).sort().join(',')!=='approvedFamily,experience,institution,sourceRoot')
      throw Error('Invalid review declaration');
    const experience=validId(entry.experience,'experience'),institution=validId(entry.institution,'institution');
    const approvedFamily=validId(entry.approvedFamily,'approved work family');
    if(policies.has(experience)||typeof entry.sourceRoot!=='string'||!entry.sourceRoot.startsWith('/'))
      throw Error('Duplicate experience or unsafe source owner');
    policies.set(experience,Object.freeze({experience,institution,approvedFamily,sourceRoot:realpathSync(entry.sourceRoot)}));
  }
  const principal=token=>{
    const value=auth.principal(token);
    if(!value||!Number.isSafeInteger(value.userId)||value.userId<1||
      typeof value.actor!=='string'||!value.actor.trim()||value.actor.length>512)
      throw refuse(401,'A current native ActorClosure is required');
    return value;
  };
  const permit=(token,institution,role)=>{
    if(auth.allowed(token,institution,role)!==true)throw refuse(403,'Current native institutional authority is required');
  };
  function source(policy,uri,digest) {
    validDigest(digest);
    let path;
    try {const u=new URL(uri);if(u.protocol!=='file:'||u.hostname||u.search||u.hash||uri.length>1600)throw Error();path=fileURLToPath(u);}
    catch{throw refuse(400,'Source requires a local, content-addressed owner artifact');}
    try {
      const base=realpathSync(policy.sourceRoot),actual=realpathSync(path);
      if(!actual.startsWith(base+sep))throw Error();
      const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW);
      try {
        const st=fstatSync(fd);
        if(!st.isFile()||st.size>2*1024*1024||
          createHash('sha256').update(readFileSync(fd)).digest('hex')!==digest)throw Error();
      }finally{closeSync(fd);}
    }catch{throw refuse(409,'Source missing, changed, or outside the declared owner');}
    return {uri,digest};
  }
  function delegate(token,policy,email) {
    if(typeof email!=='string'||email.length>300||!email.includes('@'))throw refuse(400,'Select an existing native reviewer');
    permit(token,policy.institution,'admin');
    let after=0;
    for(let index=0;index<10;index++){
      const page=auth.members(token,policy.institution,after);
      if(!Array.isArray(page?.members))throw refuse(503,'Native member directory unavailable');
      const matches=page.members.filter(v=>v.email===email&&v.effective===true&&
        ['write','admin'].includes(v.role)&&Number.isSafeInteger(v.user_id));
      if(matches.length===1)return matches[0].user_id;
      if(page.next_user_id===null)return null;
      if(!Number.isSafeInteger(page.next_user_id)||page.next_user_id<=after)
        throw refuse(503,'Invalid native member cursor');
      after=page.next_user_id;
    }
    throw refuse(503,'Reviewer directory exceeded the bounded scan');
  }
  return {policies,principal,permit,source,delegate};
}
