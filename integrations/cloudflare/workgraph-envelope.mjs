// Cloudflare transport intent: BonfyreQueue owns execution and authorization.
const enc=new TextEncoder();
const key=async s=>crypto.subtle.importKey('raw',enc.encode(s),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);
export function canonical(x){
 const {version,mission,family,digest,issued,nonce}=x??{};
 if(!x||Array.isArray(x)||[mission,family,digest,nonce].some(v=>typeof v!=='string'))throw Error('invalid envelope types');
 if(version!==1||!/^[-a-z0-9.:]{3,120}$/.test(mission??'')||!/^[-a-z0-9.]{2,60}$/.test(family??'')||!(/^[a-f0-9]{64}$/.test(digest??''))||!Number.isSafeInteger(issued)||!/^[-a-zA-Z0-9_]{12,90}$/.test(nonce??''))throw Error('invalid envelope');
 if(Object.keys(x).sort().join(',')!=='digest,family,issued,mission,nonce,version')throw Error('unexpected fields');
 return [version,mission,family,digest,issued,nonce].join('\n');
}
export async function sign(x,secret){
 if(typeof secret!=='string'||secret.length<32)throw Error('missing secret');
 const raw=await crypto.subtle.sign('HMAC',await key(secret),enc.encode(canonical(x)));
 return Array.from(new Uint8Array(raw),n=>n.toString(16).padStart(2,'0')).join('');
}
export async function verify(x,signature,secret,now=Date.now(),maxAgeMs=300000){
 const payload=canonical(x);
 if(!Number.isSafeInteger(now)||!Number.isSafeInteger(maxAgeMs)||maxAgeMs<1||maxAgeMs>86400000)throw Error('invalid clock policy');
 if(now-x.issued>maxAgeMs||x.issued-now>300000)throw Error('expired envelope');
 if(typeof secret!=='string'||secret.length<32)throw Error('missing secret');
 if(!/^[a-f0-9]{64}$/.test(signature??''))throw Error('invalid signature');
 const bytes=Uint8Array.from(signature.match(/../g),h=>parseInt(h,16));
 if(!await crypto.subtle.verify('HMAC',await key(secret),bytes,enc.encode(payload)))throw Error('invalid signature');
 return x;
}
