import {createCipheriv,createECDH,createHash,createHmac,createPrivateKey,randomBytes,sign} from 'node:crypto';
import {closeSync,constants,existsSync,fstatSync,fsyncSync,lstatSync,mkdirSync,openSync,readFileSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {encode,decode} from '../browser/workspace-msgpack.mjs';

const b64=bytes=>Buffer.from(bytes).toString('base64url');
const hmac=(key,body)=>createHmac('sha256',key).update(body).digest();
const fail=(ok,message)=>{if(!ok)throw Object.assign(new Error(message),{code:'invalid'});};
export const pushEndpointHash=value=>createHash('sha256').update(value).digest('hex');
export function pushBytes(value,length) {
  fail(typeof value==='string'&&/^[A-Za-z0-9_-]+$/.test(value),'Use a browser-provided push key.');
  const bytes=Buffer.from(value,'base64url');
  fail(bytes.length===length&&b64(bytes)===value,'The browser push key has an invalid encoding.');
  return bytes;
}
export function pushOrigin(value) {
  const url=new URL(value);
  fail(url.protocol==='https:'&&!url.username&&!url.password&&url.origin===value,'Configure the public HTTPS application origin.');
  return url.origin;
}
export function pushEndpoint(value) {
  fail(typeof value==='string'&&value.length<=4096&&!/[\x00-\x20\x7f\\]/.test(value),'Use the exact browser push endpoint.');
  let url;try{url=new URL(value);}catch{throw Object.assign(new Error('The browser push endpoint is invalid.'),{code:'invalid'});}
  const host=url.hostname;
  const allowed=host==='fcm.googleapis.com'||host==='updates.push.services.mozilla.com'||host==='web.push.apple.com'||
    /^[a-z0-9-]+\.notify\.windows\.com$/.test(host);
  fail(url.protocol==='https:'&&!url.username&&!url.password&&!url.port&&!url.hash&&allowed&&url.href===value&&url.pathname!=='/',
    'This browser push service is not admitted.');
  return value;
}
export function validatePushSubscription(value) {
  fail(value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===3&&
    Object.keys(value).every(key=>['endpoint','expirationTime','keys'].includes(key)),'Use the complete browser subscription.');
  const {endpoint,expirationTime,keys}=value;
  pushEndpoint(endpoint);
  fail(expirationTime===null||Number.isSafeInteger(expirationTime)&&expirationTime>0,'The browser subscription expiry is invalid.');
  fail(keys&&typeof keys==='object'&&!Array.isArray(keys)&&Object.keys(keys).length===2,'Use both browser push keys.');
  const publicKey=pushBytes(keys.p256dh,65);pushBytes(keys.auth,16);
  fail(publicKey[0]===4,'Use an uncompressed browser P-256 public key.');
  // OpenSSL rejects points at infinity and points that are not on P-256.
  const probe=createECDH('prime256v1');probe.generateKeys();
  try{probe.computeSecret(publicKey);}catch{throw Object.assign(new Error('The browser public key is not on P-256.'),{code:'invalid'});}
  return {endpoint,expirationTime,keys:{p256dh:keys.p256dh,auth:keys.auth}};
}

// RFC 8291, one aes128gcm record. The injectable inputs are only used by the
// published RFC vector test; delivery always supplies fresh random material.
export function encryptWebPush(payload,keys,{salt=randomBytes(16),privateKey}={}) {
  fail(payload instanceof Uint8Array&&payload.byteLength<=3993,'The push envelope exceeds the single-record limit.');
  const ua=pushBytes(keys.p256dh,65),auth=pushBytes(keys.auth,16);
  fail(salt instanceof Uint8Array&&salt.byteLength===16&&ua[0]===4,'The encryption inputs are invalid.');
  const server=createECDH('prime256v1');
  if(privateKey)server.setPrivateKey(privateKey);else server.generateKeys();
  const pub=server.getPublicKey(),shared=server.computeSecret(ua);
  const ikm=hmac(hmac(auth,shared),Buffer.concat([Buffer.from('WebPush: info\0'),ua,pub,Buffer.from([1])]));
  const prk=hmac(salt,ikm),cek=hmac(prk,Buffer.from('Content-Encoding: aes128gcm\0\x01')).subarray(0,16);
  const nonce=hmac(prk,Buffer.from('Content-Encoding: nonce\0\x01')).subarray(0,12);
  const cipher=createCipheriv('aes-128-gcm',cek,nonce);
  const encrypted=Buffer.concat([cipher.update(Buffer.concat([payload,Buffer.from([2])])),cipher.final(),cipher.getAuthTag()]);
  const header=Buffer.alloc(86);Buffer.from(salt).copy(header);header.writeUInt32BE(4096,16);header[20]=65;pub.copy(header,21);
  return Buffer.concat([header,encrypted]);
}

export function vapidIdentity(privateBytes,origin) {
  pushOrigin(origin);
  fail(privateBytes instanceof Uint8Array&&privateBytes.byteLength===32,'The native push signing key is invalid.');
  const key=createECDH('prime256v1');key.setPrivateKey(privateBytes);const pub=key.getPublicKey();
  const signingKey=createPrivateKey({key:{kty:'EC',crv:'P-256',x:b64(pub.subarray(1,33)),y:b64(pub.subarray(33)),d:b64(privateBytes)},format:'jwk'});
  return Object.freeze({publicKey:b64(pub),keyId:pushEndpointHash(pub),origin,signingKey});
}
export function loadPushIdentity(home,origin) {
  pushOrigin(origin);
  const directory=join(home,'push');
  if(!existsSync(directory))mkdirSync(directory,{mode:0o700});
  const stat=lstatSync(directory);
  fail(stat.isDirectory()&&!stat.isSymbolicLink()&&(stat.mode&0o077)===0,'The native push key directory must be private.');
  const path=join(directory,'vapid.msgpack');
  let fd;
  try {
    fd=openSync(path,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);
    const key=createECDH('prime256v1');key.generateKeys();
    writeFileSync(fd,encode([1,key.getPrivateKey(),origin]));fsyncSync(fd);
  } catch(error) { if(error.code!=='EEXIST')throw error; }
  finally {if(fd!==undefined)closeSync(fd);}
  fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try {
    const metadata=fstatSync(fd);
    fail(metadata.isFile()&&metadata.size>0&&metadata.size<=1024&&(metadata.mode&0o077)===0,'The native push signing key must be a private bounded file.');
    const packet=decode(new Uint8Array(readFileSync(fd)),{maxBytes:1024});
    fail(Array.isArray(packet)&&packet.length===3&&packet[0]===1&&packet[2]===origin,'The retained push identity belongs to another origin.');
    return vapidIdentity(packet[1],origin);
  } finally {closeSync(fd);}
}

export function vapidAuthorization(identity,endpoint,now) {
  const aud=new URL(endpoint).origin;
  fail(Number.isFinite(now)&&now>=0,'The delivery clock is invalid.');
  // JSON is mandated by the VAPID JWT standard. Native payloads remain MsgPack.
  const body=b64(Buffer.from(JSON.stringify({typ:'JWT',alg:'ES256'})))+'.'+
    b64(Buffer.from(JSON.stringify({aud,exp:Math.floor(now)+3600,sub:identity.origin+'/apps'})));
  const signature=sign('sha256',Buffer.from(body),{key:identity.signingKey,dsaEncoding:'ieee-p1363'});
  return `vapid t=${body}.${b64(signature)}, k=${identity.publicKey}`;
}
export function createWebPushCarrier({identity,request=fetch,now=()=>Date.now()/1000}={}) {
  fail(identity?.signingKey&&typeof request==='function','A native push identity is required.');
  return async function send(subscription,payload,{ttl,topic,signal}={}) {
    validatePushSubscription(subscription);
    fail(Number.isSafeInteger(ttl)&&ttl>0&&ttl<=86400&&typeof topic==='string'&&/^[A-Za-z0-9_-]{1,32}$/.test(topic),'Use a bounded push delivery lease.');
    const body=encryptWebPush(payload,subscription.keys);
    let response;
    try {
      response=await request(subscription.endpoint,{method:'POST',redirect:'error',credentials:'omit',cache:'no-store',
        signal:signal?AbortSignal.any([signal,AbortSignal.timeout(10000)]):AbortSignal.timeout(10000),
        headers:{'Content-Type':'application/octet-stream','Content-Encoding':'aes128gcm',TTL:String(ttl),Urgency:'normal',Topic:topic,
          Authorization:vapidAuthorization(identity,subscription.endpoint,now())},body});
      const status=response.status;
      let retryAfter=0;const raw=response.headers.get('retry-after');
      if(raw&&/^\d{1,6}$/.test(raw))retryAfter=Math.min(86400,Number(raw));
      else if(raw&&raw.length<128){const at=Date.parse(raw);if(Number.isFinite(at))retryAfter=Math.max(0,Math.min(86400,Math.ceil(at/1000-now())));}
      if(status===201)return {state:'accepted',status};
      if(status===404||status===410)return {state:'revoked',status};
      if(status===429||status>=500)return {state:'retry',status,retryAfter};
      return {state:'failed',status};
    } catch {
      // A network interruption cannot establish whether the push service
      // accepted the body. Retain that uncertainty without blindly resending.
      return {state:'uncertain',status:0};
    } finally {try{await response?.body?.cancel();}catch{}}
  };
}
