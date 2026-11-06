import {createHash,randomUUID} from 'node:crypto';
import {readFileSync,mkdirSync,openSync,closeSync,fsyncSync,writeFileSync,linkSync,unlinkSync,fstatSync,constants} from 'node:fs';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {line,decode,one} from '../experience-handoffs/contract.mjs';

const SOURCE='https://aurekai-work-samples.pages.dev/';
const LIMIT=1048576;
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
function readBounded(file,limit){
  const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{
    const stat=fstatSync(fd);
    if(!stat.isFile()||stat.size<1||stat.size>limit)throw Error('Source artifact size or type was refused.');
    return readFileSync(fd);
  }finally{closeSync(fd);}
}
// Only owned, existing experiences. A source route never grants authority.
const routes=new Map([
  ['/catalogue/','catalogue-workspace'],
  ['/red3-planner/','retail-fixture-planner'],
  ['/archive/','shipment-archive'],
  ['/wix-review/','wix-review'],
  ['/training/','training-workspace'],
  ['/documents/','document-workspace'],
  ['/audio/','audio-workspace']
].map(([path,id])=>[SOURCE.slice(0,-1)+path,id]));
routes.set('https://brief-studio-upgrade.aurekai-work-samples.pages.dev/brief-studio/','acquisition-brief-studio');
routes.set('https://itad-desk-upgrade.aurekai-work-samples.pages.dev/itad-exchange/','itad-acquisition-desk');

function routeFor(href){
  if(typeof href!=='string'||href.length>2048)return null;
  let url;
  try{url=new URL(href,SOURCE);}catch{return null;}
  if(url.username||url.password||url.protocol!=='https:'||url.search)return null;
  url.hash='';
  const address=url.href;
  return routes.has(address)?{address,id:routes.get(address)}:null;
}

// Only explicit anchors from verified owned content may propose a route.
export function discoverExperienceRoutes(body){
  if(!Buffer.isBuffer(body)||!body.length||body.length>LIMIT)throw Error('Invalid retained homepage body.');
  const html=body.toString('utf8').replace(/<!--[\s\S]*?-->/g,'');
  const found=new Map();
  for(const tag of html.matchAll(/<a\b[^>]{0,4096}>/gi)){
    const m=tag[0].match(/\bhref\s*=\s*(["'])(.*?)\1/i);
    const candidate=m&&routeFor(m[2].replaceAll('&amp;','&'));
    if(candidate)found.set(candidate.id,candidate);
  }
  return [...found.values()].sort((a,b)=>a.id.localeCompare(b.id));
}

export function compileSourceExperience({body,receipt,receiptPath,expectedDigest}){
  if(!receipt||receipt.source_url!==SOURCE||receipt.policy_decision!=='allow'||
     receipt.http_status!==200||!receiptPath||typeof receiptPath!=='string'||
     !/^[a-f0-9]{64}$/.test(expectedDigest??'')||sha(body)!==expectedDigest||
     receipt.content_sha256!==expectedDigest)throw Error('Native ForeignTwin observation does not match the source revision.');
  const found=discoverExperienceRoutes(body);
  if(!found.length)throw Error('No declared owned experiences found; source needs review.');
  const text=['handoff aurekai.handoff.v1\n',
    line('source',SOURCE),line('source_digest',expectedDigest),
    line('receipt',receiptPath),line('assertion','owned_page_declares_route'),
    ...found.map(x=>line('candidate',x.id,x.address,sha(SOURCE+'\0'+x.address))),
    line('authority','none'),line('cannot_infer','source_visibility_is_not_import_or_approval')
  ].join('');
  const parsed=decode(text);
  if(one(parsed,'source')!==SOURCE||one(parsed,'source_digest')!==expectedDigest||
     (parsed.get('candidate')?.length??0)!==found.length)throw Error('Typed route projection failed readback.');
  return {text,found,digest:sha(text)};
}

// Immutable, content-addressed candidate bytes become the native WorkGraph output.
export function materializeSourceExperience({source,root}){
  if(!source||typeof source.digest!=='string'||!source.body||!source.receipt)
    throw Error('Retained native source and receipt required.');
  const body=readBounded(source.body,LIMIT),receipt=JSON.parse(readBounded(source.receipt,32768).toString('utf8'));
  const projection=compileSourceExperience({body,receipt,receiptPath:resolve(source.receipt),expectedDigest:source.digest});
  const dir=join(resolve(root),'experience-projections');
  mkdirSync(dir,{recursive:true,mode:0o700});
  const file=join(dir,projection.digest+'.yaff'),temporary=join(dir,'.'+randomUUID()+'.tmp');
  let fd;
  try{
    fd=openSync(temporary,'wx',0o600);
    writeFileSync(fd,projection.text);fsyncSync(fd);closeSync(fd);fd=undefined;
    try{linkSync(temporary,file);}catch(error){
      if(error.code!=='EEXIST'||readBounded(file,262144).toString('utf8')!==projection.text)
        throw Error('Immutable source-experience artifact changed or could not be stored.');
    }
    const directory=openSync(dir,'r');try{fsyncSync(directory);}finally{closeSync(directory);}
  }finally{if(fd!==undefined)closeSync(fd);try{unlinkSync(temporary);}catch{}}
  if(sha(readBounded(file,262144))!==projection.digest)throw Error('Source-experience materialization failed readback.');
  return {uri:pathToFileURL(file).href,digest:projection.digest,sourceDigest:source.digest,
    candidates:projection.found.length,experiences:projection.found.map(x=>x.id),
    authority:'none',status:'materialized-review-candidates'};
}
