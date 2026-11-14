/* Retained external feed -> native destination-owned narration production.
 * This does NOT convert a foreign observation into an authored article,
 * license a voice, or publish a recording. The source stays in its feed.
 */
import {createProduction,productionDigest} from './narration-production.mjs';
import {openDestinationStore} from './workspace-store.mjs';
import {decode} from './workspace-msgpack.mjs';
const safeRef=value=>typeof value==='string'&&/^[a-zA-Z0-9._-]+\/[a-f0-9]{24}$/.test(value);
const safeHttp=value=>{
 let u;try{u=new URL(value);}catch{}
 if(!u||!['http:','https:'].includes(u.protocol)||u.username||u.password)return null;
 return u.href;
};
export async function saveFeedNarration({endpoint,itemRef,href,title,body,kind,script,snapshot}){
 if(!safeRef(itemRef))throw Error('Choose the exact retained source item.');
 const address=safeHttp(href);
 if(!address)throw Error('The retained source needs an ordinary publisher address.');
 if(!/^[a-f0-9]{64}$/.test(snapshot))throw Error('This feed item has no current native source snapshot. Reopen its public source.');
 const matched=/^\/apps\/(ws-[a-f0-9]{40})\/_records$/.exec(endpoint??'');
 if(!matched)throw Error('Choose an installed Audio Studio in the current destination.');
 const [sourceName,selected]=itemRef.split('/');
 const url='/apps/'+matched[1]+'/_retained_inputs?source='+encodeURIComponent(sourceName)+
   '&item='+selected+'&snapshot='+snapshot;
 const response=await fetch(url,{method:'GET',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/msgpack'}});
 if(!response.ok)throw Error('The retained source changed or its public read permission was withdrawn. Reopen the source before using it.');
 if(response.headers.get('content-type')?.split(';')[0]!=='application/msgpack')throw Error('The native source returned an unexpected representation.');
 const payload=decode(new Uint8Array(await response.arrayBuffer()));
 const row=payload?.[1]?.[0];
 if(!Array.isArray(payload)||payload.length!==4||payload[0]!==3||
    !Array.isArray(payload[1])||payload[1].length!==1||payload[2]!==0||payload[3]!==snapshot||
    !Array.isArray(row)||row.length!==10||row[0]!==selected||row[4]!==sourceName||
    row[1]!==title||row[2]!==body||row[3]!==address)
   throw Error('The native retained item does not match this production. Reopen the feed item.');
 if(typeof title!=='string'||!title.trim()||title.length>200||
    typeof body!=='string'||body.length>12000||
    typeof script!=='string'||!script.trim()||script.length>12000)
   throw Error('Review a complete narration script within the production limits.');
 const identity=await productionDigest({itemRef,href:address,title,body,kind});
 const key='draft:creator:narration:'+identity;
 const store=openDestinationStore(endpoint);
 const existing=await store.readVersion(key);
 if(existing){
  if(existing.deleted||existing.value?.schema!=='aurekai.narration-production.v1'||
     existing.value?.provenance?.itemRef!==itemRef||
     existing.value?.items?.[0]?.source?.sha256!==identity)
   throw Error('The existing source production changed. Review it in Audio Studio.');
  return {key,created:false,revision:existing.revision};
 }
 const source={sha256:identity,href:address};
 const p=createProduction({id:'feed-'+identity.slice(0,30),title,
   items:[{id:'retained-item',title,source,language:'en',
     segments:[{id:'source-script',text:script}]}]});
 // Review and rights are deliberately separate; no take is manufactured from
 // saving the production. Audio Studio owns the actual script/take review.
 p.provenance={schema:'bonfyre.feed-handoff.v1',itemRef,sourceHref:address,
   sourceKind:kind,sourceFingerprint:identity};
 p.sourcePermission='unreviewed';
 // Recheck the reviewed source cut immediately before recording the draft.
 const latest=await fetch(url,{method:'GET',credentials:'same-origin',cache:'no-store',redirect:'error'});
 if(!latest.ok)throw Error('The source changed while the production was being prepared. Nothing was saved.');
 const saved=await store.saveLocal(key,p,0);
 return {key,created:true,revision:saved.revision};
}
