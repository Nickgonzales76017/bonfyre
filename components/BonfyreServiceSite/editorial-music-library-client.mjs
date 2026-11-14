/* Recorded music library for the account-owned Audio Studio. The catalogue
 * does not create a source, a licence grant or a publication. Download and
 * native draft retention happen only after a user clicks and SHA-256 matches.
 */
import {editorialMusicCatalogue} from "./editorial-music-catalogue.mjs";
import {musicStemRecord} from "./editorial-sound-design-ui.mjs";
import {openDestinationStore} from "../../bridge/workspace-store.mjs";
const prefix="music:stem:",MAX=24*1024*1024;
const node=(tag,css,text)=>{const el=document.createElement(tag);if(css)el.className=css;if(text!==undefined)el.textContent=text;return el;};
const h=bytes=>[...new Uint8Array(bytes)].map(b=>b.toString(16).padStart(2,"0")).join("");
export async function verifiedEditorialMusicBytes(track,{fetcher=fetch,contextFactory}={}){
 if(!editorialMusicCatalogue.some(t=>t.id===track?.id&&t.sha256===track.sha256))
  throw Error("Choose a music recording from the pinned review catalogue.");
 const controller=new AbortController();
 const response=await fetcher(track.downloadUrl,{
  credentials:"omit",redirect:"follow",cache:"no-store",signal:controller.signal});
 if(!response.ok||!response.body)throw Error("Music source is unavailable. Open its source page to obtain the original recording.");
 const declared=Number(response.headers.get("content-length")||0);
 if(declared>MAX){await response.body.cancel();throw Error("Music exceeds the 24 MiB stem limit.");}
 const chunks=[],reader=response.body.getReader();let length=0;
 try{
  for(;;){
   const {done,value}=await reader.read();if(done)break;
   length+=value.byteLength;
   if(length>MAX)throw Error("Music exceeds the 24 MiB stem limit.");
   chunks.push(value);
  }
 }catch(error){try{await reader.cancel(error);}catch{}throw error;}
 finally{reader.releaseLock();}
 if(length<1024)throw Error("Music recording is incomplete.");
 const bytes=new Uint8Array(length);let at=0;
 for(const chunk of chunks){bytes.set(chunk,at);at+=chunk.byteLength;}
 const actual=h(await crypto.subtle.digest("SHA-256",bytes));
 if(actual!==track.sha256)throw Error("The source recording changed, so it was not saved.");
 const Audio=contextFactory||globalThis.AudioContext||globalThis.webkitAudioContext;
 if(!Audio)throw Error("This browser cannot measure the source audio.");
 const ctx=new Audio();let duration;
 try{
  const decoded=await ctx.decodeAudioData(bytes.buffer.slice(0));
  duration=Math.round(decoded.duration*1000);
 }finally{await ctx.close();}
 if(!Number.isSafeInteger(duration)||duration<5000||duration>7200000)
  throw Error("The source recording is not a usable music stem.");
 return musicStemRecord({sha256:track.sha256,title:track.title,bytes:new Blob([bytes],{type:"audio/mpeg"}),
  licence:track.licence,credit:track.artist,sourceUrl:track.sourceUrl,durationMs:duration});
}
export async function retainEditorialMusic(track,{store,fetcher=fetch,contextFactory}={}){
 if(!store?.readVersion||!store?.saveLocal)throw Error("Native destination store is unavailable.");
 const key=prefix+track.sha256;
 const existing=await store.readVersion(key);
 if(existing&&!existing.deleted){
  if(existing.value?.schema!=="aurekai.music-stem.v1"||existing.value.sha256!==track.sha256)
   throw Error("The same source identity already belongs to another record.");
  return {key,retained:false};
 }
 if(existing?.deleted)throw Error("The previous music record was withdrawn. Review the destination before reusing it.");
 const record=await verifiedEditorialMusicBytes(track,{fetcher,contextFactory});
 await store.saveLocal(key,record,0);
 const saved=await store.readVersion(key);
 if(!saved||saved.deleted||saved.value?.sha256!==track.sha256||
    saved.value?.schema!=="aurekai.music-stem.v1"||saved.revision<1)
  throw Error("Music save was not confirmed by the native destination; reconcile before retrying.");
 return {key,retained:true};
}
export function mountEditorialMusicLibrary(host,{store,fetcher=fetch}={}){
 if(!host||!store)throw Error("Editorial music requires an installed Audio Studio.");
 host.replaceChildren();
 const headline=node("div","audio-library-heading");
 headline.append(node("p","eyebrow","Your library"),node("h2","","A soundtrack, not a sound effect."),
  node("p","audio-library-intro","Listen to real instrumental recordings. Save to library keeps the exact file, with its credits, ready to use under your narration or in a mix."));
 const toolbar=node("div","audio-library-tools");
 const filter=node("select");filter.setAttribute("aria-label","Filter editorial music by treatment");
 for(const [key,label] of [["all","All recordings"],["newsroom","Reporting"],["feature","Feature"],["essay","Reflective essay"],["conversation","Conversation"],["documentary","Documentary"]]){
  const option=node("option","",label);option.value=key;filter.append(option);
 }
 const count=node("span","audio-library-count",editorialMusicCatalogue.length+" source-pinned recordings");
 toolbar.append(filter,count);
 const grid=node("div","audio-library-grid"),globalStatus=node("p","audio-library-status","");
 globalStatus.setAttribute("role","status");globalStatus.setAttribute("aria-live","polite");
 const cards=[];let active=null;
 for(const track of editorialMusicCatalogue){
  const card=node("article","audio-music-card");card.dataset.profile=track.profile;
  const genre=node("p","music-genre",track.style+" · "+track.profile);
  const name=node("h3","",track.title);
  const artist=node("p","music-artist",track.artist+" · "+track.instruments);
  const detail=node("p","music-caption",track.note);
  const player=node("audio","music-player");player.controls=true;player.preload="none";
  player.setAttribute("aria-label","Listen to "+track.title+" by "+track.artist);
  player.src=track.downloadUrl;
  player.addEventListener("play",()=>{
   if(active&&active!==player)active.pause();
   active=player;
  });
  const credits=node("p","music-credit","");
  const link=node("a","","Original CC0 catalogue entry");
  link.href=track.sourceUrl;link.target="_blank";link.rel="noopener noreferrer";
  credits.append(node("span","",track.licence+" · rights review required · "),link);
  const control=node("button","music-retain","Save to library");control.dataset.musicSave=track.sha256;
  control.type="button";store.readVersion(prefix+track.sha256).then(v=>{if(v&&!v.deleted){control.textContent="In your library";control.disabled=true;}},()=>{});
  const result=node("p","music-retain-status","");
  result.setAttribute("role","status");
  control.addEventListener("click",async()=>{
   control.disabled=true;result.textContent="Saving…";
   try{
    const receipt=await retainEditorialMusic(track,{store,fetcher});document.dispatchEvent(new CustomEvent('bonfyre:music-retained'));
    result.textContent=receipt.retained?"Saved to your library. Check its terms before you publish.":"Already in your library.";
    control.textContent="In your library";
   }catch(error){
    result.textContent=error.message+" You can download the original from its source page and add it under Add your own music.";
    control.disabled=false;
   }
  });
  card.append(genre,name,artist,detail,player,credits,control,result);
  grid.append(card);cards.push(card);
 }
 filter.addEventListener("change",()=>{
  let shown=0;
  for(const card of cards){card.hidden=filter.value!=="all"&&card.dataset.profile!==filter.value;if(!card.hidden)shown++;}
  count.textContent=shown+" recorded "+(shown===1?"track":"tracks");
 });
 host.append(headline,toolbar,grid,globalStatus,
  node("p","audio-library-disclaimer","Each recording's terms are as its source states them; that is not a rights check. Saved files are checked byte for byte against the source. Nothing is mixed or published until you do it."));
 return {dispose(){active?.pause();host.replaceChildren();}};
}
document.addEventListener("bonfyre:music-save",e=>document.querySelector('[data-music-save="'+e.detail+'"]')?.click());
const root=document.querySelector("[data-editorial-library]");
const route=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content;
if(root&&route){
 try{mountEditorialMusicLibrary(root,{store:openDestinationStore(route)});}
 catch(error){root.replaceChildren(node("p","audio-library-error",error.message));}
}
