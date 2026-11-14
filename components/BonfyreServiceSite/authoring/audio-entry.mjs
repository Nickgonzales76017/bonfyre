// Signed-in article-to-audio authoring uses the existing destination's
// installed app and native CMS page revisions. Discovery never grants a write.
import {saveArticleNarration} from "./article-narration.mjs";
const PACK="experience.native-creator-studio.workspace";
const binding=/^\/apps\/ws-[a-f0-9]{40}\/audio\/$/;
function label(tag,cls,text){
 const node=document.createElement(tag);
 if(cls)node.className=cls;
 if(text)node.textContent=text;
 return node;
}
export function findAudioEditorLink(doc){
 if(!doc?.querySelectorAll)return null;
 for(const element of doc.querySelectorAll("a[data-work-editor][href],a[data-instrument-entry][href]")){
  const path=element.getAttribute("href");
  if(binding.test(path))return path;
 }
 return null;
}
export function mountAccountAudioEntry(host,{fetcher=fetch,getArticle,site="",
 origin=location.origin}={}){
 if(!host||typeof host.append!=="function")throw Error("Audio entry needs a destination host");
 const box=label("section","authoring-audio-entry");
 const title=label("h3","","Audio edition");
 const note=label("p","","Save an identified article, then create a narration production in your installed Audio Studio. Review the script, voice, source permissions and background music before any release.");
 const find=label("button","","Find Audio Studio");find.type="button";
 const create=label("button","","Create audio edition from saved article");
 create.type="button";
 const status=label("p","authoring-audio-status","");
 status.setAttribute("role","status");status.setAttribute("aria-live","polite");
 const target=label("a","","Open Audio Studio");target.hidden=true;
 const setup=label("a","","Review Audio Studio installation");
 setup.href="/packs/setup?pack="+encodeURIComponent(PACK)+"&context=self";
 setup.hidden=true;
 box.append(title,note,find,create,target,setup,status);
 host.append(box);
 let checking=false,disposed=false,controller=null;
 const lookup=async()=>{
  const response=await fetcher("/apps/_cards?context=self&pack="+encodeURIComponent(PACK),{
   credentials:"same-origin",cache:"no-store",redirect:"error",
   headers:{Accept:"text/html"},signal:controller.signal});
  if(!response.ok||!/^text\/html(?:;|$)/i.test(response.headers.get("content-type")||""))
   throw Error("Audio Studio is not available in this destination.");
  if(Number(response.headers.get("content-length")||0)>262144)
   throw Error("The installed-app directory is too large.");
  const markup=await response.text();
  if(markup.length>262144)throw Error("The installed-app directory is too large.");
  return findAudioEditorLink(new DOMParser().parseFromString(markup,"text/html"));
 };
 const perform=async task=>{
  if(checking||disposed)return;
  checking=true;find.disabled=create.disabled=true;controller=new AbortController();
  status.textContent="Checking the Audio Studio installed for this account…";
  try{
   const href=await lookup();
   if(disposed)return;
   if(!href){setup.hidden=false;target.hidden=true;
    status.textContent="Install Audio Studio in this destination before creating an audio edition.";return;}
   setup.hidden=true;target.href=href;target.textContent="Open Audio Studio";target.hidden=false;
   if(task==="create"){
    if(typeof getArticle!=="function")throw Error("This profile editor has no current article owner.");
    const base=await getArticle();
    if(disposed)return;
    status.textContent="Verifying the saved article revision and creating its private narration draft…";
    const result=await saveArticleNarration({base,site,origin,editorHref:href,fetcher});
    if(disposed)return;
    target.href=result.href;
    target.textContent="Open this article in Audio Studio";
    status.textContent=result.created?
     "Narration draft saved. Open the studio to review the script, voice and soundtrack.":
     "This exact article revision already has a narration draft. Open it in Audio Studio.";
   }else status.textContent="Audio Studio is installed. You can create a source-bound audio edition from this page.";
  }catch(error){
   if(disposed||controller.signal.aborted)return;
   status.textContent=error.message||"Audio Studio could not be resolved.";
  }finally{checking=false;find.disabled=create.disabled=false;}
 };
 find.addEventListener("click",()=>void perform("find"));
 create.addEventListener("click",()=>void perform("create"));
 return {dispose(){disposed=true;controller?.abort();box.remove();}};
}
