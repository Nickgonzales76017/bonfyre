const fail=message=>{throw Error("Page authoring: "+message);};
const routeOK=value=>typeof value==="string"&&value.length<=127&&
 /^\/(?:[a-z0-9-]+\/?)*$/.test(value)&&!value.includes("//");
const elem=(name,text="",className="")=>{
 const el=document.createElement(name);
 if(text)el.textContent=text;
 if(className)el.className=className;
 return el;
};
const copy=x=>structuredClone(x);
function pageMetadata(base){
 const page=base?.snapshot?.page;
 return {title:page?.title||"Untitled page",
  heading:page?.heading||"Untitled page",lede:page?.lede||"",editorial_kind:page?.editorial_kind||"",media_subject_ref:page?.media_subject_ref||"",
  author_credit:page?.author_credit||"",author_subject_ref:page?.author_subject_ref||"",
  editorial_published_on:page?.editorial_published_on||"",editorial_updated_on:page?.editorial_updated_on||""};
}
function validMeta(value){
 if(!value||typeof value!=="object"||Array.isArray(value))fail("invalid page metadata");
 for(const [key,limit]of [["title",220],["heading",220],["lede",5000]]){
  if(typeof value[key]!=="string"||value[key].length>(limit)||value[key].includes("\0"))
   fail(key+" must be readable text within "+limit+" characters");
  if(key!=="lede"&&!value[key].trim())fail(key+" is required");
 }
 if(!["","article","essay","report","feature","longform"].includes(value.editorial_kind)||value.media_subject_ref&&!/^article:[a-z0-9][a-z0-9._-]{0,95}$/.test(value.media_subject_ref)||!!value.editorial_kind!==!!value.media_subject_ref)fail("Choose an article kind and an article identity together.");
 for(const [key,max]of [["author_credit",159],["author_subject_ref",159],["editorial_published_on",10],["editorial_updated_on",10]]){
  if(typeof value[key]!=="string"||value[key].length>max||/[\u0000-\u001f\u007f]/.test(value[key]))fail("Invalid "+key+".");
 }
 if(value.author_subject_ref&&!/^person:[a-z0-9-]{1,70}$/.test(value.author_subject_ref))fail("Use a valid published person: address.");
 if(value.author_subject_ref&&!value.author_credit.trim())fail("A linked author needs a credit.");
 const validDate=s=>{
  if(!/^(18|19|20|21)\d\d-\d\d-\d\d$/.test(s))return false;
  const [y,m,d]=s.split("-").map(Number),leap=y%4===0&&(y%100!==0||y%400===0);
  return m>=1&&m<=12&&d>=1&&d<=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31][m-1];
 };
 if(value.editorial_published_on&&!validDate(value.editorial_published_on)||value.editorial_updated_on&&(!validDate(value.editorial_updated_on)||!value.editorial_published_on||value.editorial_updated_on<value.editorial_published_on))
  fail("Dates must be real calendar days, and an update cannot precede publication.");
 if(!value.editorial_kind&&(value.author_credit||value.author_subject_ref||value.editorial_published_on||value.editorial_updated_on))fail("Choose an article kind before adding byline or dates.");
 return copy(value);
}
export function mountAuthoringPageDirectory(target,{
 base,route,site="",fetcher=fetch,onNavigate,onCreateArticle,onDraftChanged}={}){
 if(!target||typeof target.replaceChildren!=="function"||!routeOK(route)||
    typeof onNavigate!=="function")fail("destination, route and navigation owner required");
 const initial=validMeta(pageMetadata(base)),current=copy(initial);
 const bar=elem("section","","authoring-page-bar");
 bar.setAttribute("aria-label","Site pages and page settings");
 const summary=elem("div","","authoring-pages-panel");
 const heading=elem("h2","Pages in this site"),hint=elem("p",
  "Create new work here or reopen a saved draft. Publication remains a separate action.");
 const visibility=elem("p","","authoring-page-visibility");
 const state=base?.snapshot?.page?.status||"new";
 visibility.dataset.status=state;
 visibility.textContent=state==="published"?"Published · read-only. Create a new draft to revise.":
  state==="draft"?"Private draft · not published":"New page · nothing published";
 const create=elem("div","","authoring-page-create-actions");
 const newPage=elem("button","New page"),article=elem("button","Write an article");
 for(const control of [newPage,article])control.type="button";
 newPage.addEventListener("click",()=>{
  if(!globalThis.crypto?.getRandomValues){message.textContent="Secure page identity is unavailable.";return;}
  const bytes=new Uint8Array(10);globalThis.crypto.getRandomValues(bytes);
  const id=Array.from(bytes,x=>x.toString(16).padStart(2,"0")).join("");
  onNavigate("/pages/draft-"+id+"/");
 });
 article.addEventListener("click",()=>onCreateArticle?.());
 if(typeof onCreateArticle!=="function")article.disabled=true;
 create.append(newPage,article);
 const pages=elem("ul","","authoring-page-list");
 const navigation=elem("form","","authoring-page-route-form");
 const routeInput=elem("input");
 routeInput.type="text";routeInput.name="route";routeInput.value=route;
 routeInput.maxLength=127;routeInput.required=true;
 routeInput.setAttribute("aria-label","Page address");
 routeInput.setAttribute("placeholder","/docs/components/");
 const open=elem("button","Open or start page");
 open.type="submit";navigation.append(routeInput,open);
 const settings=elem("fieldset","","authoring-page-settings");
 settings.append(elem("legend","This page"));
 const fieldDefs=[
  ["title","Browser title","text",220],
  ["heading","Page heading","text",220],
  ["lede","Introduction","textarea",5000],
  ["editorial_kind","Article kind","select",20],
  ["media_subject_ref","Article identity (article:your-slug)","text",104],
  ["author_credit","Byline credit","text",159],
  ["author_subject_ref","Published author profile (person:handle)","text",159],
  ["editorial_published_on","Publication date","date",10],
  ["editorial_updated_on","Updated date","date",10]
 ];
 const inputs=new Map();
 for(const [key,label,kind,max]of fieldDefs){
  const item=elem("label",label);
  const input=elem(kind==="textarea"?"textarea":kind==="select"?"select":"input");if(kind==="select")for(const value of ["","article","essay","report","feature","longform"]){const option=elem("option",value||"Page without article audio");option.value=value;input.append(option);}
  input.name=key;if(kind==="date")input.type="date";if(kind!=="select")input.maxLength=max;input.value=current[key];
  input.addEventListener("input",()=>{
   current[key]=input.value;
   onDraftChanged?.(copy(current));
  });
  inputs.set(key,input);item.append(input);settings.append(item);
 }
 const message=elem("p","","authoring-pages-status");
 message.setAttribute("role","status");message.setAttribute("aria-live","polite");
  summary.append(heading,visibility,create,hint,navigation,pages,message);
 bar.append(summary,settings);
 target.replaceChildren(bar);
 navigation.addEventListener("submit",event=>{
  event.preventDefault();
  let dest=routeInput.value.trim();
  if(dest!=="/"&&dest.endsWith("/"))dest=dest.replace(/\/+$/,"/");
  if(!routeOK(dest)){message.textContent="Use a local route such as /docs/components/.";return;}
  if(dest===route){message.textContent="This page is already open.";return;}
  onNavigate(dest);
 });
 async function refresh(){
  try{
   const response=await fetcher("/cms/compose/pages"+(site?"?site="+encodeURIComponent(site):""),{credentials:"same-origin",cache:"no-store"});
   if(!response.ok)throw Error("page directory unavailable");
   const result=await response.json();
   if(!Array.isArray(result.pages))throw Error("invalid page directory");
   pages.replaceChildren();
   const entries=result.pages.filter(p=>routeOK(p.route)).slice(0,512);
   for(const p of entries){
    const li=elem("li");
    const button=elem("button");
    button.append(elem("span",p.title||p.route),elem("small",
     p.status==="published"?"Published":p.status==="draft"?"Draft":"Page"));
    button.dataset.status=p.status||"unknown";
    button.type="button";button.dataset.route=p.route;
    button.title=p.route;
    if(p.route===route)button.setAttribute("aria-current","page");
    button.addEventListener("click",()=>{if(p.route!==route)onNavigate(p.route);});
    li.append(button);pages.append(li);
   }
   if(!entries.some(p=>p.route===route)){
    const current=elem("li"),badge=elem("span","New draft: "+route);
    badge.className="authoring-page-unsaved";current.append(badge);pages.prepend(current);
   }
   message.textContent=entries.length+" saved pages in this account"+
    (result.has_more?" (first 512 shown)":"");
  }catch(error){message.textContent="Page list unavailable: "+error.message;}
 }
 void refresh();
 return {
  getDraft:()=>validMeta(current),
  refreshPages:()=>refresh(),
   setJournalMetadata(metadata,{previewOnly=false}={}){
    if(current.media_subject_ref&&current.media_subject_ref!==metadata.media_subject_ref)
     fail("Open a different page before importing another article identity.");
    const next=validMeta({...current,...metadata});
    if(previewOnly)return copy(next);
    Object.assign(current,next);
    for(const [key,input]of inputs)input.value=current[key];
    onDraftChanged?.(copy(current));
    return copy(current);
   },
  isDirty:()=>JSON.stringify(current)!==JSON.stringify(initial),
  committed(nextBase){
   visibility.dataset.status="draft";visibility.textContent="Private draft · not published";
   Object.assign(initial,validMeta(pageMetadata(nextBase)));
   Object.assign(current,initial);
   for(const [key,input]of inputs)input.value=current[key];
   void refresh();
  },
  get currentRoute(){return route;},
  dispose:()=>target.replaceChildren()
 };
}
