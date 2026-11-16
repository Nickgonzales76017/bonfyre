// The pages rail and the page settings are server markup
// (cmd/BonfyreWebAddress/src/cms_page_composer_visual.h). This reads and
// writes the settings form the server filled from the page record.
const fail=message=>{throw Error("Page authoring: "+message);};
const KEYS=["title","heading","lede","editorial_kind","media_subject_ref","author_credit",
 "author_subject_ref","editorial_published_on","editorial_updated_on"];
function validMeta(value){
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
 return {...value};
}
export function mountAuthoringPageDirectory(rail,{onDraftChanged,onSave}={}){
 const form=rail?.querySelector("[data-authoring-page-settings]");
 if(!form)fail("this page's settings could not be shown; open it in the section composer");
 const read=()=>Object.fromEntries(KEYS.map(k=>[k,form.elements.namedItem(k)?.value??""]));
 let initial=JSON.stringify(read());
 form.addEventListener("input",()=>onDraftChanged?.(read()));
 form.addEventListener("submit",event=>{event.preventDefault();onSave?.();});
 async function refreshPages(){
  try{
   const response=await fetch(location.href,{credentials:"same-origin",cache:"no-store"});
   const next=new DOMParser().parseFromString(await response.text(),"text/html");
   const fresh=next.querySelector(".authoring-pages-panel"),list=rail.querySelector(".authoring-pages-panel");
   if(response.ok&&fresh&&list)list.replaceWith(fresh);
  }catch{/* The rail keeps its last list. */}
 }
 return {
  getDraft:()=>validMeta(read()),
  refreshPages,
  setJournalMetadata(metadata,{previewOnly=false}={}){
   const current=read();
   if(current.media_subject_ref&&current.media_subject_ref!==metadata.media_subject_ref)
    fail("Open a different page before importing another article identity.");
   const next=validMeta({...current,...metadata});
   if(previewOnly)return next;
   for(const key of KEYS)form.elements.namedItem(key).value=next[key];
   onDraftChanged?.(next);
   return next;
  },
  isDirty:()=>JSON.stringify(read())!==initial,
  committed(){
   const state=rail.querySelector(".authoring-page-visibility");
   if(state&&state.dataset.status!=="published"){state.dataset.status="draft";state.textContent="Private draft · not published";}
   initial=JSON.stringify(read());void refreshPages();
  },
  dispose(){}
 };
}
