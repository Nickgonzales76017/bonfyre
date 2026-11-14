// A signed-in profile's existing CMS article becomes a public edition only
// after explicit review of the exact saved revision. Publication is owned by
// BonfyreCMS; this UI cannot make drafts public by itself.
import {openNativeAuthoring} from "./native-cms.mjs";

const node=(tag,className,text)=>{
 const element=document.createElement(tag);
 if(className)element.className=className;
 if(text!==undefined)element.textContent=text;
 return element;
};
const articleRoute=route=>/^\/blog\/[a-z0-9][a-z0-9-]{0,94}\/$/.test(route);
function currentArticle(base){
 const page=base?.snapshot?.page;
 return base?.id>0&&/^[a-f0-9]{64}$/.test(base.digest||"")&&
  page?.editorial_kind==="article"&&page.status==="draft"&&
  Array.isArray(base.snapshot.fragments)&&base.snapshot.fragments.length>0;
}
export function mountArticlePublication(host,{site,route,fetcher=fetch,
 getSaved,isDirty,onReleased}={}){
 if(!host||!site||!articleRoute(route))return {refresh(){},dispose(){}};
 if(typeof getSaved!=="function"||typeof isDirty!=="function")throw Error("The page owner is unavailable.");
 const panel=node("section","authoring-publication");
 panel.dataset.articlePublication="";
 const head=node("div","authoring-publication-head");
 head.append(node("span","authoring-publication-kicker","Your publication"),
  node("h2","","Make this article public"));
 const explain=node("p","","Publish the saved article, its editorial sections and their source references together. It will appear on this site's journal; your other sites stay unchanged.");
 const action=node("button","authoring-publication-open","Review public release");
 action.type="button";
 const status=node("p","authoring-publication-status","");
 status.setAttribute("role","status");status.setAttribute("aria-live","polite");
 panel.append(head,explain,action,status);
 host.append(panel);

 const dialog=node("dialog","authoring-publication-review");
 dialog.setAttribute("aria-label","Review this article for publication");
 const title=node("h2","","Review public article");
 const detail=node("p","authoring-publication-summary","");
 const sections=node("ol","authoring-publication-sections");
 const rights=node("p","authoring-publication-rights",
  "This makes the full article, its links and any included media public. Release only material you have the right to publish.");
 const buttons=node("div","authoring-publication-actions");
 const cancel=node("button","","Keep editing"),confirm=node("button","authoring-publication-confirm",
  "I have the rights — publish this article");
 for(const b of [cancel,confirm]){b.type="button";buttons.append(b);}
 dialog.append(title,detail,sections,rights,buttons);
 document.body.append(dialog);
 let busy=false,disposed=false;
 const refresh=()=>{
  if(disposed)return;
  const saved=getSaved(),page=saved?.snapshot?.page;
  const done=page?.status==="published";
  action.disabled=busy||!currentArticle(saved)||isDirty();
  action.textContent=done?"Already published":"Review public release";
  status.textContent=done?"This article is already released. Open its public journal page.":
   !saved?.id?"Save the article before publishing.":isDirty()?"Save your current changes first.":
   !currentArticle(saved)?"The current page is not a releasable article.":"Ready for exact-revision review.";
 };
 const showError=err=>{status.textContent=err instanceof Error?err.message:String(err);status.dataset.error="true";};
 let reviewed=null;
 action.addEventListener("click",async()=>{
  if(busy||disposed)return;
  const known=getSaved();
  if(!currentArticle(known)||isDirty()){refresh();return;}
  busy=true;refresh();status.dataset.error="false";
  try{
   const fresh=(await openNativeAuthoring({route,site,fetcher})).base;
   if(fresh.id!==known.id||fresh.digest!==known.digest||
      !currentArticle(fresh))throw Error("This article changed. Reopen its latest saved version.");
   reviewed={id:fresh.id,digest:fresh.digest};
   sections.replaceChildren();
   const page=fresh.snapshot.page;
   detail.textContent=(page.title||page.heading)+" · "+fresh.snapshot.fragments.length+
    " composed sections · By "+(page.author_credit||"the site owner");
   for(const fragment of fresh.snapshot.fragments){
    if(fragment.semantic_tags?.authoring_v1?.parent)continue;
    sections.append(node("li","",fragment.heading||fragment.kind));
   }
   dialog.showModal();
  }catch(error){reviewed=null;showError(error);}
  finally{busy=false;refresh();}
 });
 cancel.addEventListener("click",()=>dialog.close());
 confirm.addEventListener("click",async()=>{
  if(busy||!reviewed||disposed)return;
  busy=true;confirm.disabled=cancel.disabled=true;status.dataset.error="false";
  try{
   if(isDirty()||getSaved()?.id!==reviewed.id||getSaved()?.digest!==reviewed.digest)
    throw Error("The reviewed revision changed; reopen it before publication.");
   const url="/cms/compose/publish?site="+encodeURIComponent(site)+"&route="+encodeURIComponent(route);
   const response=await fetcher(url,{method:"POST",credentials:"same-origin",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({
     expected_id:reviewed.id,expected_digest:reviewed.digest,rights_confirmed:true})});
   const result=await response.json();
   if(!response.ok||result.committed!==true||result.id!==reviewed.id)
    throw Error(result.error||"The release is not confirmed. Reopen the current article.");
   dialog.close();
   status.textContent=result.released+" sections published."+
    (result.fact_delta==="DELIVERY_FAILED"?" Index delivery needs reconciliation.":"");
   if(result.fact_delta!=="DELIVERY_FAILED")
    onReleased?.({route,site,result});
  }catch(error){showError(error);}
  finally{busy=false;confirm.disabled=cancel.disabled=false;reviewed=null;refresh();}
 });
 refresh();
 return {refresh,dispose(){
  disposed=true;dialog.close();dialog.remove();panel.remove();
 }};
}
