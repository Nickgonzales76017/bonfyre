// The authoring page uses the existing authenticated BonfyreCMS endpoint.
import {mountAurekaiAuthoring} from "./unified-editor.mjs";
import {openNativeAuthoring,saveNativeAuthoring,revisePublishedAuthoring} from "./native-cms.mjs";
import {parseJournalMarkdown,journalPageMetadata,saveJournalBatch} from "./journal-import.mjs";
import {mountAuthoringPageDirectory} from "./page-directory.mjs";
import {mountAccountAudioEntry} from "./audio-entry.mjs";
import {mountArticlePublication} from "./article-publication.mjs";
import {renderBrowserReview} from "./review-preview.mjs";
function el(tag,style,label){
 const n=document.createElement(tag);
 if(style)n.className=style;
 if(label!==undefined)n.textContent=label;
 return n;
}
function reviewDialog(readPage){
 const dialog=el("dialog","authoring-review");
 dialog.setAttribute("aria-label","Review authored draft");
 const title=el("h2","","Review complete draft");
 const description=el("p","","Nothing here publishes a page. Saving retains an account-owned draft; public release requires separate publication review.");
 const summary=el("ol","authoring-review-list");
 const close=el("button","authoring-review-close","Close review");
 const viewport=el("div","authoring-review-devices");
 const desktop=el("button","","Desktop"),phone=el("button","","Phone");
 const preview=el("iframe","authoring-review-preview");
 preview.title="Unpublished page composition preview";
 preview.setAttribute("sandbox","");
 preview.setAttribute("referrerpolicy","no-referrer");
 preview.setAttribute("loading","lazy");
 for(const button of [desktop,phone]){button.type="button";viewport.append(button);}
 const setViewport=mobile=>{
  preview.dataset.viewport=mobile?"phone":"desktop";
  desktop.setAttribute("aria-pressed",String(!mobile));
  phone.setAttribute("aria-pressed",String(mobile));
 };
 desktop.onclick=()=>setViewport(false);phone.onclick=()=>setViewport(true);setViewport(false);
 close.type="button";close.onclick=()=>dialog.close();
 dialog.append(title,description,summary,viewport,preview,close);
 document.body.append(dialog);
 const review=async tree=>{
  summary.replaceChildren();
  let total=0;
  const walk=(nodes,depth)=>{
   for(const node of nodes){
    const item=el("li","",("· ".repeat(depth))+
     (node.props.heading||node.kind)+" ("+node.kind+")");
    summary.append(item);total++;walk(node.children,depth+1);
   }
  };
  walk(tree.blocks,0);
  if(!total)summary.append(el("li","","This draft has no authored blocks."));
  const page=readPage();
  preview.srcdoc=renderBrowserReview(tree,{name:page.heading||page.title}).html;
  preview.hidden=false;
  setViewport(false);
  dialog.showModal();
 };
 return {review,dispose:()=>{preview.removeAttribute("srcdoc");dialog.remove();}};
}
function journalIntakePanel(onImport){
 const section=el("details","authoring-journal-intake");
 const heading=el("summary","","Import a journal article");
 const note=el("p","","Bring a local .md article into this page as editable sections. The original file stays on your device; this only changes the draft until you save.");
 const file=el("input");file.type="file";file.accept=".md,text/markdown";
 file.setAttribute("aria-label","Choose a Markdown article");
 const preview=el("p","authoring-import-preview","Choose a local Markdown article to inspect.");
 const state=el("p","authoring-import-status");state.setAttribute("role","status");
 const button=el("button","","Add article to draft");button.type="button";button.disabled=true;
 let article=null;
 file.addEventListener("change",async()=>{
  article=null;button.disabled=true;preview.textContent="";state.textContent="";
  const chosen=file.files?.[0];if(!chosen)return;
  try{
   if(chosen.size>200000)throw Error("The Markdown file exceeds the import limit.");
   article=await parseJournalMarkdown(await chosen.text(),{filename:chosen.name});
   preview.textContent=article.title+" · "+article.sections.length+" editable sections · "+article.slug+
    (article.published?" · "+article.published:"");
   button.disabled=false;
  }catch(error){state.textContent=error.message;}
 });
 button.addEventListener("click",async()=>{
  if(!article||button.disabled)return;
  button.disabled=true;
  try{
   await onImport(article);
   state.textContent="Article added to the current draft. Review and save the draft; nothing is published.";
   article=null;file.value="";
  }catch(error){state.textContent=error.message;button.disabled=false;}
 });
 section.append(heading,note,file,preview,button,state);
 return section;
}
function journalCollectionPanel(onImport){
 const section=el("details","authoring-journal-intake authoring-journal-collection");
 const title=el("summary","","Import a collection of journal articles");
 const info=el("p","","Select up to 128 Markdown articles. Each becomes an account-owned draft page at /blog/article-name/. Existing pages cannot be overwritten. Nothing is published.");
 const files=el("input");files.type="file";files.multiple=true;
 files.accept=".md,text/markdown";
 files.setAttribute("aria-label","Choose journal Markdown files");
 const preview=el("p","authoring-import-preview","No articles selected.");
 const save=el("button","","Save collection as drafts");save.type="button";save.disabled=true;
 const state=el("p","authoring-import-status");
 state.setAttribute("role","status");state.setAttribute("aria-live","polite");
 let pending=[],busy=false;
 files.addEventListener("change",async()=>{
  pending=[];save.disabled=true;state.textContent="";
  const selection=[...(files.files||[])];
  if(!selection.length){preview.textContent="No articles selected.";return;}
  try{
   if(selection.length>128)throw Error("Choose 128 or fewer articles.");
   if(selection.some(file=>file.size>200000))throw Error("An article exceeds 200 KB.");
   const articles=await Promise.all(selection.map(async file=>
    parseJournalMarkdown(await file.text(),{filename:file.name})));
   if(new Set(articles.map(x=>x.slug)).size!==articles.length)
    throw Error("Several files contain the same article slug.");
   pending=articles;
   preview.textContent=articles.length+" articles ready. No drafts saved.";
   save.disabled=false;
  }catch(error){preview.textContent="No articles ready.";state.textContent=error.message;}
 });
 save.addEventListener("click",async()=>{
  if(busy||!pending.length)return;
  busy=true;save.disabled=true;files.disabled=true;
  try{
   const outcome=await onImport(pending,progress=>{
    state.textContent=progress.saved+" of "+progress.total+" drafts saved and verified.";
   });
   state.textContent=outcome.imported+" drafts saved and verified. Nothing published.";
   preview.textContent="Choose another collection to import.";
   pending=[];files.value="";
  }catch(error){
   state.textContent=error.message;
   preview.textContent=error.completed?.length?
    error.completed.length+" drafts committed. Reopen the existing pages before retrying.":
    "No drafts saved. Review the selection or account permissions.";
  }finally{busy=false;files.disabled=false;save.disabled=true;}
 });
 section.append(title,info,files,preview,save,state);
 return section;
}
export async function mountNativeAurekaiEditor(root,{route="/",site="",newArticle=false,fetcher=fetch,loaders}={}){
 if(!root)throw Error("An authoring root is required");
 const {base,tree}=await openNativeAuthoring({route,site,fetcher,editablePublished:!!site});
 let authoritative=base;
 const layout=el("div","authoring-site-workspace");
 const navigationHost=el("div","authoring-page-navigation");
 const editorHost=el("div","authoring-page-editor");
 layout.append(navigationHost,editorHost);root.replaceChildren(layout);
 const dialog=reviewDialog(()=>directory.getDraft());
 let desk=null,navigationAccepted=false;
 const directory=mountAuthoringPageDirectory(navigationHost,{base,route,site,fetcher,
  onDraftChanged:()=>desk?.setPageDirty(directory.isDirty()),
  onCreateArticle(){
   if(desk?.isDirty()||directory.isDirty()){
    if(!confirm("Discard unsaved changes and start an article?"))return;
   }
   navigationAccepted=true;
   location.assign("/cms/compose/advanced?new=article"+(site?"&site="+encodeURIComponent(site):""));
  },
  onNavigate(next){
   if(desk?.isDirty()||directory.isDirty()){
    if(!confirm("Discard unsaved changes and open another page?"))return;
   }
   navigationAccepted=true;
   location.assign("/cms/compose/advanced?route="+encodeURIComponent(next)+(site?"&site="+encodeURIComponent(site):""));
  }
 });
 // Normal authoring composes native pages. Migration is explicitly opt-in.
 if(new URLSearchParams(location.search).get("mode")==="migration"){
 navigationHost.append(journalIntakePanel(async article=>{
  const metadata=journalPageMetadata(article);
  directory.setJournalMetadata(metadata,{previewOnly:true});
  if(!desk)throw Error("The page editor has not finished opening.");
  await desk.importJournal(article);
  directory.setJournalMetadata(metadata);
 }));
 navigationHost.append(journalCollectionPanel(async (articles,onProgress)=>{
  if(!desk)throw Error("The page editor has not finished opening.");
  if(desk.isDirty()||directory.isDirty())
   throw Error("Save changes to the open page before importing a collection.");
  const result=await saveJournalBatch(articles,{site,fetcher,onProgress});
  await directory.refreshPages();
  return result;
 }));
 }
  const publication=mountArticlePublication(navigationHost,{site,route,fetcher,
   getSaved:()=>authoritative,isDirty:()=>!!desk?.isDirty()||directory.isDirty(),
   onReleased:()=>{navigationAccepted=true;location.assign("/profile?site="+encodeURIComponent(site)+"&tab=posts");}
  });
 const audioEntry=mountAccountAudioEntry(navigationHost,{fetcher,site,origin:location.origin,
  getArticle:async()=>{
   if(!desk||desk.isDirty()||directory.isDirty())
    throw Error("Save the article and page settings before creating its audio edition.");
   const fresh=await openNativeAuthoring({route,site,fetcher});
   if(!authoritative.id||fresh.base.id!==authoritative.id||
      fresh.base.digest!==authoritative.digest)
    throw Error("The native article revision changed. Reopen the page before creating its audio edition.");
   return fresh.base;
  }});
 const warnOnLeave=event=>{
  if(!navigationAccepted&&(desk?.isDirty()||directory.isDirty())){
   event.preventDefault();event.returnValue="";
  }
 };
 window.addEventListener("beforeunload",warnOnLeave);
 try{
  desk=await mountAurekaiAuthoring(editorHost,{
   initial:tree,loaders,initialSaved:base.id>0,
   onReview:dialog.review,
   onSaveDraft:async draft=>{
    let receipt;
    if(authoritative.snapshot.page?.status==="published"){
     if(!site)throw Error("An owned site is required to revise published pages.");
     if(!window.confirm("Review the changed layout, credits, sources and publication rights before replacing the current public edition. Continue?"))
      throw Error("Revision cancelled; the public page is unchanged.");
     receipt=await revisePublishedAuthoring({fetcher,base:authoritative,tree:draft,
      pageDraft:directory.getDraft(),site,rightsConfirmed:true});
    }else receipt=await saveNativeAuthoring({fetcher,base:authoritative,tree:draft,
     pageDraft:directory.getDraft(),site});
    authoritative=receipt.base;
    directory.committed(receipt.base);publication.refresh();
    return receipt;
   },
  });
  if(newArticle){
   if(authoritative.id!==0||tree.blocks.length)throw Error("Start a new article in an unused page address.");
   const slug=route.split("/").filter(Boolean).at(-1);
   if(!slug||!/^draft-[a-f0-9]{20}$/.test(slug))throw Error("New article address was not created securely.");
   directory.setJournalMetadata({title:"Untitled article",heading:"Untitled article",
    editorial_kind:"article",media_subject_ref:"article:"+slug});
   await desk.createArticle();
  }
 }catch(error){
  window.removeEventListener("beforeunload",warnOnLeave);
  directory.dispose();audioEntry.dispose();publication.dispose();dialog.dispose();throw error;
 }
 return {...desk,dispose:async()=>{
  window.removeEventListener("beforeunload",warnOnLeave);
  await desk.destroy();directory.dispose();audioEntry.dispose();publication.dispose();dialog.dispose();
 }};
}
export async function bootNativeAurekaiEditor(){
 const root=document.querySelector("[data-aurekai-authoring]");
 if(!root)return;
 const params=new URLSearchParams(location.search),site=params.get("site")||"";
 const newArticle=params.get("new")==="article"&&!params.has("route");
 let route=params.get("route")||"/";
 if(newArticle){
  if(!globalThis.crypto?.getRandomValues)throw Error("Secure browser identity required for new pages.");
  const bytes=new Uint8Array(10);globalThis.crypto.getRandomValues(bytes);
  route="/blog/draft-"+Array.from(bytes,x=>x.toString(16).padStart(2,"0")).join("")+"/";
  const next=new URL(location.href);
  next.searchParams.delete("new");next.searchParams.set("route",route);
  history.replaceState(history.state,"",next.pathname+next.search+next.hash);
 }
 const field=document.querySelector(".authoring-top input[name=route]");
 if(field)field.value=route;
 if(site){
  const input=document.createElement("input");input.type="hidden";
  input.name="site";input.value=site;field?.form?.append(input);
  const back=document.querySelector(".authoring-top a[href='/cms/compose']");
  if(back){back.href="/profile?site="+encodeURIComponent(site)+"&tab=posts";back.textContent="← Your profile";}
 }
 try{await mountNativeAurekaiEditor(root,{route,site,newArticle});}
 catch(error){
  root.replaceChildren(el("p","authoring-error",
   "The native composition workspace did not open: "+error.message));
 }
}
