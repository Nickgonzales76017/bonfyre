// The authoring page uses the existing authenticated BonfyreCMS endpoint.
import {mountAurekaiAuthoring} from "./unified-editor.mjs";
import {openNativeAuthoring,saveNativeAuthoring} from "./native-cms.mjs";
import {encodeNativeAuthoring} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-carrier.mjs";
function el(tag,style,label){
 const n=document.createElement(tag);
 if(style)n.className=style;
 if(label!==undefined)n.textContent=label;
 return n;
}
function reviewDialog(){
 const dialog=el("dialog","authoring-review");
 dialog.setAttribute("aria-label","Review authored draft");
 const title=el("h2","","Review complete draft");
 const description=el("p","","Nothing here publishes a page. Saving retains an account-owned draft; public release requires separate publication review.");
 const summary=el("ol","authoring-review-list");
 const close=el("button","authoring-review-close","Close review");
 close.type="button";close.onclick=()=>dialog.close();
 dialog.append(title,description,summary,close);
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
  dialog.showModal();
 };
 return {review,dispose:()=>dialog.remove()};
}
// A genuine C-rendered preview behind the same current CMS writer admission.
// Binary source is hex in the existing bounded text POST envelope. The iframe
// cannot run scripts or access its parent even if markup rules change later.
export async function requestNativePreview({fetcher=fetch,tree}){
 const bytes=encodeNativeAuthoring(tree);
 if(bytes.length*2+6>220000)throw Error("This draft exceeds the native preview request limit");
 const hex=Array.from(bytes,b=>b.toString(16).padStart(2,"0")).join("");
 const response=await fetcher("/cms/compose/preview",{
  credentials:"same-origin",cache:"no-store",method:"POST",
  headers:{"Content-Type":"text/plain"},body:"BFAU2:"+hex
 });
 if(!response?.ok)throw Error("Native preview was refused ("+(response?.status||"network")+")");
 const html=await response.text();
 if(!html.startsWith("<!doctype html>")||
    !html.includes('data-release="draft-preview-only"'))
  throw Error("Native renderer did not return a draft-only page");
 return html;
}
function nativePreviewDialog(){
 const dialog=el("dialog","authoring-native-preview");
 dialog.setAttribute("aria-label","Native C rendered page preview");
 const toolbar=el("div","authoring-preview-top");
 const title=el("h2","","Native page preview");
 const note=el("p","","Rendered by Bonfÿre C from your complete current draft. Not publicly released.");
 const close=el("button","authoring-preview-close","Close preview");
 close.type="button";close.onclick=()=>dialog.close();
 toolbar.append(title,note,close);
 const frame=el("iframe","authoring-preview-frame");
 frame.setAttribute("title","Account-owned native page preview");
 frame.setAttribute("sandbox","");
 frame.setAttribute("referrerpolicy","no-referrer");
 dialog.append(toolbar,frame);document.body.append(dialog);
 return {show:async html=>{frame.srcdoc=html;dialog.showModal();},
  dispose:()=>dialog.remove()};
}
export async function mountNativeAurekaiEditor(root,{route="/",fetcher=fetch,loaders}={}){
 if(!root)throw Error("An authoring root is required");
 const {base,tree}=await openNativeAuthoring({route,fetcher});
 let authoritative=base;
 const dialog=reviewDialog(),nativePreview=nativePreviewDialog();
 const desk=await mountAurekaiAuthoring(root,{
  initial:tree,loaders,
  onReview:dialog.review,
  onPreview:async draft=>nativePreview.show(await requestNativePreview({fetcher,tree:draft})),
  onSaveDraft:async draft=>{
   const receipt=await saveNativeAuthoring({fetcher,base:authoritative,tree:draft,
    pageDraft:{title:"New page",heading:"New page"}});
   authoritative=receipt.base;
   return receipt;
  }
 });
 return {...desk,dispose:async()=>{
  await desk.destroy();dialog.dispose();nativePreview.dispose();
 }};
}
export async function bootNativeAurekaiEditor(){
 const root=document.querySelector("[data-aurekai-authoring]");
 if(!root)return;
 const route=new URLSearchParams(location.search).get("route")||"/";
 const field=document.querySelector(".authoring-top input[name=route]");
 if(field)field.value=route;
 try{await mountNativeAurekaiEditor(root,{route});}
 catch(error){
  root.replaceChildren(el("p","authoring-error",
   "The native composition workspace did not open: "+error.message));
 }
}
