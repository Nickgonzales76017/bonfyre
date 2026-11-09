// The authoring page uses the existing authenticated BonfyreCMS endpoint.
import {mountAurekaiAuthoring} from "./unified-editor.mjs";
import {openNativeAuthoring,saveNativeAuthoring} from "./native-cms.mjs";
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
export async function mountNativeAurekaiEditor(root,{route="/",fetcher=fetch,loaders}={}){
 if(!root)throw Error("An authoring root is required");
 const {base,tree}=await openNativeAuthoring({route,fetcher});
 let authoritative=base;
 const dialog=reviewDialog();
 const desk=await mountAurekaiAuthoring(root,{
  initial:tree,loaders,
  onReview:dialog.review,
  onSaveDraft:async draft=>{
   const receipt=await saveNativeAuthoring({fetcher,base:authoritative,tree:draft,
    pageDraft:{title:"New page",heading:"New page"}});
   authoritative=receipt.base;
   return receipt;
  }
 });
 return {...desk,dispose:async()=>{await desk.destroy();dialog.dispose();}};
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
