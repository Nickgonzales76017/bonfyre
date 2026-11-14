// One Aurekai authoring desk: real OSS editors, one retained typed tree.
// GrapesJS handles visual canvas/blocks/styles; Puck owns typed nested layouts;
// Editor.js owns selected article text. Editors NEVER save raw HTML/CSS to CMS.
import {AUTHORING_BLOCKS} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-prototype.mjs";
import {SITE_SECTION_CATALOGUE} from "./site-section-catalogue.generated.mjs";
import {startingSectionPayload} from "./structured-fields.mjs";
import {makeStructuredPayloadField} from "./structured-field-view.mjs";
import {AUTHORING_PATTERNS,PATTERN_BY_KIND,defaultPatternPayload} from "./presentation-patterns.mjs";
import {createAuthoringPalette,createAuthoringOutline,newAuthoredComponent} from "./component-palette.mjs";
import {PAGE_COMPOSITIONS,composeNativePage} from "./editorial-compositions.mjs";
import {renderAuthoredPattern} from "../authored-pattern-renderer.mjs";
import {mergeJournalIntoTree} from "./journal-import.mjs";
import {composeAuthoredExperience} from "../../../cmd/BonfyreSurfaceScene/runtime/experience-programs.mjs";
import {authoredPatternStyles} from "../authored-pattern-styles.mjs";
import {SERVICE_COMPONENTS,createAuthoringSession,validateAuthoringTree,
 toPuckData,fromPuckData,toGrapesProject,fromGrapesProject,
 toEditorBlocks,applyEditorBlocks,applyAuthoringEdit,applyAuthoringProposal} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";

const kinds=[...AUTHORING_BLOCKS.map(x=>x.kind),...SERVICE_COMPONENTS.map(x=>"service:"+x),...AUTHORING_PATTERNS.map(x=>x.kind)];
const groups=new Set(["profile-section","service:cases","service:prose","service:hero"]);
const sectionKinds=new Map(SITE_SECTION_CATALOGUE.map(s=>[s.kind,s]));
const fields=["heading","body","eyebrow","label","href","variant"];
const newEditorCoordinate=()=>{
 const c=globalThis.crypto;
 if(!c?.getRandomValues)throw Error("Secure browser identity is required for new blocks");
 const bytes=new Uint8Array(12);c.getRandomValues(bytes);
 return "node-"+Array.from(bytes,v=>v.toString(16).padStart(2,"0")).join("");
};
const get=(obj,key,fallback="")=>obj?.[key]??fallback;
const find=(nodes,id)=>{
 for(const node of nodes){if(node.id===id)return node;const child=find(node.children,id);if(child)return child;}
 return null;
};
function all(node){
 const found=[];
 const visit=b=>{found.push(b);b.children.forEach(visit);};
 node.blocks.forEach(visit);return found;
}
function status(ui,msg,failed=false){ui.status.textContent=msg;ui.status.dataset.error=String(failed);}
function element(tag,className="",text=""){
 const el=document.createElement(tag);
 if(className)el.className=className;if(text)el.textContent=text;
 return el;
}
export function makePuckConfig(React){
 const defs={};
 for(const kind of kinds){
  const pattern=PATTERN_BY_KIND.get(kind),section=groups.has(kind)||!!pattern?.children,
   spec=kind.startsWith("service:")?sectionKinds.get(kind.slice(8)):null;
  const inputs={
   heading:{type:"text",label:"Heading"},
   body:{type:"textarea",label:"Body"},
   eyebrow:{type:"text",label:"Eyebrow"},
   label:{type:"text",label:"Button label"},
   href:{type:"text",label:"Source / link"},
   variant:{type:"select",label:"Appearance",options:
    ["paper","forest","night","rust","wide","slate","editorial"].map(value=>({label:value,value}))}
  };
  if(spec||pattern)inputs.payload=makeStructuredPayloadField(React,spec||{kind:pattern.kind,required:[]});
  if(section)inputs.children={type:"slot"};
  defs[kind]={
   label:kind.replace(/^(profile-|service:)/,"").replace(/-/g," "),
   fields:inputs,
   defaultProps:{heading:"New "+kind.replace(/^(profile-|service:)/,""),variant:"paper",
    ...(section?{children:[]}:{}),
    ...(spec?{payload:startingSectionPayload(spec)}:{}),
    ...(pattern?{payload:defaultPatternPayload(pattern.kind)}:{}),responsive:{}},
   render:props=>{
    const children=typeof props.children==="function"?React.createElement(props.children):null;
    if(pattern){
     try{
      const node={kind,id:"preview-"+kind.slice(3),props:{
       heading:props.heading||"",payload:props.payload||defaultPatternPayload(kind)},responsive:props.responsive||{}};
      const markup=renderAuthoredPattern(node);
      // This HTML comes only from the first-party typed component renderer;
      // user-supplied HTML is forbidden at the native tree boundary.
      return React.createElement("div",{"data-authoring-kind":kind,
       "data-look":props.variant||"paper",className:"authoring-typed-component"},
       React.createElement("div",{className:"authoring-trusted-preview",
        dangerouslySetInnerHTML:{__html:markup}}),children);
     }catch(error){
      return React.createElement("div",{"data-authoring-kind":kind,role:"alert",
       className:"authoring-preview-error"},"Component preview requires valid data: "+error.message,children);
     }
    }
    const summary=spec?spec.required.map(field=>{
     const value=props.payload?.[field];
     const label=Array.isArray(value)?value.length+" items":
      value&&typeof value==="object"?Object.keys(value).length+" fields":
      typeof value==="string"?value:"Missing";
     return React.createElement("div",{key:field,className:"authoring-puck-material"},
      React.createElement("small",null,field),
      React.createElement("span",null,String(label)));
    }):[];
    return React.createElement("section",{"data-authoring-kind":kind,"data-look":props.variant||"paper"},
     React.createElement("small",null,props.eyebrow||""),
     React.createElement("h2",null,props.heading||"Untitled"),
     React.createElement("p",null,props.body||""),...summary,children);
   }
  };
 }
 return {components:defs};
}
// Register dedicated GrapesJS component models instead of generic DOM export.
// The canvas can be visually rich; its project JSON still must pass tree rules.
export function registerGrapesComponents(editor){
 for(const kind of kinds){
  const id="aurekai:"+kind;
  const traits=fields.map(name=>({type:name==="body"?"text":"text",name,label:name}));
  const pattern=PATTERN_BY_KIND.get(kind);
  editor.Components.addType(id,{model:{
   defaults:{tagName:"section",name:kind,draggable:true,droppable:groups.has(kind)||!!pattern?.children,
    attributes:{"data-aurekai-kind":kind},aurekai:{heading:"New block",variant:"paper",
      ...(pattern?{payload:defaultPatternPayload(kind)}:{})},
    responsive:{},traits},
   init(){
    const attributes=this.getAttributes?.()||{};
    if(!attributes["data-coordinate"])
     this.addAttributes({"data-coordinate":newEditorCoordinate()});
    const block=this.get("aurekai")||{};
    for(const f of fields)if(block[f]!==undefined)this.set(f,block[f],{silent:true});
    for(const f of fields)this.on("change:"+f,()=>{
     this.set("aurekai",{...(this.get("aurekai")||{}),[f]:this.get(f)});
    });
   }
  },view:{
   onRender(){
    if(!pattern||!this.el)return;
    const old=this.el.querySelector(":scope > .authoring-trusted-preview");
    if(old)old.remove();
    const content=this.el.ownerDocument.createElement("div");
    content.className="authoring-trusted-preview";
    try{
     content.innerHTML=renderAuthoredPattern({kind,id:"preview-"+kind.slice(3),
      props:{heading:this.model.get("aurekai")?.heading||"",
       payload:this.model.get("aurekai")?.payload||defaultPatternPayload(kind)},responsive:this.model.get("responsive")||{}});
    }catch(error){content.textContent="Component preview requires valid data: "+error.message;}
    this.el.insertBefore(content,this.el.firstChild);
   }
  }});
  editor.Blocks.add(id,{label:kind.replace(/^(profile-|service:)/,"").replace(/-/g," "),
   category:pattern?pattern.group:kind.startsWith("service:")?"Applications":"Profiles",
   content:{type:id,aurekai:{heading:"New block",variant:"paper",
     ...(pattern?{payload:defaultPatternPayload(kind)}:{})},components:[]}});
 }
}
const deps={
 grapesjs:async()=>((await import("grapesjs")).default),
 puck:async()=>Promise.all([import("react"),import("react-dom/client"),import("@puckeditor/core")]),
 editorjs:async()=>Promise.all([
  import("@editorjs/editorjs"),import("@editorjs/header"),import("@editorjs/list"),
  import("@editorjs/quote"),import("@editorjs/table")])
};
// A mode is a different specialist editing interaction within the same desk,
// never an independent content store. The currently mounted mode is flushed
// before switching; failed validation keeps the editor and its draft intact.
export async function mountAurekaiAuthoring(root,{initial,loaders=deps,onReview,onSaveDraft,initialSaved=false}={}){
 if(!root||typeof root.replaceChildren!=="function")throw Error("A DOM host is required");
 if(typeof onSaveDraft!=="function"||typeof onReview!=="function")
  throw Error("Destination-owned save and review callbacks are required");
 const session=createAuthoringSession(validateAuthoringTree(initial));
 let savedTree=JSON.stringify(session.snapshot());
 const desk=element("div","aurekai-authoring");
 const toolbar=element("header","authoring-toolbar"),modeBar=element("nav","authoring-modes");
 const actions=element("div","authoring-actions"),statusBox=element("p","authoring-status");
 const stage=element("main","authoring-stage"),modeButtons={},deviceButtons={};
 const rail=element("aside","authoring-inspector-rail");
 const workspace=element("section","authoring-canvas-shell");
 const canvasHead=element("div","authoring-canvas-head");
 const devices=element("div","authoring-device-controls");
 const workflow=element("ol","authoring-workflow");
 const workflowBar=element("nav","authoring-workflow-bar");
 const saveState=element("span","authoring-save-state");
 const emptyState=element("section","authoring-empty-state");
 const ui={desk,toolbar,modeBar,actions,status:statusBox,stage};
 let mounted=null,mode=null,selected=null,busy=false,changes=0;
 let device="desktop",deviceEditor=null,metadataDirty=false,metadataRevision=0;
 let reviewedAt="",hasSaved=initialSaved;
 const controls=new Map();
 const palette=createAuthoringPalette({onInsert:insertComponent,onCompose:composePage,compositions:PAGE_COMPOSITIONS});
 const outline=createAuthoringOutline({onEdit:editComposition,onMode:next=>switchMode(next)});
 const authoringStyle=element("style");authoringStyle.setAttribute("data-aurekai-patterns","");
 authoringStyle.textContent=authoredPatternStyles;document.head.append(authoringStyle);
 const makeButton=(label,parent,handler)=>{
  const btn=element("button","",label);btn.type="button";btn.addEventListener("click",handler);
  parent.append(btn);return btn;
 };
 const heading=element("div","authoring-heading");
 heading.append(element("span","authoring-eyebrow","Aurekai / Creator Studio"),
  element("h1","","Edit your site"),
  element("p","authoring-page-path",session.snapshot().page));
 modeBar.setAttribute("aria-label","Authoring tools");
 const modeStrip=element("div","authoring-mode-strip");
 modeStrip.append(element("span","authoring-strip-label","WORKSPACE"),modeBar);
 toolbar.append(heading,actions,modeStrip);
 workflowBar.setAttribute("aria-label","Draft workflow");
 const steps=[["compose","Compose"],["review","Review"],["save","Save draft"]];
 const flowNodes=new Map();
 for(const [id,label]of steps){
  const item=element("li","authoring-workflow-step");
  item.dataset.step=id;
  item.append(element("span","authoring-workflow-count",String(flowNodes.size+1)),
   element("span","",label));
  workflow.append(item);flowNodes.set(id,item);
 }
 workflowBar.append(workflow,saveState,element("p","authoring-workflow-note",
  "Publication requires a separate review. Saving keeps this work private."));
 const canvasTitle=element("div","authoring-canvas-title");
 canvasTitle.append(element("span","authoring-eyebrow","LIVE CANVAS"),
  element("strong","","Page composition"));
 devices.setAttribute("role","group");devices.setAttribute("aria-label","Design canvas viewport");
 canvasHead.append(canvasTitle,devices);
 const emptyCopy=element("div","authoring-empty-copy");
 emptyCopy.append(element("span","authoring-eyebrow","START WITH YOUR OWN WORK"),
  element("h2","","What will you make?"),
  element("p","","Write an original article or build a page section. Arrange, review and save in the same account-owned draft."));
 const emptyActions=element("div","authoring-empty-actions");
 emptyState.append(emptyCopy,emptyActions);
 emptyState.setAttribute("aria-label","Start an empty composition");
 workspace.append(canvasHead,emptyState,stage);
 rail.setAttribute("aria-label","Components, layers and properties");
 rail.append(palette.element,outline.element);
 desk.append(toolbar,workflowBar,workspace,rail,statusBox);
 root.replaceChildren(desk);
 function bindModes(){
  for(const [key,label]of [["grapesjs","Design"],["puck","Structure"],["editorjs","Article"]])
   modeButtons[key]=makeButton(label,modeBar,()=>{void switchMode(key);});
  controls.set("undo",makeButton("Undo",actions,()=>{if(session.undo()){changes++;void remount();}}));
  controls.set("redo",makeButton("Redo",actions,()=>{if(session.redo()){changes++;void remount();}}));
  controls.set("review",makeButton("Review",actions,()=>void review()));
  controls.set("write",makeButton("New article",actions,()=>void insertComponent("profile-story",null,{startArticle:true})));
  controls.set("save",makeButton("Save draft",actions,()=>void save()));
  for(const name of ["desktop","tablet","phone"]){
   const label=name[0].toUpperCase()+name.slice(1);
   deviceButtons[name]=makeButton(label,devices,()=>{
    if(busy||mode!=="grapesjs"||!deviceEditor)return;
    device=name;deviceEditor.setDevice(name);dirtyControls();
   });
  }
  const write=makeButton("Write an article",emptyActions,()=>void insertComponent("profile-story",null,{startArticle:true}));
  write.className="authoring-empty-primary";
  for(const recipe of PAGE_COMPOSITIONS)makeButton(recipe.label,emptyActions,()=>void composePage(recipe.id));
 }
 const revision=()=>changes+":"+metadataRevision;
 function updateWorkflow(){
  const hasContent=session.snapshot().blocks.length>0;
  const saved=hasSaved&&savedTree===JSON.stringify(session.snapshot())&&!metadataDirty&&!mounted?.dirty?.();
  const reviewed=reviewedAt===revision()&&!mounted?.dirty?.();
  for(const [step,node]of flowNodes){
   const done=step==="compose"?hasContent:step==="review"?reviewed:saved;
   const active=!done&&(step==="compose"?!hasContent:step==="review"?hasContent&&!reviewed:reviewed&&!saved);
   node.dataset.state=done?"complete":active?"active":"waiting";
  }
  saveState.dataset.state=saved?"saved":"unsaved";
  saveState.textContent=saved?"Saved private draft":hasContent?"Unsaved changes":"New unsaved page";
  emptyState.hidden=hasContent;
  for(const [name,button]of Object.entries(deviceButtons)){
   button.disabled=busy||mode!=="grapesjs";
   button.setAttribute("aria-pressed",String(device===name));
  }
 }
 function dirtyControls(){
  controls.get("undo").disabled=!session.canUndo()||busy;
  controls.get("redo").disabled=!session.canRedo()||busy;
  controls.get("review").disabled=busy;controls.get("save").disabled=busy;
  controls.get("write").disabled=busy||!mode;
  palette.setBusy(busy||!mode);outline.setBusy(busy||!mode);
  for(const [name,button]of Object.entries(modeButtons))
   button.setAttribute("aria-current",String(mode===name));
  updateWorkflow();
 }
 async function insertComponent(kind,parent,{startArticle=false}={}){
  if(busy||!mode)return;
  busy=true;dirtyControls();
  try{
   await flush();
   const block=newAuthoredComponent(kind,newEditorCoordinate());
   const doc=applyAuthoringEdit(session.snapshot(),{type:"insert",parent,block});
   await apply(doc);
   if(startArticle)selected=block.id;
   const current=startArticle?"editorjs":mode;await destroy();mode=current;
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[current])();
   palette.refresh(session.snapshot());outline.select(block.id);
   status(ui,"Added "+kind+" to "+(parent||"page root")+". Draft not published.");
  }catch(e){status(ui,e.message,true);}
  finally{busy=false;dirtyControls();}
 }
 async function composePage(compositionId){
  if(busy||!mode)return;
  busy=true;dirtyControls();
  try{
   await flush();
   const next=composeNativePage(session.snapshot(),compositionId,{idFactory:newEditorCoordinate});
   await apply(next);
   const current=mode;await destroy();
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[current])();
   palette.refresh(session.snapshot());outline.refresh(session.snapshot());
   status(ui,"Composed native page sections. Review each section and save this private draft.");
  }catch(error){status(ui,error.message,true);}
  finally{busy=false;dirtyControls();}
 }
 async function apply(next){
  try{
   session.replace(next);changes++;
   palette.refresh(session.snapshot());outline.refresh(session.snapshot());
   status(ui,"Draft updated ("+changes+" edits).");dirtyControls();
  }catch(e){status(ui,e.message,true);throw e;}
 }
 // Compiled Frappe and native Scene material enters the existing editor
 // session with the same undo, native CMS save and publication review.
 async function composeExperience(projection){
  if(busy||!mode)throw Error("Finish the current editing action first.");
  busy=true;dirtyControls();
  try{
   await flush();
   const next=composeAuthoredExperience(session.snapshot(),projection);
   await apply(next);
   const current=mode;await destroy();
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[current])();
   palette.refresh(session.snapshot());outline.refresh(session.snapshot());
   status(ui,"Compiled source composed into the same draft. Review and save before publication.");
   return {blocksAdded:projection.blocks.length};
  }catch(error){status(ui,error.message,true);throw error;}
  finally{busy=false;dirtyControls();}
 }
 // Computed candidates (local models, native reducers, or human suggestions)
 // are ordinary revisions, not a privileged execution or publication pipeline.
 async function acceptProposal(candidate){
  if(busy||!mode)throw Error("Finish the current editing action first.");
  busy=true;dirtyControls();
  try{
   await flush();
   const next=applyAuthoringProposal(session.snapshot(),candidate);
   await apply(next);
   const current=mode;await destroy();
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[current])();
   palette.refresh(session.snapshot());outline.refresh(session.snapshot());
   status(ui,"Proposed changes retained as an unsaved revision. Review before saving.");
   return {accepted:true};
  }catch(error){status(ui,error.message,true);throw error;}
  finally{busy=false;dirtyControls();}
 }
 async function editComposition(operations){
  if(busy||!mode)throw Error("Finish the current editing action first.");
  if(!Array.isArray(operations)||!operations.length)return;
  busy=true;dirtyControls();
  try{
   await flush();
   let next=session.snapshot();
   for(const operation of operations)next=applyAuthoringEdit(next,operation);
   await apply(next);
   if(operations.at(-1).type==="remove")selected=null;
   else if(operations.at(-1).type==="duplicate")selected=operations.at(-1).newId;
   const current=mode;await destroy();
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[current])();
   palette.refresh(session.snapshot());outline.refresh(session.snapshot());
   if(operations.at(-1).type==="duplicate")outline.select(operations.at(-1).newId);
  }catch(error){status(ui,error.message,true);throw error;}
  finally{busy=false;dirtyControls();}
 }
 async function flush(){
  if(!mounted)return;
  const old=mounted;
  if(old.flush)await old.flush();
 }
 async function destroy(){
  if(mounted){const old=mounted;mounted=null;await old.destroy?.();}
  deviceEditor=null;stage.replaceChildren();
 }
 async function openGrapes(){
  const grapesjs=await loaders.grapesjs();
  const canvas=element("div","authoring-canvas");stage.append(canvas);
  const editor=grapesjs.init({container:canvas,height:"75vh",
   storageManager:false,fromElement:false,
   plugins:[registerGrapesComponents],
   deviceManager:{devices:[{id:"desktop",name:"Desktop",width:""},
    {id:"tablet",name:"Tablet",width:"768px"},
    {id:"phone",name:"Phone",width:"390px"}]},
   blockManager:{},
   allowScripts:false});
  const installStyles=()=>{
   const doc=editor.Canvas.getDocument();
   if(doc?.head&&!doc.head.querySelector("[data-aurekai-patterns]")){
    const style=doc.createElement("style");style.setAttribute("data-aurekai-patterns","");
    style.textContent=authoredPatternStyles;doc.head.append(style);
   }
  };
  editor.on("canvas:load",installStyles);
  editor.loadProjectData(toGrapesProject(session.snapshot()));
  deviceEditor=editor;editor.setDevice(device);
  installStyles();
  editor.on("component:selected",cmp=>{selected=cmp?.getAttributes()?.["data-coordinate"]||null;});
  let changed=false;
  editor.on("update",()=>{changed=true;});
  return {flush:async()=>{
   if(changed)await apply(fromGrapesProject(editor.getProjectData(),
     session.snapshot().site_id,session.snapshot().page));
   changed=false;
  },dirty:()=>changed,destroy:()=>editor.destroy()};
 }
 async function openPuck(){
  const [React,ReactDOM,PuckModule]=await loaders.puck();
  const mount=element("div","authoring-puck");stage.append(mount);
  const reactRoot=ReactDOM.createRoot(mount),Puck=PuckModule.Puck;
  let latest=null;
  reactRoot.render(React.createElement(Puck,{
   config:makePuckConfig(React),data:toPuckData(session.snapshot()),
   iframe:{enabled:true,syncHostStyles:true},
   onChange(data){latest=data;},
   onPublish(data){latest=data;void review();}
  }));
  return {flush:async()=>{
   if(latest)await apply(fromPuckData(latest,session.snapshot().site_id,session.snapshot().page));
   latest=null;
  },dirty:()=>latest!==null,destroy:()=>reactRoot.unmount()};
 }
 async function openArticle(){
  const [Core,Header,List,Quote,Table]=await loaders.editorjs();
  const snapshot=session.snapshot();
  const articles=all(snapshot).filter(n=>["profile-story","profile-section","service:prose"].includes(n.kind));
  if(!articles.length)throw Error("Create a story or prose block in Design/Structure before editing its article");
  selected=articles.some(n=>n.id===selected)?selected:articles[0].id;
  const picker=element("select","authoring-article-selector");
  for(const item of articles){const opt=element("option","",item.props.heading||item.id);
   opt.value=item.id;opt.selected=item.id===selected;picker.append(opt);}
  stage.append(picker);
  const holder=element("div","authoring-article");stage.append(holder);
  const EditorJS=Core.default;
  let current=new EditorJS({
   holder,autofocus:true,data:toEditorBlocks(find(snapshot.blocks,selected)),
   tools:{header:{class:Header.default},list:{class:List.default},
    quote:{class:Quote.default},table:{class:Table.default}},
  });
  picker.addEventListener("change",()=>{void (async()=>{
   await saveArticle();await current.destroy();selected=picker.value;
   current=new EditorJS({holder,autofocus:true,
    data:toEditorBlocks(find(session.snapshot().blocks,selected)),
    tools:{header:{class:Header.default},list:{class:List.default},
     quote:{class:Quote.default},table:{class:Table.default}}});
  })().catch(e=>status(ui,e.message,true));});
  async function saveArticle(){
   await current.isReady;
   const body=await current.save();
   await apply(applyEditorBlocks(session.snapshot(),selected,body));
  }
  return {flush:saveArticle,destroy:async()=>{await current.isReady;current.destroy();}};
 }
 async function switchMode(next){
  if(busy||!["grapesjs","puck","editorjs"].includes(next))return;
  busy=true;dirtyControls();
  try{
   await flush();await destroy();mode=next;
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[next])();
   palette.refresh(session.snapshot());status(ui,"Editing "+next+" · same Aurekai draft.");
  }catch(e){status(ui,e.message,true);}
  finally{busy=false;dirtyControls();}
 }
 async function remount(){
  const next=mode;await destroy();mode=null;await switchMode(next);
  palette.refresh(session.snapshot());outline.refresh(session.snapshot());
 }
 async function review(){
  if(busy)return;
  busy=true;dirtyControls();
  try{await flush();await onReview(session.snapshot());reviewedAt=revision();status(ui,"Review opened. Not published.");}
  catch(e){status(ui,e.message,true);}finally{busy=false;dirtyControls();}
 }
 async function save(){
  if(busy)return;
  busy=true;dirtyControls();
  try{await flush();const result=await onSaveDraft(session.snapshot());
   if(!result?.committed)throw Error("Native CMS did not acknowledge a committed draft");
   savedTree=JSON.stringify(session.snapshot());hasSaved=true;metadataDirty=false;
   status(ui,result.published?"Reviewed public edition updated at revision "+String(result.revision||"confirmed")+".":"Draft saved at revision "+String(result.revision||"confirmed")+". Not published.");
  }catch(e){status(ui,e.message,true);}finally{busy=false;dirtyControls();}
 }
 async function importJournal(article){
  if(busy||!mode)throw Error("Finish the current editing action before importing a journal.");
  busy=true;dirtyControls();
  try{
   await flush();
   const snapshot=session.snapshot(),next=mergeJournalIntoTree(snapshot,article);
   await apply(next);
   const currentMode=mode;await destroy();
   mounted=await ({grapesjs:openGrapes,puck:openPuck,editorjs:openArticle}[currentMode])();
   palette.refresh(session.snapshot());
   status(ui,"Imported "+article.sections.length+" journal sections. Review and save the draft.");
   return {blocksAdded:article.sections.length};
  }catch(error){status(ui,error.message,true);throw error;}
  finally{busy=false;dirtyControls();}
 }
 bindModes();outline.refresh(session.snapshot());dirtyControls();const ready=switchMode("grapesjs");
 return {session,mode:()=>mode,switchMode,review,save,importJournal,composeExperience,acceptProposal,ready,
  createArticle:async()=>{await ready;return insertComponent("profile-story",null,{startArticle:true});},
  setPageDirty(value){metadataDirty=!!value;metadataRevision++;dirtyControls();},
  isDirty:()=>metadataDirty||savedTree!==JSON.stringify(session.snapshot())||!!mounted?.dirty?.(),
  destroy:async()=>{await destroy();authoringStyle.remove();root.replaceChildren();}};
}
