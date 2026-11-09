// One Aurekai authoring desk: real OSS editors, one retained typed tree.
// GrapesJS handles visual canvas/blocks/styles; Puck owns typed nested layouts;
// Editor.js owns selected article text. Editors NEVER save raw HTML/CSS to CMS.
import {AUTHORING_BLOCKS} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-prototype.mjs";
import {SERVICE_COMPONENTS,createAuthoringSession,validateAuthoringTree,
 toPuckData,fromPuckData,toGrapesProject,fromGrapesProject,
 toEditorBlocks,applyEditorBlocks} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";

const kinds=[...AUTHORING_BLOCKS.map(x=>x.kind),...SERVICE_COMPONENTS.map(x=>"service:"+x)];
const groups=new Set(["profile-section","service:cases","service:prose","service:hero"]);
const fields=["heading","body","eyebrow","label","href","variant"];
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
  const section=groups.has(kind);
  const inputs={
   heading:{type:"text",label:"Heading"},
   body:{type:"textarea",label:"Body"},
   eyebrow:{type:"text",label:"Eyebrow"},
   label:{type:"text",label:"Button label"},
   href:{type:"text",label:"Source / link"},
   variant:{type:"select",label:"Appearance",options:
    ["paper","forest","night","rust","wide","slate","editorial"].map(value=>({label:value,value}))}
  };
  if(section)inputs.children={type:"slot"};
  defs[kind]={
   label:kind.replace(/^(profile-|service:)/,"").replace(/-/g," "),
   fields:inputs,
   defaultProps:{heading:"New "+kind.replace(/^(profile-|service:)/,""),variant:"paper",
    ...(section?{children:[]}:{}),responsive:{}},
   render:props=>{
    const children=typeof props.children==="function"?React.createElement(props.children):null;
    return React.createElement("section",{"data-authoring-kind":kind,"data-look":props.variant||"paper"},
     React.createElement("small",null,props.eyebrow||""),
     React.createElement("h2",null,props.heading||"Untitled"),
     React.createElement("p",null,props.body||""),children);
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
  editor.Components.addType(id,{model:{
   defaults:{tagName:"section",name:kind,draggable:true,droppable:groups.has(kind),
    attributes:{"data-aurekai-kind":kind},aurekai:{heading:"New block",variant:"paper"},
    responsive:{},traits},
   init(){
    const block=this.get("aurekai")||{};
    for(const f of fields)if(block[f]!==undefined)this.set(f,block[f],{silent:true});
    for(const f of fields)this.on("change:"+f,()=>{
     this.set("aurekai",{...(this.get("aurekai")||{}),[f]:this.get(f)});
    });
   }
  }});
  editor.Blocks.add(id,{label:kind.replace(/^(profile-|service:)/,"").replace(/-/g," "),
   category:kind.startsWith("service:")?"Applications":"Profiles",
   content:{type:id,aurekai:{heading:"New block",variant:"paper"},components:[]}});
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
export async function mountAurekaiAuthoring(root,{initial,loaders=deps,onReview,onSaveDraft}={}){
 if(!root||typeof root.replaceChildren!=="function")throw Error("A DOM host is required");
 if(typeof onSaveDraft!=="function"||typeof onReview!=="function")
  throw Error("Destination-owned save and review callbacks are required");
 const session=createAuthoringSession(validateAuthoringTree(initial));
 const desk=element("div","aurekai-authoring");
 const toolbar=element("header","authoring-toolbar"),modeBar=element("nav","authoring-modes");
 const actions=element("div","authoring-actions"),statusBox=element("p","authoring-status");
 const stage=element("main","authoring-stage"),modeButtons={};
 const ui={desk,toolbar,modeBar,actions,status:statusBox,stage};
 let mounted=null,mode=null,selected=null,busy=false,changes=0;
 const controls=new Map();
 const makeButton=(label,parent,handler)=>{
  const btn=element("button","",label);btn.type="button";btn.addEventListener("click",handler);
  parent.append(btn);return btn;
 };
 toolbar.append(element("h1","","Aurekai · Edit your site"),modeBar,actions);
 desk.append(toolbar,statusBox,stage);
 root.replaceChildren(desk);
 function bindModes(){
  for(const [key,label]of [["grapesjs","Design"],["puck","Structure"],["editorjs","Article"]])
   modeButtons[key]=makeButton(label,modeBar,()=>{void switchMode(key);});
  controls.set("undo",makeButton("Undo",actions,()=>{if(session.undo())void remount();}));
  controls.set("redo",makeButton("Redo",actions,()=>{if(session.redo())void remount();}));
  controls.set("review",makeButton("Review",actions,()=>void review()));
  controls.set("save",makeButton("Save draft",actions,()=>void save()));
 }
 function dirtyControls(){
  controls.get("undo").disabled=!session.canUndo()||busy;
  controls.get("redo").disabled=!session.canRedo()||busy;
  controls.get("review").disabled=busy;controls.get("save").disabled=busy;
  for(const [name,button]of Object.entries(modeButtons))
   button.setAttribute("aria-current",String(mode===name));
 }
 async function apply(next){
  try{session.replace(next);changes++;status(ui,"Draft updated ("+changes+" edits).");dirtyControls();}
  catch(e){status(ui,e.message,true);throw e;}
 }
 async function flush(){
  if(!mounted)return;
  const old=mounted;
  if(old.flush)await old.flush();
 }
 async function destroy(){
  if(mounted){const old=mounted;mounted=null;await old.destroy?.();}
  stage.replaceChildren();
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
  editor.loadProjectData(toGrapesProject(session.snapshot()));
  editor.on("component:selected",cmp=>{selected=cmp?.getAttributes()?.["data-coordinate"]||null;});
  let changed=false;
  editor.on("update",()=>{changed=true;});
  return {flush:async()=>{
   if(changed)await apply(fromGrapesProject(editor.getProjectData(),
     session.snapshot().site_id,session.snapshot().page));
   changed=false;
  },destroy:()=>editor.destroy()};
 }
 async function openPuck(){
  const [React,ReactDOM,PuckModule]=await loaders.puck();
  const mount=element("div","authoring-puck");stage.append(mount);
  const reactRoot=ReactDOM.createRoot(mount),Puck=PuckModule.Puck;
  let latest=null;
  reactRoot.render(React.createElement(Puck,{
   config:makePuckConfig(React),data:toPuckData(session.snapshot()),
   onChange(data){latest=data;},
   onPublish(data){latest=data;void review();}
  }));
  return {flush:async()=>{
   if(latest)await apply(fromPuckData(latest,session.snapshot().site_id,session.snapshot().page));
   latest=null;
  },destroy:()=>reactRoot.unmount()};
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
   status(ui,"Editing "+next+" · same Aurekai draft.");
  }catch(e){status(ui,e.message,true);}
  finally{busy=false;dirtyControls();}
 }
 async function remount(){const next=mode;await destroy();await switchMode(next);}
 async function review(){
  if(busy)return;
  busy=true;dirtyControls();
  try{await flush();await onReview(session.snapshot());status(ui,"Review opened. Not published.");}
  catch(e){status(ui,e.message,true);}finally{busy=false;dirtyControls();}
 }
 async function save(){
  if(busy)return;
  busy=true;dirtyControls();
  try{await flush();const result=await onSaveDraft(session.snapshot());
   if(!result?.committed)throw Error("Native CMS did not acknowledge a committed draft");
   status(ui,"Draft saved at revision "+String(result.revision||"confirmed")+". Not published.");
  }catch(e){status(ui,e.message,true);}finally{busy=false;dirtyControls();}
 }
 bindModes();dirtyControls();void switchMode("grapesjs");
 return {session,mode:()=>mode,switchMode,review,save,destroy:async()=>{await destroy();root.replaceChildren();}};
}
