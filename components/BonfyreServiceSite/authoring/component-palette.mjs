// Shared creator palette and parent targeting for native-authoring operations.
// This builds new components from declared schemas; it never imports a site.
import {AUTHORING_BLOCKS} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-prototype.mjs";
import {SITE_SECTION_CATALOGUE} from "./site-section-catalogue.generated.mjs";
import {AUTHORING_PATTERNS,PATTERN_BY_KIND,defaultPatternPayload} from "./presentation-patterns.mjs";
import {startingSectionPayload} from "./structured-fields.mjs";
const sectionParents=new Set(["service:hero","service:cases","service:prose"]);
const profileParents=new Set(["profile-section","profile-story"]);
export const AUTHORING_CREATE_OPTIONS=Object.freeze([
 ...AUTHORING_PATTERNS.map(x=>({kind:x.kind,label:x.name.replace(/-/g," "),category:x.group,canContain:x.children})),
 ...SITE_SECTION_CATALOGUE.map(x=>({kind:"service:"+x.kind,label:x.kind.replace(/-/g," "),category:"Applications",canContain:sectionParents.has("service:"+x.kind)})),
 ...AUTHORING_BLOCKS.map(x=>({kind:x.kind,label:x.type.replace(/-/g," "),category:"Profiles",canContain:profileParents.has(x.kind)}))
]);
const options=new Map(AUTHORING_CREATE_OPTIONS.map(x=>[x.kind,x]));
export function newAuthoredComponent(kind,id){
 const meta=options.get(kind);
 if(!meta||!/^[a-z0-9][a-z0-9-]{0,117}$/.test(id))throw TypeError("Unknown authoring item or coordinate");
 const pattern=PATTERN_BY_KIND.get(kind);
 const service=SITE_SECTION_CATALOGUE.find(x=>"service:"+x.kind===kind);
 return {kind,id,props:{
   heading:meta.label.charAt(0).toUpperCase()+meta.label.slice(1),variant:"paper",
   ...(pattern?{payload:defaultPatternPayload(kind)}:{}),
   ...(service?{payload:startingSectionPayload(service)}:{})
  },responsive:{desktop:{layout:meta.canContain?"stack":"full-bleed"}},children:[]};
}
export function availableAuthoringParents(tree){
 const result=[{id:null,label:"Page root",depth:0}];
 const visit=(nodes,depth)=>{
  for(const node of nodes){
   const meta=options.get(node.kind);
   if(meta?.canContain&&node.props?.readOnly!==true)
    result.push({id:node.id,label:(node.props.heading||meta.label).slice(0,90),depth});
   visit(node.children,depth+1);
  }
 };
 visit(tree.blocks,1);
 return result;
}
export function createAuthoringPalette({onInsert,onCompose,compositions=[]}){
 if(typeof document==="undefined"||typeof onInsert!=="function")
  throw TypeError("The authoring palette needs a browser and insertion owner");
 const el=(tag,text)=>{
  const node=document.createElement(tag);
  if(text)node.textContent=text;
  return node;
 };
 const root=el("div");root.className="authoring-create-bar";
 const kindLabel=el("label","Component"),kindInput=el("select");
 kindInput.className="authoring-component-picker";kindInput.setAttribute("aria-label","Component to add");
 const parentLabel=el("label","Insert into"),parentInput=el("select");
 parentInput.className="authoring-parent-picker";parentInput.setAttribute("aria-label","Parent component");
 const button=el("button","Add component");button.type="button";button.className="authoring-insert";
 const grouped=new Map();
 for(const option of AUTHORING_CREATE_OPTIONS){
  if(!grouped.has(option.category)){
   const group=el("optgroup");group.label=option.category;
   kindInput.append(group);grouped.set(option.category,group);
  }
  const item=el("option",option.label);item.value=option.kind;
  grouped.get(option.category).append(item);
 }
 kindInput.value="ui:section";
 kindLabel.append(kindInput);parentLabel.append(parentInput);
 root.append(kindLabel,parentLabel,button);
 const composition=el("section");composition.className="authoring-composition-presets";
 let recipeButton=null,recipeInput=null;
 if(typeof onCompose==="function"&&compositions.length){
  const heading=el("h3","Compose a complete page");
  const description=el("p","Add an editable arrangement of native sections. Existing work stays in this draft.");
  const row=el("div");row.className="authoring-composition-preset-row";
  recipeInput=el("select");recipeInput.setAttribute("aria-label","Editorial composition");
  for(const recipe of compositions){const option=el("option",recipe.label);option.value=recipe.id;recipeInput.append(option);}
  recipeButton=el("button","Compose");recipeButton.type="button";
  recipeButton.addEventListener("click",()=>{if(!recipeButton.disabled)Promise.resolve(onCompose(recipeInput.value)).catch(()=>{});});
  row.append(recipeInput,recipeButton);composition.append(heading,description,row);root.prepend(composition);
 }
 button.addEventListener("click",()=>{
  if(button.disabled)return;
  Promise.resolve(onInsert(kindInput.value,parentInput.value||null)).catch(()=>{});
 });
 return {element:root,refresh(tree){
   const prior=parentInput.value;
   const parents=availableAuthoringParents(tree);
   parentInput.replaceChildren();
   for(const parent of parents){
    const item=el("option",("— ".repeat(Math.max(0,parent.depth-1)))+parent.label);
    item.value=parent.id||"";parentInput.append(item);
   }
   parentInput.value=parents.some(item=>item.id===prior)?prior:"";
  },setBusy(busy){
   button.disabled=!!busy;if(recipeButton)recipeButton.disabled=!!busy;if(recipeInput)recipeInput.disabled=!!busy;kindInput.disabled=!!busy;parentInput.disabled=!!busy;
  },setKind(kind){if(options.has(kind))kindInput.value=kind;},
  get selection(){return {kind:kindInput.value,parent:parentInput.value||null};}};
}
/* Account composability: a source-independent outline over the same typed tree.
 * No imported templates, HTML, CSS or alternate persistence route. */
export function createAuthoringOutline({onEdit,onMode}){
 if(typeof document==="undefined"||typeof onEdit!=="function")
  throw TypeError("The composition outline needs a browser and edit owner");
 const el=(tag,cls="",label="")=>{
  const node=document.createElement(tag);if(cls)node.className=cls;
  if(label)node.textContent=label;return node;
 };
 const panel=el("section","authoring-composition");
 panel.setAttribute("aria-label","Page composition");
 const heading=el("h2","","Compose this page");
 const explanation=el("p","","Select a component to edit its text and layout. Drag onto a sibling to reorder; hold Shift while dropping on a container to nest. Use Move to for keyboard-accessible reparenting.");
 const list=el("ol","authoring-composition-tree");
 const inspector=el("div","authoring-composition-inspector");
 const status=el("p","authoring-composition-status");
 status.setAttribute("role","status");status.setAttribute("aria-live","polite");
 panel.append(heading,explanation,list,inspector,status);
 let tree=null,selected=null,busy=false;
 const position=id=>{
  const visit=(nodes,parent,depth)=>{
   for(let i=0;i<nodes.length;i++){
    if(nodes[i].id===id)return {node:nodes[i],parent,index:i,count:nodes.length,depth};
    const found=visit(nodes[i].children,nodes[i].id,depth+1);
    if(found)return found;
   }
   return null;
  };
  return tree?visit(tree.blocks,null,0):null;
 };
 const button=(label,action,disabled=false)=>{
  const control=el("button","",label);control.type="button";control.disabled=disabled||busy;
  control.addEventListener("click",()=>{void action();});return control;
 };
 const commit=async ops=>{
  if(busy||!ops.length)return;
  busy=true;draw();
  try{await onEdit(ops);status.textContent="Composition updated. Save the draft to retain it.";}
  catch(error){status.textContent=error.message;}
  finally{busy=false;draw();}
 };
 function draw(){
  list.replaceChildren();inspector.replaceChildren();
  if(!tree)return;
  const blocks=[];
  const visit=(nodes,depth)=>{
   for(const node of nodes){blocks.push({node,depth});visit(node.children,depth+1);}
  };
  visit(tree.blocks,0);
  if(!blocks.length){
   list.append(el("li","","Empty page — add your first component above."));
   return;
  }
  if(!selected||!position(selected))selected=blocks[0].node.id;
  for(const {node,depth}of blocks){
   const item=el("li","authoring-composition-row");
   item.style.setProperty("--nest-level",String(Math.min(depth,16)));
   item.draggable=!busy&&node.props.readOnly!==true;
   item.addEventListener("dragstart",event=>{
    if(busy||node.props.readOnly===true){event.preventDefault();return;}
    event.dataTransfer?.setData("text/plain",node.id);
    if(event.dataTransfer)event.dataTransfer.effectAllowed="move";
   });
   item.addEventListener("dragover",event=>{
    if(!busy&&event.dataTransfer?.types?.includes("text/plain"))event.preventDefault();
   });
   item.addEventListener("drop",event=>{
    if(busy)return;
    event.preventDefault();
    const from=position(event.dataTransfer?.getData("text/plain"));
    const target=position(node.id);
    if(!from||!target||from.node.id===node.id||from.node.props.readOnly===true)return;
    const nest=event.shiftKey&&options.get(node.kind)?.canContain&&node.props.readOnly!==true;
    const parent=nest?node.id:target.parent;
    let index=nest?node.children.length:target.index+
     (event.clientY>=item.getBoundingClientRect().top+item.getBoundingClientRect().height/2?1:0);
    if(from.parent===parent&&from.index<index)index--;
    void commit([{type:"move",id:from.node.id,parent,index}]);
   });
   const pick=button((node.props.heading||node.kind)+" · "+node.kind,async()=>{
    selected=node.id;draw();
   });
   pick.dataset.coordinate=node.id;
   pick.setAttribute("aria-current",String(selected===node.id));
   item.append(pick);list.append(item);
  }
  const loc=position(selected),node=loc.node,readOnly=node.props.readOnly===true;
  inspector.append(el("h3","","Selected: "+node.kind));
  const controls=el("div","authoring-composition-actions");
  controls.append(
   button("Move up",()=>commit([{type:"move",id:selected,parent:loc.parent,index:loc.index-1}]),loc.index===0||readOnly),
   button("Move down",()=>commit([{type:"move",id:selected,parent:loc.parent,index:loc.index+1}]),loc.index===loc.count-1||readOnly),
   button("Duplicate",()=>commit([{type:"duplicate",id:selected,newId:newCoordinate()}]),readOnly),
   button("Remove",()=>commit([{type:"remove",id:selected}]),readOnly),
   button("Edit structure",()=>onMode?.("puck"),false),
   button("Edit article",()=>onMode?.("editorjs"),!["profile-story","profile-section","service:prose"].includes(node.kind))
  );
  inspector.append(controls);
  const descendants=new Set([selected]);
  const gather=item=>{for(const child of item.children){descendants.add(child.id);gather(child);}};
  gather(node);
  const moveGroup=el("div","authoring-composition-move");
  const moveLabel=el("label","","Move to");
  const moveSelect=el("select");moveSelect.name="composition-parent";
  for(const parent of availableAuthoringParents(tree).filter(x=>!x.id||!descendants.has(x.id))){
   const option=el("option","",parent.label);option.value=parent.id||"";moveSelect.append(option);
  }
  moveSelect.value=loc.parent||"";
  moveSelect.disabled=busy||readOnly;
  moveLabel.append(moveSelect);
  const moveHere=button("Move here",()=>{
   const parent=moveSelect.value||null;
   const siblings=parent?position(parent)?.node.children:tree.blocks;
   if(!siblings)return;
   const index=siblings.length-(parent===loc.parent?1:0);
   void commit([{type:"move",id:selected,parent,index}]);
  },readOnly);
  moveGroup.append(moveLabel,moveHere);inspector.append(moveGroup);
  const makeField=(title,tag,value)=>{
   const label=el("label","",title),input=el(tag);
   input.value=value;input.disabled=busy||readOnly;
   label.append(input);return {label,input};
  };
  const title=makeField("Heading","input",node.props.heading||"");
  title.input.name="composition-heading";title.input.maxLength=220;
  const rich=Array.isArray(node.props.editorial);
  const body=makeField(rich?"Rich article text (edit in Article mode)":"Body text","textarea",node.props.body||"");
  body.input.name="composition-body";body.input.maxLength=50000;
  body.input.disabled=busy||readOnly||rich;body.input.rows=3;
  const saveText=button("Apply text",()=>{
   const changes=[];
   if(title.input.value!==node.props.heading)changes.push({type:"update",id:selected,field:"heading",value:title.input.value});
   if(!rich&&body.input.value!==(node.props.body||""))changes.push({type:"update",id:selected,field:"body",value:body.input.value});
   void commit(changes);
  },readOnly);
  const textForm=el("div","authoring-composition-fields");
  textForm.append(title.label,body.label,saveText);inspector.append(textForm);
  const layoutGroup=el("div","authoring-composition-layout");
  const device=makeField("Viewport","select","");
  for(const v of ["desktop","tablet","phone"]){
   const option=el("option","",v);option.value=v;device.input.append(option);
  }
  const layout=makeField("Layout","select","");
  for(const v of ["stack","grid","columns","carousel","masonry","split","full-bleed","feature"]){
   const option=el("option","",v);option.value=v;layout.input.append(option);
  }
  const columns=makeField("Columns (1–12)","input","");
  columns.input.type="number";columns.input.min="1";columns.input.max="12";columns.input.step="1";
  const loadDevice=()=>{
   const value=node.responsive?.[device.input.value]||{};
   layout.input.value=value.layout||"stack";
   columns.input.value=String(value.columns||2);
  };
  device.input.addEventListener("change",loadDevice);loadDevice();
  const applyLayout=button("Apply layout",()=>{
   const style={...(node.responsive?.[device.input.value]||{}),layout:layout.input.value};
   if(["grid","columns"].includes(style.layout))style.columns=Number(columns.input.value);
   else delete style.columns;
   void commit([{type:"style",id:selected,viewport:device.input.value,value:style}]);
  },readOnly);
  layoutGroup.append(device.label,layout.label,columns.label,applyLayout);
  inspector.append(layoutGroup);
 }
 function newCoordinate(){
  if(!globalThis.crypto?.getRandomValues)throw Error("Secure browser identity required");
  const bytes=new Uint8Array(12);globalThis.crypto.getRandomValues(bytes);
  return "node-"+Array.from(bytes,n=>n.toString(16).padStart(2,"0")).join("");
 }
 return {element:panel,refresh(next){
  tree=structuredClone(next);draw();
 },select(id){if(position(id)){selected=id;draw();}},
 setBusy(value){busy=!!value;draw();}};
}
