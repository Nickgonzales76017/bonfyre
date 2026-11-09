// Shared responsive inspector for the canonical Bonfÿre authoring tree.
// All mutations are admitted through applyAuthoringEdit, never DOM export.
const enumOptions={
 layout:["stack","grid","columns","carousel","masonry","split","full-bleed","feature"],
 align:["start","center","end","stretch","between"],
 justify:["start","center","end","stretch","between"],
 background:["default","surface","panel","muted","accent","reversed","dark","light"],
 typography:["body","display","serif","sans","mono","editorial"],
 width:["auto","narrow","medium","wide","full"]
};
const themes=["paper","forest","night","rust","wide","slate","editorial"];
const styling=["layout","columns","span","gap","align","justify","padding","radius","background","typography","width"];
const fields=[["Heading","heading"],["Eyebrow","eyebrow"],["Body","body"],
 ["Button label","label"],["HTTPS link","href"],["Image description","alt"],
 ["Credit / caption","caption"],["Media reference","asset_ref"]];
const el=(name,cls="",text="")=>{
 const v=document.createElement(name);if(cls)v.className=cls;if(text)v.textContent=text;return v;
};
const flatten=doc=>{
 const found=[];function visit(nodes,parent=null){
  nodes.forEach((node,index)=>{found.push({node,parent,index,length:nodes.length});visit(node.children,node.id);});
 }visit(doc.blocks);return found;
};
export function mountAuthoringInspector(root,{snapshot,selected,onSelect,onEdit}){
 let viewport="desktop",sequence=0,closed=false;
 function render(){
  if(closed)return;
  root.replaceChildren();
  const rows=flatten(snapshot()),current=rows.find(x=>x.node.id===selected())||rows[0];
  if(!current){root.append(el("p","authoring-inspector-note","Create a component to configure layout and media."));return;}
  const {node,parent,index,length}=current,locked=!!node.props.readOnly;
  if(node.id!==selected())onSelect(node.id);
  root.append(el("h2","","Component inspector"),
   el("p","authoring-inspector-note","One document across Design, Structure and Article."));
  const control=(label,value,options,act,{multi=false,numeric=false,disabled=false}={})=>{
   const wrap=el("label","authoring-inspector-field");wrap.append(el("span","",label));
   const input=el(options?"select":multi?"textarea":"input","authoring-inspector-control");
   if(options)for(const [key,title] of options){
    const option=el("option","",title);option.value=key;option.selected=String(value??"")===key;input.append(option);
   }else{
    input.value=String(value??"");
    if(multi)input.rows=4;
    if(numeric){input.type="number";input.min="0";input.max="64";input.step="1";}
   }
   input.disabled=disabled;input.setAttribute("aria-label",label);
   input.addEventListener("change",()=>void act(input.value));
   wrap.append(input);root.append(wrap);
  };
  control("Component",node.id,rows.map(x=>[x.node.id,(x.node.props.heading||x.node.kind).slice(0,52)]),
   v=>{onSelect(v);render();});
  root.append(el("h3","authoring-inspector-heading","Content"));
  for(const [label,key] of fields){
   if(key==="body"&&node.props.metadata?.format==="markdown"){
    root.append(el("p","authoring-inspector-note","Edit complete Markdown in Source mode to keep original provenance."));
    continue;
   }
   control(label,node.props[key],null,v=>onEdit({type:"update",id:node.id,field:key,value:v},label),
    {multi:key==="body"||key==="caption",disabled:locked});
  }
  control("Appearance",node.props.variant||"paper",themes.map(x=>[x,x]),
   v=>onEdit({type:"update",id:node.id,field:"variant",value:v},"Appearance"),{disabled:locked});
  root.append(el("h3","authoring-inspector-heading","Responsive layout"));
  control("Viewport",viewport,[["desktop","Desktop"],["tablet","Tablet"],["phone","Phone"]],
   v=>{viewport=v;render();});
  const currentStyle=node.responsive?.[viewport]||{};
  for(const key of styling){
   const options=enumOptions[key]?[["","Use default"],...enumOptions[key].map(x=>[x,x])]:null;
   control(key[0].toUpperCase()+key.slice(1),currentStyle[key],options,value=>{
    const style={...currentStyle};
    if(value==="")delete style[key];
    else if(options)style[key]=value;
    else{if(!/^\d+$/.test(value))return;style[key]=Number(value);}
    onEdit({type:"style",id:node.id,viewport,value:style},viewport+" "+key);
   },{numeric:!options,disabled:locked});
  }
  root.append(el("h3","authoring-inspector-heading","Arrangement"));
  const actions=el("div","authoring-inspector-actions");
  const button=(name,make,disabled=false)=>{
   const b=el("button","",name);b.type="button";b.disabled=locked||disabled;
   b.addEventListener("click",()=>void onEdit(make(),name));actions.append(b);
  };
  button("Move up",()=>({type:"move",id:node.id,parent,index:index-1}),index===0);
  button("Move down",()=>({type:"move",id:node.id,parent,index:index+1}),index>=length-1);
  button("Duplicate",()=>{
   let id;do{id=node.id.slice(0,65)+"-copy-"+(++sequence);}while(rows.some(x=>x.node.id===id));
   return {type:"duplicate",id:node.id,newId:id};
  });
  button("Remove",()=>({type:"remove",id:node.id}));
  root.append(actions);
  if(node.props.source_ref||node.props.source_digest){
   root.append(el("h3","authoring-inspector-heading","Original source"),
    el("p","authoring-inspector-note","Reference only. A draft does not grant publication rights."));
   if(node.props.source_ref)root.append(el("code","authoring-inspector-source",node.props.source_ref));
   if(node.props.source_digest)
    root.append(el("code","authoring-inspector-source",
     "SHA-256 "+node.props.source_digest.slice(0,24)+"…"));
  }
 }
 render();
 return {render,dispose(){closed=true;root.replaceChildren();}};
}
