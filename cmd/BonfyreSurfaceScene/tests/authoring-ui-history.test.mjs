import {test} from "node:test";
import assert from "node:assert/strict";
import {mountAurekaiAuthoring} from "../../../components/BonfyreServiceSite/authoring/unified-editor.mjs";
import {newAuthoringTree,applyAuthoringEdit} from "../runtime/authoring-tree.mjs";

class Node {
 constructor(tag){this.tag=tag;this.children=[];this.handlers={};this.dataset={};this.textContent="";this.disabled=false;}
 append(...children){this.children.push(...children);}
 replaceChildren(...children){this.children=children;}
 addEventListener(type,callback){this.handlers[type]=callback;}
 setAttribute(name,value){this[name]=value;}
}
const find=(node,label)=>{
 if(node.textContent===label)return node;
 for(const child of node.children){const hit=find(child,label);if(hit)return hit;}
 return null;
};

test("pending canvas edits become undoable before mode switching and redo restores them",async()=>{
 const oldDocument=globalThis.document;
 globalThis.document={createElement:tag=>new Node(tag)};
 const root=new Node("root"),editors=[];
 try{
  const document=applyAuthoringEdit(newAuthoringTree("person-authoring"),{
   type:"insert",block:{id:"section",kind:"profile-section",props:{heading:"Before"},
    responsive:{desktop:{layout:"stack"}},children:[]}
  });
  const api=await mountAurekaiAuthoring(root,{
   initial:document,onReview:async()=>{},onSaveDraft:async()=>({committed:true}),
   loaders:{grapesjs:async()=>({init(){
    const editor={
     project:null,handlers:{},
     loadProjectData(data){this.project=data;},
     getProjectData(){return this.project;},
     on(event,fn){this.handlers[event]=fn;},
     destroy(){}
    };
    editors.push(editor);return editor;
   }})}
  });
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(api.mode(),"grapesjs");
  const undo=find(root,"Undo"),redo=find(root,"Redo");
  assert.equal(undo.disabled,true);
  const editor=editors.at(-1);
  editor.project.pages[0].frames[0].component.components[0].aurekai.heading="After";
  editor.handlers.update();
  assert.equal(undo.disabled,false,"pending upstream changes enable Undo");
  assert.equal(await api.undo(),true);
  assert.equal(api.session.snapshot().blocks[0].props.heading,"Before");
  assert.equal(redo.disabled,false);
  assert.equal(await api.redo(),true);
  assert.equal(api.session.snapshot().blocks[0].props.heading,"After");
  await api.destroy();
 }finally{globalThis.document=oldDocument;}
});

test("Editor.js save serializes the actual text even without an upstream onChange signal",async()=>{
 const previous=globalThis.document;
 globalThis.document={createElement:tag=>new Node(tag)};
 const root=new Node("root"),editors=[];let committed;
 class StubArticle{
  constructor({data}){this.data=structuredClone(data);this.isReady=Promise.resolve();editors.push(this);}
  async save(){return structuredClone({blocks:this.data.blocks});}
  destroy(){}
 }
 const grapes={init(){
  return {loadProjectData(){},on(){},destroy(){},getProjectData(){throw Error("unchanged");}};
 }};
 try{
  const initial=applyAuthoringEdit(newAuthoringTree("person-editor"),{type:"insert",
   block:{id:"story",kind:"profile-story",props:{heading:"A story",body:"Original text"},
    responsive:{desktop:{layout:"stack"}},children:[]}
  });
  const api=await mountAurekaiAuthoring(root,{
   initial,
   loaders:{grapesjs:async()=>grapes,
    editorjs:async()=>[{default:StubArticle},{default:class{}},{default:class{}},
     {default:class{}},{default:class{}}]},
   onReview:async()=>{},
   onSaveDraft:async draft=>{committed=draft;return {committed:true};}
  });
  await new Promise(resolve=>setImmediate(resolve));
  await api.switchMode("editorjs");
  assert.equal(api.mode(),"editorjs");
  const editor=editors.at(-1);
  editor.data.blocks[0].data.text="Actual edited document";
  // A browser may edit a contenteditable before Editor.js broadcasts onChange.
  await api.save();
  assert.equal(committed.blocks[0].props.body,"Actual edited document");
  assert.equal(committed.blocks[0].props.editorial[0].data.text,"Actual edited document");
  const history=api.session.canUndo();
  await api.save();
  assert.equal(api.session.canUndo(),history,"second unchanged save adds no history");
  await api.destroy();
 }finally{globalThis.document=previous;}
});

test("Markdown source mode preserves frontmatter and creates a separate reviewed draft digest",async()=>{
 const previous=globalThis.document;
 globalThis.document={createElement:tag=>new Node(tag)};
 const {createHash}=await import("node:crypto");
 const original="---\nslug: source-story\ntitle: Full source\n---\n\n## Evidence\n\nBefore.";
 const sha=createHash("sha256").update(original).digest("hex");
 const doc=applyAuthoringEdit(newAuthoringTree("account-story"),{type:"insert",
  block:{id:"source-story",kind:"profile-story",props:{body:original,heading:"Source",
   metadata:{format:"markdown",frontmatter:true},source_digest:sha},
   responsive:{phone:{layout:"stack"}},children:[]}});
 const root=new Node("root");
 try{
  const desk=await mountAurekaiAuthoring(root,{
   initial:doc,onSaveDraft:async()=>({committed:true}),onReview:async()=>{},
   loaders:{grapesjs:async()=>({init(){return {
    loadProjectData(){},on(){},destroy(){}
   };}})}
  });
  await new Promise(resolve=>setImmediate(resolve));
  await desk.switchMode("markdown");
  assert.equal(desk.mode(),"markdown");
  const visit=node=>{
   if(node.tag==="textarea")return node;
   for(const child of node.children){const found=visit(child);if(found)return found;}
   return null;
  };
  const textarea=visit(root);
  assert.ok(textarea);
  assert.equal(textarea.value,original);
  textarea.value=original.replace("Before.","After.");
  textarea.handlers.input();
  await desk.switchMode("grapesjs");
  const changed=desk.session.snapshot().blocks[0];
  assert.ok(changed.props.body.endsWith("After."));
  assert.equal(changed.props.source_digest,sha,"original lineage SHA retained");
  assert.equal(changed.props.metadata.changed_from_original,true);
  assert.equal(changed.props.metadata.draft_sha256,
   createHash("sha256").update(changed.props.body).digest("hex"));
  await desk.destroy();
 }finally{globalThis.document=previous;}
});
