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
