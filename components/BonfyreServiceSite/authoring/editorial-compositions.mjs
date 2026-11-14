// Complete native page arrangements authored from the existing component vocabulary.
// These are editorial starting compositions, never imported sites or publishable claims.
import {newAuthoredComponent} from "./component-palette.mjs";
import {applyAuthoringEdit,validateAuthoringTree} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
const b=(kind,props={},children=[],responsive)=>({kind,props,children,...(responsive?{responsive}:{})});
export const PAGE_COMPOSITIONS=Object.freeze([
 {id:"editorial-feature",label:"Editorial feature",description:"Story lead, sections, pull quote, evidence and continuing reading.",blocks:[
  b("ui:breadcrumb",{payload:{items:[{label:"Journal",href:"/blog/"},{label:"Feature",href:""}]}}),
  b("ui:hero",{heading:"Feature opening",payload:{eyebrow:"Editorial / Feature",title:"An idea worth exploring",lede:"Give the reader the central question and why it matters.",image:{src:"",alt:""},actions:[]}}),
  b("profile-story",{heading:"The opening",body:"Write a purposeful opening that gives the reader context, stakes and the central observation."}),
  b("ui:section",{heading:"The reporting",payload:{heading:"The reporting",lede:"Move through evidence and implications in a readable sequence."}},[
   b("profile-story",{heading:"What we observed",body:"Explain the central evidence with precise source references."}),
   b("profile-story",{heading:"Why it matters",body:"Connect the findings to decisions and practical consequences."}),
   b("profile-story",{heading:"What remains unresolved",body:"State limitations and outstanding questions without guessing."})
  ],{desktop:{layout:"stack",gap:22},phone:{layout:"stack",gap:14}}),
  b("profile-quote",{heading:"Quoted perspective",body:"Insert an exact, attributed and rights-cleared quotation, or remove this block."}),
  b("ui:columns",{heading:"Reference and next reading",payload:{columns:2,gap:20}},[
   b("ui:card",{heading:"Evidence",payload:{title:"Source material",body:"Link verified references in this article and explain their relevance.",image:{src:"",alt:""},actions:[]}}),
   b("ui:card",{heading:"Next",payload:{title:"Continue the story",body:"Connect the article to another reviewed page or useful application.",image:{src:"",alt:""},actions:[]}})
  ],{desktop:{layout:"columns",columns:2,gap:20},phone:{layout:"stack"}})
 ]},
 {id:"publication-front",label:"Publication front",description:"A magazine-style introduction with a lead feature, subject shelves and editorial context.",blocks:[
  b("ui:hero",{heading:"Publication masthead",payload:{eyebrow:"Journal / Issue",title:"Notes from the work",lede:"Explain what this publication follows and why it is worth reading.",image:{src:"",alt:""},actions:[]}}),
  b("ui:columns",{heading:"Lead story and editor note",payload:{columns:2,gap:22}},[
   b("ui:card",{heading:"Lead feature",payload:{title:"Start with the strongest story",body:"Select a reviewed article and link it through its source-owned page.",image:{src:"",alt:""},actions:[]}}),
   b("profile-story",{heading:"From the editor",body:"An original note on what connects these pieces and the work behind them."})
  ],{desktop:{layout:"feature",gap:22},phone:{layout:"stack"}}),
  b("ui:section",{heading:"Selected reading",payload:{heading:"Selected reading",lede:"Organize your released articles by meaning, not by import order."}},[
   b("ui:grid",{heading:"Editorial grid",payload:{columns:3,gap:18}},[
    b("ui:card",{heading:"Research",payload:{title:"Research feature",body:"Replace with an explicitly reviewed article and destination.",image:{src:"",alt:""},actions:[]}}),
    b("ui:card",{heading:"Engineering",payload:{title:"Engineering story",body:"Connect this card to published engineering work.",image:{src:"",alt:""},actions:[]}}),
    b("ui:card",{heading:"Business",payload:{title:"Business perspective",body:"Connect this card to a published business article.",image:{src:"",alt:""},actions:[]}})
   ],{desktop:{layout:"grid",columns:3,gap:18},tablet:{layout:"grid",columns:2},phone:{layout:"stack"}})
  ]),
  b("profile-story",{heading:"About this publication",body:"Describe the editorial purpose, provenance and how readers can follow future issues."})
 ]},
 {id:"research-report",label:"Research report",description:"A research question, method, evidence comparison, caveats and conclusion.",blocks:[
  b("ui:hero",{heading:"Research opening",payload:{eyebrow:"Research / Findings",title:"The question we investigated",lede:"Lead with what is known, the research boundary and the reader decision.",image:{src:"",alt:""},actions:[]}}),
  b("ui:columns",{heading:"Question and method",payload:{columns:2,gap:20}},[
   b("profile-story",{heading:"Question and scope",body:"Define the decision, time period and exact research scope."}),
   b("profile-story",{heading:"Method and sources",body:"Describe verified sources, dates, sampling and limitations."})
  ],{desktop:{layout:"columns",columns:2},phone:{layout:"stack"}}),
  b("ui:table",{heading:"Evidence comparison",payload:{headers:["Source","Observation","Confidence"],rows:[["Source to verify","Record a dated observation","Not yet reviewed"],["Second source","Record its independent observation","Not yet reviewed"]]}}),
  b("ui:section",{heading:"Assessment",payload:{heading:"Assessment",lede:"Keep findings, interpretation and uncertainty distinct."}},[
   b("profile-story",{heading:"Findings",body:"Explain what the evidence actually supports."}),
   b("profile-story",{heading:"Uncertainty",body:"Explain conflicting evidence and what would change this conclusion."}),
   b("profile-story",{heading:"Recommendation",body:"State the practical next step with its assumptions."})
  ],{desktop:{layout:"stack",gap:24},phone:{layout:"stack"}}),
  b("ui:card",{heading:"Research continuation",payload:{title:"Keep investigating",body:"Connect the reader to the next verified question or application.",image:{src:"",alt:""},actions:[]}})
 ]},
 {id:"life-introduction",label:"Life introduction",description:"An editable public Life story, gallery and conversation opener. Dating participation remains a separate private choice.",blocks:[
  b("ui:hero",{heading:"Your Life masthead",payload:{eyebrow:"Aurekai Life",title:"An introduction, in your own words",lede:"Write a short introduction that sounds like you.",image:{src:"",alt:""},actions:[]}}),
  b("ui:columns",{heading:"A little more about me",payload:{columns:2,gap:22}},[
   b("profile-story",{heading:"What I care about",body:"Introduce the people, places and work that matter to you."}),
   b("profile-story",{heading:"Where I spend my time",body:"Share a few interests or projects that you are comfortable publishing."})
  ],{desktop:{layout:"columns",columns:2,gap:22},phone:{layout:"stack"}}),
  b("ui:section",{heading:"My stories",payload:{heading:"My stories",lede:"Choose the work and moments that you want to share."}},[
   b("ui:grid",{heading:"Things worth sharing",payload:{columns:2,gap:18}},[
    b("ui:card",{heading:"A place",payload:{title:"A place I return to",body:"Write about a place or experience you want to remember.",image:{src:"",alt:""},actions:[]}}),
    b("ui:card",{heading:"A project",payload:{title:"Something I am making",body:"Share a project or piece of work that you own.",image:{src:"",alt:""},actions:[]}})
   ],{desktop:{layout:"grid",columns:2,gap:18},phone:{layout:"stack"}})
  ]),
  b("profile-story",{heading:"A conversation opener",body:"Write a short public thought or question you would welcome discussing.",
   metadata:{excerpt_context:"life-introduction"}}),
  b("ui:card",{heading:"More from my profile",payload:{title:"More to explore",body:"Link to a published article, gallery or community on your own Life site.",image:{src:"",alt:""},actions:[]}})
 ]}
]);
const byId=new Map(PAGE_COMPOSITIONS.map(x=>[x.id,x]));
export function composeNativePage(source,compositionId,{idFactory}={}){
 const spec=byId.get(compositionId);if(!spec)throw TypeError("Unknown page composition");
 if(typeof idFactory!=="function")throw TypeError("Secure component identity owner required");
 let next=structuredClone(validateAuthoringTree(source));
 function insert(part,parent){
  const id=idFactory(),node=newAuthoredComponent(part.kind,id);
  node.props={...node.props,...structuredClone(part.props)};
  if(part.responsive)node.responsive=structuredClone(part.responsive);
  next=applyAuthoringEdit(next,{type:"insert",parent,block:node});
  for(const child of part.children)insert(child,id);
 }
 for(const item of spec.blocks)insert(item,null);
 return validateAuthoringTree(next);
}
