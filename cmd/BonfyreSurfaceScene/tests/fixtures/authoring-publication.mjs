import {newAuthoringTree,validateAuthoringTree} from "../../runtime/authoring-tree.mjs";

const e=(type,data,id)=>({type,data,id});
export function journalScene(){
 const d=newAuthoringTree("aurekai-journal","/journal/reading-room/");
 d.blocks=[
 {id:"hero",kind:"profile-hero",props:{eyebrow:"AUREKAI / JOURNAL",heading:"The public record is a process, not a file",
  body:"An investigation into testimony, dates, redaction &amp; provenance.",variant:"night",
  metadata:{edition:90,rights:{state:"review-required",source:"council:records"}}},responsive:{
   desktop:{layout:"full-bleed",padding:48,typography:"serif",background:"reversed"},
   phone:{layout:"stack",padding:12,typography:"serif"}},children:[]},
 {id:"contents",kind:"profile-section",props:{heading:"Inside the record",variant:"paper"},responsive:{
  desktop:{layout:"grid",columns:2,gap:28,width:"wide"},
  phone:{layout:"stack",columns:1,gap:12}},children:[
  {id:"chapter",kind:"profile-story",props:{heading:"Where the evidence goes",
   body:"A source may be timely without being complete.",
   editorial:[
    e("paragraph",{text:"An initial note & a later amendment."},"p1"),
    e("header",{text:"The first distinction",level:3},"h1"),
    e("quote",{text:"A public record is not the whole event.",caption:"Editorial observation"},"q1"),
    e("list",{style:"unordered",items:["Source","Decision","Publication"]},"l1"),
    e("table",{withHeadings:true,content:[["Kind","Evidence"],["Notice","Timestamp"],["Hearing","Testimony"]]},"t1")
   ],bindings:{source:{type:"public-council",keys:["file","date"]}},
   source_ref:"source:city-council",source_digest:"a".repeat(64)},
   responsive:{desktop:{layout:"stack",padding:18,typography:"editorial"},
    phone:{layout:"stack",padding:10}},children:[
     {id:"footnote",kind:"profile-quote",props:{body:"Every claim remains attached to its origin."},
       responsive:{},children:[]}
    ]},
  {id:"media",kind:"service:media",props:{heading:"From the archive",
   asset_ref:"media:council-001",alt:"A public reading room",
   caption:"Public record, source image pending rights confirmation.",
   metadata:{credit:"Public archive",verified:false}},responsive:{},children:[]}
 ]},
 {id:"next",kind:"profile-action",props:{heading:"Follow the documents",label:"Research primary evidence",
   href:"https://www.archives.gov/research",variant:"forest"},responsive:{},children:[]}
 ];
 return validateAuthoringTree(d);
}
export function profileScene(){
 const d=newAuthoringTree("person-example","/projects/");
 d.blocks=[
  {id:"profile-hero",kind:"profile-hero",props:{heading:"Design systems & public software",
    body:"Products, prototypes, and editorial work.",variant:"rust"},
   responsive:{desktop:{layout:"split",columns:2},phone:{layout:"stack"}},children:[]},
  {id:"work",kind:"service:cases",props:{heading:"Selected work"},
   responsive:{desktop:{layout:"grid",columns:3,gap:18},tablet:{layout:"grid",columns:2},phone:{layout:"stack"}},children:[
    {id:"project-a",kind:"profile-card",props:{heading:"Research-led publication",
     body:"Evidence-led writing with durable source history.",asset_ref:"media:project-a",
     caption:"Project image awaiting verified asset resolution."},responsive:{},children:[]},
    {id:"project-b",kind:"profile-card",props:{heading:"A public-facing product",
     metadata:{roles:["architecture","development"],years:[2025,2026]},
     bindings:{application:"source-library",readonly:false}},responsive:{},children:[]}
   ]}
 ];
 return validateAuthoringTree(d);
}
