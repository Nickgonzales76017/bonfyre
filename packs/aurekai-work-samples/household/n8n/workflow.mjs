// Builds an importable, inactive n8n workflow. It uses only local read operations.
import { writeFileSync } from 'node:fs';

export function householdWorkflow({id='aurekaiHouseholdReview',origin='http://127.0.0.1:4177',query='Where is the dishwasher receipt?',mode='semantic'}={}) {
  const url = new URL(origin);
  if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || url.username || url.password || url.pathname !== '/' || url.search || url.hash)
    throw Error('The sample requires a loopback workspace origin.');
  const nodes = [];
  function node(name,type,typeVersion,parameters,position) {
    nodes.push({id:name.toLowerCase().replaceAll(' ','-'),name,type:'n8n-nodes-base.'+type,typeVersion,parameters,position});
  }
  const session = `={{ $('Open local workspace').first().json.data.match(/name="document-session" content="([^"]+)"/)[1] }}`;
  const headers = {sendHeaders:true,headerParameters:{parameters:[{name:'X-Document-Session',value:session}]}};
  node('Start review','manualTrigger',1,{},[0,0]);
  node('Question','set',3.4,{mode:'raw',jsonOutput:JSON.stringify({query,mode}),options:{}},[220,0]);
  node('Open local workspace','httpRequest',4.2,{url:origin+'/',options:{timeout:15000,response:{response:{responseFormat:'text',outputPropertyName:'data'}}}},[440,0]);
  node('Find passages','httpRequest',4.2,{method:'POST',url:origin+'/api/search',...headers,sendBody:true,specifyBody:'json',
    jsonBody:"={{ { query: $('Question').first().json.query, mode: $('Question').first().json.mode } }}",options:{timeout:90000}},[660,0]);
  node('Found a passage','if',2.2,{conditions:{options:{caseSensitive:true,typeValidation:'strict',version:2},conditions:[{
    id:'has-results',leftValue:'={{ $json.results.length }}',rightValue:0,operator:{type:'number',operation:'gt'}}],combinator:'and'},options:{}},[880,0]);
  node('Read original','httpRequest',4.2,{url:`={{ '${origin}/api/source?id=' + encodeURIComponent($json.results[0].document_id) }}`,...headers,options:{timeout:15000}},[1100,-120]);
  node('Source review','set',3.4,{mode:'raw',jsonOutput:`={{ {
    status: 'source_ready_for_review', question: $('Question').first().json.query,
    method: $('Question').first().json.mode, title: $json.title, document_id: $json.id,
    source_digest: $json.digest, paragraph: $('Find passages').first().json.results[0].paragraph,
    retrieved_passage: $('Find passages').first().json.results[0].text, original_text: $json.text,
    workspace_url: '${origin}', review_required: true, cloud_handoff: false,
    note: 'Read the source before writing a note. A similar passage may not answer the question.'
  } }}`,options:{}},[1320,-120]);
  node('No matching passage','set',3.4,{mode:'raw',jsonOutput:`={{ {
    status: 'no_matching_passage', question: $('Question').first().json.query,
    method: $('Question').first().json.mode, review_required: true, cloud_handoff: false,
    note: 'No passage matched this search. Try a different phrase or review the collection.'
  } }}`,options:{}},[1100,140]);
  node('Read this first','stickyNote',1,{content:'## Household source review\nSynthetic sample • manual run • local requests only\n\nStart the household workspace on this Mac, edit Question, then run. Review the original paragraph before keeping a note. Similarity is not a verified answer.\n\nNo email, calendar, Notion, file deletion or cloud AI nodes. Import into a separate test instance; keep the workflow unpublished. This sample reads the workspace page session on the same Mac.',height:300,width:640},[180,-380]);
  const connections = {};
  const link = (from,to,branch=0) => {
    connections[from] ??= {main:[]}; connections[from].main[branch] ??= [];
    connections[from].main[branch].push({node:to,type:'main',index:0});
  };
  link('Start review','Question');link('Question','Open local workspace');link('Open local workspace','Find passages');
  link('Find passages','Found a passage');link('Found a passage','Read original');link('Read original','Source review');
  link('Found a passage','No matching passage',1);
  return {id,name:'Aurekai · Household source review',active:false,nodes,connections,
    settings:{executionOrder:'v1',saveDataSuccessExecution:'none',saveDataErrorExecution:'none',saveManualExecutions:false},pinData:{},tags:[]};
}
if (import.meta.main) writeFileSync(new URL('./household-source-review.json',import.meta.url),JSON.stringify(householdWorkflow(),null,2)+'\n');
