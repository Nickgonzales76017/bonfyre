/* One owner-backed source selection -> one native verification case.
 * Nothing in the browser can promote a candidate: the selection is the exact
 * CMS.discovery_candidate + SourceFrontier + ItemArchive readback supplied by
 * reviewed_source_frontier.inspect. No JSON plan or shadow verdict store.
 */
import {Database} from "bun:sqlite";
import {existsSync} from "node:fs";
import {createHash} from "node:crypto";
import {resolve} from "node:path";
import {spawnSync} from "node:child_process";
const domains={
  "cross-domain":{economy:"research",proof:["provenance","identity"]},
  itad:{economy:"itad",proof:["model","serial","condition","ownership"],families:["itad","equipment","hardware","inventory","computers","auction"]},
  property:{economy:"property",proof:["identity","tax","title","condition"],families:["parcel","property","assessor","real-estate","tax","estate"]},
  upper90:{economy:"disaster",proof:["provenance","location","availability","pricing"],families:["weather","disaster","storm","news","broadcast","alerts"]},
  civic:{economy:"civic",proof:["identity","provenance","rights"],families:["calendar","civic","meeting","agenda","government"]},
  media:{economy:"media",proof:["provenance","rights","consent"],families:["video","media","audio","podcast","broadcast","stream"]},
  supplier:{economy:"supply",proof:["pricing","capacity","delivery","ownership"],families:["supplier","catalogue","commerce","inventory","trade"]}
};
const ident=s=>typeof s==="string"&&/^[a-zA-Z0-9][a-zA-Z0-9:._-]{0,99}$/.test(s);
const safe=s=>String(s??"").replace(/[\r\n\t\x00-\x1f]/g," ").trim().slice(0,500);
/* A verification case is not a WorkGraph task until Queue admits a separate,
 * review-only node. Queue owns the lease, status and claim tokens.
 */
function enqueueReview(caseId,{queueBin,workDb}){
  if(!queueBin||!existsSync(queueBin)||!workDb)
    throw Error("Native review queue executable and WorkGraph database required");
  const input="bonfyre://verification/"+caseId;
  const job=spawnSync(resolve(queueBin),[
    "enqueue","verification-review",input,"--mission",caseId,
    "--family","verification-review","--if-absent","--db",resolve(workDb)
  ],{encoding:"utf8",timeout:30000,maxBuffer:65536});
  if(job.error||job.status!==0)
    throw Error("Native WorkGraph refused review mission: "+safe(job.stderr||job.error?.message));
  const db=new Database(resolve(workDb),{readonly:true});
  try{
    const row=db.query("SELECT operator_id,family,input_uri,status FROM workgraph_nodes "+
      "WHERE mission_id=? AND node_id='job'").get(caseId);
    if(!row||row.operator_id!=="queue.verification-review"||
       row.family!=="verification-review"||row.input_uri!==input||
       !["ready","running","retry_wait","complete","blocked"].includes(row.status))
      throw Error("Native WorkGraph mission does not match the exact verification source");
    return {missionId:caseId,operator:row.operator_id,state:row.status,input,ownerReadback:true};
  }finally{db.close();}
}
export function openVerification(review,{source,domain="cross-domain",recipient,budgetCents,expectedGeneration,economyBin,economyDb,queueBin=null,workDb=null}){
  const profile=domains[domain];
  if(!profile)throw Error("No declared domain verification workflow");
  if(!recipient||recipient.trim().length<2||recipient.length>130)
    throw Error("A named recipient is required");
  if(!expectedGeneration||expectedGeneration!==review.census_generation)
    throw Error("Stale accepted source generation: reopen its native working set");
  if(!ident(source))throw Error("Choose an exact retained source identity");
  const usable=review.usable.filter(row=>row.source===source);
  if(usable.length!==1)throw Error("Source is not uniquely accepted, admitted and archived");
  if(!/^(?:0|[1-9]\d{0,9})$/.test(String(budgetCents??""))||
    Number(budgetCents)>1000000000)
    throw Error("Budget must be integer cents within the native owner cap");
  if(!economyBin||!existsSync(economyBin)||!economyDb)
    throw Error("Native Economy executable and durable owner database required");
  if((queueBin&&!workDb)||(!queueBin&&workDb))
    throw Error("Queue binary and WorkGraph store must both be supplied");
  if(queueBin&&!existsSync(queueBin))
    throw Error("Native WorkGraph queue binary unavailable");
  const family=String(usable[0].family||"").toLowerCase().replaceAll("_","-");
  if(profile.families&&!profile.families.includes(family))
    throw Error("Source family "+(family||"unbound")+" has no native semantic binding for "+domain+"; use source research or choose a matched owner");
  const row=usable[0],sourceRef="CMS:internet_source:"+row.source;
  const stamp=createHash("sha256").update([review.set.set_id,review.census_generation,domain,row.source].join("\u001f")).digest("hex");
  const caseId="source-"+stamp.slice(0,32);
  const argv=["--db",resolve(economyDb),"verify","intake",caseId,
    profile.economy,sourceRef,String(budgetCents),...
    profile.proof.flatMap((kind,i)=>[kind,kind,i===0?"5":"3",sourceRef])];
  const effect=spawnSync(resolve(economyBin),argv,{encoding:"utf8",timeout:30000,maxBuffer:65536});
  if(effect.error||effect.status!==0)
    throw Error("BonfyreEconomy refused source requirements: "+safe(effect.stderr||effect.error?.message));
  const owner=new Database(resolve(economyDb),{readonly:true});
  try{
    const actual=owner.query("SELECT domain,subject_ref,budget_cents FROM verification_cases WHERE case_id=?").get(caseId);
    const needs=owner.query("SELECT need_id,kind,weight,source_ref FROM verification_needs WHERE case_id=? ORDER BY need_id").all(caseId);
    if(!actual||actual.domain!==profile.economy||actual.subject_ref!==sourceRef||
       actual.budget_cents!==Number(budgetCents)||needs.length!==profile.proof.length||
       profile.proof.some((kind,index)=>!needs.some(n=>n.need_id===kind&&
           n.kind===kind&&n.weight===(index?3:5)&&n.source_ref===sourceRef)))
      throw Error("Native Economy case readback differs from admitted source requirements");
    const work=queueBin?enqueueReview(caseId,{queueBin,workDb}):null;
    return {caseId,source,sourceRef,sourceGeneration:review.census_generation,domain,recipient,
      count:needs.length,requirements:needs,sourceLabel:row.label,sourceHost:row.host,setId:review.set.set_id,ownerReadback:true,work,unchanged:effect.stdout.includes("unchanged"),
      markdown:[
        "# Source-linked verification · "+safe(row.label),
        "","For: "+safe(recipient),"Domain: "+domain,"Working set: "+review.set.set_id,
        "Publisher: "+row.host,"Accepted source: "+row.source,
        "Source generation: sha256:"+review.census_generation,
        "Native case: "+caseId,
        "Budget cap: "+(Number(budgetCents)/100).toFixed(2)+" USD",
        "","## Proof obligations requiring actual verification",
        ...needs.map(n=>"- "+n.kind+" / "+n.source_ref),
        "","No source assertion, price quote, provider availability, license, title clearance, payment, dispatch or settlement is inferred."
      ].join("\n")+"\n"};
  }finally{owner.close();}
}
/* Real post-intake work scene derived only from reopened BonfyreEconomy rows.
 * Owner projection, not a visual claim that any verification completed.
 */
export function renderVerificationHtml(opened){
  const html=s=>String(s??"").replace(/[&<>"']/g,c=>
    ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  if(!opened?.ownerReadback||!opened.requirements?.length)
    throw Error("No complete native verification owner readback");
  const list=opened.requirements.map((n,i)=>
    '<article class="need"><b>'+String(i+1).padStart(2,"0")+'</b>'+
    '<div><h3>'+html(n.kind.replaceAll("-"," "))+'</h3><p>Required source: '+html(n.source_ref)+
    '</p><small>Needs independent evidence · not completed</small></div></article>').join("");
  const setUrl="/s/set/"+encodeURIComponent(opened.setId);
  const css=[
    ':root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#101b16;color:#eaf1e7;font:15px/1.55 system-ui,sans-serif}',
    'main{width:min(1140px,calc(100% - 32px));margin:auto;padding:32px 0 80px}a{color:#badeac}',
    '.eyebrow{font:11px ui-monospace,monospace;letter-spacing:.08em;text-transform:uppercase;color:#e7be86}',
    'h1{font:500 clamp(35px,6vw,70px)/1.07 system-ui;letter-spacing:-.06em;margin:20px 0}',
    'h1 em{font:italic 1em Georgia,serif;color:#badeac}',
    '.hero{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(210px,.8fr);gap:22px;align-items:start}',
    '.hero p,.muted{color:#b9cbb9}.card,.need{border:1px solid #45604b;background:#182a20;padding:19px;border-radius:10px}',
    '.meta{font:12px/1.8 ui-monospace,monospace;overflow-wrap:anywhere}',
    '.list{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:11px;margin:24px 0}',
    '.need{display:flex;gap:13px;min-height:130px}.need b{color:#e7be86;font:12px ui-monospace,monospace}',
    '.need h3{margin:0;font-size:22px;text-transform:capitalize}.need p{font:12px ui-monospace,monospace;overflow-wrap:anywhere}',
    '.need small{color:#d8ac83}.warning{border-left:3px solid #e2b979;background:#24352a;padding:16px}',
    '.action{display:inline-block;margin:14px 0;padding:12px 16px;background:#badeac;color:#14231b;text-decoration:none;border-radius:6px;font-weight:650}',
    'footer{padding-top:20px;border-top:1px solid #455d47;color:#b3c7b2;font-size:12px}',
    '@media(max-width:730px){.hero{grid-template-columns:1fr}}'
  ].join("");
  return ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow">',
    '<title>Aurekai / Source verification</title><style>',css,'</style></head><body><main>',
    '<a href="',html(setUrl),'">AUREKAI / SOURCE WORKING SET ←</a>',
    '<div class="eyebrow">BonfyreCMS → SourceFrontier → ItemArchive → BonfyreEconomy</div>',
    '<h1>Actual source.<br><em>Unresolved questions.</em></h1>',
    '<div class="hero"><section><p>For ',html(opened.recipient),' · ',html(opened.domain),
    '. This is a native owner-backed verification case, not an approved inspection or payment.</p>',
    '<section class="card"><strong>',html(opened.sourceLabel),'</strong><p class="meta">Publisher ',html(opened.sourceHost),
    '<br>Source ',html(opened.sourceRef),'<br>Generation sha256:',html(opened.sourceGeneration),
    '</p></section></section><aside class="card"><span class="eyebrow">Native readback</span>',
    '<h2>',opened.count,' proof obligations</h2><p>',html((opened.budgetCents/100).toFixed(2)),
    ' USD · ceiling, not spend</p><p class="meta">Case ',html(opened.caseId),
    '<br>',opened.unchanged?'Identical native replay':'Native record admitted',
    '</p></aside></div>',
    '<h2>What needs to be established</h2><section class="list">',list,'</section>',
    '<p class="warning">The source was selected by a human and eligible for consideration. The institution has not obtained a qualified provider, rights, title opinion, verified quote, signed agreement, or completed task.</p>',
    '<a class="action" href="',html(setUrl),'">Return to original source decisions →</a>',
    '<footer>Every question is read from the actual BonfyreEconomy verification case. No parallel browser approval store or transaction simulation.</footer>',
    '</main></body></html>'].join("");
}
