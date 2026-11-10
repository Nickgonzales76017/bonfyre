#!/usr/bin/env bun
/* Native owner projection: ForeignTwin/SourceFrontier -> CMS discovery decisions
 * -> retained ItemArchive -> reviewable brief and typed SourceCollection proposal.
 * Only BonfyreCMS may update a human verdict. This compiler owns no parallel
 * approval store, no shadow source facts, and performs no publication.
 */
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, writeFileSync, renameSync, rmSync } from "node:fs";
import { createHash } from "node:crypto";
import { basename, dirname, join, resolve } from "node:path";
import {openVerification,renderVerificationHtml} from "./open_reviewed_verification.mjs";
import { spawnSync } from "node:child_process";

const digest = text => createHash("sha256").update(text).digest("hex");
const field = text => String(text??"").replace(/[\t\n\r\0-\x1f]/g," ").trim();
const quote = text => '"'+field(text).replaceAll('"',"'").slice(0,900)+'"';
const ident = x => typeof x==="string" && /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,100}$/.test(x);
const hostOf = raw => {try{
  const u=new URL(raw);
  return u.protocol==="https:" && !u.username && !u.password && !u.hash && !u.port ?
    u.hostname.toLowerCase():"";
}catch{return "";}};
const flags = argv => {
  if (argv.length%2) throw Error("pass complete --key VALUE pairs");
  const result={};
  for(let i=0;i<argv.length;i+=2){
    if(!/^--[a-z-]+$/.test(argv[i])||result[argv[i].slice(2)]!==undefined)
      throw Error("unknown or duplicate argument: "+argv[i]);
    result[argv[i].slice(2)]=argv[i+1];
  }
  return result;
};
function dbRead(path) {
  if(!path||!existsSync(path)) throw Error("native source owner database does not exist: "+(path||"(unset)"));
  return new Database(resolve(path),{readonly:true});
}
function schema(db,table,cols){
  const actual=new Set(db.query("PRAGMA table_info("+table+")").all().map(x=>x.name));
  for(const col of cols)if(!actual.has(col))throw Error("native owner does not expose required column: "+table+"."+col);
}
function getSet(db,set){
  if(!ident(set))throw Error("choose a real, bounded CMS discovery_set identifier");
  schema(db,"discovery_set",["set_id","status","criteria","label"]);
  schema(db,"discovery_candidate",["id","set_id","candidate_id","root_host","verdict","updated_at","evaluated_at","decided_by","reason","status"]);
  const info=db.query("SELECT set_id,label,criteria FROM discovery_set WHERE set_id=? AND status='published'").get(set);
  if(!info)throw Error("no published native CMS working set for "+set);
  return info;
}
export function inspect({cms,set,frontier="",archive=""}){
  const db=dbRead(cms);
  let info,rows,conn=null,items=null;
  try{
    info=getSet(db,set);
    rows=db.query("SELECT id,candidate_id,set_id,root_host,COALESCE(display_name,'') display_name,"+
      " verdict,updated_at,evaluated_at,COALESCE(decided_by,'') decided_by,COALESCE(reason,'') reason "+
      "FROM discovery_candidate WHERE set_id=? AND status='published' ORDER BY root_host").all(set);
    schema(db,"internet_source",["source","url","label","family","status","state"]);
    const sources=db.query("SELECT source,url,label,family,status,state FROM internet_source").all();
    const sourceByHost=new Map();
    for(const s of sources){
      if(s.status!=="published")continue;
      const host=hostOf(s.url);
      if(!host)continue;
      if(!sourceByHost.has(host))sourceByHost.set(host,[]);
      sourceByHost.get(host).push(s);
    }
    if(frontier){
      conn=dbRead(frontier);
      schema(conn,"root",["root_id","host"]);
      schema(conn,"channel",["root_id","state","refusal"]);
    }
    if(archive){
      items=dbRead(archive);
      schema(items,"item",["source","key"]);
    }
    const candidates=rows.map(row=>{
      const sources=sourceByHost.get(row.root_host.toLowerCase())??[];
      const material=sources.map(s=>{
        let eligible=0,refused=0;
        if(conn){
          const rs=conn.query("SELECT count(*) n FROM channel c JOIN root r ON c.root_id=r.root_id "+
            "WHERE lower(r.host)=? AND c.state IN ('admitted','active') AND COALESCE(c.refusal,'')=''").get(row.root_host.toLowerCase());
          eligible=rs?.n??0;
          refused=conn.query("SELECT count(*) n FROM channel c JOIN root r ON c.root_id=r.root_id "+
            "WHERE lower(r.host)=? AND COALESCE(c.refusal,'')<>''").get(row.root_host.toLowerCase())?.n??0;
        }
        const archived=items?items.query("SELECT count(*) n FROM item WHERE source=?").get(s.source)?.n??0:0;
        return {source:s.source,url:s.url,label:s.label,family:s.family,admitted_channels:eligible,refused_channels:refused,archived,
          usable_for_brief:row.verdict==="accepted" && eligible>0 && archived>0};
      });
      return {...row,material};
    });
    const accepted=candidates.filter(c=>c.verdict==="accepted");
    const rejected=candidates.filter(c=>c.verdict==="rejected");
    const pending=candidates.filter(c=>c.verdict==="pending");
    const usable=accepted.flatMap(c=>c.material.filter(m=>m.usable_for_brief).map(m=>({...m,host:c.root_host,candidate:c.candidate_id})));
    return {set:info,candidates,accepted,rejected,pending,usable,
      census_generation:digest(JSON.stringify(candidates.map(c=>[c.candidate_id,c.verdict,c.updated_at,c.evaluated_at,
        c.material.map(m=>[m.source,m.url,m.family,m.admitted_channels,m.archived])]))),
      cannot_infer:"A publisher review is not an external observation, reuse license, content accuracy, provider authority, completed verification or purchase."};
  } finally {db.close();conn?.close();items?.close();}
}
/* Only a native CMS entry transition changes the verdict. The caller's saved
 * generation, pending verdict, and exact set/host are checked before invoking
 * CMS. CAS on updated_at rejects another committed review/re-evaluation,
 * including a same-source decision made in a separate browser tab.
 * CMS timestamps are second-resolution: such a collision remains a known
 * limitation of the existing native schema and must not be misreported as
 * an HLC-safe linearizable decision. */
export function decide({cms,set,host,actor,reason,verdict,expectedUpdated,expectedEvaluated,
  cmsBinary,schemas,namespace="root"}){
  if(!ident(set)||!ident(actor)||!ident(namespace)||!host||hostOf("https://"+host)!==host.toLowerCase())
    throw Error("set, actor, namespace and source publisher must be exact native identities");
  if(!["accepted","rejected"].includes(verdict))throw Error("only a person may record accepted or rejected");
  if(!reason||reason.trim().length<8||reason.length>500)throw Error("give a bounded reason for this decision");
  if(!expectedUpdated||!expectedEvaluated)throw Error("source revision and evaluated generation required");
  if(!cmsBinary||!existsSync(cmsBinary)||!schemas||!existsSync(schemas))
    throw Error("native CMS binary and schemas are required for a controlled decision");
  const db=dbRead(cms);
  let prior;
  try{
    getSet(db,set);
    prior=db.query("SELECT id,verdict,updated_at,evaluated_at FROM discovery_candidate "+
      "WHERE set_id=? AND root_host=? AND namespace=? AND status='published'").get(set,host,namespace);
  }finally{db.close();}
  if(!prior||prior.verdict!=="pending")throw Error("only an owner-proposed pending candidate can be decided");
  if(prior.updated_at!==expectedUpdated||prior.evaluated_at!==expectedEvaluated)
    throw Error("stale source generation: refresh the native working set and review again");
  const now=new Date().toISOString().replace(/\.\d{3}Z$/,"Z");
  const payload=JSON.stringify({verdict,decided_by:actor,decided_at:now,reason:reason.trim()});
  const r=spawnSync(resolve(cmsBinary),["entry","update","discovery_candidate",String(prior.id),
    payload,"--ns",namespace,"--db",resolve(cms),"--schemas",resolve(schemas),
    "--if-field","verdict","--if-value","pending","--if-field2","evaluated_at","--if-value2",expectedEvaluated],{
      encoding:"utf8",timeout:30000,maxBuffer:16384,env:{...process.env}});
  if(r.status!==0)throw Error("native CMS refused the transition (possibly stale): "+field(r.stderr).slice(-500));
  const afterDb=dbRead(cms);
  try{
    const after=afterDb.query("SELECT verdict,decided_by,reason,updated_at,decided_at "+
      "FROM discovery_candidate WHERE id=? AND namespace=?").get(prior.id,namespace);
    if(!after||after.verdict!==verdict||after.decided_by!==actor||after.reason!==reason.trim()||!after.decided_at)
      throw Error("native readback disagrees with requested reviewer transition");
    return {source:host,set,verdict,decided_by:after.decided_by,
      review_updated_at:after.updated_at,readback:true,
      cannot_infer:"Human selection of a publisher is not admission of its material or an external effect."};
  }finally{afterDb.close();}
}
export function compose(result,{recipient,domain="cross-domain"}){
  if(!recipient||recipient.trim().length<2||recipient.length>130)
    throw Error("name the recipient without fabricating a buyer");
  if(!["cross-domain","itad","property","upper90","civic","media","supplier"].includes(domain))
    throw Error("domain must be a declared operating context");
  const e=result;
  const title="Source-reviewed research handoff · "+field(e.set.label);
  const review=[
    "# "+title,"","Prepared for: "+field(recipient),"Context: "+domain,
    "Native CMS discovery set: "+field(e.set.set_id),"Applied criteria: "+field(e.set.criteria),
    "Native generation digest: sha256:"+e.census_generation,"",
    "## Human-reviewed publishers",
    ...e.accepted.map(c=>"- "+field(c.root_host)+" · accepted by "+field(c.decided_by)+
      " · "+field(c.reason||"No reason attached")+" · observed source rows "+c.material.length),
    ...(e.accepted.length?[]:["No accepted publishers in this working set."]),
    "","## Retained and eligible source material",
    ...e.usable.map(x=>"- "+field(x.label)+" · "+field(x.source)+" · "+field(x.host)+
      " · "+x.archived+" retained items · "+x.admitted_channels+" admitted channels"),
    ...(e.usable.length?[]:["No source is both human-accepted and supported by an admitted frontier channel and retained ItemArchive material. Do not compile a client fact brief yet."]),
    "","## Held or awaiting human review",
    ...e.accepted.filter(x=>!x.material.some(m=>m.usable_for_brief)).map(c=>
      "- "+field(c.root_host)+" · accepted publisher; material not yet admitted and retained"),
    "Pending review: "+e.pending.length+" publisher(s); excluded from eligible material.",
    "Rejected: "+e.rejected.length+" publisher(s); identities and assertions withheld from the recipient.",
    "","## Decision to verification",
    "The downstream BonfyreEconomy planner can now evaluate typed, source-specific requirements. It has not verified a quote, price, ownership, title, recording license or onsite visit merely from this review.",
    "No supplier, seller, parcel owner, candidate, client or publisher has been contacted.",
    "No CMS source collection was published, no WorkGraph effect invoked, and no payment settled."
  ];
  const yaff=[
    "reviewed_source_frontier "+quote(e.set.set_id),
    "  recipient "+quote(recipient),
    "  domain "+domain,
    "  owner CMS.discovery_candidate",
    "  criteria "+quote(e.set.criteria),
    "  generation sha256:"+e.census_generation,
    ...e.accepted.map(c=>"  accepted_publisher "+quote(c.root_host)+" by "+quote(c.decided_by)),
    "  rejected_count "+e.rejected.length,
    "  pending_count "+e.pending.length,
    ...e.usable.map(m=>"  admitted_material "+quote(m.source)+" host "+quote(m.host)+" retained "+m.archived),
    "  cannot_infer accepted_publisher_is_not_observed_verified_or_rights_cleared",
    "  effect none",
    "  publication none"
  ].join("\n")+"\n";
  return {markdown:review.join("\n")+"\n",yaff,
    accepted:e.accepted.length,rejected:e.rejected.length,pending:e.pending.length,material:e.usable.length};
}
/* Human review is an editorial view of owner-verified membership, not a dashboard or approval engine. */
export function renderHtml(result,{recipient,domain="cross-domain"}){
  compose(result,{recipient,domain});
  const e=result,h=x=>String(x??"").replace(/[&<>"']/g,c=>
    ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const back="/s/set/"+encodeURIComponent(e.set.set_id);
  const query="?set="+encodeURIComponent(e.set.set_id)+"&recipient="+
    encodeURIComponent(recipient)+"&domain="+encodeURIComponent(domain);
  const cards=e.usable.map(m=>'<article class="material"><span>ADMITTED / RETAINED</span><h3>'+
    h(m.label)+'</h3><p>'+h(m.host)+' · '+h(m.source)+'</p><small>'+
    m.archived+' retained items · '+m.admitted_channels+' admitted channels</small>'+
    '<form method="post" action="/discovery/verification" class="verification">'+
    '<input type="hidden" name="set" value="'+h(e.set.set_id)+'">'+
    '<input type="hidden" name="source" value="'+h(m.source)+'">'+
    '<input type="hidden" name="generation" value="'+h(e.census_generation)+'">'+
    '<input type="hidden" name="recipient" value="'+h(recipient)+'">'+
    '<input type="hidden" name="domain" value="'+h(domain)+'">'+
    '<label>Verification ceiling <select name="budget-cents" aria-label="Verification budget">'+
    '<option value="10000">$100</option><option value="30000" selected>$300</option>'+
    '<option value="60000">$600</option><option value="100000">$1,000</option></select></label>'+
    '<button type="submit">Open native verification requirements →</button></form></article>').join("")||
    '<p class="empty">No source has both an accepted publisher and current admitted and retained material.</p>';
  const css=[
    ':root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#101b16;color:#edf1e6;font:15px/1.6 system-ui,sans-serif}',
    'main{max-width:1160px;margin:auto;padding:28px 22px 80px}a{color:#badeac}',
    'header{border-bottom:1px solid #526d56;padding:0 0 21px;display:flex;justify-content:space-between;gap:20px}',
    'h1{font-size:clamp(37px,6vw,70px);letter-spacing:-.065em;line-height:1.08;font-weight:530}',
    'h1 em{font:italic 1em Georgia,serif;color:#badeac}',
    '.eyebrow,.material span{font:11px ui-monospace,monospace;color:#e7be86;letter-spacing:.08em;text-transform:uppercase}',
    '.hero{display:grid;grid-template-columns:1.7fr 1fr;gap:25px;padding:65px 0 32px}',
    '.summary,.material{padding:20px;background:#1c2c20;border:1px solid #4e6a51;border-radius:7px}',
    '.summary strong{font-size:22px;color:#badeac}.summary p{font-size:13px}',
    '.gallery{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}',
    '.material h3{margin:14px 0 5px;font-size:22px}.material p,.material small,.muted{color:#b3c8b5}',
    '.notice{margin:33px 0;border-left:3px solid #dabc82;padding:17px;background:#203026}',
    '.actions{display:flex;flex-wrap:wrap;gap:10px;margin:22px 0}',
    '.actions a{padding:12px;border:1px solid #badeac;border-radius:4px;min-height:44px;text-decoration:none}',
    '.actions a:first-child{background:#badeac;color:#122019}',
    'footer{margin-top:35px;border-top:1px solid #526d56;padding-top:20px;color:#a6bdac;font-size:12px}',
    '@media(max-width:750px){.hero{grid-template-columns:1fr}}'
  ].join("");
  return ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    '<meta name="robots" content="noindex,nofollow">',
    '<title>',h(e.set.label),' · Aurekai Source Review</title><style>',css,'</style></head><body><main>',
    '<header><a href="',back,'">AUREKAI / WORK</a><span class="eyebrow">CMS → Frontier → Archive</span></header>',
    '<section class="hero"><div><span class="eyebrow">Recipient source review</span>',
    '<h1>',h(e.set.label),'<br><em>What may proceed.</em></h1>',
    '<p class="muted">For ',h(recipient),' · ',h(domain),
    '. This is a source-selection decision, not an approved transaction.</p></div>',
    '<aside class="summary"><span class="eyebrow">Native readback</span>',
    '<p><strong>',e.accepted.length,'</strong> accepted publishers</p>',
    '<p><strong>',e.usable.length,'</strong> admitted and retained source bindings</p>',
    '<p>',e.pending.length,' pending · ',e.rejected.length,' rejected/withheld</p>',
    '<p class="muted">Source generation sha256:',h(e.census_generation.slice(0,24)),'…</p></aside></section>',
    '<h2>Eligible material</h2><div class="gallery">',cards,'</div>',
    '<section class="notice"><strong>Release boundary</strong><p>',
    e.accepted.filter(x=>!x.material.some(m=>m.usable_for_brief)).length,
    ' accepted publisher(s) still lack admitted and retained material. ',
    e.pending.length,' await review. Rejected identities and claims are excluded.</p>',
    '<p>Acceptance does not verify source claims, grant reuse rights, contact providers or authorize effects.</p></section>',
    '<div class="actions"><a href="',back,'">Review working set ←</a>',
    '<a href="/discovery/brief',query,'&format=md">Download human brief (.md)</a>',
    '<a href="/discovery/brief',query,'&format=yaff">Open typed handoff (.yaff)</a></div>',
    '<footer>Read-only owner projection. No CMS publication, external outreach, finance event or WorkGraph effect was created.</footer>',
    '</main></body></html>'
  ].join("");
}
export function run(argv){
  if(!argv.length)throw Error("usage: review-source (inspect|decide|brief) --cms FILE --set ID [...]");
  const [action,...rest]=argv,args=flags(rest);
  if(!["inspect","decide","brief","verification"].includes(action))throw Error("unknown review-source action");
  const base={cms:args.cms,set:args.set,frontier:args.frontier||"",archive:args.archive||""};
  if(action==="decide"){
    const r=decide({...base,host:args.host,verdict:args.verdict,reason:args.reason,
      actor:args.actor,expectedUpdated:args["expected-updated"],
      expectedEvaluated:args["expected-evaluated"],cmsBinary:args["cms-bin"],
      schemas:args.schemas,namespace:args.namespace||"root"});
    return {text:"Native owner review recorded: "+r.source+" → "+r.verdict+" by "+r.decided_by+
      "\nCMS readback confirmed. No external party was contacted.\n"};
  }
  const material=inspect(base);
  if(action==="verification"){
    const opened=openVerification(material,{source:args.source,domain:args.domain||"cross-domain",
      recipient:args.recipient,budgetCents:args["budget-cents"],
      expectedGeneration:args["expect-generation"],economyBin:args["economy-bin"],
      economyDb:args["economy-db"],queueBin:args["queue-bin"],workDb:args["work-db"]});
    if(args.format==="html")return {text:renderVerificationHtml(opened)};
    return {text:opened.markdown};
  }
  if(action==="inspect"){
    return {text:"Working set: "+material.set.label+"\n"+
      "Accepted: "+material.accepted.length+" · Pending: "+material.pending.length+
      " · Rejected: "+material.rejected.length+" · Source material ready: "+material.usable.length+"\n"+
      material.candidates.map(c=>c.root_host+"\t"+c.verdict+"\t"+c.updated_at+
        "\t"+c.evaluated_at+"\t"+c.material.length+" source bindings").join("\n")+"\n"};
  }
  const output=compose(material,{recipient:args.recipient,domain:args.domain||"cross-domain"});
  if(args.format && !["html","md","yaff"].includes(args.format))
    throw Error("only a human review page, Markdown brief or typed YAFF handoff is available");
  if(args.format && args.out)throw Error("use either a typed download or a new handoff directory, never both");
  if(args.format==="html")return {text:renderHtml(material,{recipient:args.recipient,domain:args.domain||"cross-domain"})};
  if(args.format==="yaff")return {text:output.yaff};
  if(args.out){
    const dest=resolve(args.out);
    if(existsSync(dest)||!existsSync(dirname(dest)))throw Error("choose a new, non-existing destination under an existing directory");
    const stage=mkdtempSync(join(dirname(dest),"."+basename(dest)+".staged-"));
    try{
      writeFileSync(join(stage,"Source review.md"),output.markdown,{mode:0o600});
      writeFileSync(join(stage,"Source review.yaff"),output.yaff,{mode:0o600});
      renameSync(stage,dest);
    }catch(e){rmSync(stage,{force:true,recursive:true});throw e;}
    return {text:"Reviewed source handoff: "+dest+"\nHuman brief: Source review.md\n"+
      "Native typed handoff: Source review.yaff\n"+
      "Eligible retained sources: "+output.material+". No publisher or evidence was invented.\n"};
  }
  return {text:output.markdown};
}
if(import.meta.main){
  try{process.stdout.write(run(process.argv.slice(2)).text);}
  catch(e){console.error("Source review refused: "+e.message);process.exitCode=2;}
}
