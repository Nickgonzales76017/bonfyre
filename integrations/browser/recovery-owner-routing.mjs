// Route recovered historical requirements toward existing Aurekai product owners.
// A route is a continuation candidate, never an execution grant or completion claim.
const PACKS=Object.freeze({
  research:'experience.source-library.workspace',
  programs:'experience.source-programs.workspace',
  campaign:'experience.source-campaign.workspace',
  creator:'experience.native-creator-studio.workspace',
  connections:'experience.connection-studio.workspace',
  finance:'experience.finance-workbooks.workspace',
  catalogue:'experience.multi-merchant-catalogue.workspace',
  procurement:'experience.retail-procurement.workspace',
  shipments:'experience.shipment-archive.workspace',
  instruments:'experience.connected-instruments.workspace',
  analysis:'experience.skill-instruments.workspace'
});
const RULES=Object.freeze([
  {id:'source-research',packs:[PACKS.research,PACKS.programs],terms:['research','source','discovery','monitor','news','evidence','brief']},
  {id:'program-work',packs:[PACKS.programs,PACKS.instruments],terms:['program','requirement','funding','grant','portfolio','deadline']},
  {id:'campaign-media',packs:[PACKS.campaign,PACKS.creator],terms:['campaign','publishing','publish','marketing','creator','media','video','audio','clip','design','social']},
  {id:'connections',packs:[PACKS.connections],terms:['connection','connector','integration','oauth','account','authentication','signin','sign-in','external']},
  {id:'finance',packs:[PACKS.finance],terms:['finance','bookkeeping','accounting','payment','invoice','bank','budget','ledger']},
  {id:'commerce',packs:[PACKS.catalogue,PACKS.procurement],terms:['catalog','catalogue','commerce','product','procurement','supplier','inventory','stock','purchase','quote']},
  {id:'shipment',packs:[PACKS.shipments,PACKS.procurement],terms:['shipment','shipping','logistics','delivery','archive','manifest','itad','receiving']},
  {id:'data-instrument',packs:[PACKS.instruments,PACKS.analysis],terms:['data','validation','check','quality','schema','sql','database','analysis','engineering']}
]);
const normalize=value=>String(value??'').toLowerCase().replace(/[_/.:]+/g,' ').replace(/\s+/g,' ').trim();
const tokens=value=>new Set(normalize(value).split(' ').filter(Boolean));
export function recoveryIdentity(record){
  if(!record||record.kind!=='research'||!record.recovery||!Array.isArray(record.sources))throw Error('Expected one recovered Research record.');
  const histories=record.sources.map(source=>source?.history).filter(Boolean);
  return {group:String(record.recovery.group??''),title:String(record.title??''),
    kinds:[...new Set(histories.map(h=>String(h.kind??'')).filter(Boolean))],
    originalKeys:[...new Set(histories.map(h=>String(h.originalKey??'')).filter(Boolean))],
    sourceCount:record.sources.length,inputDigest:String(record.recovery.inputDigest??'')};
}
export function routeRecoveredResearch(record,{installed=[]}={}){
  const identity=recoveryIdentity(record),haystack=[identity.group,identity.title,...identity.kinds,...identity.originalKeys].join(' '),words=tokens(haystack),matches=[];
  for(const rule of RULES){const hits=rule.terms.filter(term=>words.has(term)||normalize(haystack).includes(term));if(hits.length)matches.push({rule:rule.id,packs:rule.packs,hits});}
  const ranked=[];
  for(const match of matches)for(const pack of match.packs){let row=ranked.find(item=>item.pack===pack);if(!row){row={pack,score:0,rules:[],hits:[]};ranked.push(row);}row.score+=match.hits.length;row.rules.push(match.rule);row.hits.push(...match.hits);}
  ranked.sort((a,b)=>b.score-a.score||a.pack.localeCompare(b.pack));
  const installations=new Map(installed.map(row=>[row.pack,row]));
  for(const row of ranked){row.rules=[...new Set(row.rules)];row.hits=[...new Set(row.hits)];const installation=installations.get(row.pack);row.installed=Boolean(installation);row.ready=Boolean(installation?.ready);row.binding=installation?.binding??null;row.editor_url=installation?.editor_url??null;}
  const top=ranked[0]??null,second=ranked[1]??null,decisive=Boolean(top&&(!second||top.score>second.score));
  return {schema:'bonfyre.recovered-owner-route.v1',identity,candidates:ranked,
    state:!top?'review':!decisive?'review':top.ready?'owner_available':top.installed?'owner_unready':'owner_not_installed',
    selected:decisive?top:null,
    boundary:'Routing identifies an existing native continuation candidate only. It does not execute historical instructions, install a pack, create authority, or claim that the recovered workflow is complete.'};
}
export function recoveryCoverage(records,{installed=[]}={}){
  if(!Array.isArray(records))throw Error('records must be an array');
  const routes=records.map(record=>routeRecoveredResearch(record,{installed}));
  const states={};for(const route of routes)states[route.state]=(states[route.state]??0)+1;
  const packs={};for(const route of routes)for(const candidate of route.candidates)packs[candidate.pack]=(packs[candidate.pack]??0)+1;
  return {schema:'bonfyre.recovered-owner-coverage.v1',total:routes.length,states,packs,routes};
}
export const recoveryOwnerPacks=PACKS;
