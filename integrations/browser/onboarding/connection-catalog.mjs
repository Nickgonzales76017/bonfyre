// The retained signup list remains the source identity. Current access findings
// are a separate overlay, never a claim that an account or integration exists.
import {readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {providerAuthCapability} from '../provider-auth-capability.mjs';
export const repository=resolve(import.meta.dir,'../../..');
export const identityNames=['gmail','proton'];
const additional=[
 ['cloudflare','Cloudflare','https://dash.cloudflare.com/'],['proton','Proton mail','https://account.proton.me/'],
 ['namecheap','Namecheap','https://www.namecheap.com/myaccount/login/'],['firecrawl','Firecrawl','https://www.firecrawl.dev/app/api-keys'],
 ['exa','Exa','https://dashboard.exa.ai/api-keys'],['openai','OpenAI API','https://platform.openai.com/api-keys'],
 ['anthropic','Anthropic','https://console.anthropic.com/settings/keys'],['google-ai','Google AI Studio','https://aistudio.google.com/'],
 ['openrouter','OpenRouter','https://openrouter.ai/settings/keys'],['mistral','Mistral','https://console.mistral.ai/api-keys'],
 ['groq','Groq','https://console.groq.com/keys'],['cohere','Cohere','https://dashboard.cohere.com/api-keys'],
 ['aws','Amazon Web Services','https://console.aws.amazon.com/'],['oracle','Oracle Cloud','https://cloud.oracle.com/'],
 ['supabase','Supabase','https://supabase.com/dashboard/account/tokens'],['neon','Neon','https://console.neon.tech/'],
 ['turso','Turso','https://app.turso.tech/'],['upstash','Upstash','https://console.upstash.com/'],
 ['fly','Fly.io','https://fly.io/dashboard'],['vercel','Vercel','https://vercel.com/account/tokens'],
 ['railway','Railway','https://railway.com/account/tokens'],['render','Render','https://dashboard.render.com/'],
 ['posthog','PostHog','https://app.posthog.com/'],['sentry','Sentry','https://sentry.io/settings/'],
 ['runpod','Runpod','https://www.runpod.io/console/user/settings']
];
export const checkedFindings=Object.freeze({
 '060':{gate:'retired',note:'Pocket and its API have shut down. There is no new connection to create.',source:'https://support.mozilla.org/en-US/kb/future-of-pocket',checked:'2026-10-22'},
 '037':{gate:'existing-only',note:'Medium does not issue new integration tokens or allow new API integrations. Use its own editor/importer, RSS, or review a previously issued token.',source:'https://help.medium.com/hc/en-us/articles/213480228-API-Importing',checked:'2026-10-22'},
 '017':{gate:'provider-review',note:'Development apps share an account quota. Review the current development-user, Premium and scope requirements in the dashboard; a dashboard login is not API access.',source:'https://developer.spotify.com/blog/2026-07-23-web-api-quota-updates',checked:'2026-10-22'},
 '008':{gate:'public',note:'A public RSS feed is a source address, not an account connection. Choose a feed in Source Library.',source:'https://www.rssboard.org/rss-specification',checked:'2026-10-22'},
 '045':{gate:'public',note:'The public Hacker News API can be read without creating a provider account. Publishing and personal actions are separate.',source:'https://github.com/HackerNews/API',checked:'2026-10-22'}
});
const categories=[
 [1,5,'Open social'],[6,10,'Automation & feeds'],[11,16,'Video & streaming'],[17,22,'Audio & podcasts'],
 [23,27,'Music distribution'],[28,34,'Membership & commerce'],[35,40,'Publishing'],[41,45,'Communities'],
 [46,50,'Social & images'],[51,55,'Commerce'],[56,60,'Community tools'],[61,70,'Social platforms'],
 [71,75,'Distribution services'],[76,83,'Affiliates & advertising'],[84,90,'Video distribution'],
 [91,94,'Restricted creator platforms'],[95,98,'Audio distribution'],[99,100,'Social platforms'],
 [101,104,'AI & models'],[105,107,'Open media'],[108,115,'Creative communities'],[116,127,'Streaming & podcast tools'],
 [128,133,'Images & design'],[134,138,'Games & communities'],[139,143,'Merch & fulfilment'],[144,148,'Audio production'],
 [149,154,'Culture & social'],[155,162,'Restricted creator platforms'],[163,168,'Music rights'],
 [169,175,'Licensed media'],[176,178,'Games & virtual worlds'],[179,188,'Creator & audio accounts'],
 [189,200,'Video & social'],[201,225,'Developer & infrastructure']
];
export const readOnlySources=Object.freeze([
 {id:'news',title:'News & editorial',path:'/discover',description:'Read the current source-backed public stream, then keep selected material in Source Library.'},
 {id:'watch',title:'Video & recordings',path:'/watch',description:'Watch the published source collections; provider-account access is separate.'},
 {id:'listen',title:'Podcasts & audio',path:'/listen',description:'Listen, seek and continue a selected item on another device.'},
 {id:'sources',title:'Research & sources',path:'/sources/web',description:'Run a reviewed public-source request or use an explicitly configured provider account.'},
 {id:'models',title:'Models',path:'/s/models/all',description:'Inspect the catalogue. Actual execution requires a currently admitted local candidate and explicit review.'},
 {id:'library',title:'Your installed Source Library',path:'/apps',description:'Open your installed Source Library, retain selections and continue into a receiving editor.'}
]);
function parse(text){const lines=text.replace(/(?:\r?\n)+$/,'').split(/\r?\n/),fields=lines.shift().split('\t');return lines.map((line,index)=>{const cells=line.split('\t');if(cells.length!==fields.length)throw Error('The enrollment source changed at row '+(index+2));return Object.fromEntries(fields.map((f,i)=>[f,cells[i]]));});}
export function safeProviderURL(value){const u=new URL(value);if(u.protocol!=='https:'||u.username||u.password||u.port||/[\x00-\x20\\]/.test(value))throw Error('Use the exact HTTPS provider entry.');return u.href;}
export function loadConnectionCatalog({ledger=join(repository,'docs/engineering/handoff-backfill-20261019/onboarding-enrollment.tsv'),recovery=join(repository,'docs/engineering/handoff-backfill-20261019/onboarding-recovery-p0.tsv')}={}){
 const all=parse(readFileSync(ledger,'utf8')),original=all.filter(row=>row.identity==='gmail');
 if(original.length!==200||all.length!==400||new Set(all.map(r=>r.work_id)).size!==400)throw Error('The reviewed enrollment source membership changed.');
 const scopes=parse(readFileSync(recovery,'utf8'));
 const entries=original.map(row=>({id:row.source_id,name:row.platform,url:safeProviderURL(row.signup_url),accountGroup:row.account_group,sourceTier:Number(row.source_tier),sourceState:row.supplied_state,sourceGate:row.api_gate,origin:'retained-200-provider-list'}));
 for(const [i,[provider,name,url]]of additional.entries()){const id=String(201+i);entries.push({id,name,url:safeProviderURL(url),accountGroup:id,sourceTier:null,sourceState:'recover_history',sourceGate:'review_existing_account',origin:'existing-access-recovery',provider});}
 const known={'009':'github','011':'youtube-data','102':'hugging-face'};
 return entries.map(row=>{
  const category=categories.find(([a,b])=>+row.id>=a&&+row.id<=b)?.[2]||'Other providers',finding=checkedFindings[row.id];
  const provider=row.provider||known[row.id]||'',scopeRows=scopes.filter(s=>s.provider===provider);
  const gate=finding?.gate||(row.sourceState==='provider_retired'?'retired':row.sourceGate==='protocol_or_instance_review'?'instance':row.sourceTier>=4?'access-review':'portal');
  const base={...row,provider,category,gate,finding:finding||null,recovery:row.sourceState==='recover_history'||scopeRows.length>0,
   recoveryScopes:scopeRows.map(s=>({scope:s.scope,action:s.proton_action})),
   note:finding?.note||(gate==='instance'?'Choose the actual service instance before registering an application. The directory is not an account connection.':gate==='access-review'?'The source list describes a reviewed or restricted integration. Inspect the provider’s current eligibility and requested operation before applying.':'The retained entry opens the provider’s real setup page. API availability, required scopes and pricing still need the provider’s current review.'),
   steps:['Review existing access','Continue on the provider','Confirm account and scope','Store declared credential','Verify an actual operation'],
   handoff:gate==='public'?'Open source':gate==='retired'?'Unavailable':gate==='existing-only'?'Open provider editor':gate==='access-review'?'Review access':'Open provider setup'};
  return {...base,auth:providerAuthCapability(base)};
 });
}
export function catalogEntry(id,options){const row=loadConnectionCatalog(options).find(r=>r.id===id);if(!row)throw Error('Provider is not in the retained account plan.');return row;}
export function providerCredentialBinding(item,c){
 if(item.credential_provider)return item.credential_provider;
 const row=catalogEntry(item.source_id,c.catalog);
 if(row.name!==item.platform||row.url!==safeProviderURL(item.signup_url))throw Error('The provider entry changed. Review the native source before storing a credential.');
 return row.provider||'';
}
export function extraEnrollmentRows(identity,account){
 if(!identityNames.includes(identity)||typeof account!=='string'||!account.includes('@'))throw Error('Use one configured enrollment identity.');
 return loadConnectionCatalog().filter(r=>+r.id>200).map(r=>({work_id:r.id+'-'+identity,source_id:r.id,platform:r.name,signup_url:r.url,identity,account,api_gate:r.sourceGate,supplied_state:r.sourceState,account_group:r.accountGroup,credential_provider:r.provider}));
}
export function connectionFields(item,c){
 const provider=providerCredentialBinding(item,c);if(!provider)return[];
 const catalog=JSON.parse(readFileSync(c.providers,'utf8'));if(!Array.isArray(catalog))throw Error('Current credential declarations are unavailable.');
 const matches=catalog.filter(p=>p.provider===provider);if(matches.length!==1)return[];
 return (matches[0].fields||[]).map(f=>({field:f.field,label:f.label,kind:typeof f.kind==='string'?f.kind:'secret',sensitive:f.sensitive!==false,required:f.required!==false,help:f.help||''}));
}
export function publicPlanSummary(){const rows=loadConnectionCatalog();return{targets:rows.length,originalTargets:200,recoveryTargets:25,identityLanes:identityNames,entries:rows,sourceLinks:readOnlySources};}
