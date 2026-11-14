#!/usr/bin/env node
/*
 * OAuth boundary for the existing Bonfyre project MCP.  It intentionally owns
 * neither tools nor institutional state: it exchanges a short-lived OAuth code
 * for a BonfyreAuth session and forwards only authorized requests to the
 * already-running authenticated MCP owner.
 */
import http from 'node:http';
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdirSync, readFileSync } from 'node:fs';
import { SPINES_PROJECT_TOOLS } from './project_spines.mjs';
import { V6_PROJECT_TOOLS } from './project_v6.mjs';
import { FABRIC_PROJECT_TOOLS } from './project_fabric.mjs';
import { PRODUCTS_PROJECT_TOOLS } from './project_products.mjs';
import { knightIcons, withMcpIcons } from './brand.mjs';
import { authorizationRequestValid } from './oauth_request_policy.mjs';
import { WORKSPACE_MCP_TOOLS, createWorkspaceMcp } from './workspace_mcp.mjs';
import { readAccountMeasurements,readInstitutionMeasurements } from './connected_measurement.mjs';
import { WORKBOOK_APP_URI, WORKBOOK_APP_RESOURCE, readWorkbookApp } from './workbook_app_resource.mjs';
import { CONNECTED_APP_URI, CONNECTED_APP_RESOURCE, readConnectedApp } from './connected_measurement_app_resource.mjs';

const workspaceCall = createWorkspaceMcp({ownerUrl:process.env.BONFYRE_WORKSPACE_OWNER_URL || 'http://127.0.0.1:28901',publicOrigin:process.env.BONFYRE_WORKSPACE_PUBLIC_ORIGIN || 'https://aurekai.work'});
const workspaceTool = name => WORKSPACE_MCP_TOOLS.some(tool=>tool.name===name);
const selfInstructions = 'Use workspace_list to find editors in your personal workspace or an explicitly selected destination. Native owners check current access separately for every organization, household, record and change. Read exact saved drafts, preview field changes, then confirm the same preview with workspace_update; use workspace_views to continue in the owning product. These operations save drafts and do not publish, order, pay or grant access. Carrier recipes and retained ForeignTwin observations remain actor-local.';

const host = process.env.BONFYRE_OAUTH_MCP_HOST || '127.0.0.1';
const port = Number(process.env.BONFYRE_OAUTH_MCP_PORT || 8791);
// The protected resource and its authorization server are separate public
// identities.  Keep the retired single-origin setting as a compatibility
// fallback so an un-upgraded local fixture does not fabricate a third issuer.
const legacyOrigin = (process.env.BONFYRE_OAUTH_PUBLIC_ORIGIN || 'https://aurekai.work').replace(/\/$/, '');
const authOrigin = (process.env.BONFYRE_AUTH_PUBLIC_ORIGIN || legacyOrigin).replace(/\/$/, '');
const mcpOrigin = (process.env.BONFYRE_MCP_PUBLIC_ORIGIN || legacyOrigin).replace(/\/$/, '');
const MCP_ICONS = knightIcons(mcpOrigin);
const authBinary = process.env.BONFYRE_AUTH_BINARY || '/usr/local/bin/bonfyre-auth';
const authDb = process.env.BONFYRE_AUTH_DB || '/var/lib/bonfyre/auth.db';
const credentialsBinary = process.env.BONFYRE_CREDENTIALS_BINARY || '/usr/local/bin/bonfyre-credentials';
const credentialsDb = process.env.BONFYRE_CREDENTIALS_DB || '/var/lib/bonfyre/credentials/credentials.db';
const credentialsKey = process.env.BONFYRE_CREDENTIALS_KEY || '/etc/bonfyre/credentials.key';
const credentialProviders = process.env.BONFYRE_CREDENTIAL_PROVIDERS || '/usr/local/share/bonfyre/provider-credentials.json';
/* Which estates an account can hold a slice in, from the namespace contract.
 *
 * This was a literal here and three more times in the native organs, and it had
 * already drifted: `leads` was listed here while the contract declared no
 * namespace for it. The list decides who receives an institutional slice, so an
 * estate added to three of four places produces an account that authenticates
 * and silently has none.
 *
 * Falling back to the previous literal keeps the bridge serving if the file is
 * missing, and says so, rather than admitting nobody. */
const SLICE_ESTATES = (() => {
  try {
    const path = process.env.BONFYRE_INSTITUTION_CONTRACT
      || new URL('./institution_contract.json', import.meta.url).pathname;
    const contract = JSON.parse(readFileSync(path, 'utf8'));
    if (Array.isArray(contract.slice_estates) && contract.slice_estates.length) {
      return contract.slice_estates;
    }
  } catch (error) {
    console.error(`institution contract unreadable, using the built-in list: ${error.message}`);
  }
  return ['upper90','jrss','ai-loss','leads'];
})();

const upstream = new URL(process.env.BONFYRE_PROJECT_MCP_URL || 'http://127.0.0.1:13051/mcp');
const upstreamToken = readFileSync(process.env.BONFYRE_PROJECT_MCP_TOKEN_FILE || '/etc/bonfyre/mcp.token', 'utf8').trim();
const verticalBinary = process.env.BONFYRE_VERTICAL_BINARY || process.env.BONFYRE_UPPER90_BINARY || '/usr/local/bin/bonfyre-upper90';
const verticalDb = process.env.BONFYRE_VERTICAL_DB || process.env.BONFYRE_UPPER90_DB || '/var/lib/bonfyre/upper90.db';
const cmsBinary = process.env.BONFYRE_CMS_BINARY || '/usr/local/bin/bonfyre-cms';
const cmsDb = process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db';
const cmsSchemas = process.env.BONFYRE_CMS_SCHEMAS || '/usr/local/share/bonfyre/content-types';
const projectBinary = process.env.BONFYRE_PROJECT_BINARY || '/usr/local/bin/bonfyre-project';
const foreignRoot = process.env.BONFYRE_FOREIGN_ROOT || '/var/lib/bonfyre/foreign';
const membraneBinary = process.env.BONFYRE_MEMBRANE_BINARY || '/usr/local/bin/bonfyre-membrane';
const providerBinary = process.env.BONFYRE_PROVIDER_BINARY || '/usr/local/bin/bonfyre-provider';
const providerDb = process.env.BONFYRE_PROVIDER_DB || '/var/lib/bonfyre/provider.db';
function optionalFile(path) { try { return path ? readFileSync(path, 'utf8').trim() : ''; } catch { return ''; } }
const placementSupabaseUrl = (process.env.BONFYRE_PLACEMENT_SUPABASE_URL || '').replace(/\/$/, '');
const placementSupabaseKey = process.env.BONFYRE_PLACEMENT_SUPABASE_SERVICE_KEY || optionalFile(process.env.BONFYRE_PLACEMENT_SUPABASE_SERVICE_KEY_FILE);
const oauthSealSecret = process.env.BONFYRE_OAUTH_CODE_SEAL_KEY || optionalFile(process.env.BONFYRE_OAUTH_CODE_SEAL_KEY_FILE);
const durableOAuthTransactions = Boolean(placementSupabaseUrl && placementSupabaseKey && oauthSealSecret);
// Kept only for isolated local fixtures that intentionally omit the continuity
// membrane. A partially configured production store fails closed rather than
// silently returning to a process-local authorization code.
const volatileCodes = new Map();
const fiveMinutes = 5 * 60 * 1000;
const MCP_MODERN_VERSION = '2026-07-28';
const MCP_LEGACY_VERSION = '2025-06-18';
const MCP_LIST_TTL_MS = Number(process.env.BONFYRE_MCP_LIST_TTL_MS || 5000);

function json(res, status, value, extra = {}) { const body = Buffer.from(JSON.stringify(value)); res.writeHead(status, { 'Content-Type':'application/json; charset=utf-8', 'Content-Length':body.length, 'Cache-Control':'no-store', ...extra }); res.end(body); }
function html(res, status, value) { const body = Buffer.from(value); res.writeHead(status, { 'Content-Type':'text/html; charset=utf-8', 'Content-Length':body.length, 'Cache-Control':'no-store' }); res.end(body); }
function esc(v) { return String(v || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
/* Sign-in forms stay small; an MCP call may carry a user's file (a bank export,
 * a listing), bounded well under the project owner's own 20 MB request limit. */
const MCP_BODY_BYTES = 8 * 1024 * 1024;
function body(req, limit = 32768) { return new Promise((resolve, reject) => { const chunks=[]; let n=0; req.on('data', c => { n += c.length; if (n > limit) { reject(new Error('body too large')); } else chunks.push(c); }); req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))); req.on('error', reject); }); }
function form(text) { const params=new URLSearchParams(text); if(params.has('password')&&!authorizationRequestValid(params))throw Error('invalid_oauth_authorization_request'); return params; }
function token(req) { const h=String(req.headers.authorization || ''); return h.startsWith('Bearer ') ? h.slice(7).trim() : ''; }
/* A refresh token carries the BonfyreAuth session it was issued for, sealed,
 * so no server state is needed and a restart does not log anyone out. It is
 * honoured only while BonfyreAuth still verifies that session: logout, expiry
 * or a disabled account ends it. Before this, ChatGPT was told every token
 * died after 8 hours while the session lived 7 days, and with no refresh
 * token its calls stalled on re-authentication and it asked for the password
 * again two or three times a day. */
const refreshKey = createHash('sha256').update('bonfyre-oauth-refresh\0').update(oauthSealSecret || upstreamToken).digest();
function sealRefresh(value) {
  const iv=randomBytes(12), cipher=createCipheriv('aes-256-gcm',refreshKey,iv);
  const sealed=Buffer.concat([cipher.update(JSON.stringify(value),'utf8'),cipher.final()]);
  return `bfr1.${iv.toString('base64url')}.${cipher.getAuthTag().toString('base64url')}.${sealed.toString('base64url')}`;
}
function unsealRefresh(text) {
  const [version,ivText,tagText,bodyText,extra]=String(text || '').split('.');
  if (version !== 'bfr1' || !ivText || !tagText || !bodyText || extra !== undefined) throw new Error('refresh_token_invalid');
  const decipher=createDecipheriv('aes-256-gcm',refreshKey,Buffer.from(ivText,'base64url'));
  decipher.setAuthTag(Buffer.from(tagText,'base64url'));
  return JSON.parse(Buffer.concat([decipher.update(Buffer.from(bodyText,'base64url')),decipher.final()]).toString('utf8'));
}
// The access token is the session; its lifetime is the session's, when known.
function tokenGrant({ session, client, scope, sessionExpires }) {
  const grant={access_token:session,token_type:'Bearer',scope,refresh_token:sealRefresh({s:session,c:client,sc:scope,e:sessionExpires || null})};
  const left=sessionExpires ? Math.floor((Date.parse(sessionExpires)-Date.now())/1000) : NaN;
  if (Number.isFinite(left) && left > 0) grant.expires_in=left;
  return grant;
}
async function refreshPost(p, res) {
  let held;
  try { held=unsealRefresh(p.get('refresh_token')); } catch { return json(res,400,{error:'invalid_grant'}); }
  const client=p.get('client_id') || '';
  if (typeof held?.s !== 'string' || !held.s || (client && held.c !== client)) return json(res,400,{error:'invalid_grant'});
  if (held.e && !(Date.parse(held.e) > Date.now())) return json(res,400,{error:'invalid_grant'});
  let verified;
  try { verified=await authJson('verify',['--token',held.s]); } catch { verified=null; }
  if (verified?.valid !== true) return json(res,400,{error:'invalid_grant'});
  res.setHeader('Cache-Control','no-store');
  json(res,200,tokenGrant({session:held.s,client:held.c,scope:held.sc,sessionExpires:held.e}));
}
function codeDigest(code) { return `sha256:${createHash('sha256').update(String(code)).digest('hex')}`; }
function sealSession(session) {
  const key=createHash('sha256').update(oauthSealSecret).digest(), iv=randomBytes(12);
  const cipher=createCipheriv('aes-256-gcm',key,iv);
  const body=Buffer.concat([cipher.update(session,'utf8'),cipher.final()]);
  return `${iv.toString('base64url')}.${cipher.getAuthTag().toString('base64url')}.${body.toString('base64url')}`;
}
function unsealSession(value) {
  const [ivText,tagText,bodyText]=String(value || '').split('.');
  if (!ivText || !tagText || !bodyText) throw new Error('oauth_transaction_invalid');
  const key=createHash('sha256').update(oauthSealSecret).digest();
  const decipher=createDecipheriv('aes-256-gcm',key,Buffer.from(ivText,'base64url'));
  decipher.setAuthTag(Buffer.from(tagText,'base64url'));
  return Buffer.concat([decipher.update(Buffer.from(bodyText,'base64url')),decipher.final()]).toString('utf8');
}
async function continuityFetch(path, body) {
  const reply=await fetch(`${placementSupabaseUrl}${path}`, {method:'POST',headers:{apikey:placementSupabaseKey,Authorization:`Bearer ${placementSupabaseKey}`,'Content-Type':'application/json',Prefer:'return=representation'},body:JSON.stringify(body)});
  if (!reply.ok) throw new Error('oauth_transaction_store_unavailable');
  return reply.status === 204 ? null : reply.json();
}

async function providerHistorySummary(args) {
  const days=args.days===undefined?365:args.days,limit=args.limit===undefined?25:args.limit;
  if(!Number.isInteger(days)||days<1||days>730||!Number.isInteger(limit)||limit<1||limit>50)
    throw new Error('provider_history_bounds_invalid');
  if(!placementSupabaseUrl||!placementSupabaseKey)
    throw new Error('provider_history_source_not_configured');
  const since=new Date(Date.now()-days*86400000).toISOString();
  let response;
  try {
    response=await fetch(placementSupabaseUrl+'/rest/v1/rpc/provider_receipt_summary',{
      method:'POST',headers:{apikey:placementSupabaseKey,Authorization:'Bearer '+placementSupabaseKey,
        'Content-Type':'application/json'},
      body:JSON.stringify({p_since:since,p_limit:limit}),signal:AbortSignal.timeout(15000)});
  }catch{throw new Error('provider_history_source_unavailable');}
  if(!response.ok)throw new Error('provider_history_source_unavailable');
  let rows;try{rows=await response.json();}catch{throw new Error('provider_history_invalid_response');}
  if(!Array.isArray(rows)||rows.length>limit)throw new Error('provider_history_invalid_response');
  const data=rows.map(row=>{
    const attempts=Number(row.attempts),successes=Number(row.successes);
    if(typeof row.provider_ref!=='string'||!/^[a-z0-9_-]{1,64}$/.test(row.provider_ref)||
       typeof row.call_class!=='string'||!/^[a-z0-9_-]{1,32}$/.test(row.call_class)||
       !Number.isSafeInteger(attempts)||attempts<0||
       !Number.isSafeInteger(successes)||successes<0||successes>attempts||
       !Number.isFinite(Date.parse(row.first_observed))||
       !Number.isFinite(Date.parse(row.last_observed))||
       Date.parse(row.first_observed)>Date.parse(row.last_observed))
       throw new Error('provider_history_invalid_response');
    return {provider:row.provider_ref,call_class:row.call_class,attempts,successes,
      first_observed:row.first_observed,last_observed:row.last_observed};
  });
  return {owner:'Supabase historical provider receipt projection',period_start:since,
    current_availability:'not_measured',authority:'diagnostic_only',raw_receipts_exposed:false,
    data,source:'bonfyre-control-mirror'};
}
async function storeAuthorizationCode(code, record) {
  if (!durableOAuthTransactions) {
    if (placementSupabaseUrl || placementSupabaseKey || oauthSealSecret) throw new Error('oauth_transaction_store_incomplete');
    volatileCodes.set(code,record);
    return;
  }
  await continuityFetch('/rest/v1/oauth_authorization_transaction', {
    code_digest:codeDigest(code), client_id:record.client, redirect_uri:record.redirect,
    code_challenge:record.verifier, scope:record.scope, sealed_session:sealSession(record.session),
    expires_at:new Date(record.expires).toISOString()
  });
}
async function consumeAuthorizationCode(code) {
  if (!durableOAuthTransactions) {
    if (placementSupabaseUrl || placementSupabaseKey || oauthSealSecret) throw new Error('oauth_transaction_store_incomplete');
    const record=volatileCodes.get(code); volatileCodes.delete(code); return record || null;
  }
  const rows=await continuityFetch('/rest/v1/rpc/placement_consume_oauth_transaction',{p_code_digest:codeDigest(code)});
  if (!Array.isArray(rows) || rows.length !== 1) return null;
  const row=rows[0];
  return {session:unsealSession(row.sealed_session),client:row.client_id,redirect:row.redirect_uri,verifier:row.code_challenge,scope:row.scope,expires:Date.now()+1};
}
function runAuth(command, input) { return new Promise((resolve, reject) => { const child=spawn(authBinary, ['--db', authDb, command], { stdio:['pipe','pipe','ignore'] }); let out=''; child.stdout.on('data', c => { out += c; }); child.on('error', reject); child.on('close', code => code === 0 ? resolve(out) : reject(new Error('authentication_failed'))); child.stdin.end(input || ''); }); }
function authorizedExact(session, scope) { return new Promise(resolve => { const child=spawn(authBinary, ['--db',authDb,'authorize','--token',session,'--scope',scope], {stdio:['ignore','pipe','ignore']}); let out=''; child.stdout.on('data',c=>out+=c); child.on('error',()=>resolve(false)); child.on('close', code=>{ try { resolve(code===0 && JSON.parse(out).authorized === true); } catch { resolve(false); } }); }); }
/* BonfyreAuth stores the highest generic session scope.  A write session may
 * lawfully do a read; requiring it to carry a second independent read token
 * made ChatGPT's normal read-before-write MCP initialization fail. */
async function authorized(session, scope) {
  if (await authorizedExact(session, scope)) return true;
  if (scope === 'read' && await authorizedExact(session, 'write')) return true;
  /* A self-scoped session is transport-admitted only to its own narrow MCP
   * slice below.  It can never reach the global upstream or an institution
   * tool merely because an MCP client requested OAuth "write". */
  return await authorizedExact(session, 'self');
}
function run(argv, input = '') { return new Promise((resolve, reject) => { const child=spawn(argv[0], argv.slice(1), {stdio:['pipe','pipe','pipe']}); let out='', err=''; child.stdout.on('data',c=>{out+=c;}); child.stderr.on('data',c=>{err+=c;}); child.on('error',reject); child.on('close',code=>code===0?resolve(out):reject(new Error(err.trim() || `native_exit_${code}`))); /* A native organ that exits before reading stdin makes this write fail with EPIPE, and an unhandled socket error takes the whole authority bridge down -- every agent session with it, for one child that closed early. The close handler above already reports the real failure with the child's own stderr, so this only has to stop the process from dying first. */ child.stdin.on('error', () => {}); child.stdin.end(input); }); }
function authJson(command, args = []) { return run([authBinary,'--db',authDb,command,...args]).then(JSON.parse); }
async function institutionAuthorized(session, institution, role) { try { const v=await authJson('institution-authorize',['--token',session,'--institution',institution,'--role',role]); return v.authorized === true; } catch { return false; } }
function atom(v, max = 128) { return typeof v === 'string' && v.length > 0 && v.length <= max && /^[A-Za-z0-9:_.-]+$/.test(v); }
function closureNamespace(estate, scope) { return `private:${estate}:${scope}`; }
async function privateSlice(session) {
  const verified=await authJson('verify',['--token',session]);
  if (verified.valid !== true || !Number.isInteger(verified.user?.id)) throw new Error('session_invalid');
  const actor=`auth-user:${verified.user.id}`;
  const superuser=await institutionAuthorized(session,'bonfyre','admin');
  const estates=[];
  for (const estate of SLICE_ESTATES) {
    if (!await institutionAuthorized(session,estate,'read')) continue;
    try {
      const answer=JSON.parse(await run([verticalBinary,'--db',verticalDb,'closure','--actor',actor,'--estate',estate]));
      const scope=Array.isArray(answer.scopes) ? answer.scopes.find(s=>atom(s)) : '';
      if (scope) estates.push({estate,scope,inherited:answer.inherited === true,namespace:closureNamespace(estate,scope)});
    } catch { /* no named native role means no slice, even with host admission */ }
  }
  return {actor,superuser,estates};
}
async function selfSlice(session) {
  const verified=await authJson('verify',['--token',session]);
  if (verified.valid !== true || !Number.isInteger(verified.user?.id) ||
      !await authorizedExact(session,'self')) return null;
  const actor=`auth-user:${verified.user.id}`;
  return {actor,namespace:`actor-${verified.user.id}`,
    foreignRoot:`${foreignRoot}/private/self/actor-${verified.user.id}`};
}
function textResult(value, isError = false) { return {content:[{type:'text',text:JSON.stringify(value,null,2)}],structuredContent:value,isError}; }
function serverInfo(name) { return {name,version:'2.0.0',...(MCP_ICONS.length ? {icons:MCP_ICONS} : {})}; }
function modernEnvelope(name, result) {
  return {...(result || {}),_meta:{...(result?._meta || {}),'io.modelcontextprotocol/serverInfo':serverInfo(name)}};
}
function apertureDigest(slice, tools, resources = [], prompts = []) {
  const stable={
    actor:slice.actor || '',
    estates:(slice.estates || []).map(({estate,scope,inherited})=>({estate,scope,inherited:inherited===true})).sort((a,b)=>`${a.estate}:${a.scope}`.localeCompare(`${b.estate}:${b.scope}`)),
    tools:tools.map(v=>v.name).sort(),resources:resources.map(v=>v.uri).sort(),prompts:prompts.map(v=>v.name).sort()
  };
  return `sha256:${createHash('sha256').update(JSON.stringify(stable)).digest('hex')}`;
}
function modernList(name, result, digest) {
  return modernEnvelope(name,{...result,ttlMs:MCP_LIST_TTL_MS,cacheScope:'private',_meta:{'aurekai/addressApertureDigest':digest}});
}
function modernRequest(req, request) {
  return String(req.headers['mcp-protocol-version'] || '') === MCP_MODERN_VERSION ||
    String(request?.params?._meta?.['io.modelcontextprotocol/protocolVersion'] || '') === MCP_MODERN_VERSION;
}
function validateModernRouting(req, request) {
  const method=String(request?.method || ''), headerVersion=String(req.headers['mcp-protocol-version'] || ''), headerMethod=String(req.headers['mcp-method'] || ''), headerName=String(req.headers['mcp-name'] || '');
  if (headerVersion !== MCP_MODERN_VERSION) return 'modern_protocol_version_header_required';
  if (!headerMethod || headerMethod !== method) return 'mcp_method_header_mismatch';
  const bodyName=typeof request?.params?.name === 'string' ? request.params.name : '';
  if (bodyName && (!headerName || headerName !== bodyName)) return 'mcp_name_header_mismatch';
  return '';
}
function discoverResult(name, instructions) {
  return modernEnvelope(name,{supportedVersions:[MCP_MODERN_VERSION,MCP_LEGACY_VERSION],capabilities:{tools:{listChanged:false},resources:{subscribe:false,listChanged:false},prompts:{listChanged:false}},instructions,ttlMs:MCP_LIST_TTL_MS,cacheScope:'private'});
}
function actorClosureUri(actor) { return `bonfyre://closure/actor/${encodeURIComponent(actor)}`; }
function institutionClosureUri(estate, scope) { return `bonfyre://closure/institution/${encodeURIComponent(estate)}/${encodeURIComponent(scope)}`; }
function selfResources(slice) { return [{uri:actorClosureUri(slice.actor),name:'Current actor closure',description:'Current authenticated actor-local Bonfyre closure; projection only, never an authority grant.',mimeType:'application/json'},WORKBOOK_APP_RESOURCE,CONNECTED_APP_RESOURCE]; }
function privateResources(slice) { return [{uri:actorClosureUri(slice.actor),name:'Current actor closure',description:'Current authenticated actor coordinate for this institutional aperture.',mimeType:'application/json'},...slice.estates.map(({estate,scope})=>({uri:institutionClosureUri(estate,scope),name:`${estate} institutional closure`,description:'Current revocable institution/scope projection; native owners retain authority.',mimeType:'application/json'})),CONNECTED_APP_RESOURCE].sort((a,b)=>a.uri.localeCompare(b.uri)); }
function closureResourceRead(slice, uri, institutional) { let value; if (uri===actorClosureUri(slice.actor)) value={schema_version:'bonfyre.mcp.actor-closure.v1',actor:slice.actor,kind:institutional?'institution-participant':'self',limit:'Current closure coordinate only; not identity proof, disclosure permission, payment approval, provider admission, or effect authority.'}; else if (institutional) { const found=slice.estates.find(({estate,scope})=>institutionClosureUri(estate,scope)===uri); if (!found) return null; value={schema_version:'bonfyre.mcp.institution-closure.v1',actor:slice.actor,estate:found.estate,scope:found.scope,inherited:found.inherited===true,namespace:found.namespace,limit:'Current institutional admission only; every owner/effect boundary remains independently enforced.'}; } else return null; return {contents:[{uri,mimeType:'application/json',text:JSON.stringify(value,null,2)}]}; }
function modernRead(name, result, slice, tools, resources) { return modernEnvelope(name,{...result,ttlMs:MCP_LIST_TTL_MS,cacheScope:'private',_meta:{'aurekai/addressApertureDigest':apertureDigest(slice,tools,resources)}}); }
function privateTools() { return [
  {name:'institution_closures',description:'List only the caller\'s current private institutional closures. This returns estate and scope coordinates, never client records.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'institution_connected_measurements',description:'Read source-dated measurements for one institution in the caller current verified closure. No arbitrary subject selection, publishing, or external effects.',inputSchema:{type:'object',properties:{estate:{type:'string'},provider:{type:'string',pattern:'^[a-z][a-z0-9_]{0,47}$'},limit:{type:'integer',minimum:1,maximum:100}},required:['estate'],additionalProperties:false},annotations:{readOnlyHint:true},_meta:{ui:{resourceUri:CONNECTED_APP_URI}}},
  {name:'institution_cms_list',description:'Read entries from a content type in one currently admitted private institutional CMS namespace.',inputSchema:{type:'object',properties:{estate:{type:'string'},type:{type:'string'},limit:{type:'integer',minimum:1,maximum:100}},required:['estate','type'],additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'institution_cms_create',description:'Create a declared native CMS record inside one currently admitted private institutional namespace. This is not public publication.',inputSchema:{type:'object',properties:{estate:{type:'string'},type:{type:'string'},document:{type:'object'}},required:['estate','type','document'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'institution_foreign_observe',description:'Observe one public URL through the native ForeignTwin observer and retain its body and receipt inside the caller\'s private institutional closure. Observation is not truth, publication, or effect authority.',inputSchema:{type:'object',properties:{estate:{type:'string'},twin_id:{type:'string'},url:{type:'string',format:'uri'}},required:['estate','twin_id','url'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'institution_review_request',description:'Create bounded native Upper90 review work for an opaque reference in one currently admitted estate. It never reads or mutates the referenced client record.',inputSchema:{type:'object',properties:{estate:{type:'string'},request_kind:{type:'string'},reference:{type:'string'}},required:['estate','request_kind','reference'],additionalProperties:false},annotations:{readOnlyHint:false}}
]; }
function selfTools() { return [
  ...WORKSPACE_MCP_TOOLS,
  {name:'self_connected_measurements',description:'Read source-dated connected measurements for this verified account. No provider effect or publication.',inputSchema:{type:'object',properties:{provider:{type:'string',pattern:'^[a-z][a-z0-9_]{0,47}$'},limit:{type:'integer',minimum:1,maximum:100}},additionalProperties:false},annotations:{readOnlyHint:true},_meta:{ui:{resourceUri:CONNECTED_APP_URI}}},
  {name:'self_closure',description:'Return only this actor\'s personal closure coordinates and bounded self surfaces. It is not an institution, public profile, or authority grant.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'self_cms_list',description:'List this actor\'s private PhysicalCarrier recipes in the actor-local CMS namespace. A recipe is not a QR/payload/effect.',inputSchema:{type:'object',properties:{limit:{type:'integer',minimum:1,maximum:100}},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'self_foreign_observe',description:'Observe one public URL through ForeignTwin into this actor\'s private root. It retains a receipt/body only; observation is neither publication nor truth/effect authority.',inputSchema:{type:'object',properties:{twin_id:{type:'string'},url:{type:'string',format:'uri'}},required:['twin_id','url'],additionalProperties:false},annotations:{readOnlyHint:false}}
]; }
function adminTools() { return [
  ...WORKSPACE_MCP_TOOLS,
  {name:'bonfyre_admin_credentials_status',description:'List every provider the credential boundary knows: what each needs, which fields are set, missing or generated, and the last verification. Stored values are never returned; a sensitive field shows only its last four characters.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_credential_set',description:'Seal one credential field for one provider. The value travels in this call, so it is visible to this agent session; the loopback credentials page keeps a value out of any transcript. A stored credential is possession, never authority, admission, spend approval or an effect right.',inputSchema:{type:'object',properties:{provider:{type:'string'},field:{type:'string'},value:{type:'string',minLength:1,maxLength:4000}},required:['provider','field','value'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_credential_generate',description:'Generate a field the estate can make for itself (a signing secret, a client id or secret) and seal it. Set reveal to receive the value once, to hand to the other side; it is never shown again.',inputSchema:{type:'object',properties:{provider:{type:'string'},field:{type:'string'},reveal:{type:'boolean'}},required:['provider','field'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_credential_clear',description:'Remove one sealed credential field. Reads that need it wait until it is set again.',inputSchema:{type:'object',properties:{provider:{type:'string'},field:{type:'string'}},required:['provider','field'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_credential_verify',description:'Ask the provider whether the sealed credential works, through its declared check. Headers go to curl on standard input; the answer is only the status and outcome.',inputSchema:{type:'object',properties:{provider:{type:'string'}},required:['provider'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_credential_openapi',description:'Declare a service from its JSON OpenAPI document: its security schemes become the fields it needs and how they are applied (API key header or query, bearer, basic, OAuth client credentials with the token exchange noted).',inputSchema:{type:'object',properties:{provider:{type:'string',pattern:'^[a-z0-9_-]{1,64}$'},name:{type:'string'},spec:{type:'object'}},required:['provider','spec'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_status',description:'Read BonfyreAuth aggregate user/session status from the canonical auth owner.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_users',description:'List canonical BonfyreAuth user records for Bonfyre administration. Password hashes, salts, and session credentials are never returned.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_user_update',description:'Change one user\'s tier or active state through BonfyreAuth. This does not create institution grants, rewrite CMS, or grant effects.',inputSchema:{type:'object',properties:{user_id:{type:'integer',minimum:1},tier:{type:'string',enum:['free','pro','enterprise']},active:{type:'boolean'}},required:['user_id'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_sessions_revoke',description:'Immediately revoke every active session for one user through BonfyreAuth. The user may sign in again for a fresh closure.',inputSchema:{type:'object',properties:{user_id:{type:'integer',minimum:1}},required:['user_id'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_external_descriptors',description:'Read retained public protocol descriptor candidates from MachineMembrane. A descriptor remains only a declared carrier candidate, never an admitted provider, authorized tool, payment route, identity assertion, or effect right.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_connected_measurements',description:'Read source-dated connected measurements scoped to the native administrator identity. No external effect or provider health claim.',inputSchema:{type:'object',properties:{provider:{type:'string',pattern:'^[a-z][a-z0-9_]{0,47}$'},limit:{type:'integer',minimum:1,maximum:100}},additionalProperties:false},annotations:{readOnlyHint:true},_meta:{ui:{resourceUri:CONNECTED_APP_URI}}},
  {name:'bonfyre_admin_provider_history',description:'Read bounded historical provider attempt/success aggregates from the existing Supabase continuity projection. Observation dates are retained; this never reports current provider health, cost, spend, approval or execution authority. Requires the administrator closure and configured service-role provider.',inputSchema:{type:'object',properties:{days:{type:'integer',minimum:1,maximum:730},limit:{type:'integer',minimum:1,maximum:50}},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_provider_materializations',description:'List active, unexpired provider-file, vector-store, collection, container, batch, or remote-URI materialization receipts. These are replaceable scoped body residency only: never artifact identity, canonical knowledge, provider authority, credentials, spend approval, or an admitted result.',inputSchema:{type:'object',properties:{provider:{type:'string'},artifact_sid:{type:'string'}},additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_provider_materialization_release',description:'Retire one provider materialization receipt from reuse. This records released residency and does not delete the canonical artifact, its evidence, a provider account, or an external body.',inputSchema:{type:'object',properties:{receipt_id:{type:'integer',minimum:1},reason:{type:'string',minLength:1,maxLength:160}},required:['receipt_id','reason'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_institution_grant',description:'Grant one user one revocable read/write/admin admission for one named institution through BonfyreAuth. Object-level authority remains independently owned.',inputSchema:{type:'object',properties:{user_id:{type:'integer',minimum:1},institution:{type:'string'},role:{type:'string',enum:['read','write','admin']},expires_at:{type:'string'}},required:['user_id','institution','role'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_institution_revoke',description:'Revoke one user\'s named institutional admission immediately through BonfyreAuth.',inputSchema:{type:'object',properties:{user_id:{type:'integer',minimum:1},institution:{type:'string'}},required:['user_id','institution'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_cms_list',description:'List canonical BonfyreCMS copy in one explicit namespace. The native schema remains authoritative.',inputSchema:{type:'object',properties:{type:{type:'string'},namespace:{type:'string'},limit:{type:'integer',minimum:1,maximum:100}},required:['type'],additionalProperties:false},annotations:{readOnlyHint:true}},
  {name:'bonfyre_admin_cms_create',description:'Create schema-validated canonical CMS copy in one explicit namespace. Creation is not publication or an external effect.',inputSchema:{type:'object',properties:{type:{type:'string'},namespace:{type:'string'},document:{type:'object'}},required:['type','document'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_cms_update',description:'Update schema-validated canonical CMS copy by type and id in one explicit namespace (default root). Older cached tool schemas may pass document.__namespace as a scoped routing coordinate; it is never persisted. Conflicting scopes are refused. This does not bypass publication, evidence, financial, or effect owners.',inputSchema:{type:'object',properties:{type:{type:'string'},namespace:{type:'string'},id:{type:'integer',minimum:1},document:{type:'object'}},required:['type','id','document'],additionalProperties:false},annotations:{readOnlyHint:false}},
  {name:'bonfyre_admin_cms_delete',description:'Delete one canonical CMS row by type and id in one explicit namespace (default root) through the native CMS owner. The action is auditable but does not erase downstream canonical history.',inputSchema:{type:'object',properties:{type:{type:'string'},namespace:{type:'string'},id:{type:'integer',minimum:1}},required:['type','id'],additionalProperties:false},annotations:{readOnlyHint:false}}
]; }
function positiveId(v) { return Number.isInteger(v) && v > 0 && v <= 2147483647; }
function selectEstate(slice, value) { const estate=String(value || ''); const found=slice.estates.find(s=>s.estate===estate); if (!found) throw new Error('estate_not_in_current_closure'); return found; }
function formValue(v) { return encodeURIComponent(String(v)); }
async function privateToolCall(slice, name, args) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments_must_be_object');
  if (name === 'institution_closures') return {actor:slice.actor,closures:slice.estates.map(({estate,scope,inherited})=>({estate,scope,inherited})),limit:'closure coordinates only; query an owner through a listed tool'};
  const selected=selectEstate(slice,args.estate);
  if (name === 'institution_connected_measurements') return readInstitutionMeasurements({estates:slice.estates,estate:selected.estate,provider:args.provider,limit:args.limit??100,baseUrl:placementSupabaseUrl,key:placementSupabaseKey});
  if (name === 'institution_cms_list') {
    if (!atom(args.type,96)) throw new Error('invalid_content_type');
    const limit=Number.isInteger(args.limit) ? args.limit : 25;
    const output=await run([cmsBinary,'entry','list',args.type,'--ns',selected.namespace,'--limit',String(Math.max(1,Math.min(limit,100))),'--db',cmsDb,'--schemas',cmsSchemas]);
    try { return {estate:selected.estate,scope:selected.scope,entries:JSON.parse(output)}; } catch { return {estate:selected.estate,scope:selected.scope,receipt:output.trim()}; }
  }
  if (name === 'institution_cms_create') {
    if (!atom(args.type,96) || !args.document || typeof args.document !== 'object' || Array.isArray(args.document)) throw new Error('invalid_cms_record');
    const output=await run([cmsBinary,'entry','create',args.type,'--ns',selected.namespace,JSON.stringify(args.document),'--db',cmsDb,'--schemas',cmsSchemas]);
    return {estate:selected.estate,scope:selected.scope,created:JSON.parse(output),limit:'CMS creation does not publish a row or make it anonymously readable'};
  }
  if (name === 'institution_foreign_observe') {
    if (!atom(args.twin_id,96) || typeof args.url !== 'string' || !/^https?:\/\//.test(args.url)) throw new Error('invalid_foreign_observation');
    const root=`${foreignRoot}/private/${selected.estate}/${selected.scope}`;
    mkdirSync(`${root}/receipts`,{recursive:true,mode:0o750}); mkdirSync(`${root}/bodies`,{recursive:true,mode:0o750});
    const stamp=new Date().toISOString().replace(/\D/g,'-');
    const receipt=`${root}/receipts/${args.twin_id}-${stamp}.json`, retained=`${root}/bodies/${args.twin_id}-${stamp}.body`;
    const output=await run([projectBinary,'foreign','observe',args.twin_id,args.url,'--receipt',receipt,'--body',retained,'--observe-right','public']);
    return {estate:selected.estate,scope:selected.scope,observation:output.trim(),limit:'ForeignTwin retains an observation receipt; it does not promote a foreign claim to canonical truth'};
  }
  if (name === 'institution_review_request') {
    if (!atom(args.request_kind,96) || !atom(args.reference,96)) throw new Error('invalid_review_request');
    const form=`estate=${formValue(selected.estate)}&scope=${formValue(selected.scope)}&request_kind=${formValue(args.request_kind)}&reference=${formValue(args.reference)}`;
    const output=await run([verticalBinary,'--db',verticalDb,'submit','--actor',slice.actor,'--estate',selected.estate],form);
    return {estate:selected.estate,scope:selected.scope,review_html:output,limit:'This created bounded review work only; no referenced client record was read or mutated'};
  }
  throw new Error('unknown_private_tool');
}
async function selfToolCall(slice, name, args) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments_must_be_object');
  if (name === 'self_connected_measurements') return readAccountMeasurements({actor:slice.actor,provider:args.provider,limit:args.limit??100,baseUrl:placementSupabaseUrl,key:placementSupabaseKey});
  if (name === 'self_closure') return {actor:slice.actor,namespace:slice.namespace,territory:'life',scope:'self',surfaces:['/studio/v1/profile','/cms/','/studio/v1/self/foreign/observe'],limit:'This is an actor-local closure only; it names no public profile, institution, provider, payment, or effect authority'};
  if (name === 'self_cms_list') {
    const limit=Number.isInteger(args.limit) ? args.limit : 25;
    const output=await run([cmsBinary,'entry','list','physical_carrier','--ns',slice.namespace,'--limit',String(Math.max(1,Math.min(limit,100))),'--db',cmsDb,'--schemas',cmsSchemas]);
    try { return {namespace:slice.namespace,entries:JSON.parse(output),limit:'A recipe remains a private projection plan; it is not a carrier issuance or external effect'}; }
    catch { return {namespace:slice.namespace,receipt:output.trim()}; }
  }
  if (name === 'self_foreign_observe') {
    if (!atom(args.twin_id,96) || typeof args.url !== 'string' || !/^https?:\/\//.test(args.url)) throw new Error('invalid_foreign_observation');
    mkdirSync(`${slice.foreignRoot}/receipts`,{recursive:true,mode:0o750}); mkdirSync(`${slice.foreignRoot}/bodies`,{recursive:true,mode:0o750});
    const stamp=new Date().toISOString().replace(/\D/g,'-');
    const receipt=`${slice.foreignRoot}/receipts/${args.twin_id}-${stamp}.json`, retained=`${slice.foreignRoot}/bodies/${args.twin_id}-${stamp}.body`;
    const output=await run([projectBinary,'foreign','observe',args.twin_id,args.url,'--receipt',receipt,'--body',retained,'--observe-right','public']);
    return {actor:slice.actor,observation:output.trim(),limit:'The retained observation stays actor-private and does not publish or promote a foreign claim'};
  }
  throw new Error('unknown_self_tool');
}
async function adminToolCall(name, args, slice) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments_must_be_object');
  if (name.startsWith('bonfyre_admin_credential')) {
    const base=['--db',credentialsDb,'--providers',credentialProviders];
    const keyed=[...base,'--key',credentialsKey];
    const slot=(v,max=64)=>typeof v === 'string' && /^[a-z0-9_-]+$/.test(v) && v.length <= max;
    if (name === 'bonfyre_admin_credentials_status') return {owner:'BonfyreCredentials',...JSON.parse(await run([credentialsBinary,'status',...base]))};
    if (!slot(args.provider)) throw new Error('invalid_provider');
    if (name === 'bonfyre_admin_credential_set') {
      if (!slot(args.field) || typeof args.value !== 'string' || !args.value.length || args.value.length > 4000) throw new Error('invalid_credential_field');
      await run([credentialsBinary,'set',...keyed,'--provider',args.provider,'--field',args.field,'--actor','mcp-admin'],args.value);
      return {owner:'BonfyreCredentials',provider:args.provider,field:args.field,stored:true,shown_again:false,limit:'Possession only; no authority, provider admission, spend approval or effect right'};
    }
    if (name === 'bonfyre_admin_credential_generate') {
      if (!slot(args.field)) throw new Error('invalid_credential_field');
      const argv=[credentialsBinary,'generate',...keyed,'--provider',args.provider,'--field',args.field,'--actor','mcp-admin'];
      if (args.reveal === true) argv.push('--reveal');
      return {owner:'BonfyreCredentials',provider:args.provider,field:args.field,...JSON.parse(await run(argv))};
    }
    if (name === 'bonfyre_admin_credential_clear') {
      if (!slot(args.field)) throw new Error('invalid_credential_field');
      await run([credentialsBinary,'clear',...base,'--provider',args.provider,'--field',args.field,'--actor','mcp-admin']);
      return {owner:'BonfyreCredentials',provider:args.provider,field:args.field,cleared:true};
    }
    if (name === 'bonfyre_admin_credential_verify') {
      try { return {owner:'BonfyreCredentials',...JSON.parse(await run([credentialsBinary,'verify',...keyed,'--provider',args.provider,'--actor','mcp-admin']))}; }
      catch (error) { return {owner:'BonfyreCredentials',provider:args.provider,outcome:'not accepted',detail:String(error.message || error).slice(0,160)}; }
    }
    if (name === 'bonfyre_admin_credential_openapi') {
      if (!args.spec || typeof args.spec !== 'object') throw new Error('invalid_openapi_document');
      const { mkdtempSync, writeFileSync, rmSync } = await import('node:fs');
      const { tmpdir } = await import('node:os');
      const dir=mkdtempSync(`${tmpdir()}/bonfyre-openapi-`);
      try {
        writeFileSync(`${dir}/spec.json`,JSON.stringify(args.spec),{mode:0o600});
        const argv=[credentialsBinary,'openapi','--db',credentialsDb,'--provider',args.provider,'--spec',`${dir}/spec.json`,'--actor','mcp-admin'];
        if (typeof args.name === 'string' && args.name.length <= 200) argv.push('--name',args.name);
        return {owner:'BonfyreCredentials',declared:JSON.parse(await run(argv))};
      } finally { rmSync(dir,{recursive:true,force:true}); }
    }
    throw new Error('unknown_credential_tool');
  }
  if (name === 'bonfyre_admin_connected_measurements') return readAccountMeasurements({actor:slice?.actor,provider:args.provider,limit:args.limit??100,baseUrl:placementSupabaseUrl,key:placementSupabaseKey});
  if (name === 'bonfyre_admin_provider_history') return await providerHistorySummary(args);
  if (name === 'bonfyre_admin_status') return {owner:'BonfyreAuth',status:(await run([authBinary,'--db',authDb,'status'])).trim(),limit:'Aggregate status is administrative observability, not an institution/object/effect closure'};
  if (name === 'bonfyre_admin_users') {
    const output=await run([authBinary,'--db',authDb,'users']);
    try { return {owner:'BonfyreAuth',users:JSON.parse(output),limit:'Canonical users only; no password material or session credential is exposed'}; }
    catch { return {owner:'BonfyreAuth',receipt:output.trim()}; }
  }
  if (name === 'bonfyre_admin_user_update') {
    if (!positiveId(args.user_id) || (args.tier !== undefined && !['free','pro','enterprise'].includes(args.tier)) ||
        (args.active !== undefined && typeof args.active !== 'boolean') || (args.tier === undefined && args.active === undefined)) throw new Error('invalid_user_update');
    const argv=[authBinary,'--db',authDb,'update','--id',String(args.user_id)];
    if (args.tier !== undefined) argv.push('--tier',args.tier);
    if (args.active !== undefined) argv.push('--active',args.active ? '1' : '0');
    return {owner:'BonfyreAuth',updated:(await run(argv)).trim(),limit:'A tier/activity update is not an institution grant, CMS mutation, relationship, or effect authorization'};
  }
  if (name === 'bonfyre_admin_sessions_revoke') {
    if (!positiveId(args.user_id)) throw new Error('invalid_user_id');
    return {owner:'BonfyreAuth',revocation:(await run([authBinary,'--db',authDb,'sessions-revoke','--user-id',String(args.user_id)])).trim(),limit:'This burns active browser/MCP sessions only; it does not change tier, erase account history, or revoke institution admission'};
  }
  if (name === 'bonfyre_admin_external_descriptors') {
    const output=await run([membraneBinary,'descriptors']);
    try { return {owner:'MachineMembrane',candidates:JSON.parse(output),limit:'Public descriptor candidates require ForeignTwin observation, CUE validation, ResourceAdmission, and current Authority before provider or effect use'}; }
    catch { return {owner:'MachineMembrane',receipt:output.trim(),limit:'Descriptor inventory could not be parsed; no candidate has been promoted'}; }
  }
  if (name === 'bonfyre_admin_provider_materializations') {
    if ((args.provider !== undefined && !atom(args.provider,96)) ||
        (args.artifact_sid !== undefined && !atom(args.artifact_sid,160))) throw new Error('invalid_provider_materialization_filter');
    const argv=[providerBinary,'materializations','--db',providerDb];
    if (args.provider !== undefined) argv.push('--provider',args.provider);
    if (args.artifact_sid !== undefined) argv.push('--artifact-sid',args.artifact_sid);
    const output=await run(argv);
    try { return {owner:'PhysicalDerivation',receipts:JSON.parse(output),limit:'Only active, unexpired replaceable residency is listed; provider credentials, canonical artifact bodies, authority, provider admission, spend approval, and effects remain with their respective owners'}; }
    catch { return {owner:'PhysicalDerivation',receipt:output.trim(),limit:'Materialization inventory could not be parsed; no provider residency was promoted'}; }
  }
  if (name === 'bonfyre_admin_provider_materialization_release') {
    if (!positiveId(args.receipt_id) || !atom(args.reason,160)) throw new Error('invalid_provider_materialization_release');
    const output=await run([providerBinary,'release','--db',providerDb,'--receipt-id',String(args.receipt_id),'--reason',args.reason]);
    return {owner:'PhysicalDerivation',release:JSON.parse(output),limit:'Release only retires reuse of this scoped external representation; it does not delete canonical artifact/evidence/account state or perform a provider API deletion'};
  }
  if (name === 'bonfyre_admin_institution_grant') {
    if (!positiveId(args.user_id) || !atom(args.institution,96) || !['read','write','admin'].includes(args.role) ||
        (args.expires_at !== undefined && (typeof args.expires_at !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(args.expires_at)))) throw new Error('invalid_institution_grant');
    const argv=[authBinary,'--db',authDb,'institution-grant','--user-id',String(args.user_id),'--institution',args.institution,'--role',args.role];
    if (args.expires_at !== undefined) argv.push('--expires-at',args.expires_at);
    return {owner:'BonfyreAuth',grant:(await run(argv)).trim(),limit:'Institution admission is revocable transport/product closure only; named object, provider, financial, and effect authority remain separate'};
  }
  if (name === 'bonfyre_admin_institution_revoke') {
    if (!positiveId(args.user_id) || !atom(args.institution,96)) throw new Error('invalid_institution_revoke');
    return {owner:'BonfyreAuth',revocation:(await run([authBinary,'--db',authDb,'institution-revoke','--user-id',String(args.user_id),'--institution',args.institution])).trim(),limit:'This removes the named admission; it does not erase canonical client/CMS/ForeignTwin history'};
  }
  if (name === 'bonfyre_admin_cms_list') {
    if (!atom(args.type,96) || (args.namespace !== undefined && !atom(args.namespace,128))) throw new Error('invalid_cms_list');
    const limit=Number.isInteger(args.limit) ? args.limit : 25;
    const argv=[cmsBinary,'entry','list',args.type,'--ns',args.namespace || 'root','--limit',String(Math.max(1,Math.min(limit,100))),'--db',cmsDb,'--schemas',cmsSchemas];
    const output=await run(argv);
    let entries; try { entries=JSON.parse(output); } catch { throw new Error('cms_list_invalid_native_response'); }
    if (!entries || !Array.isArray(entries.data) || !entries.meta || !Number.isInteger(entries.meta.total)) throw new Error('cms_list_invalid_native_response');
    return {owner:'BonfyreCMS',namespace:args.namespace || 'root',entries,limit:'Copy remains subject to its declared CMS schema and downstream publication/owner boundaries'};
  }
  if (name === 'bonfyre_admin_cms_create') {
    if (!atom(args.type,96) || (args.namespace !== undefined && !atom(args.namespace,128)) || !args.document || typeof args.document !== 'object' || Array.isArray(args.document)) throw new Error('invalid_cms_create');
    const output=await run([cmsBinary,'entry','create',args.type,'--ns',args.namespace || 'root',JSON.stringify(args.document),'--db',cmsDb,'--schemas',cmsSchemas]);
    return {owner:'BonfyreCMS',namespace:args.namespace || 'root',created:JSON.parse(output),limit:'A CMS row is declared copy, not automatic publication, evidence promotion, payment, provider activation, or effect'};
  }
  if (name === 'bonfyre_admin_cms_update') {
    if (!atom(args.type,96) || (args.namespace !== undefined && !atom(args.namespace,128)) || !positiveId(args.id) || !args.document || typeof args.document !== 'object' || Array.isArray(args.document)) throw new Error('invalid_cms_update');
    // Cached MCP clients can lack the top-level namespace field even though
    // this native owner supports it. A reserved nested scope preserves the
    // exact namespace while staying inside their already-admitted schema.
    const embedded=Object.hasOwn(args.document,'__namespace')?args.document.__namespace:undefined;
    if ((embedded !== undefined && !atom(embedded,128)) ||
        (embedded !== undefined && args.namespace !== undefined && embedded !== args.namespace))
      throw new Error('conflicting_or_invalid_cms_namespace');
    const namespace=args.namespace ?? embedded ?? 'root',document={...args.document};
    delete document.__namespace;
    if (!Object.keys(document).length) throw new Error('empty_cms_update');
    const output=await run([cmsBinary,'entry','update',args.type,String(args.id),JSON.stringify(document),'--ns',namespace,'--db',cmsDb,'--schemas',cmsSchemas]);
    return {owner:'BonfyreCMS',namespace,updated:JSON.parse(output),limit:'A CMS update preserves the schema boundary and cannot itself authorize a downstream effect'};
  }
  if (name === 'bonfyre_admin_cms_delete') {
    if (!atom(args.type,96) || (args.namespace !== undefined && !atom(args.namespace,128)) || !positiveId(args.id)) throw new Error('invalid_cms_delete');
    const output=await run([cmsBinary,'entry','delete',args.type,String(args.id),'--ns',args.namespace || 'root','--db',cmsDb,'--schemas',cmsSchemas]);
    return {owner:'BonfyreCMS',namespace:args.namespace || 'root',deleted:JSON.parse(output),limit:'Delete is limited to the CMS row; it does not erase receipts, published copies, financial records, or external effects'};
  }
  throw new Error('unknown_admin_tool');
}
function oauthMetadata() { return { issuer:authOrigin, authorization_endpoint:`${authOrigin}/oauth/authorize`, token_endpoint:`${authOrigin}/oauth/token`, response_types_supported:['code'], grant_types_supported:['authorization_code','refresh_token'], token_endpoint_auth_methods_supported:['none'], code_challenge_methods_supported:['S256'], scopes_supported:['read','write'] }; }
// Some OAuth clients begin at the OpenID discovery path even when they only
// need authorization-server metadata.  It is an interoperable discovery alias
// for this OAuth-only issuer: do not add an `openid` scope or ID-token claims
// until Bonfyre has a separately governed external-identity owner.
function openidDiscoveryAlias() { return oauthMetadata(); }
function protectedMetadata() { return { resource:`${mcpOrigin}/mcp`, authorization_servers:[authOrigin], scopes_supported:['read','write'], bearer_methods_supported:['header'] }; }
function authMd() { return `# Aurekai Bonfyre agent authentication\n\nAurekai's MCP is a protected resource, not a bearer of institutional authority.\n\n- Protected-resource metadata: ${mcpOrigin}/.well-known/oauth-protected-resource/mcp\n- Authorization-server metadata: ${authOrigin}/.well-known/oauth-authorization-server\n- MCP endpoint: ${mcpOrigin}/mcp\n\nUse OAuth 2.0 authorization code with PKCE. A successful OAuth token provides only transport admission; Bonfyre recompiles the caller's current self, institution, or Bonfyre-admin closure for every request. Tokens, passwords, refresh material, private CMS copy, private ForeignTwin bodies, payment approval, and effect authority are never placed in this document.\n`; }
function challenge(v) { return createHash('sha256').update(v).digest('base64url'); }
function rejectMcp(res) { json(res, 401, { error:'oauth_required' }, { 'WWW-Authenticate':`Bearer resource_metadata="${mcpOrigin}/.well-known/oauth-protected-resource/mcp"` }); }
/* The OAuth boundary narrows transport admission only.  Tool-specific effect
 * authority remains with the native owner after forwarding.  Unknown or
 * malformed requests deliberately require write rather than being guessed
 * read-only. */
const readOnlyMcpMethods = new Set(['server/discover','initialize','ping','tools/list','resources/list','resources/templates/list','resources/read','prompts/list','prompts/get','completion/complete','logging/setLevel']);
const readOnlyTools = new Set(['project_status','project_tree','project_search','project_read','project_diff','project_patch_preview','project_test_profile','native_tool_inventory','architecture_atlas','matrix_address_resolve','matrix_subject_constellation','matrix_closure_compile','bonfyre_moe_status','operator_status','operator_backlog_audit','institution_closures','institution_cms_list','self_closure','self_cms_list','bonfyre_admin_external_descriptors','bonfyre_admin_provider_materializations']);
for (const tool of [...SPINES_PROJECT_TOOLS, ...V6_PROJECT_TOOLS, ...FABRIC_PROJECT_TOOLS, ...PRODUCTS_PROJECT_TOOLS, ...WORKSPACE_MCP_TOOLS]) if (tool.annotations.readOnlyHint === true) readOnlyTools.add(tool.name);
function requiredScope(raw) { try { const request=JSON.parse(raw || '{}'); if (request.method==='tools/call') return readOnlyTools.has(request.params?.name) ? 'read' : 'write'; return readOnlyMcpMethods.has(request.method) ? 'read' : 'write'; } catch { return 'write'; } }
function mcpReply(res, id, result) { json(res,200,{jsonrpc:'2.0',id,result}); }
async function forwardPrivate(req, res, raw, slice) {
  let request;
  try { request=JSON.parse(raw || '{}'); } catch { return json(res,400,{error:'invalid_request'}); }
  const id=request.id ?? null, method=request.method, params=request.params || {};
  const modern=modernRequest(req,request), serverName='bonfyre-institution-slice';
  if (modern) { const routingError=validateModernRouting(req,request); if (routingError) return json(res,400,{jsonrpc:'2.0',id,error:{code:-32600,message:routingError}}); }
  try {
    if (method === 'server/discover') return mcpReply(res,id,discoverResult(serverName,'This MCP request is compiled from the caller current institutional closure, not the global Bonfyre project estate. Private CMS and ForeignTwin operations stay inside the listed estate/scope. Every write remains with its native owner and never publishes private state.'));
    if (method === 'initialize') return mcpReply(res,id,{protocolVersion:MCP_LEGACY_VERSION,capabilities:{tools:{listChanged:false},resources:{subscribe:false,listChanged:false},prompts:{listChanged:false},logging:{}},serverInfo:serverInfo(serverName),instructions:'This MCP connection is a current institutional closure, not the global Bonfyre project estate. Start with institution_closures. Private CMS and ForeignTwin operations stay in the listed estate/scope. Every write remains with its native owner and never publishes private state.'});
    if (method === 'notifications/initialized' || method === 'notifications/cancelled') { res.writeHead(202); return res.end(); }
    if (method === 'ping' || method === 'logging/setLevel') return mcpReply(res,id,{});
    if (method === 'tools/list') { const tools=withMcpIcons(privateTools(),MCP_ICONS), digest=apertureDigest(slice,tools); return mcpReply(res,id,modern ? modernList(serverName,{tools},digest) : {tools}); }
    if (method === 'tools/call') {
      const name=String(params.name || '');
      if (!privateTools().some(tool=>tool.name===name)) return mcpReply(res,id,textResult({error:'tool_not_in_current_institution_slice',tool:name},true));
      try { const result=textResult(await privateToolCall(slice,name,params.arguments || {})); return mcpReply(res,id,modern ? modernEnvelope(serverName,result) : result); }
      catch (error) { return mcpReply(res,id,textResult({error:String(error.message || error),tool:name},true)); }
    }
    if (method === 'resources/list') { const resources=withMcpIcons(privateResources(slice),MCP_ICONS), result={resources}, digest=apertureDigest(slice,privateTools(),resources); return mcpReply(res,id,modern ? modernList(serverName,result,digest) : result); }
    if (method === 'resources/read') { const resources=privateResources(slice), result=await readConnectedApp(String(params.uri||'')) || closureResourceRead(slice,String(params.uri||''),true); if (!result) return mcpReply(res,id,{contents:[],isError:true}); return mcpReply(res,id,modern ? modernRead(serverName,result,slice,privateTools(),resources) : result); }
    if (method === 'resources/templates/list') { const result={resourceTemplates:[]}, digest=apertureDigest(slice,privateTools()); return mcpReply(res,id,modern ? modernList(serverName,result,digest) : result); }
    if (method === 'prompts/list') { const result={prompts:[]}, digest=apertureDigest(slice,privateTools()); return mcpReply(res,id,modern ? modernList(serverName,result,digest) : result); }
    return json(res,200,{jsonrpc:'2.0',id,error:{code:-32601,message:'method_not_in_current_institution_slice'}});
  } catch (error) { return json(res,200,{jsonrpc:'2.0',id,error:{code:-32000,message:String(error.message || error)}}); }
}
async function forwardSelf(req, res, raw, slice) {
  let request;
  try { request=JSON.parse(raw || '{}'); } catch { return json(res,400,{error:'invalid_request'}); }
  const id=request.id ?? null, method=request.method, params=request.params || {};
  const modern=modernRequest(req,request), serverName='bonfyre-self-slice';
  if (modern) { const routingError=validateModernRouting(req,request); if (routingError) return json(res,400,{jsonrpc:'2.0',id,error:{code:-32600,message:routingError}}); }
  try {
    if (method === 'server/discover') return mcpReply(res,id,discoverResult(serverName,selfInstructions));
    if (method === 'initialize') return mcpReply(res,id,{protocolVersion:MCP_LEGACY_VERSION,capabilities:{tools:{listChanged:false},resources:{subscribe:false,listChanged:false},prompts:{listChanged:false},logging:{}},serverInfo:serverInfo(serverName),instructions:selfInstructions});
    if (method === 'notifications/initialized' || method === 'notifications/cancelled') { res.writeHead(202); return res.end(); }
    if (method === 'ping' || method === 'logging/setLevel') return mcpReply(res,id,{});
    if (method === 'tools/list') { const tools=withMcpIcons(selfTools(),MCP_ICONS), digest=apertureDigest(slice,tools); return mcpReply(res,id,modern ? modernList(serverName,{tools},digest) : {tools}); }
    if (method === 'tools/call') {
      const name=String(params.name || '');
      if (!selfTools().some(tool=>tool.name===name)) return mcpReply(res,id,textResult({error:'tool_not_in_current_self_slice',tool:name},true));
      try { const result=textResult(await (workspaceTool(name)?workspaceCall(name,params.arguments || {},token(req)):selfToolCall(slice,name,params.arguments || {}))); return mcpReply(res,id,modern ? modernEnvelope(serverName,result) : result); }
      catch (error) { return mcpReply(res,id,textResult({error:String(error.message || error),tool:name,...(workspaceTool(name)?{status:error.status||400,request_id:error.request_id,reconcile:error.reconcile}:{})},true)); }
    }
    if (method === 'resources/list') { const resources=withMcpIcons(selfResources(slice),MCP_ICONS), result={resources}, digest=apertureDigest(slice,selfTools(),resources); return mcpReply(res,id,modern ? modernList(serverName,result,digest) : result); }
    if (method === 'resources/read') { const resources=selfResources(slice), result=await readWorkbookApp(String(params.uri||'')) || await readConnectedApp(String(params.uri||'')) || closureResourceRead(slice,String(params.uri||''),false); if (!result) return mcpReply(res,id,{contents:[],isError:true}); return mcpReply(res,id,modern ? modernRead(serverName,result,slice,selfTools(),resources) : result); }
    if (method === 'resources/templates/list') { const result={resourceTemplates:[]}, digest=apertureDigest(slice,selfTools()); return mcpReply(res,id,modern ? modernList(serverName,result,digest) : result); }
    if (method === 'prompts/list') { const result={prompts:[]}, digest=apertureDigest(slice,selfTools()); return mcpReply(res,id,modern ? modernList(serverName,result,digest) : result); }
    return json(res,200,{jsonrpc:'2.0',id,error:{code:-32601,message:'method_not_in_current_self_slice'}});
  } catch (error) { return json(res,200,{jsonrpc:'2.0',id,error:{code:-32000,message:String(error.message || error)}}); }
}
async function forwardSuperuser(res, req, raw, slice) {
  let parsed;
  try { parsed=JSON.parse(raw || '{}'); } catch { return json(res,400,{error:'invalid_request'}); }
  const id=parsed.id ?? null, method=parsed.method, params=parsed.params || {};
  const modern=modernRequest(req,parsed), serverName='bonfyre-admin-slice';
  if (modern) { const routingError=validateModernRouting(req,parsed); if (routingError) return json(res,400,{jsonrpc:'2.0',id,error:{code:-32600,message:routingError}}); }
  if (method === 'server/discover') return mcpReply(res,id,discoverResult(serverName,'This is the Bonfyre administrative MCP aperture. Native owners still independently enforce object, provider, financial, publication, and effect authority.'));
  if (method === 'resources/read' && params.uri === WORKBOOK_APP_URI) {
    try { const result=await readWorkbookApp(params.uri); return mcpReply(res,id,modern ? modernEnvelope(serverName,result) : result); }
    catch { return json(res,200,{jsonrpc:'2.0',id,error:{code:-32000,message:'Financial review UI is unavailable. Build and deploy workbook-mcp-build.mjs; the workbook tools remain available.'}}); }
  }
  if (method === 'resources/read' && params.uri === CONNECTED_APP_URI) {
    try { const result=await readConnectedApp(params.uri); return mcpReply(res,id,modern ? modernEnvelope(serverName,result) : result); }
    catch { return json(res,200,{jsonrpc:'2.0',id,error:{code:-32000,message:'Connected performance view is unavailable. Build and deploy connected-measurement-mcp-build.mjs; measurement tools remain available.'}}); }
  }
  if (method === 'tools/call' && adminTools().some(tool=>tool.name===String(params.name || ''))) {
    try { const name=String(params.name),result=textResult(await (workspaceTool(name)?workspaceCall(name,params.arguments || {},token(req)):adminToolCall(name,params.arguments || {},slice))); return mcpReply(res,id,modern ? modernEnvelope(serverName,result) : result); }
    catch (error) { return mcpReply(res,id,textResult({error:String(error.message || error),tool:String(params.name || ''),...(workspaceTool(String(params.name))?{status:error.status||400,request_id:error.request_id,reconcile:error.reconcile}:{})},true)); }
  }
  const forwardedHeaders={ 'content-type':req.headers['content-type'] || 'application/json', accept:req.headers.accept || 'application/json, text/event-stream', authorization:`Bearer ${upstreamToken}`, 'content-length':Buffer.byteLength(raw) };
  for (const h of ['mcp-protocol-version','mcp-method','mcp-name']) if (req.headers[h]) forwardedHeaders[h]=req.headers[h];
  const request=http.request({ protocol:upstream.protocol, hostname:upstream.hostname, port:upstream.port, path:upstream.pathname, method:req.method, headers:forwardedHeaders }, upstreamRes => {
    if (!['tools/list','resources/list','initialize'].includes(method)) { res.writeHead(upstreamRes.statusCode || 502, upstreamRes.headers); return upstreamRes.pipe(res); }
    const chunks=[]; upstreamRes.on('data',chunk=>chunks.push(chunk)); upstreamRes.on('end',()=>{
      try { const answer=JSON.parse(Buffer.concat(chunks).toString('utf8'));
        if (method === 'resources/list' && answer.error?.code === -32601 && (upstreamRes.statusCode || 200) < 400) { delete answer.error; answer.result={resources:[]}; }
        if (answer.error || (upstreamRes.statusCode || 200) >= 400) return json(res,upstreamRes.statusCode || 200,answer);
        if (!answer.result || typeof answer.result !== 'object') throw Error('Invalid upstream MCP result');
        if (method === 'initialize') answer.result.capabilities={...answer.result.capabilities,resources:{subscribe:false,listChanged:false,...answer.result.capabilities?.resources}};
        else if (method === 'resources/list') {
          if (!Array.isArray(answer.result.resources)) throw Error('Invalid upstream resources');
          answer.result.resources=[...answer.result.resources.filter(resource=>resource.uri!==WORKBOOK_APP_URI&&resource.uri!==CONNECTED_APP_URI),WORKBOOK_APP_RESOURCE,CONNECTED_APP_RESOURCE];
          if (modern) answer.result=modernList(serverName,answer.result,`sha256:${createHash('sha256').update(JSON.stringify(answer.result.resources.map(v=>v.uri))).digest('hex')}`);
        } else {
          if (!Array.isArray(answer.result.tools)) throw Error('Invalid upstream tools');
          answer.result.tools = withMcpIcons([...answer.result.tools, ...adminTools()], MCP_ICONS); answer.result.tools.sort((a,b)=>String(a.name||'').localeCompare(String(b.name||''))); if (modern) answer.result=modernList(serverName,answer.result,`sha256:${createHash('sha256').update(JSON.stringify(answer.result.tools.map(v=>v.name))).digest('hex')}`);
        }
        json(res,upstreamRes.statusCode || 200,answer); }
      catch { json(res,502,{error:'mcp_owner_invalid_tool_list'}); }
    });
  });
  request.on('error', () => json(res,502,{error:'mcp_owner_unreachable'})); request.end(raw);
}
async function forward(req, res, raw) {
  const access=token(req), scope=requiredScope(raw);
  if (!access || !(await authorized(access, scope))) return rejectMcp(res);
  let slice;
  try { slice=await privateSlice(access); } catch { return rejectMcp(res); }
  /* The project node owns the full development/operations estate.  It is
   * deliberately not a default authenticated-user tool set: only the live
   * Bonfyre admin closure may enter it. Everyone else receives a fresh,
   * independently calculated institutional slice below. */
  if (!slice.superuser) {
    if (slice.estates.length) return forwardPrivate(req,res,raw,slice);
    try { const self=await selfSlice(access); if (self) return forwardSelf(req,res,raw,self); } catch { /* refuse below */ }
    return forwardPrivate(req,res,raw,slice);
  }
  return forwardSuperuser(res,req,raw,slice);
}
function authorizePage(q, error='') { const hidden=['client_id','redirect_uri','state','scope','code_challenge','code_challenge_method'].map(k=>`<input type="hidden" name="${k}" value="${esc(q.get(k))}">`).join(''); return `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authorize Aurekai</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#101611;color:#edf1e9;font:16px/1.5 system-ui}main{width:min(28rem,calc(100% - 3rem));padding:2rem;border:1px solid #405244;border-radius:20px;background:#19231b}h1{margin:0 0:.4rem}.muted{color:#a9b9a8}.error{color:#ffb4a4}label{display:block;margin:1rem 0 .3rem}input{box-sizing:border-box;width:100%;padding:.75rem;border:1px solid #607560;border-radius:9px;background:#0f160f;color:inherit}button{width:100%;margin-top:1.25rem;padding:.8rem;border:0;border-radius:9px;background:#c5e47b;color:#15200e;font-weight:700}</style><main><p class="muted">Bonfÿre authority</p><h1>Connect Aurekai</h1><p class="muted">This grants ChatGPT the requested <b>${esc(q.get('scope') || 'write')}</b> scope through your Bonfÿre account. Tool effects still enforce their own authority boundaries.</p>${error?`<p class="error">${esc(error)}</p>`:''}<form method="post" action="/oauth/authorize">${hidden}<label>Email</label><input required type="email" name="email" autocomplete="username"><label>Password</label><input required type="password" name="password" autocomplete="current-password"><button>Authorize Aurekai</button></form></main>`; }
async function authorizePost(req, res) { const p=form(await body(req)); const redirect=p.get('redirect_uri') || ''; const verifier=p.get('code_challenge') || ''; const method=p.get('code_challenge_method') || ''; const scope=(p.get('scope') || 'write').trim(); if (!/^https:\/\//.test(redirect) || method !== 'S256' || !verifier || !['read','write','read write','write read'].includes(scope)) return html(res,400,authorizePage(p,'Invalid OAuth request.')); try { const out=await runAuth('http-login', JSON.stringify({email:p.get('email') || '',password:p.get('password') || ''})); const login=JSON.parse(out)?.session, session=login?.token, sessionExpires=typeof login?.expires_at === 'string' ? login.expires_at : null; const requestedScope=scope.split(/\s+/).includes('write') ? 'write' : 'read'; if (!session || !(await authorized(session,requestedScope))) return html(res,403,authorizePage(p,`This account does not have ${requestedScope} authority.`)); const code=randomBytes(32).toString('base64url'); await storeAuthorizationCode(code,{ session, sessionExpires, client:p.get('client_id') || '', redirect, verifier, scope, expires:Date.now()+fiveMinutes }); const u=new URL(redirect); u.searchParams.set('code',code); if (p.get('state')) u.searchParams.set('state',p.get('state')); res.writeHead(302,{Location:u.toString(),'Cache-Control':'no-store'}); res.end(); } catch (error) { const unavailable=error?.message?.startsWith('oauth_transaction_store_'); html(res,unavailable ? 503 : 401,authorizePage(p,unavailable ? 'Authorization continuity is temporarily unavailable.' : 'Sign-in failed.')); } }
async function tokenPost(req,res) { const p=form(await body(req)); if (p.get('grant_type') === 'refresh_token') return refreshPost(p,res); let record; try { record=await consumeAuthorizationCode(p.get('code')); } catch { return json(res,503,{error:'temporarily_unavailable'}); } if (!record || record.expires < Date.now() || p.get('grant_type') !== 'authorization_code' || record.client !== (p.get('client_id') || '') || record.redirect !== (p.get('redirect_uri') || '') || !p.get('code_verifier') || challenge(p.get('code_verifier')) !== record.verifier) return json(res,400,{error:'invalid_grant'}); res.setHeader('Cache-Control','no-store'); json(res,200,tokenGrant(record)); }
http.createServer(async (req,res) => { try { const u=new URL(req.url || '/',authOrigin); if (req.method==='GET' && u.pathname==='/.well-known/oauth-authorization-server') return json(res,200,oauthMetadata()); if (req.method==='GET' && u.pathname==='/.well-known/openid-configuration') return json(res,200,openidDiscoveryAlias()); if (req.method==='GET' && u.pathname==='/.well-known/oauth-protected-resource/mcp') return json(res,200,protectedMetadata()); if (req.method==='GET' && u.pathname==='/auth.md') { const value=Buffer.from(authMd()); res.writeHead(200,{'Content-Type':'text/markdown; charset=utf-8','Content-Length':value.length,'Cache-Control':'no-store'}); return res.end(value); } if (u.pathname==='/oauth/authorize') return req.method==='GET' ? html(res,200,authorizePage(u.searchParams)) : req.method==='POST' ? await authorizePost(req,res) : json(res,405,{error:'method_not_allowed'}); if (u.pathname==='/oauth/token' && req.method==='POST') return await tokenPost(req,res); if (u.pathname==='/mcp') return forward(req,res,req.method==='GET'?'':await body(req, MCP_BODY_BYTES)); json(res,404,{error:'not_found'}); } catch { json(res,400,{error:'invalid_request'}); } }).listen(port,host,()=>console.log(`bonfyre OAuth MCP gateway ${host}:${port}`));
