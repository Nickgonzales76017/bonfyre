// The site's own server side, written into the build as _worker.js when the
// record says requests are kept on a server. It does one thing: take a request
// from one of this site's forms and keep it. Everything else is the static site.
// A request is kept only if it names a form this site has and only the fields
// that form asks for; nothing about the visitor is recorded beyond what they typed.
// To stop one sender or a flood filling the store, it counts: at most PER_SENDER
// requests an hour from one network address and PER_DAY a day for the whole
// site. The sender is counted under a scrambled mark of the address that is
// kept for an hour, apart from any request, and cannot be read back as an address.
import { validateReviewDraft } from './catalogue-review.mjs';
import { validatePlan } from './planner-model.mjs';
export function workerSource(siteId, forms) {
  return `const SITE = ${JSON.stringify(siteId)}, FORMS = ${JSON.stringify(forms)}, PER_SENDER = 5, PER_DAY = 300;
const answer = (status, body) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "cache-control": "no-store" } });
const validateReviewDraft = ${validateReviewDraft.toString()};
const validatePlan = ${validatePlan.toString()};
export async function takeLead(request, env) {
  const url = new URL(request.url);
  const routed = url.pathname.startsWith('/api/lead/') ? url.pathname.slice('/api/lead/'.length) : null;
  const routeForm = routed && Object.hasOwn(FORMS, routed) && (FORMS[routed].catalogue || FORMS[routed].planner) ? FORMS[routed] : null;
  if (routed && !routeForm) return answer(404, {ok:false, why:'no such review form'});
  const maxBytes = routeForm ? routeForm.max_bytes : 8000;
  if (request.method !== "POST") return answer(405, { ok: false, why: "post only" });
  const origin = request.headers.get("origin");
  try { if (!origin || new URL(origin).origin !== url.origin) return answer(403, { ok: false, why: "not from this site" }); } catch { return answer(403, { ok: false, why: "not from this site" }); }
  if (!(request.headers.get("content-type") || "").startsWith("application/json")) return answer(415, { ok: false, why: "json only" });
  const declared=request.headers.get("content-length");
  if(declared&&(!/^[0-9]+$/.test(declared)||Number(declared)>maxBytes))return answer(413,{ok:false,why:"too long"});
  const reader=request.body?.getReader();let size=0,parts=[],text="";
  if(reader)try{for(;;){const item=await reader.read();if(item.done)break;size+=item.value.byteLength;if(size>maxBytes){await reader.cancel();return answer(413,{ok:false,why:"too long"});}parts.push(item.value);}const bytes=new Uint8Array(size);let at=0;for(const p of parts){bytes.set(p,at);at+=p.length;}text=new TextDecoder("utf-8",{fatal:true}).decode(bytes);}catch{return answer(400,{ok:false,why:"invalid body"});}finally{reader.releaseLock();}
  let body; try { body = JSON.parse(text); } catch { return answer(400, { ok: false, why: "not json" }); }
  const form = body && typeof body.form==="string" && Object.hasOwn(FORMS,body.form) ? FORMS[body.form] : null;
  if (!form || !Array.isArray(body.fields) || body.fields.length>Object.keys(form.fields).length) return answer(400, { ok: false, why: "no such form" });
  if ((routed && body.form !== routed) || ((form.catalogue || form.planner) && !routeForm)) return answer(400, {ok:false, why:'wrong review route'});
  if(form.page && body.page && body.page!==form.page)return answer(400,{ok:false,why:"wrong form page"});
  // A filled trap field is a machine; it is told yes and nothing is kept.
  if (body.company_website) return answer(201, { ok: true });
  const fields = [], seen = new Set();
  for (const field of body.fields) {
    if (!field || typeof field.label !== "string" || typeof field.value !== "string" || seen.has(field.label)) return answer(400, { ok: false, why: "bad field" });
    const allowed = Object.hasOwn(form.fields,field.label) ? form.fields[field.label] : null;
    if (!allowed) return answer(400, { ok: false, why: "not a field of this form" });
    const value = field.value.trim();
    if(value.length>(allowed.max_length ?? 300))return answer(400,{ok:false,why:"field too long"});
    if(allowed.email && !/^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$/.test(value))return answer(400,{ok:false,why:"invalid email"});
    if (Array.isArray(allowed) && value && !allowed.includes(value)) return answer(400, { ok: false, why: "not one of the choices" });
    seen.add(field.label);
    if (value) fields.push({ label: field.label, value });
  }
  for (const need of form.required) if (!fields.some(f=>f.label===need && f.value)) return answer(400, { ok: false, why: "missing " + need });
  if(form.catalogue)try{await validateReviewDraft(JSON.parse(fields.find(f=>f.label==='Product draft').value),form.catalogue);}catch(error){return answer(400,{ok:false,why:error.message});}
  if(form.planner)try{validatePlan(JSON.parse(fields.find(f=>f.label==='Fixture plan').value),form.planner);}catch(error){return answer(400,{ok:false,why:error.message});}
  if((form.catalogue || form.planner) && new TextEncoder().encode(JSON.stringify(JSON.stringify(fields))).length>24000)return answer(413,{ok:false,why:'draft exceeds review storage limit'});
  if (!env.LEADS) return answer(503, { ok: false, why: "nowhere to keep it" });
  const received_at = new Date().toISOString();
  const fingerprint = (form.catalogue || form.planner) ? [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify({form:body.form,page:form.page,fields:fields.toSorted((a,b)=>a.label.localeCompare(b.label))}))))].map(b=>b.toString(16).padStart(2,'0')).join('') : null;
  const key = fingerprint ? 'lead:'+(form.planner?'planner':'catalogue')+':'+fingerprint : "lead:" + received_at + ":" + crypto.randomUUID();
  if (fingerprint && await env.LEADS.get(key)) return answer(200,{ok:true,receipt:key});
  const day = "count:day:" + received_at.slice(0, 10), today = parseInt(await env.LEADS.get(day) || "0", 10);
  if (today >= PER_DAY) return answer(429, { ok: false, why: "too many today" });
  const mark = [...new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(SITE + "|" + received_at.slice(0, 13) + "|" + (request.headers.get("cf-connecting-ip") || "unknown"))))].map(b => b.toString(16).padStart(2, "0")).join("");
  const sender = "count:sender:" + mark, sent = parseInt(await env.LEADS.get(sender) || "0", 10);
  if (sent >= PER_SENDER) return answer(429, { ok: false, why: "too many from here" });
  await env.LEADS.put(sender, String(sent + 1), { expirationTtl: 3600 });
  await env.LEADS.put(day, String(today + 1), { expirationTtl: 60 * 60 * 48 });
  await env.LEADS.put(key, JSON.stringify({ site_id: SITE, form: body.form, page: form.page ?? String(body.page || "").slice(0, 120), received_at, fields }), { expirationTtl: 60 * 60 * 24 * 180 });
  return answer(201, { ok: true, ...(fingerprint ? {receipt:key} : {}) });
}
export default { fetch(request, env) { const path=new URL(request.url).pathname; return path === "/api/lead" || path.startsWith('/api/lead/') ? takeLead(request, env) : env.ASSETS.fetch(request); } };
`;
}
