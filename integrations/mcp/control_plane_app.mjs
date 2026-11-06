import { App } from '@modelcontextprotocol/ext-apps';

const app = new App({ name: 'Aurekai Project Control Plane', version: '1.5.0' });
const $ = name => document.querySelector(`[data-${name}]`);
let packNames = ['operate'], pack = 'operate', lens = null, runtimeIdentity = null, productPlan = null, portfolioPlan = null;

function data(result) {
  if (result?.structuredContent) return result.structuredContent;
  for (const item of result?.content || []) if (item?.type === 'text') { try { return JSON.parse(item.text); } catch {} }
  return result || {};
}
function setDetail(value) { $('detail').textContent = JSON.stringify(value, null, 2); }
function runtimeWarning() {
  const state=runtimeIdentity?.state;
  if(state==='drifted') return 'Runtime drifted: the running MCP source generation differs from the current declared source set.';
  if(state==='unattested') return 'Runtime unattested: this process was not launched with a source-set digest.';
  if(state && state!=='current') return 'Runtime source identity is unavailable; capability truth cannot be tied to the current declared source set.';
  const deployment=runtimeIdentity?.deployment_preview?.state;
  if(deployment && deployment!=='current') {
    return `Deployment metadata is ${deployment}; that is separate from whether the current process is listening or authorized.`;
  }
  return '';
}
function renderRuntime(value) {
  runtimeIdentity=value || {};
  const deployment=runtimeIdentity?.deployment_preview?.state;
  $('connection').textContent=`Connected · ${runtimeIdentity.state || 'unknown'}${deployment ? ` · deploy ${deployment}` : ''}`;
  const warning=runtimeWarning();
  if(warning) $('status').textContent=warning;
}
function metric(slot, label, value) {
  const node=$(slot); node.textContent=value ?? '—';
  const caption=node.nextElementSibling; if(caption) caption.textContent=label;
}
function lensMetrics(value) {
  metric('tools','tools in aperture',String(value.returned ?? value.tools?.length ?? 0));
  const reduction=value.performance?.payload_reduction_percent;
  metric('saved','measured payload reduction',Number.isFinite(reduction) ? `${reduction}%` : '—');
  metric('adapters','installed adapters','—');
  metric('drift','managed drift','—');
}
function snapshotMetrics(snapshot) {
  if(snapshot.readiness?.snapshot==='context_required') {
    metric('tools','snapshot readiness','context required');
    metric('saved','context reads',String(snapshot.readiness.required_context?.length ?? 0));
    metric('adapters','next effects',String(snapshot.readiness.next_effects?.length ?? 0));
    metric('drift','runtime',snapshot.readiness.runtime || 'unknown');
    return;
  }
  if(pack==='commerce') {
    const totals=snapshot.results.commercial_state?.result?.totals || {};
    metric('tools','relationships',totals.relationships ?? '—');
    metric('saved','messages sent',totals.sent ?? '—');
    metric('adapters','classified replies',totals.replied ?? '—');
    metric('drift','cost basis evidenced',totals.cost_basis_evidenced ?? '—');
    return;
  }
  if(pack==='model') {
    const estate=snapshot.results.estate_capabilities || {};
    const registry=estate.capability_registry || {};
    metric('tools','capabilities registered',registry.registry_records ?? '—');
    metric('saved','latency unmeasured',registry.metadata?.latency_unmeasured ?? '—');
    metric('adapters','cost unmeasured',registry.metadata?.cost_unmeasured ?? '—');
    metric('drift','installed adapters',(estate.adapters||[]).filter(x=>x.state==='installed').length);
    return;
  }
  if(pack==='build') {
    const project=snapshot.results.project_status || {};
    const drift=snapshot.results.organ_drift_status || {};
    metric('tools','working changes',project.working?.total ?? '—');
    metric('saved','staged changes',project.staged?.total ?? '—');
    metric('adapters','installed adapters',(snapshot.results.estate_capabilities?.adapters||[]).filter(x=>x.state==='installed').length);
    metric('drift','managed drift',(drift.behind||0)+(drift.missing||0)+(drift.refused||0));
    return;
  }
  const drift=snapshot.results.organ_drift_status || {};
  const estate=snapshot.results.estate_capabilities || {};
  const fabric=snapshot.results.fabric_status || {};
  metric('tools','live fabric tasks',fabric.tasks?.active ?? fabric.tasks?.running ?? '—');
  metric('saved','queued fabric tasks',fabric.tasks?.queued ?? '—');
  metric('adapters','installed adapters',(estate.adapters||[]).filter(x=>x.state==='installed').length);
  metric('drift','managed drift',(drift.behind||0)+(drift.missing||0)+(drift.refused||0));
}
function toolRow(tool) {
  const el = document.createElement('div'); el.className='tool';
  const name=document.createElement('b');name.textContent=tool.name;
  const desc=document.createElement('small');
  const flags=[tool.read_only===true?'read':'effect',tool.owner].filter(Boolean).join(' · ');
  desc.textContent=`${flags ? flags+' — ' : ''}${tool.description || ''}`;
  el.append(name,desc);return el;
}
function planRow(value) {
  const el=document.createElement('div');el.className='tool';
  const name=document.createElement('b');name.textContent='Outcome';
  const desc=document.createElement('small');desc.textContent=value?.pack_plan?.outcome || value?.pack_description || 'Capability shortlist';
  el.append(name,desc);
  const handoffs=value?.pack_plan?.handoffs || [];
  if(handoffs.length){
    const note=document.createElement('small');
    note.textContent=`Handoffs: ${handoffs.map(item=>`${item.name} → ${item.carrier}`).join(', ')}`;
    el.append(note);
  }
  return el;
}
function renderLens(value) {
  lens = value;
  const available = Object.keys(value?.available_packs || {});
  if(available.length) {
    packNames = available;
    if(!packNames.includes(pack)) pack = packNames[0];
  }
  lensMetrics(value);
  $('tool-list').replaceChildren(planRow(value), ...(value.tools || []).map(toolRow));
  packs();
  for (const button of document.querySelectorAll('[data-pack]')) button.setAttribute('aria-pressed', String(button.dataset.pack === pack));
}
async function call(name, args={}) {
  try { const result = await app.callServerTool({ name, arguments: args }); return data(result); }
  catch (error) { throw new Error(`${name}: ${error.message}`); }
}
function renderProductPlan(value) {
  productPlan=value || {};
  renderRuntime(productPlan.runtime_identity);
  if(productPlan.lens) renderLens(productPlan.lens);
  if(productPlan.snapshot) snapshotMetrics(productPlan.snapshot);
  setDetail(productPlan);
  const warning=runtimeWarning();
  const readiness=productPlan.readiness || {};
  $('status').textContent=warning || (readiness.snapshot==='degraded'
    ? `${productPlan.snapshot?.failures?.length || 0} read(s) unavailable; details retained below.`
    : readiness.snapshot==='context_required'
      ? `Context required: ${(readiness.required_context || []).join(', ') || 'account/destination context'}.`
      : '');
}

function portfolioRow(plan) {
  const el=document.createElement('div');el.className='tool';
  const name=document.createElement('b');name.textContent=plan.pack || 'unknown pack';
  const desc=document.createElement('small');
  const readiness=plan.readiness?.snapshot || plan.snapshot?.state || 'unknown';
  const handoffs=plan.route?.handoffs?.length || 0;
  const effects=plan.route?.next_effects?.length || 0;
  desc.textContent=`${readiness} · ${handoffs} handoff(s) · ${effects} next effect(s)`;
  el.append(name,desc);
  return el;
}
function renderPortfolio(value) {
  portfolioPlan=value || {};
  renderRuntime(portfolioPlan.runtime_identity);
  const selected=portfolioPlan.selected_packs || [];
  metric('tools','selected packs',String(selected.length));
  metric('saved','round trips avoided',String(portfolioPlan.performance?.round_trips_avoided ?? 0));
  metric('adapters','handoffs',String(portfolioPlan.handoffs?.length ?? 0));
  metric('drift','next effects',String(portfolioPlan.next_effects?.length ?? 0));
  $('tool-list').replaceChildren(...(portfolioPlan.plans || []).map(portfolioRow));
  setDetail(portfolioPlan);
  const readiness=portfolioPlan.readiness || {};
  const warning=runtimeWarning();
  $('status').textContent=warning || (readiness.state==='degraded'
    ? 'Portfolio is degraded; inspect unavailable pack reads before continuing.'
    : readiness.state==='context_required'
      ? `Portfolio needs context: ${(readiness.required_context || []).join(', ') || 'account/destination context'}.`
      : readiness.state==='packs_required'
        ? 'No capability pack matched this objective. Choose packs explicitly or refine the objective.'
        : `Portfolio ready across ${selected.join(', ') || 'no selected packs'}; effects remain separate.`);
}
async function planPortfolio() {
  const intent=String($('portfolio-intent')?.value || '').trim();
  if(!intent){$('status').textContent='Enter an objective to compose a portfolio.';return;}
  $('status').textContent='Composing cross-pack portfolio…';
  try{renderPortfolio(await call('mcp_portfolio_plan',{intent,max_packs:5,limit:8}));}
  catch(error){$('status').textContent=error.message;}
}
async function loadPack(id) {
  pack=id;
  $('status').textContent = `Loading ${id}…`;
  try { renderProductPlan(await call('mcp_product_plan',{pack:id,limit:20})); }
  catch(error){ $('status').textContent=error.message; }
}
async function refreshPack() {
  $('status').textContent = `Refreshing ${pack} truth…`;
  try { renderProductPlan(await call('mcp_product_plan',{pack,limit:20})); }
  catch(error) { $('status').textContent=error.message; }
}
function packs() {
  $('packs').replaceChildren(...packNames.map(id => { const button=document.createElement('button');button.className='pack';button.dataset.pack=id;button.textContent=id;button.setAttribute('aria-pressed',String(id===pack));button.onclick=()=>loadPack(id);return button; }));
}
$('estate').textContent='Refresh pack';
$('estate').onclick=refreshPack;
$('portfolio').onclick=planPortfolio;
$('portfolio-intent').addEventListener('keydown',(event)=>{if(event.key==='Enter'){event.preventDefault();planPortfolio();}});
$('operator').onclick=async()=>{ $('status').textContent='Checking STOP…'; try{setDetail(await call('operator_status'));$('status').textContent='';}catch(error){$('status').textContent=error.message;} };
$('drift-check').onclick=async()=>{ $('status').textContent='Checking drift…'; try{const value=await call('organ_drift_status');$('drift').textContent=String((value.behind||0)+(value.missing||0)+(value.refused||0));setDetail(value);$('status').textContent='';}catch(error){$('status').textContent=error.message;} };
app.ontoolresult=result=>{const value=data(result);if(value?.schema_version==='bonfyre.mcp.portfolio-plan.v1')renderPortfolio(value);else if(value?.schema_version==='bonfyre.mcp.product-plan.v1')renderProductPlan(value);else if(value?.schema_version==='bonfyre.mcp.capability-lens.v1')renderLens(value);else if(value?.schema_version==='bonfyre.project-mcp.runtime-identity.v1')renderRuntime(value);else if(value?.schema_version==='bonfyre.mcp.pack-snapshot.v1'){renderRuntime(value.runtime_identity);snapshotMetrics(value);setDetail(value);}};
app.ontoolcancelled=()=>{$('status').textContent='The host cancelled this operation.';};
packs(); app.connect().then(()=>loadPack(pack)).catch(error=>{$('connection').textContent='Unavailable';$('status').textContent=error.message;});
