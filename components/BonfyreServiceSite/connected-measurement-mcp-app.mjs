import {App} from '@modelcontextprotocol/ext-apps';
const app=new App({name:'Aurekai connected performance',version:'1.0.0'});
const $=key=>document.querySelector('[data-'+key+']');
const el=(tag,text,cls)=>{const x=document.createElement(tag);if(text!==undefined)x.textContent=String(text);if(cls)x.className=cls;return x};
const safe=value=>typeof value==='string'?value:'';
const labels={google_my_business:'Google Business Profile',youtube:'YouTube',linkedin_organic:'LinkedIn',supabase:'Supabase'};
const titles={impressions:'Search & Maps impressions',website_clicks:'Website visits from Google',views:'Video views',subscribers_total:'Subscribers (snapshot)',channel_views_total:'Channel views (lifetime snapshot)'};
let data=null,filter='all',busy=false,selectedEstate='',institutionChoices=[];
const exactCount=n=>Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(n);
const providerName=p=>labels[p]||p.replaceAll('_',' ');
const prettyMetric=m=>titles[m]||m.replaceAll('_',' ');
const age=x=>x<24?x+'h ago':Math.floor(x/24)+'d ago';
const date=d=>new Date(d+'T00:00:00Z').toLocaleDateString('en-US',{timeZone:'UTC',month:'short',day:'numeric',year:'numeric'});
function decode(result){
 const value=result?.structuredContent||result?.result?.structuredContent||result?.content?.find(x=>x.type==='text')?.text;
 const r=typeof value==='string'?JSON.parse(value):value;
 return r?.schema==='bonfyre.account.measurement.portfolio.v1'&&Array.isArray(r.rows)?r:null;
}
function providerRows(){return data?.rows.filter(r=>filter==='all'||r.provider===filter)||[]}
function cell(label,value){const card=el('article',undefined,'cp-stat');card.append(el('small',label),el('strong',value));return card}
function state(note){$('status').textContent=note||'';$('refresh').disabled=busy}
function render(){
 if(!data)return;const rows=providerRows(),providers=[...new Set(data.rows.map(r=>r.provider))].sort();
 $('subject').textContent='Account '+data.subject;const chosen=filter;
 $('channels').replaceChildren();
 const opts=[['all','All sources'],...providers.map(x=>[x,providerName(x)])];
 for(const [key,title] of opts){const b=el('button',title,'cp-tab');b.type='button';b.dataset.active=String(chosen===key);b.setAttribute('aria-pressed',String(chosen===key));b.onclick=()=>{filter=key;render()};$('channels').append(b);}
 const freshest=data.rows.length?Math.min(...data.rows.map(r=>r.age_hours)):null;
 $('summary').replaceChildren(
  cell('Connected sources',String(providers.length)),
  cell('Measurements',String(data.rows.length)),
  cell('Latest observation',freshest===null?'None':age(freshest)),
  cell('Needs refresh',String(data.rows.filter(r=>r.stale).length))
 );
 const groupBy=new Map();
 for(const r of rows){const arr=groupBy.get(r.metric)||[];arr.push(r);groupBy.set(r.metric,arr);}
 $('metric').replaceChildren();
 for(const key of groupBy.keys()){const o=el('option',prettyMetric(key));o.value=key;$('metric').append(o);}
 if(!$('metric').querySelector('option[value="'+($('metric').dataset.selected||'')+'"]'))$('metric').dataset.selected=[...groupBy.keys()][0]||'';
 $('metric').value=$('metric').dataset.selected;
 $('metric').disabled=!groupBy.size;
 $('metric').onchange=()=>{$('metric').dataset.selected=$('metric').value;drawChart()};
 drawChart();
 $('records').replaceChildren();
 for(const r of rows.slice().sort((a,b)=>b.period.to.localeCompare(a.period.to)||a.provider.localeCompare(b.provider)||a.metric.localeCompare(b.metric))){
  const row=el('tr');
  const isPartial=r.period.kind==='custom',name=providerName(r.provider)+(r.account==='personal_channel'?' · personal':'');
  const period=date(r.period.from)+(r.period.from===r.period.to?'':' – '+date(r.period.to))+(isPartial?' · partial':'');
  for(const t of [name,prettyMetric(r.metric),exactCount(r.value)+(r.unit==='percent'?'%':''),period,age(r.age_hours)]){
    const td=el('td',t);row.append(td);
  }row.dataset.stale=String(r.stale);$('records').append(row);
 }
 $('empty').hidden=rows.length>0;
 const notices=[];
 if(rows.some(r=>r.period.kind==='custom'))notices.push('Partial reporting periods are not compared as complete months.');
 if(rows.some(r=>r.account==='personal_channel'))notices.push('The connected YouTube account is personal; these figures do not represent a branded Aurekai channel.');
 if(rows.some(r=>r.stale))notices.push('Some source snapshots are over seven days old. Refresh the source connection before acting.');
 if(!rows.length)notices.push('No measurements have been admitted for this source. An installed connector alone is not evidence of working reporting.');
 $('notes').replaceChildren(...notices.map(x=>el('p',x)));
 $('refresh').disabled=busy;state('Data from verified account scope · read-only projection · no external posts or actions');
}
function drawChart(){
 $('chart').replaceChildren();const metric=$('metric').value;
 const rows=providerRows().filter(x=>x.metric===metric).slice().sort((a,b)=>a.period.from.localeCompare(b.period.from));
 if(!rows.length){$('chart').append(el('p','Select a measurement to view its reported periods.'));return}
 const width=720,height=220,ns='http://www.w3.org/2000/svg';
 const svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 '+width+' '+height);svg.setAttribute('role','img');svg.setAttribute('aria-label',prettyMetric(metric)+' across reported periods');
 const max=Math.max(1,...rows.map(r=>r.value));const length=Math.min(14,rows.length),samples=rows.slice(-length),barW=Math.min(56,(width-82)/Math.max(1,length)*.6);
 samples.forEach((r,i)=>{const center=55+(i+.5)*(width-85)/length,barH=Math.max(r.value>0?3:1,(r.value/max)*155);
   const shape=document.createElementNS(ns,'rect');shape.setAttribute('x',center-barW/2);shape.setAttribute('y',178-barH);shape.setAttribute('width',barW);shape.setAttribute('height',barH);shape.setAttribute('rx','5');shape.setAttribute('fill',r.period.kind==='custom'?'#799188':'#266c50');svg.append(shape);
   const t=document.createElementNS(ns,'text');t.setAttribute('x',center);t.setAttribute('y',174-barH);t.setAttribute('text-anchor','middle');t.setAttribute('class','cp-value');t.textContent=exactCount(r.value);svg.append(t);
   const d=document.createElementNS(ns,'text');d.setAttribute('x',center);d.setAttribute('y','204');d.setAttribute('text-anchor','middle');d.setAttribute('class','cp-label');d.textContent=new Date(r.period.to+'T00:00:00Z').toLocaleDateString('en-US',{timeZone:'UTC',month:'short',day:'numeric'});svg.append(d);
 });
 $('chart').append(svg);
 $('chart-note').textContent=samples.some(x=>x.period.kind==='custom')?'Lighter bars indicate partial reporting periods. Comparisons are not normalized by days.':'Only source-reported periods are displayed. No values have been estimated.';
}
const context=()=>selectedEstate?'institution:'+selectedEstate:'self';
async function openInstalled(kind){
 try{
  const result=await app.callServerTool({name:'workspace_list',arguments:{context:context()}});
  const payload=result?.structuredContent??result?.content?.find(x=>x.type==='text')?.text;
  const catalogue=typeof payload==='string'?JSON.parse(payload):payload;
  const item=catalogue?.workspaces?.find(w=>w.ready&&typeof w.pack==='string'&&
    (kind==='connections'?/connection-studio/.test(w.pack):/campaign/.test(w.pack)));
  if(!item||typeof item.editor_url!=='string'||!/^https:\/\/aurekai\.work\/apps\/ws-[a-f0-9]{40}\//.test(item.editor_url)){
    state('The installed '+(kind==='connections'?'Connection Studio':'Source-to-campaign editor')+' is not currently available from this account. The native workspace directory must be recovered.');return;
  }
  await app.openLink({url:item.editor_url});
 }catch{state('The native workspace directory did not answer. Your measurements remain available here, but the destination editor cannot be opened yet.');}
}
$('connection-link').onclick=()=>void openInstalled('connections');
$('campaign-link').onclick=()=>void openInstalled('campaign');
$('institution').onchange=()=>{selectedEstate=$('institution').value;data=null;refresh()};
async function refresh(){
 if(busy)return;busy=true;state('Reading connected account measurements…');
 const names=selectedEstate?['institution_connected_measurements']:['self_connected_measurements','bonfyre_admin_connected_measurements'];
 for(const name of names){
  try{const argumentsValue=name==='institution_connected_measurements'?{estate:selectedEstate,limit:100}:{limit:100};
   const result=await app.callServerTool({name,arguments:argumentsValue}),r=decode(result);
   if(r){data=r;busy=false;render();return}}
  catch{}
 }
 // An institutional account can have several lawful destinations. Never
 // infer an organization from a personal profile or select another account.
 if(!selectedEstate){
  try{
   const result=await app.callServerTool({name:'institution_closures',arguments:{}});
   const raw=result?.structuredContent??result?.content?.find(x=>x.type==='text')?.text;
   const c=typeof raw==='string'?JSON.parse(raw):raw;
   const list=Array.isArray(c?.closures)?c.closures.filter(x=>/^[a-z0-9][a-z0-9-]{0,63}$/.test(x.estate??'')):[];
   if(list.length){institutionChoices=[...new Set(list.map(x=>x.estate))].sort();
    const select=$('institution');select.replaceChildren();
    select.append(new Option('Choose organization',''));
    for(const estate of institutionChoices)select.append(new Option(estate,estate));
    select.hidden=false;busy=false;
    if(institutionChoices.length===1){selectedEstate=institutionChoices[0];select.value=selectedEstate;return refresh()}
    state('Choose the organization whose installed connections you want to inspect.');return;
   }
  }catch{}
 }
 busy=false;state('The current account measurement owner did not answer. The last verified result remains visible.');
}
$('refresh').onclick=refresh;
app.ontoolresult=result=>{const r=decode(result);if(r){data=r;
  if(r.subject.startsWith('institution:'))selectedEstate=r.subject.slice(12);
  render()}};
app.ontoolcancelled=()=>state('The host cancelled this read; the previous measurements remain.');
app.connect().then(refresh).catch(error=>state('This view could not connect: '+error.message));
