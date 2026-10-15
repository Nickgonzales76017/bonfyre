import {draftKey} from './catalogue-model.mjs';
import {escapeHTML as esc} from './workspace-navigation.mjs';

export const catalogueViews=[
  {id:'inventory',label:'Catalogue',available:()=>true},
  {id:'products',label:'Products',available:()=>true},
  {id:'drafts',label:'Saved drafts',available:()=>true},
  {id:'sent',label:'Sent for review',available:()=>true},
];
export function draftRows(config,records,store,receiptFor){
  const source=config.stores.find(s=>s.id===store);
  return config.entries.filter(p=>p.store===store&&records.has(draftKey(p))).map(p=>{
    const record=records.get(draftKey(p)),d=record.value;
    const intact=d?.schema==='service-site.product-draft.v1'&&d.store===store&&d.product_id===p.id&&d.source_sha256===source.sha256&&Array.isArray(d.changes)&&d.changes.every(c=>typeof c?.field==='string'&&typeof c.after==='string');
    const receipt=intact?receiptFor(p):null;
    return {product:p,title:intact?(d.changes.find(c=>c.field==='title')?.after??p.title):p.title,changes:intact?d.changes.length:0,savedAt:d?.saved_at,revision:record.revision,receipt,status:!intact?'Needs checking':receipt?'Sent for review':'Saved draft'};
  }).sort((a,b)=>(Date.parse(b.savedAt)||0)-(Date.parse(a.savedAt)||0)||a.product.id.localeCompare(b.product.id));
}
export function deskView(rows,view,query=''){
  const visible=rows.filter(r=>(view!=='sent'||r.receipt)&&`${r.title} ${r.product.id}`.toLowerCase().includes(query.toLowerCase()));
  const date=value=>Number.isFinite(Date.parse(value))?new Date(value).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}):'Date unavailable';
  return `<div class="cw-desk-heading"><div><h1>${view==='sent'?'Sent for review':'Saved drafts'}</h1><p>${view==='sent'?'Receipts for the versions you sent.':'Pick up a saved version and review its changes.'}</p></div><button type="button" class="cw-button" data-desk-refresh>Refresh</button></div><form data-desk-search class="cw-desk-search"><label>Find a draft<input type="search" name="query" value="${esc(query)}" placeholder="Product title or ID"></label><button class="cw-button" type="submit">Search</button></form><div class="cw-desk-list">${visible.map(r=>`<article class="cw-desk-row"><div><span class="cw-tag">${r.status}</span><h2>${esc(r.title)}</h2><p>${r.changes} saved ${r.changes===1?'change':'changes'} · ${esc(date(r.savedAt))}</p>${r.receipt?`<small>Receipt ${esc(r.receipt.reference.split(':').at(-1).slice(0,12))}</small>`:''}</div><a class="cw-button" href="?store=${encodeURIComponent(r.product.store)}&product=${encodeURIComponent(r.product.id)}&workspace=products&panel=review" data-desk-product="${esc(r.product.id)}">${r.receipt?'View sent version':'Review draft'}</a></article>`).join('')||`<div class="cw-empty"><h2>${query?'No matching drafts':view==='sent'?'No reviews sent':'No saved drafts'}</h2><p>${query?'Try another product title or ID.':view==='sent'?'Review a saved draft, then send it when you are ready.':'Edit a product and save your changes to see it here.'}</p><button class="cw-button" type="button" data-workspace-view="products">Open products</button></div>`}</div><p class="cw-desk-note">Drafts and receipt history are saved in this browser. A review receipt means the draft was received; it does not mean the store was updated.</p>`;
}
