import {baseline,restoreDraft,review,makeDraft,draftKey,priceCents,money} from './catalogue-model.mjs';

export function catalogueSession(products,records){
  const session=new Map();
  for(const product of products){
    const original=baseline(product),record=records.get(draftKey(product));let edit=structuredClone(original),error='';
    try{if(record)edit=restoreDraft(product,original,record.value);}catch(e){error=e.message;}
    session.set(product.id,{product,original,edit,saved:structuredClone(edit),revision:record?.revision??0,error});
  }
  return session;
}
export function inventoryRows(session){
  return [...session.values()].flatMap(s=>s.edit.variants.map(v=>({
    key:s.product.id+':'+v.id,productId:s.product.id,variantId:v.id,title:s.edit.title,missing:v.sku.trim()?0:1,
    option:s.product.variants.find(x=>x.id===v.id).label,sku:v.sku,price:v.price,tags:s.edit.tags,
    kind:s.product.kind==='service'?'Service':'Product',image:s.product.image,
    changed:JSON.stringify(s.edit)!==JSON.stringify(s.saved),
    status:s.error?'Needs checking':JSON.stringify(s.edit)!==JSON.stringify(s.saved)?'Unsaved':s.revision?'Draft saved':'Source',
    error:s.error,
  })));
}
export function inventoryProductRows(session){
  return [...session.values()].map(s=>{
    const rows=inventoryRows(new Map([[s.product.id,s]])),first=rows[0];
    const prices=s.edit.variants.map(v=>priceCents(v.price)).sort((a,b)=>a<b?-1:a>b?1:0),missing=rows.filter(r=>r.missing).length;
    return {...first,key:'product:'+s.product.id,option:`${rows.length} ${rows.length===1?'option':'options'}`,missing,
      sku:missing?`${missing} missing`:rows.length===1?first.sku:`${rows.length} SKUs`,
      price:money(prices[0])+(prices[0]===prices.at(-1)?'':' – '+money(prices.at(-1))),
      search:rows.map(r=>r.sku+' '+r.option).join(' ')};
  });
}
export function editInventoryCell(session,row,field,value){
  const s=session.get(row.productId);if(!s||s.error)throw Error(s?.error||'Product no longer available.');
  const next=structuredClone(s.edit);
  if(['title','tags'].includes(field))next[field]=String(value);
  else if(['sku','price'].includes(field)){const v=next.variants.find(v=>v.id===row.variantId);if(!v)throw Error('Option no longer available.');v[field]=String(value);}
  else throw Error('This field cannot be edited.');
  const r=review(s.product,s.original,next);if(r.errors.length)throw Error(r.errors[0].message);
  s.edit=next;
}
export function bulkInventoryProposal(session,rows,action,value){
  if(!rows.length)throw Error('Select at least one option.');
  const copies=new Map([...session].map(([id,s])=>[id,{...s,edit:structuredClone(s.edit)}])),changes=[];
  if(action==='percent'&&!/^-?\d{1,3}(?:\.\d{1,2})?$/.test(value.trim()))throw Error('Enter a percentage from -100 to 999.99.');
  const seen=new Set();
  for(const row of rows){
    const s=copies.get(row.productId),v=s?.edit.variants.find(x=>x.id===row.variantId);
    if(!s||!v||s.error)throw Error(s?.error||'Selected option no longer available.');
    let field,next,before;
    if(action==='tag'){
      const tag=value.trim();if(!tag||tag.includes(',')||tag.length>255)throw Error('Enter one tag, without commas, up to 255 characters.');
      if(seen.has(row.productId))continue;seen.add(row.productId);
      const tags=s.edit.tags.split(',').map(t=>t.trim()).filter(Boolean);
      if(!tags.some(t=>t.toLowerCase()===tag.toLowerCase()))tags.push(tag);
      field='tags';before=s.edit.tags;next=tags.join(', ');
    }else{
      field='price';before=v.price;
      if(action==='price')next=money(priceCents(value));
      else if(action==='percent'){
        const negative=value.trim().startsWith('-'),abs=value.trim().replace(/^-/,'');
        const bps=priceCents(abs)*(negative?-1n:1n);if(bps< -10000n)throw Error('A reduction cannot exceed 100%.');
        next=money((priceCents(v.price)*(10000n+bps)+5000n)/10000n);
      }else throw Error('Choose a supported bulk action.');
    }
    editInventoryCell(copies,row,field,next);
    if(before!==next)changes.push({row,field,before,after:next,title:s.edit.title,option:field==='tags'?'All options':row.option});
  }
  if(!changes.length)throw Error('The selected options already have these values.');
  return {copies,changes};
}
export function inventoryChanges(session){return [...session.values()].filter(s=>JSON.stringify(s.edit)!==JSON.stringify(s.saved));}
export function inventoryDelta(s){
  const changes=[];
  const put=(field,option,before,after)=>{if(before!==after)changes.push({title:s.edit.title,field,option,before,after});};
  for(const [field,value]of Object.entries(s.edit))if(typeof value==='string')put(field,'Product',s.saved[field],value);
  for(const v of s.edit.variants){const old=s.saved.variants.find(x=>x.id===v.id),label=s.product.variants.find(x=>x.id===v.id).label;for(const field of ['sku','price'])put(field,label,old[field],v[field]);}
  return changes;
}
export function inventorySaveBatch(session){
  return inventoryChanges(session).map(s=>{
    if(s.error)throw Error(s.error);
    return {key:draftKey(s.product),expectedRevision:s.revision,value:makeDraft(s.product,s.original,s.edit)};
  });
}
