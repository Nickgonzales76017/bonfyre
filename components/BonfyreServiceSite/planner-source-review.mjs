// Browser-only review for exact retained fixture catalogue passages.
// Uses the planner's current plan/revision; never writes a record or contacts a supplier.
import {retainedFixtureText,selectPlannerPassage,adoptPlannerPassage} from './planner-source-view.mjs';
export function openPlannerSourceReview({root,catalogue,current,adopt}){
  const opened=current();
  if(!opened?.plan||opened.plan.catalogue!==catalogue.digest)throw Error('Review a current fixture plan.');
  const dialog=document.createElement('dialog');
  dialog.className='fp-dialog';dialog.setAttribute('aria-label','Review retained fixture sources');
  dialog.setAttribute('data-source-review','');
  dialog.innerHTML='<div class="fp-dialog-head"><h2>Review fixture source</h2><button type="button" data-source-close aria-label="Close source review">Close</button></div>'+
    '<p>These are retained catalogue words, not the supplier webpage. Current price, availability and rights must be independently confirmed.</p>'+
    '<label>Retained product <select data-source-product></select></label>'+
    '<p><a data-source-link target="_blank" rel="noopener noreferrer">Open supplier listing</a></p>'+
    '<label>Highlight the exact passage <textarea data-source-reader rows="10" readonly></textarea></label>'+
    '<div class="fp-pair"><button type="button" data-source-stage>Stage highlighted passage</button>'+
    '<button type="button" data-source-adopt disabled>Add reviewed passage to plan notes</button></div>'+
    '<pre data-source-preview></pre><p data-source-status role="status" aria-live="polite"></p>';
  const field=key=>dialog.querySelector('[data-source-'+key+']');
  const picker=field('product'),reader=field('reader'),preview=field('preview'),status=field('status'),confirm=field('adopt');
  let staged=null;
  for(const product of catalogue.products)picker.add(new Option(product.label,product.id));
  function show(){
    const product=catalogue.products.find(p=>p.id===picker.value);
    staged=null;confirm.disabled=true;preview.textContent='';status.textContent='';
    reader.value=product?retainedFixtureText(product):'';
    if(product){field('link').href=product.source_url;field('link').hidden=false;}
    else field('link').hidden=true;
  }
  picker.addEventListener('change',show);
  field('stage').addEventListener('click',()=>{
    const product=catalogue.products.find(p=>p.id===picker.value);
    try{
      if(!product)throw Error('Select a product first.');
      staged=selectPlannerPassage(opened.plan,catalogue,product.id,reader.selectionStart,reader.selectionEnd);
      preview.textContent='Retained passage:\n'+staged.text+'\n\nSnapshot: '+staged.source_sha256;
      status.textContent='Review the exact passage and confirm to add it to unsaved notes.';
      confirm.disabled=false;
    }catch(e){staged=null;confirm.disabled=true;status.textContent=e.message;}
  });
  confirm.addEventListener('click',()=>{
    const now=current();
    if(!staged||now.revision!==opened.revision||
       JSON.stringify(now.plan)!==JSON.stringify(opened.plan)){
      status.textContent='The plan changed. Open a new source review.';confirm.disabled=true;return;
    }
    try{
      const next=adoptPlannerPassage(now.plan,catalogue,staged);
      adopt(next);
      dialog.close();
    }catch(e){status.textContent=e.message;}
  });
  field('close').addEventListener('click',()=>dialog.close());
  dialog.addEventListener('close',()=>dialog.remove(),{once:true});
  root.append(dialog);show();dialog.showModal();
}
