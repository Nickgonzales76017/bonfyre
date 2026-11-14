#ifndef BONFYRE_PROFILE_JOURNAL_BATCH_H
#define BONFYRE_PROFILE_JOURNAL_BATCH_H
/* Private editorial release orchestration: no new publication authority.
 * Each selected exact route is independently reviewed and committed through
 * the existing CMS page-release action with owner/CSRF/version gates. A
 * rejected or uncertain result never becomes "published" in this UI until
 * the native CMS readback confirms it. */
static void ph_journal_batch_script(void){
 fputs("<script>\n",stdout);
 fputs("(()=>{\n",stdout);
 fputs("for(const root of document.querySelectorAll('[data-profile-journal]')){\n",stdout);
 fputs(" const bar=root.querySelector('[data-journal-batch]');\n",stdout);
 fputs(" if(!bar)return;\n",stdout);
 fputs(" const site=bar.dataset.site;\n",stdout);
 fputs(" const selectAll=bar.querySelector('[data-journal-select-all]');\n",stdout);
 fputs(" const clear=bar.querySelector('[data-journal-clear]');\n",stdout);
 fputs(" const rights=bar.querySelector('[data-journal-rights]');\n",stdout);
 fputs(" const publish=bar.querySelector('[data-journal-publish]');\n",stdout);
 fputs(" const status=bar.querySelector('[data-journal-status]');\n",stdout);
 fputs(" let busy=false;\n",stdout);
 fputs(" const cards=()=>[...root.querySelectorAll('[data-journal-route]')].filter(x=>x.dataset.publicationState==='draft'&&!x.dataset.released);\n",stdout);
 fputs(" if(!cards().length){bar.hidden=true;continue;}\n",stdout);
 fputs(" const chosen=()=>cards().filter(x=>x.querySelector('[data-journal-select]')?.checked);\n",stdout);
 fputs(" const update=()=>{\n",stdout);
 fputs("  const n=chosen().length;\n",stdout);
 fputs("  publish.disabled=busy||!rights.checked||!n;\n",stdout);
 fputs("  selectAll.disabled=busy;clear.disabled=busy;rights.disabled=busy;\n",stdout);
 fputs("  status.textContent=busy?'Publishing selected saved revisions...':n+' draft'+(n===1?'':'s')+' selected for review';\n",stdout);
 fputs(" };\n",stdout);
 fputs(" selectAll.addEventListener('click',()=>{for(const c of cards())if(!c.hidden)c.querySelector('[data-journal-select]').checked=true;update();});\n",stdout);
 fputs(" clear.addEventListener('click',()=>{for(const c of cards())c.querySelector('[data-journal-select]').checked=false;rights.checked=false;update();});\n",stdout);
 fputs(" rights.addEventListener('change',update);\n",stdout);
 fputs(" root.addEventListener('change',event=>{if(event.target.matches('[data-journal-select]'))update();});\n",stdout);
 fputs(" const path=(route,publish=false)=>'/cms/compose/'+(publish?'publish':'page')+'?site='+encodeURIComponent(site)+'&route='+encodeURIComponent(route);\n",stdout);
 fputs(" async function exact(url,options){\n",stdout);
 fputs("  const response=await fetch(url,{credentials:'same-origin',cache:'no-store',...options});\n",stdout);
 fputs("  const data=await response.json().catch(()=>({}));\n",stdout);
 fputs("  if(!response.ok)throw Error(data.error||'CMS refused the requested revision');\n",stdout);
 fputs("  return data;\n",stdout);
 fputs(" }\n",stdout);
 fputs(" publish.addEventListener('click',async()=>{\n",stdout);
 fputs("  if(busy||!rights.checked)return;\n",stdout);
 fputs("  const queue=chosen();\n",stdout);
 fputs("  if(!queue.length)return;\n",stdout);
 fputs("  busy=true;update();let done=0;const blocked=[];\n",stdout);
 fputs("  for(const card of queue){\n",stdout);
 fputs("   const route=card.dataset.journalRoute;\n",stdout);
 fputs("   if(!/^\\/blog\\/[a-z0-9-]+\\/$/.test(route)){blocked.push('Invalid article address');continue;}\n",stdout);
 fputs("   try{\n",stdout);
 fputs("    const current=await exact(path(route));\n",stdout);
 fputs("    if(current.snapshot?.page?.status!=='draft'||!Number.isSafeInteger(current.id)||current.id<1||!/^[a-f0-9]{64}$/.test(current.digest))throw Error('Reopen the saved draft for review');\n",stdout);
 fputs("    const payload={expected_id:current.id,expected_digest:current.digest,rights_confirmed:true};\n",stdout);
 fputs("    try{\n",stdout);
 fputs("     const result=await exact(path(route,true),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});\n",stdout);
 fputs("     if(result.committed!==true||result.id!==current.id)throw Error('Publication was not confirmed');\n",stdout);
 fputs("    }catch(error){\n",stdout);
 fputs("     const after=await exact(path(route));\n",stdout);
 fputs("     if(after.snapshot?.page?.status!=='published')throw error;\n",stdout);
 fputs("    }\n",stdout);
 fputs("    card.dataset.released='1';card.querySelector('[data-journal-select]').checked=false;done++;\n",stdout);
 fputs("    root.dispatchEvent(new Event('journalchange'));\n",stdout);
 fputs("   }catch(error){blocked.push(route+': '+String(error.message||error));}\n",stdout);
 fputs("  }\n",stdout);
 fputs("  busy=false;rights.checked=false;update();\n",stdout);
 fputs("  status.textContent=done+' published'+(blocked.length?'; '+blocked.length+' held for review. '+blocked.slice(0,2).join(' | '):'. Refresh to see the published journal.');\n",stdout);
 fputs(" });\n",stdout);
 fputs(" update();\n",stdout);
 fputs("}\n",stdout);
 fputs("})();\n",stdout);
 fputs("</script>\n",stdout);
}
#endif
