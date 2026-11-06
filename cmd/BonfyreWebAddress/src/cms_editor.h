#ifndef BONFYRE_CMS_EDITOR_H
#define BONFYRE_CMS_EDITOR_H
/* Ephemeral editor state; native CMS remains the record and write owner. */
#define BF_CMS_EDITOR_SCRIPT \
"<style id=\"cms-editor-v2\">.cms-authoring{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:22px;grid-column:1/-1;align-items:start;min-width:0}.cms-writing{min-width:0;display:grid;gap:13px}.cms-source-field[hidden],.cms-paragraphs[hidden]{display:none!important}.cms-writing label{padding:15px;border:1px solid #d8e1d0;border-radius:14px}.cms-writing input[name=title]{font:400 32px/1.15 Georgia,serif!important;letter-spacing:-.035em;border:0!important;background:transparent!important}.cms-writing textarea{min-height:380px;resize:vertical;background:#fff!important;font:16px/1.8 system-ui!important;border:0!important;padding:5px!important;width:100%;box-sizing:border-box}.cms-editor-toolbar{display:flex;flex-wrap:wrap;gap:7px;align-items:center}.cms-editor-button{padding:9px 12px!important;min-height:40px;border:1px solid #c8d7bf!important;border-radius:10px!important;background:#edf3e8!important;color:#214332!important;font:600 12px system-ui!important;cursor:pointer}.cms-editor-button[aria-pressed=true]{background:#254d36!important;color:white!important}.cms-editor-button:disabled{opacity:.35;cursor:default}.cms-editor-button:focus-visible{outline:3px solid #83a560;outline-offset:2px}.cms-editor-metrics{font:11px monospace;color:#5a715c;padding:6px}.cms-document-preview{background:#e9eee3;padding:18px;border-radius:18px;min-width:0;position:sticky;top:90px}.cms-preview-label{font:10px/1.5 monospace;text-transform:uppercase;letter-spacing:.08em;color:#58705b;margin:0 0 16px}.cms-document-paper{padding:clamp(22px,3vw,42px);background:#fffef7;border:1px solid #dde4d3;border-radius:3px;box-shadow:0 15px 35px #172e1515;min-height:400px;box-sizing:border-box;max-width:100%;margin:auto;overflow-wrap:anywhere}.cms-document-paper h2{font:400 clamp(28px,3vw,44px)/1.07 Georgia,serif;letter-spacing:-.04em;color:#1b3426;margin:0 0 24px}.cms-document-body{font:17px/1.75 Georgia,serif;color:#243429;white-space:pre-wrap}.cms-phone-preview .cms-document-paper{max-width:390px;padding:24px}.cms-document-settings{grid-column:1/-1;display:grid;gap:12px;padding:20px;background:#f0f4eb;border-radius:15px}.cms-document-settings summary{cursor:pointer;font:600 15px system-ui;color:#254331}.cms-document-settings:not([open])>label{display:none!important}.cms-document-settings>label{margin-top:12px}.cms-paragraphs{display:grid;gap:13px}.cms-paragraph{border:1px solid #d6e0cd;border-radius:13px;background:white;padding:14px}.cms-paragraph textarea{min-height:90px}.cms-paragraph-tools{display:flex;align-items:center;gap:5px;margin-bottom:10px}.cms-paragraph-tools>span{margin-right:auto;color:#60745d;font:11px monospace}.cms-shell:has(.cms-form){overflow:clip}.cms-form-head{top:var(--cms-nav-offset,0px)}.cms-form[data-saving=true]{opacity:.8}.cms-form>input[type=hidden]{display:none!important}@media(max-width:1100px){.cms-authoring{grid-template-columns:1fr}.cms-document-preview{position:static}.cms-document-paper{min-height:240px}.cms-writing textarea{min-height:270px}}@media(prefers-reduced-motion:no-preference){.cms-document-paper{transition:max-width .2s ease}.cms-editor-button{transition:background .15s ease}}\n" \
"</style><script>\n" \
"(() => {\n" \
"  'use strict';\n" \
"  const pane = document.querySelector('#cms-pane');\n" \
"  const types = document.querySelector('#cms-types');\n" \
"  if (!pane || !types || pane.dataset.editorController) return;\n" \
"  pane.dataset.editorController = 'native-2';\n" \
"  const accountBar=document.querySelector('.ak-bar'),alignToolbar=()=>pane.style.setProperty('--cms-nav-offset',Math.ceil(accountBar?.getBoundingClientRect().height||0)+'px');\n" \
"  alignToolbar();const barObserver=new ResizeObserver(alignToolbar);if(accountBar)barObserver.observe(accountBar);window.addEventListener('pagehide',()=>barObserver.disconnect(),{once:true});\n" \
"  const el = (tag, cls, text) => { const n=document.createElement(tag); if(cls)n.className=cls; if(text!==undefined)n.textContent=text; return n; };\n" \
"  const button = (label, action) => { const b=el('button','cms-editor-button',label);b.type='button';b.addEventListener('click',action);return b; };\n" \
"  const dirty = () => pane.querySelector('.cms-form[data-dirty=\"true\"]');\n" \
"  function unlock(form) {for(const input of form?.querySelectorAll('[data-save-disabled]')||[]){input.disabled=input.dataset.saveDisabled==='true';delete input.dataset.saveDisabled;}}\n" \
"  function reviewLeave() { return !dirty() || confirm('Leave this editor and discard your unsaved changes?'); }\n" \
"  const nav=types.querySelector('.cms-types');\n" \
"  if(nav&&!types.querySelector('.cms-type-search')) {\n" \
"    const box=el('div','cms-type-search'),input=el('input'),label=el('label',null,'Find a content kind'),count=el('span','cms-nav-count');\n" \
"    input.type='search';input.placeholder='Pages, lessons, products…';input.setAttribute('aria-label','Search native content kinds');box.append(label,input,count);nav.before(box);\n" \
"    const update=()=>{const rows=[...nav.querySelectorAll('button')];let visible=0;for(const row of rows){row.hidden=!row.textContent.toLowerCase().includes(input.value.toLowerCase().trim());if(!row.hidden)visible++;}count.textContent=visible+' of '+rows.length+' registered kinds';};input.addEventListener('input',update);update();\n" \
"  }\n" \
"  function enhance() {\n" \
"    const form=pane.querySelector('.cms-form');\n" \
"    if(!form||form.dataset.authoring)return;\n" \
"    form.dataset.authoring='2';form.dataset.dirty='false';\n" \
"    form.addEventListener('invalid',event=>{const group=event.target.closest('details');if(group)group.open=true;},true);\n" \
"    const address=form.getAttribute('hx-put')||form.getAttribute('hx-post'),match=address?.match(/^\\/cms\\/v1\\/api\\/([a-z][a-z0-9_]*)(?:\\/([1-9][0-9]*))?$/);\n" \
"    if(!match)return;\n" \
"    const url=new URL(location.href);url.searchParams.set('type',match[1]);url.searchParams.set('id',match[2]||'new');history.replaceState(null,'',url.pathname+url.search+url.hash);\n" \
"    const heading=form.querySelector('.cms-form-head h3');if(heading)heading.textContent=(match[2]?'Edit ':'New ')+match[1].replaceAll('_',' ');\n" \
"    const actions=form.querySelector('.cms-form-head .actions');\n" \
"    if(actions&&!actions.querySelector('.cms-dirty-indicator'))actions.prepend(el('span','cms-dirty-indicator','Unsaved changes'));\n" \
"    const text=form.querySelector('textarea[name=\"content\"]')||form.querySelector('textarea[name=\"body\"]');\n" \
"    if(!text)return;\n" \
"    const title=form.querySelector('[name=\"title\"]'),longLabel=text.closest('label');\n" \
"    const work=el('section','cms-authoring'),write=el('div','cms-writing'),preview=el('aside','cms-document-preview'),toolbar=el('div','cms-editor-toolbar'),blocks=el('div','cms-paragraphs'),metrics=el('span','cms-editor-metrics');\n" \
"    preview.setAttribute('aria-label','Current text preview');\n" \
"    const paper=el('article','cms-document-paper'),previewHeading=el('h2'),previewBody=el('div','cms-document-body');\n" \
"    const previewLabel=el('p','cms-preview-label','Text preview · not a publication');paper.append(previewHeading,previewBody);preview.append(previewLabel,paper);work.append(write,preview);\n" \
"    longLabel.before(work);if(title)write.append(title.closest('label'));write.append(toolbar,longLabel,blocks);blocks.hidden=true;\n" \
"    longLabel.classList.add('cms-source-field');text.spellcheck=true;\n" \
"    const settings=el('details','cms-document-settings'),summary=el('summary',null,'Address, publication and other fields');settings.append(summary);\n" \
"    for(const label of [...form.children].filter(n=>n.tagName==='LABEL'))settings.append(label);\n" \
"    if(settings.children.length>1)work.after(settings);\n" \
"    let segments=[],blockMode=false;\n" \
"    const sync=()=>{text.value=segments.map(s=>s.text+s.gap).join('');text.dispatchEvent(new Event('input',{bubbles:true}));};\n" \
"    function drawBlocks(){\n" \
"      blocks.replaceChildren();segments.forEach((segment,index)=>{\n" \
"        const row=el('section','cms-paragraph'),tools=el('div','cms-paragraph-tools'),input=el('textarea');\n" \
"        input.value=segment.text;input.setAttribute('aria-label','Paragraph '+(index+1));input.rows=Math.max(3,Math.min(10,segment.text.split('\\n').length+1));\n" \
"        input.addEventListener('input',()=>{segment.text=input.value;sync();});\n" \
"        const up=button('↑',()=>{[segments[index-1].text,segments[index].text]=[segments[index].text,segments[index-1].text];sync();drawBlocks();});up.disabled=index===0;up.setAttribute('aria-label','Move paragraph '+(index+1)+' up');\n" \
"        const down=button('↓',()=>{[segments[index].text,segments[index+1].text]=[segments[index+1].text,segments[index].text];sync();drawBlocks();});down.disabled=index===segments.length-1;down.setAttribute('aria-label','Move paragraph '+(index+1)+' down');\n" \
"        tools.append(el('span',null,'Paragraph '+(index+1)),up,down);row.append(tools,input);blocks.append(row);\n" \
"      });\n" \
"    }\n" \
"    const sourceButton=button('Source',()=>{blockMode=false;blocks.hidden=true;longLabel.hidden=false;sourceButton.setAttribute('aria-pressed','true');paragraphButton.setAttribute('aria-pressed','false');text.focus();});\n" \
"    const paragraphButton=button('Paragraphs',()=>{\n" \
"      if(!blockMode){const pieces=text.value.split(/(\\n{2,})/);segments=[];for(let i=0;i<pieces.length;i+=2)segments.push({text:pieces[i],gap:pieces[i+1]||''});drawBlocks();}\n" \
"      blockMode=true;blocks.hidden=false;longLabel.hidden=true;sourceButton.setAttribute('aria-pressed','false');paragraphButton.setAttribute('aria-pressed','true');\n" \
"    });\n" \
"    sourceButton.setAttribute('aria-pressed','true');paragraphButton.setAttribute('aria-pressed','false');\n" \
"    const add=button('Add paragraph',()=>{if(!blockMode)paragraphButton.click();if(segments.length&&!segments.at(-1).gap)segments.at(-1).gap='\\n\\n';segments.push({text:'',gap:''});sync();drawBlocks();blocks.lastElementChild?.querySelector('textarea')?.focus();});\n" \
"    const size=button('Phone preview',()=>{preview.classList.toggle('cms-phone-preview');size.textContent=preview.classList.contains('cms-phone-preview')?'Full preview':'Phone preview';});\n" \
"    toolbar.append(sourceButton,paragraphButton,add,size,metrics);\n" \
"    const update=()=>{previewHeading.textContent=title?.value||'Untitled';previewBody.textContent=text.value;const words=text.value.trim()?text.value.trim().split(/\\s+/).length:0;metrics.textContent=words+' words · '+new TextEncoder().encode(text.value).length+' bytes';};\n" \
"    form.addEventListener('input',update);text.addEventListener('invalid',()=>{sourceButton.click();},true);update();\n" \
"  }\n" \
"  pane.addEventListener('input',event=>{const form=event.target.closest('.cms-form');if(form)form.dataset.dirty='true';});\n" \
"  pane.addEventListener('change',event=>{const form=event.target.closest('.cms-form');if(form)form.dataset.dirty='true';});\n" \
"  document.addEventListener('htmx:beforeRequest',event=>{\n" \
"    const trigger=event.detail?.elt,form=pane.querySelector('.cms-form');if(!trigger||!form)return;\n" \
"    const leaving=trigger.closest('[hx-get]');\n" \
"    if(leaving?.getAttribute('hx-target')==='#cms-pane'&&!reviewLeave()){event.preventDefault();return;}\n" \
"    if(trigger===form){form.dataset.submitted=JSON.stringify([...new FormData(form).entries()]);form.dataset.saving='true';form.querySelectorAll('input,textarea,select,button').forEach(b=>{b.dataset.saveDisabled=String(b.disabled);b.disabled=true;});}\n" \
"  });\n" \
"  document.addEventListener('htmx:afterSwap',event=>{\n" \
"    if(event.detail?.target?.id==='cms-pane')enhance();\n" \
"    if(event.detail?.target?.id==='cms-save'){\n" \
"      const form=pane.querySelector('.cms-form');if(!form)return;unlock(form);\n" \
"      const success=event.detail.target.querySelector('.cms-status.ok');\n" \
"      if(success&&form.dataset.submitted===JSON.stringify([...new FormData(form).entries()])){\n" \
"        form.dataset.dirty='false';const reopen=success.querySelector('[hx-get]')?.getAttribute('hx-get');\n" \
"        if(reopen&&window.htmx?.ajax)window.htmx.ajax('GET',reopen,{target:pane,swap:'innerHTML'});\n" \
"      }\n" \
"    }\n" \
"  });\n" \
"  document.addEventListener('htmx:afterRequest',event=>{const form=event.detail?.elt;if(form?.matches('.cms-form')){form.dataset.saving='false';unlock(form);}});\n" \
"  window.addEventListener('beforeunload',event=>{if(dirty()){event.preventDefault();event.returnValue='';}});\n" \
"  const query=new URLSearchParams(location.search),kind=query.get('type'),id=query.get('id');\n" \
"  if(kind&&/^[a-z][a-z0-9_]*$/.test(kind)&&[...nav?.querySelectorAll('button')||[]].some(b=>b.getAttribute('hx-get')?.endsWith('type='+kind))){\n" \
"    const part=id==='new'||/^[1-9][0-9]*$/.test(id||'')?'edit?type='+kind+'&id='+id:'list?type='+kind;\n" \
"    if(window.htmx?.ajax)window.htmx.ajax('GET','/cms/parts/'+part,{target:pane,swap:'innerHTML'});\n" \
"  }\n" \
"  enhance();\n" \
"})();\n" \
"\n" \
"</script>"
#endif
