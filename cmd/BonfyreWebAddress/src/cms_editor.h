#ifndef BONFYRE_CMS_EDITOR_H
#define BONFYRE_CMS_EDITOR_H
/* Ephemeral editor state; native CMS remains the record and write owner. */
#define BF_CMS_EDITOR_SCRIPT \
"<script>\n" \
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
