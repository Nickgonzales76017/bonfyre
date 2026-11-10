// Source reading and batch passage review inside the existing Opportunity Desk.
import {selectedRequirement, applySelectedRequirements} from './opportunity-selection.mjs';

export function openSourceReview({root, sources, getRequirements, apply, current, initialSource}) {
  const existing = root.querySelector('dialog[data-source-selection]');
  if (existing) { existing.showModal(); return existing; }
  const prior = document.activeElement;
  const node = (tag, text, attrs = {}) => {
    const n = document.createElement(tag);
    if (text !== undefined) n.textContent = text;
    for (const [key,value] of Object.entries(attrs)) n.setAttribute(key, value);
    return n;
  };
  const button = (text, key) => node('button', text, {type:'button', ['data-'+key]:''});
  const dialog = node('dialog', undefined, {'data-source-selection':'', class:'od-review od-source-review', 'aria-labelledby':'od-source-review-title'});
  const heading = node('h2', 'Read and select source passages', {id:'od-source-review-title'});
  const intro = node('p', 'Select the exact words you want to review. Finding text does not mark a requirement covered or approve its delivery.', {class:'od-hint'});
  const source = node('select', undefined, {'aria-label':'Source document', 'data-source-document':''});
  for (const [id, value] of Object.entries(sources.sources)) {
    const option = node('option', value.title || id, {value:id});
    source.append(option);
  }
  if (initialSource && Object.hasOwn(sources.texts, initialSource)) source.value = initialSource;
  const search = node('input', undefined, {type:'search', placeholder:'Find exact words', maxlength:'500', 'aria-label':'Find exact words', 'data-source-find':''});
  const find = button('Find next', 'source-find-next');
  const stage = button('Stage selected passage', 'source-stage');
  stage.disabled = true;
  const controls = node('div', undefined, {class:'od-source-controls'});
  controls.append(source, search, find, stage);
  const reader = node('pre', undefined, {class:'od-source-reader', tabindex:'0', 'data-source-reader':'', 'aria-label':'Retained source text'});
  const coordinate = node('p', '', {class:'od-hint', 'data-source-coordinate':'', role:'status'});
  const previous = button('Previous section', 'source-previous'), next = button('Next section', 'source-next');
  const paging = node('div', undefined, {class:'od-pills'}); paging.append(previous, next, coordinate);
  const left = node('section', undefined, {class:'od-source-reading'}); left.append(controls, reader, paging);
  const right = node('section', undefined, {class:'od-source-staged'});
  right.append(node('h3', 'Selected passages'));
  const bulk = node('select', undefined, {'aria-label':'Priority for all staged passages', 'data-source-bulk-priority':''});
  for (const [value,label] of [['','Set all priorities…'],['must','Must'],['should','Should'],['could','Could']])
    bulk.append(node('option',label,{value}));
  const staged = node('div', undefined, {'data-source-staged':''});
  right.append(bulk, staged);
  const split = node('div', undefined, {class:'od-source-columns'}); split.append(left, right);
  const message = node('p', '', {role:'status', 'aria-live':'polite', 'data-source-selection-status':''});
  const accept = button('Add reviewed requirements', 'source-accept'), close = button('Keep working', 'source-close');
  const actions = node('div', undefined, {class:'od-pills'}); actions.append(accept, close);
  dialog.append(heading,intro,split,message,actions);
  root.append(dialog);
  let pageStart = 0, pageEnd = 0, picked = null, findAfter = 0, stagedRows = [], closed = false;
  const sourceText = () => sources.texts[source.value] ?? '';
  function intact() {
    if (!current()) throw Error('The open review changed. Reopen its source selection.');
  }
  function showPage(start = 0, highlight = null) {
    const text = sourceText();
    pageStart = Math.max(0, Math.min(start, Math.max(0,text.length-1)));
    if (pageStart > 0 && /[\uDC00-\uDFFF]/.test(text[pageStart]) && /[\uD800-\uDBFF]/.test(text[pageStart-1])) pageStart--;
    pageEnd = Math.min(text.length,pageStart+16000);
    if (pageEnd < text.length && /[\uDC00-\uDFFF]/.test(text[pageEnd]) && /[\uD800-\uDBFF]/.test(text[pageEnd-1])) pageEnd--;
    reader.replaceChildren();
    if (highlight && highlight.start >= pageStart && highlight.end <= pageEnd) {
      const mark = node('mark', text.slice(highlight.start, highlight.end));
      reader.append(document.createTextNode(text.slice(pageStart,highlight.start)),mark,document.createTextNode(text.slice(highlight.end,pageEnd)));
    } else reader.textContent = text.slice(pageStart,pageEnd);
    picked = highlight; stage.disabled = !picked;
    previous.disabled = pageStart === 0; next.disabled = pageEnd >= text.length;
    coordinate.textContent = `Text positions ${pageStart}–${pageEnd} of ${text.length}.`;
  }
  function capture() {
    const selected = window.getSelection();
    if (!selected || selected.isCollapsed || selected.rangeCount !== 1) return;
    const range = selected.getRangeAt(0);
    if (!reader.contains(range.startContainer) || !reader.contains(range.endContainer)) return;
    try {
      const lead = document.createRange(); lead.selectNodeContents(reader); lead.setEnd(range.startContainer,range.startOffset);
      const start = pageStart + lead.toString().length, end = start + range.toString().length;
      picked = selectedRequirement(sources.texts,source.value,start,end);
      stage.disabled = false; coordinate.textContent = `Selected ${end-start} characters at ${start}–${end}.`;
    } catch (error) { picked=null; stage.disabled=true; message.textContent=error.message; }
  }
  function renderStaged() {
    staged.replaceChildren();
    for (const [index, row] of stagedRows.entries()) {
      const article = node('article', undefined, {class:'od-selected-passage'});
      article.append(node('small',sources.sources[row.source]?.title || row.source),node('blockquote',row.text));
      const priority = node('select', undefined, {'aria-label':`Priority for passage ${index+1}`, 'data-passage-priority':String(index)});
      for (const [value,label] of [['','Choose priority'],['must','Must'],['should','Should'],['could','Could']])
        priority.append(node('option',label,{value}));
      priority.value=row.priority; priority.onchange=()=>{row.priority=priority.value;checkReady();};
      const theme = node('input', undefined, {value:row.theme,maxlength:'100', 'aria-label':`Theme for passage ${index+1}`, 'data-passage-theme':String(index), list:'od-source-themes'});
      theme.oninput=()=>{row.theme=theme.value;checkReady();};
      const remove = button('Remove passage','source-remove'); remove.onclick=()=>{stagedRows.splice(index,1);renderStaged();};
      article.append(priority,theme,remove); staged.append(article);
    }
    if (!stagedRows.length) staged.append(node('p','Find exact words or select text in the source, then stage the passage.',{class:'od-hint'}));
    bulk.disabled=!stagedRows.length;
    checkReady();
  }
  const themes = node('datalist',undefined,{id:'od-source-themes'});
  for (const name of new Set(['Other',...getRequirements().map(r=>r.theme)])) themes.append(node('option',undefined,{value:name}));
  dialog.append(themes);
  function checkReady() {
    accept.disabled = !stagedRows.length || stagedRows.some(r=>!r.priority || !r.theme.trim());
  }
  function findNext() {
    try {
      intact(); const text=sourceText(), query=search.value;
      if (!query) { message.textContent='Enter the exact words to find.'; return; }
      let at=text.indexOf(query,findAfter);
      if (at<0 && findAfter>0) at=text.indexOf(query);
      if (at<0) { picked=null;stage.disabled=true;message.textContent='No exact match in this retained document.';return; }
      const selection=selectedRequirement(sources.texts,source.value,at,at+query.length);
      findAfter=selection.end;showPage(Math.max(0,at-3000),selection);
      message.textContent=`Found exact text at ${at}–${selection.end}. Stage it to review its priority and theme.`;
      reader.querySelector('mark')?.scrollIntoView({block:'nearest'});
    } catch(error) { message.textContent=error.message; }
  }
  find.onclick=findNext;
  search.oninput=()=>{findAfter=0;picked=null;stage.disabled=true;};
  search.onkeydown=event=>{if(event.key==='Enter'){event.preventDefault();findNext();}};
  source.onchange=()=>{findAfter=0;search.value='';message.textContent='';showPage();};
  previous.onclick=()=>showPage(Math.max(0,pageStart-16000)); next.onclick=()=>showPage(pageEnd);
  stage.onclick=()=>{
    try {
      intact(); if(!picked)throw Error('Select or find an exact passage first.');
      const row=selectedRequirement(sources.texts,source.value,picked.start,picked.end);
      if(stagedRows.some(r=>r.source===row.source && r.start===row.start && r.end===row.end)){
        message.textContent='That exact passage is already staged.';return;
      }
      if(stagedRows.length>=100)throw Error('Review at most 100 passages at once.');
      stagedRows.push(row);renderStaged();message.textContent=`${stagedRows.length} passages staged. Choose their priorities before adding them.`;
    } catch(error){message.textContent=error.message;}
  };
  bulk.onchange=()=>{if(bulk.value){for(const row of stagedRows)row.priority=bulk.value;renderStaged();}};
  accept.onclick=()=>{
    try {
      intact(); const result=applySelectedRequirements(getRequirements(),stagedRows,sources.texts);
      apply(result); dialog.close();
    } catch(error){message.textContent=error.message;}
  };
  close.onclick=()=>dialog.close();
  const cleanup=()=>{if(closed)return;closed=true;document.removeEventListener('selectionchange',capture);dialog.remove();if(prior?.isConnected)prior.focus();};
  dialog.addEventListener('close',cleanup,{once:true});
  document.addEventListener('selectionchange',capture);
  showPage();renderStaged();dialog.showModal();source.focus();
  return dialog;
}
