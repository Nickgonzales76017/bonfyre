import { readLocal, saveLocal } from './store.mjs';

const $ = id => document.getElementById(id);
const token = document.querySelector('meta[name="document-session"]').content;
let collection, saved = [], source = null, paragraph = 1, revision = 0, locked = false;
let playingParagraph = -1;
const time = ms => { const s=Math.floor(ms/1000); return `${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`; };
const recordingLabel = r => `${time(r.start_ms)}–${time(r.end_ms)}`;
const locationLabel = (p,r) => r ? `transcript ${recordingLabel(r)} · paragraph ${p}` : `paragraph ${p}`;
const element = (tag, text, className) => {
  const node = document.createElement(tag); node.textContent = text;
  if (className) node.className = className;
  return node;
};
const prefix = () => `documents:${collection.collection_id}:`;
const key = (d, p = paragraph) => prefix() + d.id + ':' + d.digest + ':' + p;
const noteFor = (d,p) => saved.find(r=>r.value.document_id===d.id&&r.value.digest===d.digest&&r.value.paragraph===p);
const report = error => { $('error').textContent = error?.message || ''; $('error').hidden = !error; };
async function api(path, body) {
  const response = await fetch('/api/' + path, { method:body ? 'POST' : 'GET',
    headers:{ 'X-Document-Session':token, ...(body ? {'Content-Type':'application/json'} : {}) },
    ...(body ? {body:JSON.stringify(body)} : {}) });
  const value = await response.json();
  if (!response.ok) throw Error(value.error || 'The workspace could not complete that request.');
  return value;
}
async function action(work) {
  if (locked) return;
  locked = true; report(null); document.body.setAttribute('aria-busy','true');
  const controls = [...document.querySelectorAll('button,input,select,textarea')];
  const disabled = controls.map(n => n.disabled); controls.forEach(n => n.disabled = true);
  try { await work(); } catch (error) { report(error); }
  finally { controls.forEach((n,i) => n.disabled = disabled[i]); locked = false; document.body.removeAttribute('aria-busy'); }
}
function renderNotes() {
  $('notes').replaceChildren();
  for (const row of saved) {
    const note = row.value, doc = collection.documents.find(d => d.id === note.document_id && d.digest === note.digest);
    const card = element('div','','saved-note');
    card.append(element('p',note.text), element('small',`${note.title} · ${locationLabel(note.paragraph,note.recording)}`));
    if (doc?.indexed) {
      const open = element('button','Open cited source','quiet'); open.type = 'button';
      open.onclick = () => action(() => openSource(doc.id,note.paragraph)); card.append(open);
    } else card.append(element('small','Source removed from search','removed'));
    $('notes').append(card);
  }
  if (!saved.length) $('notes').append(element('p','Notes you save here stay in this browser.','muted'));
}
async function refresh() {
  collection = await api('documents');
  saved = await readLocal(prefix());
  $('title').textContent = collection.title;
  document.title = collection.title;
  const recordings=collection.documents.filter(d=>d.duration_ms);
  document.body.classList.toggle('recording-workspace',recordings.length>0);
  $('workspace-kind').textContent=recordings.length?'RECORDING WORKSPACE':'DOCUMENT WORKSPACE';
  $('query').placeholder=recordings.length?'Find a topic, an explanation or a moment…':'Search your documents…';
  $('collection-note').textContent=recordings.length?'Find a passage and play it from the recording.':'';
  $('documents').replaceChildren();
  for (const doc of collection.documents) {
    const button = element('button','','document'); button.type = 'button';
    button.dataset.document = doc.id; button.disabled = !doc.indexed;
    button.append(element('span',doc.title),element('small',doc.indexed ? `${doc.duration_ms?time(doc.duration_ms)+' · ':''}${doc.passage_count} passages` : 'Removed from search'));
    button.onclick = () => action(() => openSource(doc.id)); $('documents').append(button);
  }
  renderNotes();
  $('status').textContent = `${collection.documents.filter(d => d.indexed).length} documents in search · Local on this Mac`;
}
async function openSource(id, selectedParagraph = 1) {
  const next = await api('source?id='+encodeURIComponent(id));
  clearRecording();
  source = next; paragraph = selectedParagraph;
  $('transcript-status').hidden=!source.media;
  $('transcript-status').textContent=source.media ? `${time(source.media.duration_ms)} · ${source.media.segments.length} transcript passages` : '';
  $('recording-controls').hidden=!source.media;
  $('source-credit').hidden=!source.media?.credit;
  $('source-credit').textContent=source.media?.credit||'';
  $('source-link').hidden=!source.media?.source_url;
  if(source.media?.source_url)$('source-link').href=source.media.source_url;else $('source-link').removeAttribute('href');
  if(source.media) {
    const player=$('recording-player');player.hidden=false;
    player.classList.toggle('audio-only',source.media.content_type.startsWith('audio/'));
    await new Promise((resolve,reject)=>{
      const cleanup=()=>{clearTimeout(timer);player.removeEventListener('loadedmetadata',ready);player.removeEventListener('error',failed);};
      const ready=()=>{cleanup();resolve();};const failed=()=>{cleanup();reject(Error('The recording could not be played. Reopen the source and try again.'));};
      const timer=setTimeout(failed,15000);player.addEventListener('loadedmetadata',ready);player.addEventListener('error',failed);
      player.src='/api/media?id='+encodeURIComponent(id);player.load();
    });
    player.currentTime=source.media.segments[paragraph-1].start_ms/1000;
  }
  const row = noteFor(source,paragraph); revision = row?.revision ?? 0;
  $('source-title').textContent = source.title; $('source-text').replaceChildren();
  source.text.split(/\n\s*\n/).filter(p => p.trim()).forEach((text,i) => {
    const p = element('p',text); p.dataset.paragraph = i + 1;
    if (i + 1 === paragraph) p.className = 'selected-passage';
    const segment=source.media?.segments[i];
    p.append(element('small',`¶ ${i+1}${segment ? ' · '+recordingLabel(segment) : ''}`));
    if(segment) {
      const play=element('button','Play from '+time(segment.start_ms),'quiet');play.type='button';play.dataset.recordingParagraph=i+1;
      play.onclick=()=>action(async()=>{
        await api('source?id='+encodeURIComponent(source.id));
        paragraph=i+1;
        const selectedNote=noteFor(source,paragraph);revision=selectedNote?.revision??0;
        for(const node of $('source-text').querySelectorAll('[data-paragraph]'))node.classList.toggle('selected-passage',Number(node.dataset.paragraph)===paragraph);
        renderCitation();$('recording-player').currentTime=segment.start_ms/1000;await $('recording-player').play();
      });p.append(play);
    }
    $('source-text').append(p);
  });
  renderCitation();
  $('note').value = row?.value.text || '';
  $('save-state').textContent = row ? 'Saved note loaded. Saving again uses the paragraph shown above.' : 'Your note will include this source and paragraph.';
  $('source').hidden = false; document.querySelector('.workspace').classList.add('has-source');
  $('source-title').tabIndex = -1; $('source-title').focus({preventScroll:true});
  const selected = $('source-text').querySelector('.selected-passage');
  if (innerWidth < 1100) $('source').scrollIntoView({block:'start'});
  else if (selected) $('source-text').scrollTop = selected.offsetTop;
}
function renderCitation() {
  const segment=source.media?.segments[paragraph-1];
  $('citation').textContent=`${source.title} · ${locationLabel(paragraph,segment)}`;
}
function clearRecording() {
  playingParagraph=-1;
  $('recording-player').pause();$('recording-player').removeAttribute('src');$('recording-player').load();$('recording-player').hidden=true;
}
function followPlayback() {
  if(!source?.media)return;
  const ms=$('recording-player').currentTime*1000;
  const index=source.media.segments.findIndex(s=>ms>=s.start_ms&&ms<s.end_ms);
  if(index===playingParagraph)return;
  playingParagraph=index;
  for(const node of $('source-text').querySelectorAll('[data-paragraph]')) {
    const active=Number(node.dataset.paragraph)===index+1;
    node.classList.toggle('playing-passage',active);
    if(active){node.setAttribute('aria-current','true');if($('follow-recording').checked)$('source-text').scrollTop=node.offsetTop;}
    else node.removeAttribute('aria-current');
  }
}
$('recording-player').addEventListener('timeupdate',followPlayback);
$('recording-player').addEventListener('seeked',followPlayback);
window.addEventListener('resize',()=>requestAnimationFrame(()=>{
  if(source?.media&&$('follow-recording').checked){playingParagraph=-1;followPlayback();}
}));
$('download-transcript').onclick=()=>{
  if(!source)return;
  const text=[source.title,source.media?.credit||'',source.media?.source_url||'',...source.text.split(/\n\s*\n/).map((text,i)=>`${source.media?recordingLabel(source.media.segments[i])+'\n':''}${text}`)].filter(Boolean).join('\n\n');
  const url=URL.createObjectURL(new Blob([text+'\n'],{type:'text/plain;charset=utf-8'})),link=document.createElement('a');
  link.href=url;link.download=source.id+'-transcript.txt';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
};
function closeSource() { clearRecording();source = null; $('source').hidden = true; document.querySelector('.workspace').classList.remove('has-source'); }
$('close-source').onclick = closeSource;
$('search-form').onsubmit = event => {
  event.preventDefault();
  action(async () => {
    $('status').textContent = 'Searching the local collection…';
    try {
      const value = await api('search',{query:$('query').value,mode:$('mode').value});
      $('results').replaceChildren(); $('results-title').textContent = value.results.length ? 'Passages to review' : 'No matching passages';
      $('guidance').textContent = value.results.length ? value.message : 'Try a different phrase or open a document from the collection.';
      for (const result of value.results) {
        const card = element('article','','result');
        card.append(element('p',`${result.title} · ${locationLabel(result.paragraph,result.recording)}`,'result-source'),element('p',result.text));
        const open = element('button','Read original','quiet'); open.type = 'button';
        open.onclick = () => action(() => openSource(result.document_id,result.paragraph)); card.append(open);
        if(result.recording){const play=element('button','Play '+time(result.recording.start_ms),'quiet');play.type='button';play.dataset.playResult=result.paragraph;play.onclick=()=>action(async()=>{await openSource(result.document_id,result.paragraph);await $('recording-player').play();});card.append(play);}
        $('results').append(card);
      }
      $('status').textContent = `${value.results.length} ${value.mode === 'phrase' ? 'matching' : 'possible'} passages · Local on this Mac`;
    } catch (error) { $('status').textContent = 'Search did not finish.'; throw error; }
  });
};
$('note').oninput = () => $('save-state').textContent = 'Changes not saved';
$('save-note').onclick = () => action(async () => {
  if (!source || !$('note').value.trim()) throw Error('Write a note before saving it.');
  // Revalidate the original before retaining a new citation. An older saved note remains readable.
  await api('source?id='+encodeURIComponent(source.id));
  const value = {document_id:source.id,digest:source.digest,title:source.title,paragraph,text:$('note').value.trim(),saved_at:new Date().toISOString()};
  if(source.media)value.recording={...source.media.segments[paragraph-1],sha256:source.media.sha256,transcript_sha256:source.media.transcript_sha256,review:source.media.review};
  const existing=noteFor(source,paragraph);
  const row = await saveLocal(existing?.key??key(source),value,revision); revision = row.revision;
  saved = saved.filter(r => r.key !== row.key).concat(row); renderNotes();
  $('save-state').textContent = 'Saved in this browser';
});
$('remove').onclick = () => action(async () => {
  if (!source) return;
  const result = await api('remove',{id:source.id,digest:source.digest});
  closeSource(); $('results').replaceChildren(); $('results-title').textContent = 'Document removed from search';
  $('guidance').textContent = result.message; await refresh();
});
await action(async () => { await refresh(); document.body.dataset.ready = 'true'; });
