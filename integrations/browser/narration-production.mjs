// Production decisions and take identities. Native workspace storage owns persistence.
export const productionLimits = Object.freeze({items:64, segments:128, languages:8, script:12000, takes:8});
const fail = (ok, message) => { if (!ok) throw Error(message); };
const clone = value => structuredClone(value);
const idOK = id => typeof id === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,95}$/.test(id);
const hashOK = hash => typeof hash === 'string' && /^[a-f0-9]{64}$/.test(hash);
const sorted = async value => value instanceof ArrayBuffer ? {$audio:await productionDigest(value)}
  : Array.isArray(value) ? Promise.all(value.map(sorted)) : value && typeof value === 'object'
  ? Object.fromEntries(await Promise.all(Object.keys(value).sort().map(async key => [key, await sorted(value[key])]))) : value;
export async function productionDigest(value) {
  const bytes = value instanceof ArrayBuffer ? value : new TextEncoder().encode(JSON.stringify(await sorted(value)));
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(b => b.toString(16).padStart(2,'0')).join('');
}
const defaultSettings = Object.freeze({voice:'system', rate:180, gapMs:300, gain:0.8});
function settings(value, partial = false) {
  fail(value && typeof value === 'object' && !Array.isArray(value), 'Choose narration settings.');
  fail(Object.keys(value).every(key => Object.hasOwn(defaultSettings,key)), 'Unknown narration setting.');
  const v = partial ? {...defaultSettings,...value} : value;
  fail(typeof v.voice === 'string' && /^[a-zA-Z0-9 _.-]{1,80}$/.test(v.voice), 'Choose an available voice.');
  fail(Number.isInteger(v.rate) && v.rate >= 80 && v.rate <= 300, 'Speaking rate must be 80–300 words per minute.');
  fail(Number.isInteger(v.gapMs) && v.gapMs >= 0 && v.gapMs <= 10000, 'Clip gaps must be 0–10 seconds.');
  fail(Number.isFinite(v.gain) && v.gain >= 0 && v.gain <= 1, 'Clip gain must be between zero and one.');
}
function source(value) {
  fail(value && hashOK(value.sha256), 'Retain the source digest before creating a production.');
  let url; try { url = new URL(value.href); } catch {}
  fail(url && ['http:','https:'].includes(url.protocol) && !url.username && !url.password, 'Use an ordinary source address.');
}
export function validateProduction(p) {
  fail(p?.schema === 'aurekai.narration-production.v1' && idOK(p.id), 'Invalid production identity.');
  fail(typeof p.title === 'string' && p.title.trim() && p.title.length <= 200, 'Name this production.');
  fail(Number.isSafeInteger(p.version) && p.version > 0, 'Invalid production revision.');
  settings(p.defaults);
  fail(Array.isArray(p.languages) && p.languages.length > 0 && p.languages.length <= productionLimits.languages, 'Choose one to eight language editions.');
  const languages = new Set();
  for (const language of p.languages) {
    fail(idOK(language.id) && !languages.has(language.id), 'Language editions must have distinct identities.');
    languages.add(language.id); settings(language.overrides, true);
  }
  fail(Array.isArray(p.items) && p.items.length > 0 && p.items.length <= productionLimits.items, 'Choose one to 64 source items.');
  const items = new Set(), segments = new Set();
  for (const item of p.items) {
    fail(idOK(item.id) && !items.has(item.id), 'Source items must have distinct identities.');
    items.add(item.id); source(item.source);
    fail(typeof item.title === 'string' && item.title.length <= 300 && Array.isArray(item.segments) && item.segments.length, 'A source item needs titled passages.');
    for (const segment of item.segments) {
      fail(idOK(segment.id) && !segments.has(segment.id), 'Passages must have distinct identities.');
      segments.add(segment.id);
      fail(typeof segment.text === 'string' && segment.text.trim() && segment.text.length <= productionLimits.script, 'Each source passage needs bounded text.');
    }
  }
  fail(segments.size <= productionLimits.segments, 'This production exceeds 128 passages.');
  fail(Array.isArray(p.cells) && p.cells.length === segments.size * languages.size, 'Every passage needs one cell per language edition.');
  const cells = new Set();let audioBytes=0;
  for (const cell of p.cells) {
    fail(cell.id === `${cell.segment}@${cell.language}` && segments.has(cell.segment) && languages.has(cell.language) && !cells.has(cell.id), 'Invalid or repeated rendition cell.');
    cells.add(cell.id); settings(cell.overrides,true);
    fail(typeof cell.script === 'string' && cell.script.length <= productionLimits.script && typeof cell.locked === 'boolean', 'Invalid script or lock.');
    fail(cell.reviewed === null || hashOK(cell.reviewed), 'Invalid script review.');
    fail(Array.isArray(cell.takes) && cell.takes.length <= productionLimits.takes, 'Keep at most eight takes per passage.');
    const takes = new Set();
    for (const take of cell.takes) {
      fail(idOK(take.id) && !takes.has(take.id) && hashOK(take.fingerprint) && hashOK(take.sha256), 'Invalid take identity.');
      takes.add(take.id);
      fail(take.bytes instanceof ArrayBuffer && take.bytes.byteLength > 44 && take.bytes.byteLength <= 24*1024*1024, 'Retain an actual bounded WAV for each take.');
      audioBytes+=take.bytes.byteLength;fail(audioBytes<=256*1024*1024,'This production exceeds its 256 MB audio working set.');
      fail(Number.isInteger(take.frames) && take.frames > 0 && Number.isInteger(take.sampleRate) && take.sampleRate >= 8000 && take.sampleRate <= 96000, 'Retain measured audio dimensions.');
    }
    fail(cell.activeTake === null || takes.has(cell.activeTake), 'The selected take is missing.');
  }
  return p;
}
export function createProduction({id,title,items,languages=[{id:'en',label:'English'}],defaults={}}) {
  const p = {schema:'aurekai.narration-production.v1',id,title,version:1,defaults:{...defaultSettings,...defaults},
    items:clone(items),languages:languages.map(l=>({...l,overrides:clone(l.overrides??{})})),cells:[]};
  for (const item of p.items) for (const segment of item.segments) for (const language of p.languages)
    p.cells.push({id:`${segment.id}@${language.id}`,segment:segment.id,language:language.id,
      script:language.id === (item.language??'en') ? segment.text : '',overrides:{},locked:false,reviewed:null,takes:[],activeTake:null});
  return validateProduction(p);
}
export function effectiveSettings(p, cell) {
  const language = p.languages.find(l=>l.id===cell.language);
  fail(language, 'The language edition is missing.');
  return {...p.defaults,...language.overrides,...cell.overrides};
}
export async function cellFingerprint(p, cell) {
  const item = p.items.find(i=>i.segments.some(s=>s.id===cell.segment));
  fail(item, 'The source passage is missing.');
  const s = effectiveSettings(p,cell);
  return productionDigest({source:item.source,segment:cell.segment,language:cell.language,script:cell.script,voice:s.voice,rate:s.rate});
}
export async function productionStatus(p) {
  validateProduction(p);
  return Promise.all(p.cells.map(async cell=>{
    const fingerprint = await cellFingerprint(p,cell),take=cell.takes.find(t=>t.id===cell.activeTake && t.fingerprint===fingerprint);
    return {id:cell.id,fingerprint,settings:effectiveSettings(p,cell),take:take?.id??null,
      state:!cell.script.trim()?'needs-script':cell.reviewed!==fingerprint?'needs-review':take?'ready':'needs-audio'};
  }));
}
// A preview is tied to the entire reviewed input; changes include the exact affected cells.
export async function previewProductionEdit(p, edit) {
  validateProduction(p);
  const next = clone(p), before = await productionStatus(p);
  fail(edit && ['defaults','language','cells'].includes(edit.scope), 'Choose the edit scope.');
  fail(edit.set && typeof edit.set==='object' && !Array.isArray(edit.set), 'Choose fields to change.');
  const allowed = ['voice','rate','gapMs','gain',...(edit.scope==='cells'?['script','locked']:[])];
  const reset = edit.reset??[];
  fail((Object.keys(edit.set).length || reset.length) && Object.keys(edit.set).every(k=>allowed.includes(k)), 'Unsupported production edit.');
  fail(Array.isArray(reset) && reset.every(k=>Object.hasOwn(defaultSettings,k)) && !reset.some(k=>Object.hasOwn(edit.set,k)), 'Reset only inherited settings.');
  if (edit.scope === 'defaults') {
    fail(!reset.length, 'Production defaults cannot inherit.');
    Object.assign(next.defaults,edit.set);
  } else if (edit.scope === 'language') {
    const language=next.languages.find(l=>l.id===edit.language);
    fail(language,'Choose an existing language edition.');
    Object.assign(language.overrides,edit.set); for(const k of reset)delete language.overrides[k];
  } else {
    fail(Array.isArray(edit.ids) && edit.ids.length && new Set(edit.ids).size===edit.ids.length, 'Choose distinct rendition cells.');
    for(const id of edit.ids) {
      const cell=next.cells.find(c=>c.id===id);
      fail(cell,'A selected rendition no longer exists.');
      fail(!cell.locked || Object.keys(edit.set).length===1 && edit.set.locked===false && !reset.length,'Unlock the selected rendition before editing it.');
      for(const [key,value]of Object.entries(edit.set)) {
        if(key==='script'||key==='locked')cell[key]=value;else cell.overrides[key]=value;
      }
      for(const k of reset)delete cell.overrides[k];
    }
  }
  // Locked cells pin their effective values when defaults change.
  if(edit.scope!=='cells')for(const cell of next.cells)if(cell.locked)cell.overrides=effectiveSettings(p,p.cells.find(c=>c.id===cell.id));
  validateProduction(next);
  const after=await productionStatus(next),affected=[];
  for(let i=0;i<before.length;i++) {
    const a=before[i],b=after[i],old=p.cells[i],cell=next.cells[i];
    if(JSON.stringify(a.settings)!==JSON.stringify(b.settings)||a.fingerprint!==b.fingerprint||old.locked!==cell.locked||JSON.stringify(old.overrides)!==JSON.stringify(cell.overrides))
      affected.push({id:a.id,before:a.settings,after:b.settings,regenerate:a.fingerprint!==b.fingerprint,locked:cell.locked});
  }
  next.version++;
  const preimage=await productionDigest(p),digest=await productionDigest({preimage,edit,next});
  return {preimage,digest,edit:clone(edit),affected,next};
}
export async function applyProductionEdit(p, preview) {
  fail(await productionDigest(p)===preview.preimage,'This production changed. Review the edit again.');
  const checked=await previewProductionEdit(p,preview.edit);
  fail(checked.digest===preview.digest,'The reviewed edit changed.');
  return checked.next;
}
export async function reviewScripts(p, ids) {
  validateProduction(p);const next=clone(p);
  fail(Array.isArray(ids)&&ids.length&&new Set(ids).size===ids.length,'Choose distinct scripts to review.');
  for(const id of ids) {
    const cell=next.cells.find(c=>c.id===id);
    fail(cell?.script.trim(),'A selected script is empty or missing.');
    cell.reviewed=await cellFingerprint(next,cell);
  }
  next.version++;return next;
}
export async function renderRequests(p, ids) {
  const rows=await productionStatus(p);
  fail(Array.isArray(ids)&&ids.length&&new Set(ids).size===ids.length,'Choose distinct renditions to render.');
  return ids.map(id=>{
    const row=rows.find(r=>r.id===id),cell=p.cells.find(c=>c.id===id);
    fail(row && ['needs-audio','ready'].includes(row.state),'Review every selected script before rendering.');
    return {production:p.id,cell:id,fingerprint:row.fingerprint,script:cell.script,language:cell.language,
      voice:row.settings.voice,rate:row.settings.rate,reusableTake:row.take};
  });
}
// Decode is supplied by the existing audio owner. No estimated duration becomes audio.
export async function acceptTake(p, request, {id,bytes,decode}) {
  validateProduction(p);
  const next=clone(p),cell=next.cells.find(c=>c.id===request.cell);
  fail(request.production===p.id && cell && request.fingerprint===await cellFingerprint(p,cell),'The script or settings changed while this take was rendering.');
  fail(cell.reviewed===request.fingerprint,'Review this script before accepting its take.');
  fail(idOK(id)&&!cell.takes.some(t=>t.id===id),'Use a distinct take identity.');
  fail(cell.takes.length<productionLimits.takes,'Remove an unwanted take before recording another.');
  fail(bytes instanceof ArrayBuffer && bytes.byteLength<=24*1024*1024,'Provide a bounded audio body.');
  const measured=decode(bytes),sha256=await productionDigest(bytes);
  cell.takes.push({id,fingerprint:request.fingerprint,sha256,bytes:bytes.slice(0),frames:measured.frames,sampleRate:measured.sample_rate});
  cell.activeTake=id;next.version++;return validateProduction(next);
}
export function selectTake(p,cellId,takeId) {
  const next=clone(validateProduction(p)),cell=next.cells.find(c=>c.id===cellId);
  fail(cell?.takes.some(t=>t.id===takeId),'Choose an existing take.');
  cell.activeTake=takeId;next.version++;return next;
}
export async function mixerSession(p, language, {allowPartial=false,maxSeconds=600,maxTracks=64}={}) {
  const states=await productionStatus(p);fail(p.languages.some(l=>l.id===language),'Choose an existing language edition.');
  const cells=p.items.flatMap(item=>item.segments.map(segment=>p.cells.find(c=>c.segment===segment.id&&c.language===language)));
  const missing=cells.filter(c=>states.find(s=>s.id===c.id).state!=='ready').map(c=>c.id);
  fail(allowPartial || !missing.length,'This edition still has unfinished passages.');
  const files=[],clips=[],chapters=[];let at=0,rate=null;
  for(const cell of cells) {
    const state=states.find(s=>s.id===cell.id);if(state.state!=='ready')continue;
    const take=cell.takes.find(t=>t.id===state.take),item=p.items.find(i=>i.segments.some(s=>s.id===cell.segment));
    fail(await productionDigest(take.bytes)===take.sha256,'A retained take no longer matches its digest.');
    if(rate===null)rate=take.sampleRate;fail(take.sampleRate===rate,'Resample these takes to one rate before arranging them together.');
    files.push({name:`${cell.segment}-${language}.wav`,bytes:take.bytes.slice(0),sha256:take.sha256});
    clips.push({at,from:0,frames:take.frames,gain:state.settings.gain,muted:false});
    chapters.push({cell:cell.id,take:take.id,source:clone(item.source),title:item.title,at,frames:take.frames});
    at+=take.frames+Math.round(state.settings.gapMs*rate/1000);
  }
  fail(files.length>0&&files.length<=maxTracks,'Choose an edition with available takes within the mixer track limit.');
  const frames=Math.max(...clips.map(c=>c.at+c.frames));
  fail(frames<=rate*maxSeconds,'This edition exceeds the mixer duration limit.');
  return {files,clips,title:`${p.title} · ${language}`,permission:'unreviewed',
    production:{id:p.id,version:p.version,language,chapters,omitted:missing,partial:missing.length>0},
    measured:{sampleRate:rate,frames,seconds:frames/rate}};
}
