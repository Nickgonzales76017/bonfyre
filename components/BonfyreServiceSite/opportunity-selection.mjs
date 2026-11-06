// Exact, reviewed requirements from retained source text. No source inference or storage owner.
const PRIORITIES = new Set(['must', 'should', 'could']);
const boundary = (text, at) => at === 0 || at === text.length ||
  !(text.charCodeAt(at - 1) >= 0xd800 && text.charCodeAt(at - 1) <= 0xdbff &&
    text.charCodeAt(at) >= 0xdc00 && text.charCodeAt(at) <= 0xdfff);

export function selectedRequirement(texts, source, start, end) {
  if (!texts || typeof source !== 'string' || !Object.hasOwn(texts, source) || typeof texts[source] !== 'string')
    throw Error('Choose a retained source document.');
  const text = texts[source];
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 ||
      end <= start || end > text.length || end - start > 4000 ||
      !boundary(text, start) || !boundary(text, end))
    throw Error('Select 1–4,000 complete characters inside the retained document.');
  const quote = text.slice(start, end);
  if (!quote.trim()) throw Error('Select a passage containing text.');
  return {source, start, end, text: quote, priority: '', theme: 'Other', note: ''};
}

export function applySelectedRequirements(requirements, reviewed, texts) {
  if (!Array.isArray(requirements) || requirements.length > 2000 ||
      !Array.isArray(reviewed) || !reviewed.length || reviewed.length > 100)
    throw Error('Review 1–100 passages within the 2,000-requirement limit.');
  const ids = new Set(), coordinates = new Set();
  for (const row of requirements) {
    if (!row || typeof row.id !== 'string' || ids.has(row.id)) throw Error('Existing requirements need distinct identities.');
    ids.add(row.id); coordinates.add(JSON.stringify([row.source,row.start,row.end]));
  }
  const staged = new Set(), additions = []; let duplicates = 0;
  for (const row of reviewed) {
    if (!row || typeof row !== 'object') throw Error('Review each selected passage.');
    const exact = selectedRequirement(texts,row.source,row.start,row.end);
    if (row.text !== exact.text) throw Error('A selected passage changed. Review the current source again.');
    if (!PRIORITIES.has(row.priority)) throw Error('Choose a priority for every selected passage.');
    if (typeof row.theme !== 'string' || !row.theme.trim() || row.theme.length > 100 ||
        /[\u0000-\u001f\u007f]/.test(row.theme) || Object.hasOwn(Object.prototype,row.theme.trim()))
      throw Error('Use a printable theme of at most 100 characters.');
    if (typeof row.note !== 'string' || row.note.length > 300) throw Error('Keep passage notes within 300 characters.');
    const coordinate = JSON.stringify([row.source,row.start,row.end]);
    if (staged.has(coordinate)) throw Error('A selected passage occurs twice in this review.');
    staged.add(coordinate);
    if (coordinates.has(coordinate)) { duplicates++; continue; }
    const id = row.source+'#'+row.start+':'+row.end;
    if (ids.has(id)) throw Error('A selected passage conflicts with an existing identity.');
    ids.add(id);
    additions.push({...exact,id,priority:row.priority,theme:row.theme.trim(),note:row.note,coverage:'',origin:'reviewed-source-selection'});
  }
  if (requirements.length + additions.length > 2000) throw Error('Keep at most 2,000 requirements in one review.');
  return {requirements:[...requirements.map(row=>structuredClone(row)),...additions],added:additions.map(row=>row.id),duplicates};
}
