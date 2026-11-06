// Reading the literature on a question, kept honest. Records come from
// OpenAlex and Europe PMC exactly as published; duplicates across them are
// merged; each one is screened with a stated reason; evidence is an exact
// span of a record's own abstract, so every quotation can be checked against
// its source; claims gather evidence and say how consistent it is. Nothing
// here writes a finding that no excerpt supports.

export const OPENALEX = 'https://api.openalex.org/works';
export const EUROPEPMC = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search';
export const REASONS = ['Not about the question', 'Wrong population or setting', 'Not a study (opinion, news, editorial)', 'No outcome reported', 'Outside the years', 'Not in a language I read', 'Duplicate'];

// ------------------------------------------------------------ queries

export function openAlexUrl({ query, from, to, perPage = 50, page = 1, types }) {
  const filter = ['has_abstract:true', from && `from_publication_date:${from}-01-01`, to && `to_publication_date:${to}-12-31`, types?.length && `type:${types.join('|')}`].filter(Boolean).join(',');
  const select = 'id,doi,title,publication_year,abstract_inverted_index,cited_by_count,authorships,primary_location,type,language';
  return `${OPENALEX}?search=${encodeURIComponent(query)}&filter=${encodeURIComponent(filter)}&per-page=${perPage}&page=${page}&select=${select}`;
}
export function europePmcUrl({ query, from, to, pageSize = 50 }) {
  const years = from || to ? ` AND (PUB_YEAR:[${from || 1900} TO ${to || 2100}])` : '';
  return `${EUROPEPMC}?query=${encodeURIComponent(`(${query}) AND HAS_ABSTRACT:Y${years}`)}&format=json&resultType=core&pageSize=${pageSize}`;
}

// ------------------------------------------------------------ records

const clean = t => String(t ?? '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
export function abstractFromIndex(index) {
  if (!index) return '';
  const words = [];
  for (const [word, places] of Object.entries(index)) for (const at of places) words[at] = word;
  return words.filter(w => w != null).join(' ');
}
// Some indexed names carry an ORCID or are written "Surname, Given"; a reference needs "Given Surname".
export function personName(raw) {
  let n = clean(raw).replace(/;?\s*(id_orcid|orcid)?\s*\d{4}-\d{4}-\d{4}-\d{3}[\dX]/gi, '').replace(/[;,]\s*$/, '').trim();
  const m = n.match(/^([^,]+),\s*([^,]+)$/); if (m) n = `${m[2]} ${m[1]}`;
  return n;
}
const doiOf = d => d ? String(d).toLowerCase().replace(/^https?:\/\/(dx\.)?doi\.org\//, '').trim() || null : null;

export function fromOpenAlex(w) {
  const doi = doiOf(w.doi);
  return { id: 'oa:' + String(w.id).split('/').pop(), doi, title: clean(w.title), year: w.publication_year ?? null, venue: clean(w.primary_location?.source?.display_name),
    authors: (w.authorships ?? []).map(a => personName(a.author?.display_name)).filter(Boolean), cited: w.cited_by_count ?? 0, type: w.type ?? '', language: w.language ?? '',
    abstract: clean(abstractFromIndex(w.abstract_inverted_index)), sources: [{ name: 'OpenAlex', url: String(w.id) }], url: doi ? `https://doi.org/${doi}` : String(w.id) };
}
export function fromEuropePmc(r) {
  const doi = doiOf(r.doi), url = r.pmid ? `https://europepmc.org/article/MED/${r.pmid}` : `https://europepmc.org/article/${r.source}/${r.id}`;
  return { id: `epmc:${r.source}:${r.id}`, doi, title: clean(r.title).replace(/\.$/, ''), year: r.pubYear ? +r.pubYear : null, venue: clean(r.journalInfo?.journal?.title ?? r.bookOrReportDetails?.publisher),
    authors: (r.authorList?.author ?? []).map(a => personName(a.fullName ?? a.collectiveName)).filter(Boolean), cited: r.citedByCount ?? 0, type: (r.pubTypeList?.pubType ?? []).join('; '), language: r.language ?? '',
    abstract: clean(r.abstractText), sources: [{ name: 'Europe PMC', url }], url: doi ? `https://doi.org/${doi}` : url };
}

const titleKey = t => clean(t).toLowerCase().normalize('NFKD').replace(/[^a-z0-9 ]/g, ' ').split(/\s+/).filter(w => w.length > 2);
export function sameWork(a, b) {
  if (a.doi && b.doi) return a.doi === b.doi;
  if (a.year && b.year && Math.abs(a.year - b.year) > 1) return false;
  const x = new Set(titleKey(a.title)), y = new Set(titleKey(b.title)); if (x.size < 4 || y.size < 4) return false;
  let both = 0; for (const w of x) if (y.has(w)) both++;
  return both / (x.size + y.size - both) >= 0.85;
}
// Records merged across sources: the longer abstract is kept (it is quoted from), and both sources are named.
export function merge(records) {
  const out = [], dropped = [];
  for (const r of records) {
    const hit = out.find(o => sameWork(o, r));
    if (!hit) { out.push({ ...r, sources: [...r.sources] }); continue; }
    dropped.push({ id: r.id, into: hit.id });
    for (const s of r.sources) if (!hit.sources.some(t => t.name === s.name)) hit.sources.push(s);
    if ((r.abstract?.length ?? 0) > (hit.abstract?.length ?? 0)) { hit.abstract = r.abstract; hit.abstractFrom = r.sources[0].name; }
    hit.doi ??= r.doi; hit.cited = Math.max(hit.cited, r.cited); if (!hit.authors.length) hit.authors = r.authors; if (!hit.venue) hit.venue = r.venue;
  }
  return { records: out, duplicates: dropped };
}

// ------------------------------------------------------------ reading aids

// What a study says about itself, found in its own words. These are suggestions to confirm, never facts.
const DESIGNS = [['systematic review or meta-analysis', /\b(meta-analy|systematic review|scoping review)/i], ['randomised trial', /\brandomi[sz]ed\b|\bRCT\b/], ['quasi-experiment', /\b(quasi-experiment|difference-in-differences|natural experiment|pilot (program|programme|trial))/i],
  ['longitudinal or panel', /\b(longitudinal|panel data|cohort)\b/i], ['survey', /\b(survey|questionnaire|respondents)\b/i], ['interviews or qualitative', /\b(interview|qualitative|focus group|thematic analysis|ethnograph)/i], ['case study', /\bcase stud/i], ['review or commentary', /\b(literature review|we review|commentary|essay)\b/i]];
export function design(text) { return DESIGNS.find(([, re]) => re.test(text))?.[0] ?? ''; }
export function sampleSizes(text) {
  const out = [];
  for (const m of String(text).matchAll(/\b[nN]\s*=\s*(\d[\d,]*)/g)) out.push({ n: +m[1].replace(/,/g, ''), at: m.index, text: m[0] });
  for (const m of String(text).matchAll(/\b(\d[\d,]{1,})\s+(participants|employees|workers|respondents|firms|companies|organi[sz]ations|staff|nurses|physicians|teachers|students|patients|people|individuals|managers|businesses)\b/gi))
    out.push({ n: +m[1].replace(/,/g, ''), at: m.index, text: m[0] });
  return out.filter(x => x.n > 1 && x.n < 1e8).sort((a, b) => a.at - b.at);
}
// Terms of the question found in a text, as spans, for highlighting what a screener should look at.
export function termsIn(text, terms) {
  const spans = [], words = [...new Set(terms.map(t => t.trim().toLowerCase()).filter(t => t.length > 2))];
  const low = String(text).toLowerCase();
  for (const w of words) for (let at = low.indexOf(w); at >= 0; at = low.indexOf(w, at + w.length)) if (!at || !/[a-z0-9]/.test(low[at - 1])) spans.push([at, at + w.length]);
  spans.sort((a, b) => a[0] - b[0]);
  return spans.reduce((acc, s) => { const last = acc.at(-1); if (last && s[0] <= last[1]) last[1] = Math.max(last[1], s[1]); else acc.push([...s]); return acc; }, []);
}
export const questionTerms = q => String(q).toLowerCase().replace(/[^a-z0-9\- ]/g, ' ').split(/\s+/).filter(w => w.length > 3 && !STOP.has(w));
const STOP = new Set(['what', 'when', 'does', 'with', 'from', 'that', 'this', 'have', 'into', 'their', 'there', 'which', 'about', 'between', 'among', 'effect', 'effects', 'happens', 'change', 'changes']);

// ------------------------------------------------------------ evidence

// An excerpt is the record's own words between two offsets. A check recomputes the text from the abstract.
export function excerpt(record, start, end, fields = {}) {
  if (!(Number.isSafeInteger(start) && Number.isSafeInteger(end) && start >= 0 && end > start && end <= record.abstract.length)) throw Error('That passage is not inside this abstract.');
  // Widen to whole words so a quotation never starts or ends mid-word.
  while (start > 0 && /\w/.test(record.abstract[start - 1])) start--;
  while (end < record.abstract.length && /\w/.test(record.abstract[end])) end++;
  return { id: `${record.id}@${start}-${end}`, record: record.id, start, end, text: record.abstract.slice(start, end).trim(), claim: fields.claim ?? null, direction: fields.direction ?? 'supports', note: fields.note ?? '' };
}
export function verify(records, ex) {
  if (!ex || typeof ex.text !== 'string' || !ex.text.trim() || !Number.isSafeInteger(ex.start) || !Number.isSafeInteger(ex.end) || ex.start < 0 || ex.end <= ex.start) return false;
  const r = records.find(x => x.id === ex.record);
  return typeof r?.abstract === 'string' && ex.end <= r.abstract.length && r.abstract.slice(ex.start, ex.end).trim() === ex.text;
}

// Where a claim stands, from the excerpts filed under it and the included studies they come from.
export function standing(claim, excerpts, records, decisions) {
  const filed = excerpts.filter(e => e.claim === claim.id && decisions[e.record]?.status === 'include');
  const mine = filed.filter(e => verify(records, e)), invalid = filed.length - mine.length;
  const studies = [...new Set(mine.map(e => e.record))].map(id => records.find(r => r.id === id)).filter(Boolean);
  const by = { supports: new Set(), against: new Set(), mixed: new Set() };
  for (const e of mine) by[e.direction]?.add(e.record);
  const n = studies.length, s = by.supports.size, a = by.against.size, m = by.mixed.size;
  const years = studies.map(r => r.year).filter(Boolean), people = studies.reduce((t, r) => t + (decisions[r.id]?.n || 0), 0);
  const designs = [...new Set(studies.map(r => decisions[r.id]?.design || '').filter(Boolean))];
  let words;
  if (!n) words = 'No included evidence yet';
  else if (n === 1) words = `One study ${s ? 'supports this' : a ? 'goes against this' : 'is mixed on this'}`;
  else if (s && !a && !m) words = `${n} studies all support this`;
  else if (a && !s && !m) words = `${n} studies all go against this`;
  else if (s >= 3 * (a + m)) words = `Mostly supported: ${s} for, ${a} against, ${m} mixed`;
  else if (a >= 3 * (s + m)) words = `Mostly not supported: ${s} for, ${a} against, ${m} mixed`;
  else words = `Divided: ${s} for, ${a} against, ${m} mixed`;
  return { studies: n, invalid, supports: s, against: a, mixed: m, excerpts: mine.length, years: years.length ? [Math.min(...years), Math.max(...years)] : null, people, designs, words };
}

// The counts a reader of a review expects, from the work actually done.
export function flow(state) {
  const all = state.records, decided = Object.values(state.decisions);
  const excluded = {}; for (const d of decided) if (d.status === 'exclude') excluded[d.reason || 'No reason given'] = (excluded[d.reason || 'No reason given'] ?? 0) + 1;
  return { identified: state.identified ?? all.length, duplicates: state.duplicates?.length ?? 0, screened: all.length, unscreened: all.filter(r => !state.decisions[r.id]).length,
    maybe: decided.filter(d => d.status === 'maybe').length, excluded, excludedTotal: decided.filter(d => d.status === 'exclude').length, included: decided.filter(d => d.status === 'include').length };
}

// ------------------------------------------------------------ writing it up

export function cite(r) {
  const a = r.authors, who = !a.length ? (r.venue || 'Anonymous') : a.length === 1 ? surname(a[0]) : a.length === 2 ? `${surname(a[0])} & ${surname(a[1])}` : `${surname(a[0])} et al.`;
  return `${who}, ${r.year ?? 'n.d.'}`;
}
const surname = n => String(n).trim().split(/\s+/).at(-1);
export function reference(r) {
  const names = r.authors.slice(0, 6).map(n => { const parts = n.trim().split(/\s+/); const last = parts.pop(); return `${last}, ${parts.map(p => p[0] + '.').join(' ')}`.trim().replace(/,\s*$/, ''); });
  const who = names.length ? names.join(', ') + (r.authors.length > 6 ? ', et al.' : '') : (r.venue || 'Anonymous');
  return `${who} (${r.year ?? 'n.d.'}). ${r.title}.${r.venue ? ` ${r.venue}.` : ''} ${r.url}`;
}

export function report(state) {
  const { question, claims, excerpts, records, decisions } = state, f = flow(state), used = new Map();
  const lines = [`# ${question}`, '', `Records found: ${f.identified} (OpenAlex and Europe PMC). Duplicates merged: ${f.duplicates}. Screened: ${f.screened - f.unscreened}. Included: ${f.included}.`, ''];
  const invalid = excerpts.filter(e => !verify(records, e)).length;
  if (invalid) lines.push(`${invalid} filed quotation${invalid === 1 ? ' no longer matches its source and is' : 's no longer match their sources and are'} omitted from this write-up. Review the affected evidence.`, '');
  for (const c of claims) {
    const st = standing(c, excerpts, records, decisions); if (!st.studies) continue;
    lines.push(`## ${c.text}`, '', `${st.words}${st.years ? ` (${st.years[0] === st.years[1] ? st.years[0] : st.years.join('–')})` : ''}${st.people ? `; about ${st.people.toLocaleString()} people or organisations in the studies that report it` : ''}.`, '');
    for (const e of excerpts.filter(x => x.claim === c.id && decisions[x.record]?.status === 'include' && verify(records, x))) {
      const r = records.find(x => x.id === e.record); used.set(r.id, r);
      lines.push(`> "${e.text}" (${cite(r)}${e.direction !== 'supports' ? `; ${e.direction === 'against' ? 'against' : 'mixed'}` : ''})`, ...(e.note ? ['', e.note] : []), '');
    }
  }
  lines.push('## References', '', ...[...used.values()].sort((a, b) => cite(a).localeCompare(cite(b))).map(r => `- ${reference(r)}`), '');
  return lines.join('\n');
}

const bibKey = r => (surname(r.authors[0] ?? 'anon') + (r.year ?? '') + (titleKey(r.title)[0] ?? '')).toLowerCase().replace(/[^a-z0-9]/g, '');
const bibEsc = t => String(t ?? '').replace(/[{}\\]/g, '');
export function bibtex(records) {
  return records.map(r => `@article{${bibKey(r)},\n  title = {${bibEsc(r.title)}},\n  author = {${r.authors.map(bibEsc).join(' and ')}},\n  year = {${r.year ?? ''}},${r.venue ? `\n  journal = {${bibEsc(r.venue)}},` : ''}${r.doi ? `\n  doi = {${r.doi}},` : ''}\n  url = {${r.url}}\n}`).join('\n\n') + '\n';
}
export function ris(records) {
  return records.map(r => ['TY  - JOUR', `TI  - ${r.title}`, ...r.authors.map(a => `AU  - ${a}`), r.year && `PY  - ${r.year}`, r.venue && `JO  - ${r.venue}`, r.doi && `DO  - ${r.doi}`, `UR  - ${r.url}`, r.abstract && `AB  - ${r.abstract}`, 'ER  - '].filter(Boolean).join('\n')).join('\n\n') + '\n';
}
export function evidenceCsv(state) {
  const q = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const head = 'claim,direction,quotation,quotation_matches_source,citation,year,design,sample,doi_or_url,note';
  return head + '\n' + state.excerpts.map(e => { const r = state.records.find(x => x.id === e.record), c = state.claims.find(x => x.id === e.claim), d = state.decisions[e.record] ?? {};
    return [q(c?.text ?? 'not filed'), e.direction, q(e.text), verify(state.records, e) ? 'yes' : 'no', q(r ? cite(r) : 'Source removed'), r?.year ?? '', q(d.design ?? ''), d.n ?? '', q(r?.url), q(e.note)].join(','); }).join('\n') + '\n';
}

// Validate before replacing the open review. Source mismatch is retained for
// correction; malformed editor structures cannot become the working project.
export function prepareReviewProject(value) {
  const plain = x => x !== null && typeof x === 'object' && !Array.isArray(x);
  const text = x => typeof x === 'string';
  const id = x => text(x) && x.length > 0 && !Object.hasOwn(Object.prototype, x);
  const check = (ok, message) => { if (!ok) throw Error(message); };
  const address = x => { if (x === '') return true; try { const u = new URL(x); return text(x) && ['https:', 'http:'].includes(u.protocol) && !u.username && !u.password; } catch { return false; } };
  check(plain(value) && text(value.question) && value.question.trim() && Array.isArray(value.records) && Array.isArray(value.excerpts), 'This is not a research desk project.');
  check(value.records.length <= 10000 && value.excerpts.length <= 100000, 'This review exceeds the supported record or quotation count.');
  const project = structuredClone({ claims: [], decisions: {}, ...value }), records = new Set(), claims = new Set(), excerpts = new Set();
  check(Array.isArray(project.claims) && plain(project.decisions), 'Invalid claims or screening decisions.');
  for (const r of project.records) {
    check(plain(r) && id(r.id) && !records.has(r.id) && text(r.title) && text(r.abstract) && Array.isArray(r.authors) && r.authors.every(text)
      && Array.isArray(r.sources) && r.sources.every(s => plain(s) && text(s.name) && address(s.url)) && address(r.url), 'Invalid or duplicate research record.');
    check((r.year == null || Number.isSafeInteger(r.year)) && (r.cited == null || Number.isFinite(r.cited) && r.cited >= 0), 'Invalid research year or citation count.');
    records.add(r.id);
  }
  for (const c of project.claims) {
    check(plain(c) && id(c.id) && !claims.has(c.id) && text(c.text), 'Invalid or duplicate research claim.'); claims.add(c.id);
  }
  for (const e of project.excerpts) {
    check(plain(e) && id(e.id) && id(e.record) && (e.claim === null || id(e.claim)) && text(e.text) && Number.isSafeInteger(e.start) && Number.isSafeInteger(e.end) && e.start >= 0 && e.end > e.start, 'Invalid quotation coordinates or identity.');
    e.direction ??= 'supports'; e.note ??= '';
    check(['supports', 'against', 'mixed'].includes(e.direction) && text(e.note), 'Invalid quotation direction or note.');
    const key = JSON.stringify([e.id, e.claim]); check(!excerpts.has(key), 'Duplicate filed quotation.'); excerpts.add(key);
  }
  for (const [key, d] of Object.entries(project.decisions)) {
    check(id(key) && plain(d) && (d.status === undefined || ['include', 'exclude', 'maybe'].includes(d.status))
      && (d.reason === undefined || text(d.reason)) && (d.design === undefined || text(d.design))
      && (d.n == null || Number.isFinite(d.n) && d.n >= 0), 'Invalid screening decision.');
  }
  check(project.identified == null || Number.isSafeInteger(project.identified) && project.identified >= 0, 'Invalid identified-record count.');
  check(project.duplicates === undefined || Array.isArray(project.duplicates) && project.duplicates.every(d => plain(d) && id(d.id) && id(d.into)), 'Invalid duplicate-record history.');
  check(project.retrieved === undefined || Array.isArray(project.retrieved), 'Invalid search history.');
  check(project.search === undefined || plain(project.search) && (project.search.query === undefined || text(project.search.query)), 'Invalid saved search.');
  return project;
}
