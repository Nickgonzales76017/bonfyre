import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { abstractFromIndex, fromOpenAlex, fromEuropePmc, merge, sameWork, excerpt, verify, standing, flow, report, bibtex, ris, evidenceCsv, sampleSizes, design, termsIn, personName, openAlexUrl, europePmcUrl } from '../synthesis.mjs';
import { synthesisFiles } from '../synthesis-material.mjs';

const sample = join(import.meta.dir, '../../../packs/aurekai-work-samples/site/synthesis-four-day-week-20261019.json');

test('an OpenAlex inverted index reads back as the abstract', () => {
  expect(abstractFromIndex({ four: [0], day: [1], week: [2, 5], a: [4], each: [3] })).toBe('four day week each a week');
  const r = fromOpenAlex({ id: 'https://openalex.org/W1', doi: 'https://doi.org/10.1/ABC', title: 'T', publication_year: 2024, abstract_inverted_index: { Hello: [0], world: [1] }, authorships: [{ author: { display_name: 'Ada Lovelace' } }], cited_by_count: 3 });
  expect(r).toMatchObject({ id: 'oa:W1', doi: '10.1/abc', abstract: 'Hello world', authors: ['Ada Lovelace'], url: 'https://doi.org/10.1/abc' });
});

test('the same work from both indexes is merged, keeping the longer abstract and both sources', () => {
  const a = fromOpenAlex({ id: 'https://openalex.org/W2', doi: 'https://doi.org/10.5/x', title: 'Four-day week trial outcomes in UK firms', publication_year: 2023, abstract_inverted_index: { Short: [0] }, authorships: [] });
  const b = fromEuropePmc({ source: 'MED', id: '9', pmid: '9', doi: '10.5/X', title: 'Four-day week trial outcomes in UK firms.', pubYear: '2023', abstractText: 'A <b>much</b> longer abstract.', authorList: { author: [{ fullName: 'Smith J' }] } });
  const { records, duplicates } = merge([a, b]);
  expect(records).toHaveLength(1); expect(duplicates).toHaveLength(1);
  expect(records[0].abstract).toBe('A much longer abstract.'); expect(records[0].sources.map(s => s.name)).toEqual(['OpenAlex', 'Europe PMC']);
  expect(sameWork({ title: 'Reduced hours and burnout among hospital nurses in Spain', year: 2020 }, { title: 'Reduced hours and burnout among hospital nurses in Spain', year: 2024 })).toBe(false);
});

test('a quotation is the abstract\'s own words, widened to whole words, and a changed source is caught', () => {
  const r = { id: 'r1', abstract: 'Staff reported less burnout and fewer sick days after the trial.' };
  const e = excerpt(r, 15, 34);
  expect(e.text).toBe('less burnout and fewer'); expect(verify([r], e)).toBe(true);
  expect(verify([{ ...r, abstract: r.abstract.replace('less', 'more') }], e)).toBe(false);
  expect(() => excerpt(r, 50, 400)).toThrow();
});

test('a claim\'s standing counts included studies only, by direction', () => {
  const records = ['a', 'b', 'c', 'd'].map((id, i) => ({ id, year: 2020 + i, abstract: 'x'.repeat(50), authors: [], title: id }));
  const ex = (id, direction) => ({ id: id + direction, record: id, claim: 'c', direction, text: 'x', start: 0, end: 1 });
  const decisions = { a: { status: 'include', n: 40 }, b: { status: 'include', n: 10 }, c: { status: 'include' }, d: { status: 'exclude', reason: 'Duplicate' } };
  const st = standing({ id: 'c' }, [ex('a', 'supports'), ex('b', 'supports'), ex('c', 'against'), ex('d', 'supports')], records, decisions);
  expect(st).toMatchObject({ studies: 3, supports: 2, against: 1, people: 50, years: [2020, 2022] });
  expect(st.words).toBe('Divided: 2 for, 1 against, 0 mixed');
});

test('reading aids find what an abstract says about itself', () => {
  expect(sampleSizes('We surveyed 1,204 employees across 61 companies (n = 3,018).').map(s => s.n)).toEqual([1204, 61, 3018]);
  expect(design('A randomised controlled trial of shorter weeks')).toBe('randomised trial');
  expect(design('Semi-structured interviews with managers')).toBe('interviews or qualitative');
  expect(termsIn('The four-day workweek and the four-day week', ['week', 'four-day'])).toEqual([[4, 12], [30, 38], [39, 43]]);
  expect(personName('Shepherd, William; id_orcid 0000-0001-8470-6251')).toBe('William Shepherd');
});

test('search addresses ask for abstracts and the years', () => {
  expect(decodeURIComponent(openAlexUrl({ query: 'four day week', from: 2015, to: 2020 }))).toContain('has_abstract:true,from_publication_date:2015-01-01,to_publication_date:2020-12-31');
  expect(decodeURIComponent(europePmcUrl({ query: 'four day week', from: 2015 }))).toContain('HAS_ABSTRACT:Y AND (PUB_YEAR:[2015 TO 2100])');
});

test('the retained sample opens with verified quotations and writes up only what is filed', () => {
  const pkg = synthesisFiles(sample), prefix = pkg.asset_prefix;
  const project = JSON.parse(pkg.files[prefix + 'sample-project.json']);
  expect(project.records.length).toBeGreaterThan(50);
  expect(project.excerpts.every(e => verify(project.records, e))).toBe(true);
  const f = flow(project); expect(f.included).toBe(4); expect(f.excluded['Not about the question']).toBe(1);
  const md = report(project);
  for (const e of project.excerpts) expect(md).toContain(`"${e.text}"`);
  expect(md).not.toContain('## Productivity holds or rises');
  const incl = project.records.filter(r => project.decisions[r.id]?.status === 'include');
  expect(bibtex(incl).match(/@article/g)).toHaveLength(4); expect(ris(incl).match(/^TY  - JOUR/gm)).toHaveLength(4);
  expect(evidenceCsv(project).split('\n')[1]).toContain(',yes,');
  expect(readFileSync(sample, 'utf8')).toContain('"sha256"');
});


test('changed evidence cannot determine claim standing or enter the write-up, but remains in the evidence export', () => {
  const record=(id,abstract)=>({id,abstract,authors:['A Reader'],title:'Study '+id,year:2024,url:'https://example.test/'+id,sources:[]});
  const a=record('a','The original supporting passage.'),b=record('b','The independent contrary passage.');
  const project={question:'What changed?',records:[a,b],claims:[{id:'c',text:'A proposed claim'}],decisions:{a:{status:'include',n:100},b:{status:'include',n:20}},excerpts:[excerpt(a,0,a.abstract.length,{claim:'c'}),excerpt(b,0,b.abstract.length,{claim:'c',direction:'against'})]};
  const oldText=project.excerpts[0].text;project.records[0]={...a,abstract:'Corrected source text.'};const frozen=structuredClone(project);
  expect(standing(project.claims[0],project.excerpts,project.records,project.decisions)).toMatchObject({studies:1,supports:0,against:1,people:20,invalid:1,excerpts:1});
  const md=report(project);expect(md).not.toContain(oldText);expect(md).not.toContain('Study a');expect(md).toContain(project.excerpts[1].text);expect(md).toContain('1 filed quotation no longer matches');
  const csv=evidenceCsv(project);expect(csv).toContain(oldText);expect(csv).toContain(',no,');expect(project).toEqual(frozen);
});
test('removed sources stay reviewable without contributing citations or causing export failures',()=>{
 const state={question:'Missing source',records:[],claims:[{id:'c',text:'Claim'}],decisions:{missing:{status:'include'}},excerpts:[{id:'e',record:'missing',claim:'c',start:0,end:7,text:'Missing',direction:'supports'}]};
 expect(standing(state.claims[0],state.excerpts,state.records,state.decisions)).toMatchObject({studies:0,invalid:1});expect(report(state)).not.toContain('> "Missing"');expect(evidenceCsv(state)).toContain('Source removed');
});
test('quotation verification refuses malformed coordinates even when JavaScript slicing would match',()=>{
 const record={id:'a',abstract:'text'},base={record:'a',start:0,end:4,text:'text'};expect(verify([record],base)).toBe(true);
 for(const patch of [{start:-4},{start:0.5},{end:99},{end:4.9},{start:'0'},{text:''},{start:4,end:4,text:''}])expect(verify([record],{...base,...patch})).toBe(false);
 expect(verify([record],null)).toBe(false);expect(()=>excerpt(record,0.5,4)).toThrow();
});


import {prepareReviewProject} from '../synthesis.mjs';
test('review preparation preserves legacy projects, stale evidence and metadata without sharing editor objects',()=>{
 const pkg=synthesisFiles(sample),project=JSON.parse(pkg.files[pkg.asset_prefix+'sample-project.json']),prepared=prepareReviewProject(project);expect(prepared).toEqual(project);prepared.records[0].title='Independent';expect(project.records[0].title).not.toBe('Independent');
 expect(prepareReviewProject({question:'New',records:[],excerpts:[]})).toEqual({question:'New',records:[],excerpts:[],claims:[],decisions:{}});
 const missing=structuredClone(project);missing.records=[];expect(prepareReviewProject(missing).excerpts).toEqual(project.excerpts);
});
test('malformed imported structures, dangerous addresses and conflicting identities are rejected before adoption',()=>{
 const pkg=synthesisFiles(sample),original=JSON.parse(pkg.files[pkg.asset_prefix+'sample-project.json']);
 const changes=[p=>p.question={},p=>p.claims=null,p=>p.decisions=[],p=>p.records[0].authors=null,p=>p.records[0].sources={},p=>p.records[0].url='javascript:alert(1)',p=>p.records[0].sources[0].url='https://user:pass@example.test',p=>p.records.push(p.records[0]),p=>p.claims.push(p.claims[0]),p=>p.excerpts.push(p.excerpts[0]),p=>p.excerpts[0].start=-1,p=>p.decisions[p.records[0].id]={n:'1" autofocus'},p=>p.duplicates={},p=>p.retrieved={},p=>p.records[0].id='__proto__'];
 for(const change of changes){const value=structuredClone(original);change(value);const before=structuredClone(value);expect(()=>prepareReviewProject(value)).toThrow();expect(value).toEqual(before);}
});
