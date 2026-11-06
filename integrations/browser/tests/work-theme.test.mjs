import {expect,test} from 'bun:test';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {workTheme,workStream,workThemeIcons} from '../aurekai-activity-source.mjs';

test('numbered connector inventories share a visual workstream but preserve native task identity',()=>{
 const child={id:'account-operator-abc',title:'Connection Studio source catalogue: Nango 1,084 provider configurations 81–160',nodes:[],continuation:null,kind:'task'};
 const original=structuredClone(child);
 expect(workTheme(child)).toBe('Connections');
 expect(workStream(child)).toBe('Connection Studio source catalogue: Nango 1,084 provider configurations');
 expect(child).toEqual(original);
});

test('research, native work and unknown titles do not imply permissions or a receiving workspace',()=>{
 expect(workTheme({kind:'research',title:'Unrelated provider label'})).toBe('Sources');
 expect(workTheme({title:'Improve native release bounds',nodes:[{family:'runtime'}]})).toBe('Engineering');
 expect(workTheme({title:'Unexpected work',nodes:[]})).toBe('Other');
 expect(workStream({title:'Customer account verification'})).toBeNull();
 expect(Object.keys(workThemeIcons).sort()).toEqual(['Commerce','Connections','Engineering','Identity','Other','Sources','Worlds'].sort());
});

test('rail SVG masks match exact native Tabler macro geometry',()=>{
 const source=readFileSync(resolve(import.meta.dir,'../../../cmd/BonfyreWebAddress/src/glyph_set.h'),'utf8');
 const css=readFileSync(resolve(import.meta.dir,'../aurekai-activity.css'),'utf8');
 for(const [name,macro] of Object.entries({link:'LINK',book:'BOOK',tool:'TOOL',group:'GROUP',world:'WORLD',coins:'COINS',board:'BOARD'})){
  const line=source.split('\n').find(line=>line.startsWith('#define BF_ICON_'+macro+' '));
  expect(line).toBeTruthy();
  const body=line.match(/BF_ICON_OPEN ("(?:\\.|[^"\\])*") BF_ICON_CLOSE/);
  expect(body).toBeTruthy();
  const svg='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="black" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'+JSON.parse(body[1])+'</svg>';
  expect(css).toContain(`[data-ak-icon="${name}"]{--ak-work-glyph:url("data:image/svg+xml,${encodeURIComponent(svg)}")}`);
 }
});
