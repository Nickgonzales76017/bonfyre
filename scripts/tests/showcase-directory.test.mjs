// Offline DOM-contract checks, not a browser or end-to-end application test.
// Run: node --test scripts/tests/showcase-directory.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {runInNewContext} from 'node:vm';
const root = new URL('../../', import.meta.url);
const html = readFileSync(new URL('site/index.html', root), 'utf8');
const script = readFileSync(new URL('site/assets/showcase.js', root), 'utf8');
const decode = value => value.replaceAll('&amp;', '&').replaceAll('&quot;', '"').replaceAll('&#x27;', "'").replaceAll('&lt;', '<').replaceAll('&gt;', '>');
class Element {
  constructor(tag) { this.tagName=tag; this.children=[]; this.dataset={}; this.attrs={}; this.listeners={}; this.textContent=''; this.scrollLeft=0; this.scrollWidth=1800; this.clientWidth=450; }
  append(...nodes) { for(const node of nodes){node.parentNode=this;this.children.push(node);} }
  insertBefore(node,before) { const index=this.children.indexOf(before); assert.notEqual(index,-1);node.parentNode=this;this.children.splice(index,0,node); }
  setAttribute(key,value) { this.attrs[key]=value; }
  getAttribute(key) { return this.attrs[key]??null; }
  get firstElementChild() { return this.children[0]; }
  getBoundingClientRect() { return {width:440}; }
  querySelector(selector) { return this.children.find(child=>selector==='strong'?child.tagName==='strong':selector==='.detail'&&child.className==='detail')??null; }
  querySelectorAll(selector) { return this.children.filter(child=>selector==='a[data-demo]'&&child.tagName==='a'&&child.attrs['data-demo']!==undefined); }
  addEventListener(name,fn) { (this.listeners[name]??=[]).push(fn); }
  emit(name,event={}) { for(const fn of this.listeners[name]??[])fn({target:this,...event}); }
  remove() { this.parentNode.children=this.parentNode.children.filter(child=>child!==this); }
  scrollBy(options) { this.lastScroll=options; this.scrollLeft=Math.max(0,Math.min(this.scrollWidth-this.clientWidth,this.scrollLeft+options.left));this.emit('scroll'); }
}
function sources() {
  // Deliberately limited to the authored anchor contract, not a general HTML parser.
  return [...html.matchAll(/<a\b([^>]*\bdata-demo(?:\s|>)[^>]*)>(.*?)<\/a>/gs)].map(([,attributes,body])=>{
    const source=new Element('a');source.attrs['data-demo']='';
    for(const [,key,value] of attributes.matchAll(/([\w-]+)="([^"]*)"/g)) {
      source.setAttribute(key,decode(value));
      if(key.startsWith('data-'))source.dataset[key.slice(5).replace(/-([a-z])/g,(_,c)=>c.toUpperCase())]=decode(value);
    }
    for(const [tag,cls,pattern] of [['strong','',/<strong>(.*?)<\/strong>/s],['span','detail',/<span class="detail">(.*?)<\/span>/s]]) {
      const node=new Element(tag);node.className=cls;node.textContent=decode(body.match(pattern)?.[1]??'');source.append(node);
    }
    return source;
  });
}
function setup(rows=sources(),{reduced=false,present=true}={}) {
  const parent=new Element('div'),list=new Element('div');list.append(...rows);parent.append(list);
  const find=(id,node=parent)=>node.id===id?node:node.children.map(child=>find(id,child)).find(Boolean);
  const document={baseURI:'https://nickgonzales76017.github.io/bonfyre/',querySelector:()=>present?list:null,getElementById:find,createElement:tag=>new Element(tag)};
  const window={matchMedia:()=>({matches:reduced}),addEventListener:()=>{}};
  const context={document,window,URL};const run=()=>runInNewContext(script,context);run();
  return {parent,list,find,run};
}
test('thirteen unique authored application links include current acquisition and Work Samples previews',()=>{
  const rows=sources(),links=rows.map(row=>row.attrs.href);
  assert.equal(rows.length,13);assert.equal(new Set(links).size,13);
  for(const href of ['https://browser-workbench.aurekai-work-samples.pages.dev/',... ['red3-planner','catalogue','audio'].map(path=>`https://aurekai-work-samples.pages.dev/${path}/`)])assert.ok(links.includes(href),href);
  assert.equal(links[0],'https://itad-desk-upgrade.aurekai-work-samples.pages.dev/itad-exchange/');
  assert.equal(links[1],'https://brief-studio-upgrade.aurekai-work-samples.pages.dev/brief-studio/');
  assert.equal(links.filter(link=>link.includes('pages-shift-handoff')).length,1);
  assert.ok(html.includes('data-demo-list aria-label="Working application directory"'));
});
test('the authored directory remains ordinary links without JavaScript',()=>{
  for(const row of sources()){assert.equal(new URL(row.attrs.href).protocol,'https:');assert.ok(row.querySelector('strong').textContent);assert.ok(row.querySelector('.detail').textContent);}
  assert.ok(html.includes('aria-label="Guides and case studies"'));
  assert.doesNotMatch(html.split('aria-label="Guides and case studies"')[1],/\bdata-demo\b/);
});
test('carousel order, destinations, titles and descriptions derive from that directory',()=>{
  const {find}=setup(),cards=find('demo-preview-track').children,rows=sources();
  assert.equal(cards.length,rows.length);
  cards.forEach((card,i)=>{assert.equal(card.href,rows[i].attrs.href);const body=card.children.at(-1);assert.equal(body.children[1].textContent,rows[i].querySelector('strong').textContent);assert.equal(body.children[2].textContent,rows[i].querySelector('.detail').textContent);});
  assert.equal(find('demo-showcase').attrs['aria-label'],'Featured application previews');
});
test('a changed source destination propagates without editing a second registry',()=>{
  const rows=sources();rows[0].setAttribute('href','https://aurekai-work-samples.pages.dev/future-reviewed-route/');
  assert.equal(setup(rows).find('demo-preview-track').children[0].href,rows[0].attrs.href);
});
test('duplicate, active-scheme and credential-bearing links cannot enter the carousel',()=>{
  const rows=sources();for(const href of [rows[0].attrs.href,'javascript:alert(1)','https://user:secret@example.test/','http://example.test/']){const row=sources()[0];row.setAttribute('href',href);rows.push(row);}
  assert.equal(setup(rows).find('demo-preview-track').children.length,sources().length);
});
test('untrusted titles remain text and do not create elements',()=>{
  const rows=sources();rows[0].querySelector('strong').textContent='<img src=x onerror=alert(1)>';
  const title=setup(rows).find('demo-preview-track').children[0].children.at(-1).children[1];
  assert.equal(title.textContent,'<img src=x onerror=alert(1)>');assert.equal(title.children.length,0);
});
test('only explicitly described published captures are shown; missing images leave the link usable',()=>{
  const {find}=setup(),cards=find('demo-preview-track').children;
  assert.equal(cards[2].children.filter(node=>node.tagName==='img').length,0);
  const images=cards.flatMap(card=>card.children.filter(node=>node.tagName==='img'));
  assert.equal(images.length,8);assert.ok(images.every(image=>image.alt&&/\.(png|jpg)$/.test(image.src))); 
  const card=images[0].parentNode,href=card.href;images[0].emit('error');
  assert.equal(card.children.length,1);assert.equal(card.href,href);assert.equal(card.children[0].className,'card-body');
  const rows=sources();rows[5].dataset.shotAlt='';assert.equal(setup(rows).find('demo-preview-track').children[5].children.length,1);
});
test('repeated initialization does not duplicate the carousel',()=>{
  const state=setup();state.run();assert.equal(state.parent.children.filter(node=>node.id==='demo-showcase').length,1);
});
test('absent and empty directories stay valid without injecting controls',()=>{
  for(const state of [setup([]),setup([],{present:false})])assert.equal(state.find('demo-showcase'),undefined);
});
test('keyboard scrolling respects reduced motion and does not consume link keystrokes',()=>{
  const {find}=setup(sources(),{reduced:true}),track=find('demo-preview-track');let prevented=0;
  track.emit('keydown',{key:'ArrowRight',preventDefault(){prevented++;}});
  assert.equal(track.lastScroll.left,456);assert.equal(track.lastScroll.behavior,'auto');assert.equal(prevented,1);
  track.emit('keydown',{key:'ArrowLeft',target:track.children[0],preventDefault(){prevented++;}});
  track.emit('keydown',{key:'ArrowLeft',ctrlKey:true,preventDefault(){prevented++;}});assert.equal(prevented,1);
});
test('controls reflect scroll boundaries and their labels target the track',()=>{
  const {find}=setup(),section=find('demo-showcase'),buttons=section.children[0].children[1].children,track=find('demo-preview-track');
  assert.equal(buttons[0].disabled,true);assert.equal(buttons[1].disabled,false);
  assert.ok(buttons.every(button=>button.type==='button'&&button.attrs['aria-controls']===track.id&&button.attrs['aria-label']));
  track.scrollLeft=1350;track.emit('scroll');assert.equal(buttons[1].disabled,true);assert.equal(buttons[0].disabled,false);
  buttons[0].emit('click');assert.equal(track.lastScroll.behavior,'smooth');assert.equal(buttons[1].disabled,false);
});
test('Aurekai adoption points to the live owner without carrying private context',()=>{
  const {find}=setup(),link=find('aurekai-adoption'),scope=find('aurekai-adoption-scope');
  assert.equal(link.href,'https://aurekai.work/packs/setup');
  assert.equal(new URL(link.href).search,'');
  assert.equal(link.attrs['aria-describedby'],scope.id);
  assert.match(scope.textContent,/Demo drafts and application backends remain separate/);
});
