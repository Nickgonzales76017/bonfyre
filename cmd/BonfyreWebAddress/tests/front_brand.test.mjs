// Compile the production C renderer instead of reimplementing its brand in a test.
import {after,test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {AUREKAI_VOICE} from '../../../branding/voice.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const temp=mkdtempSync(join(tmpdir(),'bf-brand-'));
after(()=>rmSync(temp,{recursive:true,force:true}));
const c=join(temp,'brand-render.c'),out=join(temp,'brand-render');
writeFileSync(c,[
 '#include <stdio.h>',
 '#include <string.h>',
 '#include <stddef.h>',
 'static void html(const char *text){',
 '  for(const unsigned char *s=(const unsigned char *)text;*s;s++){',
 '    if(*s==38)fputs("&amp;",stdout);',
 '    else if(*s==60)fputs("&lt;",stdout);',
 '    else if(*s==62)fputs("&gt;",stdout);',
 '    else if(*s==34)fputs("&quot;",stdout);',
 '    else putchar(*s);',
 '  }',
 '}',
 '#include "front_brand.h"',
 'int main(int argc,char **argv){',
 '  if(argc!=2)return 2;',
 '  front_brand_emit(argv[1]);',
 '  return 0;',
 '}'
].join('\n')+'\n');
const compile=spawnSync(process.env.CC||'cc',['-std=c11','-Wall','-Wextra','-Werror','-pedantic','-I',join(root,'src'),c,'-o',out],{encoding:'utf8',timeout:30000});
assert.equal(compile.status,0,'native C compilation failed: '+compile.stderr);
const render=territory=>{
 const r=spawnSync(out,[territory],{encoding:'utf8',timeout:5000});
 assert.equal(r.status,0,r.stderr);
 return r.stdout;
};
const corpus=Object.fromEntries(Object.entries(AUREKAI_VOICE.channels).map(([territory,channel])=>[territory,{
 title:channel.title,cta:AUREKAI_VOICE.fronts[territory].action,
 word:({work:'finance',life:'communities',live:'recording',shop:'catalogue'})[territory]
}]));
for(const [territory,data] of Object.entries(corpus))test('native '+territory+' edition has authored art, route and correct context',()=>{
 const html=render(territory);
 assert.match(html,new RegExp('data-brand-territory="'+territory+'"'));
 assert.equal((html.match(/data-brand-masthead/g)||[]).length,1);
 assert.ok(html.includes(data.title));
 assert.ok(html.includes('href="'+data.cta+'"'));
 assert.ok(html.toLowerCase().includes(data.word));
 assert.ok(html.includes('Built on software.<br>Driven by Aurekai.'));
 assert.ok(html.includes(AUREKAI_VOICE.fronts[territory].category));
 assert.ok(html.includes('href="https://nickgonzales76017.github.io/bonfyre/about/"'));
 assert.equal((html.match(/aria-current="page"/g)||[]).length,1);
 assert.ok(html.includes('href="https://aurekai.'+territory+'/" aria-current="page"'));
 for(const apex of Object.keys(corpus)) assert.ok(html.includes('href="https://aurekai.'+apex+'/"'));
 assert.match(html,/aria-labelledby="af-brand-title"/);
 assert.match(html,/aria-label="Aurekai destinations"/);
 assert.match(html,/@media\(max-width:600px\)/);
 assert.match(html,/:focus-visible/);
 assert.match(html,/prefers-reduced-motion/);
 assert.equal((html.match(/<nav/g)||[]).length,1);
 assert.equal((html.match(/<\/nav>/g)||[]).length,1);
 assert.ok(!html.includes('<script'));
});
test('unsupported territory and independent site cannot receive brand edition',()=>{
 for(const input of ['independent','customer','example.com','', 'WORK','work.evil.test']){
  assert.equal(render(input),'','unadmitted identity '+input+' rendered');
 }
});
test('four variations have scoped palettes and one authentic company proof route',()=>{
 const outputs=Object.fromEntries(Object.keys(corpus).map(t=>[t,render(t)]));
 assert.equal(new Set(Object.values(corpus).map(x=>x.title)).size,4);
 for(const territory of Object.keys(corpus)){
  assert.ok(outputs[territory].includes('.af-brand[data-brand-territory='+territory+']'));
  assert.ok(outputs[territory].includes('data-brand-masthead'));
 }
});
