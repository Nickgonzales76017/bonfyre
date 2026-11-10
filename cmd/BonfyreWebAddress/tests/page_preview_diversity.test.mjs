// Exercise the exact production ordering function with native C compilation.
// This test does not replace the route's source/form authorization contract.
import {test,expect} from 'bun:test';
import {mkdtempSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
const main=readFileSync(new URL('../src/page_core_rendering.h',import.meta.url),'utf8');
const start=main.indexOf('static void page_take_turns(PageStream *stream) {');
const end=main.indexOf('\n}',start)+2;
const native=main.slice(start,end);
test('section preview rotates publishers after native filtering, without changing route time order',()=>{
 const begin=main.indexOf('static void page_emit_section('),section=main.slice(begin,main.indexOf('\n}',begin));
 expect(section.indexOf('page_filter_route_contract')).toBeGreaterThan(0);
 expect(section.indexOf('page_take_turns(stream)')).toBeGreaterThan(section.indexOf('page_front_drop_used'));
 expect(section.indexOf('page_take_turns(stream)')).toBeLessThan(section.indexOf('page_emit_region(region, stream, take, &ctx)'));
 expect(section).toContain('region->material != BF_PAGE_MATERIAL_EVENT');
 expect(section).toContain('region->topology != BF_PAGE_TOPOLOGY_TABLE');
 expect(section).toContain('region->topology != BF_PAGE_TOPOLOGY_THREAD');
});
test('production rotation retains every record, consumed prefix, source order and source timestamps',()=>{
 expect(start).toBeGreaterThan(0);expect(end).toBeGreaterThan(start);
 const directory=mkdtempSync(join(tmpdir(),'public-preview-diversity-'));
 const code=`#include <assert.h>
#include <stdio.h>
#include <string.h>
#define PAGE_STREAM_MAX 320
typedef struct {char source[96];char title[128];long at;int original;} PageItem;
typedef struct {int count,cursor;PageItem item[PAGE_STREAM_MAX];} PageStream;
${native}
static void put(PageStream *s,const char *source,int id){PageItem *p=&s->item[s->count++];snprintf(p->source,sizeof(p->source),"%s",source);snprintf(p->title,sizeof(p->title),"record-%d",id);p->at=1900000000L-id;p->original=id;}
int main(void){
 PageStream s={0};put(&s,"already-shown",0);s.cursor=1;
 for(int i=1;i<=12;i++)put(&s,"prolific",i);
 put(&s,"second",13);put(&s,"second",14);put(&s,"third",15);put(&s,"fourth",16);
 PageStream before=s;page_take_turns(&s);
 assert(s.count==before.count&&s.cursor==1&&!memcmp(&s.item[0],&before.item[0],sizeof(PageItem)));
 assert(!strcmp(s.item[1].source,"prolific"));assert(!strcmp(s.item[2].source,"second"));
 assert(!strcmp(s.item[3].source,"third"));assert(!strcmp(s.item[4].source,"fourth"));
 int seen[320]={0};for(int i=0;i<s.count;i++){int id=s.item[i].original;assert(!seen[id]++);assert(!memcmp(&s.item[i],&before.item[id],sizeof(PageItem)));}
 int last=-1;for(int i=1;i<s.count;i++)if(!strcmp(s.item[i].source,"prolific")){assert(s.item[i].original>last);last=s.item[i].original;}
 PageStream once=s;page_take_turns(&s);assert(!memcmp(&s,&once,sizeof(s)));
 PageStream single={0};for(int i=0;i<8;i++)put(&single,"only",i);before=single;page_take_turns(&single);assert(!memcmp(&single,&before,sizeof(single)));
 single.cursor=single.count;before=single;page_take_turns(&single);assert(!memcmp(&single,&before,sizeof(single)));
 PageStream full={0};for(int i=0;i<320;i++)put(&full,i<315?"large":"small",i);before=full;page_take_turns(&full);
 assert(full.item[1].original==315);memset(seen,0,sizeof(seen));for(int i=0;i<320;i++){int id=full.item[i].original;assert(id>=0&&id<320&&!seen[id]++);assert(!memcmp(&full.item[i],&before.item[id],sizeof(PageItem)));}
 puts("public preview diversity: bounded, lossless, source-stable ordering passed");return 0;
}`;
 try{
  const source=join(directory,'test.c'),binary=join(directory,'test');writeFileSync(source,code);
  const compiled=Bun.spawnSync(['cc','-std=c11','-Wall','-Wextra','-Werror',source,'-o',binary],{stdout:'pipe',stderr:'pipe'});
  expect(compiled.stderr.toString()).toBe('');expect(compiled.exitCode).toBe(0);
  const result=Bun.spawnSync([binary],{stdout:'pipe',stderr:'pipe'});expect(result.exitCode).toBe(0);expect(result.stdout.toString()).toContain('lossless');
 }finally{rmSync(directory,{recursive:true,force:true});}
});
