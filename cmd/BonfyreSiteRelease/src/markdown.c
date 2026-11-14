/* Bonfÿre native Markdown compiler — effectless retained-content transform.
 * MD4C performs GFM parsing. The shared admission layer refuses arbitrary
 * HTML, remote images, unsafe links and over-budget output atomically.
 * This tool does not publish, rewrite CMS or grant source/asset rights. */
#include "../../BonfyreSurfaceScene/src/authoring_markdown.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define BF_INPUT_LIMIT 131072u
int main(int argc,char **argv){
 if(argc!=2||strcmp(argv[1],"--fragment")){
  fputs("usage: bonfyre-markdown --fragment (read Markdown from stdin)\n",stderr);
  return 2;
 }
 char *content=(char *)malloc(BF_INPUT_LIMIT+2u);
 if(!content)return 3;
 size_t size=0;int failed=0;
 while(!failed){
  if(size>BF_INPUT_LIMIT){failed=1;break;}
  size_t available=(BF_INPUT_LIMIT+1u)-size;
  if(!available){failed=1;break;}
  const size_t n=fread(content+size,1,available,stdin);
  if(n>0){if(memchr(content+size,'\0',n))failed=1;size+=n;}
  if(n<available){
   if(ferror(stdin))failed=1;
   break;
  }
 }
 if(size>BF_INPUT_LIMIT)failed=1;
 if(!size)failed=1;
 int done=0;
 if(!failed){
  content[size]='\0';
  done=bf_authoring_markdown_render(stdout,content,1048576u);
  if(done&&fflush(stdout)!=0)done=0;
 }
 free(content);
 if(!done){fputs("bonfyre-markdown: invalid or oversized Markdown\n",stderr);return 1;}
 return 0;
}
