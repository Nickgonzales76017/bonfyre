#define _GNU_SOURCE
#include "../src/authoring_markdown.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void ck(int pass,const char *reason){
 if(!pass){fprintf(stderr,"MD4C native regression FAIL: %s\n",reason);exit(1);}
}
static char *html(const char *markdown,size_t limit,int expected){
 char *result=NULL;size_t length=0;
 FILE *buffer=open_memstream(&result,&length);
 ck(buffer!=NULL,"open output buffer");
 int worked=bf_authoring_markdown_render(buffer,markdown,limit);
 ck(worked==expected,"expected Markdown publication outcome");
 ck(fclose(buffer)==0,"close native output");
 if(!expected)ck(length==0,"invalid Markdown produces no partial HTML");
 return result;
}
int main(void){
 char *s=html("# Native source\n\nA **bold** and *emphasized* paragraph.\n\n"
  "- [x] Reviewed\n- [ ] Still open\n\n"
  "| Key | Value |\n| --- | --- |\n| CMS | Native |\n\n"
  "See [the account](/work/) and [upstream](https://example.org/source).\n",
  20000,1);
 ck(strstr(s,"<h1>Native source</h1>")!=NULL,"GFM heading");
 ck(strstr(s,"<strong>bold</strong>")!=NULL,"strong emphasis");
 ck(strstr(s,"<em>emphasized</em>")!=NULL,"italic");
 ck(strstr(s,"<table>")!=NULL&&strstr(s,"<td>Native</td>")!=NULL,"GFM table");
 ck(strstr(s,"href=\"/work/\"")!=NULL&&strstr(s,"href=\"https://example.org/source\"")!=NULL,
    "approved internal/external links");
 ck(strstr(s,"<input")!=NULL,"GFM task list");
 free(s);
 s=html("<script>alert(1)</script>\n\nSafe text.\n",20000,1);
 ck(strstr(s,"<script")==NULL,"unsafe raw HTML not emitted");
 free(s);
 s=html("[unsafe](javascript:alert(1))",20000,0);free(s);
 s=html("[also unsafe](data:text/html,payload)",20000,0);free(s);
 s=html("[credentials](https://user@example.org/)",20000,0);free(s);
 s=html("![not yet cleared](https://example.org/camera.jpg)",20000,0);free(s);
 s=html("## Title\n\nText longer than small bound.",8,0);free(s);
 s=html("Simple prose.",1048576,1);ck(strstr(s,"<p>Simple prose.</p>")!=NULL,"full bound");
 free(s);
 puts("MD4C native markdown security and GFM PASS");
 return 0;
}
