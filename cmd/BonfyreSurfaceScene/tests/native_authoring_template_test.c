/* Verify upstream Mustach actually renders nested native document data. */
#define _GNU_SOURCE
#include "../src/authoring_template.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void check(int v,const char *why){if(!v){fprintf(stderr,"native template FAIL: %s\n",why);exit(1);}}
static char *render(const char *tpl,const char *data,int accepted,size_t limit){
 char *buf=NULL;size_t n=0;FILE *out=open_memstream(&buf,&n);
 check(out!=NULL,"stream");
 check(bf_authoring_template_render(out,tpl,data,limit)==accepted,"admission");
 check(fclose(out)==0,"close");return buf;
}
int main(void){
 const char *tpl="<main><h1>{{title}}</h1>{{#entries}}<article>"
                  "<h2>{{heading}}</h2>{{#details}}<span>{{name}} {{value}}</span>{{/details}}"
                  "{{/entries}}</article></main>";
 const char *data="{\"title\":\"Sustainable <work> & stories\","
                  "\"entries\":[{\"heading\":\"First \\\"record\\\"\","
                  "\"details\":[{\"name\":\"Revenue\",\"value\":125},{\"name\":\"Links\",\"value\":\"<safe>\"}]},"
                  "{\"heading\":\"Second profile\",\"details\":[{\"name\":\"Owner\",\"value\":\"Other & owner\"}]}]}";
 char *out=render(tpl,data,1,20000);
 check(strstr(out,"Sustainable &lt;work&gt; &amp; stories")!=NULL,"escape heading");
 check(strstr(out,"First &quot;record&quot;")!=NULL,"escaped records");
 check(strstr(out,"<span>Revenue 125</span>")!=NULL,"nested JSON iteration");
 check(strstr(out,"&lt;safe&gt;")!=NULL,"escaped nested value");
 check(strstr(out,"Other &amp; owner")!=NULL,"nested second item");
 free(out);
 out=render(tpl,data,0,48);check(*out=='\0',"bounded result is atomic");free(out);
 out=render("<div>{{{body}}}</div>","{\"body\":\"<script>bad</script>\"}",0,2048);
 check(*out=='\0',"unescaped input refused");free(out);
 out=render("<p>{{name}}</p>","{invalid",0,2048);check(*out=='\0',"malformed JSON refused");free(out);
 puts("native mustach composition PASS");return 0;
}
