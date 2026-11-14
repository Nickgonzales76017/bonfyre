/* Complete 43-component native render regression: exact first-party template
 * catalogue, real JSON payloads, nested C tree, bounded atomic output,
 * link/script refusal, independent account state and interactive semantics. */
#define _GNU_SOURCE
#include "../src/authoring_native_components.h"
#include "../../BonfyreWebAddress/generated/native_authoring_templates.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void check(int ok,const char *message){
 if(!ok){fprintf(stderr,"native components FAIL: %s\n",message);exit(1);}
}
static char *html(const BfAuthoringTree *tree,size_t max,int allowed){
 char *result=NULL;size_t len=0;
 FILE *out=open_memstream(&result,&len);check(out!=NULL,"open memory");
 check(bf_authoring_native_render(out,tree,max)==allowed,"render status");
 check(fclose(out)==0,"close memory");
 if(!allowed)check(len==0,"atomic denial emitted no bytes");
 return result;
}
static const char *example=
 "{"
 "\"title\":\"Original native site <safe>\","
 "\"heading\":\"User-authored section\","
 "\"body\":\"A <rich> native component & proof\","
 "\"lede\":\"Full C without a JS document export\","
 "\"eyebrow\":\"Authoring\","
 "\"brand\":\"Aurekai\","
 "\"label\":\"Inspect\","
 "\"href\":\"/work/\","
 "\"name\":\"email\","
 "\"type\":\"radio\","
 "\"hint\":\"Useful information\","
 "\"dismissible\":true,"
 "\"min\":0,\"max\":100,\"value\":35,"
 "\"columns\":3,\"required\":true,"
 "\"image\":{\"src\":\"https://example.org/photo.webp\",\"alt\":\"Documentary photograph\"},"
 "\"actions\":[{\"label\":\"Explore\",\"href\":\"/work/\"}],"
 "\"links\":[{\"label\":\"Work\",\"href\":\"/work/\"}],"
 "\"pages\":[{\"label\":\"One\",\"href\":\"/\"}],"
 "\"toc\":[{\"label\":\"Overview\",\"href\":\"#overview\"}],"
 "\"tabs\":[{\"label\":\"Overview\",\"body\":\"First panel\"},{\"label\":\"Details\",\"body\":\"Second panel\"}],"
 "\"slides\":[{\"title\":\"First\",\"body\":\"One story\",\"src\":\"\",\"alt\":\"\"},{\"title\":\"Second\",\"body\":\"Two stories\",\"src\":\"\",\"alt\":\"\"}],"
 "\"items\":[{\"title\":\"Question one\",\"body\":\"Answer one\",\"label\":\"Item\",\"href\":\"/docs/\"},"
 "{\"title\":\"Question two\",\"body\":\"Answer two\",\"label\":\"Item two\",\"href\":\"/docs/\"}],"
 "\"fields\":[{\"label\":\"Email\",\"name\":\"email\",\"type\":\"email\",\"required\":true},"
 "{\"label\":\"Your note\",\"name\":\"message\",\"type\":\"textarea\",\"required\":false}],"
 "\"options\":[{\"label\":\"First\",\"value\":\"first\"}],"
 "\"headers\":[\"Name\",\"Value\"],"
 "\"rows\":[[\"Alpha\",\"42\"],[\"Beta\",\"43\"]]"
 "}";
int main(void){
 BfAuthoringTree *tree=calloc(1,sizeof(*tree)),*other=calloc(1,sizeof(*other));
 check(tree&&other,"allocate two account trees");
 check(bf_authoring_tree_add(tree,"root","ui:documentation",NULL,0),"root");
 check(bf_authoring_tree_data(tree,"root","payload",example),"root payload");
 size_t n=sizeof(bf_native_authoring_templates)/sizeof(bf_native_authoring_templates[0]);
 check(n==43,"36 visual plus seven original profile templates");
 for(size_t i=0;i<n;i++){
  char id[36];snprintf(id,sizeof(id),"component-%zu",i);
  const BfNativeAuthoringTemplate *template=&bf_native_authoring_templates[i];
  check(bf_authoring_tree_kind(template->kind)!=NULL,"same generated tree kind");
  check(bf_authoring_tree_add(tree,id,template->kind,"root",(int)i),"nested typed insertion");
  check(bf_authoring_tree_data(tree,id,"payload",example),"rich JSON payload");
  check(bf_authoring_tree_text(tree,id,"heading","Visible <heading> & proof"),"heading");
  check(bf_native_authoring_template(template->kind)==template,"generated lookup");
 }
 char *result=html(tree,1048576,1);
 check(result&&strstr(result,"data-authored-native=\"c\""),"native page projection");
 check(strstr(result,"&lt;safe&gt;")!=NULL,"trusted Mustach escapes rich heading");
 check(strstr(result,"&lt;heading&gt; &amp; proof")!=NULL,"profile HTML escaped");
 check(strstr(result,"role=\"tablist\"")!=NULL,"tabs survive C lowering");
 check(strstr(result,"data-ak-tab=\"0\"")!=NULL&&
       strstr(result,"data-ak-tab=\"1\"")!=NULL,"indexed tabs generated natively");
 check(strstr(result,"data-ak-slide=\"1\"")!=NULL,"indexed slides generated natively");
 check(strstr(result,"<dialog")!=NULL,"native modal present");
 check(strstr(result,"data-ak-effect=\"review-required\"")!=NULL,"form never self-authorizes");
 check(strstr(result,"<td>Beta</td>")!=NULL,"nested tabular source retained");
 check(strstr(result,"<textarea name=\"message\"")!=NULL,"native textarea typed");
 check(strstr(result,"<img")!=NULL,"native media markup");
 for(size_t i=0;i<n;i++){
  char id[55];snprintf(id,sizeof(id),"data-ak-id=\"component-%zu\"",i);
  check(strstr(result,id)!=NULL,"every component has stable native identity");
 }
 free(result);
 char *shorter=html(tree,128,0);check(shorter!=NULL&&!*shorter,"short output denied");free(shorter);
 int idx=bf_authoring_tree_index(tree,"component-0");check(idx>0,"lookup");
 char *before=tree->nodes[idx].payload;
 tree->nodes[idx].payload=bf_authoring_tree_copytext(
  "{\"label\":\"Unsafe\",\"href\":\"javascript:alert(1)\"}",256);
 result=html(tree,1048576,0);free(result);
 free(tree->nodes[idx].payload);tree->nodes[idx].payload=before;
 check(bf_authoring_tree_add(other,"independent","ui:modal",NULL,0),"second account");
 check(bf_authoring_tree_data(other,"independent","payload",example),"second material");
 result=html(other,1048576,1);
 check(strstr(result,"component-0")==NULL&&strstr(result,"independent")!=NULL,
   "second native account state isolated");
 free(result);
 BfAuthoringTree *article=calloc(1,sizeof(*article));check(article!=NULL,"article tree");
 check(bf_authoring_tree_add(article,"article","profile-story",NULL,0),"article block");
 check(bf_authoring_tree_text(article,"article","heading","A reviewed article"),"article title");
 check(bf_authoring_tree_text(article,"article","body","Flattened article body marker"),"fallback body");
 const char *editorial="[{\"type\":\"paragraph\",\"data\":{\"text\":\"Structured article body marker <safe>\"}},{\"type\":\"header\",\"data\":{\"level\":3,\"text\":\"Heading <three>\"}},{\"type\":\"quote\",\"data\":{\"text\":\"Evidence & attribution\",\"caption\":\"Author\"}},{\"type\":\"code\",\"data\":{\"code\":\"if (n < 5) return;\"}},{\"type\":\"delimiter\",\"data\":{}},{\"type\":\"list\",\"data\":{\"style\":\"ordered\",\"items\":[\"Alpha\",\"Beta\"]}},{\"type\":\"checklist\",\"data\":{\"items\":[{\"text\":\"Read sources\",\"checked\":true},{\"text\":\"Review\",\"checked\":false}]}},{\"type\":\"table\",\"data\":{\"content\":[[\"Name\",\"Value\"],[\"Example\",\"42\"]]}}]";
 check(bf_authoring_tree_data(article,"article","editorial",editorial),"all eight native editorial types");
 result=html(article,1048576,1);
 check(strstr(result,"<p class=\"ak-editorial-paragraph\">Structured article body marker &lt;safe&gt;</p>")!=NULL,"paragraph escaped");
 check(strstr(result,"<h3>Heading &lt;three&gt;</h3>")!=NULL,"heading level 3");
 check(strstr(result,"<blockquote class=\"ak-editorial-quote\">")!=NULL,"quoted block");
 check(strstr(result,"Evidence &amp; attribution")!=NULL,"quote escaped");
 check(strstr(result,"<pre class=\"ak-editorial-code\"><code>if (n &lt; 5) return;")!=NULL,"literal code escaped");
 check(strstr(result,"<hr class=\"ak-editorial-break\">")!=NULL,"delimiter block");
 check(strstr(result,"<ol class=\"ak-editorial-list\">")!=NULL&&strstr(result,"<li>Alpha</li>")!=NULL,"ordered list items");
 check(strstr(result,"<ul class=\"ak-editorial-checklist\">")!=NULL&&strstr(result,"Read sources")!=NULL,"checked list retained");
 check(strstr(result,"<td>Example</td>")!=NULL,"tabular editorial content retained");
 check(strstr(result,"Flattened article body marker")==NULL,"do not duplicate flattened body");
 check(strstr(result,"<p></p>")==NULL,"no empty fallback paragraph");
 free(result);
 result=html(article,175,0);free(result);
 check(!bf_authoring_tree_data(article,"article","editorial",
  "[{\"type\":\"list\",\"data\":{\"style\":\"position:absolute\",\"items\":[\"item\"]}}]"),
  "unreviewed CSS style refused");
 check(bf_authoring_tree_data(article,"article","editorial",
  "[{\"type\":\"arbitrary-script\",\"data\":{\"text\":\"not allowed\"}}]"),
  "unknown editorial syntax is retained but not rendered");
 result=html(article,1048576,0);
 free(result);bf_authoring_tree_clear(article);free(article);
 bf_authoring_tree_clear(tree);bf_authoring_tree_clear(other);
 free(tree);free(other);
 puts("native 43-component render PASS");return 0;
}
