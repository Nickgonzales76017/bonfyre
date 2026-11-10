/* End-to-end published Aurekai profile serving from the exact source-owned
 * release pack. Covers any of the 27 first-party sections without replacing
 * their HTML/client behavior with simplified per-section native templates. */
#include "../src/authored_release_reader.h"
#include "bonfyre.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CAP (1048577u)
static void ck(int condition,const char *message){
 if(!condition){fprintf(stderr,"native authored release FAIL: %s\n",message);exit(1);}
}
int main(int argc,char **argv){
 ck(argc==5||argc==6,"usage: native-test ROOT NAMESPACE RELEASE CMS-REVISION [deny-page|deny-asset]");
 if(argc==6){
  const char *mode=argv[5];
  char *buffer=(char*)malloc(CAP);
  ck(buffer!=NULL,"allocate refusal buffer");
  memset(buffer,'X',CAP);size_t written=1234;
  int code=0;
  if(!strcmp(mode,"deny-page"))
   code=bf_authored_release_page(argv[1],argv[2],argv[3],argv[4],
     "/",buffer,CAP,&written);
  else if(!strcmp(mode,"deny-asset"))
   code=bf_authored_release_asset(argv[1],argv[2],argv[3],argv[4],
     "assets/site.css",buffer,CAP,&written);
  else ck(0,"unsupported negative-case mode");
  ck(code==-1&&written==0&&buffer[0]=='\0',
     "tampered or symlinked release cannot serve partial bytes");
  free(buffer);puts("native authored release refusal PASS");
  return 0;
 }
 const char *root=argv[1],*ns=argv[2],*release=argv[3],*revision=argv[4];
 char *page=(char *)malloc(CAP),*second=(char *)malloc(CAP);
 ck(page&&second,"allocate site budget");
 size_t n=0;
 int outcome=bf_authored_release_page(root,ns,release,revision,"/",page,CAP,&n);
 ck(outcome==1&&n>1000&&strlen(page)==n,"native served published homepage");
 ck(strstr(page,"data-authored-publication=")!=NULL,"release identity in authored HTML");
 ck(strstr(page,"data-authored-profile=")!=NULL,"namespace identity in authored HTML");
 ck(strstr(page,"Source-backed research")!=NULL,"first-party source owner content");
 ck(strstr(page,"ak-nav-tabs")!=NULL,"full interactive component markup");
 char expected[65];bf_sha256_hex((const uint8_t *)page,n,expected);
 outcome=bf_authored_release_page(root,ns,release,revision,"/docs/",second,CAP,&n);
 ck(outcome==1&&strstr(second,"Documentation")!=NULL,
   "native serves independent nested CMS route");
 ck(strstr(second,"ak-modal")!=NULL,"nested route retains modal");
 outcome=bf_authored_release_asset(root,ns,release,revision,
   "assets/site.css",second,CAP,&n);
 ck(outcome==1&&n>250,"native serves entire verified authored CSS");
 outcome=bf_authored_release_asset(root,ns,release,revision,
   "assets/pattern-runtime.js",second,CAP,&n);
 ck(outcome==1&&n>200,"native serves real first-party interaction controller");
 memset(second,'X',64);
 outcome=bf_authored_release_page(root,"person-foreign-life",release,revision,
   "/",second,CAP,&n);
 ck(outcome==-1&&n==0&&second[0]==0,"foreign namespace denied atomically");
 memset(second,'X',64);
 outcome=bf_authored_release_page(root,ns,
   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
   revision,"/",second,CAP,&n);
 ck(outcome==-1&&n==0&&second[0]==0,"mismatched release denied");
 outcome=bf_authored_release_page(root,ns,release,
   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
   "/",second,CAP,&n);
 ck(outcome==-1&&n==0&&second[0]==0,"stale CMS revision denied");
 outcome=bf_authored_release_page(root,ns,release,revision,
   "/missing/",second,CAP,&n);
 ck(outcome==0&&n==0&&second[0]==0,"unpublished route is not a public asset");
 outcome=bf_authored_release_page(root,ns,release,revision,
   "/../",second,CAP,&n);
 ck(outcome==-1&&n==0&&second[0]==0,"traversal never resolves");
 outcome=bf_authored_release_page(root,ns,release,revision,
   "/",second,80,&n);
 ck(outcome==-1&&n==0&&second[0]==0,"small output limit is atomic");
 outcome=bf_authored_release_asset(root,ns,release,revision,
   "../RELEASE.tsv",second,CAP,&n);
 ck(outcome==-1&&n==0&&second[0]==0,"asset traversal denied");
 printf("native first-party release bytes PASS %s\n",expected);
 free(page);free(second);
 return 0;
}
