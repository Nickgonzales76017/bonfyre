#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 1
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "../src/workspace_package.h"
static int mode,calls;
static int serve_application_workspace(const char *method,const char *target,const char *query,const char *body,const char *raw,const char *host,const char *bearer,const char *auth) {
    assert(!strcmp(method,"GET")&&!*query&&!*body&&!*raw&&!*bearer&&!*auth&&!strcmp(host,"aurekai.work"));calls++;
    const char *text=!strcmp(target,"/apps/a.mjs")?"import './b.mjs';import('./nested/c.mjs');":!strcmp(target,"/apps/b.mjs")?"import './a.mjs';":!strcmp(target,"/apps/nested/c.mjs")?"import '../b.mjs';":NULL;
    if(mode==1&&!strcmp(target,"/apps/b.mjs"))text=NULL;
    printf("HTTP/1.1 %d OK\r\nCache-Control: %s\r\nContent-Length: %zu\r\n\r\n%s",text?200:404,mode==2?"private, no-store":"public, no-cache",text?strlen(text):0,text?text:"");return 1;
}
#include "../src/public_asset_export.h"
static int checks;
static void check(int ok,const char *why){checks++;if(!ok){fprintf(stderr,"asset closure FAIL: %s\n",why);exit(1);}}
static void put(const char *p,const char *text){FILE *f=fopen(p,"w");check(f!=NULL,"fixture open");check(fputs(text,f)>=0&&!fclose(f),"fixture write");}
static void erase(int fd){DIR *d=fdopendir(dup(fd));struct dirent *e;while((e=readdir(d))){if(!strcmp(e->d_name,".")||!strcmp(e->d_name,".."))continue;struct stat st;check(!fstatat(fd,e->d_name,&st,AT_SYMLINK_NOFOLLOW),"cleanup stat");if(S_ISDIR(st.st_mode)){int c=openat(fd,e->d_name,O_RDONLY|O_DIRECTORY|O_NOFOLLOW);erase(c);close(c);unlinkat(fd,e->d_name,AT_REMOVEDIR);}else unlinkat(fd,e->d_name,0);}closedir(d);}
int main(void){
    char temp[]="/tmp/public-assets-XXXXXX";check(mkdtemp(temp)!=NULL,"temporary directory");int saved=open(".",O_RDONLY|O_DIRECTORY);check(saved>=0&&!chdir(temp),"fixture cwd");
    check(!mkdir("site",0700)&&!mkdir("out",0700)&&!mkdir("out/h",0700)&&!mkdir("out/h/example",0700),"fixture directories");
    put("site/style.css","@font-face{src:url('./font.woff2')}");put("site/font.woff2","fixture-font");
    put("out/h/example/index.html","Someone's note.<link href=\"/assets/style.css\"><script src='/apps/a.mjs'></script><a href='/apps/_activity?context=self'>Private</a><img src='https://foreign.test/image.png'>");
    mode=1;check(publish_assets_cli("site","out")!=0,"missing transitive dependency refuses");check(access("out/apps/a.mjs",F_OK)!=0,"no partial write on unresolved closure");
    mode=2;check(publish_assets_cli("site","out")!=0,"private successful response refuses");check(access("out/assets/style.css",F_OK)!=0,"private refusal writes nothing");
    mode=0;calls=0;check(!publish_assets_cli("site","out"),"complete closure exports");check(calls==3,"shared imports and cycles loaded once");
    size_t n=0;unsigned char *p=aw_read_file("out","apps/nested/c.mjs",1024,&n);check(p&&!strcmp((char *)p,"import '../b.mjs';"),"dynamic nested import bytes");free(p);
    p=aw_read_file("out","assets/font.woff2",1024,&n);check(p&&!strcmp((char *)p,"fixture-font"),"CSS dependency bytes");free(p);
    check(access("out/apps/_activity",F_OK)!=0,"account endpoint never exported");check(!publish_assets_cli("site","out"),"replay succeeds");
    char path[SAC_PATH];check(sac_route("/apps/a.mjs","../../escape.js",path)<0,"root traversal refuses");check(sac_route(NULL,"/apps/%2e%2e/escape.js",path)<0,"encoded traversal refuses");
    check(sac_route(NULL,"//foreign.test/a.js",path)==0,"foreign network path skipped");check(sac_route("/apps/nested/a.mjs","../b.mjs?v=1",path)==1&&!strcmp(path,"/apps/b.mjs"),"bounded relative reference resolves");
    check(!symlink("index.html","out/h/example/link.html"),"fixture page symlink");check(publish_assets_cli("site","out")!=0,"page symlink refuses");unlink("out/h/example/link.html");
    unlink("out/apps/b.mjs");check(!symlink("a.mjs","out/apps/b.mjs"),"fixture output symlink");check(publish_assets_cli("site","out")!=0,"output symlink refuses");
    int fd=open(".",O_RDONLY|O_DIRECTORY);erase(fd);close(fd);check(!fchdir(saved),"restore cwd");close(saved);rmdir(temp);
    printf("Public asset closure: %d assertions: PASS\n",checks);return 0;
}
