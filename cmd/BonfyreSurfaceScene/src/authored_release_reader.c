/* Bonfÿre native serving of compiled first-party ServiceSite/authoring output.
 * The 27 section owners compile only once at publication. C verifies and
 * serves the exact HTML and packaged clients without hand-coded duplicates.
 * These APIs do NOT grant publication: the caller supplies native-attested
 * account, release and aggregate revision. */
#define _GNU_SOURCE
#include "authored_release_reader.h"
#include "bonfyre.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif
#define BF_RELEASE_PAGE_MAX (1048576u)
#define BF_RELEASE_ASSET_MAX (16u*1024u*1024u)
#define BF_RELEASE_MANIFEST_MAX (512u*1024u)
static int hex_ok(const char *s){
 if(!s||strlen(s)!=64)return 0;
 for(size_t i=0;i<64;i++)if(!((s[i]>='0'&&s[i]<='9')||
   (s[i]>='a'&&s[i]<='f')))return 0;
 return 1;
}
static int id_ok(const char *s){
 const size_t n=s?strlen(s):0;
 if(n<14||n>95||strncmp(s,"person-",7))return 0;
 for(size_t i=7;i<n;i++)if(!((s[i]>='a'&&s[i]<='z')||
   (s[i]>='0'&&s[i]<='9')||s[i]=='-'))return 0;
 const char *suffixes[]={"-work","-life","-live","-shop"};
 for(size_t i=0;i<4;i++){size_t k=strlen(suffixes[i]);
  if(n>7+k&&!strcmp(s+n-k,suffixes[i]))return 1;
 }return 0;
}
static int member_ok(const char *path){
 const size_t n=path?strlen(path):0;
 if(!n||n>255||path[0]=='/'||strstr(path,"//"))return 0;
 const char *p=path;
 for(size_t i=0;i<n;i++)
  if(!isalnum((unsigned char)p[i])&&p[i]!='/'&&p[i]!='-'&&
     p[i]!='_'&&p[i]!='.')return 0;
 if(!strcmp(path,".")||strstr(path,"/../")||strstr(path,"/./")||
    !strncmp(path,"../",3)||!strncmp(path,"./",2)||
    (n>=3&&!strcmp(path+n-3,"/.."))||
    (n>=2&&!strcmp(path+n-2,"/.")))return 0;
 return 1;
}
static int route_ok(const char *route){
 size_t n=route?strlen(route):0;
 if(!n||n>127||route[0]!='/'||route[n-1]!='/')return 0;
 if(n==1)return 1;
 for(size_t i=1;i<n;i++)if(!((route[i]>='a'&&route[i]<='z')||
    (route[i]>='0'&&route[i]<='9')||route[i]=='-'||route[i]=='/'))return 0;
 if(strstr(route,"//"))return 0;
 return 1;
}
/* Descriptor-relative walk forbids symlinks in EVERY path component. A
 * release file changed during a read fails the content-hash gate. */
static char *read_member(const char *root,const char *path,size_t maximum,size_t *count){
 *count=0;if(!root||!member_ok(path)||strlen(root)>2000)return NULL;
 int dir=open(root,O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
 if(dir<0)return NULL;
 char relative[256];strcpy(relative,path);
 char *cursor=relative,*slash=NULL;
 while((slash=strchr(cursor,'/'))!=NULL){
  *slash='\0';int next=openat(dir,cursor,
   O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
  close(dir);if(next<0)return NULL;
  dir=next;cursor=slash+1;
 }
 int file=openat(dir,cursor,O_RDONLY|O_NOFOLLOW|O_CLOEXEC);
 close(dir);
 if(file<0)return NULL;
 struct stat st;
 if(fstat(file,&st)!=0||!S_ISREG(st.st_mode)||st.st_size<0||
   (uint64_t)st.st_size>maximum){close(file);return NULL;}
 const size_t n=(size_t)st.st_size;
 char *data=(char *)malloc(n+1);
 if(!data){close(file);return NULL;}
 size_t used=0;
 while(used<n){ssize_t got=read(file,data+used,n-used);
  if(got<=0){free(data);close(file);return NULL;}
  used+=(size_t)got;
 }
 char extra;
 if(read(file,&extra,1)!=0){free(data);close(file);return NULL;}
 if(close(file)!=0){free(data);return NULL;}
 data[n]='\0';*count=n;return data;
}
static int verified_release(const char *root,const char *namespace_id,
 const char *release_id,const char *cms_revision){
 if(!id_ok(namespace_id)||!hex_ok(release_id)||!hex_ok(cms_revision))return 0;
 size_t size=0;char *data=read_member(root,"RELEASE.tsv",BF_RELEASE_MANIFEST_MAX,&size);
 if(!data)return 0;
 const char *names[]={"schema","publication","account_namespace","release_id",
  "cms_revision","pages"};
 const char *expected[]={"bonfyre.authored-site-release.v1","published",
  namespace_id,release_id,cms_revision,NULL};
 int found[6]={0},rows=0,ok=1;
 char *context=NULL;
 for(char *line=strtok_r(data,"\n",&context);line&&ok;
     line=strtok_r(NULL,"\n",&context)){
  char *tab=strchr(line,'\t');
  if(!tab||strchr(tab+1,'\t')){ok=0;break;}
  *tab++='\0';int key=-1;
  for(int i=0;i<6;i++)if(!strcmp(line,names[i]))key=i;
  if(key<0||found[key]++){ok=0;break;}
  if(key==5){char *end=NULL;long pages=strtol(tab,&end,10);
   if(!end||*end||pages<1||pages>512)ok=0;
  }else if(strcmp(tab,expected[key]))ok=0;
  rows++;
 }
 for(int i=0;i<6;i++)if(!found[i])ok=0;
 free(data);return ok&&rows==6;
}
static int manifest_digest(const char *root,const char *path,char expected[65]){
 size_t len=0;char *manifest=read_member(root,"FILES.tsv",BF_RELEASE_MANIFEST_MAX,&len);
 if(!manifest)return -1;
 int found=0,ok=1,rows=0;expected[0]=0;
 char *context=NULL,*line=strtok_r(manifest,"\n",&context);
 if(!line||strcmp(line,"path\tsha256"))ok=0;
 while(ok&&(line=strtok_r(NULL,"\n",&context))){
  if(++rows>12000){ok=0;break;}
  char *tab=strchr(line,'\t');
  if(!tab||strchr(tab+1,'\t')){ok=0;break;}
  *tab++='\0';
  if(!member_ok(line)||!hex_ok(tab)||strncmp(line,"site/",5)){
   ok=0;break;
  }
  if(!strcmp(line,path)){
   if(found++){ok=0;break;}
   strcpy(expected,tab);
  }
 }
 free(manifest);
 if(!ok)return -1;
 return found?1:0;
}
static int page_digest(const char *root,const char *route,char expected[65]){
 size_t len=0;char *manifest=read_member(root,"SITE-PAGES.tsv",BF_RELEASE_MANIFEST_MAX,&len);
 if(!manifest)return -1;
 int found=0,ok=1;expected[0]='\0';
 char *context=NULL,*line=strtok_r(manifest,"\n",&context);
 if(!line||strcmp(line,"route\ttitle\tnative_revision\thtml_sha256"))ok=0;
 while(ok&&(line=strtok_r(NULL,"\n",&context))){
  char *one=strchr(line,'\t');if(!one){ok=0;break;}*one++='\0';
  char *two=strchr(one,'\t');if(!two){ok=0;break;}*two++='\0';
  char *three=strchr(two,'\t');if(!three){ok=0;break;}*three++='\0';
  if(!route_ok(line)||!*one||!hex_ok(two)||!hex_ok(three)||
      strchr(three,'\t')){ok=0;break;}
  if(!strcmp(line,route)){
   if(found++){ok=0;break;}strcpy(expected,three);
  }
 }
 free(manifest);if(!ok)return -1;return found?1:0;
}
static int project(const char *root,const char *namespace_id,
 const char *release_id,const char *cms_revision,const char *member,
 const char *route,char *output,size_t cap,size_t *written){
 if(written)*written=0;
 if(output&&cap)output[0]='\0';
 if(!output||cap<2||cap>BF_RELEASE_ASSET_MAX+1||
  !member_ok(member)||!verified_release(root,namespace_id,release_id,cms_revision))
  return -1;
 char stored[65]={0},page_hash[65]={0};
 const int present=manifest_digest(root,member,stored);
 if(present<=0)return present;
 if(route){
  const int found=page_digest(root,route,page_hash);
  if(found<=0)return found;
  if(strcmp(stored,page_hash))return -1;
 }
 size_t size=0;char *content=read_member(root,member,
   route?BF_RELEASE_PAGE_MAX:BF_RELEASE_ASSET_MAX,&size);
 if(!content)return -1;
 char actual[65];bf_sha256_hex((const uint8_t *)content,size,actual);
 int ok=!strcmp(actual,stored)&&size<cap;
 if(ok&&route){
  char ns[140],release[150];
  snprintf(ns,sizeof(ns),"data-authored-profile=\"%s\"",namespace_id);
  snprintf(release,sizeof(release),"data-authored-publication=\"%s\"",release_id);
  if(!strstr(content,ns)||!strstr(content,release)||
     !strstr(content,"<html")||
     strstr(content,"content=\"noindex"))ok=0;
 }
 if(ok){
  memcpy(output,content,size);
  output[size]='\0';
  if(written)*written=size;
 }
 free(content);return ok?1:-1;
}
int bf_authored_release_page(const char *root,const char *namespace_id,
 const char *release_id,const char *cms_revision,const char *route,
 char *output,size_t cap,size_t *written){
 if(!route_ok(route)){
  if(written)*written=0;
  if(output&&cap)output[0]='\0';
  return -1;
 }
 char filename[260];
 const int n=snprintf(filename,sizeof(filename),"site%sindex.html",route);
 if(n<0||(size_t)n>=sizeof(filename))return -1;
 return project(root,namespace_id,release_id,cms_revision,
  filename,route,output,cap,written);
}
int bf_authored_release_asset(const char *root,const char *namespace_id,
 const char *release_id,const char *cms_revision,const char *asset_path,
 char *output,size_t cap,size_t *written){
 if(!asset_path||!member_ok(asset_path)||
  (strncmp(asset_path,"assets/",7)&&strncmp(asset_path,"data/",5))){
  if(written)*written=0;
  if(output&&cap)output[0]='\0';
  return -1;
 }
 char filename[261];
 int n=snprintf(filename,sizeof(filename),"site/%s",asset_path);
 if(n<0||(size_t)n>=sizeof(filename))return -1;
 return project(root,namespace_id,release_id,cms_revision,
  filename,NULL,output,cap,written);
}
