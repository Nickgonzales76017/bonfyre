#ifndef BONFYRE_WORKSPACE_PACKAGE_H
#define BONFYRE_WORKSPACE_PACKAGE_H
/* Physical realization of a reviewed workspace manifest. Auth and Pack still
 * decide access; a body digest by itself is never an addressable workspace. */
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>

#define AW_FILE (16u * 1024u * 1024u)
#define AW_MANIFEST (256u * 1024u)
#define AW_TOTAL (96u * 1024u * 1024u)
#define AW_MEMBERS 1000u
#define AW_STORAGE "layout\tshared-bodies-v1\n"
/* The existing libbonfyre implementation, not a second hash owner. */
void bf_sha256_hex(const uint8_t *data,size_t len,char hex[65]);
static int aw_hex(const char *s,size_t n) {return s&&strlen(s)==n&&strspn(s,"abcdef0123456789")==n;}
static int aw_path(const char *s) {
    if(!s||!*s||strlen(s)>1000||*s=='/'||strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./-")!=strlen(s))return 0;
    for(const char *p=s;*p;){const char *end=strchr(p,'/');size_t n=end?(size_t)(end-p):strlen(p);if(!n||(n==1&&p[0]=='.')||(n==2&&p[0]=='.'&&p[1]=='.'))return 0;if(!end)break;p=end+1;}
    return 1;
}
/* Walk below a caller-owned directory descriptor. No symlink member or path
 * component is followed, and non-regular final members never block on open. */
static int aw_openat(int root,const char *path,int directory) {
    char name[1001];if(!aw_path(path)){errno=EINVAL;return -1;}
    memcpy(name,path,strlen(path)+1);int current=dup(root);if(current<0)return -1;
    char *p=name,*slash;
    while((slash=strchr(p,'/'))){*slash=0;int next=openat(current,p,O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);int error=errno;close(current);if(next<0){errno=error;return -1;}current=next;p=slash+1;}
    int fd=openat(current,p,O_RDONLY|O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK|(directory?O_DIRECTORY:0));int error=errno;close(current);errno=error;return fd;
}
static unsigned char *aw_fd_file(int root,const char *path,size_t limit,size_t *size) {
    struct stat before,after;unsigned char *body=NULL;*size=0;
    int fd=aw_openat(root,path,0);if(fd<0)return NULL;
    if(fstat(fd,&before)||!S_ISREG(before.st_mode)||before.st_size<0||(uint64_t)before.st_size>limit){errno=EINVAL;goto done;}
    body=malloc((size_t)before.st_size+1);if(!body)goto done;
    size_t at=0,want=(size_t)before.st_size;
    while(at<want){ssize_t n=read(fd,body+at,want-at);if(n<0&&errno==EINTR)continue;if(n<=0){free(body);body=NULL;errno=EIO;goto done;}at+=(size_t)n;}
    if(fstat(fd,&after)||after.st_size!=before.st_size){free(body);body=NULL;errno=EIO;goto done;}
    body[want]=0;*size=want;
 done:{int error=errno;close(fd);errno=error;return body;}
}
static unsigned char *aw_read_file(const char *root,const char *path,size_t limit,size_t *size) {
    *size=0;if(!root){errno=EINVAL;return NULL;}
    int fd=open(root,O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);if(fd<0)return NULL;
    unsigned char *body=aw_fd_file(fd,path,limit,size);int error=errno;close(fd);errno=error;return body;
}
typedef struct {char *path,*hash;size_t bytes;} AwMember;
static int aw_by_path(const void *a,const void *b) {return strcmp(((const AwMember *)a)->path,((const AwMember *)b)->path);}
static int aw_by_hash(const void *a,const void *b) {return strcmp(((const AwMember *)a)->hash,((const AwMember *)b)->hash);}
/* Validate the whole manifest, including entries after the requested member.
 * A valid digest authenticates bytes, not their grammar or unambiguous shape. */
static int aw_member(unsigned char *manifest,size_t size,const char *path,char expected[65],size_t *want) {
    const char *header="path\tsha256\tbytes\n";size_t h=strlen(header),total=0,count=0;int found=0,ok=0;
    if(size<=h||memcmp(manifest,header,h)||manifest[size-1]!='\n'||memchr(manifest,0,size))return 0;
    AwMember *rows=calloc(AW_MEMBERS,sizeof(*rows));if(!rows)return 0;
    char *p=(char *)manifest+h,*end=(char *)manifest+size;
    while(p<end){
        if(count>=AW_MEMBERS)goto done;
        char *next=strchr(p,'\n');if(!next)goto done;*next=0;
        char *sha=strchr(p,'\t');if(!sha)goto done;*sha++=0;
        char *raw=strchr(sha,'\t');if(!raw)goto done;*raw++=0;
        if(!aw_path(p)||!strcmp(p,"MANIFEST.tsv")||!strcmp(p,"STORAGE.tsv")||!aw_hex(sha,64)||!*raw||strspn(raw,"0123456789")!=strlen(raw))goto done;
        errno=0;char *finish=NULL;unsigned long long n=strtoull(raw,&finish,10);
        if(errno||!finish||*finish||n>AW_FILE||total>AW_TOTAL-(size_t)n)goto done;
        total+=(size_t)n;rows[count++]=(AwMember){p,sha,(size_t)n};
        if(!strcmp(p,path)){found++;*want=(size_t)n;memcpy(expected,sha,65);}
        p=next+1;
    }
    if(found!=1)goto done;
    qsort(rows,count,sizeof(*rows),aw_by_path);
    for(size_t i=1;i<count;i++)if(!strcmp(rows[i-1].path,rows[i].path))goto done;
    for(size_t i=0;i<count;i++){char prefix[1001];memcpy(prefix,rows[i].path,strlen(rows[i].path)+1);
        for(char *slash=prefix;(slash=strchr(slash,'/'));slash++){*slash=0;AwMember key={prefix,NULL,0};
            int conflict=bsearch(&key,rows,count,sizeof(*rows),aw_by_path)!=NULL;*slash='/';if(conflict)goto done;}}
    qsort(rows,count,sizeof(*rows),aw_by_hash);
    for(size_t i=1;i<count;i++)if(!strcmp(rows[i-1].hash,rows[i].hash)&&rows[i-1].bytes!=rows[i].bytes)goto done;
    ok=1;
 done:free(rows);return ok;
}
static unsigned char *aw_package(const char *release,const char *path,size_t *size) {
    *size=0;if(!aw_hex(release,64)||!aw_path(path))return NULL;
    const char *base=getenv("BONFYRE_WORKSPACE_PACKAGES"),*home=getenv("HOME");char fallback[PATH_MAX];
    if(!base){int n=snprintf(fallback,sizeof(fallback),"%s/.bonfyre/workspace-packages",home?home:"/var/lib/bonfyre");if(n<0||(size_t)n>=sizeof(fallback))return NULL;base=fallback;}
    int root=open(base,O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC),dir=-1;unsigned char *manifest=NULL,*layout=NULL,*body=NULL;
    size_t manifest_size=0,layout_size=0,want=0;char actual[65],expected[65],coordinate[100];
    if(root<0)return NULL;
    dir=aw_openat(root,release,1);if(dir<0)goto done;
    manifest=aw_fd_file(dir,"MANIFEST.tsv",AW_MANIFEST,&manifest_size);if(!manifest)goto done;
    bf_sha256_hex(manifest,manifest_size,actual);
    if(strcmp(actual,release)||!aw_member(manifest,manifest_size,path,expected,&want))goto done;
    layout=aw_fd_file(dir,"STORAGE.tsv",128,&layout_size);
    if(layout){
        if(layout_size!=strlen(AW_STORAGE)||memcmp(layout,AW_STORAGE,layout_size))goto done;
        int n=snprintf(coordinate,sizeof(coordinate),"_bodies/sha256/%.2s/%s",expected,expected);
        if(n<0||(size_t)n>=sizeof(coordinate))goto done;
        body=aw_fd_file(root,coordinate,AW_FILE,size);
    }else{
        if(errno!=ENOENT)goto done;
        body=aw_fd_file(dir,path,AW_FILE,size);
    }
    if(!body||*size!=want){free(body);body=NULL;goto done;}
    bf_sha256_hex(body,*size,actual);if(strcmp(actual,expected)){free(body);body=NULL;}
 done:free(manifest);free(layout);if(dir>=0)close(dir);close(root);if(!body)*size=0;return body;
}
#endif
