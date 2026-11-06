#ifndef BF_STATIC_ASSET_CLOSURE_H
#define BF_STATIC_ASSET_CLOSURE_H
#include <dirent.h>
/* Public static dependency export, shared by the native publisher and tests.
 * The callback is the existing public asset owner. No account record, bearer,
 * private response or arbitrary file is an exportable dependency. */
#define SAC_MAX 4096
#define SAC_PATH 1024
#define SAC_FILE (16u * 1024u * 1024u)
typedef unsigned char *(*SacLoad)(void *,const char *,size_t *);
typedef struct { char path[SAC_PATH]; unsigned char *body; size_t size; } SacAsset;
typedef struct { SacAsset *asset; size_t count,total,pages; const char *out; SacLoad load; void *owner; } Sac;
static int sac_static(const char *p) {
    const char *ext=strrchr(p,'.');
    static const char *const types[]={".mjs",".js",".css",".png",".jpg",".jpeg",".svg",".webp",".woff",".woff2",".wasm",".ico"};
    for(size_t i=0;ext&&i<sizeof(types)/sizeof(types[0]);i++)if(!strcmp(ext,types[i]))return 1;
    return 0;
}
static int sac_route(const char *base,const char *value,char out[SAC_PATH]) {
    char joined[SAC_PATH],copy[SAC_PATH],*parts[128],*save=NULL;size_t n=0;
    if(value[0]=='/'&&value[1]!='/') {if(strlen(value)>=sizeof(joined))return -1;strcpy(joined,value);}
    else if(base&&(!strncmp(value,"./",2)||!strncmp(value,"../",3))){
        const char *slash=strrchr(base,'/');if(!slash)return -1;
        if(snprintf(joined,sizeof(joined),"%.*s/%s",(int)(slash-base),base,value)>=(int)sizeof(joined))return -1;
    }else return 0;
    joined[strcspn(joined,"?#")]=0;
    if(!sac_static(joined))return 0;
    if(strspn(joined,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./-")!=strlen(joined))return -1;
    strcpy(copy,joined);
    for(char *p=strtok_r(copy,"/",&save);p;p=strtok_r(NULL,"/",&save)){
        if(!strcmp(p,"."))continue;
        if(!strcmp(p,"..")){if(n<=1)return -1;n--;continue;}
        if(n==128)return -1;parts[n++]=p;
    }
    if(!n|| (strcmp(parts[0],"apps")&&strcmp(parts[0],"assets")))return 0;
    size_t used=0;for(size_t i=0;i<n;i++){int k=snprintf(out+used,SAC_PATH-used,"/%s",parts[i]);if(k<0||(size_t)k>=SAC_PATH-used)return -1;used+=(size_t)k;}
    return 1;
}
static int sac_add(Sac *s,const char *base,const char *value) {
    char path[SAC_PATH];int valid=sac_route(base,value,path);
    if(valid<=0)return valid==0;
    for(size_t i=0;i<s->count;i++)if(!strcmp(s->asset[i].path,path))return 1;
    if(s->count==SAC_MAX)return 0;
    strcpy(s->asset[s->count++].path,path);return 1;
}
static int sac_scan(Sac *s,const char *base,const unsigned char *body,size_t size) {
    for(size_t i=0;i<size;i++){
        if((body[i]!='\''&&body[i]!='"')||i+1==size||(body[i+1]!='/'&&body[i+1]!='.'))continue;
        unsigned char quote=body[i];size_t start=++i;
        while(i<size&&body[i]!=quote){if(body[i]=='\\'&&i+1<size)i++;i++;}
        if(i==size)break;
        size_t n=i-start;if(!n||n>=SAC_PATH)continue;
        char value[SAC_PATH];memcpy(value,body+start,n);value[n]=0;
        if(!sac_add(s,base,value))return 0;
    }
    return 1;
}
static int sac_pages(Sac *s,int fd,unsigned depth) {
    if(depth>32)return 0;
    DIR *dir=fdopendir(dup(fd));if(!dir)return 0;int ok=1;struct dirent *entry;
    while(ok&&(entry=readdir(dir))){
        if(!strcmp(entry->d_name,".")||!strcmp(entry->d_name,".."))continue;
        struct stat st;if(fstatat(fd,entry->d_name,&st,AT_SYMLINK_NOFOLLOW)){ok=0;break;}
        if(S_ISLNK(st.st_mode)){ok=0;break;}
        if(S_ISDIR(st.st_mode)){int child=openat(fd,entry->d_name,O_RDONLY|O_DIRECTORY|O_NOFOLLOW);ok=child>=0&&sac_pages(s,child,depth+1);if(child>=0)close(child);}
        else if(S_ISREG(st.st_mode)){
            size_t len=strlen(entry->d_name);if(len<5||strcmp(entry->d_name+len-5,".html"))continue;
            size_t bytes=0;unsigned char *body=aw_fd_file(fd,entry->d_name,SAC_FILE,&bytes);
            ok=body&&sac_scan(s,NULL,body,bytes);free(body);s->pages++;
        }else ok=0;
    }
    closedir(dir);return ok;
}
static int sac_write(int root,const SacAsset *asset) {
    char path[SAC_PATH],tmp[96];strcpy(path,asset->path+1);int dir=dup(root);if(dir<0)return 0;
    char *p=path,*slash;
    while((slash=strchr(p,'/'))){*slash=0;if(mkdirat(dir,p,0755)&&errno!=EEXIST){close(dir);return 0;}
        int next=openat(dir,p,O_RDONLY|O_DIRECTORY|O_NOFOLLOW);close(dir);if(next<0)return 0;dir=next;p=slash+1;}
    struct stat st;if(!fstatat(dir,p,&st,AT_SYMLINK_NOFOLLOW)&&!S_ISREG(st.st_mode)){close(dir);return 0;}
    snprintf(tmp,sizeof(tmp),".static-asset-%ld.tmp",(long)getpid());
    int fd=openat(dir,tmp,O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0600);if(fd<0){close(dir);return 0;}
    size_t n=0;int ok=1;while(n<asset->size){ssize_t wrote=write(fd,asset->body+n,asset->size-n);if(wrote<0&&errno==EINTR)continue;if(wrote<=0){ok=0;break;}n+=(size_t)wrote;}
    if(fchmod(fd,0644))ok=0;if(close(fd))ok=0;
    if(ok&&renameat(dir,tmp,dir,p))ok=0;if(!ok)unlinkat(dir,tmp,0);close(dir);return ok;
}
static int sac_export(const char *out,SacLoad load,void *owner) {
    Sac s={.out=out,.load=load,.owner=owner};int root=-1,pages=-1,ok=0;
    if(!out||!*out||!load||(root=open(out,O_RDONLY|O_DIRECTORY|O_NOFOLLOW))<0)goto done;
    if((pages=openat(root,"h",O_RDONLY|O_DIRECTORY|O_NOFOLLOW))<0)goto done;
    s.asset=calloc(SAC_MAX,sizeof(*s.asset));if(!s.asset||!sac_pages(&s,pages,0))goto done;
    for(size_t i=0;i<s.count;i++){
        SacAsset *a=&s.asset[i];a->body=load(owner,a->path,&a->size);
        if(!a->body||a->size>SAC_FILE||s.total>96u*1024u*1024u-a->size){fprintf(stderr,"public asset unavailable: %s\n",a->path);goto done;}
        s.total+=a->size;const char *ext=strrchr(a->path,'.');
        if(ext&&(!strcmp(ext,".mjs")||!strcmp(ext,".js")||!strcmp(ext,".css"))&&!sac_scan(&s,a->path,a->body,a->size))goto done;
    }
    /* Resolve the entire closure before writing any member. The caller stages
     * a bundle; only its normal publish owner may replace a live release. */
    for(size_t i=0;i<s.count;i++)if(!sac_write(root,&s.asset[i]))goto done;
    printf("PUBLIC_ASSETS pages=%zu files=%zu bytes=%zu\n",s.pages,s.count,s.total);ok=1;
 done:
    if(s.asset){for(size_t i=0;i<s.count;i++)free(s.asset[i].body);free(s.asset);}
    if(pages>=0)close(pages);if(root>=0)close(root);
    return ok?0:2;
}
#endif
