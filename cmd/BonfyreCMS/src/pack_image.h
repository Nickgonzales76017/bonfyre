#ifndef BONFYRE_CMS_PACK_IMAGE_H
#define BONFYRE_CMS_PACK_IMAGE_H
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/* Compiled PackGraph closure image: the exact bounded, versioned, normalized
 * bytes the resolver already hashes, now retained instead of discarded.
 * BFP1 envelope: magic(4) + big-endian payload size(4) + canonical closure
 * SHA256(32) + existing typed length-prefixed closure bytes. This is shared
 * across recipients and does not change a head, installation or authority. */
static int cpack_hex(const char *h,unsigned char bytes[32]){
    if(!h||strlen(h)!=64)return 0;
    for(int i=0;i<32;i++){
        int x=-1,y=-1;unsigned char a=h[2*i],b=h[2*i+1];
        if(a>='0'&&a<='9')x=a-'0';else if(a>='a'&&a<='f')x=a-'a'+10;
        if(b>='0'&&b<='9')y=b-'0';else if(b>='a'&&b<='f')y=b-'a'+10;
        if(x<0||y<0)return 0;bytes[i]=(unsigned char)((x<<4)|y);
    }
    return 1;
}
static int cpack_image_file(const char *path,const unsigned char *bytes,size_t size,int *existing){
    *existing=0;
    int flags=O_CREAT|O_EXCL|O_WRONLY;
#ifdef O_NOFOLLOW
    flags|=O_NOFOLLOW;
#endif
    int fd=open(path,flags,0600);
    if(fd<0&&errno==EEXIST){
        struct stat lst;
        if(lstat(path,&lst)!=0||!S_ISREG(lst.st_mode))return 0;
        int rdflags=O_RDONLY;
#ifdef O_NOFOLLOW
        rdflags|=O_NOFOLLOW;
#endif
        fd=open(path,rdflags);
        if(fd<0)return 0;
        struct stat st;if(fstat(fd,&st)!=0||!S_ISREG(st.st_mode)||st.st_ino!=lst.st_ino||st.st_dev!=lst.st_dev||st.st_size<0||(uint64_t)st.st_size!=size){close(fd);return 0;}
        unsigned char probe[4096];size_t at=0;
        while(at<size){size_t want=size-at;if(want>sizeof(probe))want=sizeof(probe);
            ssize_t n=read(fd,probe,want);if(n<=0||memcmp(bytes+at,probe,(size_t)n)){close(fd);return 0;}at+=(size_t)n;}
        close(fd);*existing=1;return 1;
    }
    if(fd<0)return 0;
    size_t at=0;while(at<size){ssize_t n=write(fd,bytes+at,size-at);
        if(n<=0){close(fd);unlink(path);return 0;}at+=(size_t)n;}
    int ok=fsync(fd)==0;close(fd);if(!ok)unlink(path);
    return ok;
}
/* Only a published source demand makes a repository cut part of this pack.
 * The source cut's identity is already present in the fingerprinted closure. */
static int cpack_source_cut_bound(sqlite3 *db,const BfPackResolution *p,const char *cut){
    if(!cut||!*cut)return 1;
    unsigned char checked[32];if(!cpack_hex(cut,checked))return 0;
    char selector[96];snprintf(selector,sizeof(selector),"repository-cut:%s",cut);
    sqlite3_stmt *q=NULL;
    if(sqlite3_prepare_v2(db,
        "SELECT 1 FROM pack_demand WHERE namespace='root' AND status='published' "
        "AND pack=?1 AND demand_kind='source' AND preferred_sources=?2 LIMIT 1",
        -1,&q,NULL)!=SQLITE_OK)return 0;
    int ok=0;
    for(size_t i=0;i<p->count;i++){
        sqlite3_reset(q);sqlite3_clear_bindings(q);
        sqlite3_bind_text(q,1,p->nodes[i].pack,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(q,2,selector,-1,SQLITE_STATIC);
        if(sqlite3_step(q)==SQLITE_ROW){ok=1;break;}
    }
    sqlite3_finalize(q);return ok;
}
/* Read the compiled image through the canonical CMS owner. A stale snapshot,
 * altered byte, symlink, or changed definition must never become an installer
 * input merely because it still has a filename ending in .bfp. */
static int cpack_verify_image(sqlite3 *db,const char *pack,const char *version,const char *path,const char *cut){
    if(!pack||!bfpg_token(pack,96)||!version||(version[0]&&!bfpg_token(version,64))||
       !path||*path!='/'||strlen(path)>1600||strchr(path,'\n')){
        fputs("error\tinvalid_pack_image_coordinate\n",stdout);return 2;
    }
    struct stat seen,actual;
    if(lstat(path,&seen)!=0||!S_ISREG(seen.st_mode)||seen.st_size<40||
       (uint64_t)seen.st_size>BFPG_BYTES+40){fputs("error\timage_unavailable\n",stdout);return 1;}
    int flags=O_RDONLY;
#ifdef O_NOFOLLOW
    flags|=O_NOFOLLOW;
#endif
    int fd=open(path,flags);
    if(fd<0||fstat(fd,&actual)!=0||actual.st_dev!=seen.st_dev||
       actual.st_ino!=seen.st_ino||!S_ISREG(actual.st_mode)||actual.st_size!=seen.st_size){
        if(fd>=0)close(fd);fputs("error\timage_unavailable\n",stdout);return 1;
    }
    size_t bytes=(size_t)actual.st_size;
    unsigned char *image=malloc(bytes);if(!image){close(fd);return 1;}
    size_t pos=0;while(pos<bytes){
        ssize_t n=read(fd,image+pos,bytes-pos);
        if(n<=0){close(fd);free(image);fputs("error\timage_unreadable\n",stdout);return 1;}
        pos+=(size_t)n;
    }
    close(fd);
    uint32_t length=0;for(int i=0;i<4;i++)length=(length<<8)|image[4+i];
    char content_digest[65],image_digest[65];unsigned char claimed_digest[32];
    bf_sha256_hex(image+40,bytes-40,content_digest);
    int good=memcmp(image,"BFP1",4)==0&&length==bytes-40&&
        cpack_hex(content_digest,claimed_digest)&&memcmp(claimed_digest,image+8,32)==0;
    if(!good){free(image);fputs("error\timage_corrupt\n",stdout);return 1;}
    BfPackResolution closure={0};unsigned char *current=NULL;size_t current_size=0;
    good=bfpg_compile(db,pack,version,NULL,&closure,&current,&current_size)&&
        current&&current_size==length&&memcmp(current,image+40,length)==0&&
        !strcmp(closure.digest,content_digest);
    sqlite3_free(current);
    if(!good){free(image);fputs("error\timage_stale_or_unbound\n",stdout);return 1;}
    if(!cpack_source_cut_bound(db,&closure,cut)){free(image);fputs("error\tundeclared_repository_cut\n",stdout);return 1;}
    bf_sha256_hex(image,bytes,image_digest);free(image);
    printf("schema\tbonfyre.pack-image.v1\npack\t%s\nclosure_digest\t%s\n"
           "image_digest\t%s\nimage_bytes\t%zu\nstate\tverified\n",
           pack,closure.digest,image_digest,bytes);
    for(size_t i=0;i<closure.count;i++)
        printf("pin\t%s\t%s\t%s\n",closure.nodes[i].pack,closure.nodes[i].version,closure.nodes[i].root);
    if(cut&&*cut)printf("repository_cut\t%s\n",cut);
    fputs("installed\t0\nactivated\t0\n",stdout);
    return 0;
}
static int cpack_image(sqlite3 *db,const char *pack,const char *version,const char *out_dir,const char *cut){
    BfPackResolution closure={0};unsigned char *raw=NULL,*image=NULL;size_t n=0;
    char digest[65]="",path[2048]="";
    if(!pack||!bfpg_token(pack,96)||!version||(version[0]&&!bfpg_token(version,64))||
       (out_dir&&(*out_dir!='/'||strlen(out_dir)>1600||strchr(out_dir,'\n')))){
        fputs("error\tinvalid_pack_image_coordinate\n",stdout);return 2;
    }
    if(!bfpg_compile(db,pack,version,NULL,&closure,&raw,&n)||!raw||n>BFPG_BYTES||n>UINT32_MAX){
        printf("error\t%s\nsubject\t%s\n",closure.error[0]?closure.error:"closure_unavailable",closure.subject);
        sqlite3_free(raw);return 1;
    }
    if(!cpack_source_cut_bound(db,&closure,cut)){sqlite3_free(raw);fputs("error\tundeclared_repository_cut\n",stdout);return 1;}
    if(n>SIZE_MAX-40){sqlite3_free(raw);return 1;}
    image=malloc(n+40);if(!image){sqlite3_free(raw);return 1;}
    memcpy(image,"BFP1",4);
    uint32_t length=(uint32_t)n;
    for(int i=0;i<4;i++)image[4+i]=(unsigned char)(length>>(24-i*8));
    if(!cpack_hex(closure.digest,image+8)){sqlite3_free(raw);free(image);return 1;}
    memcpy(image+40,raw,n);sqlite3_free(raw);
    bf_sha256_hex(image,n+40,digest);
    int existing=0,ok=1;
    if(out_dir){
        int size=snprintf(path,sizeof(path),"%s/%s.bfp",out_dir,digest);
        if(size<=0||(size_t)size>=sizeof(path)||!cpack_image_file(path,image,n+40,&existing))ok=0;
    }
    free(image);
    if(!ok){fputs("error\timage_store_refused\n",stdout);return 1;}
    printf("schema\tbonfyre.pack-image.v1\npack\t%s\nclosure_digest\t%s\n"
           "image_digest\t%s\nimage_bytes\t%zu\nstate\t%s\n",
           pack,closure.digest,digest,n+40,out_dir?(existing?"existing":"compiled"):"preview");
    for(size_t i=0;i<closure.count;i++)
        printf("pin\t%s\t%s\t%s\n",closure.nodes[i].pack,closure.nodes[i].version,closure.nodes[i].root);
    if(out_dir)printf("image_path\t%s\n",path);
    if(cut&&*cut)printf("repository_cut\t%s\n",cut);
    fputs("installed\t0\nactivated\t0\n",stdout);
    return 0;
}
#endif
