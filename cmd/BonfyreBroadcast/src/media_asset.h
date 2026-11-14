/* Stage locally produced broadcast bytes without claiming they came from a
 * foreign observation. Only reviewed MediaProgram records can expose these
 * content-addressed assets through WebAddress; staging stays a draft action.
 *
 * The root is an explicitly configured, operator-owned directory. Filenames
 * are SHA-256 identities, never sourced from untrusted page or program names. */
#ifndef BONFYRE_BROADCAST_MEDIA_ASSET_H
#define BONFYRE_BROADCAST_MEDIA_ASSET_H
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
static int bf_media_asset_stage(const char *root,const char *source,const char *ext,
                                char digest[65],char *href,size_t href_cap) {
    char canonical[PATH_MAX],target[PATH_MAX],tmp[PATH_MAX],check[65]={0};
    int input=-1,output=-1,ok=0;
    struct stat st={0};
    tmp[0]=0;
    if(!root||!*root||!source||!*source||
       (strcmp(ext,"m4a")&&strcmp(ext,"vtt"))||!realpath(root,canonical)||
       stat(canonical,&st)||!S_ISDIR(st.st_mode)||
       lstat(source,&st)||!S_ISREG(st.st_mode)||st.st_size<=0||
       st.st_size>128LL*1024*1024)return 0;
    if(bf_sha256_file(source,digest)!=0)return 0;
    if(snprintf(target,sizeof(target),"%s/%s.%s",canonical,digest,ext)>=(int)sizeof(target)||
       snprintf(href,href_cap,"/assets/twin/%s.%s",digest,ext)>=(int)href_cap)return 0;
    if(lstat(target,&st)==0){
        if(!S_ISREG(st.st_mode)||bf_sha256_file(target,check)!=0||
           strcmp(check,digest))return 0;
        return 1;
    }
    if(errno!=ENOENT)return 0;
    if(snprintf(tmp,sizeof(tmp),"%s/.broadcast-staging-XXXXXX",canonical)>=(int)sizeof(tmp))return 0;
    input=open(source,O_RDONLY|O_NOFOLLOW);
    if(input<0)return 0;
    if(fstat(input,&st)||!S_ISREG(st.st_mode)||st.st_size<=0||
       st.st_size>128LL*1024*1024)goto done;
    output=mkstemp(tmp);
    if(output<0)goto done;
    unsigned char buffer[65536];
    ssize_t bytes;
    while((bytes=read(input,buffer,sizeof(buffer)))>0){
        for(ssize_t pos=0;pos<bytes;){
            ssize_t written=write(output,buffer+pos,(size_t)(bytes-pos));
            if(written<=0)goto done;
            pos+=written;
        }
    }
    if(bytes<0||fchmod(output,0644)||fsync(output))goto done;
    close(output);output=-1;
    if(bf_sha256_file(tmp,check)!=0||strcmp(check,digest))goto done;
    if(link(tmp,target)!=0){
        if(errno!=EEXIST||lstat(target,&st)||!S_ISREG(st.st_mode)||
           bf_sha256_file(target,check)!=0||strcmp(check,digest))goto done;
    }
    ok=1;
done:
    if(output>=0)close(output);
    if(input>=0)close(input);
    if(tmp[0])unlink(tmp);
    if(!ok)href[0]=0;
    return ok;
}
#endif
