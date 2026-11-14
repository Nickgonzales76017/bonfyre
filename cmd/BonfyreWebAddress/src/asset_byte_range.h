#ifndef BONFYRE_ASSET_BYTE_RANGE_H
#define BONFYRE_ASSET_BYTE_RANGE_H
#include <stdint.h>
#include <string.h>
/* One strict byte interval; malformed/multiple ranges never widen a read. */
static int asset_dec(const char **ptr,uint64_t *value){
    const char *s=*ptr;uint64_t n=0;
    if(*s<'0'||*s>'9')return 0;
    while(*s>='0'&&*s<='9'){
        unsigned d=(unsigned)(*s-'0');
        if(n>(UINT64_MAX-d)/10)return 0;
        n=n*10+d;s++;
    }
    *ptr=s;*value=n;return 1;
}
static int asset_range(const char *request,long len,long *first,long *last){
    const char *p=request?strstr(request,"\nRange:"):NULL;
    if(!p&&request)p=strstr(request,"\nrange:");
    if(!p)return 0;
    p=strchr(p,':')+1;
    while(*p==' '||*p=='\t')p++;
    if(strncmp(p,"bytes=",6))return -1;
    p+=6;uint64_t begin=0,end=0;
    if(*p=='-'){
        p++;
        if(!asset_dec(&p,&end)||end==0)return -1;
        *first=end>=(uint64_t)len?0:len-(long)end;*last=len-1;
    }else{
        if(!asset_dec(&p,&begin)||*p++!='-'||begin>=(uint64_t)len)return -1;
        *first=(long)begin;
        if(*p>='0'&&*p<='9'){
            if(!asset_dec(&p,&end)||end<begin)return -1;
            *last=end>=(uint64_t)len?len-1:(long)end;
        }else *last=len-1;
    }
    while(*p==' '||*p=='\t')p++;
    if(*p&&(strstr(p,"\nRange:")||strstr(p,"\nrange:")))return -1;
    return (*p=='\0'||*p=='\r'||*p=='\n')?1:-1;
}
#endif
