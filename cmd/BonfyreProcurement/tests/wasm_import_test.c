#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int uleb(const unsigned char *b,size_t n,size_t *at,uint32_t *out){
    uint32_t v=0;unsigned shift=0;
    while(*at<n&&shift<35){unsigned char c=b[(*at)++];v|=(uint32_t)(c&127)<<shift;if(!(c&128)){*out=v;return 1;}shift+=7;}
    return 0;
}
static int skip_limits(const unsigned char *b,size_t n,size_t *at){
    uint32_t flags=0,x=0;if(!uleb(b,n,at,&flags)||!uleb(b,n,at,&x))return 0;
    if(flags&1)if(!uleb(b,n,at,&x))return 0;return 1;
}
static int skip_import_type(const unsigned char *b,size_t n,size_t *at,unsigned char kind){
    uint32_t x=0;
    if(kind==0)return uleb(b,n,at,&x);
    if(kind==1){if(*at>=n)return 0;(*at)++;return skip_limits(b,n,at);}
    if(kind==2)return skip_limits(b,n,at);
    if(kind==3){if(*at+2>n)return 0;*at+=2;return 1;}
    if(kind==4){if(*at>=n)return 0;(*at)++;return uleb(b,n,at,&x);}
    return 0;
}
int main(int argc,char **argv){
    if(argc!=2){fprintf(stderr,"usage: wasm_import_test FILE\n");return 2;}
    FILE *f=fopen(argv[1],"rb");if(!f){perror("open");return 1;}
    if(fseek(f,0,SEEK_END)||ftell(f)<8){fclose(f);return 1;}long z=ftell(f);rewind(f);
    unsigned char *b=malloc((size_t)z);if(!b){fclose(f);return 1;}
    if(fread(b,1,(size_t)z,f)!=(size_t)z){free(b);fclose(f);return 1;}fclose(f);
    if(memcmp(b,"\0asm\1\0\0\0",8)){free(b);fprintf(stderr,"not wasm v1\n");return 1;}
    size_t at=8;unsigned imports=0;int saw_exports=0;
    const char *need[]={"memory","malloc","free","bf_procurement_wasm_run","bf_procurement_wasm_reset",
        "bf_procurement_wasm_output","bf_procurement_wasm_output_len","bf_procurement_wasm_error","bf_procurement_wasm_error_len"};
    const char *allowed[]={"fd_prestat_get","fd_prestat_dir_name","proc_exit","random_get"};
    int found[9]={0},allowed_found[4]={0};
    while(at<(size_t)z){
        unsigned char id=b[at++];uint32_t size=0;if(!uleb(b,(size_t)z,&at,&size)||size>(size_t)z-at){free(b);return 1;}
        size_t end=at+size;
        if(id==2){
            uint32_t count=0;if(!uleb(b,end,&at,&count)){free(b);return 1;}imports=count;
            for(uint32_t i=0;i<count;i++){
                uint32_t ml=0,nl=0;
                if(!uleb(b,end,&at,&ml)||ml>end-at){free(b);return 1;}const unsigned char *module=b+at;at+=ml;
                if(!uleb(b,end,&at,&nl)||nl>end-at){free(b);return 1;}const unsigned char *name=b+at;at+=nl;
                if(at>=end){free(b);return 1;}unsigned char kind=b[at++];
                const char *wasi="wasi_snapshot_preview1";
                if(ml!=strlen(wasi)||memcmp(module,wasi,ml)){fprintf(stderr,"unexpected wasm import module\n");free(b);return 1;}
                int admitted=0;for(size_t k=0;k<4;k++)if(strlen(allowed[k])==nl&&!memcmp(name,allowed[k],nl)){allowed_found[k]=1;admitted=1;}
                if(!admitted){fprintf(stderr,"unexpected WASI import: %.*s\n",(int)nl,name);free(b);return 1;}
                if(!skip_import_type(b,end,&at,kind)){free(b);return 1;}
            }
        }else if(id==7){
            uint32_t count=0;if(!uleb(b,end,&at,&count)){free(b);return 1;}saw_exports=1;
            for(uint32_t i=0;i<count;i++){
                uint32_t len=0,index=0;if(!uleb(b,end,&at,&len)||len>end-at){free(b);return 1;}
                const unsigned char *name=b+at;at+=len;if(at>=end){free(b);return 1;}at++;
                if(!uleb(b,end,&at,&index)){free(b);return 1;}(void)index;
                for(size_t k=0;k<sizeof need/sizeof *need;k++)if(strlen(need[k])==len&&!memcmp(name,need[k],len))found[k]=1;
            }
        }
        at=end;
    }
    if(imports!=4){fprintf(stderr,"browser kernel has %u imports; expected the four bounded WASI imports\n",imports);free(b);return 1;}
    for(size_t i=0;i<4;i++)if(!allowed_found[i]){fprintf(stderr,"missing expected WASI import %s\n",allowed[i]);free(b);return 1;}
    if(!saw_exports){free(b);fprintf(stderr,"missing export section\n");return 1;}
    for(size_t i=0;i<sizeof need/sizeof *need;i++)if(!found[i]){fprintf(stderr,"missing export %s\n",need[i]);free(b);return 1;}
    printf("procurement wasm: %ld bytes, four bounded WASI imports, browser exports verified\n",z);
    free(b);return 0;
}
