#ifndef BF_PUBLIC_ASSET_EXPORT_H
#define BF_PUBLIC_ASSET_EXPORT_H
#include "static_asset_closure.h"
typedef struct { const char *site; const char *out; } SacWeb;
static unsigned char *sac_web_load(void *owner,const char *route,size_t *size) {
    SacWeb *web=owner;*size=0;
    if(!strncmp(route,"/assets/",8)){
        unsigned char *held=aw_read_file(web->out,route+1,SAC_FILE,size);
        return held?held:aw_read_file(web->site,route+8,SAC_FILE,size);
    }
    if(strncmp(route,"/apps/",6))return NULL;
    FILE *capture=tmpfile();if(!capture)return NULL;
    fflush(stdout);int saved=dup(STDOUT_FILENO);if(saved<0){fclose(capture);return NULL;}
    if(dup2(fileno(capture),STDOUT_FILENO)<0){close(saved);fclose(capture);return NULL;}
    int handled=serve_application_workspace("GET",route,"","","","aurekai.work","","");
    fflush(stdout);int restored=dup2(saved,STDOUT_FILENO);close(saved);
    long length=0;unsigned char *wire=NULL,*body=NULL;
    if(restored<0||!handled||fseek(capture,0,SEEK_END)||(length=ftell(capture))<=0||
       length>(long)SAC_FILE+8192||fseek(capture,0,SEEK_SET))goto done;
    wire=malloc((size_t)length+1);if(!wire||fread(wire,1,(size_t)length,capture)!=(size_t)length)goto done;wire[length]=0;
    char *end=strstr((char *)wire,"\r\n\r\n"),*policy=strstr((char *)wire,"Cache-Control: public");int code=0;
    if(!end||!policy||policy>end||sscanf((char *)wire,"HTTP/1.1 %d",&code)!=1||code!=200)goto done;
    *size=(size_t)length-(size_t)(end+4-(char *)wire);body=malloc(*size+1);
    if(body){memcpy(body,end+4,*size);body[*size]=0;}
 done:free(wire);fclose(capture);return body;
}
static int publish_assets_cli(const char *site,const char *out) {
    if(!site||!*site||!out||!*out){fputs("publish-assets: --site-root and --out required\n",stderr);return 2;}
    SacWeb owner={site,out};return sac_export(out,sac_web_load,&owner);
}
#endif
