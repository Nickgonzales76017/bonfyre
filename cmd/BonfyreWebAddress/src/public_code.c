#include "public_code.h"
#include "../build/public-code/qrcodegen.h"
#include <ctype.h>
#include <stdint.h>
#include <string.h>
#include "ui_tree.h"

static int code_host(const char *host) {
    if(!host||!*host||strlen(host)>253)return 0;
    size_t label=0;
    for(size_t i=0;host[i];i++) {
        unsigned char c=(unsigned char)host[i];
        if(c=='.'){if(!label||host[i-1]=='-')return 0;label=0;}
        else {if(!((c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-')||(!label&&c=='-')||++label>63)return 0;}
    }
    return label&&host[strlen(host)-1]!='-';
}
/* An item page: the emit_item_page prefix row in schemas/control/web-routes.cue,
 * then <namespace>/<24 hex>. A code is only ever made for that shape. */
#define CODE_ITEM_PREFIX "/o/"
static int code_item_path(const char *path) {
    if(!path||strncmp(path,CODE_ITEM_PREFIX,sizeof(CODE_ITEM_PREFIX)-1))return 0;
    const char *slash=strchr(path+3,'/');
    size_t n=slash?(size_t)(slash-(path+3)):0;
    if(!n||n>=128||!slash||strlen(slash+1)!=24||strspn(slash+1,"0123456789abcdef")!=24)return 0;
    return strspn(path+3,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==n;
}
int bf_public_code_url(const char *host,const char *path,char *out,size_t cap) {
    if(!out||!cap)return 0;
    out[0]='\0';
    if(!code_host(host)||!code_item_path(path))return 0;
    size_t n=8+strlen(host)+strlen(path);
    if(n>BF_PUBLIC_CODE_URI_MAX||n>=cap)return 0;
    int written=snprintf(out,cap,"https://%s%s",host,path);
    return written>0&&(size_t)written==n;
}
int bf_public_code_item_path(const char *request,char *item,size_t cap) {
    static const char suffix[]=".qr.svg";
    if(!request||!item||!cap)return 0;
    item[0]='\0';size_t n=strlen(request),tail=sizeof(suffix)-1;
    if(n<=tail||strcmp(request+n-tail,suffix)||n-tail>=cap)return 0;
    memcpy(item,request,n-tail);item[n-tail]='\0';
    if(!code_item_path(item)){item[0]='\0';return 0;}
    return 1;
}
static int code_url_valid(const char *url) {
    if(!url||strncmp(url,"https://",8)||strlen(url)>BF_PUBLIC_CODE_URI_MAX)return 0;
    const char *slash=strchr(url+8,'/');char host[254],checked[BF_PUBLIC_CODE_URI_MAX+1];
    if(!slash||(size_t)(slash-(url+8))>=sizeof(host))return 0;
    memcpy(host,url+8,(size_t)(slash-(url+8)));host[slash-(url+8)]='\0';
    return bf_public_code_url(host,slash,checked,sizeof(checked))&&!strcmp(url,checked);
}
int bf_public_code_emit_svg(FILE *out,const char *url) {
    /* Version 20 is a finite upper bound; ordinary Aurekai item addresses use
     * considerably less. Encoding runs only when this representation is read. */
    uint8_t work[qrcodegen_BUFFER_LEN_FOR_VERSION(20)],qr[qrcodegen_BUFFER_LEN_FOR_VERSION(20)];
    if(!out||!code_url_valid(url)||!qrcodegen_encodeText(url,work,qr,qrcodegen_Ecc_MEDIUM,1,20,qrcodegen_Mask_AUTO,true))return 0;
    int size=qrcodegen_getSize(qr),edge=size+8;
    ui_start(out,"svg");ui_attr(out,"xmlns","http://www.w3.org/2000/svg");
    ui_attr_open(out,"viewBox");fprintf(out,"0 0 %d %d",edge,edge);ui_attr_close(out);
    ui_attr_open(out,"width");fprintf(out,"%d",edge*6);ui_attr_close(out);
    ui_attr_open(out,"height");fprintf(out,"%d",edge*6);ui_attr_close(out);
    ui_attr(out,"role","img");ui_attr(out,"aria-label","QR code for this public page");ui_attr(out,"shape-rendering","crispEdges");ui_gt(out);
    ui_open(out,"title",NULL);ui_copy(out,"Open this public page");ui_close(out,"title");ui_open(out,"desc",NULL);
    /* code_url_valid admits no XML metacharacters. */
    fputs(url,out);ui_close(out,"desc");
    ui_elem(out,"rect",(const UiAttr[]){{"width","100%"},{"height","100%"},{"fill","#fff"}},3,1);
    ui_start(out,"path");ui_attr(out,"fill","#000");ui_attr_open(out,"d");
    for(int y=0;y<size;y++) {
        for(int x=0;x<size;) {
            if(!qrcodegen_getModule(qr,x,y)){x++;continue;}
            int start=x;while(x<size&&qrcodegen_getModule(qr,x,y))x++;
            fprintf(out,"M%d %dh%dv1h-%dz",start+4,y+4,x-start,x-start);
        }
    }
    fputs("\"/></svg>\n",out);
    return !ferror(out);
}
int bf_public_code_emit_details(FILE *out,const char *host,const char *path) {
    char url[BF_PUBLIC_CODE_URI_MAX+1];
    if(!out||!bf_public_code_url(host,path,url,sizeof(url)))return 0;
    ui_section_open(out, "bf-public-code", "Open on another device");ui_open(out,"div","bf-public-code-body");
    ui_start(out,"img");ui_attr(out,"loading","lazy");ui_attr(out,"decoding","async");ui_attr(out,"width","280");ui_attr(out,"height","280");
    ui_attr_open(out,"src");fputs(path,out);ui_copy(out,".qr.svg");ui_attr_close(out);
    ui_attr(out,"alt","Scan this QR code to open the same public page");ui_gt(out);
    ui_open(out,"div",NULL);ui_open(out,"p",NULL);ui_copy(out,"Scan to open this page. Your saved notes and account access are not included.");ui_close(out,"p");
    ui_open(out,"p",NULL);ui_start(out,"a");ui_attr_open(out,"href");fputs(path,out);ui_copy(out,".qr.svg");ui_attr_close(out);
    ui_attr(out,"download","aurekai-page-code.svg");ui_gt(out);ui_copy(out,"Download QR code");ui_close(out,"a");ui_close(out,"p");
    ui_open(out,"p",NULL);ui_start(out,"a");ui_attr_open(out,"href");fputs(url,out);ui_attr_close(out);ui_gt(out);
    ui_copy(out,"Open the page link");ui_close(out,"a");ui_close(out,"p");
    ui_close(out,"div");ui_close(out,"div");ui_section_close(out);
    return !ferror(out);
}
