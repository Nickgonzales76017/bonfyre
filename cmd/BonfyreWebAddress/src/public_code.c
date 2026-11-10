#include "public_code.h"
#include "../build/public-code/qrcodegen.h"
#include <ctype.h>
#include <stdint.h>
#include <string.h>

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
static int code_item_path(const char *path) {
    if(!path||strncmp(path,"/o/",3))return 0;
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
    fprintf(out,"<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"%d\" height=\"%d\" role=\"img\" aria-label=\"QR code for this public page\" shape-rendering=\"crispEdges\">",edge,edge,edge*6,edge*6);
    fputs("<title>Open this public page</title><desc>",out);
    /* code_url_valid admits no XML metacharacters. */
    fputs(url,out);fputs("</desc><rect width=\"100%\" height=\"100%\" fill=\"#fff\"/><path fill=\"#000\" d=\"",out);
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
    fputs("<details class=\"bf-public-code\" id=\"open-on-device\" data-bf-carrier=\"public-page-uri\"><summary>Open on another device</summary><div class=\"bf-public-code-body\">",out);
    fputs("<img loading=\"lazy\" decoding=\"async\" width=\"280\" height=\"280\" src=\"",out);fputs(path,out);
    fputs(".qr.svg\" alt=\"Scan this QR code to open the same public page\"><div><p>Scan to open this page. Your saved notes and account access are not included.</p><p><a href=\"",out);
    fputs(path,out);fputs(".qr.svg\" download=\"aurekai-page-code.svg\">Download QR code</a></p><p><a href=\"",out);
    fputs(url,out);fputs("\">Open the page link</a></p></div></div></details>",out);
    fputs("<style>.bf-public-code{margin:1.2rem 0;border:1px solid var(--w-rule,#aeb9b2);border-radius:12px;max-width:100%;overflow:hidden}.bf-public-code>summary{padding:14px 18px;cursor:pointer;font:600 15px/1.4 var(--w-face-body,system-ui)}.bf-public-code>summary:focus-visible,.bf-public-code a:focus-visible{outline:3px solid var(--w-accent,#345dd0);outline-offset:3px}.bf-public-code-body{display:grid;grid-template-columns:minmax(0,280px) minmax(0,1fr);gap:20px;padding:18px;align-items:center}.bf-public-code-body>img{display:block;max-width:100%;height:auto;background:#fff;border-radius:0;filter:none;mix-blend-mode:normal;image-rendering:pixelated}.bf-public-code-body p{font:400 14px/1.6 var(--w-face-body,system-ui);overflow-wrap:anywhere}.bf-public-code-body a{text-decoration:underline;text-underline-offset:3px}@media(max-width:650px){.bf-public-code-body{grid-template-columns:minmax(0,1fr)}.bf-public-code-body>img{width:min(100%,280px);margin:auto}}</style>",out);
    return !ferror(out);
}
