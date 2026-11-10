/* Shared native Markdown grammar, not a second handwritten Markdown parser.
 * MD4C source and license live under ../vendor/md4c; generated HTML is
 * admitted only after link/media and output-budget review. */
#include "authoring_markdown.h"
#include "../vendor/md4c/md4c-html.h"
#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#define BF_MD_SOURCE_LIMIT 131072u
#define BF_MD_HTML_LIMIT 1048576u
typedef struct {char *bytes;size_t used,limit;int failed;} MdOutput;
static void receive(const MD_CHAR *data,MD_SIZE size,void *owner){
 MdOutput *o=(MdOutput *)owner;
 if(!o||o->failed)return;
 if((size_t)size>o->limit-o->used){o->failed=1;return;}
 memcpy(o->bytes+o->used,data,(size_t)size);
 o->used+=(size_t)size;o->bytes[o->used]=0;
}
static int url_ok(const char *src,size_t len){
 if(!src||!len||len>2048)return 0;
 for(size_t i=0;i<len;i++){
  const unsigned char c=(unsigned char)src[i];
  if(c<33||c==127||c=='<'||c=='>'||c=='"'||c==39||c=='\\')return 0;
 }
 if(src[0]=='#'){
  if(len<2||!isalnum((unsigned char)src[1]))return 0;
  for(size_t i=2;i<len;i++)if(!isalnum((unsigned char)src[i])&&src[i]!='-'&&src[i]!='_')return 0;
  return 1;
 }
 if(src[0]=='/'){
  if(len>1&&src[1]=='/')return 0;
  for(size_t i=0;i+1<len;i++)if(src[i]=='.'&&src[i+1]=='.')return 0;
  return 1;
 }
 if(len<10||memcmp(src,"https://",8))return 0;
 const char *host=src+8,*stop=src+len;
 for(const char *p=host;p<stop;p++)if(*p=='/'||*p=='?'||*p=='#'){stop=p;break;}
 if(host==stop)return 0;
 int has_dot=0;
 for(const char *p=host;p<stop;p++){
  const unsigned char ch=(unsigned char)*p;
  if(ch=='.')has_dot=1;
  else if(!isalnum(ch)&&ch!='-'&&ch!=':')return 0;
 }
 return has_dot;
}
static int inspect(const char *html,size_t len){
 if(!html||len>BF_MD_HTML_LIMIT)return 0;
 /* MD4C creates only first-party elements with MD_FLAG_NOHTML. Every
  * generated external destination is checked; image Markdown must use
  * source-cleared CMS media rather than unreviewed remote embeds. */
 for(size_t i=0;i<len;i++){
  if(i+4<=len&&!memcmp(html+i,"<img",4))return 0;
  if(i+5<=len&&!memcmp(html+i,"src=\"",5))return 0;
  if(i+6<=len&&!memcmp(html+i,"href=\"",6)){
   const char *start=html+i+6,*stop=memchr(start,'"',len-(i+6));
   if(!stop||!url_ok(start,(size_t)(stop-start)))return 0;
   i=(size_t)(stop-html);
  }
 }
 return 1;
}
int bf_authoring_markdown_render(FILE *out,const char *markdown,size_t max_html){
 if(!out||!markdown||max_html<1||max_html>BF_MD_HTML_LIMIT)return 0;
 const size_t size=strlen(markdown);
 if(size>BF_MD_SOURCE_LIMIT||size>INT32_MAX)return 0;
 MdOutput buffer={0};
 buffer.bytes=(char *)malloc(max_html+1);
 if(!buffer.bytes)return 0;
 buffer.used=0;buffer.limit=max_html;buffer.failed=0;buffer.bytes[0]=0;
 const unsigned flags=MD_DIALECT_GITHUB|MD_FLAG_NOHTML;
 const int result=md_html(markdown,(MD_SIZE)size,receive,&buffer,flags,0);
 const int admitted=result==0&&!buffer.failed&&inspect(buffer.bytes,buffer.used);
 const int ok=admitted&&fwrite(buffer.bytes,1,buffer.used,out)==buffer.used;
 free(buffer.bytes);return ok;
}
