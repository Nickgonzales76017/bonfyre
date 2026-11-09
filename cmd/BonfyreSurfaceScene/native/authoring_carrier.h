/* One native HTML compiler for the canonical Aurekai authoring carrier.
 * C owns the wire validation, tree closure, escaping, responsive layout and
 * editorial projection. Browser editors are inputs; NOT alternate publishers.
 * Preview output is explicitly non-public and carries no rights decision.
 * Independent of framework, hosted API, template engine or network access.
 */
#ifndef BF_AUTHORING_CARRIER_H
#define BF_AUTHORING_CARRIER_H
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include "../../BonfyreWebAddress/generated/native_authoring_tree.h"

#define BF_WIRE_LIMIT (4u * 1024u * 1024u)
#define BF_WIRE_FIELDS 30000u
typedef struct {char *path,*value;char type;} BfWireField;
typedef struct {
 char *id,*kind,*parent;
 uint32_t position,fields_count;
 BfWireField *fields;
} BfWireNode;
typedef struct {
 char *site,*route;
 uint32_t count;
 BfWireNode *nodes;
} BfWireScene;
typedef struct {const unsigned char *p;size_t len,at;int ok;} BfReader;

static uint32_t bf_wire_u32(BfReader *r){
 if(!r->ok||r->len-r->at<4){r->ok=0;return 0;}
 const unsigned char *p=r->p+r->at;r->at+=4;
 return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}
static char *bf_wire_str(BfReader *r){
 uint32_t len=bf_wire_u32(r);
 if(!r->ok||len>2097152u||len>r->len-r->at){r->ok=0;return NULL;}
 char *s=(char*)malloc((size_t)len+1);
 if(!s){r->ok=0;return NULL;}
 if(memchr(r->p+r->at,0,len)){free(s);r->ok=0;return NULL;}
 memcpy(s,r->p+r->at,len);s[len]='\0';r->at+=len;return s;
}
static void bf_wire_clear(BfWireScene *s){
 if(!s)return;
 for(uint32_t i=0;i<s->count;i++){
  BfWireNode *n=&s->nodes[i];free(n->id);free(n->kind);free(n->parent);
  for(uint32_t j=0;j<n->fields_count;j++){free(n->fields[j].path);free(n->fields[j].value);}
  free(n->fields);
 }
 free(s->nodes);free(s->site);free(s->route);memset(s,0,sizeof(*s));
}
static const BfWireField *bf_wire_field(const BfWireNode *node,const char *path){
 if(!node||!path)return NULL;
 for(uint32_t i=0;i<node->fields_count;i++)
  if(!strcmp(node->fields[i].path,path))return &node->fields[i];
 return NULL;
}
static const char *bf_wire_val(const BfWireNode *node,const char *path){
 const BfWireField *f=bf_wire_field(node,path);
 return f&&f->type=='s'?f->value:NULL;
}
static int bf_wire_integer(const BfWireNode *node,const char *path,int fallback,int lo,int hi){
 const BfWireField *f=bf_wire_field(node,path);
 if(!f)return fallback;
 if(f->type!='d'||!f->value||!*f->value)return -1;
 int n=0;
 for(const unsigned char *p=(const unsigned char*)f->value;*p;p++){
  if(*p<'0'||*p>'9'||n>100000)return -1;
  n=n*10+(int)(*p-'0');
 }
 return n<lo||n>hi?-1:n;
}
static int bf_wire_bool(const BfWireNode *node,const char *path){
 const BfWireField *f=bf_wire_field(node,path);
 return f&&f->type=='b'&&!strcmp(f->value,"1");
}
static int bf_wire_array(const BfWireNode *node,const char *path){
 const BfWireField *f=bf_wire_field(node,path);
 if(!f)return -1;
 if(f->type!='a'||!f->value||!*f->value)return -2;
 int n=0;
 for(const unsigned char *p=(const unsigned char*)f->value;*p;p++){
  if(*p<'0'||*p>'9'||n>30000)return -2;
  n=n*10+(int)(*p-'0');
 }
 return n<=30000?n:-2;
}
static int bf_wire_token(const char *s){
 if(!s)return 0;
 for(const unsigned char *p=(const unsigned char*)s;*p;p++)
  if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='-'))return 0;
 return 1;
}
static int bf_wire_path_ok(const char *path){
 if(strncmp(path,"/props",6)&&strncmp(path,"/responsive",11))return 0;
 if(!(path[6]=='/'||path[6]=='\0')&&strncmp(path,"/responsive",11))return 0;
 const char *p=path;
 for(;*p;p++)if((unsigned char)*p<32||*p=='\\'||*p=='"'||*p=='<')return 0;
 if(strstr(path,"/__proto__")||strstr(path,"/constructor")||strstr(path,"/prototype"))return 0;
 return strlen(path)<=512;
}
static int bf_wire_read(FILE *input,BfWireScene *out){
 if(!input||!out)return 0;
 memset(out,0,sizeof(*out));
 unsigned char *data=(unsigned char*)malloc(BF_WIRE_LIMIT+1);
 if(!data)return 0;
 size_t size=fread(data,1,BF_WIRE_LIMIT+1,input);
 if(ferror(input)||size<7||size>BF_WIRE_LIMIT||memcmp(data,"BFAU2\r\n",7)){free(data);return 0;}
 BfReader r={data,size,7,1};
 out->site=bf_wire_str(&r);out->route=bf_wire_str(&r);
 uint32_t count=bf_wire_u32(&r);
 if(!r.ok||!out->site||!out->route||count>BF_AUTHORING_TREE_MAX||!bf_authoring_tree_id(out->site)||
    out->route[0]!='/'){free(data);bf_wire_clear(out);return 0;}
 out->nodes=(BfWireNode*)calloc(count?count:1,sizeof(*out->nodes));
 BfAuthoringTree *tree=(BfAuthoringTree*)calloc(1,sizeof(*tree));
 if(!out->nodes||!tree){free(tree);free(data);bf_wire_clear(out);return 0;}
 int valid=1;
 for(uint32_t i=0;i<count&&valid;i++){
  BfWireNode *n=&out->nodes[i];out->count=i+1;
  n->id=bf_wire_str(&r);n->kind=bf_wire_str(&r);n->parent=bf_wire_str(&r);
  n->position=bf_wire_u32(&r);n->fields_count=bf_wire_u32(&r);
  if(!r.ok||!n->id||!n->kind||!n->parent||n->fields_count>BF_WIRE_FIELDS||
     n->position!=(uint32_t)bf_authoring_tree_nchildren(tree,n->parent)||
     !bf_authoring_tree_add(tree,n->id,n->kind,*n->parent?n->parent:NULL,(int)n->position)){
   valid=0;break;
  }
  n->fields=(BfWireField*)calloc(n->fields_count?n->fields_count:1,sizeof(*n->fields));
  if(!n->fields){valid=0;break;}
  for(uint32_t j=0;j<n->fields_count;j++){
   BfWireField *f=&n->fields[j];
   f->path=bf_wire_str(&r);char *type=bf_wire_str(&r);f->value=bf_wire_str(&r);
   if(!r.ok||!f->path||!type||!f->value||strlen(type)!=1||
      !strchr("sonbda",type[0])||!bf_wire_path_ok(f->path))valid=0;
   if(type&&strlen(type)==1)f->type=type[0];
   free(type);
   if(!valid)break;
   for(uint32_t k=0;k<j;k++)if(!strcmp(f->path,n->fields[k].path)){valid=0;break;}
  }
  if(!bf_wire_field(n,"/props")||!bf_wire_field(n,"/responsive"))valid=0;
  if(valid){
   const char *heading=bf_wire_val(n,"/props/heading"),*body=bf_wire_val(n,"/props/body");
   if((heading&&!bf_authoring_tree_text(tree,n->id,"heading",heading))||
      (body&&!bf_authoring_tree_text(tree,n->id,"body",body)))valid=0;
  }
 }
 if(!r.ok||r.at!=size||out->count!=count)valid=0;
 bf_authoring_tree_clear(tree);free(tree);free(data);
 if(!valid){bf_wire_clear(out);return 0;}
 return 1;
}
static void bf_html_escape(FILE *f,const char *s){
 if(!s)return;
 for(const unsigned char *p=(const unsigned char*)s;*p;p++){
  switch(*p){
   case '&':fputs("&amp;",f);break;case '<':fputs("&lt;",f);break;
   case '>':fputs("&gt;",f);break;case '"':fputs("&quot;",f);break;
   case '\'':fputs("&#39;",f);break;
   default:fputc(*p,f);break;
  }
 }
}
static int bf_safe_https(const char *s){
 if(!s||strncmp(s,"https://",8))return 0;
 const char *host=s+8;if(!strchr(host,'.')||strchr(host,'@'))return 0;
 for(const unsigned char *p=(const unsigned char*)s;*p;p++)
  if(*p<=32||*p=='<'||*p=='>'||*p=='"'||*p=='\''||*p=='\\')return 0;
 return 1;
}
static int bf_inline(FILE *f,const char *s){
 // Editor.js permits a small inline formatting alphabet. This parser only
 // emits its known tags, escapes every other byte, and rejects unsafe links.
 if(!s)return 1;
 for(size_t i=0;s[i];){
  if(s[i]=='<'){
   const char *tags[]={"<strong>","</strong>","<em>","</em>","<b>","</b>",
     "<i>","</i>","<code>","</code>","</a>"};
   int matched=0;
   for(size_t t=0;t<sizeof(tags)/sizeof(tags[0]);t++){
    size_t z=strlen(tags[t]);
    if(!strncmp(s+i,tags[t],z)){fwrite(tags[t],1,z,f);i+=z;matched=1;break;}
   }
   if(matched)continue;
   if(!strncmp(s+i,"<a href=\"",9)){
    const char *start=s+i+9,*end=strstr(start,"\">");
    if(!end||end-start>1800)return 0;
    size_t n=(size_t)(end-start);char url[1801];
    memcpy(url,start,n);url[n]='\0';
    if(!bf_safe_https(url))return 0;
    fputs("<a href=\"",f);bf_html_escape(f,url);fputs("\" rel=\"noopener noreferrer\">",f);
    i=(size_t)(end-s)+2;continue;
   }
  }
  unsigned char c=(unsigned char)s[i++];
  if(c=='&')fputs("&amp;",f);else if(c=='<')fputs("&lt;",f);
  else if(c=='>')fputs("&gt;",f);else if(c=='"')fputs("&quot;",f);
  else if(c=='\'')fputs("&#39;",f);else fputc(c,f);
 }
 return 1;
}
static int bf_render_editorial(FILE *f,const BfWireNode *n){
 int count=bf_wire_array(n,"/props/editorial");
 if(count==-1)return -1; // caller renders plain-body paragraphs
 if(count<0||count>256)return 0;
 for(int i=0;i<count;i++){
  char prefix[96],path[180];snprintf(prefix,sizeof(prefix),"/props/editorial/%d",i);
  snprintf(path,sizeof(path),"%s/type",prefix);
  const char *type=bf_wire_val(n,path);if(!type)return 0;
  snprintf(path,sizeof(path),"%s/data/text",prefix);const char *txt=bf_wire_val(n,path);
  if(!strcmp(type,"paragraph")||!strcmp(type,"header")){
   if(!txt)return 0;
   int level=3;
   if(!strcmp(type,"header")){
    snprintf(path,sizeof(path),"%s/data/level",prefix);
    level=bf_wire_integer(n,path,3,1,6);if(level<0)return 0;
    fprintf(f,"<h%d>",level);if(!bf_inline(f,txt))return 0;fprintf(f,"</h%d>",level);
   }else{fputs("<p>",f);if(!bf_inline(f,txt))return 0;fputs("</p>",f);}
  }else if(!strcmp(type,"quote")){
   if(!txt)return 0;fputs("<blockquote><p>",f);if(!bf_inline(f,txt))return 0;fputs("</p>",f);
   snprintf(path,sizeof(path),"%s/data/caption",prefix);
   const char *caption=bf_wire_val(n,path);
   if(caption){fputs("<cite>",f);bf_html_escape(f,caption);fputs("</cite>",f);}
   fputs("</blockquote>",f);
  }else if(!strcmp(type,"code")){
   snprintf(path,sizeof(path),"%s/data/code",prefix);const char *code=bf_wire_val(n,path);
   if(!code)return 0;fputs("<pre><code>",f);bf_html_escape(f,code);fputs("</code></pre>",f);
  }else if(!strcmp(type,"delimiter"))fputs("<hr class=\"bf-editorial-rule\">",f);
  else if(!strcmp(type,"list")||!strcmp(type,"checklist")){
   snprintf(path,sizeof(path),"%s/data/items",prefix);
   int items=bf_wire_array(n,path);if(items<0||items>256)return 0;
   snprintf(path,sizeof(path),"%s/data/style",prefix);
   int ordered=!strcmp(bf_wire_val(n,path)?bf_wire_val(n,path):"","ordered");
   fputs(ordered?"<ol>":"<ul>",f);
   for(int j=0;j<items;j++){
    char item[200];snprintf(item,sizeof(item),"%s/data/items/%d",prefix,j);
    const char *val=bf_wire_val(n,item);
    if(!val){snprintf(path,sizeof(path),"%s/content",item);val=bf_wire_val(n,path);}
    if(!val){snprintf(path,sizeof(path),"%s/text",item);val=bf_wire_val(n,path);}
    if(!val)return 0;
    fputs("<li>",f);
    if(!strcmp(type,"checklist")){
     snprintf(path,sizeof(path),"%s/checked",item);
     fprintf(f,"<input type=\"checkbox\" disabled%s> ",bf_wire_bool(n,path)?" checked":"");
    }
    if(!bf_inline(f,val))return 0;fputs("</li>",f);
   }
   fputs(ordered?"</ol>":"</ul>",f);
  }else if(!strcmp(type,"table")){
   snprintf(path,sizeof(path),"%s/data/content",prefix);
   int rows=bf_wire_array(n,path);if(rows<0||rows>100)return 0;
   snprintf(path,sizeof(path),"%s/data/withHeadings",prefix);int has_head=bf_wire_bool(n,path);
   fputs("<div class=\"bf-table-scroll\"><table>",f);
   for(int row=0;row<rows;row++){
    char rowpath[200];snprintf(rowpath,sizeof(rowpath),"%s/data/content/%d",prefix,row);
    int cells=bf_wire_array(n,rowpath);if(cells<0||cells>32)return 0;
    fputs("<tr>",f);
    for(int col=0;col<cells;col++){
     char cell[220];snprintf(cell,sizeof(cell),"%s/%d",rowpath,col);
     const char *value=bf_wire_val(n,cell);if(!value)return 0;
     fputs(row==0&&has_head?"<th scope=\"col\">":"<td>",f);
     if(!bf_inline(f,value))return 0;
     fputs(row==0&&has_head?"</th>":"</td>",f);
    }
    fputs("</tr>",f);
   }
   fputs("</table></div>",f);
  }else return 0; // No silent lossy fallback for unsupported editorial tools.
 }
 return 1;
}
static const char *bf_semantic_tag(const char *kind){
 if(!strcmp(kind,"profile-hero")||!strcmp(kind,"service:hero"))return "header";
 if(!strcmp(kind,"profile-quote"))return "blockquote";
 if(!strcmp(kind,"service:media"))return "figure";
 if(!strcmp(kind,"profile-card"))return "article";
 return "section";
}
static int bf_render_node(FILE *f,const BfWireScene *s,const BfWireNode *n,int depth){
 if(depth>BF_AUTHORING_TREE_DEPTH)return 0;
 const char *tag=bf_semantic_tag(n->kind),*heading=bf_wire_val(n,"/props/heading");
 const char *body=bf_wire_val(n,"/props/body"),*variant=bf_wire_val(n,"/props/variant");
 const char *eyebrow=bf_wire_val(n,"/props/eyebrow");
 if(!variant)variant="paper";
 if(!bf_wire_token(variant))return 0;
 const char *layouts[3];const char *devices[]={"desktop","tablet","phone"};
 int columns[3];
 for(int k=0;k<3;k++){
  char path[90];snprintf(path,sizeof(path),"/responsive/%s/layout",devices[k]);
  layouts[k]=bf_wire_val(n,path);
  if(!layouts[k])layouts[k]="stack";
  if(!bf_wire_token(layouts[k]))return 0;
  snprintf(path,sizeof(path),"/responsive/%s/columns",devices[k]);
  columns[k]=bf_wire_integer(n,path,1,1,12);if(columns[k]<0)return 0;
 }
 fprintf(f,"<%s class=\"bf-block bf-variant-",tag);bf_html_escape(f,variant);
 fputs("\" data-id=\"",f);bf_html_escape(f,n->id);
 fputs("\" data-kind=\"",f);bf_html_escape(f,n->kind);
 fputs("\" data-layout-desktop=\"",f);bf_html_escape(f,layouts[0]);
 fputs("\" data-layout-tablet=\"",f);bf_html_escape(f,layouts[1]);
 fputs("\" data-layout-phone=\"",f);bf_html_escape(f,layouts[2]);
 fprintf(f,"\" style=\"--bf-col-d:%d;--bf-col-t:%d;--bf-col-p:%d",columns[0],columns[1],columns[2]);
 const char *pad[]={"desktop","phone"};
 for(int k=0;k<2;k++){
  char path[80];snprintf(path,sizeof(path),"/responsive/%s/padding",pad[k]);
  int value=bf_wire_integer(n,path,0,0,64);if(value<0)return 0;
  fprintf(f,";--bf-pad-%c:%dpx",k?'p':'d',value);
 }
 fputs("\">",f);
 const char *ref=bf_wire_val(n,"/props/source_ref");
 if(ref){fputs("<span class=\"bf-source\" data-source-ref=\"",f);bf_html_escape(f,ref);
  fputs("\">Source retained · publication rights separately reviewed</span>",f);}
 if(eyebrow&&*eyebrow){fputs("<p class=\"bf-eyebrow\">",f);bf_html_escape(f,eyebrow);fputs("</p>",f);}
 if(heading&&*heading){
  const char *h=!strcmp(tag,"header")?"h1":"h2";
  fprintf(f,"<%s>",h);bf_html_escape(f,heading);fprintf(f,"</%s>",h);
 }
 if(!strcmp(tag,"figure")){
  const char *asset=bf_wire_val(n,"/props/asset_ref");
  if(asset){fputs("<div class=\"bf-media-pending\" data-asset-ref=\"",f);bf_html_escape(f,asset);
   fputs("\">Media pending rights-approved asset resolution</div>",f);}
  const char *alt=bf_wire_val(n,"/props/alt");if(alt&&*alt){fputs("<p class=\"bf-media-alt\">",f);bf_html_escape(f,alt);fputs("</p>",f);}
  const char *caption=bf_wire_val(n,"/props/caption");
  if(caption&&*caption){fputs("<figcaption>",f);bf_html_escape(f,caption);fputs("</figcaption>",f);}
 }
 int result=bf_render_editorial(f,n);
 if(!result)return 0;
 if(result==-1&&body&&*body){
  fputs("<div class=\"bf-prose\">",f);
  const char *start=body,*p=body;
  while(1){
   if(*p=='\0'||(*p=='\n'&&p[1]=='\n')){
    fputs("<p>",f);
    for(const char *q=start;q<p;q++){
     if(*q=='&')fputs("&amp;",f);else if(*q=='<')fputs("&lt;",f);
     else if(*q=='>')fputs("&gt;",f);else if(*q=='"')fputs("&quot;",f);
     else if(*q=='\'')fputs("&#39;",f);else if(*q=='\n')fputs("<br>",f);else fputc(*q,f);
    }
    fputs("</p>",f);
    if(!*p)break;
    p+=2;start=p;continue;
   }
   p++;
  }
  fputs("</div>",f);
 }
 const char *href=bf_wire_val(n,"/props/href"),*label=bf_wire_val(n,"/props/label");
 if(href&&*href){if(!bf_safe_https(href))return 0;
  fputs("<a class=\"bf-action\" href=\"",f);bf_html_escape(f,href);
  fputs("\" rel=\"noopener noreferrer\">",f);bf_html_escape(f,label&&*label?label:"Open source");
  fputs("</a>",f);}
 int child_count=0;
 for(uint32_t i=0;i<s->count;i++)if(!strcmp(s->nodes[i].parent,n->id))child_count++;
 if(child_count){
  fputs("<div class=\"bf-children\">",f);
  for(int k=0;k<child_count;k++){
   const BfWireNode *child=NULL;
   for(uint32_t i=0;i<s->count;i++)if(!strcmp(s->nodes[i].parent,n->id)&&s->nodes[i].position==(uint32_t)k){
    child=&s->nodes[i];break;}
   if(!child||!bf_render_node(f,s,child,depth+1))return 0;
  }
  fputs("</div>",f);
 }
 fprintf(f,"</%s>",tag);return !ferror(f);
}
static int bf_wire_render(FILE *f,const BfWireScene *s){
 if(!f||!s||!s->site||!s->route)return 0;
 const char *title="Aurekai authoring preview";
 for(uint32_t i=0;i<s->count;i++){
  const char *heading=bf_wire_val(&s->nodes[i],"/props/heading");
  if(heading&&*heading){title=heading;break;}
 }
 fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
  "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
  "<meta name=\"robots\" content=\"noindex,nofollow\"><title>",f);
 bf_html_escape(f,title);
 fputs("</title><style>"
 ":root{font-family:system-ui,-apple-system,Arial,sans-serif;color:#1c2830;background:#f6f5f0}"
 "*,*::before,*::after{box-sizing:border-box}body{margin:0;line-height:1.6}"
 "main{margin:auto;max-width:1200px;padding:clamp(20px,4vw,65px)}"
 ".bf-block{min-width:0;margin:0 0 22px;padding:var(--bf-pad-d,0px)}"
 ".bf-block h1,.bf-block h2{line-height:1.12;letter-spacing:-.035em}"
 ".bf-block h1{font:600 clamp(40px,6.8vw,91px)/1.05 Georgia,serif;max-width:17ch}"
 ".bf-block h2{font:600 clamp(24px,3.5vw,45px)/1.12 Georgia,serif}"
 ".bf-block p{max-width:72ch}.bf-eyebrow{font-size:.72rem;font-weight:800;letter-spacing:.17em;text-transform:uppercase}"
 ".bf-variant-night{background:#16242a;color:#f7f6ee;padding:clamp(22px,5vw,70px)}"
 ".bf-variant-forest{background:#244c3b;color:#fff;padding:26px}"
 ".bf-variant-rust{background:#f2e6d7;color:#67341e;padding:26px}"
 ".bf-kind-profile-card{background:white;border:1px solid #d9dbd4;padding:26px}"
 ".bf-media-pending{border:1px solid #c7c9c3;padding:5vw;text-align:center;background:#e9e8e1}"
 ".bf-source{display:block;font:12px system-ui;color:#78817f;margin-bottom:14px}"
 ".bf-variant-night .bf-source{color:#c1d1cb}"
 ".bf-action{display:inline-flex;margin-top:18px;padding:12px 18px;background:#e9ac64;color:#17242a;text-decoration:none;font-weight:700}"
 ".bf-prose{font:1.1rem/1.8 Georgia,serif}.bf-block blockquote{border-left:3px solid #d08a43;margin:22px 0;padding:10px 20px}"
 ".bf-block pre{overflow:auto;background:#14282d;color:white;padding:20px}"
 ".bf-block table{border-collapse:collapse;width:100%}.bf-block td,.bf-block th{text-align:left;border-bottom:1px solid #d2d5d0;padding:10px}"
 ".bf-table-scroll{max-width:100%;overflow-x:auto}"
 ".bf-block[data-layout-desktop=grid]>.bf-children,.bf-block[data-layout-desktop=columns]>.bf-children{display:grid;grid-template-columns:repeat(var(--bf-col-d),minmax(0,1fr));gap:24px}"
 "@media(max-width:1000px){.bf-block[data-layout-tablet=grid]>.bf-children{display:grid;grid-template-columns:repeat(var(--bf-col-t),minmax(0,1fr))}}"
 "@media(max-width:680px){main{padding:16px}.bf-block{padding:var(--bf-pad-p,0px)}"
 ".bf-block[data-layout-phone=stack]>.bf-children{display:block}"
 ".bf-block[data-layout-phone=grid]>.bf-children{display:grid;grid-template-columns:repeat(var(--bf-col-p),minmax(0,1fr))}}"
 "</style></head><body data-release=\"draft-preview-only\" data-site=\"",f);
 bf_html_escape(f,s->site);fputs("\" data-route=\"",f);bf_html_escape(f,s->route);
 fputs("\"><main id=\"content\">",f);
 int roots=0;
 for(uint32_t i=0;i<s->count;i++)if(!*s->nodes[i].parent)roots++;
 for(int k=0;k<roots;k++){
  const BfWireNode *node=NULL;
  for(uint32_t i=0;i<s->count;i++)if(!*s->nodes[i].parent&&s->nodes[i].position==(uint32_t)k){
   node=&s->nodes[i];break;}
  if(!node||!bf_render_node(f,s,node,0))return 0;
 }
 fputs("</main><footer role=\"note\" style=\"padding:20px;text-align:center\">"
 "Authoring preview only. No publication, rights or account admission inferred.</footer></body></html>\n",f);
 return !ferror(f);
}
#endif
