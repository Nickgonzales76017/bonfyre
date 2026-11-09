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
#include "../vendor/md4c/md4c-html.h"
#include "../generated/native_editorial_media.h"
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
  if(n->fields)for(uint32_t j=0;j<n->fields_count;j++){
   free(n->fields[j].path);free(n->fields[j].value);
  }
  free(n->fields);
 }
 free(s->nodes);free(s->site);free(s->route);memset(s,0,sizeof(*s));
}
static const BfWireField *bf_wire_field(const BfWireNode *node,const char *path){
 if(!node||!path)return NULL;
 for(uint32_t i=0;i<node->fields_count;i++)
  if(node->fields[i].path&&!strcmp(node->fields[i].path,path))return &node->fields[i];
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
  if(valid&&(!bf_wire_field(n,"/props")||!bf_wire_field(n,"/responsive")))valid=0;
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
/* Native lossless emitter: an entire authoring document, not a rendered
 * approximation, can roundtrip through C without dropping metadata,
 * bindings, editorial tools, or responsive variants.
 */
static inline int bf_wire_write_u32(FILE *f,uint32_t n){
 unsigned char b[4]={(unsigned char)n,(unsigned char)(n>>8),
  (unsigned char)(n>>16),(unsigned char)(n>>24)};
 return fwrite(b,1,4,f)==4;
}
static inline int bf_wire_write_str(FILE *f,const char *s){
 if(!s)return 0;
 size_t n=strlen(s);if(n>2097152u)return 0;
 return bf_wire_write_u32(f,(uint32_t)n)&&fwrite(s,1,n,f)==n;
}
static inline int bf_wire_write(FILE *f,const BfWireScene *s){
 if(!f||!s||!s->site||!s->route||s->count>BF_AUTHORING_TREE_MAX)return 0;
 if(fwrite("BFAU2\r\n",1,7,f)!=7||!bf_wire_write_str(f,s->site)||
    !bf_wire_write_str(f,s->route)||!bf_wire_write_u32(f,s->count))return 0;
 for(uint32_t i=0;i<s->count;i++){
  const BfWireNode *n=&s->nodes[i];
  if(!bf_wire_write_str(f,n->id)||!bf_wire_write_str(f,n->kind)||
     !bf_wire_write_str(f,n->parent)||!bf_wire_write_u32(f,n->position)||
     !bf_wire_write_u32(f,n->fields_count))return 0;
  for(uint32_t j=0;j<n->fields_count;j++){
   const BfWireField *field=&n->fields[j];
   char type[2]={field->type,0};
   if(!bf_wire_write_str(f,field->path)||!bf_wire_write_str(f,type)||
      !bf_wire_write_str(f,field->value))return 0;
  }
 }
 return !ferror(f);
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
/* Source Markdown is a typed retained value; native MD4C changes only its
 * preview. Raw HTML is disabled; external media become rights-held placeholders.
 */
typedef struct {char *bytes;size_t used,capacity;int failed;} BfMarkdownSink;
static void bf_md_output(const MD_CHAR *data,MD_SIZE length,void *context){
 BfMarkdownSink *sink=(BfMarkdownSink*)context;
 size_t amount=(size_t)length;
 if(sink->failed)return;
 if(amount>1048576u-sink->used){sink->failed=1;return;}
 size_t required=sink->used+amount;
 if(required>sink->capacity){
  size_t capacity=sink->capacity?sink->capacity:4096;
  while(capacity<required)capacity*=2;
  char *newbytes=(char *)realloc(sink->bytes,capacity);
  if(!newbytes){sink->failed=1;return;}
  sink->bytes=newbytes;sink->capacity=capacity;
 }
 if(amount)memcpy(sink->bytes+sink->used,data,amount);
 sink->used=required;
}
static int bf_md_safe_url(const char *s,size_t n){
 if(!s||!n||n>1800)return 0;
 char value[1801];memcpy(value,s,n);value[n]='\0';
 if(strstr(value,"&#")||strstr(value,"&colon;"))return 0;
 for(size_t i=0;i<n;i++)
  if((unsigned char)value[i]<32||value[i]=='\\'||value[i]=='"'||
     value[i]=='\''||value[i]=='<'||value[i]=='>')return 0;
 if(!strncmp(value,"https://",8)||!strncmp(value,"http://",7)){
  const char *authority=strstr(value,"://")+3;
  return *authority&&strchr(authority,'.')&&!strchr(authority,'@');
 }
 if(!strncmp(value,"mailto:",7))return strchr(value+7,'@')!=NULL;
 if(value[0]=='/'&&value[1]=='/')return 0;
 return !strchr(value,':')&&!strstr(value,"//");
}
/* Build navigation from the same MD4C HTML that supplies the article. A
 * heading becomes a link only when the parser actually emitted an H2, so
 * source text inside fenced code cannot invent TOC entries.
 */
static int bf_md_navigation(FILE *out,const char *html,size_t n){
 const char *p=html,*end=html+n;
 unsigned count=0;
 while(p<end){
  if(end-p<4||memcmp(p,"<h2>",4)){p++;continue;}
  const char *head=p+4,*close=NULL;
  for(const char *q=head;q+5<=end;q++)if(!memcmp(q,"</h2>",5)){close=q;break;}
  if(!close)return 0;
  if(++count>96)return 0;
  if(count==1)fputs("<nav class=\"bf-article-toc\" aria-label=\"Article chapters\"><p>Inside this investigation</p><ol>",out);
  fprintf(out,"<li><a href=\"#chapter-%02u\"><span>%02u</span><span>",count,count);
  int markup=0;
  for(const char *q=head;q<close;q++){
   if(*q=='<'){markup=1;continue;}
   if(*q=='>'&&markup){markup=0;continue;}
   if(!markup)fputc(*q,out);
  }
  fputs("</span></a></li>",out);
  p=close+5;
 }
 if(count)fputs("</ol></nav>",out);
 return !ferror(out);
}
static int bf_md_verified_markup(FILE *out,const char *html,size_t n){
 const char *end=html+n;
 unsigned headings=0;
 for(const char *p=html;p<end;){
  if(end-p>=4&&!memcmp(p,"<h2>",4)){
   if(++headings>96)return 0;
   fprintf(out,"<h2 id=\"chapter-%02u\">",headings);p+=4;continue;
  }
  if(end-p>=4&&!strncmp(p,"<img",4)){
   const char *close=memchr(p,'>',(size_t)(end-p));
   if(!close||close-p>4096)return 0;
   fputs("<span class=\"bf-media-pending\">Referenced image held for rights review</span>",out);
   p=close+1;continue;
  }
  if(end-p>=6&&!strncmp(p,"href=\"",6)){
   const char *start=p+6,*close=memchr(start,'"',(size_t)(end-start));
   if(!close||!bf_md_safe_url(start,(size_t)(close-start)))return 0;
   fwrite(p,1,(size_t)(close+1-p),out);p=close+1;continue;
  }
  if(end-p>=5&&!strncmp(p,"src=\"",5))return 0;
  if(end-p>=7&&!strncmp(p,"<script",7))return 0;
  fputc(*p++,out);
 }
 return !ferror(out);
}
static int bf_render_markdown(FILE *out,const char *original,int frontmatter){
 if(!original)return 0;
 const char *body=original;
 if(frontmatter){
  if(strncmp(original,"---\n",4))return 0;
  const char *end=strstr(original+4,"\n---\n");
  if(!end)return 0;
  body=end+5;
 }
 BfMarkdownSink sink={0};
 const unsigned flags=MD_FLAG_NOHTML|MD_FLAG_TABLES|MD_FLAG_STRIKETHROUGH|MD_FLAG_TASKLISTS;
 int status=md_html(body,(MD_SIZE)strlen(body),bf_md_output,&sink,flags,0);
 if(status||sink.failed){free(sink.bytes);return 0;}
 if(!bf_md_navigation(out,sink.bytes?sink.bytes:"",sink.used)){
  free(sink.bytes);return 0;
 }
 fputs("<div class=\"bf-markdown\">",out);
 int safe=bf_md_verified_markup(out,sink.bytes?sink.bytes:"",sink.used);
 fputs("</div>",out);free(sink.bytes);
 return safe&&!ferror(out);
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
  char prefix[96],path[240];snprintf(prefix,sizeof(prefix),"/props/editorial/%d",i);
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
   if(!txt)return 0;
   fputs("<blockquote><p>",f);
   if(!bf_inline(f,txt))return 0;
   fputs("</p>",f);
   snprintf(path,sizeof(path),"%s/data/caption",prefix);
   const char *caption=bf_wire_val(n,path);
   if(caption){fputs("<cite>",f);bf_html_escape(f,caption);fputs("</cite>",f);}
   fputs("</blockquote>",f);
  }else if(!strcmp(type,"code")){
   snprintf(path,sizeof(path),"%s/data/code",prefix);const char *code=bf_wire_val(n,path);
   if(!code)return 0;
   fputs("<pre><code>",f);bf_html_escape(f,code);fputs("</code></pre>",f);
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
    if(!bf_inline(f,val))return 0;
    fputs("</li>",f);
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
/* Only the original, reviewed Journal route may claim its own media key.
 * The indexed source registry, not an editable CMS property, supplies URI and
 * attribution. This is presentation in a non-public preview, not a rights grant.
 */
static const BfEditorialVisual *bf_editorial_visual(const char *route){
 if(!route||strncmp(route,"/blog/",6))return NULL;
 const char *slug=route+6;
 for(unsigned i=0;i<bf_reviewed_journal_visual_count;i++){
  const BfEditorialVisual *record=&bf_reviewed_journal_visuals[i];
  size_t len=strlen(record->slug);
  if(!strncmp(slug,record->slug,len)&&slug[len]=='/'&&slug[len+1]=='\0')
   return record;
 }
 return NULL;
}
static int bf_editorial_cover_chapter(FILE *out,const BfWireScene *scene){
 for(uint32_t i=0;i<scene->count;i++){
  const BfWireNode *part=&scene->nodes[i];
  const char *format=bf_wire_val(part,"/props/metadata/format");
  if(!format||strcmp(format,"markdown"))continue;
  const char *text=bf_wire_val(part,"/props/body");
  if(!text)continue;
  const char *chapter=strstr(text,"\n## ");
  if(!chapter)continue;
  chapter+=4;
  const char *end=strchr(chapter,'\n');
  if(!end)end=chapter+strlen(chapter);
  if(end==chapter)continue;
  size_t n=(size_t)(end-chapter);
  if(n>230)n=230;
  char label[231];memcpy(label,chapter,n);label[n]=0;
  fputs("<div class=\"bf-chapter-cover\"><span>The article / Contents</span><strong>",out);
  bf_html_escape(out,label);
  fputs("</strong></div>",out);
  return 1;
 }
 return 0;
}
static int bf_render_editorial_cover(FILE *out,const BfWireScene *scene,
                                     const char *format,const char *tone,
                                     const char *key){
 const BfEditorialVisual *reviewed=bf_editorial_visual(scene->route);
 if(!reviewed){
  if(key&&*key)return 0; // Unknown routes must never claim curated imagery.
  return 1;
 }
 if(!format||strcmp(format,reviewed->format)||
    !tone||strcmp(tone,reviewed->color)||
    !key||strcmp(key,reviewed->key))return 0;
 if(!*reviewed->key)return bf_editorial_cover_chapter(out,scene);
 if(strncmp(reviewed->url,"https://",8))return 0;
 fputs("<figure class=\"bf-edition-image\" data-media-key=\"",out);
 bf_html_escape(out,reviewed->key);
 fputs("\"><img src=\"",out);
 bf_html_escape(out,reviewed->url);
 fputs("\" alt=\"",out);bf_html_escape(out,reviewed->caption);
 fputs("\" loading=\"lazy\" decoding=\"async\" width=\"1200\" height=\"900\">",out);
 fputs("<figcaption>",out);
 if(*reviewed->credit){
  fputs("<a href=\"",out);bf_html_escape(out,reviewed->credit);
  fputs("\" rel=\"noopener noreferrer\" target=\"_blank\">",out);
  bf_html_escape(out,reviewed->caption);fputs("</a>",out);
 }else bf_html_escape(out,reviewed->caption);
 fputs("</figcaption></figure>",out);
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
 const char *source_format=bf_wire_val(n,"/props/metadata/format");
 const char *edition_format=bf_wire_val(n,"/props/metadata/edition_format");
 const char *colorway=bf_wire_val(n,"/props/metadata/colorway");
 const char *media_key=bf_wire_val(n,"/props/metadata/media_key");
 if((edition_format&&!bf_wire_token(edition_format))||
    (colorway&&!bf_wire_token(colorway))||
    (media_key&&*media_key&&!bf_wire_token(media_key)))return 0;
 int is_markdown=source_format&&!strcmp(source_format,"markdown");
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
 if(edition_format){fputs("\" data-edition-format=\"",f);bf_html_escape(f,edition_format);}
 if(colorway){fputs("\" data-colorway=\"",f);bf_html_escape(f,colorway);}
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
 if(!is_markdown&&heading&&*heading){
  const char *h=!strcmp(tag,"header")?"h1":"h2";
  fprintf(f,"<%s>",h);bf_html_escape(f,heading);fprintf(f,"</%s>",h);
 }
 if(edition_format&&!bf_render_editorial_cover(f,s,edition_format,colorway,media_key))return 0;
 if(!strcmp(tag,"figure")){
  const char *asset=bf_wire_val(n,"/props/asset_ref");
  if(asset){fputs("<div class=\"bf-media-pending\" data-asset-ref=\"",f);bf_html_escape(f,asset);
   fputs("\">Media pending rights-approved asset resolution</div>",f);}
  const char *alt=bf_wire_val(n,"/props/alt");if(alt&&*alt){fputs("<p class=\"bf-media-alt\">",f);bf_html_escape(f,alt);fputs("</p>",f);}
  const char *caption=bf_wire_val(n,"/props/caption");
  if(caption&&*caption){fputs("<figcaption>",f);bf_html_escape(f,caption);fputs("</figcaption>",f);}
 }
 int result=0;
 if(is_markdown){
  if(!bf_render_markdown(f,body,bf_wire_bool(n,"/props/metadata/frontmatter")))return 0;
 }else result=bf_render_editorial(f,n);
 if(!is_markdown&&!result)return 0;
 if(!is_markdown&&result==-1&&body&&*body){
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
 ".bf-block[data-colorway=ink]{background:#17232c;color:#faf6ef}"
 ".bf-block[data-colorway=forest]{background:#183d33;color:#f6f6ea}"
 ".bf-block[data-colorway=olive]{background:#e9ead7;color:#2b3324}"
 ".bf-block[data-colorway=slate]{background:#e4e8ea;color:#28343b}"
 ".bf-block[data-colorway=brick]{background:#f1e4db;color:#612d28}"
 ".bf-block[data-colorway=copper]{background:#f5e7d5;color:#5a381f}"
 ".bf-block[data-edition-format=cover-spread]{padding:clamp(32px,7vw,110px);min-height:50vh}"
 ".bf-block[data-edition-format=broadsheet]{border-top:10px double currentColor;text-align:center;padding:55px 24px}"
 ".bf-block[data-edition-format=manifesto]{border-left:10px solid currentColor;padding:40px 55px}"
 ".bf-block[data-edition-format=column]{max-width:73ch;margin:0 auto 30px;padding:30px}"
 ".bf-block[data-edition-format=field-report]{border-top:5px solid #b59162;padding:36px}"
 ".bf-block[data-edition-format=business-review]{border-top:5px solid #809b8c;padding:40px}"
 ".bf-block[data-edition-format=market-ledger]{border-top:5px solid #a26f45;padding:40px}"
 ".bf-block[data-edition-format=archive]{border:1px double #8a8d7c;padding:clamp(25px,5vw,80px)}"
 ".bf-block[data-edition-format=dossier]{border-left:5px solid #83938a;padding:40px}"
 ".bf-block[data-edition-format=research-note]{border:1px solid #a0aba5;padding:40px}"
 ".bf-block[data-edition-format=system-brief]{border-top:8px solid #597d70;padding:38px}"
 ".bf-block[data-edition-format=culture-feature]{padding:clamp(30px,5vw,75px);border-bottom:4px solid #c28b60}"
 ".bf-block[data-edition-format=comparative]{border-top:3px solid #728d88;border-bottom:3px solid #728d88;padding:50px}"
 ".bf-block[data-edition-format=studio-edition]{padding:45px;box-shadow:inset 0 0 0 1px #c8cbc3}"
 ".bf-journal-back{margin-top:65px;padding:25px;border-top:1px solid #c9d3cb;font:700 13px system-ui}"
 ".bf-journal-back a{color:#274b3b;text-decoration-thickness:1px}"
 ".bf-edition-image{margin:25px 0 0;max-width:100%;background:#101d1b;overflow:hidden}"
 ".bf-edition-image img{display:block;width:100%;height:auto;max-height:450px;object-fit:cover}"
 ".bf-edition-image figcaption{padding:12px 15px;font:12px/1.5 system-ui;color:#f3f4e9;background:#172a25}"
 ".bf-edition-image figcaption a{color:inherit;text-decoration:underline;text-underline-offset:3px}"
 ".bf-chapter-cover{border-top:1px solid currentColor;margin-top:35px;padding:18px 0 28px;max-width:68ch}"
 ".bf-chapter-cover span{display:block;letter-spacing:.14em;text-transform:uppercase;font:bold 11px system-ui;opacity:.75}"
 ".bf-chapter-cover strong{display:block;margin:18px 0 5px;font:500 clamp(23px,3vw,49px)/1.16 Georgia,serif;letter-spacing:-.02em}"
 ".bf-cover-source{font:600 12px/1.6 system-ui;letter-spacing:.06em;padding:60px 20px;background:transparent;border:1px solid currentColor;margin-top:24px}"
 "@media(max-width:680px){.bf-block[data-edition-format]{padding:22px}.bf-cover-source{padding:32px 15px}}"
 ".bf-article-toc{border:1px solid #b9c6bd;padding:clamp(18px,2.8vw,33px);margin:34px auto 45px;max-width:830px;background:#f2f4ee;color:#203b31}"
 ".bf-article-toc>p{font:800 11px system-ui;letter-spacing:.16em;text-transform:uppercase;margin:0 0 18px}"
 ".bf-article-toc ol{margin:0;padding:0;list-style:none;columns:2;column-gap:35px}"
 ".bf-article-toc li{break-inside:avoid;margin:0;padding:0;border-top:1px solid #d0dbd3}"
 ".bf-article-toc a{display:flex;gap:18px;padding:13px 0;color:inherit;font:500 15px/1.3 Georgia,serif;text-decoration:none}"
 ".bf-article-toc a:hover,.bf-article-toc a:focus-visible{text-decoration:underline;text-underline-offset:4px}"
 ".bf-article-toc a span:first-child{font:800 11px system-ui;color:#657e6c;padding-top:3px}"
 "html{scroll-behavior:smooth}.bf-markdown h2[id]{scroll-margin-top:28px}"
 "@media(max-width:680px){.bf-article-toc ol{columns:1}.bf-article-toc{padding:18px}}"
 ".bf-markdown{font:1.13rem/1.78 Georgia,serif;max-width:77ch;margin:auto}"
 ".bf-markdown h2{font:600 clamp(29px,3vw,42px)/1.18 Georgia,serif;margin:1.7em 0 .6em}"
 ".bf-markdown h3{font:600 1.45rem/1.3 Georgia,serif;margin:1.3em 0 .5em}"
 ".bf-markdown a{color:inherit;text-decoration-color:#ae824f;text-underline-offset:3px}"
 ".bf-markdown p{margin:1.1em 0}.bf-markdown li{margin:.5em 0}"
 ".bf-markdown ul,.bf-markdown ol{padding-left:1.6em}"
 ".bf-markdown blockquote{font-style:italic;margin:1.5em 0}"
 ".bf-markdown table{display:block;max-width:100%;overflow-x:auto}"
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
 if(bf_editorial_visual(s->route)){
  fputs("<nav class=\"bf-journal-back\" aria-label=\"Original Journal source\"><a href=\"https://nickgonzales76017.github.io/bonfyre",f);
  bf_html_escape(f,s->route);
  fputs("\">Read the original published edition</a></nav>",f);
 }
 fputs("</main><footer role=\"note\" style=\"padding:20px;text-align:center\">"
 "Authoring preview only. No publication, rights or account admission inferred.</footer></body></html>\n",f);
 return !ferror(f);
}
#endif
