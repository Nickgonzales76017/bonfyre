#ifndef BONFYRE_SOURCE_LIBRARY_WORKSPACE_H
#define BONFYRE_SOURCE_LIBRARY_WORKSPACE_H
#include "form_tree.h"
/* Source library, native on the one shell. One flow: paste a link or pick a
 * source Aurekai already reads, see what it is, press Follow, and it is in
 * your library. Following writes one bonfyre.source-material.v1 link record
 * through the CMS workspace owner (same record the editor wrote); it never
 * fetches the page, signs in anywhere or tells the publisher. Catalogue rows
 * are SourceFrontier observations, not records of this person. */
#define SLW_MAX 60
typedef struct{char key[96],title[256],url[1100],created[32];} SlwSaved;
typedef struct{char ref[128],url[1100],host[256],family[65],carries[65];long long items;} SlwKnown;

/* WHATWG-stable subset of a public http(s) address: anything a browser would
 * rewrite is refused rather than guessed, so the id matches the editor's. */
static int slw_url(const char *in,char *out,size_t cap){
    while(*in==' ')in++;size_t n=strlen(in);while(n&&in[n-1]==' ')n--;
    if(!n||n>=cap||n>2000)return 0;
    char s[2048];memcpy(s,in,n);s[n]=0;
    for(size_t i=0;i<n;i++){unsigned char c=(unsigned char)s[i];if(c<=32||c>=127||strchr("\"<>\\^`{|}",c))return 0;}
    size_t scheme=0;if(!strncasecmp(s,"https://",8))scheme=8;else if(!strncasecmp(s,"http://",7))scheme=7;else return 0;
    char *host=s+scheme,*end=host+strcspn(host,"/?#");if(end==host||memchr(host,'@',(size_t)(end-host))||*host=='[')return 0;
    char h[256];size_t hl=(size_t)(end-host);if(hl>=sizeof(h))return 0;for(size_t i=0;i<hl;i++)h[i]=(char)tolower((unsigned char)host[i]);h[hl]=0;
    char *port=strchr(h,':');if(port){if(!port[1]||strspn(port+1,"0123456789")!=strlen(port+1))return 0;if((scheme==8&&!strcmp(port+1,"443"))||(scheme==7&&!strcmp(port+1,"80")))*port=0;}
    char bare[256];snprintf(bare,sizeof(bare),"%s",h);char *colon=strchr(bare,':');if(colon)*colon=0;
    if(!strchr(bare,'.')||!strcmp(bare,"localhost")||strspn(bare,"0123456789.")==strlen(bare)||(strlen(bare)>6&&!strcmp(bare+strlen(bare)-6,".local")))return 0;
    if(strstr(end,"/./")||strstr(end,"/../")||strchr(end,'#'))return 0;
    const char *q=strchr(end,'?');
    if(q){static const char *bad[]={"access_token=","api_key=","apikey=","password=","secret=","token="};for(const char *p=q;p&&*p;p=strchr(p+1,'&')){for(size_t k=0;k<6;k++)if(!strncasecmp(p+1,bad[k],strlen(bad[k])))return 0;}}
    int w=snprintf(out,cap,"%s%s%s%s",scheme==8?"https://":"http://",h,*end=='/'?"":"/",end);
    return w>0&&(size_t)w<cap;
}
static void slw_host(const char *url,char *out,size_t cap){
    const char *h=strstr(url,"://");h=h?h+3:url;size_t n=strcspn(h,"/?:");
    if(n>4&&!strncmp(h,"www.",4)){h+=4;n-=4;}else if(n>6&&!strncmp(h,"feeds.",6)){h+=6;n-=6;}
    if(n>=cap)n=cap-1;memcpy(out,h,n);out[n]=0;
}
static void slw_id(const char *url,char hex[65]){bf_sha256_hex((const uint8_t *)url,strlen(url),hex);}

/* Your followed sources, read through the workspace owner like the editor. */
static int slw_mp_text(bf_mp_reader_t *r,char *out,size_t cap){bf_mp_value_t v;if(bf_mp_read_next(r,&v)!=BF_MP_STR)return 0;size_t n=v.v.raw.len<cap-1?v.v.raw.len:cap-1;memcpy(out,v.v.raw.ptr,n);out[n]=0;return 1;}
static int slw_library(const char *id,const char *subject,const char *scope,const char *actor,SlwSaved *rows,int *count){
    *count=0;const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char cursor[512]="";
    for(int page=0;page<4&&*count<SLW_MAX;page++){
        char *const argv[]={(char *)bin,"pack-workspace","--operation","read","--binding",(char *)id,"--subject",(char *)subject,"--scope",(char *)scope,"--actor",(char *)actor,"--prefix","source-material:","--cursor",cursor,"--epoch","-1","--db",(char *)cms,NULL};
        unsigned char *out=NULL;size_t size=0;if(aw_native(argv,NULL,0,&out,&size)){free(out);return 0;}
        bf_mp_reader_t r;bf_mp_value_t v;bf_mp_reader_init(&r,out,size);int more=0,ok=bf_mp_read_next(&r,&v)==BF_MP_ARRAY&&v.v.count==5&&!bf_mp_skip(&r)&&!bf_mp_skip(&r);
        if(ok){ok=bf_mp_read_next(&r,&v)==BF_MP_BOOL;more=ok&&v.v.b;}
        ok=ok&&slw_mp_text(&r,cursor,sizeof(cursor))&&bf_mp_read_next(&r,&v)==BF_MP_ARRAY;uint32_t n=ok?v.v.count:0;
        for(uint32_t i=0;ok&&i<n;i++){
            SlwSaved row;memset(&row,0,sizeof(row));char state[16]="active";
            if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=3||!slw_mp_text(&r,row.key,sizeof(row.key))||bf_mp_skip(&r)){ok=0;break;}
            bf_mp_type_t t=bf_mp_read_next(&r,&v);if(t!=BF_MP_BIN)continue;
            bf_mp_reader_t b;bf_mp_value_t x;bf_mp_reader_init(&b,v.v.raw.ptr,v.v.raw.len);
            if(bf_mp_read_next(&b,&x)!=BF_MP_ARRAY||x.v.count!=3||bf_mp_skip(&b)||bf_mp_read_next(&b,&x)!=BF_MP_MAP)continue;
            uint32_t fields=x.v.count;char kind[16]="";
            for(uint32_t f=0;f<fields;f++){char name[24];if(!slw_mp_text(&b,name,sizeof(name)))break;
                char *dest=!strcmp(name,"title")?row.title:!strcmp(name,"url")?row.url:!strcmp(name,"createdAt")?row.created:!strcmp(name,"state")?state:!strcmp(name,"kind")?kind:NULL;
                size_t cap=dest==row.title?sizeof(row.title):dest==row.url?sizeof(row.url):dest==row.created?sizeof(row.created):16;
                bf_mp_reader_t peek=b;bf_mp_value_t y;if(dest&&bf_mp_read_next(&peek,&y)==BF_MP_STR)slw_mp_text(&b,dest,cap);else bf_mp_skip(&b);}
            if(strcmp(state,"active")||!*row.title)continue;
            if(*count<SLW_MAX)rows[(*count)++]=row;
        }
        free(out);if(!ok)return 0;if(!more)break;
    }
    return 1;
}
static int slw_saved(const SlwSaved *rows,int count,const char *url){char hex[65],key[96];slw_id(url,hex);snprintf(key,sizeof(key),"source-material:%s",hex);for(int i=0;i<count;i++)if(!strcmp(rows[i].key,key))return 1;return 0;}

/* Sources Aurekai already reads: the SourceFrontier catalogue, read-only. */
static int slw_known(const char *search,long long offset,SlwKnown *rows,int cap,int *count,long long *total,long long *next){
    static char out[4*1024*1024];const char *dir=getenv("BONFYRE_BIN_DIR");char bin[PATH_MAX],page[24],limit[8];*count=0;*total=0;*next=-1;
    if(!dir||!*dir)dir="/usr/local/bin";if(snprintf(bin,sizeof(bin),"%s/bonfyre-project",dir)>=(int)sizeof(bin))return 0;
    snprintf(page,sizeof(page),"%lld",offset);snprintf(limit,sizeof(limit),"%d",cap);
    char *argv[]={bin,"foreign","twins","--page",page,"--limit",limit,"--query",(char *)search,"--expect-generation","",NULL};
    out[0]=0;if(auth_call(argv,NULL,out,sizeof(out))!=0||!out[0])return 0;
    bf_json_doc_t *d=bf_json_parse(out,strlen(out),NULL,0);if(!d)return 0;
    const bf_json_node_t *root=bf_json_root(d),*twins=root?bf_json_obj_get(d,root,"twins"):NULL,*t=root?bf_json_obj_get(d,root,"total"):NULL,*nx=root?bf_json_obj_get(d,root,"next_offset"):NULL;
    if(t&&t->type==BF_JSON_INT)*total=t->val.i;if(nx&&nx->type==BF_JSON_INT)*next=nx->val.i;
    for(int i=0;twins&&twins->type==BF_JSON_ARRAY&&i<twins->child_count&&*count<cap;i++){
        const bf_json_node_t *row=bf_json_arr_get(d,twins,i),*url=row?bf_json_obj_get(d,row,"url"):NULL,*rel=row?bf_json_obj_get(d,row,"release"):NULL,
            *sc=rel?bf_json_obj_get(d,rel,"source_contract"):NULL,*st=rel?bf_json_obj_get(d,rel,"stages"):NULL,*ar=st?bf_json_obj_get(d,st,"archive"):NULL,*it=ar?bf_json_obj_get(d,ar,"items"):NULL,
            *ref=sc?bf_json_obj_get(d,sc,"source"):NULL,*fam=sc?bf_json_obj_get(d,sc,"family"):NULL,*car=sc?bf_json_obj_get(d,sc,"carries"):NULL;
        SlwKnown k;memset(&k,0,sizeof(k));
        if(!url||bf_json_get_str_copy(url,k.url,sizeof(k.url))<=0||!ref||bf_json_get_str_copy(ref,k.ref,sizeof(k.ref))<=0)continue;
        if(fam)bf_json_get_str_copy(fam,k.family,sizeof(k.family));if(car)bf_json_get_str_copy(car,k.carries,sizeof(k.carries));
        k.items=it&&it->type==BF_JSON_INT?it->val.i:0;slw_host(k.url,k.host,sizeof(k.host));
        /* Feed relays name the show in the path, not the host. */
        if(!strcmp(k.host,"feedburner.com")||!strcmp(k.host,"feedblitz.com")||!strncmp(k.host,"rss.",4)){
            const char *pp=strstr(k.url,"://");pp=pp?strchr(pp+3,'/'):NULL;char seg[128]="";
            for(const char *x=pp;x&&*x;){x++;size_t n=strcspn(x,"/?&");if(n&&n<sizeof(seg)){memcpy(seg,x,n);seg[n]=0;}x+=n;if(*x!='/')break;}
            if(*seg)snprintf(k.host,sizeof(k.host),"%s",seg);
        }
        rows[(*count)++]=k;
    }
    bf_json_free(d);return 1;
}

static int slw_param(const char *query,const char *name,char *out,size_t cap){
    out[0]=0;size_t len=strlen(name);
    for(const char *p=query?query:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);
        if((size_t)(end-p)>len&&!strncmp(p,name,len)&&p[len]=='=')return cw_catalog_decode(p+len+1,(size_t)(end-p-len-1),out,cap);
        p=*end?end+1:end;}
    return 0;
}
static void slw_follow_form(const char *id,const char *url,const char *title,const char *back,int primary){
    char action[200];snprintf(action,sizeof(action),"/apps/%s/_follow",id);
    const FtField hidden[]={{"url",url},{title&&*title?"title":NULL,title},{back&&*back?"back":NULL,back}};
    const FtSubmit go[]={{.label="Follow",.tone=primary?NULL:"quiet"}};
    const FtForm f={.id="src-follow",.action=action,.hook="src-follow",.hidden=hidden,.nhidden=3,.submits=go,.nsubmits=1};
    ft_form(stdout,&f);
}

static int slw_follow(const char *method,const char *body,const char *raw,const char *host,const char *id,const char *subject,const char *scope,const char *actor){
    char origin[300],expected[300],url_in[2100]="",title[256]="",back[600]="",url[1100];
    if(strcmp(method,"POST")){aw_error(405,"Use the Follow button.\n");return 1;}
    int local=host&&(!strncmp(host,"127.0.0.1:",10)||!strncmp(host,"localhost:",10));snprintf(expected,sizeof(expected),"%s://%s",local?"http":"https",host?host:"");
    if(!aw_header(raw,"Origin",origin,sizeof(origin))||strcmp(origin,expected)){aw_error(403,"Follow from your own source library page.\n");return 1;}
    slw_param(body,"url",url_in,sizeof(url_in));slw_param(body,"title",title,sizeof(title));slw_param(body,"back",back,sizeof(back));
    if(*back&&(strchr(back,'\r')||strchr(back,'\n')||strstr(back,"//")||*back=='/'))*back=0;
    printf("HTTP/1.1 303 See Other\r\nLocation: /apps/%s/sources/",id);
    if(!slw_url(url_in,url,sizeof(url))){fputs("?mode=link&error=address",stdout);goto sent;}
    SlwSaved *rows=calloc(SLW_MAX,sizeof(*rows));int count=0;
    if(!rows||!slw_library(id,subject,scope,actor,rows,&count)){free(rows);fputs("?error=unavailable",stdout);goto sent;}
    int already=slw_saved(rows,count,url);free(rows);
    char hex[65],key[96],host_name[256],when[40],request[80];slw_id(url,hex);snprintf(key,sizeof(key),"source-material:%s",hex);
    if(!already){
        slw_host(url,host_name,sizeof(host_name));time_t now=time(NULL);struct tm tm;gmtime_r(&now,&tm);strftime(when,sizeof(when),"%Y-%m-%dT%H:%M:%S.000Z",&tm);
        snprintf(request,sizeof(request),"follow-%.24s-%lld",hex,(long long)now);
        bf_mp_writer_t v,p;bf_mp_writer_init(&v,512);bf_mp_writer_init(&p,768);
        int ok=!bf_mp_pack_array(&v,3)&&!bf_mp_pack_uint(&v,2)&&!bf_mp_pack_map(&v,12)&&
            !bf_mp_pack_kv_str(&v,"schema","bonfyre.source-material.v1")&&!bf_mp_pack_kv_str(&v,"id",hex)&&!bf_mp_pack_kv_str(&v,"kind","link")&&!bf_mp_pack_kv_str(&v,"state","active")&&
            !bf_mp_pack_kv_str(&v,"title",*title?title:host_name)&&!bf_mp_pack_kv_str(&v,"collection","")&&!bf_mp_pack_str_cstr(&v,"tags")&&!bf_mp_pack_array(&v,0)&&
            !bf_mp_pack_kv_str(&v,"notes","")&&!bf_mp_pack_kv_str(&v,"url",url)&&!bf_mp_pack_kv_str(&v,"contentSHA256",hex)&&!bf_mp_pack_kv_str(&v,"createdAt",when)&&
            !bf_mp_pack_str_cstr(&v,"history")&&!bf_mp_pack_array(&v,0)&&!bf_mp_pack_array(&v,0)&&
            !bf_mp_pack_array(&p,3)&&!bf_mp_pack_uint(&p,1)&&!bf_mp_pack_str_cstr(&p,request)&&!bf_mp_pack_array(&p,1)&&!bf_mp_pack_array(&p,4)&&
            !bf_mp_pack_str_cstr(&p,key)&&!bf_mp_pack_uint(&p,0)&&!bf_mp_pack_uint(&p,0)&&!bf_mp_pack_bin(&p,v.buf,(uint32_t)v.len);
        int rc=6;
        if(ok){const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms"),*cms=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
            char *const argv[]={(char *)bin,"pack-workspace","--operation","write","--binding",(char *)id,"--subject",(char *)subject,"--scope",(char *)scope,"--actor",(char *)actor,"--prefix","","--cursor","","--epoch","-1","--db",(char *)cms,NULL};
            unsigned char *out=NULL;size_t size=0;rc=aw_native(argv,p.buf,p.len,&out,&size);free(out);}
        bf_mp_writer_free(&v);bf_mp_writer_free(&p);
        if(rc){printf("?error=%s",rc==4?"changed":"unavailable");goto sent;}
    }
    printf("?followed=%s",hex);if(*back){fputs("&",stdout);fputs(back,stdout);}fputs("#library",stdout);
 sent:fputs("\r\nCache-Control: private, no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;
}

static void slw_card(const char *id,const SlwKnown *k,const SlwSaved *rows,int count,const char *back){
    char url[1100];int valid=slw_url(k->url,url,sizeof(url));
    fputs("<article class=\"ak-src-card\"><header><h3><a href=\"/sources?source=",stdout);url_query_value(k->ref);fputs("\">",stdout);html(k->host);fputs("</a></h3>",stdout);
    if(*k->family){ui_text(stdout, "span", "ak-tag is-on", k->family);}
    fputs("</header><p class=\"ak-src-meta\"><span class=\"ak-src-path\">",stdout);{const char *pp=strstr(k->url,"://");html(pp?pp+3:k->url);}fputs("</span>",stdout);
    if(k->items>0)printf("%lld pieces to read",k->items);else fputs("Nothing to read yet",stdout);
    if(*k->carries&&strcmp(k->carries,"text")){fputs(" &middot; carries ",stdout);html(k->carries);}
    fputs("</p><footer><a class=\"ak-src-read\" href=\"/sources?source=",stdout);url_query_value(k->ref);fputs("\">Read what it publishes</a>",stdout);
    if(valid&&slw_saved(rows,count,url))ui_text(stdout, "span", "ak-tag", "Following");
    else if(valid)slw_follow_form(id,url,k->host,back,0);
    fputs("</footer></article>",stdout);
}

static int slw_page(const char *query,const char *id,const char *subject,const char *scope,const char *actor,const char *context,const char *pack){
    char mode[8]="",q[160]="",pasted[2100]="",followed[80]="",error[24]="",off[16]="";
    slw_param(query,"mode",mode,sizeof(mode));slw_param(query,"q",q,sizeof(q));slw_param(query,"url",pasted,sizeof(pasted));
    slw_param(query,"followed",followed,sizeof(followed));slw_param(query,"error",error,sizeof(error));slw_param(query,"offset",off,sizeof(off));
    int link=!strcmp(mode,"link")||*pasted;long long offset=*off&&strspn(off,"0123456789")==strlen(off)?strtoll(off,NULL,10):0;
    SlwSaved *rows=calloc(SLW_MAX,sizeof(*rows));SlwKnown *known=calloc(24,sizeof(*known));int count=0,kn=0,lib_ok=rows&&slw_library(id,subject,scope,actor,rows,&count);long long total=0,next=-1;
    int known_ok=known&&!link&&slw_known(q,offset,known,24,&kn,&total,&next);
    char back[400]="";if(*q){char *b=back;b+=snprintf(b,sizeof(back),"q=");for(const char *s=q;*s&&b<back+sizeof(back)-4;s++){unsigned char c=(unsigned char)*s;if(isalnum(c)||strchr("-._~",c))*b++=(char)c;else b+=snprintf(b,4,"%%%02X",c);}*b=0;}
    psetup_open(200,"Your source library","ak-wb ak-src");
    size_t nav_bytes=0;unsigned char *nav=ac_navigation(id,context,pack,&nav_bytes);if(nav){fwrite(nav,1,nav_bytes,stdout);free(nav);}
    printf("<div class=\"ak-wb-strip\"><span>Links and sources</span><span><b>%d</b> in your library</span>",count);
    if(known_ok)printf("<span><b>%lld</b> sources Aurekai reads%s</span>",total,*q?" for this search":"");
    fputs("<span class=\"ak-live\">Nothing is fetched until you open it</span></div>",stdout);
    fputs("<section class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\">From the internet to your work</p><h1>Find a source. <em>Work with what it publishes.</em></h1>"
          "<p class=\"ak-lede\">Browse the sources Aurekai already reads, or bring a web address. Follow the ones you want and they stay in your library.</p></div></section>",stdout);
    if(*error){fputs("<p class=\"ak-src-alert\" role=\"alert\">",stdout);
        fputs(!strcmp(error,"address")?"That doesn't look like a public web address. Paste the full link, starting with https://.":!strcmp(error,"changed")?"Your library changed in another tab. Nothing was overwritten; reload and try again.":"Your library couldn't be saved just now. Nothing changed; try again in a moment.",stdout);fputs("</p>",stdout);}
    /* The finder: one control at a time. */
    fputs("<section class=\"ak-ask ak-src-finder\"><nav class=\"ak-src-modes\" aria-label=\"How to find a source\">",stdout);
    printf("<a href=\"/apps/%s/sources/\"%s>Browse known sources</a><a href=\"/apps/%s/sources/?mode=link\"%s>Bring a web address</a></nav>",id,link?"":" aria-current=\"page\"",id,link?" aria-current=\"page\"":"");
    if(!link){
        {char action[200];snprintf(action,sizeof(action),"/apps/%s/sources/",id);
         const FtInput find[]={{.label="Search sources",.type="search",.name="q",.maxlength="140",.value=q,.placeholder="Search by name, address or topic"}};
         const FtForm f={.id="src-search",.method="get",.action=action,.label="Search",.hook="src-search",.inputs=find,.ninputs=1};
         ft_form(stdout,&f);}
        fputs("<nav class=\"ak-src-topics\" aria-label=\"Topics\">",stdout);
        static const char *topics[][2]={{"","All sources"},{"podcast","Podcasts"},{"music","Music"},{"science","Science"},{"video","Video"},{"recall","Recalls"},{"gov","Government"}};
        for(size_t i=0;i<sizeof(topics)/sizeof(topics[0]);i++){printf("<a class=\"ak-chip\" href=\"/apps/%s/sources/%s%s\"%s>",id,*topics[i][0]?"?q=":"",topics[i][0],!strcmp(q,topics[i][0])?" aria-current=\"page\"":"");fputs(topics[i][1],stdout);if(!strcmp(q,topics[i][0])&&known_ok)printf(" <b>%lld</b>",total);fputs("</a>",stdout);}
        fputs("</nav>",stdout);
    }else{
        char url[1100];int valid=*pasted&&slw_url(pasted,url,sizeof(url));
        {char action[200];snprintf(action,sizeof(action),"/apps/%s/sources/",id);
         const FtInput link_in[]={{.label="Web address",.type="url",.name="url",.required=1,.maxlength="2000",.value=pasted,.placeholder="Paste a link, like https://example.org/feed"}};
         const FtForm f={.id="src-link",.method="get",.action=action,.label="Look it up",.hook="src-link",.inputs=link_in,.ninputs=1};
         ft_form(stdout,&f);}
        if(*pasted&&!valid)fputs("<p class=\"ak-src-alert\" role=\"alert\">That doesn't look like a public web address. Use the full link, starting with https://, without a login or private network address.</p>",stdout);
        if(valid){
            char host_name[256];slw_host(url,host_name,sizeof(host_name));SlwKnown match[3];int mn=0;long long mt=0,mx=-1;
            int looked=slw_known(host_name,0,match,3,&mn,&mt,&mx);
            fputs("<article class=\"ak-src-found\"><p class=\"ak-kicker\">What it is</p><h2>",stdout);html(host_name);fputs("</h2><p class=\"ak-src-address\">",stdout);html(url);fputs("</p>",stdout);
            if(looked&&mn){fputs("<p>Aurekai already reads this publisher. ",stdout);printf("%s</p><ul class=\"ak-rows\">",mn==1?"Here is what it publishes:":"These are its feeds:");
                for(int i=0;i<mn;i++){fputs("<li><strong>",stdout);html(match[i].host);fputs("</strong><small>",stdout);if(*match[i].family){html(match[i].family);fputs(" &middot; ",stdout);}printf("%lld pieces",match[i].items);fputs("</small><a class=\"ak-go\" href=\"/sources?source=",stdout);url_query_value(match[i].ref);fputs("\">Read</a></li>",stdout);}
                fputs("</ul>",stdout);}
            else fputs("<p>Aurekai doesn't read this address yet. Following keeps the link in your library so you can come back to it.</p>",stdout);
            if(lib_ok&&slw_saved(rows,count,url))ui_text(stdout, "p", "ak-src-done", "Already in your library.");
            else{fputs("<div class=\"ak-actions\">",stdout);slw_follow_form(id,url,NULL,"mode=link",1);printf("<a class=\"ak-btn is-quiet\" href=\"/apps/%s/sources/?mode=link\">Not this one</a></div>",id);}
            fputs("<p class=\"ak-src-fine\">Following saves the address only. Aurekai doesn't sign in anywhere, copy paywalled pages or tell the publisher.</p></article>",stdout);
        }
    }
    fputs("</section>",stdout);
    /* Your library: where a followed source appears. */
    fputs("<section class=\"ak-sec\" id=\"library\"><header><div><span class=\"ak-kicker\">Your library</span><h2>Sources you follow</h2></div></header>",stdout);
    if(!lib_ok)fputs("<p class=\"ak-empty\">Your library couldn't be read just now. Nothing was changed.</p>",stdout);
    else if(!count)ui_text(stdout, "p", "ak-empty", "Nothing here yet. Paste a link above, or press Follow on a source below.");
    else{fputs("<ul class=\"ak-rows ak-src-library\">",stdout);
        for(int i=0;i<count;i++){char h[256];slw_host(rows[i].url,h,sizeof(h));int fresh=*followed&&strstr(rows[i].key,followed)!=NULL;
            printf("<li%s><strong>",fresh?" data-fresh":"");html(rows[i].title);fputs("</strong><small>",stdout);html(h);
            if(*rows[i].created){fputs(" &middot; added ",stdout);char day[11];snprintf(day,sizeof(day),"%.10s",rows[i].created);html(day);}
            if(fresh)fputs(" &middot; just added",stdout);
            fputs("</small>",stdout);if(*rows[i].url){fputs("<a class=\"ak-go\" href=\"",stdout);html(rows[i].url);fputs("\" rel=\"noopener noreferrer\" target=\"_blank\">Open &#8599;</a>",stdout);}fputs("</li>",stdout);}
        fputs("</ul>",stdout);}
    fputs("</section>",stdout);
    if(!link){
        fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Sources Aurekai reads</span><h2>",stdout);
        if(*q){fputs("Matching &ldquo;",stdout);html(q);fputs("&rdquo;",stdout);}else fputs("Known sources",stdout);fputs("</h2></div>",stdout);
        if(known_ok)printf("<p>%lld sources. Each one links to what it has published.</p>",total);fputs("</header>",stdout);
        if(!known_ok)fputs("<p class=\"ak-empty\">The list of known sources couldn't be read just now.</p>",stdout);
        else if(!kn)printf("<p class=\"ak-empty\">No known source matches that. <a href=\"/apps/%s/sources/?mode=link\">Bring the web address instead.</a></p>",id);
        else{fputs("<div class=\"ak-grid is-2 ak-src-grid\">",stdout);for(int i=0;i<kn;i++)slw_card(id,&known[i],rows,count,back);fputs("</div>",stdout);
            if(next>=0){printf("<p class=\"ak-src-more\"><a class=\"ak-btn\" href=\"/apps/%s/sources/?offset=%lld",id,next);if(*back){fputs("&amp;",stdout);html(back);}fputs("\">More sources</a></p>",stdout);}}
        fputs("</section>",stdout);
    }
    psetup_end();free(rows);free(known);return 1;
}
#endif
